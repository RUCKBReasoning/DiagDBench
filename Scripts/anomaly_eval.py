#!/usr/bin/env python3
"""Single-file anomaly evaluation script for containerized database targets."""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import re
import signal
import shutil
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence


# Number of MEASURED verification-workload runs. Defaults to 1 (a single
# measured run, whose value is its own median), preceded by one warm-up pass to
# absorb cold-start (buffer-pool / cache) cost. Set `--median-runs N` (N > 1) to
# take the median over N measured runs and smooth out latency noise. Every run
# (warm-up and measured alike) first re-applies the initialization workload so a
# repeated run always starts from the same restored state.
DEFAULT_RUNS = 1
SUCCESS_THRESHOLD_PERCENT = 20.0
PROCESS_TERMINATION_GRACE_SECONDS = 5.0


class EvaluationError(Exception):
    """Raised when the evaluation flow cannot continue."""


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    user: str
    database: str
    schema: str
    password: str


def normalize_dbms(dbms: str) -> str:
    return "mysql" if "mysql" in str(dbms).strip().lower() else "pg"


@dataclass(frozen=True)
class SqlRunResult:
    """Stored SQL execution details. These values are intentionally not logged."""

    duration_seconds: float
    return_code: int
    stdout: str
    stderr: str
    has_error: bool


@dataclass(frozen=True)
class PhaseResult:
    phase_name: str
    durations: List[float]
    runs: List[SqlRunResult]
    median_seconds: float


@dataclass(frozen=True)
class OutputFilterRule:
    """A line-oriented rule for suppressing deterministic client-side noise."""

    name: str
    pattern: re.Pattern[str]


DEFAULT_OUTPUT_NOISE_RULES: Sequence[OutputFilterRule] = (
    OutputFilterRule(
        name="client_cli_credential_noise",
        pattern=re.compile(
            r"^[^:\n]+:\s+\[(Warning|WARNING)\]\s+.*\b(password|credential)s?\b"
            r".*\b(command line|command-line|cli)\b.*\b(insecure|unsafe)\b.*$",
            re.IGNORECASE,
        ),
    ),
)


class OutputSanitizer:
    """Normalize child-process output before it is written to observation logs.

    The sanitizer is intentionally conservative: it filters only deterministic
    client-side noise that carries no diagnostic value, while preserving SQL
    errors, shell errors, DBMS warnings, and timing/progress messages. It also
    redacts known secret values so generated scripts can be diagnosed without
    leaking credentials into LLM-visible logs.
    """

    def __init__(
        self,
        env: Dict[str, str],
        noise_rules: Sequence[OutputFilterRule] = DEFAULT_OUTPUT_NOISE_RULES,
    ) -> None:
        self.noise_rules = tuple(noise_rules)
        self.secret_values = tuple(
            sorted(
                {
                    value
                    for key, value in env.items()
                    if self._is_secret_key(key) and value
                },
                key=len,
                reverse=True,
            )
        )

    @staticmethod
    def _is_secret_key(key: str) -> bool:
        normalized = key.upper()
        return any(token in normalized for token in ("PASSWORD", "TOKEN", "SECRET", "API_KEY"))

    def should_suppress_line(self, line: str) -> bool:
        stripped = line.rstrip("\r\n")
        return any(rule.pattern.match(stripped) for rule in self.noise_rules)

    def sanitize_line(self, line: str) -> Optional[str]:
        if self.should_suppress_line(line):
            return None

        sanitized = line
        for secret in self.secret_values:
            sanitized = sanitized.replace(secret, "[REDACTED]")
        return sanitized


def forward_sanitized_stream(
    source,
    target,
    sanitizer: OutputSanitizer,
) -> None:
    """Forward a text stream line by line after sanitization."""

    for line in source:
        sanitized = sanitizer.sanitize_line(line)
        if sanitized is None:
            continue
        target.write(sanitized)
        target.flush()


def terminate_process_group(process: subprocess.Popen[str]) -> None:
    """Terminate a subprocess group without leaving long-lived children behind."""

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        process.wait(timeout=PROCESS_TERMINATION_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return


def configure_logging() -> None:
    """Configure unified logging output."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse required command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Evaluate anomaly injection by comparing SQL runtime medians."
    )
    parser.add_argument("--script-path", required=True, type=Path, help="Path to the anomaly injection shell script (.sh)")
    parser.add_argument("--verified-workload-path", required=True, type=Path, help="Path to the verification workload SQL file (.sql)")
    parser.add_argument("--initialization-workload-path", required=True, type=Path, help="Path to the initialization workload SQL file (.sql)")
    parser.add_argument("--dbms", default="PostgreSQL 16.11", help="Database system name, e.g. PostgreSQL 16.11 or MySQL 8.4")
    parser.add_argument("--host", required=True, help="Database host")
    parser.add_argument("--port", required=True, type=int, help="Database port")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--schema", required=True, help="Database schema")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument(
        "--timeout",
        required=True,
        type=float,
        help="Global timeout for the whole Python evaluation flow, in seconds",
    )
    parser.add_argument(
        "--median-runs",
        dest="median_runs",
        type=int,
        default=1,
        help="Number of MEASURED runs (median taken), preceded by one warm-up "
             "pass. Default 1 = one warm-up + one measured run. Use N > 1 to take "
             "the median over N measured runs; each run first re-applies the "
             "initialization workload.",
    )
    # Optional override for the performance-change success threshold (percent).
    parser.add_argument(
        "--success-threshold-percent",
        dest="success_threshold_percent",
        type=float,
        default=20,
        help="Performance-change percentage threshold used to judge anomaly success",
    )
    return parser.parse_args(argv)


def validate_file(path: Path, suffix: str, description: str) -> Path:
    """Validate input files before execution starts."""

    resolved = path.expanduser().resolve()
    if not resolved.exists():
        raise EvaluationError(f"{description} does not exist: {resolved}")
    if not resolved.is_file():
        raise EvaluationError(f"{description} is not a regular file: {resolved}")
    if resolved.suffix.lower() != suffix:
        raise EvaluationError(f"{description} must use the {suffix} suffix: {resolved}")
    if not os.access(resolved, os.R_OK):
        raise EvaluationError(f"{description} is not readable: {resolved}")
    return resolved


def detect_container() -> bool:
    """Best-effort Docker/container detection."""

    if Path("/.dockerenv").exists():
        return True
    try:
        cgroup_text = Path("/proc/1/cgroup").read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    return any(token in cgroup_text for token in ("docker", "containerd", "kubepods"))


def remaining_seconds(deadline: float) -> float:
    """Return remaining wall-clock seconds before the global deadline."""

    left = deadline - time.monotonic()
    if left <= 0:
        raise EvaluationError("The whole Python evaluation flow timed out.")
    return left


def is_timeout_error(exc: EvaluationError) -> bool:
    """Return True when an evaluation error was caused by a timeout.

    Timeout-style errors (raised by run_command on subprocess.TimeoutExpired and
    by remaining_seconds when the global deadline is exhausted) all contain the
    phrase "timed out". This is used to tell apart a workload that was blocked by
    the injected anomaly from other, non-timeout failures.
    """

    return "timed out" in str(exc).lower()


def run_command(
    command: Sequence[str],
    deadline: float,
    description: str,
    env: Optional[Dict[str, str]] = None,
    check: bool = True,
    log_command: bool = True,
    log_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run a command with the remaining global timeout."""

    timeout = remaining_seconds(deadline)
    if log_command:
        logging.info("Running %s.", description)
    try:
        completed = subprocess.run(
            list(command),
            text=True,
            capture_output=True,
            timeout=timeout,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise EvaluationError(
            f"{description} timed out with {timeout:.2f} seconds remaining."
        ) from exc
    except FileNotFoundError as exc:
        raise EvaluationError(f"{description} failed because the command was not found: {command[0]}") from exc
    except PermissionError as exc:
        raise EvaluationError(f"{description} failed because permission was denied: {command[0]}") from exc

    if log_output and completed.stdout.strip():
        logging.info("%s stdout:\n%s", description, completed.stdout.strip())
    if log_output and completed.stderr.strip():
        logging.info("%s stderr:\n%s", description, completed.stderr.strip())

    if check and completed.returncode != 0:
        raise EvaluationError(
            f"{description} failed with return code {completed.returncode}."
        )
    return completed


def install_package_if_possible(package_name: str, deadline: float) -> bool:
    """Try to install a missing runtime package inside a container."""

    package_managers = [
        (["apt-get", "update"], ["apt-get", "install", "-y", package_name]),
        (["apk", "update"], ["apk", "add", "--no-cache", package_name]),
        (["dnf", "install", "-y", package_name], None),
        (["yum", "install", "-y", package_name], None),
    ]
    for prepare_cmd, install_cmd in package_managers:
        manager = prepare_cmd[0]
        if shutil.which(manager) is None:
            continue
        try:
            logging.info("Trying to install missing dependency %s with %s.", package_name, manager)
            if install_cmd is None:
                run_command(prepare_cmd, deadline, f"installing {package_name}")
            else:
                run_command(prepare_cmd, deadline, f"{manager} preparation")
                run_command(install_cmd, deadline, f"installing {package_name}")
            return True
        except EvaluationError as exc:
            logging.error("Installation of %s with %s failed: %s", package_name, manager, exc)
    return False


def ensure_runtime(deadline: float, in_container: bool, dbms: str) -> str:
    """Check and, when possible, install the minimal runtime tools."""

    logging.info("Python version: %s", sys.version.split()[0])
    if importlib.util.find_spec("pip") is not None:
        logging.info("Detected pip in the current environment.")
    else:
        logging.info("pip was not detected. Trying to install it with ensurepip.")
        run_command([sys.executable, "-m", "ensurepip", "--upgrade"], deadline, "installing pip")

    dbms_kind = normalize_dbms(dbms)
    if dbms_kind == "mysql":
        mysql_path = shutil.which("mysql")
        if mysql_path:
            logging.info("Detected mysql client: %s", mysql_path)
            return mysql_path

        if not in_container:
            raise EvaluationError(
                "mysql client was not found and automatic installation is only supported inside a container."
            )

        logging.info("mysql client was not found in the container. Trying to install a MySQL client.")
        package_candidates = ["mysql-client", "default-mysql-client"]
        for package_name in package_candidates:
            if install_package_if_possible(package_name, deadline):
                mysql_path = shutil.which("mysql")
                if mysql_path:
                    logging.info("mysql client installation succeeded: %s", mysql_path)
                    return mysql_path
        raise EvaluationError(
            "Automatic mysql client installation failed. Please preinstall a MySQL client in the container."
        )

    psql_path = shutil.which("psql")
    if psql_path:
        logging.info("Detected psql: %s", psql_path)
        return psql_path

    if not in_container:
        raise EvaluationError(
            "psql was not found and automatic installation is only supported inside a container."
        )

    logging.info("psql was not found in the container. Trying to install a PostgreSQL client.")
    package_candidates = ["postgresql-client", "postgresql15-client", "postgresql"]
    for package_name in package_candidates:
        if install_package_if_possible(package_name, deadline):
            psql_path = shutil.which("psql")
            if psql_path:
                logging.info("psql installation succeeded: %s", psql_path)
                return psql_path
    raise EvaluationError(
        "Automatic psql installation failed. Please preinstall a PostgreSQL client in the container."
    )


def build_db_env(config: DatabaseConfig, dbms: str) -> Dict[str, str]:
    """Create a clean client environment for SQL and shell workloads."""

    env = os.environ.copy()
    dbms_kind = normalize_dbms(dbms)
    env.update(
        {
            "PGHOST": config.host,
            "PGPORT": str(config.port),
            "PGUSER": config.user,
            "PGDATABASE": config.database,
            "PGPASSWORD": config.password,
            "MYSQL_PWD": config.password,
            "DB_HOST": config.host,
            "DB_PORT": str(config.port),
            "DB_USER": config.user,
            "DB_NAME": config.database,
            "DB_SCHEMA": config.schema,
            "DB_PASSWORD": config.password,
            "DBMS": dbms,
        }
    )
    if dbms_kind != "mysql":
        env["PGOPTIONS"] = f"-c search_path={config.schema}"
    return env


def build_sql_client_command(
    client_path: str,
    sql_path: Path,
    db_config: DatabaseConfig,
    dbms: str,
    stop_on_error: bool,
) -> List[str]:
    if normalize_dbms(dbms) == "mysql":
        command = [
            client_path,
            "-h", db_config.host,
            "-P", str(db_config.port),
            "-u", db_config.user,
        ]
        if not stop_on_error:
            command.append("--force")
        command.extend([db_config.database, "-e", f"source {sql_path}"])
        return command

    on_error_stop = "1" if stop_on_error else "0"
    return [
        client_path,
        "-X",
        "-q",
        "-v", f"ON_ERROR_STOP={on_error_stop}",
        "-f", str(sql_path),
    ]


def run_initialization_workload(
    initialization_path: Path,
    client_path: str,
    db_config: DatabaseConfig,
    dbms: str,
    db_env: Dict[str, str],
    deadline: float,
) -> SqlRunResult:
    """Run the initialization (state-restoration) workload before a verified run.

    The initialization workload is a PRECONDITION for the verified/user workload:
    it restores the data-access state so every (warm-up or measured) run starts
    from the same baseline. Its timing/result is recorded for the record -- it is
    NOT used to drive the pass/fail or performance decision, which is based on the
    user (verified) workload only. Non-timeout SQL errors are ignored here because
    the restoration statements are best-effort.
    """

    start = time.perf_counter()
    completed = run_command(
        build_sql_client_command(client_path, initialization_path, db_config, dbms, False),
        deadline,
        "initialization workload",
        env=db_env,
        check=False,
        log_command=False,
        log_output=False,
    )
    elapsed = time.perf_counter() - start
    return SqlRunResult(
        duration_seconds=elapsed,
        return_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        has_error=completed.returncode != 0,
    )


def run_verified_workload_once(
    verified_path: Path,
    client_path: str,
    db_config: DatabaseConfig,
    dbms: str,
    db_env: Dict[str, str],
    deadline: float,
) -> SqlRunResult:
    """Run verification workload once and store output only in memory."""

    start = time.perf_counter()
    completed = run_command(
        build_sql_client_command(client_path, verified_path, db_config, dbms, True),
        deadline,
        "verification workload",
        env=db_env,
        check=False,
        log_command=False,
        log_output=False,
    )
    elapsed = time.perf_counter() - start
    return SqlRunResult(
        duration_seconds=elapsed,
        return_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        has_error=completed.returncode != 0,
    )


def run_warmup_workload(
    initialization_path: Path,
    verified_path: Path,
    client_path: str,
    db_config: DatabaseConfig,
    dbms: str,
    db_env: Dict[str, str],
    deadline: float,
) -> None:
    """Run one unmeasured workload pass before baseline timing.

    The first workload execution can pay disk-read and DB buffer-pool cold-start
    costs. Running the same initialization + verification sequence once before
    baseline timing keeps that cold-start cost out of the measured median while
    preserving the existing per-run initialization behaviour.
    """

    init_result = run_initialization_workload(
        initialization_path,
        client_path,
        db_config,
        dbms,
        db_env,
        deadline,
    )
    result = run_verified_workload_once(
        verified_path,
        client_path,
        db_config,
        dbms,
        db_env,
        deadline,
    )
    logging.info(
        "warmup initialization workload completed in %.6f seconds (precondition, not scored).",
        init_result.duration_seconds,
    )
    if result.has_error:
        logging.error("warmup user workload failed in %.6f seconds.", result.duration_seconds)
        raise EvaluationError("warmup verification workload failed.")
    logging.info("warmup user workload completed in %.6f seconds (timing discarded).", result.duration_seconds)


def run_measured_workload_phase(
    phase_name: str,
    initialization_path: Path,
    verified_path: Path,
    client_path: str,
    db_config: DatabaseConfig,
    dbms: str,
    db_env: Dict[str, str],
    deadline: float,
    raise_on_error: bool = True,
) -> PhaseResult:
    """Run initialization + verification `DEFAULT_RUNS` times and take the median.

    Each iteration first re-applies the initialization workload, then executes
    and times the verification workload, so every measured run starts from the
    same restored state. With the default `DEFAULT_RUNS == 1` this is a single
    measured run (its own value being the median); with `--median-runs N` it is
    the median over N measured runs. A separate warm-up pass (see
    `run_warmup_workload`) always runs before this phase to absorb cold-start
    cost, regardless of the measured-run count.
    """

    durations: List[float] = []
    runs: List[SqlRunResult] = []
    for index in range(1, DEFAULT_RUNS + 1):
        # Each measured run first re-applies the initialization workload (a
        # precondition) and then times the verified/user workload. Both timings
        # are recorded, but only the user-workload duration feeds the median that
        # drives the decision; the initialization timing is informational.
        init_result = run_initialization_workload(initialization_path, client_path, db_config, dbms, db_env, deadline)
        result = run_verified_workload_once(verified_path, client_path, db_config, dbms, db_env, deadline)
        durations.append(result.duration_seconds)
        runs.append(result)
        logging.info(
            "%s run %d initialization workload completed in %.6f seconds (precondition, not scored).",
            phase_name, index, init_result.duration_seconds,
        )
        if result.has_error:
            logging.error("%s run %d user workload failed in %.6f seconds.", phase_name, index, result.duration_seconds)
        else:
            logging.info("%s run %d user workload completed in %.6f seconds.", phase_name, index, result.duration_seconds)

    # When raise_on_error is False, keep the failed result so the caller can treat
    # a verified-workload error after injection as a successful anomaly signal.
    if raise_on_error and any(item.has_error for item in runs):
        raise EvaluationError(f"{phase_name} verification workload failed.")

    median_seconds = statistics.median(durations)
    logging.info("%s median duration: %.6f seconds", phase_name, median_seconds)
    return PhaseResult(
        phase_name=phase_name,
        durations=durations,
        runs=runs,
        median_seconds=median_seconds,
    )


def run_injection_script(
    script_path: Path,
    db_env: Dict[str, str],
    deadline: float,
) -> None:
    """Run anomaly script and stream sanitized child output to the terminal."""

    shell_path = shutil.which("bash") or shutil.which("sh") or "/bin/sh"
    command = [shell_path, str(script_path)]
    env = db_env.copy()
    env["PYTHONUNBUFFERED"] = "1"
    sanitizer = OutputSanitizer(env)

    logging.info("========== Anomaly Injection Script: START ==========")
    logging.info("Running anomaly injection script with streaming output.")

    try:
        # Capture both streams and forward them through the same sanitizer. This
        # keeps real-time visibility while preventing deterministic client noise
        # and secrets from polluting the LLM-visible observation log.
        process = subprocess.Popen(
            command,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )

        assert process.stdout is not None
        assert process.stderr is not None
        stdout_thread = threading.Thread(
            target=forward_sanitized_stream,
            args=(process.stdout, sys.stdout, sanitizer),
            name="anomaly-eval-stdout-forwarder",
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=forward_sanitized_stream,
            args=(process.stderr, sys.stderr, sanitizer),
            name="anomaly-eval-stderr-forwarder",
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        try:
            return_code = process.wait(timeout=remaining_seconds(deadline))
        except subprocess.TimeoutExpired as exc:
            terminate_process_group(process)
            raise EvaluationError("anomaly injection script timed out.") from exc
        stdout_thread.join(timeout=1.0)
        stderr_thread.join(timeout=1.0)
    except FileNotFoundError as exc:
        raise EvaluationError(f"anomaly injection script failed because the command was not found: {command[0]}") from exc
    except PermissionError as exc:
        raise EvaluationError(f"anomaly injection script failed because permission was denied: {command[0]}") from exc

    logging.info("========== Anomaly Injection Script: END ==========")
    if return_code != 0:
        raise EvaluationError(f"anomaly injection script failed with return code {return_code}.")


def performance_change_percent(baseline: float, injected: float) -> float:
    """Calculate performance delta percentage."""

    if baseline <= 0:
        raise EvaluationError("The baseline median duration must be greater than 0.")
    return ((injected - baseline) / baseline) * 100.0


def log_summary(baseline: PhaseResult, injected: PhaseResult, change_percent: float) -> None:
    """Write concise summary logs."""

    logging.info("Baseline median duration: %.6f seconds", baseline.median_seconds)
    logging.info("Injected median duration: %.6f seconds", injected.median_seconds)
    logging.info("Performance change percentage: %.2f%%", change_percent)
    logging.info("Success threshold: %.2f%%", SUCCESS_THRESHOLD_PERCENT)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Program entry point."""

    configure_logging()
    try:
        global DEFAULT_RUNS, SUCCESS_THRESHOLD_PERCENT
        args = parse_args(argv)
        # Clamp to at least 1 so the workload always runs at least once even if a
        # non-positive value slips through; 1 keeps the safe single measured-run
        # default (always preceded by one warm-up pass, regardless of the count).
        DEFAULT_RUNS = max(1, int(args.median_runs))
        # Override the global threshold when provided on the command line.
        SUCCESS_THRESHOLD_PERCENT = args.success_threshold_percent
        if args.timeout <= 0:
            raise EvaluationError("--timeout must be a positive number.")

        verified_path = validate_file(
            args.verified_workload_path,
            ".sql",
            "Verification workload file",
        )
        initialization_path = validate_file(
            args.initialization_workload_path,
            ".sql",
            "Initialization workload file",
        )
        script_path = validate_file(args.script_path, ".sh", "Anomaly injection script")
        deadline = time.monotonic() + args.timeout
        in_container = detect_container()

        logging.info("Container environment detected: %s", "yes" if in_container else "no")
        logging.info("Verification workload: %s", verified_path)
        logging.info("Initialization workload: %s", initialization_path)
        logging.info("Anomaly injection script: %s", script_path)
        logging.info(
            "Database connection: host=%s port=%s user=%s database=%s schema=%s",
            args.host,
            args.port,
            args.user,
            args.database,
            args.schema,
        )

        client_path = ensure_runtime(deadline, in_container, args.dbms)
        db_config = DatabaseConfig(
            host=args.host,
            port=args.port,
            user=args.user,
            database=args.database,
            schema=args.schema,
            password=args.password,
        )
        db_env = build_db_env(db_config, args.dbms)

        # Warm-up primes caches/plans (buffer pool / OS page cache) and absorbs
        # the cold-start cost of the very first execution, which would otherwise
        # inflate the measured latency. It is ALWAYS performed -- including in the
        # single measured-run mode (median_runs == 1) -- so that the measured
        # run(s) never pay the cold-start penalty. The initialization workload
        # alone only restores the data-access state; it does not guarantee caches
        # are warm, so a dedicated warm-up pass is still required for accurate
        # latency. The warm-up is an EXTRA execution whose timing is discarded.
        logging.info("========== Warmup Verification: START ==========")
        run_warmup_workload(
            initialization_path,
            verified_path,
            client_path,
            db_config,
            args.dbms,
            db_env,
            deadline,
        )
        logging.info("========== Warmup Verification: END ==========")

        logging.info("========== Baseline Verification: START ==========")
        baseline = run_measured_workload_phase(
            "baseline",
            initialization_path,
            verified_path,
            client_path,
            db_config,
            args.dbms,
            db_env,
            deadline,
        )
        logging.info("========== Baseline Verification: END ==========")

        run_injection_script(script_path, db_env, deadline)

        logging.info("========== Injected Verification: START ==========")
        try:
            injected = run_measured_workload_phase(
                "injected",
                initialization_path,
                verified_path,
                client_path,
                db_config,
                args.dbms,
                db_env,
                deadline,
                raise_on_error=False,
            )
        except EvaluationError as exc:
            # Case 1: the injection script already finished successfully, but the
            # verification workload was blocked by the injected anomaly until it
            # timed out. A blocked workload after a successful injection is itself
            # strong evidence that the anomaly took effect, so report success
            # rather than a flow failure. This avoids misleading the downstream
            # LLM into thinking the timeout threshold was merely too small.
            if is_timeout_error(exc):
                logging.info("========== Injected Verification: END ==========")
                logging.info(
                    "Verified workload was blocked until timeout after injection (%s).",
                    exc,
                )
                logging.info(
                    "Anomaly successfully injected: the injected anomaly blocked the "
                    "verification workload until it timed out. This is treated as a "
                    "successful injection, not an insufficient timeout threshold."
                )
                logging.info("Anomaly successfully simulated and verified.")
                return 0
            # Any non-timeout error keeps the original failure behaviour.
            raise
        logging.info("========== Injected Verification: END ==========")

        change_percent = performance_change_percent(
            baseline.median_seconds, injected.median_seconds
        )
        log_summary(baseline, injected, change_percent)

        # New judgement: a verified-workload error after injection also indicates success.
        injected_has_error = any(item.has_error for item in injected.runs)
        if injected_has_error:
            logging.info("Verified workload failed after injection, indicating an anomaly.")

        # Success if either the performance change exceeds the threshold or an error occurred.
        if change_percent >= SUCCESS_THRESHOLD_PERCENT or injected_has_error:
            logging.info("Anomaly successfully simulated and verified.")
            return 0

        logging.error("Performance change did not reach the success threshold.")
        logging.error("Anomaly simulation or verification failed.")
        return 1

    except EvaluationError as exc:
        logging.error("Evaluation flow failed: %s", exc)
        logging.error("Anomaly simulation or verification failed.")
        return 1
    except KeyboardInterrupt:
        logging.error("Execution was interrupted by the user.")
        logging.error("Anomaly simulation or verification failed.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
