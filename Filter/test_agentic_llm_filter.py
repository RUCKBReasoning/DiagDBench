#!/usr/bin/env python3
"""Filter anomaly samples by testing whether an Agentic LLM can recover them.

Use ``--config Filter/Configs/example_agentic.json --dry-run`` after setting the
sample path. Before a real run, fill in the model and API endpoint, and export
the API key through ``FILTER_LLM_API_KEY``; never commit a populated private
configuration. Config/sample paths are project-root relative. Dry-run neither
requires an API key nor starts Docker. Recovered samples go to ``failed_ids``
(filtered out); unrecovered samples go to ``pass_ids`` (retained).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import queue
import re
import shlex
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    class tqdm:  # type: ignore[no-redef]
        """Minimal terminal progress fallback when tqdm is unavailable."""

        def __init__(
            self,
            *,
            total: int,
            initial: int = 0,
            desc: str = "",
            unit: str = "",
            disable: bool = False,
            **_: Any,
        ) -> None:
            self.total = total
            self.n = initial
            self.desc = desc
            self.unit = unit
            self.disable = disable
            self.postfix = ""
            self._render()

        def _render(self) -> None:
            if not self.disable:
                suffix = f" {self.postfix}" if self.postfix else ""
                print(
                    f"{self.desc}: {self.n}/{self.total} {self.unit}{suffix}",
                    file=sys.stderr,
                    flush=True,
                )

        def update(self, amount: int = 1) -> None:
            self.n += amount
            self._render()

        def set_postfix_str(self, value: str, **_: Any) -> None:
            self.postfix = value

        def close(self) -> None:
            self._render()


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINES_DIR = PROJECT_ROOT / "Baselines"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(BASELINES_DIR) not in sys.path:
    sys.path.insert(0, str(BASELINES_DIR))

from Filter import test_filter_syn_data as legacy  # noqa: E402
from Filter import test_filter_syn_data as workload_v1  # noqa: E402
from Baselines.ReAct import react  # noqa: E402
from Scripts import health_report  # noqa: E402


DEFAULT_CONFIG_PATH = (
    PROJECT_ROOT / "Filter" / "Configs" / "example_agentic.json"
)
RESULT_FILENAME = "agentic_fil_result.json"
EVIDENCE_FILENAME = "agentic_filter_evidence.json"
V1_RECHECKED_IDS_FIELD = "agentic_llm_v1_rechecked_ids"
DEFAULT_WORKLOAD_TIMEOUT_SECONDS = 3600
DEFAULT_RECOVERY_TIME_RATIO = 1.05
DEFAULT_MIN_RUNTIME_JITTER_SECONDS = 0.1
DEFAULT_TRANSIENT_WORKLOAD_RETRY_ATTEMPTS = 2
DEFAULT_PRE_RECOVERY_TIMEOUT_SECONDS = 300
DEFAULT_HEALTH_REPORT_TIMEOUT_SECONDS = 30
DEFAULT_MYSQL_HEALTH_USER = "monitor"
DEFAULT_MYSQL_HEALTH_PASSWORD = "monitor"
DOCKER_PREFLIGHT_EXIT_CODE = 3
RUNTIME_START_PATTERN = re.compile(r"^__DB_DIAG_RUNTIME_START__=(\d+)$")
RUNTIME_END_PATTERN = re.compile(r"^__DB_DIAG_RUNTIME_END__=(\d+)$")
OBSERVATION_DONE_MARKER = "__DONE__"
# `create_marked_verified_workload` creates a fresh temporary SQL file for
# each run. PostgreSQL's psql prefixes NOTICE messages with that file path.
# The path is framework metadata, not part of the workload result.
GENERATED_MARKED_WORKLOAD_LOCATION_PATTERN = re.compile(
    r"(?P<client>\bpsql:)(?:[^\s:]+/)?"
    r"\.db_diag_verified_[^/\s:]+\.sql(?P<line>:\d+:)"
)
TRANSIENT_CONNECTION_FAILURE_MARKERS = (
    "can't connect to mysql server",
    "connection refused",
    "could not connect to server",
    "server closed the connection unexpectedly",
    "lost connection to mysql server",
)

RECOVERY_PROMPT_SUFFIX = """

# FILTER-SPECIFIC RECOVERY REQUIREMENTS

This run is an active recovery evaluation. You are explicitly authorized to
make targeted changes to the target database environment, including changing
runtime parameters, terminating the anomaly's sessions or processes, resolving
locks, and applying safe database-level corrective actions. Do not merely
describe these operations: execute them through code actions and verify the
recovery before returning the finish action.

You may optimize the supplied verified workload when useful, but its business
semantics and result set must remain equivalent to the original workload. Do
not remove, change, or reorder functional statements, predicates, rows, or
side effects merely to make the measurement faster. The final evaluation
compares ordered core outputs for every functional statement and runtime
against the original healthy baseline. Only LOCK, UNLOCK, SET, START
TRANSACTION, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE SAVEPOINT, and USE
are treated as auxiliary statements and ignored by that output comparison.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter samples by Agentic LLM diagnosis and recovery."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Workflow configuration (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and sample files without Docker or LLM calls.",
    )
    return parser.parse_args()


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"JSON file does not exist: {path}") from None
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON file {path}: {exc}") from exc


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")
    temporary_path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(path)


class SampleIdentityError(RuntimeError):
    """A missing/invalid/conflicting identity marker must never finalize a case."""


def copy_sample_description(
    sample_directory: Path, output_directory: Path
) -> Path:
    """Copy the complete input JSON before execution, without overwriting identity.

    Publication is atomic and exclusive. Repeated attempts with equivalent JSON
    are harmless; a conflicting marker leaves the sample pending for inspection.
    """
    source = sample_directory / "anomaly_description.json"
    destination = output_directory / "anomaly_description.json"
    temporary_path: Path | None = None
    try:
        content = source.read_bytes()
        source_value = json.dumps(json.loads(content), sort_keys=True, ensure_ascii=False)
        output_directory.mkdir(parents=True, exist_ok=True)
        if destination.is_symlink():
            raise SampleIdentityError(f"Identity marker is a symlink: {destination}")
        if destination.exists():
            if json.dumps(json.loads(destination.read_bytes()), sort_keys=True, ensure_ascii=False) != source_value:
                raise SampleIdentityError(
                    f"Sample identity conflict: {source} != {destination}"
                )
            return destination
        with tempfile.NamedTemporaryFile(
            prefix=".anomaly_description.", suffix=".tmp",
            dir=output_directory, delete=False,
        ) as stream:
            temporary_path = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary_path, source.stat().st_mode & 0o777)
        try:
            os.link(temporary_path, destination)
        except FileExistsError:
            if destination.is_symlink() or json.dumps(json.loads(destination.read_bytes()), sort_keys=True, ensure_ascii=False) != source_value:
                raise SampleIdentityError(
                    f"Concurrent sample identity conflict: {destination}"
                )
        return destination
    except SampleIdentityError:
        raise
    except (OSError, ValueError, UnicodeError) as exc:
        raise SampleIdentityError(
            f"Cannot establish sample identity from {source} to {destination}: {exc}"
        ) from exc
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass


def case_id(case: dict[str, Any]) -> int:
    raw_id = case.get("sample_id", case.get("anomaly_id"))
    if isinstance(raw_id, bool) or not re.fullmatch(r"[0-9]+", str(raw_id).strip()):
        raise ValueError(f"Invalid sample ID: {raw_id!r}")
    return int(raw_id)


def validate_filter_config(config: dict[str, Any]) -> None:
    if not isinstance(config, dict):
        raise ValueError("Configuration root must be a JSON object.")
    required = {
        "key_information_path", "median_runs", "max_turn_num", "llm_configuration",
        "docker_name", "host_port", "parallelism_num", "superuser_config", "output_base_dir",
    }
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError("Missing required config fields: " + ", ".join(missing))

    integer_defaults = {
        "median_runs": None, "max_turn_num": None, "parallelism_num": None,
        "host_port": None, "docker_retry_times": 10, "database_init_timeout": 3600,
        "workload_timeout_seconds": DEFAULT_WORKLOAD_TIMEOUT_SECONDS,
        "transient_workload_retry_attempts": DEFAULT_TRANSIENT_WORKLOAD_RETRY_ATTEMPTS,
        "pre_recovery_timeout_seconds": DEFAULT_PRE_RECOVERY_TIMEOUT_SECONDS,
        "health_report_timeout_seconds": DEFAULT_HEALTH_REPORT_TIMEOUT_SECONDS,
    }
    for field, default in integer_defaults.items():
        value = config.get(field, default)
        if type(value) is not int or value < 1:
            raise ValueError(f"`{field}` must be a positive integer.")
    if config["median_runs"] % 2 == 0:
        raise ValueError("`median_runs` must be a positive odd integer.")
    if config["host_port"] + config["parallelism_num"] - 1 > 65535:
        raise ValueError("The worker host-port range must stay within 1..65535.")
    for field in ("key_information_path", "output_base_dir"):
        if not isinstance(config[field], str) or not config[field].strip():
            raise ValueError(f"`{field}` must be a nonempty path.")
    docker_name = config["docker_name"]
    if not isinstance(docker_name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", docker_name):
        raise ValueError("`docker_name` must be a nonempty Docker-compatible name.")

    llm = config["llm_configuration"]
    if not isinstance(llm, dict):
        raise ValueError("`llm_configuration` must be an object.")
    for field in ("base_url", "api_key", "model_name"):
        if not isinstance(llm.get(field), str):
            raise ValueError(f"`llm_configuration.{field}` must be a string.")
    if type(llm.get("retry_times")) is not int or llm["retry_times"] < 1:
        raise ValueError("`llm_configuration.retry_times` must be a positive integer.")
    temperature = llm.get("temperature")
    if isinstance(temperature, bool) or not isinstance(temperature, (int, float)) or not math.isfinite(temperature):
        raise ValueError("`llm_configuration.temperature` must be a finite number.")
    if "api_key_env" in llm and (not isinstance(llm["api_key_env"], str) or not llm["api_key_env"].strip()):
        raise ValueError("`llm_configuration.api_key_env` must be a nonempty environment-variable name.")
    for field in ("provider", "api_version", "azure_endpoint"):
        if field in llm and not isinstance(llm[field], str):
            raise ValueError(f"`llm_configuration.{field}` must be a string.")
    react.validate_config(config, requires_superuser=True)

    for field in ("finalize_execution_errors", "retry_in_place"):
        if not isinstance(config.get(field, False), bool):
            raise ValueError(f"`{field}` must be a boolean.")
    if any(key.startswith("health_report_") and key != "health_report_timeout_seconds" for key in config):
        health_report.validate_budget(config)
    if config.get("retry_in_place") and not config.get("retry_source_result_path"):
        raise ValueError("retry_in_place requires retry_source_result_path")
    for field in ("retry_source_result_path", "detail_output_base_dir", "v1_workload_result_path"):
        if field in config and (not isinstance(config[field], str) or not config[field].strip()):
            raise ValueError(f"{field} must be a nonempty path")
    for field, default, lower_bound in (
        ("recovery_time_ratio", DEFAULT_RECOVERY_TIME_RATIO, 1),
        ("min_runtime_jitter_seconds", DEFAULT_MIN_RUNTIME_JITTER_SECONDS, 0),
    ):
        value = config.get(field, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < lower_bound:
            raise ValueError(f"`{field}` must be finite and at least {lower_bound}.")
    timeout_overrides = config.get("anomaly_injection_timeout_seconds_by_sample", {})
    if not isinstance(timeout_overrides, dict):
        raise ValueError("`anomaly_injection_timeout_seconds_by_sample` must be an object.")
    for sample_id, timeout_seconds in timeout_overrides.items():
        case_id({"sample_id": sample_id})
        if type(timeout_seconds) is not int or timeout_seconds < 1:
            raise ValueError(f"Anomaly injection timeout must be a positive integer for sample {sample_id!r}.")


def validate_llm_runtime_config(llm_config: dict[str, Any]) -> None:
    """Fail before Docker startup; use the same credential resolver as requests."""
    from llm_api import _resolve_api_key, resolve_llm_provider

    model = llm_config["model_name"].strip()
    endpoint = (llm_config.get("base_url") or llm_config.get("azure_endpoint") or "").strip()
    parsed = urlsplit(endpoint)
    if not model or model.upper().startswith(("REPLACE_", "YOUR_")):
        raise ValueError("Fill in llm_configuration.model_name before running Agentic filtering.")
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.hostname.endswith(".invalid") or "YOUR_" in endpoint.upper()
            or "REPLACE_" in endpoint.upper()):
        raise ValueError("Fill in a valid llm_configuration.base_url before running Agentic filtering.")
    azure = resolve_llm_provider(llm_config) == "azure"
    if azure and not (llm_config.get("api_version") or os.environ.get("OPENAI_API_VERSION")):
        raise ValueError("Azure endpoints require llm_configuration.api_version or OPENAI_API_VERSION.")
    key = _resolve_api_key(llm_config, azure=azure)
    if not isinstance(key, str) or not key.strip():
        raise ValueError("Missing model API key; set the environment variable named by llm_configuration.api_key_env.")


def redacted_config(value: Any) -> Any:
    """Keep runtime snapshots useful without persisting model/DB credentials."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            name = key.lower().replace("-", "_")
            sensitive = (not name.endswith("_env") and any(
                word in name for word in ("api_key", "password", "secret", "token", "authorization", "base_url", "azure_endpoint")
            ))
            if sensitive and isinstance(item, (str, dict, list)) and item:
                result[key] = "<redacted>"
            else:
                result[key] = redacted_config(item)
        return result
    if isinstance(value, list):
        return [redacted_config(item) for item in value]
    return value


class ResultStore:
    """Thread-safe, resumable store.

    `processed_ids` follows the requested schema and contains IDs still awaiting
    processing. A completed ID moves from it to pass_ids or failed_ids.
    """

    def __init__(
        self, path: Path, sample_ids: list[int],
        retry_in_place_ids: set[int] | None = None,
        baseline_pass_ids: set[int] | None = None,
    ) -> None:
        self.path = path
        self._lock = threading.Lock()
        valid_ids = set(sample_ids)
        existing = read_json(path) if path.is_file() else {}
        passed = self._normalize_ids(existing.get("pass_ids", [])) & valid_ids
        passed |= set(baseline_pass_ids or ()) & valid_ids
        failed = self._normalize_ids(existing.get("failed_ids", [])) & valid_ids
        failed -= passed
        completed = passed | failed
        iteration_rounds = self._normalize_iteration_rounds(
            existing.get("agentic_llm_iteration_rounds", {}), valid_ids
        )

        self._passed = passed
        self._failed = failed
        self._pending = valid_ids - completed
        self._iteration_rounds = {
            sample_id: rounds
            for sample_id, rounds in iteration_rounds.items()
            if sample_id in completed
        }
        self._retry_in_place = retry_in_place_ids is not None
        self._v1_rechecked = self._normalize_ids(existing.get(V1_RECHECKED_IDS_FIELD, [])) & valid_ids
        if self._retry_in_place:
            retry_ids = set(retry_in_place_ids or ()) & valid_ids
            self._passed -= retry_ids
            self._failed -= retry_ids
            self._pending |= retry_ids
            for sample_id in retry_ids:
                self._iteration_rounds.pop(sample_id, None)
        self.persist()

    @staticmethod
    def _normalize_ids(values: Any) -> set[int]:
        normalized: set[int] = set()
        if not isinstance(values, list):
            return normalized
        for value in values:
            if isinstance(value, bool):
                continue
            try:
                normalized.add(int(value))
            except (TypeError, ValueError):
                continue
        return normalized

    @staticmethod
    def _normalize_iteration_rounds(
        values: Any, valid_ids: set[int]
    ) -> dict[int, int]:
        normalized: dict[int, int] = {}
        if not isinstance(values, dict):
            return normalized
        for raw_id, raw_rounds in values.items():
            try:
                sample_id = int(raw_id)
                rounds = int(raw_rounds)
            except (TypeError, ValueError):
                continue
            if sample_id in valid_ids and rounds >= 0:
                normalized[sample_id] = rounds
        return normalized

    def _payload(self) -> dict[str, Any]:
        payload = {
            "pass_ids": sorted(self._passed),
            "failed_ids": sorted(self._failed),
            "processed_ids": sorted(self._pending),
            "agentic_llm_iteration_rounds": {
                str(sample_id): self._iteration_rounds[sample_id]
                for sample_id in sorted(self._iteration_rounds)
            },
        }
        if self._retry_in_place or self._v1_rechecked:
            payload[V1_RECHECKED_IDS_FIELD] = sorted(self._v1_rechecked)
        return payload

    def persist(self) -> None:
        with self._lock:
            write_json_atomic(self.path, self._payload())

    def pending_ids(self) -> set[int]:
        with self._lock:
            return set(self._pending)

    def record(
        self,
        sample_id: int,
        recovered: bool,
        agentic_llm_iteration_rounds: int,
    ) -> None:
        if isinstance(agentic_llm_iteration_rounds, bool):
            raise ValueError("Agentic iteration rounds must be an integer.")
        rounds = int(agentic_llm_iteration_rounds)
        if rounds < 0:
            raise ValueError("Agentic iteration rounds must be non-negative.")
        with self._lock:
            self._pending.discard(sample_id)
            if recovered:
                self._failed.add(sample_id)
                self._passed.discard(sample_id)
            else:
                self._passed.add(sample_id)
                self._failed.discard(sample_id)
            self._iteration_rounds[sample_id] = rounds
            if self._retry_in_place:
                self._v1_rechecked.add(sample_id)
            write_json_atomic(self.path, self._payload())

    def counts(self) -> tuple[int, int, int]:
        with self._lock:
            return len(self._passed), len(self._failed), len(self._pending)

    def counts_for(self, sample_ids: set[int]) -> tuple[int, int, int]:
        with self._lock:
            return (len(self._passed & sample_ids), len(self._failed & sample_ids),
                    len(self._pending & sample_ids))


def retry_source_ids(config: dict[str, Any]) -> tuple[set[int], set[int], Path] | None:
    """Select prior failed IDs; preserve the V1 in-place retry checkpoint."""
    raw_path = config.get("retry_source_result_path")
    if raw_path is None:
        return None
    path = react.project_path(raw_path)
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("retry_source_result_path must contain an object")
    parsed = {}
    for field in ("pass_ids", "failed_ids", "processed_ids", V1_RECHECKED_IDS_FIELD):
        values = payload.get(field, [] if field not in ("pass_ids", "failed_ids") else None)
        if not isinstance(values, list):
            raise ValueError(f"Retry source field {field} must be a list")
        parsed[field] = {case_id({"sample_id": value}) for value in values}
    if parsed["pass_ids"] & parsed["failed_ids"]:
        raise ValueError("Retry source has overlapping pass_ids and failed_ids")
    if config.get("retry_in_place", False):
        return ((parsed["failed_ids"] - parsed[V1_RECHECKED_IDS_FIELD]) | parsed["processed_ids"],
                set(), path)
    return parsed["failed_ids"], parsed["pass_ids"], path


def v1_workload_eligible_ids(config: dict[str, Any]) -> tuple[set[int], Path] | None:
    """Return V1 anomaly-pass IDs that also passed workload consistency."""
    raw_path = config.get("v1_workload_result_path")
    if raw_path is None:
        return None

    result_path = react.project_path(str(raw_path))
    payload = read_json(result_path)
    if not isinstance(payload, dict):
        raise ValueError(
            "`v1_workload_result_path` must point to a JSON object: "
            f"{result_path}"
        )

    parsed_sets: list[set[int]] = []
    for field_name in ("passed_sample_ids", "workload_pass_ids"):
        values = payload.get(field_name)
        if not isinstance(values, list):
            raise ValueError(
                f"V1 result {result_path} must contain a list `{field_name}`."
            )
        parsed_ids: set[int] = set()
        for value in values:
            parsed_ids.add(case_id({"sample_id": value}))
        parsed_sets.append(parsed_ids)
    return parsed_sets[0] & parsed_sets[1], result_path


class DockerPreflightError(RuntimeError):
    """Raised when this task cannot safely claim its configured Docker ports."""


class HealthyBaselineFailure(RuntimeError):
    """A healthy baseline cannot support a recovery comparison."""


def assert_agentic_docker_preflight(config: dict[str, Any]) -> None:
    host_port = int(config["host_port"])
    parallelism = int(config["parallelism_num"])
    worker_ports = list(range(host_port, host_port + parallelism))
    docker_name = str(config["docker_name"])

    try:
        container_result = subprocess.run(
            [
                "docker",
                "ps",
                "-a",
                "--filter",
                f"name={docker_name}",
                "--format",
                "{{.Names}}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise DockerPreflightError("docker executable is unavailable") from exc
    if container_result.returncode != 0:
        details = container_result.stderr.strip() or "docker ps failed"
        raise DockerPreflightError(details)
    stale_containers = sorted(
        name for name in container_result.stdout.splitlines() if name
    )

    occupied_ports: list[int] = []
    try:
        for port in worker_ports:
            port_result = subprocess.run(
                ["ss", "-ltnH", f"sport = :{port}"],
                check=False,
                capture_output=True,
                text=True,
            )
            if port_result.returncode != 0:
                raise DockerPreflightError(
                    f"unable to inspect host port {port}: "
                    f"{port_result.stderr.strip() or 'ss failed'}"
                )
            if port_result.stdout.strip():
                occupied_ports.append(port)
    except FileNotFoundError as exc:
        raise DockerPreflightError("ss executable is unavailable") from exc

    if stale_containers or occupied_ports:
        details: list[str] = []
        if stale_containers:
            details.append(
                "existing task containers=" + ",".join(stale_containers)
            )
        if occupied_ports:
            details.append(
                "occupied worker ports=" + ",".join(map(str, occupied_ports))
            )
        raise DockerPreflightError(
            "Agentic Docker preflight refused to start: " + "; ".join(details)
        )


def sample_context(case: dict[str, Any], key_information_path: Path) -> dict[str, Any]:
    sample_id_text, sample_directory = react.find_sample_directory(
        key_information_path.parent, case
    )
    files = react.required_files(sample_directory)
    sample_config = react.load_json_file(files["config"])

    dbms = sample_config.get(
        "dbms", case.get("dbms", "PostgreSQL 16.11")
    )
    database_name = str(
        sample_config.get("database_name", case["database_name"])
    )
    schema_name = str(
        sample_config.get(
            "database_schema_name",
            case.get("database_schema_name", "diagnosis"),
        )
    )
    if react.normalize_dbms(dbms) == "mysql":
        schema_name = database_name

    user_config = dict(react.DEFAULT_DB_USER)
    user_config.update(sample_config.get("system_user_config") or {})
    user_config["DB_NAME"] = database_name
    if react.normalize_dbms(dbms) == "mysql":
        # Agentic execution is inside the MySQL container, whose daemon listens
        # on its fixed loopback TCP endpoint. The generic project default is
        # PostgreSQL's 5432 and can appear to work only because mysql falls back
        # to a Unix socket for `localhost`; anomaly scripts may force TCP.
        user_config["DB_HOST"] = "127.0.0.1"
        user_config["DB_PORT"] = "3306"
    hardware = dict(react.DEFAULT_HARDWARE)
    hardware.update(sample_config.get("system_hardware_config") or {})

    return {
        "sample_id": int(sample_id_text),
        "sample_directory": sample_directory,
        "files": files,
        "sample_config": sample_config,
        "dbms": dbms,
        "database_name": database_name,
        "schema_name": schema_name,
        "user_config": user_config,
        "hardware": hardware,
    }


def initialize_sample_container(
    context: dict[str, Any],
    config: dict[str, Any],
    output_directory: Path,
    worker_docker_name: str,
    worker_host_port: int,
    logger: logging.Logger,
) -> tuple[str, dict[str, Any]]:
    user_config = context["user_config"]
    container_config = react.build_container_config(
        context["dbms"],
        context["hardware"],
        worker_host_port,
        user_config,
    )
    retries = int(config.get("docker_retry_times", 10))
    init_timeout = int(
        config.get(
            "database_init_timeout",
            react.DEFAULT_TIMEOUTS.get(context["database_name"], 3600),
        )
    )

    container_name: str | None = None
    for attempt in range(1, retries + 1):
        if container_name is None:
            container_name = react.make_unique_container_name(worker_docker_name)
            initialized = react.docker_container_init(
                container_name, container_config
            )
        else:
            initialized, container_name = react.recreate_container_with_new_name(
                container_name, worker_docker_name, container_config
            )
        if initialized:
            break
        if attempt == retries:
            raise RuntimeError("Unable to initialize Docker container.")
        time.sleep(5)
    assert container_name is not None

    db_type = context["sample_config"].get(
        "db_type",
        "load_data" if react.normalize_dbms(context["dbms"]) == "mysql" else "dump",
    )
    init_db_config = {
        "base_dir": str(PROJECT_ROOT),
        "dbms": context["dbms"],
        "db_host": user_config["DB_HOST"],
        "db_port": user_config["DB_PORT"],
        "db_name": context["database_name"],
        "db_schema": context["schema_name"],
        "db_type": db_type,
        "db_user": user_config["DB_USER"],
        "password": user_config["DB_PASSWORD"],
        "timeout": init_timeout,
        "output_dir": str(output_directory),
    }
    for attempt in range(1, retries + 1):
        if react.docker_container_init_db(
            container_name, init_db_config, session_name=container_name
        ):
            break
        if attempt == retries:
            raise RuntimeError(
                f"Unable to initialize database {context['database_name']}."
            )
        time.sleep(5)
        _, container_name = react.recreate_container_with_new_name(
            container_name, worker_docker_name, container_config
        )

    modification_log = output_directory / "data_modification_log.txt"
    modification_command = react.build_sql_file_command(
        context["dbms"],
        user_config,
        str(context["files"]["modification"]),
        context["database_name"],
    )
    modification_ok, details = react.run_docker_exec_observation(
        container_name,
        modification_command,
        str(modification_log),
        timeout=600,
        session_name=container_name,
    )
    if not modification_ok:
        raise RuntimeError(
            f"Data modification failed; see {modification_log}. {details}"
        )
    logger.info(
        "[Sample %d] Container and database initialized.",
        context["sample_id"],
    )
    return container_name, container_config


def statement_role(statement: str) -> str:
    """Classify only explicitly allowed setup/control statements as auxiliary."""
    remaining = statement.lstrip()
    while remaining:
        if remaining.startswith("--"):
            newline = remaining.find("\n")
            remaining = "" if newline == -1 else remaining[newline + 1 :].lstrip()
            continue
        if remaining.startswith("/*"):
            comment_end = remaining.find("*/", 2)
            if comment_end == -1:
                break
            remaining = remaining[comment_end + 2 :].lstrip()
            continue
        break

    if not remaining:
        return "auxiliary:comment"

    normalized = remaining.upper()
    auxiliary_patterns = (
        (r"^LOCK\b", "lock"),
        (r"^UNLOCK\b", "unlock"),
        (r"^SET\b", "set"),
        (r"^START\s+TRANSACTION\b", "start_transaction"),
        (r"^BEGIN\b", "begin"),
        (r"^COMMIT\b", "commit"),
        (r"^ROLLBACK\b", "rollback"),
        (r"^SAVEPOINT\b", "savepoint"),
        (r"^RELEASE\s+SAVEPOINT\b", "release_savepoint"),
        (r"^USE\b", "use"),
    )
    for pattern, label in auxiliary_patterns:
        if re.match(pattern, normalized):
            return f"auxiliary:{label}"
    return "functional"


def workload_statement_roles(verified_path: Path) -> list[str]:
    statements = workload_v1.split_sql_statements(
        verified_path.read_text(encoding="utf-8")
    )
    return [statement_role(statement) for statement in statements]


def normalized_output_line(line: str) -> str:
    return GENERATED_MARKED_WORKLOAD_LOCATION_PATTERN.sub(
        r"\g<client><generated-marked-workload>\g<line>",
        line.strip(),
    )


def normalized_output_set(statement: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        sorted(
            normalized_output_line(line)
            for line in statement.get("output_set", [])
            if isinstance(line, str) and line.strip()
        )
    )


def functional_contract(
    observation: dict[str, Any], statement_roles: list[str]
) -> dict[str, Any] | None:
    if (
        not observation.get("command_ok")
        or observation.get("exit_code") not in (0, None)
        or not observation.get("completed")
        or not observation.get("marker_parse_complete")
    ):
        return None

    results_by_index: dict[int, dict[str, Any]] = {}
    for statement in observation.get("statement_results") or []:
        try:
            statement_index = int(statement["statement_index"])
        except (KeyError, TypeError, ValueError):
            return None
        if statement_index in results_by_index:
            return None
        results_by_index[statement_index] = statement

    expected_indexes = set(range(1, len(statement_roles) + 1))
    if set(results_by_index) != expected_indexes:
        return None

    roles = [
        {"statement_index": index, "role": role}
        for index, role in enumerate(statement_roles, start=1)
    ]
    functional_indexes = [
        index
        for index, role in enumerate(statement_roles, start=1)
        if role == "functional"
    ]
    auxiliary_indexes = [
        index
        for index, role in enumerate(statement_roles, start=1)
        if role != "functional"
    ]
    functional_output_signature = tuple(
        normalized_output_set(results_by_index[index])
        for index in functional_indexes
    )
    return {
        "statement_roles": roles,
        "source_statement_count": len(statement_roles),
        "functional_statement_indexes": functional_indexes,
        "auxiliary_statement_indexes": auxiliary_indexes,
        "functional_statement_count": len(functional_indexes),
        "functional_output_signature": functional_output_signature,
    }


def comparison_signature(
    observation: dict[str, Any], statement_roles: list[str]
) -> Any | None:
    contract = functional_contract(observation, statement_roles)
    if contract is None:
        return None
    return contract["functional_output_signature"]


def contract_evidence(contract: dict[str, Any] | None) -> dict[str, Any] | None:
    if contract is None:
        return None
    return {
        "statement_roles": contract["statement_roles"],
        "source_statement_count": contract["source_statement_count"],
        "functional_statement_count": contract["functional_statement_count"],
        "functional_statement_indexes": contract["functional_statement_indexes"],
        "auxiliary_statement_indexes": contract["auxiliary_statement_indexes"],
        "functional_output_signature": [
            list(output_set)
            for output_set in contract["functional_output_signature"]
        ],
    }


def compare_functional_contracts(
    baseline_contract: dict[str, Any] | None,
    recovered_contract: dict[str, Any] | None,
) -> dict[str, Any]:
    evidence = {
        "baseline": contract_evidence(baseline_contract),
        "post_recovery": contract_evidence(recovered_contract),
        "changed_functional_statement_indexes": [],
        "missing_baseline_functional_indexes": [],
        "unexpected_post_recovery_functional_indexes": [],
    }
    if baseline_contract is None or recovered_contract is None:
        evidence["equivalent"] = False
        return evidence

    baseline_indexes = baseline_contract["functional_statement_indexes"]
    recovered_indexes = recovered_contract["functional_statement_indexes"]
    baseline_signature = baseline_contract["functional_output_signature"]
    recovered_signature = recovered_contract["functional_output_signature"]
    common_count = min(len(baseline_signature), len(recovered_signature))

    for position in range(common_count):
        if baseline_signature[position] != recovered_signature[position]:
            evidence["changed_functional_statement_indexes"].append(
                {
                    "functional_position": position + 1,
                    "baseline_statement_index": baseline_indexes[position],
                    "post_recovery_statement_index": recovered_indexes[position],
                }
            )

    evidence["missing_baseline_functional_indexes"] = baseline_indexes[common_count:]
    evidence["unexpected_post_recovery_functional_indexes"] = recovered_indexes[
        common_count:
    ]
    evidence["equivalent"] = not (
        evidence["changed_functional_statement_indexes"]
        or evidence["missing_baseline_functional_indexes"]
        or evidence["unexpected_post_recovery_functional_indexes"]
    )
    return evidence


def timed_command(command: list[str]) -> list[str]:
    """Wrap a command with in-container timestamps around its actual runtime."""
    inner_command = shlex.join(command)
    script = (
        "start=$(date +%s%N); "
        'printf "__DB_DIAG_RUNTIME_START__=%s\\n" "$start"; '
        f"{inner_command}; "
        "rc=$?; end=$(date +%s%N); "
        'printf "\\n__DB_DIAG_RUNTIME_END__=%s\\n" "$end"; '
        "exit $rc"
    )
    return ["bash", "-lc", script]


def measured_duration(log_path: Path) -> float:
    start_ns = None
    end_ns = None
    for line in log_path.read_text(
        encoding="utf-8", errors="replace"
    ).splitlines():
        start_match = RUNTIME_START_PATTERN.match(line.strip())
        end_match = RUNTIME_END_PATTERN.match(line.strip())
        if start_match:
            start_ns = int(start_match.group(1))
        if end_match:
            end_ns = int(end_match.group(1))
    if start_ns is None or end_ns is None or end_ns < start_ns:
        raise RuntimeError(f"Missing valid runtime markers in {log_path}.")
    return (end_ns - start_ns) / 1_000_000_000


class RetryableSampleExecutionError(RuntimeError):
    """A transient infrastructure failure requires a new sample container."""


class AnomalyInjectionFailure(RuntimeError):
    """An anomaly that cannot be injected is terminal for filtering."""


class IncompleteWorkloadObservationError(RetryableSampleExecutionError):
    """A tmux observation did not produce the log needed for evaluation."""


def observation_log_status(log_path: Path) -> tuple[bool, bool, int]:
    """Return existence, completion marker, and byte count for an observation."""
    try:
        contents = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False, False, 0
    return True, OBSERVATION_DONE_MARKER in contents.splitlines(), len(
        contents.encode("utf-8")
    )


def captured_observation_exit_code(log_path: Path) -> int | None:
    try:
        return int(
            log_path.with_name(f"{log_path.name}.exit").read_text(
                encoding="ascii"
            ).strip()
        )
    except (OSError, ValueError):
        return None


def run_agentic_observation(
    container_name: str,
    command: list[str],
    log_path: Path,
    timeout_seconds: int,
) -> tuple[bool, str]:
    success, details = react.run_docker_exec_observation(
        container_name,
        command,
        str(log_path),
        timeout=timeout_seconds,
        session_name=container_name,
    )
    if success or "codec can't decode byte" not in details:
        return success, details

    _, completed, _ = observation_log_status(log_path)
    if completed and captured_observation_exit_code(log_path) == 0:
        return True, (
            f"{details.rstrip()}\n"
            "[Agentic filter] Accepted completed command with non-UTF-8 output."
        )
    return success, details


def persist_incomplete_phase(
    phase_directory: Path,
    phase_name: str,
    run_number: int,
    stage: str,
    log_path: Path,
    command_ok: bool,
    details: str,
    cleanup_error: str = "",
) -> None:
    log_exists, log_completed, log_bytes = observation_log_status(log_path)
    write_json_atomic(
        phase_directory / "phase_failure.json",
        {
            "phase": phase_name,
            "status": "incomplete_workload_observation",
            "run_number": run_number,
            "stage": stage,
            "log_path": str(log_path),
            "log_exists": log_exists,
            "log_completed": log_completed,
            "log_bytes": log_bytes,
            "command_ok": bool(command_ok),
            "execution_details": details,
            "cleanup_error": cleanup_error,
        },
    )


def abort_incomplete_observation(container_name: str) -> str:
    """Stop a timed-out tmux command before retrying the sample in a new container."""
    cleanup_errors: list[str] = []
    for operation in (
        react.interrupt_tmux_window,
        react.reset_tmux_window,
    ):
        try:
            operation(container_name, "observation")
        except Exception as exc:  # noqa: BLE001
            cleanup_errors.append(f"{operation.__name__}: {exc}")
    return "; ".join(cleanup_errors)


def incomplete_phase_result(
    phase_name: str,
    verified_path: Path,
    run_number: int,
    stage: str,
    timeout_seconds: int,
    initialization_ok: bool,
    command_ok: bool,
) -> dict[str, Any]:
    """Represent an expected pre-recovery timeout without evaluating a partial log."""
    result = {
        "phase": phase_name,
        "verified_workload_path": str(verified_path),
        "median_seconds": float(timeout_seconds),
        "median_run": {
            "run_number": run_number,
            "duration_seconds": float(timeout_seconds),
            "initialization_ok": bool(initialization_ok),
            "command_ok": bool(command_ok),
        },
        "runs": [
            {
                "run_number": run_number,
                "duration_seconds": float(timeout_seconds),
                "initialization_ok": bool(initialization_ok),
                "command_ok": bool(command_ok),
            }
        ],
        "all_runs_successful": False,
        "results_consistent": False,
        "incomplete_workload_observation": True,
        "incomplete_stage": stage,
    }
    return result


def handle_incomplete_phase(
    container_name: str,
    phase_directory: Path,
    phase_name: str,
    verified_path: Path,
    run_number: int,
    stage: str,
    log_path: Path,
    command_ok: bool,
    details: str,
    timeout_seconds: int,
    initialization_ok: bool,
    allow_incomplete: bool,
) -> dict[str, Any] | None:
    cleanup_error = abort_incomplete_observation(container_name)
    persist_incomplete_phase(
        phase_directory,
        phase_name,
        run_number,
        stage,
        log_path,
        command_ok,
        details,
        cleanup_error,
    )
    if allow_incomplete:
        result = incomplete_phase_result(
            phase_name,
            verified_path,
            run_number,
            stage,
            timeout_seconds,
            initialization_ok,
            command_ok,
        )
        write_json_atomic(phase_directory / "phase_result.json", result)
        return result
    raise IncompleteWorkloadObservationError(
        "%s run %d did not complete %s"
        % (phase_name, run_number, log_path)
    )


def pre_recovery_timeout_seconds(
    config: dict[str, Any],
    healthy_baseline: dict[str, Any],
    workload_timeout_seconds: int,
) -> int:
    configured_timeout = config.get("pre_recovery_timeout_seconds")
    if configured_timeout is not None:
        return int(configured_timeout)
    baseline_seconds = float(healthy_baseline["median_seconds"])
    return min(
        workload_timeout_seconds,
        max(
            DEFAULT_PRE_RECOVERY_TIMEOUT_SECONDS,
            math.ceil(baseline_seconds * 2),
        ),
    )


def run_workload_phase(
    container_name: str,
    context: dict[str, Any],
    phase_name: str,
    output_directory: Path,
    median_runs: int,
    timeout_seconds: int,
    verified_workload_path: Path | None = None,
    allow_incomplete: bool = False,
) -> dict[str, Any]:
    phase_directory = output_directory / phase_name
    phase_directory.mkdir(parents=True, exist_ok=True)
    (phase_directory / "phase_failure.json").unlink(missing_ok=True)
    verified_path = verified_workload_path or context["files"]["verified"]
    statement_roles = workload_statement_roles(verified_path)
    runs: list[dict[str, Any]] = []

    for run_number in range(1, median_runs + 1):
        run_directory = phase_directory / f"run_{run_number}"
        run_directory.mkdir(parents=True, exist_ok=True)

        initialization_log = run_directory / "initialization_workload.log"
        initialization_command = react.build_sql_file_command(
            context["dbms"],
            context["user_config"],
            str(context["files"]["initialization"]),
            context["database_name"],
        )
        initialization_ok, initialization_details = run_agentic_observation(
            container_name,
            initialization_command,
            initialization_log,
            timeout_seconds,
        )
        _, initialization_completed, _ = observation_log_status(
            initialization_log
        )
        if not initialization_completed:
            incomplete_result = handle_incomplete_phase(
                container_name,
                phase_directory,
                phase_name,
                verified_path,
                run_number,
                "initialization_workload",
                initialization_log,
                initialization_ok,
                initialization_details,
                timeout_seconds,
                initialization_ok,
                allow_incomplete,
            )
            if incomplete_result is not None:
                return incomplete_result

        marked_path, marker_prefix, statement_count = (
            workload_v1.create_marked_verified_workload(
                context["dbms"], str(verified_path)
            )
        )
        verified_log = run_directory / "verified_workload.log"
        started_at = time.perf_counter()
        command_ok = False
        details = "Verified workload command did not return."
        try:
            verified_command = react.build_sql_file_command(
                context["dbms"],
                context["user_config"],
                marked_path,
                context["database_name"],
            )
            command_ok, details = run_agentic_observation(
                container_name,
                timed_command(verified_command),
                verified_log,
                timeout_seconds,
            )
        finally:
            host_duration_seconds = time.perf_counter() - started_at
            Path(marked_path).unlink(missing_ok=True)

        _, verified_completed, _ = observation_log_status(verified_log)
        if not verified_completed:
            incomplete_result = handle_incomplete_phase(
                container_name,
                phase_directory,
                phase_name,
                verified_path,
                run_number,
                "verified_workload",
                verified_log,
                command_ok,
                details,
                timeout_seconds,
                initialization_ok,
                allow_incomplete,
            )
            if incomplete_result is not None:
                return incomplete_result

        try:
            duration_seconds = measured_duration(verified_log)
        except (OSError, RuntimeError):
            # A timed-out or forcibly interrupted query has no end marker.
            duration_seconds = host_duration_seconds
        observation = legacy.workload_log_observation(str(verified_log))
        observation["command_ok"] = bool(command_ok)
        observation["execution_details"] = details
        workload_v1.parse_marked_verified_observation(
            observation, marker_prefix, statement_count
        )
        contract = functional_contract(observation, statement_roles)
        runs.append(
            {
                "run_number": run_number,
                "duration_seconds": duration_seconds,
                "initialization_ok": bool(initialization_ok),
                "initialization_details": initialization_details,
                "observation": observation,
                "functional_contract": contract,
                "signature": comparison_signature(observation, statement_roles),
            }
        )

    median_run = sorted(
        runs, key=lambda item: (item["duration_seconds"], item["run_number"])
    )[len(runs) // 2]
    signatures = [item["signature"] for item in runs]
    successful = all(signature is not None for signature in signatures)
    consistent = successful and all(
        signature == signatures[0] for signature in signatures[1:]
    )
    result = {
        "phase": phase_name,
        "verified_workload_path": str(verified_path),
        "median_seconds": statistics.median(
            item["duration_seconds"] for item in runs
        ),
        "median_run": median_run,
        "runs": [
            {
                "run_number": item["run_number"],
                "duration_seconds": item["duration_seconds"],
                "initialization_ok": item["initialization_ok"],
                "command_ok": item["observation"]["command_ok"],
            }
            for item in runs
        ],
        "all_runs_successful": successful,
        "results_consistent": consistent,
    }
    write_json_atomic(phase_directory / "phase_result.json", result)
    return result


def injection_is_transient_failure(
    injection_log: Path, details: str
) -> bool:
    log_exists, log_completed, _ = observation_log_status(injection_log)
    if not log_exists or not log_completed:
        return True
    try:
        log_text = injection_log.read_text(
            encoding="utf-8", errors="replace"
        )
    except OSError:
        return True
    failure_text = f"{details}\n{log_text}".lower()
    return any(
        marker in failure_text
        for marker in TRANSIENT_CONNECTION_FAILURE_MARKERS
    )


def persist_injection_failure(
    output_directory: Path,
    injection_log: Path,
    details: str,
    transient: bool,
) -> None:
    log_exists, log_completed, log_bytes = observation_log_status(injection_log)
    write_json_atomic(
        output_directory / "anomaly_injection_failure.json",
        {
            "status": "anomaly_injection_failed",
            "transient": transient,
            "log_path": str(injection_log),
            "log_exists": log_exists,
            "log_completed": log_completed,
            "log_bytes": log_bytes,
            "execution_details": details,
        },
    )


def run_anomaly_injection(
    container_name: str,
    context: dict[str, Any],
    config: dict[str, Any],
    output_directory: Path,
) -> None:
    output_directory.mkdir(parents=True, exist_ok=True)
    user = context["user_config"]
    environment = {
        "DB_HOST": user["DB_HOST"],
        "DB_PORT": user["DB_PORT"],
        "DB_USER": user["DB_USER"],
        "DB_PASSWORD": user["DB_PASSWORD"],
        "DB_NAME": context["database_name"],
        "DB_SCHEMA": context["schema_name"],
        "PGHOST": user["DB_HOST"],
        "PGPORT": user["DB_PORT"],
        "PGUSER": user["DB_USER"],
        "PGPASSWORD": user["DB_PASSWORD"],
        "PGDATABASE": context["database_name"],
        "MYSQL_HOST": user["DB_HOST"],
        "MYSQL_PORT": user["DB_PORT"],
        "MYSQL_USER": user["DB_USER"],
        "MYSQL_PWD": user["DB_PASSWORD"],
        "MYSQL_DATABASE": context["database_name"],
    }
    command = ["env"]
    command.extend(f"{key}={value}" for key, value in environment.items())
    command.extend(["bash", str(context["files"]["inject"])])
    timeout = (
        react.timeout_from_script(
            react.read_text(context["files"]["inject"]), "Anomaly injection"
        )
        + 60
    )
    timeout_overrides = config.get(
        "anomaly_injection_timeout_seconds_by_sample", {}
    )
    if timeout_overrides:
        timeout = max(
            timeout,
            int(timeout_overrides.get(str(context["sample_id"]), 0)),
        )
    injection_log = output_directory / "anomaly_injection.log"
    (output_directory / "anomaly_injection_failure.json").unlink(
        missing_ok=True
    )
    success, details = run_agentic_observation(
        container_name,
        command,
        injection_log,
        timeout,
    )
    if not success:
        transient = injection_is_transient_failure(injection_log, details)
        cleanup_error = ""
        if transient:
            cleanup_error = abort_incomplete_observation(container_name)
        persist_injection_failure(
            output_directory,
            injection_log,
            details + (
                f"\nObservation cleanup error: {cleanup_error}"
                if cleanup_error
                else ""
            ),
            transient,
        )
        message = (
            f"Anomaly injection failed; see {injection_log}. {details}"
        )
        raise AnomalyInjectionFailure(message)


def capture_health_report(
    stage_name: str,
    context: dict[str, Any],
    config: dict[str, Any],
    output_directory: Path,
    container_name: str,
    worker_host_port: int,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Collect a non-fatal current-state health report."""
    dbms = health_report.normalize_dbms(context["dbms"])
    user_config = context["user_config"]
    health_user = user_config["DB_USER"]
    health_password = user_config["DB_PASSWORD"]
    if dbms == "mysql":
        # The project health collector needs the dedicated read-only monitor
        # account to inspect server-level MySQL state.
        health_user = str(
            config.get("mysql_health_user", DEFAULT_MYSQL_HEALTH_USER)
        )
        health_password = str(
            config.get("mysql_health_password", DEFAULT_MYSQL_HEALTH_PASSWORD)
        )

    report_path = output_directory / f"{stage_name}_health_report.json"
    db_config = {
        "dbms": dbms,
        "host": "127.0.0.1",
        "port": worker_host_port,
        "user": health_user,
        "password": health_password,
        "database": context["database_name"],
        "schema": context["schema_name"],
    }
    if dbms == "postgresql":
        started = time.monotonic()
        logger.info("[Sample %d] Health collection started: stage=%s strategy=bounded_pg_v1",
                    context["sample_id"], stage_name)
        report = health_report.collect_bounded(db_config, config, container_name)
        write_json_atomic(report_path, report)
        logger.info("[Sample %d] Health collection finished: stage=%s status=%s elapsed=%.3fs",
                    context["sample_id"], stage_name, report["collection_status"],
                    time.monotonic() - started)
        return {"path": str(report_path), "collection_status": report["collection_status"],
                "healthy": report["healthy"]}
    try:
        report = health_report.judge_database_health(
            db_config,
            logger=logger,
            connect_timeout=int(
                config.get(
                    "health_report_timeout_seconds",
                    DEFAULT_HEALTH_REPORT_TIMEOUT_SECONDS,
                )
            ),
            container_name=container_name,
        )
        health_report.write_database_state_report(report, str(report_path))
        return {
            "path": str(report_path),
            "collection_status": report.get("collection_status", "completed"),
            "healthy": report.get("healthy"),
        }
    except Exception as exc:  # noqa: BLE001
        failure_report = {
            "stage": stage_name,
            "collection_status": "collection_failed",
            "healthy": None,
            "error": str(exc),
        }
        write_json_atomic(report_path, failure_report)
        logger.warning(
            "[Sample %d] %s health report failed but filtering will continue: %s",
            context["sample_id"],
            stage_name,
            exc,
        )
        return {
            "path": str(report_path),
            "collection_status": "collection_failed",
            "healthy": None,
        }


def run_agentic_recovery(
    container_name: str,
    context: dict[str, Any],
    config: dict[str, Any],
    output_directory: Path,
    logger: logging.Logger,
) -> tuple[bool, Path, int]:
    agentic_directory = output_directory / "agentic_llm"
    for directory_name in ("actions", "logs", "reasoning_process"):
        (agentic_directory / directory_name).mkdir(parents=True, exist_ok=True)

    prompt = react.build_prompt(
        True,
        int(config["max_turn_num"]),
        context["hardware"],
        context["user_config"],
        config["superuser_config"],
        context["files"],
    )
    messages = [prompt + RECOVERY_PROMPT_SUFFIX]
    llm_config = dict(config["llm_configuration"])
    max_turn_num = int(config["max_turn_num"])
    finished = False
    iteration_rounds = 0
    optimized_path = agentic_directory / "optimized_verified_workload.sql"

    for turn in range(max_turn_num):
        action = None
        responses = []
        response = ""
        for retry in range(int(llm_config["retry_times"])):
            iteration_rounds = turn + 1
            response = react.request_llm(
                messages,
                llm_config,
                logger=logger,
                tag=f"[Sample {context['sample_id']}][Agentic][Turn {turn + 1}]",
            )
            responses.append(
                f"\n=== Turn {turn + 1}, retry {retry + 1} ===\n{response}\n"
            )
            (
                agentic_directory
                / "reasoning_process"
                / f"{turn}_reasoning_messages.txt"
            ).write_text("".join(responses), encoding="utf-8")
            action, reason = react.parse_action(response)
            if action is not None:
                break
            logger.warning(
                "[Sample %d] Invalid action at turn %d retry %d: %s",
                context["sample_id"],
                turn + 1,
                retry + 1,
                reason,
            )

        if action is None:
            messages.extend(
                [
                    response,
                    "Return a valid action JSON matching the required schema.",
                ]
            )
            continue

        messages.append(response)
        if action["action"] == "code":
            messages.append(
                react.execute_action(
                    container_name,
                    agentic_directory,
                    turn,
                    action,
                    logger,
                )
            )
            continue

        (agentic_directory / "diagnosis_report.md").write_text(
            action["diagnosis_report.md"], encoding="utf-8"
        )
        (agentic_directory / "solution_script.sh").write_text(
            action["solution_script.sh"], encoding="utf-8"
        )
        optimized_workload = action["verified_workload.sql"].strip()
        if not optimized_workload:
            optimized_workload = react.read_text(context["files"]["verified"])
        optimized_path.write_text(optimized_workload, encoding="utf-8")
        messages.append("Diagnosis and recovery completed.")
        finished = True
        break

    messages_path = agentic_directory / "messages.json"
    write_json_atomic(
        messages_path,
        {
            f"message_{index}": message
            for index, message in enumerate(messages)
        },
    )
    if not finished:
        optimized_path.write_text(
            react.read_text(context["files"]["verified"]), encoding="utf-8"
        )
    return finished, optimized_path, iteration_rounds


def judge_recovery(
    baseline: dict[str, Any],
    recovered: dict[str, Any],
    diagnosis_finished: bool,
    recovery_time_ratio: float = DEFAULT_RECOVERY_TIME_RATIO,
    min_runtime_jitter_seconds: float = DEFAULT_MIN_RUNTIME_JITTER_SECONDS,
) -> dict[str, Any]:
    functional_comparison = compare_functional_contracts(
        baseline["median_run"].get("functional_contract"),
        recovered["median_run"].get("functional_contract"),
    )
    result_equivalent = (
        functional_comparison["equivalent"]
        and recovered["all_runs_successful"]
        and recovered["results_consistent"]
    )
    baseline_seconds = float(baseline["median_seconds"])
    recovered_seconds = float(recovered["median_seconds"])
    allowed_seconds = max(
        baseline_seconds * recovery_time_ratio,
        baseline_seconds + min_runtime_jitter_seconds,
    )
    performance_recovered = recovered_seconds <= allowed_seconds
    recovered_successfully = result_equivalent and performance_recovered
    return {
        "recovered_successfully": recovered_successfully,
        "diagnosis_finished": diagnosis_finished,
        "result_equivalent_to_healthy_baseline": result_equivalent,
        "functional_contract_comparison": functional_comparison,
        "performance_recovered": performance_recovered,
        "healthy_baseline_median_seconds": baseline_seconds,
        "post_recovery_median_seconds": recovered_seconds,
        "allowed_recovery_seconds": allowed_seconds,
        "relative_time_ratio": recovery_time_ratio,
        "minimum_runtime_jitter_seconds": min_runtime_jitter_seconds,
        "classification": (
            "failed_ids (successfully recovered and therefore filtered out)"
            if recovered_successfully
            else "pass_ids (not proven recoverable and therefore retained)"
        ),
    }


def process_case(
    case: dict[str, Any],
    config: dict[str, Any],
    key_information_path: Path,
    output_base: Path,
    worker_docker_name: str,
    worker_host_port: int,
    result_store: ResultStore,
    logger: logging.Logger,
) -> None:
    context = sample_context(case, key_information_path)
    sample_id = context["sample_id"]
    output_directory = output_base / str(sample_id)
    output_directory.mkdir(parents=True, exist_ok=True)
    copy_sample_description(context["sample_directory"], output_directory)
    container_name: str | None = None

    try:
        container_name, _ = initialize_sample_container(
            context,
            config,
            output_directory,
            worker_docker_name,
            worker_host_port,
            logger,
        )
        median_runs = int(config["median_runs"])
        timeout_seconds = int(
            config.get(
                "workload_timeout_seconds",
                DEFAULT_WORKLOAD_TIMEOUT_SECONDS,
            )
        )

        healthy_baseline = run_workload_phase(
            container_name,
            context,
            "healthy_baseline",
            output_directory,
            median_runs,
            timeout_seconds,
        )
        if not (
            healthy_baseline["all_runs_successful"]
            and healthy_baseline["results_consistent"]
        ):
            write_json_atomic(
                output_directory / "healthy_baseline_failure.json",
                {
                    "sample_id": sample_id,
                    "status": "healthy_baseline_invalid",
                    "all_runs_successful": bool(
                        healthy_baseline["all_runs_successful"]
                    ),
                    "results_consistent": bool(
                        healthy_baseline["results_consistent"]
                    ),
                    "phase_result_path": str(
                        output_directory
                        / "healthy_baseline"
                        / "phase_result.json"
                    ),
                },
            )
            raise HealthyBaselineFailure(
                "Healthy baseline workload failed or produced inconsistent results."
            )

        pre_injection_health = capture_health_report(
            "pre_injection",
            context,
            config,
            output_directory,
            container_name,
            worker_host_port,
            logger,
        )
        run_anomaly_injection(container_name, context, config, output_directory)
        post_injection_health = capture_health_report(
            "post_injection",
            context,
            config,
            output_directory,
            container_name,
            worker_host_port,
            logger,
        )
        pre_recovery_timeout = pre_recovery_timeout_seconds(
            config,
            healthy_baseline,
            timeout_seconds,
        )
        pre_recovery = run_workload_phase(
            container_name,
            context,
            "pre_recovery",
            output_directory,
            median_runs,
            pre_recovery_timeout,
            allow_incomplete=True,
        )
        diagnosis_finished, optimized_path, iteration_rounds = run_agentic_recovery(
            container_name,
            context,
            config,
            output_directory,
            logger,
        )
        post_recovery = run_workload_phase(
            container_name,
            context,
            "post_recovery",
            output_directory,
            median_runs,
            timeout_seconds,
            optimized_path,
        )
        judgement = judge_recovery(
            healthy_baseline,
            post_recovery,
            diagnosis_finished,
            float(
                config.get(
                    "recovery_time_ratio",
                    DEFAULT_RECOVERY_TIME_RATIO,
                )
            ),
            float(
                config.get(
                    "min_runtime_jitter_seconds",
                    DEFAULT_MIN_RUNTIME_JITTER_SECONDS,
                )
            ),
        )
        write_json_atomic(
            output_directory / EVIDENCE_FILENAME,
            {
                "sample_id": sample_id,
                "healthy_baseline": healthy_baseline,
                "pre_injection_health_report": pre_injection_health,
                "post_injection_health_report": post_injection_health,
                "pre_recovery": pre_recovery,
                "post_recovery": post_recovery,
                "agentic_llm_iteration_rounds": iteration_rounds,
                "judgement": judgement,
            },
        )
        result_store.record(
            sample_id,
            bool(judgement["recovered_successfully"]),
            iteration_rounds,
        )
        logger.info(
            "[Sample %d] Completed: %s",
            sample_id,
            judgement["classification"],
        )
    finally:
        if container_name is not None:
            react.kill_tmux_session(container_name)
            react.force_remove_container(container_name)


def finalize_execution_error(
    sample_id: int,
    output_base: Path,
    error_type: str,
    attempts: int,
    error: Exception,
    result_store: ResultStore,
) -> None:
    """Retain an unevaluated sample while making its failure terminal."""
    output_directory = output_base / str(sample_id)
    write_json_atomic(
        output_directory / "terminal_execution_failure.json",
        {
            "sample_id": sample_id,
            "status": "execution_error_finalized",
            "error_type": error_type,
            "attempts": attempts,
            "error": str(error),
            "classification": "pass_ids (retained; no second filtering)",
        },
    )
    result_store.record(
        sample_id,
        recovered=False,
        agentic_llm_iteration_rounds=0,
    )


def run_filter(
    config: dict[str, Any],
    cases: list[dict[str, Any]],
    logger: logging.Logger,
) -> int:
    key_information_path = react.project_path(config["key_information_path"])
    result_output_base = react.project_path(config["output_base_dir"])
    output_base = react.project_path(config.get("detail_output_base_dir", config["output_base_dir"]))
    result_output_base.mkdir(parents=True, exist_ok=True)
    output_base.mkdir(parents=True, exist_ok=True)
    write_json_atomic(output_base / "config.json", redacted_config(config))

    sample_by_id = {case_id(case): case for case in cases}
    if len(sample_by_id) != len(cases):
        raise ValueError("key_information.json contains duplicate sample IDs.")
    all_sample_by_id = dict(sample_by_id)

    v1_eligible = v1_workload_eligible_ids(config)
    if v1_eligible is not None:
        eligible_ids, v1_result_path = v1_eligible
        unavailable_ids = eligible_ids - set(sample_by_id)
        if unavailable_ids:
            preview = ",".join(map(str, sorted(unavailable_ids)[:10]))
            raise ValueError(
                "V1 workload-eligible sample IDs are absent from "
                f"key_information.json: {preview}"
            )
        total_cases = len(sample_by_id)
        sample_by_id = {
            sample_id: case
            for sample_id, case in sample_by_id.items()
            if sample_id in eligible_ids
        }
        logger.info(
            "Restricted Agentic input to %d/%d V1 workload-eligible samples "
            "from %s.",
            len(sample_by_id),
            total_cases,
            v1_result_path,
        )

    retry_source = retry_source_ids(config)
    retry_ids = None
    inherited_passed = set()
    retry_in_place = config.get("retry_in_place", False)
    if retry_source is not None:
        retry_ids, inherited_passed, source_path = retry_source
        if retry_ids - set(sample_by_id) or inherited_passed - set(all_sample_by_id):
            raise ValueError("Retry-source IDs are absent from the selected input")
        if retry_in_place and source_path.resolve() != (result_output_base / RESULT_FILENAME).resolve():
            raise ValueError("retry_in_place requires the source and output result paths to match")
        if not retry_in_place and source_path.resolve() == (result_output_base / RESULT_FILENAME).resolve():
            raise ValueError("Use retry_in_place when writing to the source result")
        sample_by_id = {sid: case for sid, case in sample_by_id.items() if sid in retry_ids}
        logger.info("Selected %d samples for retry from %s.", len(sample_by_id), source_path)
    selected_ids = set(sample_by_id)
    result_sample_ids = set(all_sample_by_id) if retry_in_place else selected_ids | inherited_passed
    result_store = ResultStore(
        result_output_base / RESULT_FILENAME, sorted(result_sample_ids),
        retry_in_place_ids=retry_ids if retry_in_place else None,
        baseline_pass_ids=inherited_passed,
    )
    pending_ids = result_store.pending_ids() & selected_ids
    total_samples = len(sample_by_id)
    passed, failed, pending = result_store.counts_for(selected_ids)
    finalized_before_launch = passed + failed
    parallelism = int(config["parallelism_num"])
    slot_queue: queue.Queue[int] = queue.Queue()
    for slot in range(parallelism):
        slot_queue.put(slot)

    def worker(sample_id: int) -> None:
        slot = slot_queue.get()
        worker_prefix = f"{config['docker_name']}_w{slot}"
        try:
            retry_attempts = int(
                config.get(
                    "transient_workload_retry_attempts",
                    DEFAULT_TRANSIENT_WORKLOAD_RETRY_ATTEMPTS,
                )
            )
            for attempt in range(1, retry_attempts + 1):
                try:
                    process_case(
                        sample_by_id[sample_id],
                        config,
                        key_information_path,
                        output_base,
                        worker_prefix,
                        int(config["host_port"]) + slot,
                        result_store,
                        logger,
                    )
                    return
                except SampleIdentityError as exc:
                    logger.error(
                        "[Sample %d] Identity marker validation failed; "
                        "no container/LLM execution, leaving pending: %s",
                        sample_id,
                        exc,
                    )
                    return
                except AnomalyInjectionFailure as exc:
                    result_store.record(
                        sample_id,
                        recovered=True,
                        agentic_llm_iteration_rounds=0,
                    )
                    logger.warning(
                        "[Sample %d] Anomaly injection failed; recording "
                        "failed_ids without retry: %s",
                        sample_id,
                        exc,
                    )
                    return
                except HealthyBaselineFailure as exc:
                    result_store.record(
                        sample_id,
                        recovered=False,
                        agentic_llm_iteration_rounds=0,
                    )
                    logger.warning(
                        "[Sample %d] Healthy baseline is invalid; retaining "
                        "the sample without retry: %s",
                        sample_id,
                        exc,
                    )
                    return
                except RetryableSampleExecutionError as exc:
                    legacy.kill_tmux_sessions_by_prefix(worker_prefix)
                    legacy.remove_containers_by_prefix(worker_prefix)
                    if attempt < retry_attempts:
                        logger.warning(
                            "[Sample %d] Transient execution failure; "
                            "restarting the sample (%d/%d): %s",
                            sample_id,
                            attempt + 1,
                            retry_attempts,
                            exc,
                        )
                        continue
                    logger.exception(
                        "[Sample %d] Transient execution failure remained "
                        "after %d attempt(s) and remains pending: %s",
                        sample_id,
                        retry_attempts,
                        exc,
                    )
                    if bool(config.get("finalize_execution_errors", False)):
                        finalize_execution_error(
                            sample_id,
                            output_base,
                            "retryable_execution_error",
                            retry_attempts,
                            exc,
                            result_store,
                        )
                        logger.warning(
                            "[Sample %d] Finalized the execution error as "
                            "retained without a V1 retry.",
                            sample_id,
                        )
                    return
                except Exception as exc:  # noqa: BLE001
                    legacy.kill_tmux_sessions_by_prefix(worker_prefix)
                    legacy.remove_containers_by_prefix(worker_prefix)
                    logger.exception(
                        "[Sample %d] Processing failed and remains pending: %s",
                        sample_id,
                        exc,
                    )
                    if bool(config.get("finalize_execution_errors", False)):
                        finalize_execution_error(
                            sample_id,
                            output_base,
                            "unhandled_execution_error",
                            attempt,
                            exc,
                            result_store,
                        )
                        logger.warning(
                            "[Sample %d] Finalized the execution error as "
                            "retained without a V1 retry.",
                            sample_id,
                        )
                    return
        finally:
            slot_queue.put(slot)

    logger.info(
        "Starting Agentic LLM filter: input_total=%d selected_total=%d "
        "finalized=%d pending=%d parallelism=%d",
        len(cases),
        total_samples,
        finalized_before_launch,
        len(pending_ids),
        parallelism,
    )
    processed_this_launch = 0
    progress_bar = tqdm(
        total=total_samples,
        initial=finalized_before_launch,
        desc="Agentic filter",
        unit="sample",
        dynamic_ncols=True,
        disable=not sys.stderr.isatty(),
    )

    def report_progress() -> None:
        nonlocal processed_this_launch
        processed_this_launch += 1
        passed, failed, pending = result_store.counts_for(selected_ids)
        finalized = passed + failed
        progress_bar.update(1)
        progress_bar.set_postfix_str(
            f"finalized={finalized}/{total_samples}, pending={pending}",
            refresh=True,
        )
        logger.info(
            "Agentic LLM filter progress: processed_this_launch=%d/%d "
            "selected_total=%d finalized=%d/%d pending=%d",
            processed_this_launch,
            len(pending_ids),
            total_samples,
            finalized,
            total_samples,
            pending,
        )

    try:
        if parallelism == 1:
            for sample_id in sorted(pending_ids):
                worker(sample_id)
                report_progress()
        else:
            with ThreadPoolExecutor(max_workers=parallelism) as executor:
                futures = [
                    executor.submit(worker, sample_id)
                    for sample_id in sorted(pending_ids)
                ]
                for future in as_completed(futures):
                    future.result()
                    report_progress()
    finally:
        progress_bar.close()

    passed, failed, pending = result_store.counts_for(selected_ids)
    completed = passed + failed
    recovery_ratio = failed / completed if completed else 0.0
    retained_ratio = passed / completed if completed else 0.0
    logger.info(
        "Agentic LLM filter finished: processed_this_launch=%d/%d "
        "completed=%d/%d pending=%d "
        "recovered_and_filtered=%d (%.2f%%), retained=%d (%.2f%%)",
        processed_this_launch,
        len(pending_ids),
        completed,
        total_samples,
        pending,
        failed,
        recovery_ratio * 100,
        passed,
        retained_ratio * 100,
    )
    return pending


def main() -> int:
    args = parse_args()
    config_path = react.project_path(args.config.expanduser())
    try:
        config = read_json(config_path)
        if not isinstance(config, dict):
            raise ValueError("Configuration root must be a JSON object.")
        validate_filter_config(config)
        cases = list(react.validate_input_data(config))
        if not cases:
            raise ValueError("key_information.json must contain at least one sample.")
        case_ids = [case_id(case) for case in cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("key_information.json contains duplicate sample IDs.")
        sample_root = react.project_path(config["key_information_path"]).parent
        for case in cases:
            _, sample_directory = react.find_sample_directory(sample_root, case)
            for name in ("config.json", "anomaly_description.json"):
                if not isinstance(read_json(sample_directory / name), dict):
                    raise ValueError(f"{sample_directory / name} must contain a JSON object.")
            react.timeout_from_script(
                react.read_text(sample_directory / "anomaly_inject.sh"), "Anomaly injection script"
            )
        if not args.dry_run:
            validate_llm_runtime_config(config["llm_configuration"])
    except (FileNotFoundError, OSError, TypeError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(
            f"Validated {len(cases)} samples using {config_path}; "
            f"median_runs={config['median_runs']}."
        )
        return 0

    output_base = react.project_path(config["output_base_dir"])
    logger = react.set_up_logger(str(output_base / "log.txt"))
    try:
        assert_agentic_docker_preflight(config)
        pending = run_filter(config, cases, logger)
        if pending:
            logger.warning("%d samples remain pending; restart with the same configuration to resume.", pending)
            return 1
    except DockerPreflightError as exc:
        logger.error("Agentic Docker preflight failed: %s", exc)
        return DOCKER_PREFLIGHT_EXIT_CODE
    except (OSError, TypeError, ValueError, RuntimeError) as exc:
        logger.exception("Agentic LLM filter aborted: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
