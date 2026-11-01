"""Shared environment preparation, diagnosis runtime policy and suite launcher.

Environment preparation is not a diagnosis attempt or a scored failure. The
admission wait is outside the command deadline. SQL is never retried in a
partially modified database; callers retry only with a freshly imported fixture.
No agent budget, anomaly mechanism, measurement timeout or score formula changes.

Importing this module does not launch a suite. Run it as a script or with
``python -m Baselines.preparation`` to use the sequential baseline launcher.
"""

import argparse
import hashlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if not __package__:
    # Direct script execution must find the project-local admission controller,
    # including when launched from outside the repository.
    sys.path.insert(0, str(ROOT))

def _lazy_io_heavy_phase(function):
    # Admission was consolidated into docker_func; importing this dependency-
    # light module must still not import database drivers at module load time.
    from functools import wraps
    @wraps(function)
    def admitted(*args, **kwargs):
        from docker_func import io_heavy_phase
        return io_heavy_phase(function)(*args, **kwargs)
    return admitted


class PreparationError(RuntimeError):
    def __init__(self, phase, message):
        super().__init__(message)
        self.phase = phase


def preparation_failure(status):
    if not isinstance(status, dict):
        return False
    if status.get("status") == "preparation_failed":
        return True
    if status.get("status") != "failed":
        return False
    error = status.get("error", "")
    return isinstance(error, str) and error.startswith((
        "Data modification failed:", "Anomaly injection failed:",
        "Unable to initialize Docker container `", "Unable to initialize database `",
    ))


@_lazy_io_heavy_phase
def run_data_preparation(executor, container_name, command, log_path,
                         timeout=600, *, session_name=None):
    budget = int(os.environ.get("DB_DIAG_DATA_PREPARATION_TIMEOUT_SECONDS", "1800"))
    if not 600 <= budget <= 7200:
        raise ValueError("Data preparation timeout must be between 600 and 7200 seconds")
    return executor(container_name, command, log_path, max(timeout, budget),
                    session_name=session_name)


OUTPUT = ROOT / "Output/baseline_suite"
CONFIGS = ROOT / "Configs/baselines"
FRAMEWORKS = ("react", "codex", "claude_code")
ENTRYPOINTS = {
    "react": ROOT / "Baselines/ReAct/react.py",
    "codex": ROOT / "Baselines/Codex/codex.py",
    "claude_code": ROOT / "Baselines/Claude_Code/claude_code.py",
}


def diagnosis_llm_config(config):
    """Use bounded transport attempts without multiplying SDK-level retries."""
    result = dict(config)
    result.setdefault("retry_times", 50)
    result.setdefault("request_retry_times", 49)
    result.setdefault("request_retry_delay_seconds", 30)
    result.setdefault("timeout", 300)
    result.setdefault("sdk_max_retries", 0)
    return result


def direct_injection_command(script_path, dbms, database, schema, user_config, timeout):
    """Run the supplied injection once, without replaying or scoring workloads."""
    timeout = int(timeout)
    if timeout <= 0:
        raise ValueError("direct_injection_timeout_seconds must be positive")
    host = str(user_config["DB_HOST"])
    port = "3306" if "mysql" in dbms.lower() else "5432"
    user = str(user_config["DB_USER"])
    password = str(user_config["DB_PASSWORD"])
    env = {
        "DB_HOST": host, "DB_PORT": port, "DB_USER": user,
        "DB_PASSWORD": password, "DB_NAME": database, "DB_SCHEMA": schema,
        "DBMS": dbms, "PGHOST": host, "PGPORT": port, "PGUSER": user,
        "PGPASSWORD": password, "PGDATABASE": database,
        "MYSQL_HOST": host, "MYSQL_PORT": port, "MYSQL_USER": user,
        "MYSQL_DATABASE": database, "MYSQL_PWD": password,
        "PYTHONUNBUFFERED": "1",
    }
    if "mysql" not in dbms.lower():
        env["PGOPTIONS"] = f"-c search_path={schema}"
    return [
        "timeout", "--signal=TERM", "--kill-after=30s", str(timeout),
        "env", *(f"{key}={value}" for key, value in env.items()),
        "bash", str(script_path),
    ]


def write_status(state):
    OUTPUT.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT / ".suite_status.tmp"
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(OUTPUT / "suite_status.json")


def stop_group(process):
    # Some SDK transports start their own sessions. Include descendant PIDs,
    # rather than assuming every CLI/MCP process stays in the parent's group.
    parents = {}
    for path in Path("/proc").glob("[0-9]*"):
        try:
            fields = (path / "stat").read_text().rsplit(")", 1)[1].split()
            parents[int(path.name)] = int(fields[1])
        except (OSError, ValueError, IndexError):
            pass
    descendants = {process.pid}
    while True:
        found = {pid for pid, parent in parents.items() if parent in descendants}
        if found <= descendants:
            break
        descendants.update(found)
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for pid in descendants - {process.pid}:
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            continue
    process.wait()


def cleanup(prefix):
    """Remove only resources with this run's explicit, unique framework prefix."""
    containers = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=30)
    for name in containers.stdout.splitlines():
        if name.startswith(prefix + "_w"):
            subprocess.run(["docker", "rm", "-f", name], timeout=60, check=False)
    sessions = subprocess.run(["tmux", "list-sessions", "-F", "#{session_name}"], capture_output=True, text=True, timeout=10)
    for name in sessions.stdout.splitlines():
        if name.startswith(prefix + "_w"):
            subprocess.run(["tmux", "kill-session", "-t", "=" + name], timeout=10, check=False)


def ensure_ports(config, config_path):
    start = int(config["host_port"])
    workers = config.get("parallelism_num", 1)
    if type(workers) is not int or workers < 1 or not 1 <= start <= 65536 - workers:
        raise ValueError("Worker count and host port must define a valid port range.")
    for port in range(start, min(start + 1000, 65537 - workers), workers):
        probes = []
        try:
            for value in range(port, port + workers):
                probe = socket.socket()
                probes.append(probe)
                probe.bind(("0.0.0.0", value))
            if port != start:
                config["host_port"] = port
                config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n")
                print(f"Worker ports changed to {port}-{port + workers - 1}", flush=True)
            return
        except OSError:
            pass
        finally:
            for probe in probes:
                probe.close()
    raise RuntimeError("No free worker port range available.")


def main():
    global OUTPUT, CONFIGS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--framework", nargs="+", choices=FRAMEWORKS,
                        help="Run only these frameworks, preserving other framework statuses.")
    parser.add_argument("--config-dir", type=Path, default=CONFIGS,
                        help="Directory containing METHOD/example.json or legacy METHOD.json configurations.")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT,
                        help="Directory containing Agentic smoke evidence and suite status.")
    args = parser.parse_args()
    CONFIGS = args.config_dir.expanduser().resolve()
    OUTPUT = args.output_dir.expanduser().resolve()
    frameworks = tuple(name for name in FRAMEWORKS if not args.framework or name in args.framework)
    state = {name: {"status": "pending"} for name in FRAMEWORKS}
    status_path = OUTPUT / "suite_status.json"
    if args.framework and status_path.exists():
        state.update(json.loads(status_path.read_text()))
        for name in frameworks:
            state[name] = {"status": "pending"}
    proof_path = OUTPUT / "agentic_smoke_status.json"
    proofs = json.loads(proof_path.read_text()) if proof_path.exists() else {}
    for name in frameworks:
        proof = proofs.get(name)
        if not proof:
            state[name] = {"status": "blocked_by_preflight", "error": "No Agentic smoke-test evidence available."}
            continue
        config_path = CONFIGS / f"{name}.json"
        if not config_path.is_file():
            config_path = CONFIGS / name / "example.json"
        if hashlib.sha256(config_path.read_bytes()).hexdigest() != proof["config_sha256"]:
            state[name] = {"status": "blocked_by_preflight", "error": "Configuration changed; run the Agentic smoke test again."}
        elif not proof["passed"]:
            state[name] = {
                "status": "api_error_stopped" if name == "claude_code" else "blocked_by_preflight",
                "error": proof["error"], "test_result": proof["test_result"],
            }
    write_status(state)
    for name in frameworks:
        if state[name]["status"] != "pending":
            print(f"=== SKIP {name}: {state[name]['status']}: {state[name]['error']} ===", flush=True)
            continue
        config_path = CONFIGS / f"{name}.json"
        if not config_path.is_file():
            config_path = CONFIGS / name / "example.json"
        config = json.loads(config_path.read_text())
        workers = config.get("parallelism_num", 1)
        if type(workers) is not int or workers < 1:
            raise ValueError("parallelism_num must be a positive integer.")
        if name == "db_gpt" and workers != 1:
            raise ValueError("DB-GPT requires parallelism_num=1 because native files are shared.")
        output = Path(config["output_base_dir"]).expanduser()
        if not output.is_absolute():
            output = ROOT / output
        output.mkdir(parents=True, exist_ok=True)
        fatal = output / "fatal_api_error.json"
        if fatal.exists():
            raise RuntimeError(f"Previous API failure marker exists: {fatal}; inspect before restarting.")
        ensure_ports(config, config_path)
        print(f"=== START {name}; parallelism={workers}; ports={config['host_port']}-{config['host_port'] + workers - 1} ===", flush=True)
        with (output / "console.log").open("a", buffering=1) as log:
            process = subprocess.Popen(
                [sys.executable, "-u", str(ENTRYPOINTS[name]), "--config", str(config_path)],
                cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            state[name] = {"status": "running", "pid": process.pid, "started_at": time.time()}
            write_status(state)
            try:
                while process.poll() is None:
                    if name == "claude_code" and fatal.exists():
                        print("Claude API error detected: stopping all Claude workers.", flush=True)
                        state[name]["status"] = "api_error_stopped"
                        state[name]["error"] = json.loads(fatal.read_text())
                        write_status(state)
                        stop_group(process)
                        break
                    time.sleep(1)
            except BaseException:
                state[name].update(status="interrupted", ended_at=time.time())
                write_status(state)
                raise
            finally:
                if process.poll() is None:
                    stop_group(process)
                cleanup(config["docker_name"])
            state[name].update(returncode=process.returncode, ended_at=time.time())
            if state[name]["status"] == "running":
                state[name]["status"] = "completed" if process.returncode == 0 else "failed"
            write_status(state)
            print(f"=== END {name}: {state[name]['status']} (exit {process.returncode}) ===", flush=True)
    return 0 if all(state[name]["status"] == "completed" for name in frameworks) else 1


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    raise SystemExit(main())
