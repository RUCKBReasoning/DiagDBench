"""Bounded PG health collection, isolated from workload and injection processes."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import uuid

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from Scripts import anomaly_judge

PROBES = (
    "identity", "runtime_activity", "blocking_sessions", "tables", "table_sizes",
    "indexes", "constraints", "routines", "extensions", "configuration",
    "database_size", "current_waits", "prepared_transactions", "replication",
)
MAX_CHECKPOINT_BYTES = 16 * 1024 * 1024


def validate_budget(config):
    fields = {
        "health_report_timeout_seconds": 30,
        "health_report_connect_timeout_seconds": 5,
        "health_report_statement_timeout_ms": 5000,
        "health_report_lock_timeout_ms": 1000,
    }
    values = {}
    for name, default in fields.items():
        value = config.get(name, default)
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
        values[name] = value
    total = values["health_report_timeout_seconds"]
    connect = values["health_report_connect_timeout_seconds"]
    statement = values["health_report_statement_timeout_ms"]
    lock = values["health_report_lock_timeout_ms"]
    if not (lock <= statement <= total * 1000 and connect <= total):
        raise ValueError("Require lock <= statement <= total and connect <= total")
    return {"total_seconds": total, "connect_seconds": connect,
            "statement_ms": statement, "lock_ms": lock}


def empty_snapshot(db):
    return {
        "schema_version": 1, "captured_at": anomaly_judge._utc_now(),
        "dbms": "postgresql", "database": db["database"], "schema": db["schema"],
        "collection_status": "partial", "collection_strategy": "bounded_pg_v1",
        "probe_status": {name: {"status": "not_run"} for name in PROBES},
        "collection_errors": [], "policy": {"verdict_scope": "current operational state"},
        "layers": {
            "container": {"available": False, "state": {}, "resources": {}, "processes": [], "errors": []},
            "durable": {"available": False, "identity": {}, "schema": db["schema"],
                        "tables": [], "indexes": [], "constraints": [], "routines": [],
                        "extensions": [], "configuration": {}, "invalid_indexes": [],
                        "high_dead_tuple_tables": [], "statistics_issues": []},
            "runtime": {"available": False},
            "functional": {"available": False, "simple_query_succeeded": False},
        },
    }


def write_checkpoint(path, payload):
    data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    if len(data) > MAX_CHECKPOINT_BYTES:
        raise ValueError("Health checkpoint exceeds size limit")
    temporary = str(path) + ".tmp"
    with open(temporary, "wb") as stream:
        stream.write(data)
    os.replace(temporary, path)


def stop_worker(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        pass
    # Also reclaim child CLI processes, even if the Python parent already exited.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=2)


def collect_bounded(db_config, config, container_name, *, worker_path=None):
    budget = validate_budget(config)
    started = time.monotonic()
    request_id = uuid.uuid4().hex
    snapshot = empty_snapshot(db_config)
    complete = False
    error = None
    process = None
    with tempfile.TemporaryDirectory(prefix="db_diag_health_") as directory:
        path = Path(directory) / "checkpoint.json"
        request = {"request_id": request_id, "db_config": db_config, "budget": budget,
                   "container_name": container_name, "checkpoint": str(path)}
        try:
            process = subprocess.Popen(
                [sys.executable, "-B", str(worker_path or Path(__file__).resolve()), "--worker"],
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            process.communicate(
                json.dumps(request).encode(),
                timeout=max(.001, budget["total_seconds"] - (time.monotonic() - started)),
            )
            if process.returncode != 0:
                error = "health_worker_failed"
        except subprocess.TimeoutExpired:
            error = "health_collection_budget_exhausted"
        except Exception as exc:
            error = "health_worker_error:" + type(exc).__name__
        finally:
            if process is not None:
                stop_worker(process)
        if path.exists():
            try:
                with path.open("rb") as stream:
                    data = stream.read(MAX_CHECKPOINT_BYTES + 1)
                if len(data) > MAX_CHECKPOINT_BYTES:
                    raise ValueError("oversized checkpoint")
                payload = json.loads(data)
                candidate = payload["snapshot"]
                if (payload["request_id"] != request_id
                        or candidate["database"] != db_config["database"]
                        or candidate["schema"] != db_config["schema"]
                        or candidate["dbms"] != "postgresql"
                        or not isinstance(candidate["layers"], dict)):
                    raise ValueError("checkpoint identity mismatch")
                snapshot = candidate
                complete = payload.get("finished") is True
            except (ValueError, KeyError, TypeError):
                error = "health_checkpoint_invalid"
        if not complete or error:
            snapshot["collection_status"] = "partial"
            snapshot["collection_errors"].append(error or "health_worker_no_final_checkpoint")
            for info in snapshot["probe_status"].values():
                if info["status"] == "not_run":
                    info["status"] = "budget_exhausted" if error == "health_collection_budget_exhausted" else "not_run"
        snapshot["collection_duration_ms"] = round((time.monotonic() - started) * 1000, 3)
        snapshot["collection_budget_seconds"] = budget["total_seconds"]
        report = anomaly_judge.evaluate_current_state_snapshot(snapshot)
        report["health_request_id"] = request_id
        return report


def worker_main():
    request = json.load(sys.stdin)
    request_id = request["request_id"]
    path = Path(request["checkpoint"])
    budget = request["budget"]

    def checkpoint(snapshot, finished=False):
        write_checkpoint(path, {"request_id": request_id, "snapshot": snapshot, "finished": finished})

    snapshot = anomaly_judge.capture_database_state_snapshot(
        request["db_config"], connect_timeout=budget["connect_seconds"],
        container_name=request["container_name"], health_budget=budget,
        checkpoint=checkpoint,
    )
    checkpoint(snapshot, finished=True)


if __name__ == "__main__":
    if sys.argv[1:] != ["--worker"]:
        raise SystemExit("This entry point is for the health collector only")
    worker_main()
