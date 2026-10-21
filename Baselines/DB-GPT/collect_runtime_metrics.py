"""Collect real container and PostgreSQL time-series evidence for DB-GPT."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


SIZE_UNITS = {
    "B": 1, "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4,
    "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4,
}
COUNTER_METRICS = {
    "pg_stat_database_tup_returned",
    "pg_stat_database_xact_commit",
    "pg_stat_database_xact_rollback",
    "pg_stat_database_temp_bytes",
    "pg_stat_database_deadlocks",
}


def parse_size(value: str) -> float:
    match = re.fullmatch(r"\s*([0-9.]+)\s*([KMGT]?i?B)\s*", value, re.IGNORECASE)
    if match is None:
        raise ValueError(f"Unsupported Docker size value: {value!r}")
    return float(match.group(1)) * SIZE_UNITS[match.group(2).upper()]


def parse_pair(value: str) -> tuple[float, float]:
    left, right = value.split("/", 1)
    return parse_size(left), parse_size(right)


def counter_rates(
    current: dict[str, float],
    previous: dict[str, float],
    elapsed: float,
) -> dict[str, float]:
    elapsed = max(elapsed, 1e-6)
    return {
        f"{name}_per_second": max(value - previous[name], 0.0) / elapsed
        for name, value in current.items()
        if name in previous
    }


def docker_sample(container: str) -> dict[str, float]:
    completed = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
        text=True, capture_output=True, check=False, timeout=20,
    )
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or "docker stats failed")
    payload = json.loads(completed.stdout.strip())
    network_rx, network_tx = parse_pair(payload["NetIO"])
    block_read, block_write = parse_pair(payload["BlockIO"])
    return {
        "cpu_usage": float(payload["CPUPerc"].rstrip("%")),
        "memory_usage": float(payload["MemPerc"].rstrip("%")),
        "container_network_receive_bytes": network_rx,
        "container_network_transmit_bytes": network_tx,
        "container_block_read_bytes": block_read,
        "container_block_write_bytes": block_write,
    }


def query_rows(conn, sql: str) -> list[dict[str, Any]] | dict[str, str]:
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def database_sample(conn) -> tuple[dict[str, float], dict[str, Any]]:
    stats = query_rows(
        conn,
        """SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
                  tup_returned, tup_fetched, tup_inserted, tup_updated,
                  tup_deleted, conflicts, temp_files, temp_bytes, deadlocks
           FROM pg_stat_database WHERE datname = current_database()""",
    )
    activity = query_rows(
        conn,
        """SELECT COALESCE(state, 'unknown') AS state,
                  COALESCE(wait_event_type, 'none') AS wait_event_type,
                  COALESCE(wait_event, 'none') AS wait_event, count(*) AS sessions
           FROM pg_stat_activity WHERE datname = current_database()
           GROUP BY state, wait_event_type, wait_event ORDER BY sessions DESC""",
    )
    blocked = query_rows(
        conn,
        """SELECT count(*) AS blocked_sessions FROM pg_stat_activity
           WHERE datname = current_database()
             AND cardinality(pg_blocking_pids(pid)) > 0""",
    )
    sessions = query_rows(
        conn,
        """SELECT pid, backend_start, xact_start, query_start, state,
                  wait_event_type, wait_event, pg_blocking_pids(pid) AS blocking_pids,
                  left(query, 1000) AS query
           FROM pg_stat_activity
           WHERE datname = current_database() AND pid <> pg_backend_pid()
           ORDER BY query_start NULLS LAST LIMIT 100""",
    )
    stats_row = stats[0] if isinstance(stats, list) and stats else {}
    activity_rows = activity if isinstance(activity, list) else []
    blocked_count = (
        float(blocked[0].get("blocked_sessions", 0))
        if isinstance(blocked, list) and blocked else 0.0
    )
    hits = float(stats_row.get("blks_hit", 0) or 0)
    reads = float(stats_row.get("blks_read", 0) or 0)
    metrics = {
        "pg_stat_activity_count": sum(float(row["sessions"]) for row in activity_rows),
        "pg_stat_activity_active_count": sum(
            float(row["sessions"]) for row in activity_rows if row["state"] == "active"
        ),
        "pg_locks_count": blocked_count,
        "pg_stat_database_tup_returned": float(stats_row.get("tup_returned", 0) or 0),
        "pg_stat_database_xact_commit": float(stats_row.get("xact_commit", 0) or 0),
        "pg_stat_database_xact_rollback": float(stats_row.get("xact_rollback", 0) or 0),
        "pg_stat_database_temp_bytes": float(stats_row.get("temp_bytes", 0) or 0),
        "pg_stat_database_deadlocks": float(stats_row.get("deadlocks", 0) or 0),
        "pg_stat_database_blks_hit_ratio": hits / (hits + reads) if hits + reads else 1.0,
    }
    return metrics, {
        "database_stats": stats,
        "activity_summary": activity,
        "blocked_sessions": blocked,
        "sessions": sessions,
    }


def connect_readonly(
    args: argparse.Namespace,
    user: str,
    password: str,
):
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        user=user,
        password=password,
        dbname=args.database,
        connect_timeout=10,
        application_name="db_gpt_metric_collector",
        options="-c default_transaction_read_only=on -c statement_timeout=10000",
        gssencmode="disable",
    )


def collect(args: argparse.Namespace) -> dict[str, Any]:
    effective_user = args.user
    used_superuser_fallback = False
    try:
        conn = connect_readonly(args, args.user, args.password)
    except psycopg2.OperationalError as exc:
        message = str(exc).lower()
        connection_exhausted = (
            "remaining connection slots are reserved" in message
            or "too many connections" in message
        )
        if not connection_exhausted or not args.superuser:
            raise
        conn = connect_readonly(args, args.superuser, args.superpassword)
        effective_user = args.superuser
        used_superuser_fallback = True
    conn.set_session(readonly=True, autocommit=True)
    started = datetime.now(timezone.utc)
    server = query_rows(
        conn,
        """SELECT version() AS version, current_database() AS database,
                  current_user AS user, now() AS observed_at,
                  pg_database_size(current_database()) AS database_size_bytes""",
    )
    settings = query_rows(
        conn,
        """SELECT name, setting, unit FROM pg_settings
           WHERE name IN ('shared_buffers', 'effective_cache_size', 'work_mem',
                          'maintenance_work_mem', 'max_connections',
                          'random_page_cost', 'max_parallel_workers')
           ORDER BY name""",
    )
    series: dict[str, list[float]] = {}
    samples = []
    previous_counters: dict[str, float] = {}
    previous_observed_at: datetime | None = None
    try:
        for index in range(args.samples):
            observed_at = datetime.now(timezone.utc)
            database_metrics, database_detail = database_sample(conn)
            cumulative_metrics = {
                name: database_metrics.pop(name)
                for name in COUNTER_METRICS
                if name in database_metrics
            }
            metrics = {**database_metrics, **docker_sample(args.container)}
            if previous_observed_at is not None:
                elapsed = max(
                    (observed_at - previous_observed_at).total_seconds(),
                    1e-6,
                )
                # A negative delta means PostgreSQL statistics were reset.
                metrics.update(
                    counter_rates(cumulative_metrics, previous_counters, elapsed)
                )
            for name, value in metrics.items():
                series.setdefault(name, []).append(value)
            samples.append(
                {
                    "observed_at": observed_at.isoformat(),
                    "epoch": observed_at.timestamp(),
                    "metrics": metrics,
                    "cumulative_metrics": cumulative_metrics,
                    **database_detail,
                }
            )
            previous_counters = cumulative_metrics
            previous_observed_at = observed_at
            if index + 1 < args.samples:
                time.sleep(args.interval)
    finally:
        conn.close()
    ended = datetime.now(timezone.utc)
    return {
        "diagnosis_start_time": started.isoformat(),
        "diagnosis_start_epoch": int(started.timestamp()),
        "snapshot_end_time": ended.isoformat(),
        "snapshot_end_epoch": max(int(ended.timestamp()), int(started.timestamp()) + 1),
        "target": {
            "container": args.container,
            "host": args.host,
            "port": args.port,
            "database": args.database,
            "user": effective_user,
            "requested_user": args.user,
            "superuser_fallback": used_superuser_fallback,
        },
        "server": server,
        "settings": settings,
        "samples": samples,
        "time_series": series,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", default="")
    parser.add_argument("--superuser", default="")
    parser.add_argument("--superpassword", default="")
    parser.add_argument("--database", required=True)
    parser.add_argument("--samples", type=int, default=12)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.samples < 10:
        parser.error("--samples must be at least 10 for DB-GPT anomaly analysis")
    if args.interval <= 0:
        parser.error("--interval must be positive")
    payload = collect(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
