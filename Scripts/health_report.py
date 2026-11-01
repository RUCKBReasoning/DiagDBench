#!/usr/bin/env python3
"""Standalone current-state health report for PostgreSQL and MySQL containers.

The module captures a normalized four-layer state snapshot:

1. container: service/process/resource state when a container name is supplied;
2. durable: schema objects, selected configuration and table health;
3. runtime: current sessions, waits, locks and other live failure signals;
4. functional: whether the database accepted and completed read-only probes.

Historical counters and log history are deliberately excluded from the health
verdict. The report does not require an anomaly definition, a pre-injection
baseline, a workload run, or any other prior state. Every invocation executes
the same low-cost read-only probes and judges only what is observable now.

Two independently generated reports can be compared with
``compare_database_health_reports``. This supports an initialized-state report
and a post-recovery report without making either individual probe depend on
anomaly metadata or injection logic.
"""

from __future__ import annotations

import argparse
import datetime as datetime_module
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    import psycopg2
except Exception:  # pragma: no cover - optional when only MySQL is evaluated
    psycopg2 = None

try:
    import mysql.connector as mysql_connector
except Exception:  # pragma: no cover - optional when only PostgreSQL is evaluated
    mysql_connector = None


SNAPSHOT_SCHEMA_VERSION = 1

DEFAULT_THRESHOLDS = {
    "idle_in_transaction_seconds": 60.0,
    "long_running_query_seconds": 300.0,
    "connection_saturation_ratio": 0.90,
    "disk_used_ratio": 0.95,
    "memory_used_ratio": 0.95,
    "cpu_used_ratio": 0.95,
    "dead_tuple_ratio": 0.30,
    "dead_tuple_minimum": 10000,
    "contention_session_count": 2,
    "replication_lag_seconds": 60.0,
    "connection_warning_ratio": 0.75,
    "disk_warning_ratio": 0.85,
    "resource_warning_ratio": 0.85,
    "blocked_sessions_critical": 5,
    "idle_transactions_critical": 3,
    "long_running_queries_critical": 3,
}

# These values affect availability, query planning, memory, logging or
# durability. Capturing a curated set keeps reports stable across versions and
# avoids flooding them with harmless session/display settings.
POSTGRES_CONFIG_NAMES = (
    "archive_mode",
    "autovacuum",
    "autovacuum_max_workers",
    "checkpoint_timeout",
    "deadlock_timeout",
    "effective_cache_size",
    "enable_bitmapscan",
    "enable_hashjoin",
    "enable_indexscan",
    "enable_mergejoin",
    "enable_nestloop",
    "enable_seqscan",
    "fsync",
    "full_page_writes",
    "lock_timeout",
    "log_min_duration_statement",
    "maintenance_work_mem",
    "max_connections",
    "max_locks_per_transaction",
    "max_parallel_workers",
    "max_parallel_workers_per_gather",
    "max_wal_size",
    "random_page_cost",
    "shared_buffers",
    "statement_timeout",
    "synchronous_commit",
    "temp_file_limit",
    "wal_level",
    "work_mem",
)

MYSQL_CONFIG_NAMES = (
    "autocommit",
    "binlog_format",
    "innodb_buffer_pool_size",
    "innodb_flush_log_at_trx_commit",
    "innodb_io_capacity",
    "innodb_lock_wait_timeout",
    "innodb_log_file_size",
    "innodb_redo_log_capacity",
    "max_connections",
    "max_execution_time",
    "max_heap_table_size",
    "optimizer_switch",
    "read_only",
    "slow_query_log",
    "sort_buffer_size",
    "sql_mode",
    "sync_binlog",
    "table_open_cache",
    "thread_cache_size",
    "tmp_table_size",
)

HISTORICAL_SIGNALS_EXCLUDED = (
    "PostgreSQL pg_stat_database.deadlocks and cumulative counters",
    "MySQL Aborted_connects and cumulative status counters",
    "past server error-log entries",
    "past statement errors and completed lock waits",
)


def _utc_now() -> str:
    return datetime_module.datetime.now(datetime_module.timezone.utc).isoformat()


def normalize_dbms(value: Any) -> str:
    normalized = str(value or "postgresql").strip().lower()
    if "mysql" in normalized or "maria" in normalized:
        return "mysql"
    if "postgres" in normalized or normalized in {"pg", "pgsql"}:
        return "postgresql"
    raise ValueError(f"Unsupported DBMS: {value!r}. Expected PostgreSQL or MySQL.")


def _resolve_connection_params(db_config: Mapping[str, Any], dbms: str) -> Dict[str, Any]:
    default_port = 3306 if dbms == "mysql" else 5432
    return {
        "host": db_config.get("host", "localhost"),
        "port": int(db_config.get("host_port") or db_config.get("port") or default_port),
        "user": db_config.get("user"),
        "password": db_config.get("password"),
        "database": (
            db_config.get("db_name")
            or db_config.get("database")
            or db_config.get("dbname")
        ),
    }


def _rows_as_dicts(cursor: Any) -> List[Dict[str, Any]]:
    columns = [description[0] for description in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


class _Collector:
    def __init__(self, cursor: Any, budget: Any = None):
        self.cursor = cursor
        self.errors: List[str] = []
        self.budget = budget

    def query(
        self,
        label: str,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> List[Dict[str, Any]]:
        if self.budget is not None and label in self.budget["rows"]:
            return self.budget["rows"][label]
        started = time.monotonic()
        status = "ok"
        rows = []
        try:
            if self.budget is not None:
                remaining = self.budget["deadline"] - started
                if remaining <= 0:
                    raise TimeoutError("health collection budget exhausted")
                self.cursor.execute(
                    "SELECT set_config('statement_timeout', %s, false), "
                    "set_config('lock_timeout', %s, false)",
                    (str(max(1, min(self.budget["statement_ms"], int(remaining * 1000)))),
                     str(max(1, min(self.budget["lock_ms"], int(remaining * 1000))))),
                )
            self.cursor.execute(sql, params)
            rows = _rows_as_dicts(self.cursor)
            return rows
        except Exception as exc:
            code = getattr(exc, "pgcode", None)
            status = (
                "lock_timeout" if code == "55P03" else
                "statement_timeout" if code == "57014" else
                "budget_exhausted" if isinstance(exc, TimeoutError) else "error"
            )
            message = " ".join(str(exc).split()) if self.budget is None else status
            self.errors.append(f"{label}: {message}")
            try:
                self.cursor.connection.rollback()
            except Exception:
                pass
            return []
        finally:
            if self.budget is not None:
                self.budget["rows"][label] = rows
                self.budget["probe_status"][label] = {
                    "status": status,
                    "duration_ms": round((time.monotonic() - started) * 1000, 3),
                }
                self.budget["publish"]()


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sort_records(records: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized = [
        {str(key): value for key, value in record.items()}
        for record in records
    ]
    return sorted(
        normalized,
        key=lambda item: json.dumps(item, sort_keys=True, default=str),
    )


def _config_map(rows: Iterable[Mapping[str, Any]], name_key: str, value_key: str) -> Dict[str, Any]:
    return {
        str(row[name_key]).lower(): row.get(value_key)
        for row in rows
        if row.get(name_key) is not None
    }


def _postgres_snapshot(
    connection: Any,
    schema: str,
    thresholds: Mapping[str, Any],
    budget: Any = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    cursor = connection.cursor()
    collector = _Collector(cursor, budget)
    if budget is not None:
        collector.query(
            "identity",
            "SELECT current_database() AS database_name, current_user AS current_user, "
            "current_setting('server_version') AS server_version;",
        )
        collector.query(
            "runtime_activity",
            """SELECT count(*) FILTER (WHERE cardinality(pg_blocking_pids(pid)) > 0) AS blocked_sessions,
            count(*) FILTER (WHERE state='idle in transaction' AND xact_start IS NOT NULL
            AND now()-xact_start > make_interval(secs => %s)) AS idle_in_transaction_sessions,
            count(*) FILTER (WHERE state='active' AND query_start IS NOT NULL
            AND now()-query_start > make_interval(secs => %s)) AS long_running_queries,
            count(*) AS used_connections FROM pg_stat_activity
            WHERE datname=current_database() AND pid<>pg_backend_pid();""",
            (thresholds["idle_in_transaction_seconds"], thresholds["long_running_query_seconds"]),
        )
        collector.query(
            "blocking_sessions",
            """SELECT pid, wait_event_type, wait_event, pg_blocking_pids(pid) AS blockers
            FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
            AND cardinality(pg_blocking_pids(pid))>0 ORDER BY pid LIMIT 32;""",
        )

    tables = collector.query(
        "tables",
        """
        SELECT n.nspname AS schema_name,
               c.relname AS object_name,
               c.relkind AS object_kind,
               c.reltuples::bigint AS estimated_rows,
               pg_total_relation_size(c.oid) AS total_bytes,
               COALESCE(s.n_live_tup, 0) AS live_rows,
               COALESCE(s.n_dead_tup, 0) AS dead_rows,
               s.last_analyze,
               s.last_autoanalyze
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE n.nspname = %s
          AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
        ORDER BY c.relkind, c.relname;
        """.replace(
            "pg_total_relation_size(c.oid)",
            "NULL::bigint" if budget is not None else "pg_total_relation_size(c.oid)",
        ),
        (schema,),
    )
    if budget is not None:
        sizes = collector.query(
            "table_sizes",
            """SELECT c.relname AS object_name, pg_total_relation_size(c.oid) AS total_bytes
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=%s AND c.relkind IN ('r','p','v','m','S')
            ORDER BY c.relkind,c.relname;""", (schema,),
        )
        by_name = {row["object_name"]: row["total_bytes"] for row in sizes}
        for row in tables:
            row["total_bytes"] = by_name.get(row["object_name"])
    indexes = collector.query(
        "indexes",
        """
        SELECT ns.nspname AS schema_name,
               tbl.relname AS table_name,
               idx.relname AS index_name,
               i.indisvalid AS is_valid,
               i.indisready AS is_ready,
               pg_get_indexdef(idx.oid) AS definition
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = %s
        ORDER BY tbl.relname, idx.relname;
        """,
        (schema,),
    )
    constraints = collector.query(
        "constraints",
        """
        SELECT ns.nspname AS schema_name,
               tbl.relname AS table_name,
               con.conname AS constraint_name,
               con.contype AS constraint_type,
               con.convalidated AS is_validated,
               pg_get_constraintdef(con.oid, true) AS definition
        FROM pg_constraint con
        JOIN pg_class tbl ON tbl.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = %s
        ORDER BY tbl.relname, con.conname;
        """,
        (schema,),
    )
    routines = collector.query(
        "routines",
        """
        SELECT n.nspname AS schema_name,
               p.proname AS routine_name,
               p.prokind AS routine_kind,
               pg_get_function_identity_arguments(p.oid) AS arguments
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s
        ORDER BY p.proname, arguments;
        """,
        (schema,),
    )
    extensions = collector.query(
        "extensions",
        "SELECT extname AS name, extversion AS version FROM pg_extension ORDER BY extname;",
    )
    config_rows = collector.query(
        "configuration",
        """
        SELECT name, setting, unit, source, pending_restart
        FROM pg_settings
        WHERE name = ANY(%s)
        ORDER BY name;
        """,
        (list(POSTGRES_CONFIG_NAMES),),
    )
    identity_rows = collector.query(
        "identity",
        """
        SELECT current_database() AS database_name,
               current_user AS current_user,
               current_setting('server_version') AS server_version,
               pg_database_size(current_database()) AS database_bytes;
        """,
    )
    if budget is not None:
        database_size = collector.query(
            "database_size", "SELECT pg_database_size(current_database()) AS database_bytes;"
        )
        for row in identity_rows:
            row["database_bytes"] = database_size[0]["database_bytes"] if database_size else None

    runtime_rows = collector.query(
        "runtime_activity",
        """
        SELECT
          count(*) FILTER (
            WHERE cardinality(pg_blocking_pids(pid)) > 0
          ) AS blocked_sessions,
          count(*) FILTER (
            WHERE state = 'idle in transaction'
              AND xact_start IS NOT NULL
              AND now() - xact_start > make_interval(secs => %s)
          ) AS idle_in_transaction_sessions,
          count(*) FILTER (
            WHERE state = 'active'
              AND query_start IS NOT NULL
              AND now() - query_start > make_interval(secs => %s)
          ) AS long_running_queries,
          count(*) AS used_connections
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid();
        """,
        (
            thresholds["idle_in_transaction_seconds"],
            thresholds["long_running_query_seconds"],
        ),
    )
    waits = collector.query(
        "current_waits",
        """
        SELECT COALESCE(wait_event_type, 'none') AS wait_type, count(*) AS sessions
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND state = 'active'
        GROUP BY COALESCE(wait_event_type, 'none')
        ORDER BY wait_type;
        """,
    )
    prepared = collector.query(
        "prepared_transactions",
        "SELECT count(*) AS prepared_transactions FROM pg_prepared_xacts WHERE database = current_database();",
    )
    replication = collector.query(
        "replication",
        """
        SELECT count(*) AS sender_count,
               COALESCE(max(EXTRACT(EPOCH FROM replay_lag)), 0) AS max_replay_lag_seconds
        FROM pg_stat_replication;
        """,
    )

    runtime = dict(runtime_rows[0]) if runtime_rows else {}
    if budget is not None:
        runtime["blocking_sessions"] = budget["rows"].get("blocking_sessions", [])
    max_connections = _config_map(config_rows, "name", "setting").get("max_connections")
    try:
        max_connections_int = int(max_connections)
    except (TypeError, ValueError):
        max_connections_int = 0
    used_connections = int(runtime.get("used_connections") or 0)
    runtime["max_connections"] = max_connections_int
    runtime["connection_saturation_ratio"] = (
        round(used_connections / max_connections_int, 4)
        if max_connections_int > 0
        else None
    )
    runtime["current_waits"] = _sort_records(waits)
    runtime["prepared_transactions"] = (
        int(prepared[0].get("prepared_transactions") or 0) if prepared else None
    )
    runtime["replication"] = replication[0] if replication else {"configured": False}

    invalid_indexes = [
        row["index_name"]
        for row in indexes
        if not row.get("is_valid") or not row.get("is_ready")
    ]
    high_dead_tuple_tables = []
    statistics_issues = []
    for row in tables:
        live = int(row.get("live_rows") or 0)
        dead = int(row.get("dead_rows") or 0)
        ratio = dead / max(live + dead, 1)
        estimated_rows = row.get("estimated_rows")
        if (
            row.get("object_kind") in {"r", "p"}
            and (estimated_rows is None or int(estimated_rows) < 0)
        ):
            statistics_issues.append(
                {
                    "table": row["object_name"],
                    "reason": "table has no usable planner row estimate",
                }
            )
        if (
            dead >= thresholds["dead_tuple_minimum"]
            and ratio >= thresholds["dead_tuple_ratio"]
        ):
            high_dead_tuple_tables.append(
                {
                    "table": row["object_name"],
                    "live_rows": live,
                    "dead_rows": dead,
                    "dead_tuple_ratio": round(ratio, 4),
                }
            )

    durable = {
        "available": bool(identity_rows),
        "identity": identity_rows[0] if identity_rows else {},
        "schema": schema,
        "tables": _sort_records(tables),
        "indexes": _sort_records(indexes),
        "constraints": _sort_records(constraints),
        "routines": _sort_records(routines),
        "extensions": _sort_records(extensions),
        "configuration": _config_map(config_rows, "name", "setting"),
        "invalid_indexes": sorted(invalid_indexes),
        "high_dead_tuple_tables": high_dead_tuple_tables,
        "statistics_issues": statistics_issues,
    }
    if budget is not None:
        for setting in ("statement_timeout", "lock_timeout"):
            durable["configuration"][setting] = None
        durable["probe_session_settings"] = {
            "statement_timeout_ms": budget["statement_ms"],
            "lock_timeout_ms": budget["lock_ms"],
            "application_name": "db_diag_health_probe",
            "original_session_timeout_values": "unavailable",
        }
    durable["schema_fingerprint"] = _stable_hash(
        {
            "tables": durable["tables"],
            "indexes": durable["indexes"],
            "constraints": durable["constraints"],
            "routines": durable["routines"],
            "extensions": durable["extensions"],
        }
    )
    runtime["available"] = bool(runtime_rows)
    cursor.close()
    return durable, runtime, collector.errors


def _mysql_snapshot(
    connection: Any,
    schema: str,
    thresholds: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    cursor = connection.cursor()
    collector = _Collector(cursor)

    tables = collector.query(
        "tables",
        """
        SELECT TABLE_SCHEMA AS schema_name,
               TABLE_NAME AS object_name,
               TABLE_TYPE AS object_kind,
               ENGINE AS engine,
               TABLE_ROWS AS estimated_rows,
               COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0) AS total_bytes,
               AUTO_INCREMENT AS auto_increment_value
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_TYPE, TABLE_NAME;
        """,
        (schema,),
    )
    indexes = collector.query(
        "indexes",
        """
        SELECT TABLE_SCHEMA AS schema_name,
               TABLE_NAME AS table_name,
               INDEX_NAME AS index_name,
               NON_UNIQUE AS non_unique,
               INDEX_TYPE AS index_type,
               GROUP_CONCAT(
                   CONCAT(COLUMN_NAME, ':', SEQ_IN_INDEX)
                   ORDER BY SEQ_IN_INDEX SEPARATOR ','
               ) AS columns
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s
        GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, NON_UNIQUE, INDEX_TYPE
        ORDER BY TABLE_NAME, INDEX_NAME;
        """,
        (schema,),
    )
    constraints = collector.query(
        "constraints",
        """
        SELECT TABLE_SCHEMA AS schema_name,
               TABLE_NAME AS table_name,
               CONSTRAINT_NAME AS constraint_name,
               CONSTRAINT_TYPE AS constraint_type
        FROM information_schema.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, CONSTRAINT_NAME;
        """,
        (schema,),
    )
    triggers = collector.query(
        "triggers",
        """
        SELECT TRIGGER_SCHEMA AS schema_name,
               TRIGGER_NAME AS trigger_name,
               EVENT_OBJECT_TABLE AS table_name,
               EVENT_MANIPULATION AS event_name,
               ACTION_TIMING AS action_timing,
               ACTION_STATEMENT AS definition
        FROM information_schema.TRIGGERS
        WHERE TRIGGER_SCHEMA = %s
        ORDER BY TRIGGER_NAME;
        """,
        (schema,),
    )
    routines = collector.query(
        "routines",
        """
        SELECT ROUTINE_SCHEMA AS schema_name,
               ROUTINE_NAME AS routine_name,
               ROUTINE_TYPE AS routine_type
        FROM information_schema.ROUTINES
        WHERE ROUTINE_SCHEMA = %s
        ORDER BY ROUTINE_NAME;
        """,
        (schema,),
    )
    events = collector.query(
        "events",
        """
        SELECT EVENT_SCHEMA AS schema_name,
               EVENT_NAME AS event_name,
               STATUS AS status,
               EVENT_TYPE AS event_type
        FROM information_schema.EVENTS
        WHERE EVENT_SCHEMA = %s
        ORDER BY EVENT_NAME;
        """,
        (schema,),
    )
    config_rows = collector.query("configuration", "SHOW GLOBAL VARIABLES;")
    all_config = _config_map(config_rows, "Variable_name", "Value")
    configuration = {
        name: all_config[name]
        for name in MYSQL_CONFIG_NAMES
        if name in all_config
    }
    identity_rows = collector.query(
        "identity",
        """
        SELECT DATABASE() AS database_name,
               CURRENT_USER() AS `current_user`,
               VERSION() AS server_version;
        """,
    )
    processlist = collector.query(
        "processlist",
        """
        SELECT ID, USER, DB, COMMAND, TIME, STATE
        FROM information_schema.PROCESSLIST
        WHERE ID <> CONNECTION_ID();
        """,
    )
    transactions = collector.query(
        "transactions",
        """
        SELECT trx_mysql_thread_id AS thread_id,
               trx_state,
               TIMESTAMPDIFF(SECOND, trx_started, NOW()) AS age_seconds,
               trx_rows_locked AS rows_locked,
               trx_rows_modified AS rows_modified
        FROM information_schema.INNODB_TRX;
        """,
    )
    lock_waits = collector.query(
        "lock_waits",
        "SELECT count(*) AS blocked_sessions FROM performance_schema.data_lock_waits;",
    )
    replication = collector.query("replication", "SHOW REPLICA STATUS;")

    max_connections = int(all_config.get("max_connections") or 0)
    used_connections = len(processlist)
    long_running = sum(
        1
        for row in processlist
        if str(row.get("COMMAND") or "").lower() != "sleep"
        and int(row.get("TIME") or 0) > thresholds["long_running_query_seconds"]
    )
    idle_in_transaction = sum(
        1
        for row in transactions
        if int(row.get("age_seconds") or 0) > thresholds["idle_in_transaction_seconds"]
        and str(row.get("trx_state") or "").upper() != "COMMITTED"
    )
    runtime = {
        "available": bool(identity_rows),
        "blocked_sessions": (
            int(lock_waits[0].get("blocked_sessions") or 0) if lock_waits else None
        ),
        "idle_in_transaction_sessions": idle_in_transaction,
        "long_running_queries": long_running,
        "used_connections": used_connections,
        "max_connections": max_connections,
        "connection_saturation_ratio": (
            round(used_connections / max_connections, 4)
            if max_connections > 0
            else None
        ),
        "current_waits": _sort_records(
            {
                "state": row.get("STATE") or "none",
                "seconds": int(row.get("TIME") or 0),
            }
            for row in processlist
            if row.get("STATE")
        ),
        "transactions": _sort_records(transactions),
        "replication": {
            "configured": bool(replication),
            "channels": _sort_records(replication),
        },
    }
    durable = {
        "available": bool(identity_rows),
        "identity": identity_rows[0] if identity_rows else {},
        "schema": schema,
        "tables": _sort_records(tables),
        "indexes": _sort_records(indexes),
        "constraints": _sort_records(constraints),
        "triggers": _sort_records(triggers),
        "routines": _sort_records(routines),
        "events": _sort_records(events),
        "configuration": configuration,
        "invalid_indexes": [],
        "high_dead_tuple_tables": [],
        "statistics_issues": [],
    }
    durable["schema_fingerprint"] = _stable_hash(
        {
            "tables": durable["tables"],
            "indexes": durable["indexes"],
            "constraints": durable["constraints"],
            "triggers": durable["triggers"],
            "routines": durable["routines"],
            "events": durable["events"],
        }
    )
    cursor.close()
    return durable, runtime, collector.errors


def _run_command(command: Sequence[str], timeout: int = 10) -> Tuple[Optional[str], Optional[str]]:
    try:
        result = subprocess.run(
            list(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, " ".join(str(exc).split())
    if result.returncode != 0:
        return None, " ".join((result.stderr or result.stdout).split())
    return result.stdout.strip(), None


def _parse_percent(value: Any) -> Optional[float]:
    match = re.search(r"([0-9.]+)", str(value or ""))
    return round(float(match.group(1)) / 100.0, 4) if match else None


def _container_snapshot(container_name: Optional[str], deadline: Optional[float] = None) -> Dict[str, Any]:
    if not container_name:
        return {
            "available": False,
            "reason": "container_name was not supplied",
        }

    def run(command: Sequence[str], timeout: int = 10):
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None, "health collection budget exhausted"
            timeout = min(timeout, remaining)
        return _run_command(command, timeout=timeout)

    errors: List[str] = []
    state_text, error = run(
        ["docker", "inspect", "--format", "{{json .State}}", container_name]
    )
    if error:
        errors.append(f"inspect: {error}")
    stats_text, error = run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", container_name],
        timeout=20,
    )
    if error:
        errors.append(f"stats: {error}")
    top_text, error = run(
        ["docker", "top", container_name, "-eo", "comm,args"]
    )
    if error:
        errors.append(f"top: {error}")
    disk_text, error = run(
        [
            "docker", "exec", container_name, "sh", "-lc",
            "df -Pk / | awk 'NR==2 {print $2,$3,$4,$5}'",
        ]
    )
    if error:
        errors.append(f"disk: {error}")
    limits_text, error = run(
        ["docker", "exec", container_name, "sh", "-lc", "ulimit -n; ulimit -u"]
    )
    if error:
        errors.append(f"limits: {error}")

    try:
        state = json.loads(state_text) if state_text else {}
    except json.JSONDecodeError:
        state = {}
        errors.append("inspect: invalid JSON")
    try:
        stats = json.loads(stats_text) if stats_text else {}
    except json.JSONDecodeError:
        stats = {}
        errors.append("stats: invalid JSON")

    processes = []
    if top_text:
        for line in top_text.splitlines()[1:]:
            parts = line.strip().split(None, 1)
            if not parts:
                continue
            processes.append(
                {
                    "command": parts[0],
                    "arguments": re.sub(r"\s+", " ", parts[1] if len(parts) > 1 else ""),
                }
            )

    disk = {}
    if disk_text:
        values = disk_text.split()
        if len(values) >= 4:
            disk = {
                "total_kb": int(values[0]),
                "used_kb": int(values[1]),
                "available_kb": int(values[2]),
                "used_ratio": _parse_percent(values[3]),
            }
    limits = {}
    if limits_text:
        values = limits_text.splitlines()
        if values:
            limits["open_files"] = values[0].strip()
        if len(values) > 1:
            limits["processes"] = values[1].strip()

    return {
        "available": bool(state),
        "state": {
            "running": state.get("Running"),
            "paused": state.get("Paused"),
            "restarting": state.get("Restarting"),
            "oom_killed": state.get("OOMKilled"),
            "dead": state.get("Dead"),
            "exit_code": state.get("ExitCode"),
        },
        "resources": {
            "cpu_used_ratio": _parse_percent(stats.get("CPUPerc")),
            "memory_used_ratio": _parse_percent(stats.get("MemPerc")),
            "pids": stats.get("PIDs"),
            "block_io": stats.get("BlockIO"),
            "network_io": stats.get("NetIO"),
            "disk": disk,
            "limits": limits,
        },
        "processes": _sort_records(processes),
        "errors": errors,
    }


def capture_database_state_snapshot(
    db_config: Mapping[str, Any],
    logger: Any = None,
    thresholds: Optional[Mapping[str, Any]] = None,
    connect_timeout: int = 10,
    container_name: Optional[str] = None,
    *,
    health_budget: Optional[Mapping[str, Any]] = None,
    checkpoint: Any = None,
) -> Dict[str, Any]:
    """Capture a normalized, read-only PostgreSQL or MySQL state snapshot."""
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    if thresholds:
        active_thresholds.update(thresholds)

    dbms = normalize_dbms(db_config.get("dbms"))
    bounded = dbms == "postgresql" and health_budget is not None
    deadline = time.monotonic() + health_budget["total_seconds"] if bounded else None
    params = _resolve_connection_params(db_config, dbms)
    schema = str(
        db_config.get("schema")
        or db_config.get("db_schema")
        or (params["database"] if dbms == "mysql" else "public")
    )
    snapshot = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "captured_at": _utc_now(),
        "dbms": dbms,
        "database": params["database"],
        "schema": schema,
        "collection_status": "pending",
        "policy": {
            "verdict_scope": "current operational state",
            "historical_signals_excluded": list(HISTORICAL_SIGNALS_EXCLUDED),
        },
        "layers": {
            "container": {"available": False} if bounded else _container_snapshot(container_name),
            "durable": {"available": False},
            "runtime": {"available": False},
            "functional": {
                "available": False,
                "simple_query_succeeded": False,
            },
        },
        "collection_errors": [],
    }
    budget = None
    if bounded:
        snapshot["collection_strategy"] = "bounded_pg_v1"
        snapshot["probe_status"] = {
            name: {"status": "not_run"} for name in (
                "identity", "runtime_activity", "blocking_sessions", "tables", "table_sizes",
                "indexes", "constraints", "routines", "extensions", "configuration",
                "database_size", "current_waits", "prepared_transactions", "replication",
            )
        }
        budget = {
            "deadline": deadline, "statement_ms": health_budget["statement_ms"],
            "lock_ms": health_budget["lock_ms"], "rows": {},
            "probe_status": snapshot["probe_status"],
        }

        def publish():
            rows = budget["rows"]
            identity = rows.get("identity", [])
            runtime = dict((rows.get("runtime_activity") or [{}])[0])
            runtime.update({
                "available": snapshot["probe_status"]["runtime_activity"]["status"] == "ok",
                "current_waits": rows.get("current_waits", []),
                "blocking_sessions": rows.get("blocking_sessions", []),
                "replication": (rows.get("replication") or [{}])[0],
                "prepared_transactions": (rows.get("prepared_transactions") or [{}])[0].get("prepared_transactions"),
            })
            snapshot["layers"]["runtime"] = runtime
            snapshot["layers"]["durable"] = {
                "available": bool(identity), "identity": identity[0] if identity else {},
                "schema": schema, "tables": rows.get("tables", []),
                "indexes": rows.get("indexes", []), "constraints": rows.get("constraints", []),
                "routines": rows.get("routines", []), "extensions": rows.get("extensions", []),
                "configuration": {}, "invalid_indexes": [
                    row.get("index_name") for row in rows.get("indexes", [])
                    if not row.get("is_valid") or not row.get("is_ready")
                ],
                "high_dead_tuple_tables": [], "statistics_issues": [],
            }
            snapshot["layers"]["functional"] = {
                "available": bool(identity), "simple_query_succeeded": bool(identity),
                "schema_accessible": None,
            }
            snapshot["collection_status"] = "partial"
            snapshot["collection_errors"] = [
                f"{name}: {info['status']}" for name, info in snapshot["probe_status"].items()
                if info["status"] != "ok"
            ]
            if checkpoint is not None:
                checkpoint(snapshot)

        budget["publish"] = publish
        publish()

    dependency = psycopg2 if dbms == "postgresql" else mysql_connector
    if dependency is None:
        message = (
            "psycopg2 is not available"
            if dbms == "postgresql"
            else "mysql-connector-python is not available"
        )
        snapshot["collection_errors"].append(message)
        snapshot["collection_status"] = "dependency_unavailable"
        if logger is not None:
            logger.warning(f"[ANOMALY JUDGE] {message}; snapshot is unavailable.")
        return snapshot

    connection = None
    collection_started = time.perf_counter()
    try:
        if dbms == "postgresql":
            options = {}
            if bounded:
                options = {"options": (
                    f"-c statement_timeout={health_budget['statement_ms']} "
                    f"-c lock_timeout={health_budget['lock_ms']} "
                    "-c default_transaction_read_only=on"
                ), "application_name": "db_diag_health_probe"}
            connection = psycopg2.connect(
                host=params["host"],
                port=params["port"],
                user=params["user"],
                password=params["password"],
                dbname=params["database"],
                connect_timeout=connect_timeout,
                **options,
            )
            connection.autocommit = True
            if bounded:
                durable, runtime, errors = _postgres_snapshot(connection, schema, active_thresholds, budget)
            else:
                durable, runtime, errors = _postgres_snapshot(connection, schema, active_thresholds)
        else:
            connection = mysql_connector.connect(
                host=params["host"],
                port=params["port"],
                user=params["user"],
                password=params["password"],
                database=params["database"],
                connection_timeout=connect_timeout,
                use_pure=True,
                autocommit=True,
            )
            durable, runtime, errors = _mysql_snapshot(
                connection, schema, active_thresholds
            )
        snapshot["layers"]["durable"] = durable
        snapshot["layers"]["runtime"] = runtime
        if bounded:
            snapshot["derived_checks_complete"] = True
        snapshot["layers"]["functional"] = {
            "available": bool(durable.get("available") and runtime.get("available")),
            "simple_query_succeeded": bool(
                durable.get("available") and runtime.get("available")
            ),
            "schema_accessible": bool(durable.get("available")),
            "probe_collection_duration_ms": round(
                (time.perf_counter() - collection_started) * 1000.0, 3
            ),
        }
        if bounded:
            snapshot["collection_errors"] = []
        snapshot["collection_errors"].extend(errors)
        snapshot["collection_status"] = "partial" if errors else "complete"
    except Exception as exc:
        message = (f"database snapshot could not be collected: {type(exc).__name__}"
                   if bounded else f"database snapshot could not be collected: {' '.join(str(exc).split())}")
        snapshot["collection_errors"].append(message)
        snapshot["collection_status"] = "partial" if bounded else "connection_failed"
        if logger is not None:
            logger.warning(f"[ANOMALY JUDGE] {message}")
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
    if bounded:
        if checkpoint is not None:
            checkpoint(snapshot)
        snapshot["layers"]["container"] = _container_snapshot(container_name, deadline)
        if snapshot["layers"]["container"].get("errors"):
            snapshot["collection_errors"].extend(
                "container: " + error for error in snapshot["layers"]["container"]["errors"]
            )
            snapshot["collection_status"] = "partial"
        if checkpoint is not None:
            checkpoint(snapshot)
    return snapshot


def _add_check(
    checks: List[Dict[str, Any]],
    check_id: str,
    status: str,
    critical: bool,
    summary: str,
    evidence: Any = None,
) -> None:
    check = {
        "id": check_id,
        "status": status,
        "critical": bool(critical and status == "fail"),
        "summary": summary,
    }
    if evidence is not None:
        check["evidence"] = evidence
    checks.append(check)


def evaluate_current_state_snapshot(
    snapshot: Mapping[str, Any],
    thresholds: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Judge one current snapshot without anomaly or baseline context."""
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    if thresholds:
        active_thresholds.update(thresholds)

    checks: List[Dict[str, Any]] = []
    layers = snapshot.get("layers", {})
    durable = layers.get("durable", {})
    runtime = layers.get("runtime", {})
    container = layers.get("container", {})
    functional = layers.get("functional", {})
    collection_status = snapshot.get("collection_status")

    available = bool(durable.get("available") and runtime.get("available"))
    probe_unavailable = collection_status in {
        "dependency_unavailable",
        "invalid_input",
    }
    _add_check(
        checks,
        "connectivity",
        "pass" if available else ("unknown" if probe_unavailable else "fail"),
        not probe_unavailable,
        "Database accepts read-only monitoring queries."
        if available
        else "Database state could not be queried.",
        snapshot.get("collection_errors"),
    )

    if container.get("available"):
        state = container.get("state", {})
        running = bool(state.get("running")) and not any(
            state.get(key) for key in ("paused", "restarting", "oom_killed", "dead")
        )
        _add_check(
            checks,
            "container_service",
            "pass" if running else "fail",
            True,
            "Container service is running normally." if running else "Container service is not in a runnable state.",
            state,
        )
    else:
        _add_check(
            checks,
            "container_service",
            "unknown",
            False,
            "Container-level evidence was not requested or unavailable.",
        )

    functional_available = bool(functional.get("simple_query_succeeded"))
    _add_check(
        checks,
        "functional_probe",
        "pass" if functional_available else (
            "unknown" if probe_unavailable else "fail"
        ),
        not probe_unavailable,
        "Read-only functional probe completed successfully."
        if functional_available
        else "Read-only functional probe did not complete.",
        functional,
    )
    collection_errors = snapshot.get("collection_errors") or []
    _add_check(
        checks,
        "collection_completeness",
        "unknown" if probe_unavailable else (
            "warn" if collection_errors else "pass"
        ),
        False,
        "All configured probes completed."
        if not collection_errors
        else "Some optional probes could not be collected.",
        collection_errors,
    )

    table_count = len(durable.get("tables") or [])
    _add_check(
        checks,
        "schema_access",
        "unknown" if not durable.get("available") else (
            "warn" if table_count == 0 else "pass"
        ),
        False,
        f"Visible schema objects: {table_count}.",
    )

    invalid_indexes = durable.get("invalid_indexes") or []
    _add_check(
        checks,
        "index_integrity",
        "fail" if invalid_indexes else "pass",
        bool(invalid_indexes),
        "No invalid indexes are visible."
        if not invalid_indexes
        else "Invalid indexes are currently visible.",
        invalid_indexes,
    )

    statistics_issues = durable.get("statistics_issues") or []
    _add_check(
        checks,
        "optimizer_statistics",
        "warn" if statistics_issues else "pass",
        False,
        "Planner statistics have usable row estimates."
        if not statistics_issues
        else "Some tables do not expose usable planner row estimates.",
        statistics_issues,
    )

    high_dead_tables = durable.get("high_dead_tuple_tables") or []
    _add_check(
        checks,
        "mvcc_table_health",
        "warn" if high_dead_tables else "pass",
        False,
        "No currently excessive dead-tuple ratio is visible."
        if not high_dead_tables
        else "Tables with a high current dead-tuple ratio are visible.",
        high_dead_tables,
    )

    blocked = runtime.get("blocked_sessions")
    blocked_count = int(blocked or 0)
    blocked_status = (
        "unknown" if blocked is None
        else "fail" if blocked_count >= active_thresholds["blocked_sessions_critical"]
        else "warn" if blocked_count > 0
        else "pass"
    )
    _add_check(
        checks,
        "lock_contention",
        blocked_status,
        blocked_status == "fail",
        f"Current blocked sessions: {blocked}.",
    )

    idle = runtime.get("idle_in_transaction_sessions")
    idle_count = int(idle or 0)
    idle_status = (
        "unknown" if idle is None
        else "fail" if idle_count >= active_thresholds["idle_transactions_critical"]
        else "warn" if idle_count > 0
        else "pass"
    )
    _add_check(
        checks,
        "idle_transactions",
        idle_status,
        idle_status == "fail",
        f"Current over-threshold idle transactions: {idle}.",
    )

    long_running = runtime.get("long_running_queries")
    long_count = int(long_running or 0)
    long_status = (
        "unknown" if long_running is None
        else "fail" if long_count >= active_thresholds["long_running_queries_critical"]
        else "warn" if long_count > 0
        else "pass"
    )
    _add_check(
        checks,
        "long_running_queries",
        long_status,
        long_status == "fail",
        f"Current over-threshold active queries: {long_running}.",
    )

    saturation = runtime.get("connection_saturation_ratio")
    saturation_value = float(saturation) if saturation is not None else None
    saturation_status = (
        "unknown" if saturation_value is None
        else "fail"
        if saturation_value >= active_thresholds["connection_saturation_ratio"]
        else "warn"
        if saturation_value >= active_thresholds["connection_warning_ratio"]
        else "pass"
    )
    _add_check(
        checks,
        "connection_capacity",
        saturation_status,
        saturation_status == "fail",
        f"Current connection saturation ratio: {saturation}.",
    )

    waits = runtime.get("current_waits") or []
    contention_sessions = sum(
        int(row.get("sessions") or 1)
        for row in waits
        if any(
            token in str(row.get("wait_type") or row.get("state") or "").lower()
            for token in ("lock", "latch", "buffer", "io")
        )
    )
    contention_status = (
        "warn"
        if contention_sessions >= active_thresholds["contention_session_count"]
        else "pass"
    )
    _add_check(
        checks,
        "current_wait_contention",
        contention_status,
        False,
        f"Current contention-related waiting sessions: {contention_sessions}.",
        waits,
    )

    replication = runtime.get("replication") or {}
    replication_configured = bool(
        replication.get("configured")
        or int(replication.get("sender_count") or 0) > 0
        or replication.get("channels")
    )
    replication_failures = []
    replication_lag = 0.0
    if replication_configured:
        try:
            replication_lag = float(
                replication.get("max_replay_lag_seconds") or 0
            )
        except (TypeError, ValueError):
            replication_lag = 0.0
        for channel in replication.get("channels") or []:
            io_state = str(
                channel.get("Replica_IO_Running")
                or channel.get("Slave_IO_Running")
                or ""
            ).lower()
            sql_state = str(
                channel.get("Replica_SQL_Running")
                or channel.get("Slave_SQL_Running")
                or ""
            ).lower()
            if io_state and io_state != "yes":
                replication_failures.append("replica I/O thread is not running")
            if sql_state and sql_state != "yes":
                replication_failures.append("replica SQL thread is not running")
            lag_value = (
                channel.get("Seconds_Behind_Source")
                if channel.get("Seconds_Behind_Source") is not None
                else channel.get("Seconds_Behind_Master")
            )
            try:
                replication_lag = max(replication_lag, float(lag_value or 0))
            except (TypeError, ValueError):
                pass
    if replication_lag > active_thresholds["replication_lag_seconds"]:
        replication_failures.append(
            f"replication lag is {replication_lag}s"
        )
    _add_check(
        checks,
        "replication",
        "fail" if replication_failures else "pass",
        bool(replication_failures),
        "Replication is not configured for this instance."
        if not replication_configured
        else "Configured replication has no current failure."
        if not replication_failures
        else "A current replication failure remains.",
        replication_failures,
    )

    prepared = runtime.get("prepared_transactions")
    _add_check(
        checks,
        "prepared_transactions",
        "warn" if prepared is not None and int(prepared) > 0 else (
            "unknown" if prepared is None else "pass"
        ),
        False,
        f"Current unresolved prepared transactions: {prepared}.",
    )

    resources = container.get("resources", {})
    disk_ratio = resources.get("disk", {}).get("used_ratio")
    disk_status = (
        "unknown" if disk_ratio is None
        else "fail" if disk_ratio >= active_thresholds["disk_used_ratio"]
        else "warn" if disk_ratio >= active_thresholds["disk_warning_ratio"]
        else "pass"
    )
    _add_check(
        checks,
        "storage_capacity",
        disk_status,
        disk_status == "fail",
        f"Current root filesystem used ratio: {disk_ratio}.",
    )
    for check_id, key, threshold_key in (
        ("memory_pressure", "memory_used_ratio", "memory_used_ratio"),
        ("cpu_pressure", "cpu_used_ratio", "cpu_used_ratio"),
    ):
        ratio = resources.get(key)
        status = (
            "unknown" if ratio is None
            else "fail" if ratio >= active_thresholds[threshold_key]
            else "warn" if ratio >= active_thresholds["resource_warning_ratio"]
            else "pass"
        )
        _add_check(
            checks,
            check_id,
            status,
            status == "fail",
            f"Current {check_id.replace('_', ' ')} ratio: {ratio}.",
        )

    probe_status = snapshot.get("probe_status")
    if probe_status is not None:
        dependencies = {
            "connectivity": ("identity",), "functional_probe": ("identity",),
            "schema_access": ("tables",), "index_integrity": ("indexes",),
            "optimizer_statistics": ("tables",), "mvcc_table_health": ("tables",),
            "lock_contention": ("runtime_activity",), "idle_transactions": ("runtime_activity",),
            "long_running_queries": ("runtime_activity",),
            "connection_capacity": ("runtime_activity", "configuration"),
            "current_wait_contention": ("current_waits",),
            "replication": ("replication",), "prepared_transactions": ("prepared_transactions",),
        }
        for check in checks:
            missing = [name for name in dependencies.get(check["id"], ())
                       if probe_status.get(name, {}).get("status") != "ok"]
            if check["id"] in ("optimizer_statistics", "mvcc_table_health") and not snapshot.get("derived_checks_complete"):
                missing.append("derived_table_checks")
            if missing:
                check.update(status="unknown", critical=False,
                             summary="Required probe evidence unavailable: " + ", ".join(missing),
                             evidence={"missing_probes": missing})
    critical_findings = [check for check in checks if check["critical"]]
    observations = [
        check for check in checks if check["status"] in {"warn", "unknown"}
    ]
    observed_checks = [check for check in checks if check["status"] != "unknown"]
    confidence = round(len(observed_checks) / max(len(checks), 1), 3)
    if probe_unavailable:
        healthy: Optional[bool] = None
        operational_status = "indeterminate"
    elif critical_findings:
        healthy = False
        operational_status = "unhealthy"
    elif probe_status is not None and any(
        check["status"] == "unknown" for check in checks
        if check["id"] not in ("container_service", "storage_capacity", "memory_pressure", "cpu_pressure")
    ):
        healthy = None
        operational_status = "indeterminate"
    elif any(check["status"] == "warn" for check in checks):
        healthy = True
        operational_status = "degraded"
    else:
        healthy = True
        operational_status = "operational"

    checks_by_id = {check["id"]: check for check in checks}

    def layer_verdict(check_ids: Sequence[str]) -> Optional[bool]:
        selected = [
            checks_by_id[check_id]
            for check_id in check_ids
            if check_id in checks_by_id
        ]
        if any(check["critical"] for check in selected):
            return False
        if probe_status is not None and any(check["status"] == "unknown" for check in selected):
            return None
        if not selected or all(check["status"] == "unknown" for check in selected):
            return None
        return True

    layer_verdicts = {
        "container_state_clean": layer_verdict(
            (
                "container_service",
                "storage_capacity",
                "memory_pressure",
                "cpu_pressure",
            )
        ) if container.get("available") else None,
        "durable_state_acceptable": layer_verdict(
            (
                "schema_access",
                "index_integrity",
                "optimizer_statistics",
                "mvcc_table_health",
            )
        ) if durable.get("available") else None,
        "runtime_state_clean": layer_verdict(
            (
                "connectivity",
                "lock_contention",
                "idle_transactions",
                "long_running_queries",
                "connection_capacity",
                "current_wait_contention",
                "replication",
                "prepared_transactions",
            )
        ) if runtime.get("available") else None,
        "functional_probe_passed": (
            layer_verdict(("functional_probe",))
            if functional.get("available")
            else None
        ),
    }

    report = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "healthy": healthy,
        "operational_status": operational_status,
        "confidence": confidence,
        "dbms": snapshot.get("dbms"),
        "database": snapshot.get("database"),
        "layer_verdicts": layer_verdicts,
        "anomalies": [check["summary"] for check in critical_findings],
        "metrics": runtime,
        "critical_findings": critical_findings,
        "observations": observations,
        "checks": checks,
        "current_snapshot": snapshot,
        "policy": {
            "historical_signals_excluded": list(HISTORICAL_SIGNALS_EXCLUDED),
            "verdict": (
                "all low-cost read-only probes run on every invocation; "
                "only current operational signals affect health"
            ),
        },
    }
    if probe_status is not None:
        report["collection_status"] = snapshot.get("collection_status", "partial")
        report["probe_status"] = probe_status
    return report


def _report_check_map(report: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    return {
        str(check.get("id")): check
        for check in report.get("checks", [])
        if check.get("id")
    }


def _comparison_metrics(report: Mapping[str, Any]) -> Dict[str, Any]:
    snapshot_layers = report.get("current_snapshot", {}).get("layers", {})
    runtime = snapshot_layers.get("runtime", {})
    durable = snapshot_layers.get("durable", {})
    resources = snapshot_layers.get("container", {}).get("resources", {})
    functional = snapshot_layers.get("functional", {})
    metrics = {
        "blocked_sessions": runtime.get("blocked_sessions"),
        "idle_in_transaction_sessions": runtime.get(
            "idle_in_transaction_sessions"
        ),
        "long_running_queries": runtime.get("long_running_queries"),
        "connection_saturation_ratio": runtime.get(
            "connection_saturation_ratio"
        ),
        "cpu_used_ratio": resources.get("cpu_used_ratio"),
        "memory_used_ratio": resources.get("memory_used_ratio"),
        "disk_used_ratio": resources.get("disk", {}).get("used_ratio"),
        "schema_object_count": len(durable.get("tables") or []),
        "index_count": len(durable.get("indexes") or []),
        "probe_collection_duration_ms": functional.get(
            "probe_collection_duration_ms"
        ),
    }
    probes = report.get("current_snapshot", {}).get("probe_status")
    if probes is not None:
        for key, probe in (("schema_object_count", "tables"), ("index_count", "indexes")):
            if probes.get(probe, {}).get("status") != "ok":
                metrics[key] = None
    return metrics


def compare_database_health_reports(
    initial_report: Mapping[str, Any],
    recovered_report: Mapping[str, Any],
) -> Dict[str, Any]:
    """Compare two independently captured current-state health reports.

    Recovery succeeds only when the initialized baseline was valid, the
    post-recovery instance is operational, and no new critical check appears.
    Warning-level metric drift is retained as evidence but is not a hard failure.
    """
    same_target = (
        initial_report.get("dbms") == recovered_report.get("dbms")
        and initial_report.get("database") == recovered_report.get("database")
    )
    initial_available = initial_report.get("healthy") is not None
    recovered_available = recovered_report.get("healthy") is not None
    comparison_available = bool(
        same_target and initial_available and recovered_available
    )
    bounded_comparison = any(
        r.get("current_snapshot", {}).get("probe_status") is not None
        for r in (initial_report, recovered_report)
    )
    if bounded_comparison and any(
        r.get("current_snapshot", {}).get("collection_status") != "complete"
        for r in (initial_report, recovered_report)
    ):
        comparison_available = False

    initial_checks = _report_check_map(initial_report)
    recovered_checks = _report_check_map(recovered_report)
    initial_critical_ids = {
        check_id
        for check_id, check in initial_checks.items()
        if check.get("critical")
    }
    recovered_critical_ids = {
        check_id
        for check_id, check in recovered_checks.items()
        if check.get("critical")
    }
    new_critical_ids = sorted(recovered_critical_ids - initial_critical_ids)
    resolved_critical_ids = sorted(initial_critical_ids - recovered_critical_ids)
    if bounded_comparison:
        resolved_critical_ids = [
            key for key in resolved_critical_ids
            if recovered_checks.get(key, {}).get("status") in ("pass", "warn")
        ]

    severity = {"pass": 0, "warn": 1, "fail": 2}
    check_changes = []
    warning_regressions = []
    for check_id in sorted(set(initial_checks) | set(recovered_checks)):
        before = initial_checks.get(check_id, {})
        after = recovered_checks.get(check_id, {})
        before_status = before.get("status", "missing")
        after_status = after.get("status", "missing")
        if before_status == after_status:
            continue
        change = {
            "id": check_id,
            "initial_status": before_status,
            "recovered_status": after_status,
            "initial_summary": before.get("summary"),
            "recovered_summary": after.get("summary"),
        }
        check_changes.append(change)
        if (
            before_status in severity
            and after_status in severity
            and severity[after_status] > severity[before_status]
            and after_status != "fail"
        ):
            warning_regressions.append(change)

    initial_metrics = _comparison_metrics(initial_report)
    recovered_metrics = _comparison_metrics(recovered_report)
    metric_changes = {}
    for name in sorted(set(initial_metrics) | set(recovered_metrics)):
        before = initial_metrics.get(name)
        after = recovered_metrics.get(name)
        change = {"initial": before, "recovered": after}
        if (
            isinstance(before, (int, float))
            and not isinstance(before, bool)
            and isinstance(after, (int, float))
            and not isinstance(after, bool)
        ):
            change["delta"] = round(float(after) - float(before), 6)
        metric_changes[name] = change

    initial_environment_acceptable = initial_report.get("healthy") is True
    recovered_operational = recovered_report.get("healthy") is True
    recovery_successful = bool(
        comparison_available
        and initial_environment_acceptable
        and recovered_operational
        and not new_critical_ids
    )

    if not comparison_available:
        comparison_status = "indeterminate"
    elif not initial_environment_acceptable:
        comparison_status = "invalid_initial_state"
    elif recovery_successful:
        comparison_status = "recovered"
    else:
        comparison_status = "not_recovered"

    new_critical_findings = [
        recovered_checks[check_id]
        for check_id in new_critical_ids
        if check_id in recovered_checks
    ]
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "healthy": recovery_successful if comparison_available else None,
        "recovery_successful": recovery_successful,
        "comparison_status": comparison_status,
        "comparison_available": comparison_available,
        "same_target": same_target,
        "dbms": recovered_report.get("dbms"),
        "database": recovered_report.get("database"),
        "initial_environment_acceptable": initial_environment_acceptable,
        "recovered_operational": recovered_operational,
        "new_critical_findings": new_critical_findings,
        "resolved_critical_check_ids": resolved_critical_ids,
        "warning_regressions": warning_regressions,
        "check_changes": check_changes,
        "metric_changes": metric_changes,
        "anomalies": [
            finding.get("summary") for finding in new_critical_findings
        ],
        "initial_health_report": initial_report,
        "recovered_health_report": recovered_report,
        "policy": {
            "requires_same_target": True,
            "initial_report_must_be_healthy": True,
            "recovered_report_must_be_healthy": True,
            "new_critical_failures_allowed": False,
            "warning_level_drift_is_informational": True,
        },
    }


def judge_database_health(
    db_config: Mapping[str, Any],
    logger: Any = None,
    thresholds: Optional[Mapping[str, Any]] = None,
    connect_timeout: int = 10,
    container_name: Optional[str] = None,
    *,
    health_budget: Optional[Mapping[str, Any]] = None,
    checkpoint: Any = None,
) -> Dict[str, Any]:
    """Capture and judge the current instance with no prerequisite state."""
    try:
        extra = {"health_budget": health_budget, "checkpoint": checkpoint} if health_budget is not None else {}
        snapshot = capture_database_state_snapshot(
            db_config,
            logger=logger,
            thresholds=thresholds,
            connect_timeout=connect_timeout,
            container_name=container_name,
            **extra,
        )
    except (TypeError, ValueError) as exc:
        if logger is not None:
            logger.warning(f"[ANOMALY JUDGE] Invalid health-probe input: {exc}")
        snapshot = {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "captured_at": _utc_now(),
            "collection_status": "invalid_input",
            "collection_errors": [str(exc)],
            "layers": {
                "container": {"available": False},
                "durable": {"available": False},
                "runtime": {"available": False},
                "functional": {"available": False},
            },
        }
    return evaluate_current_state_snapshot(snapshot, thresholds=thresholds)


def write_database_state_report(report: Mapping[str, Any], output_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2, ensure_ascii=False, default=str)


def read_database_state_report(input_path: str) -> Dict[str, Any]:
    with open(input_path, "r", encoding="utf-8") as file:
        report = json.load(file)
    if not isinstance(report, dict):
        raise ValueError(f"Health report must be a JSON object: {input_path}")
    return report


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a current PostgreSQL/MySQL container health report and "
            "optionally compare it with an initialized-state report."
        )
    )
    parser.add_argument(
        "--dbms", required=True, choices=("postgresql", "pg", "mysql"),
        help="Target DBMS",
    )
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", type=int, help="Database host-mapped port")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--schema", help="PostgreSQL schema; defaults to public")
    parser.add_argument("--container", required=True, help="Docker container name")
    parser.add_argument(
        "--initial-report",
        help="Optional initialized-state health report to compare with the current state",
    )
    parser.add_argument(
        "--output",
        default="database_state_snapshot.json",
        help="Output JSON path",
    )
    parser.add_argument(
        "--connect-timeout", type=int, default=10,
        help="Connection timeout in seconds",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    db_config = {
        "dbms": args.dbms,
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "db_name": args.database,
        "schema": args.schema,
    }
    current_report = judge_database_health(
        db_config,
        connect_timeout=args.connect_timeout,
        container_name=args.container,
    )
    if args.initial_report:
        initial_report = read_database_state_report(args.initial_report)
        output = compare_database_health_reports(initial_report, current_report)
    else:
        output = current_report
    exit_code = 2 if output["healthy"] is None else (0 if output["healthy"] else 1)

    write_database_state_report(output, args.output)
    print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
    return exit_code


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
        "schema_version": 1, "captured_at": _utc_now(),
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
    # Reclaim child CLI processes even if the Python parent already exited.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=2)


def collect_bounded(db_config, config, container_name, *, worker_path=None):
    """Collect PostgreSQL health in a bounded, isolated subprocess."""
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
        report = evaluate_current_state_snapshot(snapshot)
        report["health_request_id"] = request_id
        return report


def worker_main():
    request = json.load(sys.stdin)
    request_id = request["request_id"]
    path = Path(request["checkpoint"])
    budget = request["budget"]

    def checkpoint(snapshot, finished=False):
        write_checkpoint(path, {"request_id": request_id, "snapshot": snapshot, "finished": finished})

    snapshot = capture_database_state_snapshot(
        request["db_config"], connect_timeout=budget["connect_seconds"],
        container_name=request["container_name"], health_budget=budget,
        checkpoint=checkpoint,
    )
    checkpoint(snapshot, finished=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["--worker"]:
        worker_main()
    else:
        sys.exit(main())
