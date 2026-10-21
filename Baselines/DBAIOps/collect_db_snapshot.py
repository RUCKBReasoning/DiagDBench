"""Collect a bounded, read-only PostgreSQL or MySQL diagnosis snapshot."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any


def postgres_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.database,
        connect_timeout=10,
        gssencmode="disable",
        application_name="dbaiops_snapshot",
        options="-c default_transaction_read_only=on -c statement_timeout=10000",
    )
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT pid, usename, application_name, state, xact_start,
                       query_start, wait_event_type, wait_event,
                       pg_blocking_pids(pid) AS blocking_pids,
                       left(query, 1000) AS query
                FROM pg_stat_activity
                WHERE datname = current_database() AND pid <> pg_backend_pid()
                ORDER BY query_start NULLS LAST LIMIT 100
                """
            )
            activity = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT version() AS version, current_database() AS database,
                       current_user AS user,
                       pg_database_size(current_database()) AS database_size_bytes
                """
            )
            server = dict(cursor.fetchone())
    finally:
        conn.close()
    return {"server": server, "activity": activity}


def mysql_query(args: argparse.Namespace, sql: str) -> list[dict[str, str]]:
    environment = os.environ.copy()
    environment["MYSQL_PWD"] = args.password
    command = [
            "mysql",
            "--batch",
            "--raw",
            "--connect-timeout=10",
            "--host",
            args.host,
            "--port",
            str(args.port),
            "--user",
            args.user,
            "--database",
            args.database,
            "--execute",
            "SET SESSION TRANSACTION READ ONLY; " + sql,
        ]
    if getattr(args, "local_admin", False):
        environment["MYSQL_PWD"] = args.admin_password
        command = [
            "docker", "exec", "-e", "MYSQL_PWD", args.container_name,
            "mysql", "--protocol=SOCKET", "--batch", "--raw", "--connect-timeout=10",
            "--user", args.admin_user, "--database", args.database, "--execute",
            "SET SESSION TRANSACTION READ ONLY; " + sql,
        ]
    completed = subprocess.run(
        command,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
        timeout=20,
    )
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or "mysql snapshot query failed")
    return list(csv.DictReader(StringIO(completed.stdout), delimiter="\t"))


def mysql_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    server = mysql_query(
        args,
        """
        SELECT VERSION() AS version, DATABASE() AS `database`,
               CURRENT_USER() AS `user`, CONNECTION_ID() AS connection_id;
        """,
    )
    processlist = mysql_query(
        args,
        """
        SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 1000) AS INFO
        FROM information_schema.PROCESSLIST
        WHERE ID <> CONNECTION_ID()
        ORDER BY TIME DESC LIMIT 100;
        """,
    )
    status = mysql_query(
        args,
        """
        SELECT VARIABLE_NAME, VARIABLE_VALUE
        FROM performance_schema.global_status
        WHERE VARIABLE_NAME IN (
            'Threads_connected', 'Threads_running', 'Created_tmp_disk_tables',
            'Created_tmp_tables', 'Innodb_buffer_pool_reads',
            'Innodb_buffer_pool_read_requests', 'Innodb_deadlocks',
            'Aborted_connects'
        )
        ORDER BY VARIABLE_NAME;
        """,
    )
    return {
        "server": server[0] if server else {},
        "processlist": processlist,
        "global_status": status,
    }


def postgres_local_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    # Local peer authentication uses the container's OS postgres account and the
    # reserved database superuser slot. No password/role/session changes.
    sql = """
    SELECT json_build_object(
      'server', json_build_object('version', version(), 'database', current_database(),
          'user', current_user, 'database_size_bytes', pg_database_size(current_database())),
      'activity', COALESCE((SELECT json_agg(a) FROM (
          SELECT pid, usename, application_name, state, xact_start, query_start,
                 wait_event_type, wait_event, pg_blocking_pids(pid) AS blocking_pids,
                 left(query,1000) AS query
          FROM pg_stat_activity
          WHERE datname=current_database() AND pid<>pg_backend_pid()
          ORDER BY query_start NULLS LAST LIMIT 100
      ) a), '[]'::json));
    """
    completed = subprocess.run(
        ["docker", "exec", "-u", "postgres",
         "-e", "PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=10000",
         "-e", "PGCONNECT_TIMEOUT=10", args.container_name,
         "psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
         "-h", "/var/run/postgresql", "-U", "postgres", "-d", args.database, "-c", sql],
        text=True, capture_output=True, check=False, timeout=20,
    )
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or "local PostgreSQL snapshot failed")
    return json.loads(completed.stdout)


def collect_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    """Preserve failed access as diagnostic evidence, then try isolated admin access."""
    errors = []

    def record(stage, exc):
        # Avoid logging subprocess argv (it may contain connection credentials).
        detail = "snapshot command timed out" if isinstance(exc, subprocess.TimeoutExpired) else str(exc)
        for secret in (args.password, getattr(args, "admin_password", "")):
            if secret:
                detail = detail.replace(secret, "<redacted>")
        errors.append({"stage": stage, "error_type": type(exc).__name__, "message": detail[:2000]})

    try:
        details = postgres_snapshot(args) if args.dbms == "pg" else mysql_snapshot(args)
        access = "configured_user"
    except Exception as exc:
        record("configured_user", exc)
        container = getattr(args, "container_name", "")
        if container and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", container):
            raise ValueError("Invalid snapshot container name")
        try:
            if not container:
                raise RuntimeError("Container-local fallback was not configured")
            if args.dbms == "pg":
                details = postgres_local_snapshot(args)
            else:
                local_args = argparse.Namespace(**vars(args))
                local_args.local_admin = True
                details = mysql_snapshot(local_args)
            access = "container_local_admin"
        except Exception as admin_exc:
            record("container_local_admin", admin_exc)
            # An inaccessible database is itself a live observation. Keep it
            # explicit rather than inventing an empty healthy server snapshot.
            details = {}
            access = "unavailable"
    return {
        "mode": "read_only_live_snapshot" if access != "unavailable" else "degraded_live_snapshot",
        "snapshot_access": access,
        "snapshot_complete": access != "unavailable",
        "connection_errors": errors,
        **details,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dbms", choices=("pg", "mysql"), required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", default="")
    parser.add_argument("--database", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--container-name", default=os.getenv("DBAIOPS_SNAPSHOT_CONTAINER", ""))
    parser.add_argument("--admin-user", default=os.getenv("DBAIOPS_SNAPSHOT_ADMIN_USER", "root"))
    parser.add_argument("--admin-password", default=os.getenv("DBAIOPS_SNAPSHOT_ADMIN_PASSWORD", ""))
    args = parser.parse_args()

    details = collect_snapshot(args)
    payload = {
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "dbms": args.dbms,
        "target": {
            "host": args.host,
            "port": args.port,
            "database": args.database,
            "user": args.user,
        },
        **details,
    }
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
