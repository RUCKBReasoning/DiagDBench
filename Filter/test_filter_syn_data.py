#!/usr/bin/env python3
"""Synthetic anomaly filter: V1 pipeline and its runtime dependencies.

This distribution includes the original filtering stages, SQL observations,
Docker helpers, health probes, evaluator programs and database startup/import
scripts. Only installed third-party packages, database images and input data
are external. Historical result filenames and resume semantics are preserved.
Private dependency prefixes avoid shared-global collisions.

Use ``--config Filter/Configs/example_rule.json --dry-run`` after setting the
input/output paths. Relative paths are project-root relative. This rule-based
filter measures database behavior and does not require an LLM configuration.
The optional ``start_id``/``end_id`` interval is inclusive/exclusive.
"""
from __future__ import annotations
from pathlib import Path
import os
import hashlib
import tempfile
from typing import Any, Dict, List, Optional, Sequence

PROJECT_ROOT = Path(os.environ.get("DB_DIAG_PROJECT_ROOT", Path(__file__).resolve().parents[1])).resolve()
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "Filter/Configs/example_rule.json"


# ======================== utils.py ========================
import json as _util_json
import os as _util_os
import time as _util_time
import logging as _util_logging
import re as _util_re
import subprocess as _util_subprocess
from datetime import datetime as _util_datetime

try:
    import sqlparse as _util_sqlparse
except ImportError:
    _util_sqlparse = None

try:
    import psycopg2 as _util_psycopg2
    from psycopg2 import sql as _util_pg_sql
except ImportError:
    _util_psycopg2 = None
    _util_pg_sql = None

try:
    import mysql.connector as _util_mysql_connector
except ImportError:
    _util_mysql_connector = None

_util_logger = _util_logging.getLogger(f"DB_DIAG.{__name__}")

# Connection-establishment timeout (seconds) for all psycopg2 connects below.
# Bounds the "container half-dead / DB unreachable" case so schema extraction
# and related sub-flows fail fast instead of blocking forever.
_util_DB_CONNECT_TIMEOUT = 30
_util_MYSQL_SCHEMA_PROMPT_VALUE = "[MySQL does not involve this definition]"


def _util_normalize_dbms(dbms=None):
    dbms_name = str(dbms or "").strip().lower()
    if "mysql" in dbms_name:
        return "mysql"
    return "pg"


def _util_resolve_db_type(config, dbms=None):
    dbms_kind = _util_normalize_dbms(dbms or config.get("dbms"))
    default_db_type = "load_data" if dbms_kind == "mysql" else "dump"
    db_type = config.get("db_type", config.get("database_type", default_db_type))
    db_type = str(db_type or default_db_type).strip().lower()
    if db_type not in {"dump", "load_data"}:
        raise ValueError(f"Unsupported db_type `{db_type}`. Expected `dump` or `load_data`.")
    return db_type






def _util_set_up_logger(log_path, log_file_level=_util_logging.DEBUG) :
    log_dir = _util_os.path.dirname(log_path)
    if not _util_os.path.exists(log_dir) : _util_os.makedirs(log_dir)
    
    logger = _util_logging.getLogger(f"DB_DIAG")
    logger.setLevel(log_file_level)

    if logger.hasHandlers():
        logger.handlers.clear()
    logger.propagate = False
    
    console_handler = _util_logging.StreamHandler()
    console_handler.setLevel(_util_logging.INFO)

    file_handler = _util_logging.FileHandler(log_path, mode='w')
    file_handler.setLevel(_util_logging.DEBUG)
    
    formatter = _util_logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


    logger = _util_logging.getLogger(f"DB_DIAG.{__name__}")
    logger.info("[UTILS] SET UP LOGGER SUCCESSFULLY.")

    return logger























def _util_resolve_path_from_base(path_value, base_dir):
    if path_value is None:
        return None
    path_value = _util_os.path.expanduser(str(path_value))
    
    if _util_os.path.isabs(path_value):
        return _util_os.path.realpath(path_value)
    return _util_os.path.realpath(_util_os.path.join(base_dir, path_value))


# =============================================================================
# Fast environment-reset helpers (pure, dependency-free).
#
# These support the container-reuse + golden-clone optimization used by
# test_syn_ano.py to avoid tearing down and re-importing a multi-GB database on
# every LLM turn. They are intentionally side-effect free (no docker / no db) so
# they can be unit-tested and reused by the orchestration functions in
# docker_func.py.
# =============================================================================

# Valid reset scopes, ordered from cheapest/least invasive to full rebuild. An
# anomaly may declare its footprint via `ResetScope` in anomaly_description.json;
# the harness picks the cheapest scope that still guarantees an identical
# pre-injection baseline.
_util_RESET_SCOPES = ("SESSION_ONLY", "SCHEMA", "DATA", "FULL")











_ASSET_ROOT = Path(os.environ.get("DB_DIAG_TMP_ROOT", str(PROJECT_ROOT.parent / "TMP"))) / "filter_runtime_0a161ce576c16b44"

# ======================== docker_func.py ========================
import os as _util_os
import subprocess as _util_subprocess
import shlex as _docker_shlex
import time as _util_time
import re as _util_re
# NOTE: `from utils import *` below re-exports utils's `from datetime import
# datetime` (utils has no __all__), which would REBIND a bare `import datetime`
# name to the datetime CLASS and break `datetime.datetime.now()`. Import the
# module under an alias that the star-import cannot shadow.
import datetime as _docker_datetime_module
import logging as _util_logging
import statistics as _docker_statistics



try:
    from psycopg2 import errors as _docker_errors
except ImportError:  # pragma: no cover - optional dependency at orchestration time
    class _docker__Psycopg2ErrorsFallback:
        class QueryCanceled(Exception):
            pass

    _docker_errors = _docker__Psycopg2ErrorsFallback()

_docker_logger = _util_logging.getLogger(f"DB_DIAG.{__name__}")

# Use the canonical on-disk path, not a symlinked alias such as
# ``/home/... -> /data00/home/...``. Docker bind-mounts against the canonical
# host path, and inside the container the mounted destination is that canonical
# path as well. If later commands are constructed with the symlink alias
# (``/home/...``), `docker exec` inside the container cannot find the mounted
# files and database initialization fails with ``No such file or directory``.
_docker_BASE_DIR = str(PROJECT_ROOT)
# Keep the default project-local temporary directory, while allowing a host
# with a separate Docker disk to place container-visible staging files there.
_docker_TMP_ROOT = _util_os.path.realpath(
    _util_os.environ.get("DB_DIAG_TMP_ROOT", _util_os.path.join(_util_os.path.dirname(_docker_BASE_DIR), "TMP"))
)

# Static hang-detection thresholds. These bound the three operations that can
# otherwise block forever (Docker container init, recording merged-workload
# timing, collecting the user observation), turning a silent hang into a
# bounded failure that the shell driver's per-step retry re-runs from the
# failed checkpoint. The execution ceilings are 2x the prior 600s default;
# the connection-establishment timeout is left at its own small value.
_docker_DOCKER_INIT_TIMEOUT = 1200
_docker_SQL_EXEC_TIMEOUT = 1200
_docker_DB_CONNECT_TIMEOUT = 30
# Per-invocation ceiling for a single ``docker rm -f``. Kept separate from
# ``DB_CONNECT_TIMEOUT`` (which only bounds DB connects) so a slow daemon-side
# teardown -- e.g. many concurrent syntheses tearing down quota-backed
# (``--storage-opt size=..``) containers at once -- has enough head-room to
# finish instead of being killed at 30s and retried forever.
_docker_DOCKER_RM_CMD_TIMEOUT = 300
# Overall ceiling for confirming a container is fully removed. High-concurrency
# teardown of large quota-backed containers can take minutes, so this bounds the
# synchronous wait generously; it only blocks this long when a removal genuinely
# never completes.
_docker_DOCKER_REMOVE_TIMEOUT = 1800
_docker_DOCKER_POLL_INTERVAL = 2
# Number of CONSECUTIVE confirmed-absent probes required before a container is
# treated as fully removed. Guards against a name that briefly disappears mid
# transition being mistaken for a completed removal.
_docker_DOCKER_ABSENT_STABLE_CHECKS = 3
# ``docker ps`` bounds / retries. Under heavy parallel load ``docker ps`` itself
# can be slow or transiently fail; retrying (instead of treating one failed
# probe as "container gone") is what prevents a stale container from silently
# keeping its name and causing the next ``docker run`` to hit a 125 conflict.
_docker_DOCKER_PS_TIMEOUT = 60
_docker_DOCKER_PS_RETRIES = 3
_docker_DOCKER_PS_RETRY_INTERVAL = 2
# Length of one confirmation slice inside ``force_remove_container``. If the name
# is not confirmed released within a slice, ``docker rm -f`` is re-issued (covers
# the case where a prior client-side timeout killed the CLI before the daemon
# acted) until the overall ``DOCKER_REMOVE_TIMEOUT`` is reached.
_docker_DOCKER_RM_CONFIRM_SLICE = 60
# Overall ceiling (seconds) for waiting until a target container NAME is free
# before issuing ``docker run``. Even with timestamped unique names a just-torn-
# down name can linger briefly in the daemon's registry; we poll until it is
# confirmed absent, and only give up (reporting failure) after this budget. Set
# per the requirement to 3600s.
_docker_DOCKER_NAME_FREE_TIMEOUT = 3600
# Poll interval (seconds) while waiting for a container name to become free.
_docker_DOCKER_NAME_FREE_POLL_INTERVAL = 10
# Golden-baseline snapshots may need to copy multi-gigabyte database objects
# under concurrent synthesis load. Keep their budget separate from ordinary
# initialization and execution timeouts. Operators can lower or raise this
# independently for a constrained environment.
try:
    _docker_FAST_RESET_SNAPSHOT_TIMEOUT_SECONDS = max(
        60,
        int(_util_os.environ.get("FAST_RESET_SNAPSHOT_TIMEOUT_SECONDS", "21600")),
    )
except ValueError:
    _docker_FAST_RESET_SNAPSHOT_TIMEOUT_SECONDS = 21600
_docker_FAST_RESET_HEARTBEAT_INTERVAL_SECONDS = 600

_docker_create_container_str = f'docker run --cpus={{}} -m {{}}g --memory-swap {{}}g --storage-opt size={{}}G -p {{}}:5432 -v {_docker_BASE_DIR}:{_docker_BASE_DIR} -v {_docker_TMP_ROOT}:{_docker_TMP_ROOT} --name {{}} -d pg_diag'
_docker_init_container_str = f'docker exec {{}} bash {_ASSET_ROOT}/Docker/Ubuntu_PG/init_dbms_postgresql.sh -h {{}} -port {{}} -path {_docker_BASE_DIR} -tmp {_docker_TMP_ROOT}'
_docker_mysql_create_container_str = f'docker run --cpus={{}} -m {{}}g --memory-swap {{}}g --storage-opt size={{}}G -p {{}}:3306 -v {_docker_BASE_DIR}:{_docker_BASE_DIR} -v {_docker_TMP_ROOT}:{_docker_TMP_ROOT} --name {{}} -d mysql_diag'
_docker_mysql_init_container_str = f'docker exec {{}} bash {_ASSET_ROOT}/Docker/Ubuntu_MySQL/init_dbms_mysql.sh -h {{}} -port {{}} -path {_docker_BASE_DIR} -tmp {_docker_TMP_ROOT}'


def _docker__docker_resource_value(value):
    value = str(value).strip()
    for suffix in ("GB", "gb", "G", "g"):
        if value.endswith(suffix):
            return value[:-len(suffix)]
    return value


def _docker_build_container_config(dbms, system_hardware_config, host_port, system_user_config):
    return {
        "dbms": dbms,
        "cpus": _docker__docker_resource_value(system_hardware_config["CPU"]),
        "memory": _docker__docker_resource_value(system_hardware_config["Memory"]),
        "memory_swap": _docker__docker_resource_value(system_hardware_config["Memory + Swap"]),
        "storage": _docker__docker_resource_value(system_hardware_config["Storage"]),
        "host_port": host_port,
        "db_host": system_user_config["DB_HOST"],
        "db_port": system_user_config["DB_PORT"],
    }


def _docker__container_config_values(container_config):
    if isinstance(container_config, dict):
        return (
            _util_normalize_dbms(container_config.get("dbms")),
            container_config["cpus"],
            container_config["memory"],
            container_config["memory_swap"],
            container_config["storage"],
            container_config["host_port"],
            container_config.get("db_host", "localhost"),
            container_config.get("db_port", "5432"),
        )

    cpus, mem_limit, mem_swap, storage_size, port, pg_host, pg_port = container_config
    return "pg", cpus, mem_limit, mem_swap, storage_size, port, pg_host, pg_port


def _docker_build_sql_file_command(dbms, system_user_config, sql_file_path, database_name=None):
    dbms_kind = _util_normalize_dbms(dbms)
    db_name = database_name or system_user_config["DB_NAME"]
    if dbms_kind == "mysql":
        inner_command = (
            f"MYSQL_PWD={_docker_shlex.quote(str(system_user_config['DB_PASSWORD']))} "
            f"mysql -h {_docker_shlex.quote(str(system_user_config['DB_HOST']))} "
            f"-P {_docker_shlex.quote(str(system_user_config['DB_PORT']))} "
            f"-u {_docker_shlex.quote(str(system_user_config['DB_USER']))} "
            f"{_docker_shlex.quote(str(db_name))} < {_docker_shlex.quote(str(sql_file_path))}"
        )
        return ["bash", "-lc", inner_command]

    inner_command = (
        f"PGPASSWORD={_docker_shlex.quote(str(system_user_config['DB_PASSWORD']))} "
        f"psql -h {_docker_shlex.quote(str(system_user_config['DB_HOST']))} "
        f"-p {_docker_shlex.quote(str(system_user_config['DB_PORT']))} "
        f"-U {_docker_shlex.quote(str(system_user_config['DB_USER']))} "
        f"-d {_docker_shlex.quote(str(db_name))} "
        f"-f {_docker_shlex.quote(str(sql_file_path))}"
    )
    return ["bash", "-lc", inner_command]

def _docker_initialize_shared_tmp_directory(base_dir, dbms):
    tmp_root = _docker_TMP_ROOT
    last_error = None
    tmp_image = "mysql_diag" if _util_normalize_dbms(dbms) == "mysql" else "pg_diag"

    try:
        tmp_dbms_dir = _util_os.path.join(tmp_root, dbms)
        tmp_timestamp_dir = _util_os.path.join(tmp_dbms_dir, _util_time.strftime("%Y%m%d_%H%M%S"))
        docker_command = [
            "docker", "run", "--rm",
            "-v", f"{tmp_root}:{tmp_root}",
            tmp_image,
            "bash", "-lc",
            f"mkdir -p {_docker_shlex.quote(tmp_dbms_dir)} {_docker_shlex.quote(tmp_timestamp_dir)} && chmod 1777 {_docker_shlex.quote(tmp_root)} && chmod 777 {_docker_shlex.quote(tmp_dbms_dir)} {_docker_shlex.quote(tmp_timestamp_dir)}"
        ]

        try:
            _util_os.makedirs(tmp_root, exist_ok=True)
        except PermissionError:
            _docker_logger.warning(f"[DOCKER FUNC][WARNING] PermissionError when creating TMP root directory: {tmp_root}. Attempting to prepare permissions via Docker command.")
            _util_subprocess.run(docker_command, check=True, text=True, stdout=_util_subprocess.PIPE, stderr=_util_subprocess.PIPE)
       
        try:
            _util_os.makedirs(tmp_dbms_dir, exist_ok=True)
        except PermissionError:
            _util_subprocess.run(docker_command, check=True, text=True, stdout=_util_subprocess.PIPE, stderr=_util_subprocess.PIPE)
        
        try:
            _util_os.makedirs(tmp_timestamp_dir, exist_ok=True)
        except PermissionError:
            _util_subprocess.run(docker_command, check=True, text=True, stdout=_util_subprocess.PIPE, stderr=_util_subprocess.PIPE)

        if not _util_os.access(tmp_dbms_dir, _util_os.W_OK):
            _util_subprocess.run(docker_command, check=True, text=True, stdout=_util_subprocess.PIPE, stderr=_util_subprocess.PIPE)
        if not _util_os.access(tmp_timestamp_dir, _util_os.W_OK):
            _util_subprocess.run(docker_command, check=True, text=True, stdout=_util_subprocess.PIPE, stderr=_util_subprocess.PIPE)
        if not _util_os.access(tmp_dbms_dir, _util_os.W_OK):
            _docker_logger.error(f"[DOCKER FUNC][ERROR] Shared TMP directory is not writable after permission preparation: {tmp_dbms_dir}")
            exit()
        if not _util_os.access(tmp_timestamp_dir, _util_os.W_OK):
            _docker_logger.error(f"[DOCKER FUNC][ERROR] Shared TMP timestamp directory is not writable after permission preparation: {tmp_timestamp_dir}")
            exit()

        return tmp_timestamp_dir
    except (PermissionError, OSError, _util_subprocess.CalledProcessError) as e:
        last_error = e

    _docker_logger.error(f"[DOCKER FUNC][ERROR] Failed to prepare shared TMP directory for DBMS {dbms}. Last error: {last_error}")
    exit()

def _docker_wait_for_log_file_done(path, timeout=10, heartbeat_message=None): # "__DONE__" in lines
    start = _util_time.time()
    last_heartbeat = start
    while _util_time.time() - start < timeout:
        if _util_os.path.exists(path):
            with open(path, 'r') as file :
                lines = file.readlines()
                lines = [line.strip() for line in lines]
            if "__DONE__" in lines : return True
        if heartbeat_message and _util_time.time() - last_heartbeat >= _docker_FAST_RESET_HEARTBEAT_INTERVAL_SECONDS:
            _docker_logger.info(heartbeat_message)
            last_heartbeat = _util_time.time()
        _util_time.sleep(0.1)
    return False

def _docker_wait_for_log_file_signal(path, timeout, ready_pattern=None):
    """Wait for command completion or an optional explicit readiness signal."""
    compiled_ready = _util_re.compile(ready_pattern) if ready_pattern else None
    start = _util_time.time()
    while _util_time.time() - start < timeout:
        if _util_os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="replace") as file:
                content = file.read()
            if "__DONE__" in content.splitlines():
                return "completed"
            if compiled_ready and compiled_ready.search(content):
                return "ready"
        _util_time.sleep(0.1)
    return "timeout"


_docker_EXIT_CODE_SUFFIX = ".exit"

def _docker_build_exit_code_capture(inner_command_str, log_path):
    """Build a shell snippet that runs `inner_command_str`, tees its combined
    output to `log_path`, and records the command's true exit code.

    The exit code is taken from `${PIPESTATUS[0]}` so that the status reflects
    the actual command rather than the `tee` at the end of the pipe. The exit
    code is written to a SEPARATE sentinel file (`log_path + EXIT_CODE_SUFFIX`),
    keeping the original log content untouched for callers that read it back.
    The `__DONE__` marker is still appended to `log_path` so that the existing
    `wait_for_log_file_done` completion detection keeps working unchanged.
    """
    exit_code_path = log_path + _docker_EXIT_CODE_SUFFIX
    # Wrap the whole pipeline in a host-side bash so PIPESTATUS is available and
    # is evaluated immediately after the docker command finishes.
    snippet = (
        f"bash -c {_docker_shlex.quote(inner_command_str + f' 2>&1 | tee {_docker_shlex.quote(log_path)}; echo ${{PIPESTATUS[0]}} > {_docker_shlex.quote(exit_code_path)}')}"
        f"; echo \"__DONE__\" >> {_docker_shlex.quote(log_path)}"
    )
    return snippet, exit_code_path

def _docker_read_captured_exit_code(exit_code_path, timeout=10):
    """Read the in-container command's exit code from the sentinel file.

    Returns the integer exit code on success, or None when the file is missing,
    empty or unparsable within `timeout` seconds (which the caller should treat
    as a failure / indeterminate result).
    """
    start = _util_time.time()
    while _util_time.time() - start < timeout:
        if _util_os.path.exists(exit_code_path):
            try:
                with open(exit_code_path, 'r') as file:
                    content = file.read().strip()
                if content != "":
                    return int(content)
            except (ValueError, OSError):
                # File present but not yet fully written / not an integer.
                pass
        _util_time.sleep(0.1)
    return None

def _docker_reset_captured_log(log_path):
    """Remove a previous run's log and exit-code sentinel before a new dispatch.

    The exec helpers reuse the same `log_path` (and its `.exit` sentinel) across
    multi-turn retries. Because the in-container command is launched
    asynchronously via `tmux send-keys`, the poller could otherwise observe the
    PREVIOUS run's `__DONE__` marker and stale exit code instantly, return before
    the new command finishes, and let the caller tear the container down while an
    import is still in flight (the SIGKILL -> exit code 137 false report).
    Clearing both files forces the poller to wait for the genuine new completion.
    """
    for path in (log_path, log_path + _docker_EXIT_CODE_SUFFIX):
        try:
            _util_os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as e:
            _docker_logger.warning(f"[DOCKER FUNC][WARNING] Could not reset stale log file '{path}': {' '.join(str(e).split())}")

def _docker_tmux_has_session(session_name):
    result = _util_subprocess.run(
        ["tmux", "has-session", "-t", session_name],
        stdout=_util_subprocess.DEVNULL,
        stderr=_util_subprocess.DEVNULL
    )
    return result.returncode == 0

def _docker_tmux_has_window(session_name, window_name):
    result = _util_subprocess.run(
        ["tmux", "list-windows", "-t", session_name],
        stdout=_util_subprocess.PIPE,
        stderr=_util_subprocess.DEVNULL,
        text=True
    )
    if result.returncode != 0:
        return False

    for line in result.stdout.splitlines():
        if line.split(":")[1].strip().startswith(window_name):
            return True
    return False

def _docker_tmux_window_running(session_name, window_name):
    try:
        result = _util_subprocess.check_output(
            ["tmux", "list-panes", "-t", f"{session_name}:{window_name}", "-F", "#{pane_current_command}"],
            stderr=_util_subprocess.DEVNULL
        ).decode().strip()

        # 常见 shell
        shells = {"bash", "zsh", "sh", "fish"}

        return result not in shells
    except _util_subprocess.CalledProcessError:
        return False
    

def _docker_reset_tmux_window(session_name, window_name):
    _docker_logger.info("[DOCKER FUNC][RESET THE CURRENT TMUX WINDOW]")

    if not _docker_tmux_has_session(session_name):
        _util_subprocess.run(
            ["tmux", "new-session", "-d", "-s", session_name],
            stdout=_util_subprocess.DEVNULL,
            stderr=_util_subprocess.DEVNULL
        )

    if not _docker_tmux_has_window(session_name, window_name):
        _util_subprocess.run(
            ["tmux", "new-window", "-d", "-t", session_name, "-n", window_name],
            stdout=_util_subprocess.DEVNULL,
            stderr=_util_subprocess.DEVNULL
        )
    
    _util_subprocess.run(
        ["tmux","kill-window","-t",f"{session_name}:{window_name}"],
        stdout=_util_subprocess.DEVNULL,
        stderr=_util_subprocess.DEVNULL
    )

    _util_subprocess.run(
        ["tmux","new-window","-t",session_name,"-n",window_name],
        stdout=_util_subprocess.DEVNULL,
        stderr=_util_subprocess.DEVNULL
    )

def _docker_kill_tmux_session(session_name):
    """Kill the whole tmux session ``session_name`` if it exists.

    Because ``session_name`` tracks the (now timestamped, unique) container name,
    this must be called whenever the container name changes (e.g. on a per-turn
    recreation) so the OLD session — which carried the previous container's
    ``docker exec`` workload — does not linger and accumulate across turns.
    """
    if not _docker_tmux_has_session(session_name):
        return
    _util_subprocess.run(
        ["tmux", "kill-session", "-t", session_name],
        stdout=_util_subprocess.DEVNULL,
        stderr=_util_subprocess.DEVNULL
    )

def _docker_kill_tmux_sessions_by_prefix(base_name):
    """Kill every tmux session whose name equals ``base_name`` or starts with
    ``base_name + '_'`` (our timestamped-name convention).

    Symmetric to ``remove_containers_by_prefix``: used for end-of-sample / startup
    cleanup so lingering per-turn sessions of THIS worker are reaped without
    touching other parallel workers' sessions (their numeric prefixes differ).
    """
    result = _util_subprocess.run(
        ["tmux", "list-sessions", "-F", "#{session_name}"],
        stdout=_util_subprocess.PIPE,
        stderr=_util_subprocess.DEVNULL,
        text=True
    )
    if result.returncode != 0 or not result.stdout:
        return
    prefix = f"{base_name}_"
    for name in result.stdout.splitlines():
        name = name.strip()
        if name == base_name or name.startswith(prefix):
            _docker_kill_tmux_session(name)

def _docker__docker_list_container_names(all_containers=False):
    cmd = ["docker", "ps", "--format", "{{.Names}}"]
    if all_containers:
        cmd.insert(2, "-a")

    result = _util_subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        timeout=_docker_DOCKER_PS_TIMEOUT,
    )
    return result.stdout.strip().splitlines()

def _docker__probe_container_presence(container_name, all_containers=True):
    """Reliably probe whether ``container_name`` is present.

    Returns a tri-state so callers can tell a *confirmed* answer apart from a
    *failed probe*:
      - ``True``  : the name is confirmed present.
      - ``False`` : ``docker ps`` succeeded and the name is confirmed absent.
      - ``None``  : every ``docker ps`` attempt failed / timed out, so presence
                    is UNKNOWN (must NOT be treated as "removed").

    ``docker ps`` is retried a few times because under heavy parallel load it can
    be transiently slow or fail; a single failed probe used to be misread as
    "container gone", which let the next ``docker run`` reuse a name that was
    still taken and hit a 125 conflict.
    """
    last_error = None
    for attempt in range(1, _docker_DOCKER_PS_RETRIES + 1):
        try:
            return container_name in _docker__docker_list_container_names(all_containers=all_containers)
        except (_util_subprocess.CalledProcessError, _util_subprocess.TimeoutExpired) as e:
            last_error = e
            _docker_logger.warning(
                f"[DOCKER FUNC][WARNING] `docker ps` probe for {container_name} "
                f"failed (attempt {attempt}/{_docker_DOCKER_PS_RETRIES}) [{e}]"
            )
            if attempt < _docker_DOCKER_PS_RETRIES:
                _util_time.sleep(_docker_DOCKER_PS_RETRY_INTERVAL)

    _docker_logger.error(
        f"[DOCKER FUNC][ERROR] Could not determine presence of container "
        f"{container_name} after {_docker_DOCKER_PS_RETRIES} attempts [{last_error}]"
    )
    return None

def _docker__is_docker_run_command(cmd_list):
    return len(cmd_list) >= 2 and cmd_list[0] == "docker" and cmd_list[1] == "run"

def _docker__is_name_conflict_error(error):
    stderr = error.stderr or ""
    return (
        error.returncode == 125
        and "Conflict. The container name" in stderr
        and "is already in use by container" in stderr
    )

def _docker_wait_for_container_removed(
    container_name,
    timeout=_docker_DOCKER_REMOVE_TIMEOUT,
    stable_checks=1,
):
    """Block until ``container_name`` is CONFIRMED removed or ``timeout`` elapses.

    Only a positive ``docker ps`` confirmation of absence
    (``is_container_confirmed_absent``) counts toward ``stable_checks``. A failed
    or timed-out probe is treated as "still unknown" and resets the streak, so we
    never declare a container removed just because ``docker ps`` was unreachable.
    """
    start_time = _util_time.time()
    absent_checks = 0
    while _util_time.time() - start_time < timeout:
        if _docker_is_container_confirmed_absent(container_name):
            absent_checks += 1
            if absent_checks >= stable_checks:
                _docker_logger.info(
                    f"[DOCKER FUNC] Container {container_name} confirmed removed "
                    f"({absent_checks}/{stable_checks} stable checks)."
                )
                return True
        else:
            absent_checks = 0
        _util_time.sleep(_docker_DOCKER_POLL_INTERVAL)

    _docker_logger.error(
        f"[DOCKER FUNC][ERROR] Container name still exists (or presence "
        f"unconfirmed) after waiting {timeout}s: {container_name}"
    )
    return False

def _docker_force_remove_container(
    container_name,
    timeout=_docker_DOCKER_REMOVE_TIMEOUT,
    wait_when_absent=False,
):
    """Force-remove ``container_name`` and SYNCHRONOUSLY confirm it is gone.

    The function only returns ``True`` once ``docker ps`` positively confirms the
    name has been released (``DOCKER_ABSENT_STABLE_CHECKS`` consecutive absent
    probes). Because a client-side ``docker rm -f`` timeout does NOT necessarily
    mean the daemon stopped removing, after each confirmation slice that still
    finds the name present we re-issue ``docker rm -f`` (idempotent) until the
    overall ``timeout`` is exhausted. This is what prevents the caller from
    racing ahead to ``docker run`` while the old container still holds the name.
    """
    if _docker_is_container_confirmed_absent(container_name):
        if wait_when_absent:
            return _docker_wait_for_container_removed(
                container_name,
                timeout=timeout,
                stable_checks=_docker_DOCKER_ABSENT_STABLE_CHECKS,
            )
        return True

    deadline = _util_time.time() + timeout
    rm_attempts = 0
    while _util_time.time() < deadline:
        rm_attempts += 1
        _docker_logger.info(
            f"[DOCKER FUNC] Removing container {container_name} "
            f"(attempt {rm_attempts}) ..."
        )
        try:
            _util_subprocess.run(
                ["docker", "rm", "-f", container_name],
                check=True,
                text=True,
                stdout=_util_subprocess.PIPE,
                stderr=_util_subprocess.PIPE,
                timeout=_docker_DOCKER_RM_CMD_TIMEOUT,
            )
        except _util_subprocess.TimeoutExpired as e:
            # The CLI was killed at DOCKER_RM_CMD_TIMEOUT, but the daemon may
            # still be tearing the container down. Do NOT give up: fall through
            # to confirm-and-retry so we keep waiting for the real completion.
            _docker_logger.warning(
                f"[DOCKER FUNC][WARNING] Removing Container {container_name} "
                f"timed out after {_docker_DOCKER_RM_CMD_TIMEOUT}s; will re-confirm and "
                f"retry [{e}]"
            )
        except _util_subprocess.CalledProcessError as e:
            stderr = e.stderr or ""
            if "No such container" in stderr:
                # Already gone from the daemon's perspective; still confirm via
                # docker ps to honour the stable-checks contract.
                return _docker_wait_for_container_removed(
                    container_name,
                    timeout=max(timeout, _docker_DOCKER_RM_CONFIRM_SLICE),
                    stable_checks=_docker_DOCKER_ABSENT_STABLE_CHECKS,
                )
            if "removal of container" in stderr and "is already in progress" in stderr:
                _docker_logger.info(
                    f"[DOCKER FUNC] Container removal already in progress: "
                    f"{container_name}; waiting for it to complete."
                )
            else:
                _docker_logger.warning(
                    f"[DOCKER FUNC][WARNING] Removing Container {container_name} "
                    f"failed [{e}]"
                )

        # After issuing the removal, wait one confirmation slice for the daemon
        # to actually release the name. If confirmed absent within the slice we
        # are done; otherwise loop and re-issue ``docker rm -f``.
        remaining = deadline - _util_time.time()
        if remaining <= 0:
            break
        slice_timeout = min(_docker_DOCKER_RM_CONFIRM_SLICE, remaining)
        if _docker_wait_for_container_removed(
            container_name,
            timeout=slice_timeout,
            stable_checks=_docker_DOCKER_ABSENT_STABLE_CHECKS,
        ):
            return True

    _docker_logger.error(
        f"[DOCKER FUNC][ERROR] Failed to confirm removal of container "
        f"{container_name} within {timeout}s after {rm_attempts} attempt(s)."
    )
    return False

def _docker_is_container_running(container_name):
    presence = _docker__probe_container_presence(container_name, all_containers=False)
    # A failed/indeterminate probe (None) is reported as "not running" to keep
    # the historical boolean contract for callers that only gate a start on it.
    return presence is True


def _docker_is_container_confirmed_absent(container_name):
    """Return True only when ``docker ps`` positively confirms the name is gone.

    An indeterminate probe (``docker ps`` failing/timing out) returns False so
    the caller keeps waiting/retrying instead of racing ahead to ``docker run``
    and hitting a name conflict.
    """
    return _docker__probe_container_presence(container_name, all_containers=True) is False

def _docker_make_unique_container_name(base_name):
    """Return a per-creation UNIQUE container name derived from ``base_name``.

    A microsecond-precision timestamp suffix (``%Y%m%d_%H%M%S_%f``) is appended
    so that two reinitializations within the same wall-clock second still get
    distinct names. Because names are never reused, a slow daemon-side teardown of
    the PREVIOUS container can no longer collide with the NEXT ``docker run`` (the
    root cause of the intermittent ``RC 125`` name conflicts under heavy parallel
    load). ``base_name`` (e.g. ``6_pg_syn_ano``) remains the stable PREFIX used by
    ``remove_containers_by_prefix`` for orphan sweeps.
    """
    timestamp = _docker_datetime_module.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return f"{base_name}_{timestamp}"

def _docker_list_containers_by_prefix(base_name):
    """Return the names of all existing containers whose name starts with
    ``base_name`` followed by our timestamp separator (``base_name + '_'``), plus
    an exact match on ``base_name`` itself (covers legacy fixed-name containers).

    The ``base_name + '_'`` guard prevents a shorter prefix from accidentally
    matching an unrelated container that merely shares a leading substring
    (e.g. base ``6_pg_syn_ano`` must not match ``60_pg_syn_ano_...``). A failed/
    timed-out ``docker ps`` is surfaced as an empty list so the caller does not
    mistake an unreachable daemon for "no orphans present".
    """
    for attempt in range(1, _docker_DOCKER_PS_RETRIES + 1):
        try:
            names = _docker__docker_list_container_names(all_containers=True)
            prefix = f"{base_name}_"
            return [n for n in names if n == base_name or n.startswith(prefix)]
        except (_util_subprocess.CalledProcessError, _util_subprocess.TimeoutExpired) as e:
            _docker_logger.warning(
                f"[DOCKER FUNC][WARNING] `docker ps` prefix scan for {base_name} "
                f"failed (attempt {attempt}/{_docker_DOCKER_PS_RETRIES}) [{e}]"
            )
            if attempt < _docker_DOCKER_PS_RETRIES:
                _util_time.sleep(_docker_DOCKER_PS_RETRY_INTERVAL)
    _docker_logger.error(
        f"[DOCKER FUNC][ERROR] Could not list containers with prefix "
        f"{base_name} after {_docker_DOCKER_PS_RETRIES} attempts."
    )
    return []

def _docker_remove_containers_by_prefix(base_name):
    """Force-remove EVERY container sharing ``base_name`` as prefix and confirm
    each is gone. Used to reap orphans left behind by a previous crashed/killed
    run before a fresh start, and as a defensive sweep. Because each worker's
    ``base_name`` carries a unique numeric prefix (``0_``, ``6_`` ...), this only
    ever touches the calling worker's own containers and never interferes with
    other parallel workers.

    Returns True if all matched containers were confirmed removed (or none were
    present), False if at least one could not be confirmed gone.
    """
    stale = _docker_list_containers_by_prefix(base_name)
    if not stale:
        return True

    _docker_logger.info(
        f"[DOCKER FUNC] Prefix sweep for '{base_name}': found "
        f"{len(stale)} container(s) to remove: {stale}"
    )
    all_removed = True
    for name in stale:
        if not _docker_force_remove_container(name, wait_when_absent=True):
            _docker_logger.error(
                f"[DOCKER FUNC][ERROR] Prefix sweep failed to confirm removal "
                f"of container {name}"
            )
            all_removed = False
    return all_removed

def _docker_wait_for_container_name_free(container_name, timeout=_docker_DOCKER_NAME_FREE_TIMEOUT):
    """Block until ``container_name`` is CONFIRMED absent (free to reuse) or the
    ``timeout`` budget is exhausted.

    Implements requirement (1): before creating a container, if a container with
    the target name still exists, sleep and re-check; only after the name remains
    present beyond ``timeout`` (default 3600s) do we log an error and report
    failure to the caller (which surfaces it as a bounded failure rather than a
    silent hang). Returns True as soon as the name is confirmed free.
    """
    start_time = _util_time.time()
    while _util_time.time() - start_time < timeout:
        if _docker_is_container_confirmed_absent(container_name):
            return True
        _docker_logger.info(
            f"[DOCKER FUNC] Container name '{container_name}' still in use; "
            f"waiting {_docker_DOCKER_NAME_FREE_POLL_INTERVAL}s before re-checking "
            f"(elapsed {int(_util_time.time() - start_time)}s / {timeout}s)."
        )
        _util_time.sleep(_docker_DOCKER_NAME_FREE_POLL_INTERVAL)

    _docker_logger.error(
        f"[DOCKER FUNC][ERROR] Container name '{container_name}' is STILL in use "
        f"after waiting {timeout}s; aborting creation to avoid a name conflict."
    )
    return False

def _docker_docker_container_init(container_name, container_config) :
    if not _docker_force_remove_container(container_name):
        _docker_logger.error(f"[DOCKER FUNC][ERROR] Pre-cleaning Container {container_name}")
        return False

    dbms_kind, cpus, mem_limit, mem_swap, storage_size, port, db_host, db_port = _docker__container_config_values(container_config)

    _docker_logger.info(f"[DOCKER FUNC] Docker Container ``{container_name}'' Initialization.")
    if dbms_kind == "mysql":
        docker_commands = [
            _docker_mysql_create_container_str.format(cpus, mem_limit, mem_swap, storage_size, port, container_name),
            _docker_mysql_init_container_str.format(container_name, db_host, db_port)
        ]
    else:
        docker_commands = [
            _docker_create_container_str.format(cpus, mem_limit, mem_swap, storage_size, port, container_name),
            _docker_init_container_str.format(container_name, db_host, db_port)
        ]
    for command_index, docker_command in enumerate(docker_commands):
        command_stage = "container creation" if command_index == 0 else "DBMS bootstrap"
        cmd_list = _docker_shlex.split(docker_command)
        if _docker__is_docker_run_command(cmd_list):
            # Requirement (1): before ``docker run``, make sure the target name is
            # genuinely free. First force-remove any leftover holding this exact
            # name, then poll until the name is CONFIRMED absent. If it is still in
            # use after DOCKER_NAME_FREE_TIMEOUT (3600s), abort with an error rather
            # than racing into a guaranteed RC 125 conflict.
            if not _docker_force_remove_container(container_name):
                _docker_logger.error(f"[DOCKER FUNC][ERROR] Pre-cleaning Container {container_name}")
                return False
            if not _docker_wait_for_container_name_free(container_name):
                _docker_logger.error(
                    f"[DOCKER FUNC][ERROR] Aborting creation: container name "
                    f"{container_name} did not become free within "
                    f"{_docker_DOCKER_NAME_FREE_TIMEOUT}s."
                )
                return False

        attempts = 0
        while True :
            try:
                _ = _util_subprocess.run(cmd_list, check=True, text=True, stdout=_util_subprocess.PIPE, stderr=_util_subprocess.PIPE, timeout=_docker_DOCKER_INIT_TIMEOUT)
                break

            except _util_subprocess.TimeoutExpired as e:
                # A hung `docker run` / init script no longer blocks forever:
                # report failure so the caller's retry path can clean up and
                # the shell driver can re-run this step from the checkpoint.
                _docker_logger.error(
                    f"[DOCKER FUNC][ERROR] {command_stage} timed out after "
                    f"{_docker_DOCKER_INIT_TIMEOUT}s for container `{container_name}`: "
                    f"{' '.join(cmd_list)} [{str(e)}]"
                )
                if _docker__is_docker_run_command(cmd_list):
                    _docker_force_remove_container(container_name, wait_when_absent=True)
                return False

            except _util_subprocess.CalledProcessError as e:
                _docker_logger.error(
                    f"[DOCKER FUNC][ERROR] {command_stage} failed for container "
                    f"`{container_name}` (attempt {attempts + 1}/3): "
                    f"{' '.join(cmd_list)}"
                )
                _docker_logger.error(f"[DOCKER FUNC][ERROR] [Return Code]: {e.returncode}")
                if e.stdout and e.stdout.strip() != "" : _docker_logger.error(f"[DOCKER FUNC][ERROR][STDOUT]:\n{e.stdout}")
                if e.stderr and e.stderr.strip() != "" : _docker_logger.error(f"[DOCKER FUNC][ERROR][STDERR]:\n{e.stderr}")

                if _docker__is_docker_run_command(cmd_list) and _docker__is_name_conflict_error(e):
                    _docker_logger.warning(
                        f"[DOCKER FUNC][WARNING] Docker name conflict detected "
                        f"for {container_name}; force removing stale container."
                    )
                    if not _docker_force_remove_container(container_name, wait_when_absent=True):
                        return False

                # Never block on interactive stdin in a batch/tmux run (which
                # would hang forever). Retry a bounded number of times, then
                # surface failure to the caller's existing retry logic.
                attempts += 1
                if attempts >= 3 :
                    _docker_logger.error(
                        f"[DOCKER FUNC][ERROR] {command_stage} permanently failed "
                        f"for container `{container_name}` after {attempts} attempts."
                    )
                    return False
                _util_time.sleep(5)
                continue
    
    # Check Current Container Status
    if _docker_is_container_running(container_name):
        return True
    _docker_logger.error(
        f"[DOCKER FUNC][ERROR] Container `{container_name}` completed initialization "
        "commands but is not running."
    )
    return False

def _docker_docker_container_del(container_name):
    return _docker_force_remove_container(container_name)


def _docker_recreate_container_with_new_name(old_container_name, base_name, container_config):
    """Tear down ``old_container_name`` (container + its tmux session) and create a
    fresh container under a NEW unique name derived from ``base_name``.

    This is the core of the "never reuse a name" strategy: instead of deleting a
    name and immediately recreating the SAME name (which races the daemon's
    still-in-progress teardown and yields RC 125), we always move to a brand new
    timestamped name. The old container is force-removed and confirmed absent so
    no orphan accumulates.

    Returns ``(success, new_container_name)``. On failure ``new_container_name``
    is still returned (the newly attempted name) so the caller can track/clean it.
    """
    # 1) Reap the previous container and its tmux session synchronously so the
    #    steady state stays at one container/session per worker (no orphans).
    if old_container_name:
        _docker_kill_tmux_session(old_container_name)
        if not _docker_force_remove_container(old_container_name, wait_when_absent=True):
            _docker_logger.error(
                f"[DOCKER FUNC][ERROR] Failed to confirm removal of previous "
                f"container {old_container_name} before recreation."
            )
            # Continue anyway: the new unique name will not collide, and a later
            # prefix sweep / final cleanup will reap the leftover.

    # 2) Create under a fresh unique name (never reused -> no name conflict).
    new_container_name = _docker_make_unique_container_name(base_name)
    init_status = _docker_docker_container_init(new_container_name, container_config)
    if not init_status:
        _docker_logger.error(
            f"[DOCKER FUNC][ERROR] Initializing new container {new_container_name}"
        )
        return False, new_container_name
    return True, new_container_name

_docker_POSTGRES_INIT_ERROR_PATTERN = _util_re.compile(
    r"^(?:psql:[^:]+:\d+:\s*)?(?:ERROR|FATAL|PANIC):",
    _util_re.MULTILINE,
)


def _docker_database_init_log_errors(status_log, dbms):
    """Return PostgreSQL client errors that may be masked by psql exit status 0."""
    if not _util_os.path.isfile(status_log) or _util_normalize_dbms(dbms) == "mysql":
        return []
    try:
        with open(status_log, "r", encoding="utf-8", errors="replace") as file:
            content = file.read()
    except OSError as exc:
        return [f"unable to read initialization log: {exc}"]
    return [
        line.strip()
        for line in content.splitlines()
        if _docker_POSTGRES_INIT_ERROR_PATTERN.match(line)
    ]


def _docker_docker_container_init_db(container_name, init_db_config, session_name="docker_test"):
    base_dir = _util_os.path.realpath(init_db_config['base_dir'])
    dbms_kind = _util_normalize_dbms(init_db_config.get("dbms"))
    if dbms_kind == "mysql":
        init_db_script_path = str(_ASSET_ROOT / 'Scripts/initialize_database_mysql.sh')
        instance_directory = _util_os.path.join(base_dir, 'Database_Instances', 'mysql_instances')
        default_port = 3306
    else:
        init_db_script_path = str(_ASSET_ROOT / 'Scripts/initialize_database_pg.sh')
        instance_directory = _util_os.path.join(base_dir, 'Database_Instances', 'postgresql_instances')
        default_port = 5432

    db_host = init_db_config.get('db_host', init_db_config.get('pg_host', 'localhost'))
    db_port = init_db_config.get('db_port', init_db_config.get('pg_port', default_port))
    db_name = init_db_config.get('db_name', 'tpch_1')
    db_schema = init_db_config.get('db_schema', 'diagnosis')
    if dbms_kind == "mysql":
        db_schema = db_name
    db_type = init_db_config.get('db_type', 'dump')
    db_user = init_db_config.get('db_user', 'diagnosis')
    db_password = init_db_config.get('password', 'diagnosis')
    tmp_directory = _util_os.path.realpath(
        init_db_config.get('tmp_directory', _util_os.path.join(base_dir, 'TMP'))
    )

    init_db_str = f"bash {init_db_script_path} --host {db_host} --port {db_port} --database_name {db_name} --database_type {db_type} --database_schema {db_schema} --instance_dir {instance_directory} --tmp_dir {tmp_directory} --user {db_user} --password {db_password}"
    # print(init_db_str)

    init_db_cmd = init_db_str.split()
    status_log = _util_os.path.join(init_db_config['output_dir'], "init_db_log.txt")
    docker_status = _docker_run_docker_exec_status(container_name, init_db_cmd, status_log, timeout = init_db_config['timeout'] + 120, session_name = session_name)
    init_errors = _docker_database_init_log_errors(status_log, dbms_kind)
    if docker_status and init_errors:
        _docker_logger.error(
            "[DOCKER FUNC][ERROR] Database initialization emitted "
            f"{len(init_errors)} PostgreSQL error(s) despite exit status 0. "
            f"First errors: {init_errors[:3]}"
        )
        docker_status = False
    _docker_logger.info(f"[DOCKER FUNC] Database Initialization Status: {'Success' if docker_status else 'Failure'}")
    
    return docker_status


# =============================================================================
# Fast environment reset (container reuse + golden clone).
#
# These two functions implement the container-reuse optimization for the
# per-turn re-initialization in test_syn_ano.py. Rather than destroying the
# container and re-importing a multi-GB dump on every LLM turn, we:
#   1) build a golden baseline ONCE (docker_container_build_baseline_snapshot),
#      after the first data-modification is applied, capturing the exact
#      pre-injection state; and
#   2) restore that baseline every turn (docker_container_fast_reset) via an
#      engine-internal clone -- seconds instead of minutes -- inside the SAME
#      live container. This preserves per-worker isolation (the golden db lives
#      in that worker's own container) and guarantees a bit-for-bit identical
#      baseline for both anomaly reproduction and repeated data-modification.
#
# Both reuse the existing run_docker_exec_status dispatch (tmux + exit-code
# capture) and are additive: on ANY failure they return False so the caller can
# fall back to the original full re-initialization path.
# =============================================================================







def _docker_run_docker_exec_observation(container_name, command, obser_log, timeout=60,
                                session_name="docker_test", window_name="observation",
                                ready_pattern=None):
    if _docker_tmux_window_running(session_name, window_name) or not _docker_tmux_has_session(session_name) or not _docker_tmux_has_window(session_name, window_name):
        _docker_reset_tmux_window(session_name, window_name)

    # docker_command = ["docker", "exec", container_name] + command
    # docker_cmd = " ".join(
    #     ["timeout",f"{timeout + 10}", "docker", "exec", "-it", container_name] +
    #     [shlex.quote(c) for c in command]
    # )
    # "export PAGER=cat; export LESS=FRX; " + 
    # docker_cmd = " ".join(
    #     ["docker", "exec", "-it", container_name] +
    #     [shlex.quote(c) for c in command]
    # ) + f" 2>&1 | tee {obser_log}; echo \"__DONE__\" >> {obser_log}"
    command_str = " ".join([_docker_shlex.quote(c) for c in command])

    # Wrap with exit-code capture: the real exit code of the `docker exec`
    # command is recorded to a sentinel file via ${PIPESTATUS[0]}, so a failing
    # in-container command is detected even though `tee`/send-keys return 0. The
    # exit code goes to a separate file, leaving obser_log content intact for the
    # callers that read it back.
    inner_command_str = f'docker exec -i {container_name} bash -c {_docker_shlex.quote(command_str)}'
    docker_cmd, exit_code_path = _docker_build_exit_code_capture(inner_command_str, obser_log)
    _docker_reset_captured_log(obser_log)

    output = ""

    def to_text(data):
        if data is None:
            return ""
        if isinstance(data, bytes):
            return data.decode("utf-8", errors="replace")
        return data 

    try:
        # Run the Docker command with a timeout and capture both stdout and stderr
        # result = subprocess.run(docker_command, capture_output=True, text=True, check=True, timeout=timeout)
        commands = ["tmux", "send-keys", "-t", f"{session_name}:{window_name}", docker_cmd, "C-m"]
        # print(commands)
        # print(" ".join(commands))
        result = _util_subprocess.run(commands, capture_output=True, text=True, check=True, timeout=timeout)

        # Return the combined output (stdout + stderr)
        # output += f"Script executed successfully, with the output as follows:\n{stdout}\n\n"
    
        obser_state = _docker_wait_for_log_file_signal(
            obser_log, timeout + 60, ready_pattern=ready_pattern
        )

        output = ""
        if obser_state == "timeout":
            _docker_logger.warning(f"[DOCKER FUNC][ERROR] Observation log file did not complete within the expected time frame of {timeout + 60} seconds.")
            output += f"[Error in Script Exec]: Command timed out after {str(timeout + 60)} seconds\n"
            return False, output
        if obser_state == "ready":
            _docker_logger.info(
                f"[DOCKER FUNC] Readiness signal observed for `{obser_log}`; "
                "command remains active in its dedicated tmux window."
            )
            return True, output

        stdout = to_text(result.stdout)
        stderr = to_text(result.stderr)
        
        if stderr.strip():
            output += f"[STDOUT]\n{stdout}\n"
            output += f"[STDERR]\n{stderr}\n"
            
            return False, output

        # Verify the in-container exit code: a missing or non-zero code means the
        # executed script failed, so report failure rather than success.
        exit_code = _docker_read_captured_exit_code(exit_code_path)
        if exit_code is None :
            _docker_logger.error(f"[DOCKER FUNC][ERROR] Could not determine exit code for command: {obser_log}")
            output += f"[Error in Script Exec]: Could not determine command exit code\n"
            return False, output
        if exit_code != 0 :
            _docker_logger.error(f"[DOCKER FUNC][ERROR] Command failed with exit code {exit_code}: {obser_log}")
            output += f"[Error in Script Exec]: Command failed with exit code {exit_code}\n"
            return False, output

        return True, output
    
    except _util_subprocess.TimeoutExpired as e:
        stdout = to_text(e.stdout)
        stderr = to_text(e.stderr)
        # Timeout handling
        output += f"[Error in Script Exec]: Command timed out after {timeout} seconds [{str(e)}]\n"
        if stdout.strip():
            output += "[Partial Output]\n" + stdout + "\n"
        if stderr.strip():
            output += "[Error output]\n" + stderr + "\n"
        
        return False, output
    
    except _util_subprocess.CalledProcessError as e:
        stdout = to_text(e.stdout)
        stderr = to_text(e.stderr)
        # Command failed (non-zero exit status)
        output += f"[Error in Script Exec]: Command failed with exit code {e.returncode} [{str(e)}]\n"
        if stdout.strip():
            output += "[Script Output]\n" + stdout + "\n"
        if stderr.strip():
            output += "[Error output]\n" + stderr + "\n"
        
        return False, output
    
    except Exception as e:
        # Catch all other exceptions
        output += f"[Error in Script Exec]: Unexpected error occurred: {str(e)}\n"

        return False, output

def _docker_run_docker_exec_status(container_name, command, status_log, timeout=1200, backend=False, session_name="docker_test", window_name="status", heartbeat_message=None):
    if _docker_tmux_window_running(session_name, window_name) or not _docker_tmux_has_session(session_name) or not _docker_tmux_has_window(session_name, window_name):
        _docker_reset_tmux_window(session_name, window_name)

    # Build the in-container command and wrap it so the REAL exit code of the
    # docker command (via ${PIPESTATUS[0]}) is captured to a sentinel file. The
    # tmux send-keys subprocess only reports the status of send-keys itself, so
    # the exit code file is the only reliable signal of in-container failure.
    inner_command_str = " ".join(["docker", "exec", container_name]).strip() + " " + " ".join(command).strip()
    docker_command, exit_code_path = _docker_build_exit_code_capture(inner_command_str, status_log)
    _docker_reset_captured_log(status_log)
    try:
        commands = ["tmux", "send-keys", "-t", f"{session_name}:{window_name}", docker_command, "C-m"]
        if backend : process = _util_subprocess.Popen(commands, stdout=_util_subprocess.PIPE, stderr=_util_subprocess.PIPE, text=True)
        else : result = _util_subprocess.run(commands, capture_output=True, text=True, check=True, timeout=timeout)

        # First make sure the command actually finished (the __DONE__ marker).
        status_state = _docker_wait_for_log_file_done(
            status_log,
            timeout + 60,
            heartbeat_message=heartbeat_message,
        )
        if not status_state :
            _docker_logger.error(f"[DOCKER FUNC][ERROR] Command did not complete within {timeout + 60} seconds: {status_log}")
            return False

        # Then verify the captured exit code: a missing/non-zero code means the
        # in-container command failed even though the log reached __DONE__.
        exit_code = _docker_read_captured_exit_code(exit_code_path)
        if exit_code is None :
            _docker_logger.error(f"[DOCKER FUNC][ERROR] Could not determine exit code for command: {status_log}")
            return False
        if exit_code != 0 :
            _docker_logger.error(f"[DOCKER FUNC][ERROR] Command failed with exit code {exit_code}: {status_log}")
            return False

        return True
    
    except _util_subprocess.CalledProcessError as e:
        _docker_logger.error(f"[DOCKER FUNC][ERROR] Error in Script Execution [{str(e)}]")

        return False

    except _util_subprocess.TimeoutExpired as e:
        # The send-keys dispatch (or synchronous wait) exceeded the timeout.
        _docker_logger.error(f"[DOCKER FUNC][ERROR] Command timed out after {timeout} seconds [{str(e)}]")
        return False

    except Exception as e:
        # Any other unforeseen error must be treated as a failure, never success.
        _docker_logger.error(f"[DOCKER FUNC][ERROR] Unexpected error in Script Execution [{str(e)}]")
        return False
























# ======================== health_report.py ========================
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



import argparse as _health_argparse
import datetime as _health_datetime_module
import hashlib as _health_hashlib
import json as _health_json
import os as _health_os
from pathlib import Path as _health_Path
import re as _health_re
import signal as _health_signal
import subprocess as _health_subprocess
import sys as _health_sys
import tempfile as _health_tempfile
import time as _health_time
import uuid as _health_uuid
from typing import Any as _health_Any, Dict as _health_Dict, Iterable as _health_Iterable, List as _health_List, Mapping as _health_Mapping, Optional as _health_Optional, Sequence as _health_Sequence, Tuple as _health_Tuple

try:
    import psycopg2 as _health_psycopg2
except Exception:  # pragma: no cover - optional when only MySQL is evaluated
    _health_psycopg2 = None

try:
    import mysql.connector as _health_mysql_connector
except Exception:  # pragma: no cover - optional when only PostgreSQL is evaluated
    _health_mysql_connector = None


_health_SNAPSHOT_SCHEMA_VERSION = 1

_health_DEFAULT_THRESHOLDS = {
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
_health_POSTGRES_CONFIG_NAMES = (
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

_health_MYSQL_CONFIG_NAMES = (
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

_health_HISTORICAL_SIGNALS_EXCLUDED = (
    "PostgreSQL pg_stat_database.deadlocks and cumulative counters",
    "MySQL Aborted_connects and cumulative status counters",
    "past server error-log entries",
    "past statement errors and completed lock waits",
)


def _health__utc_now() -> str:
    return _health_datetime_module.datetime.now(_health_datetime_module.timezone.utc).isoformat()


def _health_normalize_dbms(value: _health_Any) -> str:
    normalized = str(value or "postgresql").strip().lower()
    if "mysql" in normalized or "maria" in normalized:
        return "mysql"
    if "postgres" in normalized or normalized in {"pg", "pgsql"}:
        return "postgresql"
    raise ValueError(f"Unsupported DBMS: {value!r}. Expected PostgreSQL or MySQL.")


def _health__resolve_connection_params(db_config: _health_Mapping[str, _health_Any], dbms: str) -> _health_Dict[str, _health_Any]:
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


def _health__rows_as_dicts(cursor: _health_Any) -> _health_List[_health_Dict[str, _health_Any]]:
    columns = [description[0] for description in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


class _health__Collector:
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
        started = _health_time.monotonic()
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
            rows = _health__rows_as_dicts(self.cursor)
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
                    "duration_ms": round((_health_time.monotonic() - started) * 1000, 3),
                }
                self.budget["publish"]()


def _health__stable_hash(value: _health_Any) -> str:
    encoded = _health_json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return _health_hashlib.sha256(encoded).hexdigest()


def _health__sort_records(records: _health_Iterable[_health_Mapping[str, _health_Any]]) -> _health_List[_health_Dict[str, _health_Any]]:
    normalized = [
        {str(key): value for key, value in record.items()}
        for record in records
    ]
    return sorted(
        normalized,
        key=lambda item: _health_json.dumps(item, sort_keys=True, default=str),
    )


def _health__config_map(rows: _health_Iterable[_health_Mapping[str, _health_Any]], name_key: str, value_key: str) -> _health_Dict[str, _health_Any]:
    return {
        str(row[name_key]).lower(): row.get(value_key)
        for row in rows
        if row.get(name_key) is not None
    }


def _health__postgres_snapshot(
    connection: _health_Any,
    schema: str,
    thresholds: _health_Mapping[str, _health_Any],
    budget: _health_Any = None,
) -> _health_Tuple[_health_Dict[str, _health_Any], _health_Dict[str, _health_Any], _health_List[str]]:
    cursor = connection.cursor()
    collector = _health__Collector(cursor, budget)
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
        (list(_health_POSTGRES_CONFIG_NAMES),),
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
    max_connections = _health__config_map(config_rows, "name", "setting").get("max_connections")
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
    runtime["current_waits"] = _health__sort_records(waits)
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
        "tables": _health__sort_records(tables),
        "indexes": _health__sort_records(indexes),
        "constraints": _health__sort_records(constraints),
        "routines": _health__sort_records(routines),
        "extensions": _health__sort_records(extensions),
        "configuration": _health__config_map(config_rows, "name", "setting"),
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
    durable["schema_fingerprint"] = _health__stable_hash(
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


def _health__mysql_snapshot(
    connection: _health_Any,
    schema: str,
    thresholds: _health_Mapping[str, _health_Any],
) -> _health_Tuple[_health_Dict[str, _health_Any], _health_Dict[str, _health_Any], _health_List[str]]:
    cursor = connection.cursor()
    collector = _health__Collector(cursor)

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
    all_config = _health__config_map(config_rows, "Variable_name", "Value")
    configuration = {
        name: all_config[name]
        for name in _health_MYSQL_CONFIG_NAMES
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
        "current_waits": _health__sort_records(
            {
                "state": row.get("STATE") or "none",
                "seconds": int(row.get("TIME") or 0),
            }
            for row in processlist
            if row.get("STATE")
        ),
        "transactions": _health__sort_records(transactions),
        "replication": {
            "configured": bool(replication),
            "channels": _health__sort_records(replication),
        },
    }
    durable = {
        "available": bool(identity_rows),
        "identity": identity_rows[0] if identity_rows else {},
        "schema": schema,
        "tables": _health__sort_records(tables),
        "indexes": _health__sort_records(indexes),
        "constraints": _health__sort_records(constraints),
        "triggers": _health__sort_records(triggers),
        "routines": _health__sort_records(routines),
        "events": _health__sort_records(events),
        "configuration": configuration,
        "invalid_indexes": [],
        "high_dead_tuple_tables": [],
        "statistics_issues": [],
    }
    durable["schema_fingerprint"] = _health__stable_hash(
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


def _health__run_command(command: _health_Sequence[str], timeout: int = 10) -> _health_Tuple[_health_Optional[str], _health_Optional[str]]:
    try:
        result = _health_subprocess.run(
            list(command),
            stdout=_health_subprocess.PIPE,
            stderr=_health_subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, _health_subprocess.TimeoutExpired) as exc:
        return None, " ".join(str(exc).split())
    if result.returncode != 0:
        return None, " ".join((result.stderr or result.stdout).split())
    return result.stdout.strip(), None


def _health__parse_percent(value: _health_Any) -> _health_Optional[float]:
    match = _health_re.search(r"([0-9.]+)", str(value or ""))
    return round(float(match.group(1)) / 100.0, 4) if match else None


def _health__container_snapshot(container_name: _health_Optional[str], deadline: _health_Optional[float] = None) -> _health_Dict[str, _health_Any]:
    if not container_name:
        return {
            "available": False,
            "reason": "container_name was not supplied",
        }

    def run(command: Sequence[str], timeout: int = 10):
        if deadline is not None:
            remaining = deadline - _health_time.monotonic()
            if remaining <= 0:
                return None, "health collection budget exhausted"
            timeout = min(timeout, remaining)
        return _health__run_command(command, timeout=timeout)

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
        state = _health_json.loads(state_text) if state_text else {}
    except _health_json.JSONDecodeError:
        state = {}
        errors.append("inspect: invalid JSON")
    try:
        stats = _health_json.loads(stats_text) if stats_text else {}
    except _health_json.JSONDecodeError:
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
                    "arguments": _health_re.sub(r"\s+", " ", parts[1] if len(parts) > 1 else ""),
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
                "used_ratio": _health__parse_percent(values[3]),
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
            "cpu_used_ratio": _health__parse_percent(stats.get("CPUPerc")),
            "memory_used_ratio": _health__parse_percent(stats.get("MemPerc")),
            "pids": stats.get("PIDs"),
            "block_io": stats.get("BlockIO"),
            "network_io": stats.get("NetIO"),
            "disk": disk,
            "limits": limits,
        },
        "processes": _health__sort_records(processes),
        "errors": errors,
    }


def _health_capture_database_state_snapshot(
    db_config: _health_Mapping[str, _health_Any],
    logger: _health_Any = None,
    thresholds: _health_Optional[_health_Mapping[str, _health_Any]] = None,
    connect_timeout: int = 10,
    container_name: _health_Optional[str] = None,
    *,
    health_budget: _health_Optional[_health_Mapping[str, _health_Any]] = None,
    checkpoint: _health_Any = None,
) -> _health_Dict[str, _health_Any]:
    """Capture a normalized, read-only PostgreSQL or MySQL state snapshot."""
    active_thresholds = dict(_health_DEFAULT_THRESHOLDS)
    if thresholds:
        active_thresholds.update(thresholds)

    dbms = _health_normalize_dbms(db_config.get("dbms"))
    bounded = dbms == "postgresql" and health_budget is not None
    deadline = _health_time.monotonic() + health_budget["total_seconds"] if bounded else None
    params = _health__resolve_connection_params(db_config, dbms)
    schema = str(
        db_config.get("schema")
        or db_config.get("db_schema")
        or (params["database"] if dbms == "mysql" else "public")
    )
    snapshot = {
        "schema_version": _health_SNAPSHOT_SCHEMA_VERSION,
        "captured_at": _health__utc_now(),
        "dbms": dbms,
        "database": params["database"],
        "schema": schema,
        "collection_status": "pending",
        "policy": {
            "verdict_scope": "current operational state",
            "historical_signals_excluded": list(_health_HISTORICAL_SIGNALS_EXCLUDED),
        },
        "layers": {
            "container": {"available": False} if bounded else _health__container_snapshot(container_name),
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

    dependency = _health_psycopg2 if dbms == "postgresql" else _health_mysql_connector
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
    collection_started = _health_time.perf_counter()
    try:
        if dbms == "postgresql":
            options = {}
            if bounded:
                options = {"options": (
                    f"-c statement_timeout={health_budget['statement_ms']} "
                    f"-c lock_timeout={health_budget['lock_ms']} "
                    "-c default_transaction_read_only=on"
                ), "application_name": "db_diag_health_probe"}
            connection = _health_psycopg2.connect(
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
                durable, runtime, errors = _health__postgres_snapshot(connection, schema, active_thresholds, budget)
            else:
                durable, runtime, errors = _health__postgres_snapshot(connection, schema, active_thresholds)
        else:
            connection = _health_mysql_connector.connect(
                host=params["host"],
                port=params["port"],
                user=params["user"],
                password=params["password"],
                database=params["database"],
                connection_timeout=connect_timeout,
                use_pure=True,
                autocommit=True,
            )
            durable, runtime, errors = _health__mysql_snapshot(
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
                (_health_time.perf_counter() - collection_started) * 1000.0, 3
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
        snapshot["layers"]["container"] = _health__container_snapshot(container_name, deadline)
        if snapshot["layers"]["container"].get("errors"):
            snapshot["collection_errors"].extend(
                "container: " + error for error in snapshot["layers"]["container"]["errors"]
            )
            snapshot["collection_status"] = "partial"
        if checkpoint is not None:
            checkpoint(snapshot)
    return snapshot


def _health__add_check(
    checks: _health_List[_health_Dict[str, _health_Any]],
    check_id: str,
    status: str,
    critical: bool,
    summary: str,
    evidence: _health_Any = None,
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


def _health_evaluate_current_state_snapshot(
    snapshot: _health_Mapping[str, _health_Any],
    thresholds: _health_Optional[_health_Mapping[str, _health_Any]] = None,
) -> _health_Dict[str, _health_Any]:
    """Judge one current snapshot without anomaly or baseline context."""
    active_thresholds = dict(_health_DEFAULT_THRESHOLDS)
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
    _health__add_check(
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
        _health__add_check(
            checks,
            "container_service",
            "pass" if running else "fail",
            True,
            "Container service is running normally." if running else "Container service is not in a runnable state.",
            state,
        )
    else:
        _health__add_check(
            checks,
            "container_service",
            "unknown",
            False,
            "Container-level evidence was not requested or unavailable.",
        )

    functional_available = bool(functional.get("simple_query_succeeded"))
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
        checks,
        "schema_access",
        "unknown" if not durable.get("available") else (
            "warn" if table_count == 0 else "pass"
        ),
        False,
        f"Visible schema objects: {table_count}.",
    )

    invalid_indexes = durable.get("invalid_indexes") or []
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
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
    _health__add_check(
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
        _health__add_check(
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
        "schema_version": _health_SNAPSHOT_SCHEMA_VERSION,
        "generated_at": _health__utc_now(),
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
            "historical_signals_excluded": list(_health_HISTORICAL_SIGNALS_EXCLUDED),
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








def _health_judge_database_health(
    db_config: _health_Mapping[str, _health_Any],
    logger: _health_Any = None,
    thresholds: _health_Optional[_health_Mapping[str, _health_Any]] = None,
    connect_timeout: int = 10,
    container_name: _health_Optional[str] = None,
    *,
    health_budget: _health_Optional[_health_Mapping[str, _health_Any]] = None,
    checkpoint: _health_Any = None,
) -> _health_Dict[str, _health_Any]:
    """Capture and judge the current instance with no prerequisite state."""
    try:
        extra = {"health_budget": health_budget, "checkpoint": checkpoint} if health_budget is not None else {}
        snapshot = _health_capture_database_state_snapshot(
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
            "schema_version": _health_SNAPSHOT_SCHEMA_VERSION,
            "captured_at": _health__utc_now(),
            "collection_status": "invalid_input",
            "collection_errors": [str(exc)],
            "layers": {
                "container": {"available": False},
                "durable": {"available": False},
                "runtime": {"available": False},
                "functional": {"available": False},
            },
        }
    return _health_evaluate_current_state_snapshot(snapshot, thresholds=thresholds)


def _health_write_database_state_report(report: _health_Mapping[str, _health_Any], output_path: str) -> None:
    _health_os.makedirs(_health_os.path.dirname(_health_os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file:
        _health_json.dump(report, file, indent=2, ensure_ascii=False, default=str)








_health_PROBES = (
    "identity", "runtime_activity", "blocking_sessions", "tables", "table_sizes",
    "indexes", "constraints", "routines", "extensions", "configuration",
    "database_size", "current_waits", "prepared_transactions", "replication",
)
_health_MAX_CHECKPOINT_BYTES = 16 * 1024 * 1024






def _health_write_checkpoint(path, payload):
    data = _health_json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    if len(data) > _health_MAX_CHECKPOINT_BYTES:
        raise ValueError("Health checkpoint exceeds size limit")
    temporary = str(path) + ".tmp"
    with open(temporary, "wb") as stream:
        stream.write(data)
    _health_os.replace(temporary, path)






def _health_worker_main():
    request = _health_json.load(_health_sys.stdin)
    request_id = request["request_id"]
    path = _health_Path(request["checkpoint"])
    budget = request["budget"]

    def checkpoint(snapshot, finished=False):
        _health_write_checkpoint(path, {"request_id": request_id, "snapshot": snapshot, "finished": finished})

    snapshot = _health_capture_database_state_snapshot(
        request["db_config"], connect_timeout=budget["connect_seconds"],
        container_name=request["container_name"], health_budget=budget,
        checkpoint=checkpoint,
    )
    checkpoint(snapshot, finished=True)





# ======================== test_filter_syn_data_v0.py ========================
import json as _util_json
import os as _util_os
import time as _util_time
import argparse as _legacy_argparse
import logging as _util_logging
import queue as _legacy_queue
import shlex as _docker_shlex
import shutil as _legacy_shutil
import threading as _legacy_threading

from tqdm import tqdm as _legacy_tqdm





try:
    pass
except Exception:
    _health_judge_database_health = None
    _health_write_database_state_report = None

# python Filter/test_filter_syn_data.py --config Filter/Configs/example_rule.json

_legacy_LOG_NAME = _util_os.path.basename(__file__).split('.')[0]
_legacy_DEFAULT_CONFIG_PATH = str(DEFAULT_CONFIG_PATH)
_legacy_START_ID = 0
_legacy_END_ID = 3332
_legacy_MAX_TIMEOUT_SECONDS = 86400
_legacy_NEGATIVE_EXECUTION_TIME_FALLBACK = 10.0

# Number of times to retry Docker / database initialization before giving up on
# a sample.
_legacy_RETRY_TIMES = 10

_legacy_DEFAULT_MEDIAN_RUNS = 1
_legacy_DEFAULT_DOCKER_NAME = "pg_filter_syn_data"
_legacy_DEFAULT_DOCKER_PORT = 63002
_legacy_DEFAULT_SUCCESS_THRESHOLD_PERCENT = 20.0
_legacy_DEFAULT_MIN_ABS_DELTA_SECONDS = 0.0
_legacy_DEFAULT_ANOMALY_TYPE = "BOTH"
_legacy_DEFAULT_SUCCESS_CRITERION = "AUTO"
_legacy_THIRD_FILTER_ROUNDS = 3
_legacy_ANOMALY_SUCCESS_MARKERS = (
    "Anomaly successfully simulated and verified.",
    "Anomaly successfully reproduced and verified.",
)
_legacy_ANOMALY_INJECTION_LOG_START = "========== Anomaly Injection Script: START =========="
_legacy_ANOMALY_INJECTION_LOG_END = "========== Anomaly Injection Script: END =========="
_legacy_ANOMALY_INJECTION_LOG_MAX_LINES = 2000
_legacy_ANOMALY_INJECTION_OUTPUT_TRUNCATED = (
    "[FILTER_SYN_DATA] Anomaly injection command output truncated after "
    f"{_legacy_ANOMALY_INJECTION_LOG_MAX_LINES} lines."
)
_legacy_EVALUATOR_CLI_ERROR_MARKERS = (
    "unrecognized arguments:",
)
_legacy_POST_INJECTION_HEALTH_REPORT_NAME = "post_injection_health_report.json"
_legacy_HEALTH_STATUS_RESULT_KEY = "post_injection_health_status"
_legacy_HEALTH_STATUS_NOT_COLLECTED = "not_collected"
_legacy_HEALTH_STATUS_INDETERMINATE = "indeterminate"
_legacy_REP_EVAL_MARKER_FLAG = "--success-criterion"
_legacy_VALID_SUCCESS_CRITERIA = frozenset(
    {"PERFORMANCE_DEGRADATION", "BLOCKING_TIMEOUT", "EXECUTION_ERROR", "AUTO"}
)
_legacy_VALID_ANOMALY_TYPES = frozenset({"OLAP", "OLTP", "BOTH"})

# Parallel-evaluation defaults. Each worker owns a dedicated container name and
# host port derived from these bases plus the worker index, so concurrent
# workers never collide on either resource.
_legacy_DEFAULT_PARALLELISM = 1
# Spacing between per-worker host ports. Workers use
# ``docker_port + worker_index * PORT_STRIDE`` so a stray neighbouring port can
# never be claimed by two workers at once.
_legacy_WORKER_PORT_STRIDE = 1

# Ceiling (seconds) for the `docker run` + in-container init step. Creating a
# container with `--storage-opt size=64G` provisions a quota-backed filesystem,
# which on some storage drivers / loaded hosts is far slower than the previous
# 1200s allowance and was timing out. This value overrides
# ``docker_func.DOCKER_INIT_TIMEOUT`` at runtime and can be tuned from the config
# file via ``docker_init_timeout_seconds``.
_legacy_DEFAULT_DOCKER_INIT_TIMEOUT = 2400
# Database import is I/O-bound and must not inherit a sample's anomaly-workload
# timeout. A dedicated ceiling prevents valid bulk imports from being cut short.
_legacy_DEFAULT_DATABASE_INIT_TIMEOUT = 3600
_legacy_DATABASE_INIT_TIMEOUT_SECONDS = _legacy_DEFAULT_DATABASE_INIT_TIMEOUT
_legacy_DATABASE_INIT_SEMAPHORE = None

# Marker printed to STDOUT only after the whole batch truly finishes. The shell
# watchdog requires BOTH a zero exit status AND this marker before treating the
# run as complete, so a premature/abnormal exit (including any ``exit()`` call
# deep inside the imported helpers that would exit with status 0) is retried
# instead of being misreported as success.
_legacy_RUN_COMPLETION_MARKER = "[FILTER_SYN_DATA][RUN_COMPLETED]"

# Every file a synthetic anomaly sample directory is expected to contain. Used
# to validate the sample before evaluation.
_legacy_REQUIRED_SAMPLE_FILES = [
    'anomaly_description.json',
    'user_phenomenon_description.txt',
    'initialization_workload.sql',
    'verified_workload.sql',
    'merged_workload.sql',
    'merged_workload_performance.json',
    'data_modification.sql',
    'data_feature_summarization.txt',
    'anomaly_inject.sh',
]

# Name of the per-sample evaluation log written by ``run_anomaly_evaluation``.
# It is the LAST artifact produced for a sample, and the in-container command
# wrapper (``docker_func.build_exit_code_capture``) appends the ``__DONE__``
# marker to it only after the command truly finishes, writing the command's
# real exit code to a sibling ``.exit`` sentinel just before that. So this pair
# is the authoritative signal of "the sample evaluation flow ran to completion".
_legacy_ANOMALY_EVAL_LOG_NAME = "anomaly_eval_log.txt"
# Sentinel that stores the in-container exit code (see
# ``docker_func.EXIT_CODE_SUFFIX`` / ``build_exit_code_capture``). An exit code
# of 0 means the anomaly evaluation PASSED, mirroring the pass/fail decision in
# ``docker_func.run_docker_exec_observation``.
_legacy_EVAL_EXIT_CODE_SUFFIX = ".exit"
# Completion marker appended by the in-container command wrapper once the
# evaluation command has finished (see ``docker_func.wait_for_log_file_done``).
_legacy_LOG_COMPLETION_MARKER = "__DONE__"


def _legacy_parse_sample_id(sample_info, fallback_id):
    """Accept both public ID fields, retaining the legacy index fallback."""
    if not isinstance(sample_info, dict):
        raise ValueError("Each key-information entry must be an object.")
    raw_id = sample_info.get('sample_id', sample_info.get('anomaly_id', fallback_id))
    if isinstance(raw_id, bool) or not _util_re.fullmatch(r"[0-9]+", str(raw_id).strip()):
        raise ValueError(f"Invalid sample ID `{raw_id}`; expected a nonnegative integer.")
    return int(raw_id)


def _legacy_find_sample_directory(sample_base_directory, anomaly_id, database_name):
    """Locate a sample directory.

    The requested path is ``input_directory/<anomaly_id>``; for backward
    compatibility the ``<anomaly_id>_<database_name>`` naming scheme is also
    accepted. The first existing candidate is returned, otherwise the primary
    candidate is returned so the caller can report a meaningful "not found".
    """
    candidate_dirs = [
        _util_os.path.join(sample_base_directory, str(anomaly_id)),
        _util_os.path.join(sample_base_directory, f"{anomaly_id}_{database_name}"),
    ]
    for candidate_dir in candidate_dirs:
        if _util_os.path.isdir(candidate_dir):
            return candidate_dir
    return candidate_dirs[0]


def _legacy_default_user_config(dbms):
    """Return a sensible default database user configuration for the DBMS."""
    if _util_normalize_dbms(dbms) == "mysql":
        return {
            "DB_USER": "diagnosis",
            "DB_PASSWORD": "diagnosis",
            "DB_HOST": "localhost",
            "DB_PORT": "3306",
            "DB_NAME": "",
        }
    return {
        "DB_USER": "diagnosis",
        "DB_PASSWORD": "diagnosis",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "",
    }


def _legacy_default_hardware_config():
    """Return the default container hardware configuration.

    The filtering pipeline pins every sample container to a fixed footprint of
    4 CPU cores, 4 GB memory, 8 GB memory + swap and 64 GB storage so that the
    ``docker run`` invocation in ``docker_func.py`` matches the resources the
    anomaly evaluation expects.
    """
    return {
        "CPU": 4,
        "Memory": 4,
        "Memory + Swap": 8,
        "Storage": 64,
    }


def _legacy_load_sample_config(sample_directory):
    """Load a per-sample ``config.json`` if present, otherwise return ``{}``."""
    sample_config_path = _util_os.path.join(sample_directory, 'config.json')
    if not _util_os.path.exists(sample_config_path):
        return {}
    with open(sample_config_path, 'r', encoding='utf-8') as file:
        return _util_json.load(file)


def _legacy_read_anomaly_description(sample_directory):
    """Load anomaly metadata used by reproduction-oriented evaluations."""
    path = _util_os.path.join(sample_directory, 'anomaly_description.json')
    try:
        with open(path, 'r', encoding='utf-8') as file:
            return _util_json.load(file)
    except (OSError, _util_json.JSONDecodeError):
        return {}


def _legacy_normalize_success_criterion_value(value):
    normalized = str(value or "").strip().upper().replace(" ", "_").replace("-", "_")
    normalized = "_".join(part for part in normalized.split("_") if part)
    return normalized if normalized in _legacy_VALID_SUCCESS_CRITERIA else _legacy_DEFAULT_SUCCESS_CRITERION


def _legacy_normalize_anomaly_type_value(value):
    normalized = str(value or "").strip().upper().replace(" ", "")
    if normalized in _legacy_VALID_ANOMALY_TYPES:
        return normalized
    if normalized in {"OLAP+OLTP", "OLTP+OLAP"}:
        return "BOTH"
    return _legacy_DEFAULT_ANOMALY_TYPE


def _legacy_resolve_success_criterion(sample_info, anomaly_description):
    if "success_criterion" in _FILTER_OPTIONS:
        return _FILTER_OPTIONS["success_criterion"]
    return _legacy_normalize_success_criterion_value(
        sample_info.get('success_criterion') or anomaly_description.get('SuccessCriterion')
    )


def _legacy_resolve_anomaly_type(sample_info, anomaly_description):
    if "anomaly_type" in _FILTER_OPTIONS:
        return _FILTER_OPTIONS["anomaly_type"]
    return _legacy_normalize_anomaly_type_value(
        sample_info.get('anomaly_type') or anomaly_description.get('Type')
    )


def _legacy_anomaly_eval_supports_flag(anomaly_eval_path, flag):
    """Return whether an anomaly evaluator source declares a CLI flag."""
    name = Path(anomaly_eval_path).name
    embedded = _EMBEDDED_ASSETS.get("Scripts/" + name)
    if embedded is not None:
        return flag in embedded
    try:
        with open(anomaly_eval_path, 'r', encoding='utf-8', errors='replace') as file:
            return flag in file.read()
    except OSError:
        return False


def _legacy_anomaly_eval_supports_rep_flags(anomaly_eval_path):
    """Detect whether this sample's anomaly evaluator accepts reproduction flags."""
    return _legacy_anomaly_eval_supports_flag(anomaly_eval_path, _legacy_REP_EVAL_MARKER_FLAG)


def _legacy_build_filtered_anomaly_eval_command(
        anomaly_eval_command,
        injection_log_max_lines=_legacy_ANOMALY_INJECTION_LOG_MAX_LINES):
    """Wrap an evaluator command and limit streamed injection-script output.

    Both evaluator variants delimit the child anomaly-injection stream with
    stable START/END log messages. The wrapper preserves evaluator logs and its
    exit status while retaining only the first configured number of lines inside
    that interval. Critical evaluator failures are retained even after the limit
    or when an exception prevents the END marker.
    """
    injection_log_max_lines = max(0, int(injection_log_max_lines))
    truncated_message = _legacy_ANOMALY_INJECTION_OUTPUT_TRUNCATED
    if injection_log_max_lines != _legacy_ANOMALY_INJECTION_LOG_MAX_LINES:
        truncated_message = (
            "[FILTER_SYN_DATA] Anomaly injection command output truncated after "
            f"{injection_log_max_lines} lines."
        )
    filter_program = "\n".join([
        "import sys",
        f"start = {_legacy_ANOMALY_INJECTION_LOG_START!r}",
        f"end = {_legacy_ANOMALY_INJECTION_LOG_END!r}",
        f"max_lines = {injection_log_max_lines!r}",
        f"truncated_message = {truncated_message!r}",
        "critical = (",
        "    'Evaluation flow failed:',",
        "    'Anomaly simulation or verification failed.',",
        "    'Anomaly reproduction or verification failed.',",
        "    'Execution was interrupted by the user.',",
        ")",
        "inside = False",
        "retained_lines = 0",
        "truncation_reported = False",
        "for line in sys.stdin:",
        "    if start in line:",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
        "        inside = True",
        "        retained_lines = 0",
        "        truncation_reported = False",
        "        continue",
        "    if end in line:",
        "        inside = False",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
        "        continue",
        "    if not inside:",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
        "        continue",
        "    if retained_lines < max_lines:",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
        "        retained_lines += 1",
        "        continue",
        "    if not truncation_reported:",
        "        sys.stdout.write(truncated_message + '\\n')",
        "        sys.stdout.flush()",
        "        truncation_reported = True",
        "    if any(marker in line for marker in critical):",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
    ])
    evaluator_shell = " ".join(
        _docker_shlex.quote(str(argument)) for argument in anomaly_eval_command
    )
    filter_shell = " ".join([
        "python",
        "-u",
        "-c",
        _docker_shlex.quote(filter_program),
    ])
    return [
        "bash",
        "-o",
        "pipefail",
        "-c",
        f"{evaluator_shell} 2>&1 | {filter_shell}",
    ]


def _legacy_build_target_samples(key_information, logger):
    """Collect samples whose ``anomaly_id`` falls in the configured ID range.

    Returns a tuple of:
    - target_samples: ``[(idx, anomaly_id, sample_info), ...]``
    - non_included_sample_ids: IDs in ``[START_ID, END_ID)`` that do not appear
      in ``key_information.json``
    """
    target_samples = []
    seen_sample_ids = set()
    duplicate_sample_ids = set()
    start_id = _FILTER_OPTIONS.get("start_id", _legacy_START_ID)
    end_id = _FILTER_OPTIONS.get("end_id", _legacy_END_ID)

    for idx, sample_info in enumerate(key_information):
        try:
            anomaly_id = _legacy_parse_sample_id(sample_info, idx)
        except ValueError as exc:
            logger.warning(f"[MAIN][WARNING] Skip invalid sample entry #{idx}: {exc}")
            continue

        if not start_id <= anomaly_id < end_id:
            continue

        if anomaly_id in seen_sample_ids:
            duplicate_sample_ids.add(anomaly_id)
        seen_sample_ids.add(anomaly_id)
        target_samples.append((idx, anomaly_id, sample_info))

    if duplicate_sample_ids:
        logger.warning(
            "[MAIN][WARNING] Duplicate sample IDs detected in the configured "
            f"range: {sorted(duplicate_sample_ids)}"
        )

    non_included_sample_ids = sorted(set(range(start_id, end_id)) - seen_sample_ids)
    return target_samples, non_included_sample_ids






def _legacy_log_reached_completion_marker(log_path):
    """Return ``True`` only when the log's final line is ``__DONE__``.

    Mirrors ``docker_func.wait_for_log_file_done``: the in-container command
    wrapper appends this marker to the log only after the command truly
    finishes, so its presence means the corresponding pipeline stage ran to
    completion (as opposed to being killed part-way when the process died).
    """
    if not _util_os.path.isfile(log_path):
        return False
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as file:
            lines = file.read().splitlines()
    except OSError:
        return False
    return bool(lines) and lines[-1].strip() == _legacy_LOG_COMPLETION_MARKER








def _legacy_log_contains_any(log_path, expected_texts):
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as file:
            content = file.read()
    except OSError:
        return False
    return any(expected_text in content for expected_text in expected_texts)


def _legacy_read_log_exit_code(log_path):
    """Read the captured in-container exit code for a completed stage log.

    The exit code lives in the ``<log>.exit`` sentinel written by
    ``docker_func.build_exit_code_capture``. Returns the integer exit code, or
    ``None`` when the sentinel is missing / empty / unparsable (which the caller
    treats as "verdict indeterminate", i.e. not safely complete).
    """
    exit_code_path = log_path + _legacy_EVAL_EXIT_CODE_SUFFIX
    if not _util_os.path.isfile(exit_code_path):
        return None
    try:
        with open(exit_code_path, 'r', encoding='utf-8', errors='replace') as file:
            content = file.read().strip()
    except OSError:
        return None
    if content == "":
        return None
    try:
        return int(content)
    except ValueError:
        return None








def _legacy_load_existing_filter_result(filter_result_path):
    """Load an existing filter result file if present."""
    if not _util_os.path.exists(filter_result_path):
        return set(), set()

    with open(filter_result_path, 'r', encoding='utf-8') as file:
        filter_result = _util_json.load(file)

    passed_sample_ids = {
        int(sample_id)
        for sample_id in filter_result.get("passed_sample_ids", [])
    }
    failed_sample_ids = {
        int(sample_id)
        for sample_id in filter_result.get("failed_sample_ids", [])
    }
    return passed_sample_ids, failed_sample_ids


def _legacy_load_health_statuses(filter_result_path):
    if not _util_os.path.exists(filter_result_path):
        return {}
    try:
        with open(filter_result_path, 'r', encoding='utf-8') as file:
            result = _util_json.load(file)
    except (OSError, _util_json.JSONDecodeError):
        return {}
    statuses = result.get(_legacy_HEALTH_STATUS_RESULT_KEY, {})
    if not isinstance(statuses, dict):
        return {}
    return {str(sample_id): str(status) for sample_id, status in statuses.items()}


def _legacy_persist_filter_result(filter_result_path, passed_sample_ids, failed_sample_ids,
                          non_included_sample_ids, health_statuses=None):
    """Atomically persist the current in-progress filtering result."""
    if health_statuses is None:
        health_statuses = _legacy_load_health_statuses(filter_result_path)
    filter_result = {
        "passed_sample_ids": sorted(passed_sample_ids),
        "failed_sample_ids": sorted(failed_sample_ids),
        "non_included_sample_ids": sorted(non_included_sample_ids),
        _legacy_HEALTH_STATUS_RESULT_KEY: {
            str(sample_id): health_statuses[str(sample_id)]
            for sample_id in sorted(
                (str(key) for key in health_statuses),
                key=lambda value: (
                    (0, int(value)) if value.isdigit() else (1, value)
                ),
            )
        },
    }
    _util_os.makedirs(_util_os.path.dirname(filter_result_path), exist_ok=True)
    tmp_path = f"{filter_result_path}.tmp"
    with open(tmp_path, 'w', encoding='utf-8') as file:
        _util_json.dump(filter_result, file, ensure_ascii=False, indent=4)
    _util_os.replace(tmp_path, filter_result_path)


def _legacy_update_sample_result(anomaly_id, is_passed, passed_sample_ids, failed_sample_ids):
    """Update the in-memory result sets for one sample."""
    if is_passed:
        passed_sample_ids.add(anomaly_id)
        failed_sample_ids.discard(anomaly_id)
        return

    failed_sample_ids.add(anomaly_id)
    passed_sample_ids.discard(anomaly_id)


def _legacy_save_sample_result(filter_result_path, anomaly_id, is_passed,
                       passed_sample_ids, failed_sample_ids,
                       non_included_sample_ids, health_statuses,
                       health_status=None):
    """Update and persist one sample's filtering result."""
    _legacy_update_sample_result(anomaly_id, is_passed, passed_sample_ids, failed_sample_ids)
    if health_status is not None:
        health_statuses[str(anomaly_id)] = str(health_status)
    _legacy_persist_filter_result(
        filter_result_path,
        passed_sample_ids,
        failed_sample_ids,
        non_included_sample_ids,
        health_statuses,
    )


def _legacy_extract_base_timeout_from_script(script_path):
    """Extract ``# TIMEOUT THRESHOLD: <seconds>`` from the anomaly script."""
    with open(script_path, 'r', encoding='utf-8') as file:
        for line in file:
            if "# TIMEOUT THRESHOLD:" not in line:
                continue
            timeout_str = line.split("# TIMEOUT THRESHOLD:")[-1].strip().split()[0].strip()
            return int(timeout_str)
    raise ValueError(f"Timeout threshold marker not found in anomaly script: {script_path}")


def _legacy_calculate_merged_workload_execution_time(merged_workload_performance_path):
    """Sum execution time from ``merged_workload_performance.json``.

    When a record contains a negative execution time, treat it as an error case
    and use ``NEGATIVE_EXECUTION_TIME_FALLBACK`` seconds instead.
    """
    with open(merged_workload_performance_path, 'r', encoding='utf-8') as file:
        merged_workload_results = _util_json.load(file)

    total_execution_time = 0.0
    for result in merged_workload_results:
        execution_time = result.get('execution_time', 0.0)
        if not isinstance(execution_time, (int, float)):
            continue
        if execution_time < 0:
            total_execution_time += _legacy_NEGATIVE_EXECUTION_TIME_FALLBACK
        else:
            total_execution_time += execution_time
    return total_execution_time


def _legacy_calculate_dynamic_timeout_threshold(anomaly_inject_path, merged_workload_performance_path, median_runs):
    """Compute the runtime timeout threshold for one sample.

    The formula mirrors ``test_syn_ano.py``:
    ``base_timeout + median_runs * 2.5 * merged_workload_total_exec_time``,
    capped at 24 hours.
    """
    base_timeout_threshold = _legacy_extract_base_timeout_from_script(anomaly_inject_path)
    merged_workload_total_exec_time = _legacy_calculate_merged_workload_execution_time(
        merged_workload_performance_path
    )
    timeout_threshold = base_timeout_threshold + median_runs * 2.5 * merged_workload_total_exec_time
    if timeout_threshold > _legacy_MAX_TIMEOUT_SECONDS:
        _util_logging.warning(
            "[MAIN][WARNING] Timeout Threshold %s seconds exceeds 24 hours. "
            "Capping to %s seconds.",
            timeout_threshold,
            _legacy_MAX_TIMEOUT_SECONDS,
        )
    return min(timeout_threshold, _legacy_MAX_TIMEOUT_SECONDS)


def _legacy_initialize_docker_container(container_name, base_docker_name, container_config, logger):
    """Step 1: create the Docker container under a per-attempt UNIQUE name.

    ``container_name`` is a freshly generated unique name (see
    ``make_unique_container_name``). On every failed attempt we move to a BRAND
    NEW unique name via ``recreate_container_with_new_name`` instead of reusing
    the same name, so a slow daemon-side teardown of the previous attempt can no
    longer race the next ``docker run`` and trigger an ``RC 125`` name conflict
    (the root cause of the intermittent create-time waits under heavy parallel
    load). ``recreate_container_with_new_name`` reaps the previous container and
    its tmux session first, so no orphan accumulates.

    Neither this helper nor its callee relies on ``docker_container_reinit`` from
    ``docker_func`` -- that helper calls the builtin ``exit()`` on failure, which
    raises ``SystemExit(None)`` and would make the whole process terminate with
    status code 0 (the shell watchdog would then misread the premature exit as a
    successful evaluation). A genuine failure after every retry raises
    ``RuntimeError`` so the caller exits non-zero and the run is retried.

    Returns the live container name so the caller keeps tracking the container
    that is actually running.
    """
    logger.info(f"[MAIN] Initializing Docker Container [{container_name}] ...")
    initialization_times = 0
    while initialization_times < _legacy_RETRY_TIMES:
        if _docker_docker_container_init(container_name, container_config) is True:
            logger.info(f"[MAIN] Docker Container `{container_name}` Initialized Successfully.")
            return container_name

        logger.error(
            f"[MAIN][ERROR] Error in Initialize Docker Container `{container_name}`. "
            f"Retrying ({initialization_times + 1}/{_legacy_RETRY_TIMES})..."
        )
        initialization_times += 1
        _util_time.sleep(30)
        # Tear the failed attempt down and recreate under a NEW unique name so the
        # next ``docker run`` starts from a clean, conflict-free namespace.
        docker_init_status, container_name = _docker_recreate_container_with_new_name(
            container_name, base_docker_name, container_config
        )
        if docker_init_status is True:
            logger.info(f"[MAIN] Docker Container `{container_name}` Initialized Successfully.")
            return container_name

    logger.error(f"[MAIN][ERROR] Error in Initialize Docker Container `{container_name}`. EXIT()")
    raise RuntimeError(f"[MAIN][ERROR] Docker Container `{container_name}` initialization failed after {_legacy_RETRY_TIMES} retries.")


def _legacy_initialize_database(container_name, base_docker_name, init_db_config, container_config, database_name, logger):
    """Step 2: initialize the target database inside the container.

    On each failed attempt the container is recreated under a NEW unique name via
    ``recreate_container_with_new_name`` (never reusing a name, and never calling
    ``docker_container_reinit`` -- whose internal ``exit()`` would abort the
    process with status 0 and be misreported as success). A genuine failure after
    every retry raises ``RuntimeError`` so the caller exits non-zero and the run
    is retried.

    Returns the live container name so the caller keeps tracking the container
    that is actually running after any mid-init recreation.
    """
    initialization_times = 0
    while initialization_times < _legacy_RETRY_TIMES:
        semaphore = _legacy_DATABASE_INIT_SEMAPHORE
        if semaphore is not None:
            logger.info(
                f"[MAIN][DB_INIT] Waiting for an import slot for `{database_name}` "
                f"in container `{container_name}`."
            )
            semaphore.acquire()
        try:
            initialized = _docker_docker_container_init_db(
                container_name, init_db_config, session_name=container_name
            )
        finally:
            if semaphore is not None:
                semaphore.release()

        if initialized is True:
            logger.info(f"[MAIN] Database `{database_name}` Initialization in Docker Container `{container_name}` Succeeded.")
            return container_name

        logger.error(f"[MAIN][ERROR] Database `{database_name}` Initialization in Docker Container `{container_name}` Failed. Retrying ({initialization_times + 1}/{_legacy_RETRY_TIMES})...")
        initialization_times += 1
        _util_time.sleep(5)
        # Recreate the container under a NEW unique name before the next DB init.
        _, container_name = _docker_recreate_container_with_new_name(container_name, base_docker_name, container_config)

    logger.error(f"[MAIN][ERROR] Database `{database_name}` Initialization in Docker Container `{container_name}` Failed. EXIT()")
    raise RuntimeError(f"[MAIN][ERROR] Database `{database_name}` initialization in Docker Container `{container_name}` failed after {_legacy_RETRY_TIMES} retries.")


def _legacy_configure_database_init_parallelism(limit):
    """Restrict only bulk database imports, leaving evaluation workers concurrent."""
    global _legacy_DATABASE_INIT_SEMAPHORE
    _legacy_DATABASE_INIT_SEMAPHORE = _legacy_threading.BoundedSemaphore(limit) if limit > 0 else None


def _legacy_run_data_modification(docker_name, dbms, system_user_config, data_modification_path, database_name, output_dir, logger):
    """Step 3: apply the sample's ``data_modification.sql`` inside the container.

    Mirrors ``test_syn_ano.py`` (L519-531). Returns ``True`` on success.
    """
    logger.info("[MAIN] Modifying Data in Docker Container if Needed...")
    if data_modification_path is None or not _util_os.path.exists(data_modification_path):
        logger.warning("[MAIN][WARNING] Data Modification SQL File is not provided or does not exist.")
        return False

    cmd = _docker_build_sql_file_command(dbms, system_user_config, data_modification_path, database_name)
    obser_log = _util_os.path.join(output_dir, "data_modification_log.txt")
    state, _ = _docker_run_docker_exec_observation(docker_name, cmd, obser_log, 600, session_name=docker_name)
    if state is False:
        logger.warning(f"[MAIN][WARNING] Data Modification Failed. Check the log file for details : {obser_log}.")
    return state


def _legacy_run_anomaly_evaluation(docker_name, anomaly_eval_path, sample_files, system_user_config,
                           dbms, database_name, database_schema_name, timeout_threshold, median_runs,
                           is_rep, success_threshold_percent, min_abs_delta_seconds,
                           anomaly_type, success_criterion, output_dir, logger):
    """Step 4: execute the anomaly evaluation script inside the container.

    Mirrors the command construction used in ``test_syn_ano.py`` (L387-401).
    Optional flags are added only when the shipped evaluator declares them, so
    older per-sample ``anomaly_eval.py`` files remain runnable. A pass requires
    both exit code zero and the evaluator's explicit success sentence in the
    captured log.
    """
    temp_shell_script_path = sample_files['anomaly_inject.sh']
    verified_workload_path = sample_files['verified_workload.sql']
    initialization_workload_path = sample_files['initialization_workload.sql']

    anomaly_eval_command_list = [
        "python", anomaly_eval_path,
        "--script-path", temp_shell_script_path,
        "--verified-workload-path", verified_workload_path,
        "--initialization-workload-path", initialization_workload_path,
        "--host", str(system_user_config['DB_HOST']),
        "--port", str(system_user_config['DB_PORT']),
        "--user", str(system_user_config['DB_USER']),
        "--database", str(database_name),
        "--schema", str(database_schema_name),
        "--password", str(system_user_config['DB_PASSWORD']),
        "--timeout", str(timeout_threshold),
        "--median-runs", str(median_runs),
    ]

    if _legacy_anomaly_eval_supports_flag(anomaly_eval_path, "--dbms"):
        anomaly_eval_command_list[2:2] = ["--dbms", str(dbms)]
    elif _util_normalize_dbms(dbms) == "mysql":
        logger.warning(
            f"[MAIN][Filtering][WARNING] `{anomaly_eval_path}` does not declare "
            "`--dbms`; running without DBMS override. Ensure this evaluator is "
            "compatible with MySQL samples."
        )

    if is_rep:
        anomaly_eval_command_list.extend([
            "--success-threshold-percent", str(success_threshold_percent),
            "--min-abs-delta-seconds", str(min_abs_delta_seconds),
            "--anomaly-type", str(anomaly_type),
            "--success-criterion", str(success_criterion),
        ])

    filtered_eval_command = _legacy_build_filtered_anomaly_eval_command(
        anomaly_eval_command_list
    )
    logger.info(f"[MAIN][Filtering] - Executing Anomaly Evaluation with Timeout Threshold {timeout_threshold} ...")
    logger.info(
        f"[MAIN][Filtering] The first {_legacy_ANOMALY_INJECTION_LOG_MAX_LINES} lines "
        "of anomaly injection command output will be retained in the evaluation "
        "log; additional lines will be discarded."
    )
    obser_log = _util_os.path.join(output_dir, _legacy_ANOMALY_EVAL_LOG_NAME)
    command_ok, _ = _docker_run_docker_exec_observation(
        docker_name,
        filtered_eval_command,
        obser_log,
        timeout_threshold,
        session_name=docker_name,
    )
    marker_found = _legacy_log_contains_any(obser_log, _legacy_ANOMALY_SUCCESS_MARKERS)
    if not command_ok and _legacy_log_contains_any(obser_log, _legacy_EVALUATOR_CLI_ERROR_MARKERS):
        logger.error(
            f"[MAIN][Filtering][ERROR] Anomaly evaluator CLI argument mismatch "
            f"detected in `{obser_log}`. This is an evaluator/filter-script "
            "compatibility error, not an anomaly synthesis failure."
        )
    if command_ok and not marker_found:
        logger.error(
            f"[MAIN][Filtering] Evaluation exited successfully but none of "
            f"`{_legacy_ANOMALY_SUCCESS_MARKERS}` was found in `{obser_log}`."
        )
    return command_ok and marker_found


def _legacy_capture_post_injection_health(
        container_name, dbms, system_user_config, host_port,
        database_name, database_schema_name, output_dir, logger):
    """Capture advisory health after anomaly evaluation without changing verdict."""
    report_path = _util_os.path.join(output_dir, _legacy_POST_INJECTION_HEALTH_REPORT_NAME)
    report = None
    try:
        if _health_judge_database_health is None:
            raise RuntimeError("Scripts.health_report could not be imported")
        db_config = {
            "dbms": dbms,
            "host": "localhost",
            "host_port": int(host_port),
            "user": system_user_config["DB_USER"],
            "password": system_user_config["DB_PASSWORD"],
            "db_name": database_name,
            "schema": database_schema_name,
        }
        report = _health_judge_database_health(
            db_config,
            logger=logger,
            container_name=container_name,
        )
        if not isinstance(report, dict):
            raise ValueError("health probe returned a non-dictionary report")
    except Exception as exc:
        logger.warning(
            f"[MAIN][HEALTH][WARNING] Post-injection health collection failed: {exc}"
        )
        report = {
            "healthy": None,
            "operational_status": _legacy_HEALTH_STATUS_INDETERMINATE,
            "collection_error": str(exc),
            "container_name": container_name,
            "database": database_name,
        }

    try:
        if _health_write_database_state_report is not None:
            _health_write_database_state_report(report, report_path)
        else:
            _legacy_write_json_atomic(report_path, report)
    except Exception as exc:
        logger.warning(
            f"[MAIN][HEALTH][WARNING] Failed to write `{report_path}`: {exc}"
        )
    return str(
        report.get("operational_status") or _legacy_HEALTH_STATUS_INDETERMINATE
    )


class _legacy_SampleResultStore:
    """Thread-safe store for the incremental filtering result.

    Every mutation funnels through :meth:`record` under a single lock so that
    concurrent workers can update the shared passed / failed sets and rewrite
    ``filter_result.json`` without racing or corrupting the file.
    """

    def __init__(self, filter_result_path, passed_sample_ids, failed_sample_ids,
                 non_included_sample_ids, health_statuses=None):
        self._filter_result_path = filter_result_path
        self._passed_sample_ids = set(passed_sample_ids)
        self._failed_sample_ids = set(failed_sample_ids)
        self._non_included_sample_ids = list(non_included_sample_ids)
        self._health_statuses = dict(
            health_statuses
            if health_statuses is not None
            else _legacy_load_health_statuses(filter_result_path)
        )
        self._lock = _legacy_threading.Lock()

    def record(self, anomaly_id, is_passed, health_status=None):
        """Update and atomically persist one sample's result."""
        with self._lock:
            _legacy_save_sample_result(
                self._filter_result_path, anomaly_id, is_passed,
                self._passed_sample_ids, self._failed_sample_ids, self._non_included_sample_ids,
                self._health_statuses, health_status,
            )

    def discard(self, anomaly_id):
        with self._lock:
            self._passed_sample_ids.discard(anomaly_id)
            self._failed_sample_ids.discard(anomaly_id)

    def persist(self):
        with self._lock:
            _legacy_persist_filter_result(
                self._filter_result_path,
                self._passed_sample_ids,
                self._failed_sample_ids,
                self._non_included_sample_ids,
                self._health_statuses,
            )

    def counts(self):
        with self._lock:
            return len(self._passed_sample_ids), len(self._failed_sample_ids)


def _legacy_process_single_sample(sample_task, worker_base_docker_name, worker_docker_port,
                          base_dir, output_directory, sample_base_directory,
                          median_runs, result_store, isolate_resources,
                          collect_health_report, logger):
    """Evaluate a single synthetic anomaly sample end-to-end.

    This is the per-sample body shared by the serial and parallel drivers.
    ``worker_base_docker_name`` is this worker's stable NAME PREFIX and
    ``worker_docker_port`` its dedicated host port, so concurrent workers never
    collide on either resource. The actual container for each sample is created
    under a per-sample UNIQUE name derived from that prefix (see
    ``make_unique_container_name``); names are never reused, so a slow daemon-side
    teardown of the previous sample's container can no longer race the next
    ``docker run`` and stall on an ``RC 125`` name conflict. The unique name
    doubles as the tmux session name, and it is force-removed together with its
    session in a ``finally`` block so the steady state stays at one
    container/session per worker (no orphan accumulates). Results are persisted
    through the thread-safe ``result_store``. A ``RuntimeError`` (Docker /
    database initialization exhausted its retries) is propagated so the caller
    can abort the whole run with a non-zero status; every other error is recorded
    as a sample failure and swallowed so the batch continues.
    """
    _idx, anomaly_id, sample_info = sample_task
    database_name = sample_info.get('database_name', "")
    database_schema_name = sample_info.get('database_schema_name', "")
    sample_dir_name = str(anomaly_id)

    # Per-sample output directory (holds the initialization / data modification
    # / anomaly evaluation logs for this sample).
    output_dir = _util_os.path.join(output_directory, sample_dir_name)

    # Live container name for this sample. Assigned once the per-sample unique
    # name is generated below; kept as ``None`` until then so the ``finally``
    # cleanup is a no-op if we bail out before any container is created.
    container_name = None

    try:
        _util_os.makedirs(output_dir, exist_ok=True)

        logger.info(f"[MAIN][Filtering] Processing Sample `{sample_dir_name}` "
                    f"({sample_info.get('anomaly_name', '')}) for Database `{database_name}` "
                    f"on Worker `{worker_base_docker_name}` (host port {worker_docker_port})")

        # -- Locate the sample directory : input_directory/<anomaly_id> --
        sample_directory = _legacy_find_sample_directory(sample_base_directory, anomaly_id, database_name)
        if not _util_os.path.isdir(sample_directory):
            logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` FAILED : sample directory not found: {sample_directory}")
            result_store.record(anomaly_id, False)
            return

        # -- Validate that every required file exists in the sample dir. --
        sample_files = {file_name: _util_os.path.join(sample_directory, file_name) for file_name in _legacy_REQUIRED_SAMPLE_FILES}
        missing_files = [file_name for file_name, file_path in sample_files.items() if not _util_os.path.exists(file_path)]
        if missing_files:
            logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` FAILED : missing required sample files: {missing_files}")
            result_store.record(anomaly_id, False)
            return

        timeout_threshold = _legacy_calculate_dynamic_timeout_threshold(
            sample_files['anomaly_inject.sh'],
            sample_files['merged_workload_performance.json'],
            median_runs,
        )
        logger.info(
            f"[MAIN][Filtering] Computed Dynamic Timeout Threshold for Sample "
            f"`{sample_dir_name}`: {timeout_threshold} seconds"
        )

        # -- Read the decoupled per-sample configuration. --
        sample_config = _legacy_load_sample_config(sample_directory)
        sample_config.update({k: v for k, v in _FILTER_OPTIONS.items() if k in ('success_threshold_percent', 'min_abs_delta_seconds')})
        DBMS = sample_config.get('dbms', "PostgreSQL 16.11")
        database_name = sample_config.get('database_name', database_name)
        DB_TYPE = _util_resolve_db_type(sample_config, DBMS)
        DB_SCHEMA_NAME = sample_config.get(
            'database_schema_name',
            sample_info.get('database_schema_name', "diagnosis"),
        )
        database_schema_name = sample_config.get('database_schema_name', database_schema_name or DB_SCHEMA_NAME)
        if not database_schema_name:
            database_schema_name = DB_SCHEMA_NAME
        SYSTEM_USER_CONFIG_JSON = sample_config.get('system_user_config', _legacy_default_user_config(DBMS))
        SYSTEM_HARDWARE_CONFIG = sample_config.get('system_hardware_config', _legacy_default_hardware_config())

        # The anomaly evaluation script ships inside the sample directory.
        anomaly_eval_path = _embedded_evaluator(sample_config, sample_files)
        anomaly_eval_path = _util_resolve_path_from_base(anomaly_eval_path, base_dir)
        if not _util_os.path.exists(anomaly_eval_path):
            logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` FAILED : anomaly evaluation script not found: {anomaly_eval_path}")
            result_store.record(anomaly_id, False)
            return

        is_rep = _legacy_anomaly_eval_supports_rep_flags(anomaly_eval_path)
        anomaly_description = _legacy_read_anomaly_description(sample_directory)
        success_threshold_percent = sample_config.get(
            'success_threshold_percent', _legacy_DEFAULT_SUCCESS_THRESHOLD_PERCENT
        )
        min_abs_delta_seconds = sample_config.get(
            'min_abs_delta_seconds', _legacy_DEFAULT_MIN_ABS_DELTA_SECONDS
        )
        anomaly_type = _legacy_resolve_anomaly_type(sample_info, anomaly_description)
        success_criterion = _legacy_resolve_success_criterion(sample_info, anomaly_description)
        logger.info(
            f"[MAIN][Filtering] Sample `{sample_dir_name}` uses "
            f"{'reproduction' if is_rep else 'legacy'} anomaly evaluation."
        )

        container_config = _docker_build_container_config(
            DBMS,
            SYSTEM_HARDWARE_CONFIG,
            worker_docker_port,
            SYSTEM_USER_CONFIG_JSON,
        )

        # ---- Step 1 : Docker container initialization. ----
        # Create under a per-sample UNIQUE name derived from this worker's stable
        # prefix. ``initialize_docker_container`` may move to a fresh unique name
        # on a retry, so track whatever it returns as the live container name.
        container_name = _docker_make_unique_container_name(worker_base_docker_name)
        container_name = _legacy_initialize_docker_container(container_name, worker_base_docker_name, container_config, logger)

        # ---- Step 2 : Database initialization inside the container. ----
        logger.info(f"[MAIN] Initializing Database in Docker Container `{container_name}`...")
        tmp_dbms_name = DBMS.replace(" ", "_")
        # In parallel mode give each worker its own TMP sub-tree so concurrent
        # dump / load staging cannot clash on shared files; serial runs keep the
        # original single-level layout for backward compatibility. Keyed on the
        # STABLE worker prefix (not the per-sample unique name) so the sub-tree is
        # reused across samples instead of leaking one directory per sample.
        worker_tmp_dbms_name = _util_os.path.join(tmp_dbms_name, worker_base_docker_name) if isolate_resources else tmp_dbms_name
        tmp_directory = _docker_initialize_shared_tmp_directory(base_dir, worker_tmp_dbms_name)
        init_db_config = {
            "dbms": DBMS,
            "base_dir": base_dir,
            "db_host": SYSTEM_USER_CONFIG_JSON['DB_HOST'],
            "db_port": SYSTEM_USER_CONFIG_JSON['DB_PORT'],
            "db_name": database_name,
            "db_type": DB_TYPE,
            "db_schema": DB_SCHEMA_NAME,
            "db_user": SYSTEM_USER_CONFIG_JSON['DB_USER'],
            "password": SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
            "timeout": _legacy_DATABASE_INIT_TIMEOUT_SECONDS,
            "output_dir": output_dir,
            "tmp_directory": tmp_directory,
        }
        container_name = _legacy_initialize_database(container_name, worker_base_docker_name, init_db_config, container_config, database_name, logger)

        # ---- Step 3 : Data modification execution. ----
        data_modification_path = sample_files['data_modification.sql']
        if _legacy_run_data_modification(container_name, DBMS, SYSTEM_USER_CONFIG_JSON, data_modification_path,
                                 database_name, output_dir, logger) is False:
            logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` FAILED : data modification failed.")
            _docker_reset_tmux_window(session_name=container_name, window_name="observation")
            result_store.record(anomaly_id, False)
            return

        # ---- Step 4 : Anomaly evaluation code execution. ----
        state = _legacy_run_anomaly_evaluation(
            container_name, anomaly_eval_path, sample_files, SYSTEM_USER_CONFIG_JSON,
            DBMS, database_name, database_schema_name, timeout_threshold, median_runs,
            is_rep, success_threshold_percent, min_abs_delta_seconds,
            anomaly_type, success_criterion, output_dir, logger,
        )

        health_status = None
        if collect_health_report:
            # Advisory only: this status is persisted independently and never
            # participates in the anomaly-evaluation pass/fail decision.
            health_status = _legacy_capture_post_injection_health(
                container_name,
                DBMS,
                SYSTEM_USER_CONFIG_JSON,
                worker_docker_port,
                database_name,
                database_schema_name,
                output_dir,
                logger,
            )

        # Decide success / failure from the anomaly evaluation result.
        if state is True:
            logger.info(f"[MAIN][Filtering] Sample `{sample_dir_name}` PASSED the anomaly evaluation.")
        else:
            logger.error(f"[MAIN][Filtering] Sample `{sample_dir_name}` FAILED the anomaly evaluation.")
            _docker_reset_tmux_window(session_name=container_name, window_name="observation")
        result_store.record(
            anomaly_id, state is True, health_status=health_status
        )

    except Exception as exc:
        if isinstance(exc, RuntimeError):
            # Docker / database initialization failure : abort the whole run so
            # the shell watchdog restarts it from the resume checkpoint.
            raise
        # Non-initialization sample errors are recorded and the batch continues.
        logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` raised an unexpected error during evaluation: {exc}")
        result_store.record(anomaly_id, False)
    finally:
        # Synchronously reap THIS sample's container + tmux session before moving
        # on. Because the next sample always gets a brand new unique name, this
        # teardown never blocks the next ``docker run``; it just keeps the steady
        # state at one container/session per worker so no orphan accumulates
        # (the daemon may finish the removal lazily in the background).
        if container_name is not None:
            _docker_kill_tmux_session(container_name)
            _docker_docker_container_del(container_name)


def _legacy_run_evaluation_workers(target_samples, worker_count, base_dir, docker_name, docker_port,
                           median_runs, output_directory, sample_base_directory,
                           result_store, collect_health_report, logger):
    """Drive sample evaluation with ``worker_count`` concurrent workers.

    Samples are pulled from a shared queue; each worker owns a dedicated NAME
    PREFIX (``<docker_name>_w<index>``) and host port
    (``docker_port + index * WORKER_PORT_STRIDE``). Every sample is run in a
    container created under a per-sample UNIQUE name derived from that prefix, so
    concurrent workers never collide and a slow teardown never blocks the next
    ``docker run``. ``worker_count == 1`` keeps the original single-prefix / port
    for full backward compatibility. The first ``RuntimeError`` raised by any
    worker (initialization exhausted its retries) stops the remaining workers and
    is re-raised in the caller so the process exits non-zero and the run is
    retried.

    Before starting, and once every worker has drained the queue, each worker's
    stale containers / tmux sessions are swept by prefix so a previously
    crashed / killed run leaves no orphan behind.
    """
    isolate_resources = worker_count > 1

    # Per-worker stable name prefixes (identical to the naming used inside the
    # worker threads). Sweep any orphan containers / tmux sessions left by a
    # previously interrupted run BEFORE spawning workers, and again at the end.
    def worker_base_name(worker_index):
        return f"{docker_name}_w{worker_index}" if isolate_resources else docker_name

    worker_prefixes = [worker_base_name(worker_index) for worker_index in range(worker_count)]

    logger.info(f"[MAIN] Sweeping stale containers/sessions for worker prefixes {worker_prefixes} before start ...")
    for worker_prefix in worker_prefixes:
        _docker_remove_containers_by_prefix(worker_prefix)
        _docker_kill_tmux_sessions_by_prefix(worker_prefix)

    task_queue = _legacy_queue.Queue()
    for sample_task in target_samples:
        task_queue.put(sample_task)

    progress_bar = _legacy_tqdm(
        total=len(target_samples),
        desc="Filtering Synthetic Anomaly Samples",
    )
    progress_lock = _legacy_threading.Lock()
    stop_event = _legacy_threading.Event()
    fatal_error = {}
    fatal_lock = _legacy_threading.Lock()

    def worker(worker_index):
        worker_base_docker_name = worker_base_name(worker_index)
        worker_docker_port = docker_port + worker_index * _legacy_WORKER_PORT_STRIDE

        while not stop_event.is_set():
            try:
                sample_task = task_queue.get_nowait()
            except _legacy_queue.Empty:
                return

            try:
                _legacy_process_single_sample(
                    sample_task, worker_base_docker_name, worker_docker_port,
                    base_dir, output_directory, sample_base_directory,
                    median_runs, result_store, isolate_resources,
                    collect_health_report, logger,
                )
            except (RuntimeError, SystemExit) as exc:
                # RuntimeError  : Docker / database initialization exhausted its
                #                 retries.
                # SystemExit    : an ``exit()`` fired deep inside an imported
                #                 helper (e.g. TMP-permission failure in
                #                 ``initialize_shared_tmp_directory``). In a
                #                 worker thread that would otherwise kill the
                #                 thread silently and let the main thread report
                #                 a false success, so escalate it to fatal.
                with fatal_lock:
                    fatal_error.setdefault("error", exc)
                stop_event.set()
                return
            finally:
                with progress_lock:
                    progress_bar.update(1)

    threads = [
        _legacy_threading.Thread(target=worker, args=(worker_index,), name=f"filter-worker-{worker_index}")
        for worker_index in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    try:
        for thread in threads:
            thread.join()
    finally:
        progress_bar.close()
        # Final sweep: once every worker has drained the queue (or the run was
        # aborted), reap any container / tmux session still lingering under each
        # worker prefix so a crashed / killed run leaves no orphan behind.
        logger.info(f"[MAIN] Sweeping stale containers/sessions for worker prefixes {worker_prefixes} after finish ...")
        for worker_prefix in worker_prefixes:
            _docker_remove_containers_by_prefix(worker_prefix)
            _docker_kill_tmux_sessions_by_prefix(worker_prefix)

    if "error" in fatal_error:
        error = fatal_error["error"]
        if isinstance(error, SystemExit):
            # A bare ``exit()`` carries code ``None`` -> exit status 0, which the
            # shell watchdog would misread as success. Re-raise as a RuntimeError
            # so the process fails loudly with a non-zero status instead.
            raise RuntimeError(
                f"[MAIN][ERROR] A worker aborted via exit()/SystemExit "
                f"(code={error.code}); treating the run as failed."
            )
        raise error




def _legacy_write_json_atomic(path, value):
    _util_os.makedirs(_util_os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, 'w', encoding='utf-8') as file:
        _util_json.dump(value, file, ensure_ascii=False, indent=4)
    _util_os.replace(tmp_path, path)


def _legacy_workload_log_observation(log_path):
    """Return stable command output, including warnings/errors but not timing."""
    exit_code = _legacy_read_log_exit_code(log_path)
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as file:
            lines = file.read().splitlines()
    except OSError as exc:
        lines = [f"<LOG_READ_ERROR: {exc}>"]

    normalized_lines = []
    for line in lines:
        if line.strip() == _legacy_LOG_COMPLETION_MARKER:
            continue
        # psql's optional \timing output is runtime noise, not a query result.
        if line.lstrip().startswith("Time:") and line.rstrip().endswith(" ms"):
            continue
        normalized_lines.append(line)
    return {
        "completed": _legacy_log_reached_completion_marker(log_path),
        "exit_code": exit_code,
        "output": normalized_lines,
    }


def _legacy_run_workload_consistency_rounds(container_name, dbms, user_config,
                                    database_name, sample_files, output_dir,
                                    timeout_seconds, logger):
    """Run initialization + verified workload three times and compare output."""
    observations = []
    for round_number in range(1, _legacy_THIRD_FILTER_ROUNDS + 1):
        round_dir = _util_os.path.join(output_dir, f"round_{round_number}")
        _util_os.makedirs(round_dir, exist_ok=True)
        round_result = {}
        for workload_name, file_name in (
            ("initialization", "initialization_workload.sql"),
            ("verified", "verified_workload.sql"),
        ):
            log_path = _util_os.path.join(round_dir, f"{workload_name}_workload.log")
            command = _docker_build_sql_file_command(
                dbms, user_config, sample_files[file_name], database_name
            )
            command_ok, _ = _docker_run_docker_exec_observation(
                container_name,
                command,
                log_path,
                timeout_seconds,
                session_name=container_name,
            )
            observation = _legacy_workload_log_observation(log_path)
            observation["command_ok"] = bool(command_ok)
            round_result[workload_name] = observation
        _legacy_write_json_atomic(_util_os.path.join(round_dir, "round_result.json"), round_result)
        observations.append(round_result)
        logger.info(
            f"[MAIN][THIRD_FILTER] Completed workload round {round_number}/"
            f"{_legacy_THIRD_FILTER_ROUNDS}."
        )

    is_consistent = all(item == observations[0] for item in observations[1:])
    return is_consistent, observations




def _legacy_process_third_filter_sample(sample_task, worker_base_docker_name,
                                worker_docker_port, base_dir, output_directory,
                                sample_base_directory, workload_timeout,
                                result_store, isolate_resources, logger):
    """Validate one stage-two passing sample in a fresh healthy environment."""
    _idx, anomaly_id, sample_info = sample_task
    sample_dir_name = str(anomaly_id)
    output_dir = _util_os.path.join(output_directory, sample_dir_name)
    container_name = None
    verdict_path = _util_os.path.join(output_dir, "third_filter_result.json")

    try:
        _util_os.makedirs(output_dir, exist_ok=True)
        database_name = sample_info.get('database_name', "")
        sample_directory = _legacy_find_sample_directory(
            sample_base_directory, anomaly_id, database_name
        )
        sample_files = {
            file_name: _util_os.path.join(sample_directory, file_name)
            for file_name in _legacy_REQUIRED_SAMPLE_FILES
        }
        missing_files = [
            name for name, path in sample_files.items() if not _util_os.path.exists(path)
        ]
        if missing_files:
            raise ValueError(f"missing required sample files: {missing_files}")

        sample_config = _legacy_load_sample_config(sample_directory)
        sample_config.update({k: v for k, v in _FILTER_OPTIONS.items() if k in ('success_threshold_percent', 'min_abs_delta_seconds')})
        dbms = sample_config.get('dbms', "PostgreSQL 16.11")
        database_name = sample_config.get('database_name', database_name)
        db_type = _util_resolve_db_type(sample_config, dbms)
        schema_name = sample_config.get(
            'database_schema_name',
            sample_info.get('database_schema_name', "diagnosis"),
        )
        user_config = sample_config.get(
            'system_user_config', _legacy_default_user_config(dbms)
        )
        hardware_config = sample_config.get(
            'system_hardware_config', _legacy_default_hardware_config()
        )
        container_config = _docker_build_container_config(
            dbms, hardware_config, worker_docker_port, user_config
        )

        container_name = _docker_make_unique_container_name(worker_base_docker_name)
        container_name = _legacy_initialize_docker_container(
            container_name, worker_base_docker_name, container_config, logger
        )
        tmp_dbms_name = dbms.replace(" ", "_")
        worker_tmp_name = (
            _util_os.path.join(tmp_dbms_name, worker_base_docker_name)
            if isolate_resources else tmp_dbms_name
        )
        init_db_config = {
            "dbms": dbms,
            "base_dir": base_dir,
            "db_host": user_config['DB_HOST'],
            "db_port": user_config['DB_PORT'],
            "db_name": database_name,
            "db_type": db_type,
            "db_schema": schema_name,
            "db_user": user_config['DB_USER'],
            "password": user_config['DB_PASSWORD'],
            "timeout": _legacy_DATABASE_INIT_TIMEOUT_SECONDS,
            "output_dir": output_dir,
            "tmp_directory": _docker_initialize_shared_tmp_directory(
                base_dir, worker_tmp_name
            ),
        }
        container_name = _legacy_initialize_database(
            container_name, worker_base_docker_name, init_db_config,
            container_config, database_name, logger
        )

        # Data modification is part of the sample's healthy baseline, while the
        # anomaly injection is deliberately not executed in this stage.
        if not _legacy_run_data_modification(
            container_name, dbms, user_config,
            sample_files['data_modification.sql'], database_name,
            output_dir, logger,
        ):
            raise ValueError("third-filter data modification failed")

        is_consistent, observations = _legacy_run_workload_consistency_rounds(
            container_name, dbms, user_config, database_name, sample_files,
            output_dir, workload_timeout, logger,
        )
        _legacy_write_json_atomic(verdict_path, {
            "sample_id": anomaly_id,
            "completed": True,
            "passed": is_consistent,
            "round_count": _legacy_THIRD_FILTER_ROUNDS,
            "comparison_rule": (
                "initialization and verified workload exit status plus normalized "
                "stdout/stderr must match in all rounds"
            ),
            "rounds": observations,
        })
        result_store.record(anomaly_id, is_consistent)
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error(
            f"[MAIN][THIRD_FILTER][ERROR] Sample `{sample_dir_name}` failed: {exc}"
        )
        _legacy_write_json_atomic(verdict_path, {
            "sample_id": anomaly_id,
            "completed": True,
            "passed": False,
            "error": str(exc),
        })
        result_store.record(anomaly_id, False)
    finally:
        if container_name is not None:
            _docker_kill_tmux_session(container_name)
            _docker_docker_container_del(container_name)


def _legacy_run_third_filter_workers(target_samples, worker_count, base_dir, docker_name,
                             docker_port, output_directory, sample_base_directory,
                             workload_timeout, result_store, logger):
    isolate_resources = worker_count > 1
    task_queue = _legacy_queue.Queue()
    for task in target_samples:
        task_queue.put(task)
    worker_prefixes = [
        f"{docker_name}_third_w{index}" if isolate_resources else f"{docker_name}_third"
        for index in range(worker_count)
    ]
    for prefix in worker_prefixes:
        _docker_remove_containers_by_prefix(prefix)
        _docker_kill_tmux_sessions_by_prefix(prefix)

    fatal_error = {}
    fatal_lock = _legacy_threading.Lock()
    progress = _legacy_tqdm(total=len(target_samples), desc="Third-Level Filtering")

    def worker(index):
        while True:
            try:
                task = task_queue.get_nowait()
            except _legacy_queue.Empty:
                return
            try:
                _legacy_process_third_filter_sample(
                    task, worker_prefixes[index],
                    docker_port + index * _legacy_WORKER_PORT_STRIDE,
                    base_dir, output_directory, sample_base_directory,
                    workload_timeout, result_store, isolate_resources, logger,
                )
            except (RuntimeError, SystemExit) as exc:
                with fatal_lock:
                    fatal_error.setdefault("error", exc)
                return
            finally:
                progress.update(1)

    threads = [
        _legacy_threading.Thread(target=worker, args=(index,), name=f"third-filter-{index}")
        for index in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    progress.close()
    for prefix in worker_prefixes:
        _docker_remove_containers_by_prefix(prefix)
        _docker_kill_tmux_sessions_by_prefix(prefix)
    if fatal_error:
        raise RuntimeError(
            f"[MAIN][THIRD_FILTER] Worker aborted: {fatal_error['error']}"
        )





# ======================== test_filter_syn_data_v1.py ========================
"""V1 synthetic anomaly filtering with non-terminal workload observations.

Existing first-filter results complete their legacy follow-up stages before
new first-filter work begins. Samples that have not reached the first filter
run initialization before every verified workload, and record verified-workload
output consistency before anomaly injection, but workload consistency never
rejects a sample by itself.
"""

import argparse as argparse
import copy as copy
import hashlib as hashlib
import json as json
import logging as logging
import os as os
import queue as queue
import shutil as shutil
import tempfile as tempfile
import threading as threading





V1_FIRST_RESULT_FILENAME = "0_filter_result.json"
V1_SECOND_RESULT_FILENAME = "1_filter_result.json"
V1_EVIDENCE_FILENAME = "v1_workload_consistency.json"
V1_FORMAT_VERSION = 5
DEFAULT_WORKLOAD_VALIDATION_ROUNDS = 3
DEFAULT_WORKLOAD_VALIDATION_TIMEOUT = 3600
SUCCESS_MARKERS = (
    "Anomaly successfully simulated and verified.",
    "Anomaly successfully reproduced and verified.",
)


def write_json_atomic(path, value):
    _legacy_write_json_atomic(path, value)


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return default


def normalize_ids(values):
    normalized = set()
    for value in values or []:
        try:
            normalized.add(int(value))
        except (TypeError, ValueError):
            continue
    return normalized


def split_sql_statements(sql_text):
    """Split on top-level semicolons without changing statement text or order."""
    statements = []
    statement_start = 0
    index = 0
    state = None
    dollar_quote = None
    length = len(sql_text)

    while index < length:
        character = sql_text[index]
        following = sql_text[index:index + 2]
        if state is None:
            if following == "--":
                state = "line_comment"
                index += 2
                continue
            if following == "/*":
                state = "block_comment"
                index += 2
                continue
            if character == "'":
                state = "single_quote"
            elif character == '"':
                state = "double_quote"
            elif character == "`":
                state = "backtick_quote"
            elif character == "$":
                delimiter_end = sql_text.find("$", index + 1)
                if delimiter_end != -1:
                    candidate = sql_text[index:delimiter_end + 1]
                    if candidate == "$$" or (
                            candidate[1:-1].replace("_", "a").isalnum()
                            and not candidate[1:-1][0].isdigit()):
                        dollar_quote = candidate
                        state = "dollar_quote"
                        index = delimiter_end + 1
                        continue
            elif character == ";":
                statements.append(sql_text[statement_start:index + 1])
                statement_start = index + 1
        elif state == "line_comment":
            if character == "\n":
                state = None
        elif state == "block_comment":
            if following == "*/":
                state = None
                index += 2
                continue
        elif state == "single_quote":
            if character == "'" and sql_text[index + 1:index + 2] == "'":
                index += 2
                continue
            if character == "'":
                state = None
        elif state == "double_quote":
            if character == '"' and sql_text[index + 1:index + 2] == '"':
                index += 2
                continue
            if character == '"':
                state = None
        elif state == "backtick_quote":
            if character == "`" and sql_text[index + 1:index + 2] == "`":
                index += 2
                continue
            if character == "`":
                state = None
        elif state == "dollar_quote":
            if sql_text.startswith(dollar_quote, index):
                state = None
                index += len(dollar_quote)
                continue
        index += 1

    if sql_text[statement_start:].strip():
        statements.append(sql_text[statement_start:])
    return statements


def create_marked_verified_workload(dbms, source_path):
    """Create a temporary workload that frames every SQL statement's output."""
    with open(source_path, "r", encoding="utf-8") as source_file:
        source_sql = source_file.read()

    statements = split_sql_statements(source_sql)
    marker_seed = hashlib.sha256(
        (os.path.abspath(source_path) + "\0" + source_sql).encode("utf-8")
    ).hexdigest()[:16]
    marker_prefix = "__DB_DIAG_WORKLOAD_%s_" % marker_seed
    is_mysql = _util_normalize_dbms(dbms) == "mysql"
    fragments = []
    for statement_index, statement in enumerate(statements, start=1):
        begin_marker = "%sBEGIN_%04d__" % (marker_prefix, statement_index)
        end_marker = "%sEND_%04d__" % (marker_prefix, statement_index)
        if is_mysql:
            fragments.extend((
                "SELECT '%s' AS __db_diag_workload_marker__;\n" % begin_marker,
                statement,
                "\nSELECT '%s' AS __db_diag_workload_marker__;\n" % end_marker,
            ))
        else:
            fragments.extend((
                "\\echo %s\n" % begin_marker,
                statement,
                "\n\\echo %s\n" % end_marker,
            ))

    descriptor, marked_path = tempfile.mkstemp(
        prefix=".db_diag_verified_",
        suffix=".sql",
        dir=os.path.dirname(os.path.abspath(source_path)),
        text=True,
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as marked_file:
        marked_file.write("".join(fragments))
    return marked_path, marker_prefix, len(statements)


def parse_marked_verified_observation(observation, marker_prefix, statement_count):
    """Preserve SQL order while reducing each statement's output to a set."""
    begin_markers = {
        "%sBEGIN_%04d__" % (marker_prefix, index): index
        for index in range(1, statement_count + 1)
    }
    end_markers = {
        "%sEND_%04d__" % (marker_prefix, index): index
        for index in range(1, statement_count + 1)
    }
    statement_lines = {index: [] for index in range(1, statement_count + 1)}
    unframed_lines = []
    active_index = None
    seen_begin = set()
    seen_end = set()
    parse_complete = True

    for line in observation.get("output", []):
        matching_begin = next(
            (index for marker, index in begin_markers.items() if marker in line),
            None,
        )
        matching_end = next(
            (index for marker, index in end_markers.items() if marker in line),
            None,
        )
        if matching_begin is not None:
            if active_index is not None or matching_begin in seen_begin:
                parse_complete = False
            active_index = matching_begin
            seen_begin.add(matching_begin)
            continue
        if matching_end is not None:
            if active_index != matching_end or matching_end in seen_end:
                parse_complete = False
            active_index = None
            seen_end.add(matching_end)
            continue
        if active_index is None:
            unframed_lines.append(line)
        else:
            statement_lines[active_index].append(line)

    if active_index is not None:
        parse_complete = False
    expected_indexes = set(range(1, statement_count + 1))
    parse_complete = (
        parse_complete
        and seen_begin == expected_indexes
        and seen_end == expected_indexes
    )
    observation["statement_results"] = [
        {
            "statement_index": index,
            "output_set": sorted(set(statement_lines[index])),
        }
        for index in range(1, statement_count + 1)
    ]
    observation["unframed_output_set"] = sorted(set(unframed_lines))
    observation["marker_parse_complete"] = parse_complete
    return observation


def verified_comparison_view(observation):
    """Return only the verified-workload fields that define consistency."""
    return {
        "completed": observation.get("completed"),
        "exit_code": observation.get("exit_code"),
        "command_ok": observation.get("command_ok"),
        "marker_parse_complete": observation.get("marker_parse_complete"),
        "statement_results": observation.get("statement_results"),
        "unframed_output_set": observation.get("unframed_output_set"),
    }


class V1InitialResultStore:
    """Extend a copied legacy first-filter result with V1-only observations."""

    def __init__(self, path, valid_ids, legacy_result):
        self.path = path
        self.valid_ids = set(valid_ids)
        self._legacy_result = copy.deepcopy(legacy_result)
        self._legacy_passed = (
            normalize_ids(self._legacy_result.get("passed_sample_ids"))
            - self.valid_ids
        )
        self._legacy_failed = (
            normalize_ids(self._legacy_result.get("failed_sample_ids"))
            - self.valid_ids
        )
        self._legacy_passed -= self._legacy_failed
        raw = load_json(path, {})
        self._workload_passed = (
            normalize_ids(raw.get("workload_pass_ids")) & self.valid_ids
        )
        self._workload_failed = (
            normalize_ids(raw.get("workload_failed_ids")) & self.valid_ids
        )
        self._anomaly_passed = (
            normalize_ids(raw.get("passed_sample_ids")) & self.valid_ids
        )
        self._anomaly_failed = (
            normalize_ids(raw.get("failed_sample_ids")) & self.valid_ids
        )
        self._anomaly_passed -= self._anomaly_failed
        self._lock = threading.Lock()
        self.persist()

    def _result(self):
        result = copy.deepcopy(self._legacy_result)
        result.update({
            "format_version": V1_FORMAT_VERSION,
            "stage": "v1_first_filter",
            "workload_evidence_file": V1_EVIDENCE_FILENAME,
            "workload_pass_ids": sorted(self._workload_passed),
            "workload_failed_ids": sorted(self._workload_failed),
            "passed_sample_ids": sorted(
                self._legacy_passed | self._anomaly_passed
            ),
            "failed_sample_ids": sorted(
                self._legacy_failed | self._anomaly_failed
            ),
        })
        return result

    def persist(self):
        with self._lock:
            write_json_atomic(self.path, self._result())

    def record(self, sample_id, workload_passed, anomaly_passed):
        with self._lock:
            if workload_passed:
                self._workload_passed.add(sample_id)
                self._workload_failed.discard(sample_id)
            else:
                self._workload_failed.add(sample_id)
                self._workload_passed.discard(sample_id)
            if anomaly_passed:
                self._anomaly_passed.add(sample_id)
                self._anomaly_failed.discard(sample_id)
            else:
                self._anomaly_failed.add(sample_id)
                self._anomaly_passed.discard(sample_id)
            write_json_atomic(self.path, self._result())

    def completed_ids(self):
        with self._lock:
            return set(self._anomaly_passed) | set(self._anomaly_failed)

    def anomaly_failed_ids(self):
        with self._lock:
            return set(self._anomaly_failed)

    def workload_passed_ids(self):
        with self._lock:
            return set(self._workload_passed)

    def workload_failed_ids(self):
        with self._lock:
            return set(self._workload_failed)


class V1SecondResultStore:
    """Persist V1 second-stage outcomes while retaining first-stage metadata."""

    def __init__(
            self, path, base_result, inherited_passed, inherited_failed,
            workload_passed, workload_failed, revalidation_candidates,
            non_included):
        self.path = path
        self._base_result = copy.deepcopy(base_result)
        self._passed = set(inherited_passed)
        self._failed = set(inherited_failed)
        self._passed -= self._failed
        self._workload_passed = set(workload_passed)
        self._workload_failed = set(workload_failed)
        self._candidates = set(revalidation_candidates)
        self._non_included = list(non_included)
        raw = load_json(path, {})
        self._health_statuses = raw.get(
            _legacy_HEALTH_STATUS_RESULT_KEY,
            self._base_result.get(_legacy_HEALTH_STATUS_RESULT_KEY, {}),
        )
        if not isinstance(self._health_statuses, dict):
            self._health_statuses = {}
        raw_passed = normalize_ids(raw.get("passed_sample_ids"))
        self._completed = (
            normalize_ids(raw.get("v1_second_completed_ids"))
            & self._candidates
        )
        for sample_id in self._completed:
            if sample_id in raw_passed:
                self._passed.add(sample_id)
                self._failed.discard(sample_id)
            else:
                self._failed.add(sample_id)
                self._passed.discard(sample_id)
        self._lock = threading.Lock()
        self.persist()

    def _result(self):
        result = copy.deepcopy(self._base_result)
        result.update({
            "format_version": V1_FORMAT_VERSION,
            "stage": "v1_second_filter",
            "workload_evidence_file": V1_EVIDENCE_FILENAME,
            "workload_pass_ids": sorted(self._workload_passed),
            "workload_failed_ids": sorted(self._workload_failed),
            "v1_second_completed_ids": sorted(self._completed),
            "passed_sample_ids": sorted(self._passed),
            "failed_sample_ids": sorted(self._failed),
            "non_included_sample_ids": sorted(self._non_included),
            _legacy_HEALTH_STATUS_RESULT_KEY: self._health_statuses,
        })
        return result

    def record(self, sample_id, is_passed, health_status=None):
        with self._lock:
            if is_passed:
                self._passed.add(sample_id)
                self._failed.discard(sample_id)
            else:
                self._failed.add(sample_id)
                self._passed.discard(sample_id)
            self._completed.add(sample_id)
            if health_status is not None:
                self._health_statuses[str(sample_id)] = str(health_status)
            write_json_atomic(self.path, self._result())

    def completed_ids(self):
        with self._lock:
            return set(self._completed)

    def persist(self):
        with self._lock:
            write_json_atomic(self.path, self._result())

    def passed_ids(self):
        with self._lock:
            return set(self._passed)

    def failed_ids(self):
        with self._lock:
            return set(self._failed)


def run_anomaly_evaluation_compatible(
        docker_name, anomaly_eval_path, sample_files, system_user_config,
        dbms, database_name, database_schema_name, timeout_threshold, median_runs,
        is_rep, success_threshold_percent, min_abs_delta_seconds,
        anomaly_type, success_criterion, output_dir, logger):
    """Use both established evaluator success markers on every server."""
    command = [
        "python", anomaly_eval_path,
        "--script-path", sample_files["anomaly_inject.sh"],
        "--verified-workload-path", sample_files["verified_workload.sql"],
        "--initialization-workload-path", sample_files["initialization_workload.sql"],
        "--host", str(system_user_config["DB_HOST"]),
        "--port", str(system_user_config["DB_PORT"]),
        "--user", str(system_user_config["DB_USER"]),
        "--database", str(database_name),
        "--schema", str(database_schema_name),
        "--password", str(system_user_config["DB_PASSWORD"]),
        "--timeout", str(timeout_threshold),
        "--median-runs", str(median_runs),
    ]
    if _legacy_anomaly_eval_supports_flag(anomaly_eval_path, "--dbms"):
        command[2:2] = ["--dbms", str(dbms)]
    if is_rep:
        command.extend([
            "--success-threshold-percent", str(success_threshold_percent),
            "--min-abs-delta-seconds", str(min_abs_delta_seconds),
            "--anomaly-type", str(anomaly_type),
            "--success-criterion", str(success_criterion),
        ])

    log_path = os.path.join(output_dir, _legacy_ANOMALY_EVAL_LOG_NAME)
    command_ok, _ = _docker_run_docker_exec_observation(
        docker_name,
        _legacy_build_filtered_anomaly_eval_command(command),
        log_path,
        timeout_threshold,
        session_name=docker_name,
    )
    marker_found = _legacy_log_contains_any(log_path, SUCCESS_MARKERS)
    if command_ok and not marker_found:
        logger.error(
            "[V1][FIRST_FILTER] Evaluator exited successfully without a "
            "recognized success marker: %s", log_path
        )
    return command_ok and marker_found


# The inherited second-stage worker resolves this global in its own module.
_legacy_run_anomaly_evaluation = run_anomaly_evaluation_compatible


def resolve_sample_context(sample_task, base_dir):
    """Resolve paths and configuration shared by precheck and evaluation."""
    _index, sample_id, sample_info = sample_task
    database_name = sample_info.get("database_name", "")
    sample_directory = _legacy_find_sample_directory(
        resolve_sample_context.sample_base_directory, sample_id, database_name
    )
    if not os.path.isdir(sample_directory):
        raise ValueError("sample directory not found: %s" % sample_directory)

    sample_files = {
        name: os.path.join(sample_directory, name)
        for name in _legacy_REQUIRED_SAMPLE_FILES
    }
    missing = [name for name, path in sample_files.items() if not os.path.exists(path)]
    if missing:
        raise ValueError("missing required sample files: %s" % missing)

    sample_config = _legacy_load_sample_config(sample_directory)
    sample_config.update({k: v for k, v in _FILTER_OPTIONS.items() if k in ('success_threshold_percent', 'min_abs_delta_seconds')})
    dbms = sample_config.get("dbms", "PostgreSQL 16.11")
    database_name = sample_config.get("database_name", database_name)
    schema_name = sample_config.get(
        "database_schema_name",
        sample_info.get("database_schema_name", "diagnosis"),
    )
    if not schema_name:
        schema_name = "diagnosis"
    user_config = sample_config.get(
        "system_user_config", _legacy_default_user_config(dbms)
    )
    hardware_config = sample_config.get(
        "system_hardware_config", _legacy_default_hardware_config()
    )
    anomaly_eval_path = _embedded_evaluator(sample_config, sample_files)
    if not anomaly_eval_path:
        raise ValueError("anomaly evaluator not found: %s" % anomaly_eval_path)

    return {
        "sample_id": sample_id,
        "sample_files": sample_files,
        "dbms": dbms,
        "database_name": database_name,
        "schema_name": schema_name,
        "user_config": user_config,
        "hardware_config": hardware_config,
        "db_type": _util_resolve_db_type(sample_config, dbms),
        "anomaly_eval_path": anomaly_eval_path,
        "is_rep": _legacy_anomaly_eval_supports_rep_flags(anomaly_eval_path),
        "success_threshold_percent": sample_config.get(
            "success_threshold_percent", _legacy_DEFAULT_SUCCESS_THRESHOLD_PERCENT
        ),
        "min_abs_delta_seconds": sample_config.get(
            "min_abs_delta_seconds", _legacy_DEFAULT_MIN_ABS_DELTA_SECONDS
        ),
        "anomaly_type": _legacy_resolve_anomaly_type(
            sample_info, _legacy_read_anomaly_description(sample_directory)
        ),
        "success_criterion": _legacy_resolve_success_criterion(
            sample_info, _legacy_read_anomaly_description(sample_directory)
        ),
    }


def run_workload_consistency_rounds(
        container_name, context, output_dir, rounds, timeout_seconds, logger):
    """Run initialization before verified SQL and compare per-statement results."""
    observations = []
    for round_number in range(1, rounds + 1):
        round_dir = os.path.join(output_dir, "v1_workload_round_%d" % round_number)
        os.makedirs(round_dir, exist_ok=True)
        round_result = {}
        for workload_name, file_name in (
            ("initialization", "initialization_workload.sql"),
            ("verified", "verified_workload.sql"),
        ):
            log_path = os.path.join(round_dir, "%s_workload.log" % workload_name)
            sql_path = context["sample_files"][file_name]
            marked_path = None
            marker_prefix = None
            statement_count = None
            if workload_name == "verified":
                marked_path, marker_prefix, statement_count = (
                    create_marked_verified_workload(context["dbms"], sql_path)
                )
                sql_path = marked_path
            try:
                command = _docker_build_sql_file_command(
                    context["dbms"],
                    context["user_config"],
                    sql_path,
                    context["database_name"],
                )
                command_ok, _ = _docker_run_docker_exec_observation(
                    container_name, command, log_path, timeout_seconds,
                    session_name=container_name,
                )
            finally:
                if marked_path is not None:
                    try:
                        os.remove(marked_path)
                    except OSError:
                        pass
            observation = _legacy_workload_log_observation(log_path)
            observation["command_ok"] = bool(command_ok)
            if workload_name == "verified":
                parse_marked_verified_observation(
                    observation, marker_prefix, statement_count
                )
            round_result[workload_name] = observation
        write_json_atomic(
            os.path.join(round_dir, "round_result.json"), round_result
        )
        observations.append(round_result)
        logger.info(
            "[V1][WORKLOAD] Completed workload round %d/%d.",
            round_number, rounds,
        )
    verified_observations = [
        verified_comparison_view(item["verified"]) for item in observations
    ]
    return (
        all(
            item == verified_observations[0]
            for item in verified_observations[1:]
        ),
        observations,
    )


def process_v1_first_sample(
        sample_task, worker_base_name, worker_port, base_dir, output_directory,
        median_runs, workload_rounds, workload_timeout, result_store,
        isolate_resources, logger):
    """Record workload consistency, then always run the first anomaly filter."""
    sample_id = sample_task[1]
    output_dir = os.path.join(output_directory, str(sample_id))
    container_name = None
    workload_passed = False
    try:
        os.makedirs(output_dir, exist_ok=True)
        context = resolve_sample_context(sample_task, base_dir)
        container_config = _docker_build_container_config(
            context["dbms"], context["hardware_config"], worker_port,
            context["user_config"],
        )
        container_name = _docker_make_unique_container_name(worker_base_name)
        container_name = _legacy_initialize_docker_container(
            container_name, worker_base_name, container_config, logger
        )
        tmp_dbms = context["dbms"].replace(" ", "_")
        if isolate_resources:
            tmp_dbms = os.path.join(tmp_dbms, worker_base_name)
        init_config = {
            "dbms": context["dbms"],
            "base_dir": base_dir,
            "db_host": context["user_config"]["DB_HOST"],
            "db_port": context["user_config"]["DB_PORT"],
            "db_name": context["database_name"],
            "db_type": context["db_type"],
            "db_schema": context["schema_name"],
            "db_user": context["user_config"]["DB_USER"],
            "password": context["user_config"]["DB_PASSWORD"],
            "timeout": _legacy_DATABASE_INIT_TIMEOUT_SECONDS,
            "output_dir": output_dir,
            "tmp_directory": _docker_initialize_shared_tmp_directory(base_dir, tmp_dbms),
        }
        container_name = _legacy_initialize_database(
            container_name, worker_base_name, init_config, container_config,
            context["database_name"], logger,
        )
        if not _legacy_run_data_modification(
                container_name, context["dbms"], context["user_config"],
                context["sample_files"]["data_modification.sql"],
                context["database_name"], output_dir, logger):
            raise ValueError("data modification failed")

        workload_passed, observations = run_workload_consistency_rounds(
            container_name, context, output_dir, workload_rounds,
            workload_timeout, logger,
        )
        evidence = {
            "format_version": V1_FORMAT_VERSION,
            "sample_id": sample_id,
            "completed": True,
            "passed": workload_passed,
            "round_count": workload_rounds,
            "comparison_rule": (
                "initialization runs before every verified workload but is not "
                "compared; verified workload status must match, SQL statement "
                "order is preserved, and each statement's normalized output is "
                "compared as an unordered set in every round"
            ),
            "rounds": observations,
        }
        write_json_atomic(os.path.join(output_dir, V1_EVIDENCE_FILENAME), evidence)
        if not workload_passed:
            logger.warning(
                "[V1][WORKLOAD] Sample %s is inconsistent; continuing to "
                "anomaly evaluation.",
                sample_id,
            )

        timeout = _legacy_calculate_dynamic_timeout_threshold(
            context["sample_files"]["anomaly_inject.sh"],
            context["sample_files"]["merged_workload_performance.json"],
            median_runs,
        )
        passed = run_anomaly_evaluation_compatible(
            container_name, context["anomaly_eval_path"], context["sample_files"],
            context["user_config"], context["dbms"], context["database_name"],
            context["schema_name"], timeout, median_runs, context["is_rep"],
            context["success_threshold_percent"], context["min_abs_delta_seconds"],
            context["anomaly_type"], context["success_criterion"], output_dir, logger,
        )
        result_store.record(sample_id, workload_passed, passed)
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[V1][FIRST_FILTER] Sample %s failed: %s", sample_id, exc)
        result_store.record(sample_id, workload_passed, False)
    finally:
        if container_name is not None:
            _docker_kill_tmux_session(container_name)
            _docker_docker_container_del(container_name)


def run_v1_first_workers(
        tasks, worker_count, base_dir, docker_name, docker_port, median_runs,
        workload_rounds, workload_timeout, output_directory, result_store,
        logger):
    isolate_resources = worker_count > 1
    prefixes = [
        "%s_v1_w%d" % (docker_name, index) if isolate_resources else "%s_v1" % docker_name
        for index in range(worker_count)
    ]
    for prefix in prefixes:
        _docker_remove_containers_by_prefix(prefix)
        _docker_kill_tmux_sessions_by_prefix(prefix)

    task_queue = queue.Queue()
    for task in tasks:
        task_queue.put(task)
    stop_event = threading.Event()
    fatal_error = {}
    fatal_lock = threading.Lock()

    def worker(index):
        while not stop_event.is_set():
            try:
                task = task_queue.get_nowait()
            except queue.Empty:
                return
            try:
                process_v1_first_sample(
                    task, prefixes[index], docker_port + index, base_dir,
                    output_directory, median_runs, workload_rounds, workload_timeout,
                    result_store, isolate_resources, logger,
                )
            except (RuntimeError, SystemExit) as exc:
                with fatal_lock:
                    fatal_error.setdefault("error", exc)
                stop_event.set()
                return

    threads = [
        threading.Thread(target=worker, args=(index,), name="v1-first-%d" % index)
        for index in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for prefix in prefixes:
        _docker_remove_containers_by_prefix(prefix)
        _docker_kill_tmux_sessions_by_prefix(prefix)
    if fatal_error:
        raise RuntimeError("[V1][FIRST_FILTER] Worker aborted: %s" % fatal_error["error"])


def load_stage_result(path):
    if not os.path.isfile(path):
        return set(), set()
    return _legacy_load_existing_filter_result(path)


def persist_inherited_filter_result(
        result_path, inherited_path, passed, failed, non_included,
        health_statuses=None):
    """Write a stage result without discarding inherited JSON attributes."""
    inherited = load_json(inherited_path, {})
    current = load_json(result_path, {})
    result = copy.deepcopy(inherited)
    for key, value in current.items():
        result.setdefault(key, copy.deepcopy(value))
    inherited_health = result.get(_legacy_HEALTH_STATUS_RESULT_KEY, {})
    if not isinstance(inherited_health, dict):
        inherited_health = {}
    current_health = current.get(_legacy_HEALTH_STATUS_RESULT_KEY, {})
    if isinstance(current_health, dict):
        inherited_health.update(current_health)
    if health_statuses is not None:
        inherited_health.update(health_statuses)
    result.update({
        "passed_sample_ids": sorted(set(passed)),
        "failed_sample_ids": sorted(set(failed) - set(passed)),
        "non_included_sample_ids": sorted(non_included),
        _legacy_HEALTH_STATUS_RESULT_KEY: inherited_health,
    })
    write_json_atomic(result_path, result)


def clean_incomplete_sample_outputs(stage_dir, incomplete_ids, stage_name, logger):
    """Remove only numeric sample directories without a completed stage result."""
    removed = 0
    for sample_id in sorted(set(incomplete_ids)):
        output_dir = os.path.join(stage_dir, str(sample_id))
        if os.path.isdir(output_dir):
            shutil.rmtree(output_dir)
            removed += 1
    if removed:
        logger.info(
            "[V1][%s] Removed %d incomplete sample output directorie(s).",
            stage_name, removed,
        )


def run_historical_second_stage(
        first_passed, first_failed, valid_ids, sample_by_id, second_path,
        second_dir, parallelism, base_dir, docker_name, docker_port, median_runs,
        input_directory, non_included, first_path, inherited_health_statuses,
        logger):
    """Retry historical failures while retaining the complete first-stage JSON."""
    retry_eligible = set(first_failed) & valid_ids
    second_passed = set(first_passed) & valid_ids
    second_failed = set()
    old_second_passed, old_second_failed = load_stage_result(second_path)
    completed_retries = (old_second_passed | old_second_failed) & retry_eligible
    for sample_id in completed_retries:
        if sample_id in old_second_passed:
            second_passed.add(sample_id)
            second_failed.discard(sample_id)
        else:
            second_failed.add(sample_id)
            second_passed.discard(sample_id)
    second_pending_ids = retry_eligible - completed_retries
    clean_incomplete_sample_outputs(
        second_dir, second_pending_ids, "SECOND_FILTER", logger
    )
    second_pending = [
        sample_by_id[sample_id] for sample_id in sorted(second_pending_ids)
    ]
    health_statuses = dict(inherited_health_statuses)
    health_statuses.update(_legacy_load_health_statuses(second_path))
    second_store = _legacy_SampleResultStore(
        second_path, second_passed, second_failed, non_included,
        health_statuses=health_statuses,
    )
    second_store.persist()
    logger.info(
        "[V1][SECOND_FILTER] Retrying %d historical anomaly-evaluation "
        "failure(s).",
        len(second_pending),
    )
    _legacy_run_evaluation_workers(
        second_pending, parallelism, base_dir, "%s_v1_second" % docker_name,
        docker_port, median_runs, second_dir, input_directory, second_store,
        False, logger,
    )
    second_store.persist()
    completed_passed, completed_failed = load_stage_result(second_path)
    persist_inherited_filter_result(
        second_path, first_path, completed_passed, completed_failed,
        non_included, health_statuses=_legacy_load_health_statuses(second_path),
    )
    return load_stage_result(second_path)


def run_historical_third_stage(
        second_passed, valid_ids, sample_by_id, third_path, third_dir,
        parallelism, base_dir, docker_name, docker_port, input_directory,
        workload_timeout, non_included, second_path, inherited_health_statuses,
        logger):
    """Run third filtering while retaining the complete second-stage JSON."""
    legacy_third_candidates = set(second_passed) & valid_ids
    old_third_passed, old_third_failed = load_stage_result(third_path)
    completed_third = (
        (old_third_passed | old_third_failed) & legacy_third_candidates
    )
    third_passed = old_third_passed & completed_third
    third_failed = old_third_failed & completed_third
    third_pending_ids = legacy_third_candidates - completed_third
    clean_incomplete_sample_outputs(
        third_dir, third_pending_ids, "THIRD_FILTER", logger
    )
    third_pending = [
        sample_by_id[sample_id] for sample_id in sorted(third_pending_ids)
    ]
    health_statuses = dict(inherited_health_statuses)
    health_statuses.update(_legacy_load_health_statuses(third_path))
    third_store = _legacy_SampleResultStore(
        third_path, third_passed, third_failed, non_included,
        health_statuses=health_statuses,
    )
    third_store.persist()
    logger.info(
        "[V1][THIRD_FILTER] Running legacy compatibility validation for %d "
        "historical passing sample(s).",
        len(third_pending),
    )
    _legacy_run_third_filter_workers(
        third_pending, parallelism, base_dir, "%s_v1_legacy_third" % docker_name,
        docker_port, third_dir, input_directory, workload_timeout, third_store,
        logger,
    )
    third_store.persist()
    completed_passed, completed_failed = load_stage_result(third_path)
    persist_inherited_filter_result(
        third_path, second_path, completed_passed, completed_failed,
        non_included, health_statuses=_legacy_load_health_statuses(third_path),
    )
    return load_stage_result(third_path)


def run_v1_second_stage(
        initial_failed, valid_ids, sample_by_id, result_path, output_directory,
        parallelism, base_dir, docker_name, docker_port, median_runs,
        input_directory, non_included, inherited_result, inherited_passed,
        inherited_failed, workload_passed, workload_failed, logger):
    """Retry V1 anomaly-evaluation failures in a result inheriting first output."""
    candidates = set(initial_failed) & valid_ids
    store = V1SecondResultStore(
        result_path, inherited_result, inherited_passed, inherited_failed,
        workload_passed, workload_failed, candidates, non_included,
    )
    pending_ids = candidates - store.completed_ids()
    clean_incomplete_sample_outputs(
        output_directory, pending_ids, "V1_SECOND_FILTER", logger
    )
    pending = [sample_by_id[sample_id] for sample_id in sorted(pending_ids)]
    logger.info(
        "[V1][V1_SECOND_FILTER] Retrying %d initial anomaly-evaluation "
        "failure(s).",
        len(pending),
    )
    _legacy_run_evaluation_workers(
        pending, parallelism, base_dir, "%s_v1_second" % docker_name,
        docker_port, median_runs, output_directory, input_directory, store,
        False, logger,
    )
    store.persist()
    return store.passed_ids(), store.failed_ids()


def main():
    global _legacy_DATABASE_INIT_TIMEOUT_SECONDS, _docker_DOCKER_INIT_TIMEOUT
    parser = argparse.ArgumentParser(
        description="Synthetic anomaly filtering V1 with workload consistency gate"
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--parallelism", type=int, default=None)
    args = parser.parse_args()

    base_dir = str(PROJECT_ROOT)
    config_path = _util_resolve_path_from_base(args.config, base_dir)
    config = load_json(config_path, None)
    if not isinstance(config, dict):
        raise RuntimeError("Invalid config file: %s" % config_path)
    if args.parallelism is not None:
        config["parallelism"] = args.parallelism
    for field in ("input_directory", "output_directory"):
        if not isinstance(config.get(field), str) or not config[field].strip():
            raise ValueError(f"{field} must be a nonempty path")

    input_directory = _util_resolve_path_from_base(config.get("input_directory"), base_dir)
    output_directory = _util_resolve_path_from_base(config.get("output_directory"), base_dir)
    if not input_directory or not os.path.isdir(input_directory):
        raise RuntimeError("Invalid input_directory: %s" % input_directory)
    if not output_directory:
        raise RuntimeError("Missing output_directory")
    _FILTER_OPTIONS.clear()
    _FILTER_OPTIONS.update(config)
    sample_count = _validate_standalone_config(config, input_directory)
    if args.dry_run:
        print(f"Validated {sample_count} samples; standalone V1 filter ready.")
        return
    _prepare_runtime_assets()
    os.makedirs(output_directory, exist_ok=True)
    resolve_sample_context.sample_base_directory = input_directory

    parallelism = int(config.get("parallelism", 1))
    parallelism = max(1, int(parallelism))
    median_runs = max(1, int(config.get("median_runs", _legacy_DEFAULT_MEDIAN_RUNS)))
    workload_rounds = max(
        2, int(config.get("workload_validation_rounds", DEFAULT_WORKLOAD_VALIDATION_ROUNDS))
    )
    workload_timeout = max(
        1, int(config.get(
            "workload_validation_timeout_seconds", DEFAULT_WORKLOAD_VALIDATION_TIMEOUT
        ))
    )
    _legacy_DATABASE_INIT_TIMEOUT_SECONDS = max(
        1, int(config.get(
            "database_init_timeout_seconds", _legacy_DEFAULT_DATABASE_INIT_TIMEOUT
        ))
    )
    _legacy_configure_database_init_parallelism(min(
        parallelism,
        max(1, int(config.get("database_init_parallelism", parallelism))),
    ))
    docker_timeout = int(config.get(
        "docker_init_timeout_seconds", _legacy_DEFAULT_DOCKER_INIT_TIMEOUT
    ))
    if docker_timeout > 0:
        _docker_DOCKER_INIT_TIMEOUT = docker_timeout

    docker_name = str(config.get("docker_name", _legacy_DEFAULT_DOCKER_NAME))
    docker_port = int(config.get("docker_port", _legacy_DEFAULT_DOCKER_PORT))
    first_dir = os.path.join(output_directory, "first_filter")
    second_dir = os.path.join(output_directory, "second_filter")
    third_dir = os.path.join(output_directory, "third_filter")
    v1_first_dir = os.path.join(output_directory, "v1_first_filter")
    v1_second_dir = os.path.join(output_directory, "v1_second_filter")
    for path in (first_dir, second_dir, third_dir, v1_first_dir, v1_second_dir):
        os.makedirs(path, exist_ok=True)
    first_path = os.path.join(first_dir, "0_filter_result.json")
    second_path = os.path.join(second_dir, "1_filter_result.json")
    third_path = os.path.join(third_dir, "2_filter_results.json")
    v1_first_path = os.path.join(v1_first_dir, V1_FIRST_RESULT_FILENAME)
    v1_second_path = os.path.join(v1_second_dir, V1_SECOND_RESULT_FILENAME)

    logger = _util_set_up_logger(os.path.join(output_directory, "log_v1.txt"))
    logger = logging.getLogger("DB_DIAG.%s" % __name__)
    key_info = load_json(os.path.join(input_directory, "key_information.json"), None)
    if not isinstance(key_info, list):
        raise RuntimeError("key_information.json is unavailable or invalid")
    samples, non_included = _legacy_build_target_samples(key_info, logger)
    sample_by_id = {sample[1]: sample for sample in samples}
    valid_ids = set(sample_by_id)

    logger.info("[V1] Starting with %d target samples.", len(samples))
    logger.info(
        "[V1] Workload observation: %d rounds, %ss timeout; inconsistency "
        "does not reject anomaly evaluation.",
        workload_rounds, workload_timeout,
    )

    first_passed, first_failed = load_stage_result(first_path)
    first_passed &= valid_ids
    first_failed &= valid_ids
    first_completed = first_passed | first_failed
    logger.info(
        "[V1][HISTORICAL_FIRST] Reusing %d completed first-filter sample(s); "
        "%d sample(s) remain outside the historical first filter.",
        len(first_completed), len(valid_ids - first_completed),
    )

    # Complete all historical first-filter results before creating new V1 ones.
    logger.info(
        "[V1] Completing second/third filtering for %d existing first-filter "
        "sample(s) before V1 initial filtering.",
        len(first_completed),
    )
    second_passed, second_failed = run_historical_second_stage(
        first_passed, first_failed, valid_ids, sample_by_id, second_path,
        second_dir, parallelism, base_dir, docker_name, docker_port, median_runs,
        input_directory, non_included, first_path,
        _legacy_load_health_statuses(first_path), logger,
    )
    third_passed, third_failed = run_historical_third_stage(
        second_passed, valid_ids, sample_by_id, third_path, third_dir,
        parallelism, base_dir, docker_name, docker_port, input_directory,
        workload_timeout, non_included, second_path,
        _legacy_load_health_statuses(second_path), logger,
    )

    historical_candidates = second_passed & valid_ids
    historical_passed = third_passed & historical_candidates
    historical_failed = (
        (second_failed & valid_ids) | (third_failed & historical_candidates)
    )
    historical_failed -= historical_passed
    persist_inherited_filter_result(
        third_path, second_path, historical_passed, historical_failed,
        non_included, health_statuses=_legacy_load_health_statuses(third_path),
    )
    legacy_third_result = load_json(third_path, {})
    third_completed = historical_passed | historical_failed
    v1_pending_ids = valid_ids - third_completed
    logger.info(
        "[V1][HISTORICAL_FINAL] third_filter now contains %d completed "
        "historical sample(s); %d sample(s) require V1 initial filtering.",
        len(third_completed), len(v1_pending_ids),
    )

    initial_store = V1InitialResultStore(
        v1_first_path, v1_pending_ids, legacy_third_result
    )
    v1_initial_pending_ids = v1_pending_ids - initial_store.completed_ids()
    clean_incomplete_sample_outputs(
        v1_first_dir, v1_initial_pending_ids, "V1_INITIAL_FILTER", logger
    )
    v1_initial_pending = [
        sample_by_id[sample_id] for sample_id in sorted(v1_initial_pending_ids)
    ]
    logger.info(
        "[V1][INITIAL_FILTER] Running workload observation plus anomaly "
        "evaluation for %d sample(s).",
        len(v1_initial_pending),
    )
    run_v1_first_workers(
        v1_initial_pending, parallelism, base_dir, docker_name, docker_port,
        median_runs, workload_rounds, workload_timeout, v1_first_dir, initial_store,
        logger,
    )
    initial_store.persist()
    v1_initial_failed = initial_store.anomaly_failed_ids()

    logger.info(
        "[V1] Initial workload/anomaly filtering is complete; retrying the "
        "initial anomaly-evaluation failure set.",
    )
    v1_first_result = load_json(v1_first_path, legacy_third_result)
    inherited_passed, inherited_failed = load_stage_result(v1_first_path)
    inherited_passed &= valid_ids
    inherited_failed &= valid_ids
    inherited_failed -= inherited_passed
    v1_second_passed, v1_second_failed = run_v1_second_stage(
        v1_initial_failed, v1_pending_ids, sample_by_id, v1_second_path,
        v1_second_dir, parallelism, base_dir, docker_name, docker_port, median_runs,
        input_directory, non_included, v1_first_result, inherited_passed,
        inherited_failed, initial_store.workload_passed_ids(),
        initial_store.workload_failed_ids(), logger,
    )

    final_passed = v1_second_passed & valid_ids
    final_failed = v1_second_failed & valid_ids
    final_failed -= final_passed
    missing = valid_ids - final_passed - final_failed
    if missing:
        raise RuntimeError("V1 final state is incomplete for sample IDs: %s" % sorted(missing))
    logger.info(
        "[V1] Finished. Passed: %d, failed: %d. Final result is in %s.",
        len(final_passed), len(final_failed),
        v1_second_path,
    )
    print(_legacy_RUN_COMPLETION_MARKER, flush=True)





# Embedded executable assets, staged only when a filtering run starts.
_EMBEDDED_ASSETS = {
    'Scripts/anomaly_eval.py': r'''#!/usr/bin/env python3
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
''',
    'Scripts/anomaly_eval_rep.py': r'''#!/usr/bin/env python3
"""Anomaly evaluation script tailored for Online Diagnosis Incident reproduction.

This script is a reproduction-oriented variant of ``anomaly_eval.py``. It keeps
the original baseline/injected median-timing flow (one warm-up pass plus
`median_runs` measured runs), the "blocked-until-timeout means success" handling and the
"verified workload error after injection means success" handling, together with
every original CLI argument, so it can be invoked by the reproduction harness
with the exact same argument matrix used for ``anomaly_eval.py``.

Motivation for a dedicated variant
----------------------------------
Real Online Diagnosis Incident records do not always degrade in a way that a fixed 20%
relative-slowdown threshold can measure deterministically. Replication lag,
topology, throttling and monitoring-alert style records often manifest as
blocking / errors / timeouts, or as a small-but-clear absolute latency increase
on a tiny baseline (where relative percentages are noisy).

Category-driven success judgement
---------------------------------
Rather than defining anomaly-injection success through a fixed performance
threshold hyperparameter shared by every record, the driving LLM inspects the
concrete Online Diagnosis Incident scenario and declares WHICH manifestation category the
reproduced anomaly belongs to. That declared category is passed in via
``--success-criterion`` and selects the authoritative success signal:

    PERFORMANCE_DEGRADATION -> the verified workload still completes but is
        significantly slower after injection (relative slowdown reaching the
        built-in engineering threshold, OR a clear absolute latency increase).
    BLOCKING_TIMEOUT        -> the verified workload is blocked by the injected
        anomaly until it times out (or otherwise cannot complete).
    EXECUTION_ERROR         -> the verified workload starts erroring after the
        anomaly is injected.

"Hard" evidence (a verified-workload error after injection, or a
blocked-until-timeout) unambiguously proves the workload can no longer complete
normally, so it is accepted as success under every category; only PURE slowdown
is category-gated (it satisfies PERFORMANCE_DEGRADATION but is too weak for the
BLOCKING_TIMEOUT / EXECUTION_ERROR categories). When no category is supplied the
script falls back to the historical broadened OR judgement (``AUTO``) so it stays
drop-in compatible with the exact argument matrix used for ``anomaly_eval.py``.

The judgement branch that fired is logged explicitly so the driving LLM can use
it in the next Observation. Structure and exit codes (0 success / 1 failure /
130 interrupt) are preserved so the harness observation parsing stays unchanged.
"""

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
# Built-in engineering thresholds used ONLY to quantify what counts as a
# "significant" slowdown within the PERFORMANCE_DEGRADATION category. They are no
# longer synthesis-config hyperparameters that decide success on their own: the
# authoritative success signal is the LLM-declared manifestation category (see
# --success-criterion). A CLI override is still accepted for backward
# compatibility with the anomaly_eval.py argument matrix.
SUCCESS_THRESHOLD_PERCENT = 20.0
# Absolute latency-increase fallback (seconds). 0 disables the fallback and keeps
# the original percentage-only behaviour unless a value is supplied on the CLI.
MIN_ABS_DELTA_SECONDS = 0.0
PROCESS_TERMINATION_GRACE_SECONDS = 5.0

# Success-criterion categories declared by the LLM for a given Online Diagnosis Incident
# scenario. AUTO preserves the historical broadened OR judgement and is used when
# no explicit category is supplied on the command line.
SUCCESS_CRITERION_PERFORMANCE = "PERFORMANCE_DEGRADATION"
SUCCESS_CRITERION_BLOCKING = "BLOCKING_TIMEOUT"
SUCCESS_CRITERION_ERROR = "EXECUTION_ERROR"
SUCCESS_CRITERION_AUTO = "AUTO"
VALID_SUCCESS_CRITERIA = frozenset(
    {
        SUCCESS_CRITERION_PERFORMANCE,
        SUCCESS_CRITERION_BLOCKING,
        SUCCESS_CRITERION_ERROR,
        SUCCESS_CRITERION_AUTO,
    }
)


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


def normalize_anomaly_type(anomaly_type: Optional[str]) -> str:
    """Normalize the anomaly type to one of OLAP / OLTP / BOTH.

    The value only influences logging and (optionally) the effective threshold
    selection; an unknown value degrades gracefully to BOTH so the run never
    fails because of a free-text record classification.
    """

    normalized = str(anomaly_type or "").strip().upper().replace(" ", "")
    if normalized in {"OLAP", "OLTP", "BOTH"}:
        return normalized
    if normalized in {"OLAP+OLTP", "OLTP+OLAP"}:
        return "BOTH"
    return "BOTH"


def normalize_success_criterion(criterion: Optional[str]) -> str:
    """Normalize the LLM-declared success criterion to a known category.

    Accepts the three explicit manifestation categories
    (PERFORMANCE_DEGRADATION / BLOCKING_TIMEOUT / EXECUTION_ERROR) plus a handful
    of common synonyms, and degrades gracefully to ``AUTO`` (the historical
    broadened OR judgement) for anything unrecognized, so a noisy free-text
    classification never aborts the run.
    """

    normalized = str(criterion or "").strip().upper().replace(" ", "_").replace("-", "_")
    # Collapse any run of underscores produced by the substitutions above so
    # inputs like "performance   degradation" still map to the canonical form.
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    normalized = normalized.strip("_")
    if normalized in VALID_SUCCESS_CRITERIA:
        return normalized
    performance_aliases = {
        "PERFORMANCE",
        "DEGRADATION",
        "SLOWDOWN",
        "LATENCY",
        "PERFORMANCE_DEGRADE",
        "PERF_DEGRADATION",
    }
    blocking_aliases = {
        "BLOCKING",
        "BLOCKED",
        "TIMEOUT",
        "BLOCK",
        "HANG",
        "BLOCKED_UNTIL_TIMEOUT",
        "BLOCK_TIMEOUT",
    }
    error_aliases = {
        "ERROR",
        "EXECUTION_FAILURE",
        "FAILURE",
        "EXEC_ERROR",
        "WORKLOAD_ERROR",
        "EXECUTION_ERRORS",
    }
    if normalized in performance_aliases:
        return SUCCESS_CRITERION_PERFORMANCE
    if normalized in blocking_aliases:
        return SUCCESS_CRITERION_BLOCKING
    if normalized in error_aliases:
        return SUCCESS_CRITERION_ERROR
    return SUCCESS_CRITERION_AUTO


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
        description="Evaluate anomaly reproduction by comparing SQL runtime medians."
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
    # NEW: absolute latency-increase fallback, robust to tiny/noisy baselines.
    parser.add_argument(
        "--min-abs-delta-seconds",
        dest="min_abs_delta_seconds",
        type=float,
        default=0.0,
        help=(
            "Absolute median latency increase (injected - baseline, in seconds) "
            "that also counts as a successful anomaly reproduction. 0 disables "
            "this fallback."
        ),
    )
    # NEW: anomaly type hint (OLAP / OLTP / BOTH) carried from the incident record.
    parser.add_argument(
        "--anomaly-type",
        dest="anomaly_type",
        default="BOTH",
        help="Anomaly type hint from the record: OLAP, OLTP, or BOTH (default BOTH)",
    )
    # NEW: LLM-declared manifestation category that selects the authoritative
    # success signal (see module docstring). Defaults to AUTO (historical
    # broadened OR judgement) so the script remains drop-in compatible.
    parser.add_argument(
        "--success-criterion",
        dest="success_criterion",
        default=SUCCESS_CRITERION_AUTO,
        help=(
            "LLM-declared success criterion selecting how anomaly-injection "
            "success is judged for this record: PERFORMANCE_DEGRADATION, "
            "BLOCKING_TIMEOUT, EXECUTION_ERROR, or AUTO (default AUTO = accept "
            "any of relative slowdown / absolute delta / injected error / "
            "blocked-until-timeout)."
        ),
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


def log_summary(
    baseline: PhaseResult,
    injected: PhaseResult,
    change_percent: float,
    abs_delta_seconds: float,
    anomaly_type: str,
    success_criterion: str,
) -> None:
    """Write concise summary logs."""

    logging.info("Anomaly type hint: %s", anomaly_type)
    logging.info("Declared success criterion: %s", success_criterion)
    logging.info("Baseline median duration: %.6f seconds", baseline.median_seconds)
    logging.info("Injected median duration: %.6f seconds", injected.median_seconds)
    logging.info("Performance change percentage: %.2f%%", change_percent)
    logging.info("Absolute median latency increase: %.6f seconds", abs_delta_seconds)
    logging.info("Performance-degradation relative threshold: %.2f%%", SUCCESS_THRESHOLD_PERCENT)
    if MIN_ABS_DELTA_SECONDS > 0:
        logging.info("Performance-degradation absolute fallback: %.6f seconds", MIN_ABS_DELTA_SECONDS)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Program entry point."""

    configure_logging()
    try:
        global DEFAULT_RUNS, SUCCESS_THRESHOLD_PERCENT, MIN_ABS_DELTA_SECONDS
        args = parse_args(argv)
        # Clamp to at least 1 so the workload always runs at least once even if a
        # non-positive value slips through; 1 keeps the safe single measured-run
        # default (always preceded by one warm-up pass, regardless of the count).
        DEFAULT_RUNS = max(1, int(args.median_runs))
        # Override the built-in performance-degradation thresholds when provided
        # on the command line (kept for backward compatibility; they now only
        # quantify "significant" slowdown within the PERFORMANCE_DEGRADATION
        # category rather than deciding success by themselves).
        SUCCESS_THRESHOLD_PERCENT = args.success_threshold_percent
        MIN_ABS_DELTA_SECONDS = max(0.0, args.min_abs_delta_seconds)
        anomaly_type = normalize_anomaly_type(args.anomaly_type)
        success_criterion = normalize_success_criterion(args.success_criterion)
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
        logging.info("Anomaly type hint: %s", anomaly_type)
        logging.info("Declared success criterion: %s", success_criterion)
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
            # strong ("hard") evidence that the anomaly took effect and the
            # workload can no longer complete normally, so it is accepted as a
            # successful reproduction under EVERY declared success criterion
            # (a blocked-until-timeout is at least as severe as the declared
            # performance-degradation / execution-error manifestation). This also
            # avoids misleading the downstream LLM into thinking the timeout
            # threshold was merely too small.
            if is_timeout_error(exc):
                logging.info("========== Injected Verification: END ==========")
                logging.info(
                    "Verified workload was blocked until timeout after injection (%s).",
                    exc,
                )
                logging.info(
                    "[JUDGEMENT] success branch = BLOCKED_UNTIL_TIMEOUT (declared "
                    "criterion=%s): the injected anomaly blocked the verification "
                    "workload until it timed out. This hard evidence is treated as a "
                    "successful reproduction under any criterion, not an insufficient "
                    "timeout threshold.",
                    success_criterion,
                )
                logging.info("Anomaly successfully reproduced and verified.")
                return 0
            # Any non-timeout error keeps the original failure behaviour.
            raise
        logging.info("========== Injected Verification: END ==========")

        change_percent = performance_change_percent(
            baseline.median_seconds, injected.median_seconds
        )
        abs_delta_seconds = injected.median_seconds - baseline.median_seconds
        log_summary(baseline, injected, change_percent, abs_delta_seconds, anomaly_type, success_criterion)

        # A verified-workload error after injection is "hard" evidence.
        injected_has_error = any(item.has_error for item in injected.runs)
        if injected_has_error:
            logging.info("Verified workload failed after injection, indicating an anomaly.")

        # Category-driven judgement.
        #   - "Hard" evidence (injected workload error, or blocked-until-timeout
        #     handled above) unambiguously proves the workload can no longer
        #     complete normally, so it is accepted under EVERY criterion.
        #   - "Soft" evidence (pure slowdown: relative degradation OR the absolute
        #     latency-increase fallback) only satisfies the PERFORMANCE_DEGRADATION
        #     category (and AUTO, the historical broadened OR judgement). For a
        #     record declared as BLOCKING_TIMEOUT / EXECUTION_ERROR, a mere
        #     slowdown is NOT the declared manifestation and is reported as a
        #     failure so the LLM strengthens the anomaly toward a real block/error.
        # The branch that fired is logged explicitly for the LLM Observation.
        meets_relative = change_percent >= SUCCESS_THRESHOLD_PERCENT
        meets_absolute = MIN_ABS_DELTA_SECONDS > 0 and abs_delta_seconds >= MIN_ABS_DELTA_SECONDS
        meets_degradation = meets_relative or meets_absolute
        degradation_allowed = success_criterion in (
            SUCCESS_CRITERION_PERFORMANCE,
            SUCCESS_CRITERION_AUTO,
        )

        # Hard evidence: injected workload error. Accepted under every criterion.
        if injected_has_error:
            logging.info(
                "[JUDGEMENT] success branch = INJECTED_WORKLOAD_ERROR (declared "
                "criterion=%s): the verified workload errored after injection. This "
                "hard evidence is accepted under any criterion.",
                success_criterion,
            )
            logging.info("Anomaly successfully reproduced and verified.")
            return 0

        # Soft evidence: pure slowdown, category-gated.
        if meets_degradation and degradation_allowed:
            if meets_relative:
                logging.info(
                    "[JUDGEMENT] success branch = RELATIVE_DEGRADATION (declared "
                    "criterion=%s): performance change %.2f%% >= threshold %.2f%%.",
                    success_criterion,
                    change_percent,
                    SUCCESS_THRESHOLD_PERCENT,
                )
            if meets_absolute:
                logging.info(
                    "[JUDGEMENT] success branch = ABSOLUTE_DELTA (declared "
                    "criterion=%s): median latency increase %.6f s >= fallback %.6f s.",
                    success_criterion,
                    abs_delta_seconds,
                    MIN_ABS_DELTA_SECONDS,
                )
            logging.info("Anomaly successfully reproduced and verified.")
            return 0

        if meets_degradation and not degradation_allowed:
            # A slowdown was measured, but the record was declared as a
            # blocking/error manifestation; a mere slowdown does not satisfy it.
            logging.error(
                "[JUDGEMENT] no success branch fired: a performance slowdown was "
                "observed (relative change %.2f%%, absolute delta %.6f s) but the "
                "declared success criterion is %s, which requires the verified "
                "workload to actually %s after injection rather than merely slow "
                "down. Strengthen the anomaly toward the declared manifestation and "
                "finish again.",
                change_percent,
                abs_delta_seconds,
                success_criterion,
                "block until timeout"
                if success_criterion == SUCCESS_CRITERION_BLOCKING
                else "raise an execution error",
            )
            logging.error("Anomaly reproduction or verification failed.")
            return 1

        logging.error(
            "[JUDGEMENT] no success branch fired (declared criterion=%s): relative "
            "change %.2f%% (< %.2f%%), absolute delta %.6f s (< %.6f s), no injected "
            "workload error, and the verified workload was not blocked until timeout.",
            success_criterion,
            change_percent,
            SUCCESS_THRESHOLD_PERCENT,
            abs_delta_seconds,
            MIN_ABS_DELTA_SECONDS,
        )
        logging.error("Anomaly reproduction or verification failed.")
        return 1

    except EvaluationError as exc:
        logging.error("Evaluation flow failed: %s", exc)
        logging.error("Anomaly reproduction or verification failed.")
        return 1
    except KeyboardInterrupt:
        logging.error("Execution was interrupted by the user.")
        logging.error("Anomaly reproduction or verification failed.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
''',
    'Scripts/initialize_database_pg.sh': r'''#!/bin/sh
# set -eu
set -euo pipefail

# =============================================
# Default values (can be overridden by command-line arguments)
# =============================================
DB_HOST="127.0.0.1"
DB_PORT="5432"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"
DB_SCHEMA="diagnosis"

REMOTE_HIGH_PRV_USER="root"
REMOTE_HIGH_PRV_PASSWORD="root"

DB_NAME=""
INSTANCE_DIR=""
TMP_DIR=""
DB_TYPE="dump"

# =============================================
# Input Parameters and Usage
# =============================================
usage() {
    echo "Usage: $0 [--host <host>] [--port <port>] --database_name <dbname> --database_type <type> [--database_schema <schema>] --instance_dir <instance_dir> --tmp_dir <tmp_dir> [--user <user>] [--password <pwd>]"
    echo "  --host           Remote PostgreSQL host (default: 127.0.0.1)"
    echo "  --port           Remote PostgreSQL port (default: 5432)"
    echo "  --database_name  Target database name (required)"
    echo "  --database_type  only 'dump' file is allowed for now (required)"
    echo "  --database_schema  Target database schema (default: diagnosis)"
    echo "  --instance_dir   Local directory containing database instance (required)"
    echo "  --tmp_dir        Temporary directory for configuration files (required)"
    echo "  --user           Database user (default: diagnosis)"
    echo "  --password       Database password (default: diagnosis)"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --host)
            DB_HOST="$2"
            shift 2
            ;;
        --port)
            DB_PORT="$2"
            shift 2
            ;;
        --database_name)
            DB_NAME="$2"
            shift 2
            ;;
        --database_type)
            DB_TYPE="$2"
            shift 2
            ;;
        --database_schema)
            DB_SCHEMA="$2"
            shift 2
            ;;
        --instance_dir)
            INSTANCE_DIR="$2"
            shift 2
            ;;
        --tmp_dir)
            TMP_DIR="$2"
            shift 2
            ;;
        --user)
            DB_USER="$2"
            shift 2
            ;;
        --password)
            DB_PASSWORD="$2"
            shift 2
            ;;
        
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

if [ -z "$DB_NAME" ] ; then
    echo "Error: Missing required argument '--database_name'."
    usage
fi

if [ -z "$DB_TYPE" ] || { [ "$DB_TYPE" != "dump" ]; }; then
    echo "Error: Missing or invalid required argument '--database_type'. Must be 'dump' for now."
    usage
fi

if [ -z "$INSTANCE_DIR" ] ; then
    echo "Error: Missing required argument '--instance_dir'."
    usage
fi

if [ -z "$TMP_DIR" ] ; then
    echo "Error: Missing required argument '--tmp_dir'."
    usage
fi

# =============================================
# Connection options
# =============================================
export PGPASSWORD="$DB_PASSWORD"
CONN_OPTS="-h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_USER"
HIGH_CONN_OPTS="-h $DB_HOST -p $DB_PORT -U $REMOTE_HIGH_PRV_USER -d $REMOTE_HIGH_PRV_USER"

# =============================================
# 0. TERMINATE ACTIVE CONNECTIONS (Robust)
# =============================================
echo "-- Checking active connections to database '$DB_NAME' ..."

export PGPASSWORD="$REMOTE_HIGH_PRV_PASSWORD"

ADMIN_OPTS=$(echo "$HIGH_CONN_OPTS" | sed -E 's/(\s+-d\s+\S+)|(\s+--dbname=\S+)//g')" -d postgres"

echo "Executing: psql $ADMIN_OPTS -t -c \"SELECT count(*) FROM pg_stat_activity WHERE datname = '$DB_NAME';\"" >&2

set +e
ACTIVE_CONN_OUTPUT=$(psql $ADMIN_OPTS -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname = '$DB_NAME';" 2>&1)
STATUS=$?
set -e

if [ $STATUS -ne 0 ]; then
    echo "❌ Failed to execute query for active connection count." >&2
    echo "Error message:" >&2
    echo "$ACTIVE_CONN_OUTPUT" >&2
    exit 1
fi

ACTIVE_CONN_COUNT=$(echo "$ACTIVE_CONN_OUTPUT" | xargs)

if [ -z "$ACTIVE_CONN_COUNT" ]; then
    echo "❌ Failed to get active connection count (empty result). Please check if postgres database is accessible." >&2
    exit 1
fi

echo "🔢 Current active connections: $ACTIVE_CONN_COUNT"

echo "-- Terminating all active connections to database '$DB_NAME' ..."
psql $ADMIN_OPTS -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();" > /dev/null 2>&1 || true

echo "✅ Active connections terminated."

export PGPASSWORD="$DB_PASSWORD"

if ! psql $CONN_OPTS -c "SELECT 1" > /dev/null 2>&1; then
    echo "Error: Cannot connect to remote PostgreSQL at $DB_HOST:$DB_PORT as $DB_USER"
    exit 1
fi
echo "✅ Connected to remote PostgreSQL."

# =============================================
# 1. CREATE DATABASE IF NOT EXISTS
# =============================================
DB_EXISTS=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'")

if [ "$DB_EXISTS" == "1" ]; then
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
        -c "DROP DATABASE $DB_NAME;"
    echo "✅ Existing database $DB_NAME dropped."
fi

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
    -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;"
echo "✅ Database $DB_NAME created."


# =============================================
# 2. Apply configuration parameters from JSON
# =============================================
echo "-- Applying configuration parameters from Default Backup ..."

LOCAL_CONFIG_DIR="$TMP_DIR/Local_Instance"
DATA_DIR="/var/lib/postgresql/16/main"
REMOTE_CONFIG_DIR="$TMP_DIR/RDS"

PG_CONF="$DATA_DIR/postgresql.conf"
PG_HBA="$DATA_DIR/pg_hba.conf"


if [ "$DB_HOST" = "127.0.0.1" ] || [ "$DB_HOST" = "localhost" ]; then
    echo "Local instance detected. Copying configuration files from $LOCAL_CONFIG_DIR ..."

    mkdir -p "$LOCAL_CONFIG_DIR"
    mkdir -p "$REMOTE_CONFIG_DIR"
    chmod 755 "$LOCAL_CONFIG_DIR" "$REMOTE_CONFIG_DIR"
    

    cp "$PG_CONF" "$LOCAL_CONFIG_DIR/postgresql.conf.bak"
    cp "$PG_HBA" "$LOCAL_CONFIG_DIR/pg_hba.conf.bak"
    chmod 644 "$LOCAL_CONFIG_DIR/postgresql.conf.bak" "$LOCAL_CONFIG_DIR/pg_hba.conf.bak"
    
    echo "✅ Local configuration files restored. Restart PostgreSQL if needed."
    su - postgres -c "/usr/lib/postgresql/16/bin/pg_ctl -D $DATA_DIR restart"
    sleep 3
else
    echo "REMOTE CONNECTION needs to support initialization via remote superuser"

    CONFIG_JSON="$REMOTE_CONFIG_DIR/configurations.json"   

    if ! command -v jq > /dev/null 2>&1; then
        echo "Error: 'jq' is required to parse JSON config."
        exit 1
    fi

    jq -r '.[] | "\(.name) \(.setting)"' "$CONFIG_JSON" | while read -r param value; do
        if [ -z "$value" ] || [ "$value" = "null" ]; then
            echo "   Skipping $param (no valid setting)"
            continue
        fi
        echo "   Setting $param = $value"

        set +e
        err_output=$(psql $CONN_OPTS -c "ALTER SYSTEM SET $param TO '$value';" 2>&1)
        cmd_exit=$?
        set -e

        if [ $cmd_exit -ne 0 ]; then
            error_msg=$(echo "$err_output" | grep -i "error:" | head -1 | sed 's/^ERROR: //')
            if [ -z "$error_msg" ]; then
                error_msg="$err_output"
            fi
            echo "   Warning: Failed to set $param - $error_msg"
        fi
    done

    echo "✅ Configuration parameters applied. Reloading PostgreSQL configuration..."

    PGPASSWORD=$REMOTE_HIGH_PRV_PASSWORD psql $HIGH_CONN_OPTS -c "SELECT pg_reload_conf();"
    echo "✅ Configuration applied and reloaded."
fi

# =============================================
# 3. Recreate database (DROP + CREATE)
# =============================================
echo "-- Recreating database '$DB_NAME' ..."
psql $CONN_OPTS -c "DROP DATABASE IF EXISTS $DB_NAME;" > /dev/null 2>&1
psql $CONN_OPTS -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;" > /dev/null 2>&1
echo "✅ Database '$DB_NAME' created."

echo "-- Creating schema '$DB_SCHEMA' and setting default search_path ..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA AUTHORIZATION $DB_USER;" > /dev/null 2>&1

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -c "ALTER DATABASE $DB_NAME SET search_path TO $DB_SCHEMA, public;" > /dev/null 2>&1

echo "✅ Schema '$DB_SCHEMA' created (Owner: $DB_USER) and search_path configured."

# =============================================
# 4. DATA IMPORT
# =============================================
cd "$INSTANCE_DIR" || { echo "Error: Cannot cd into $INSTANCE_DIR"; exit 1; }

if [ "$DB_TYPE" = "dump" ]; then
    DUMP_FILE="${DB_NAME}.dump"
    if [ -f "$DUMP_FILE" ]; then
        echo "-- Importing dump file $DUMP_FILE using psql ..."

        echo "   Ensuring '$DB_SCHEMA' schema ownership and privileges for user $DB_USER..."
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
            -c "ALTER SCHEMA $DB_SCHEMA OWNER TO \"$DB_USER\";" || true
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
            -c "GRANT ALL ON SCHEMA $DB_SCHEMA TO \"$DB_USER\";" || true

        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" < "$DUMP_FILE"
        echo "✅ Dump imported."
    else
        SQL_FILE="${DB_NAME}.sql"
        if [ -f "$SQL_FILE" ]; then
            echo "-- Importing SQL file $SQL_FILE using psql ..."
            psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$SQL_FILE"
            echo "✅ SQL file imported."
        else
            echo "Error: No dump or SQL file found for database $DB_NAME (expected ${DB_NAME}.dump or ${DB_NAME}.sql)"
            exit 1
        fi
    fi
    
else
    echo "Error: Invalid database type '$DB_TYPE'. Use 'dump' for now."
    exit 1
fi

echo "✅ PostgreSQL initialization completed successfully."
''',
    'Scripts/initialize_database_mysql.sh': r'''#!/bin/bash
# set -eu
set -euo pipefail

# =============================================
# Default values (can be overridden by command-line arguments)
# =============================================
DB_HOST="127.0.0.1"
DB_PORT="3306"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"
DB_SCHEMA="diagnosis"

REMOTE_HIGH_PRV_USER="root"
REMOTE_HIGH_PRV_PASSWORD="root"

DB_NAME=""
INSTANCE_DIR=""
TMP_DIR=""
DB_TYPE="dump"

# =============================================
# Input Parameters and Usage
# =============================================
usage() {
    echo "Usage: $0 [--host <host>] [--port <port>] --database_name <dbname> --database_type <type> [--database_schema <schema>] --instance_dir <instance_dir> --tmp_dir <tmp_dir> [--user <user>] [--password <pwd>]"
    echo "  --host           Remote MySQL host (default: 127.0.0.1)"
    echo "  --port           Remote MySQL port (default: 3306)"
    echo "  --database_name  Target database name (required)"
    echo "  --database_type  'dump' or 'load_data' (required)"
    echo "  --database_schema  Ignored for MySQL; tables are imported into --database_name"
    echo "  --instance_dir   Local directory containing database instance (required)"
    echo "  --tmp_dir        Temporary directory for configuration files (required)"
    echo "  --user           Database user (default: diagnosis)"
    echo "  --password       Database password (default: diagnosis)"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --host)
            DB_HOST="$2"
            shift 2
            ;;
        --port)
            DB_PORT="$2"
            shift 2
            ;;
        --database_name)
            DB_NAME="$2"
            shift 2
            ;;
        --database_type)
            DB_TYPE="$2"
            shift 2
            ;;
        --database_schema)
            DB_SCHEMA="$2"
            shift 2
            ;;
        --instance_dir)
            INSTANCE_DIR="$2"
            shift 2
            ;;
        --tmp_dir)
            TMP_DIR="$2"
            shift 2
            ;;
        --user)
            DB_USER="$2"
            shift 2
            ;;
        --password)
            DB_PASSWORD="$2"
            shift 2
            ;;

        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

if [ -z "$DB_NAME" ] ; then
    echo "Error: Missing required argument '--database_name'."
    usage
fi

if [ -z "$DB_TYPE" ] || { [ "$DB_TYPE" != "dump" ] && [ "$DB_TYPE" != "load_data" ]; }; then
    echo "Error: Missing or invalid required argument '--database_type'. Must be 'dump' or 'load_data'."
    usage
fi

if [ -z "$INSTANCE_DIR" ] ; then
    echo "Error: Missing required argument '--instance_dir'."
    usage
fi

if [ -z "$TMP_DIR" ] ; then
    echo "Error: Missing required argument '--tmp_dir'."
    usage
fi

# =============================================
# Connection options
# =============================================
export MYSQL_PWD="$DB_PASSWORD"
MYSQL_CLIENT_HOST="$DB_HOST"
if [ "$MYSQL_CLIENT_HOST" = "localhost" ]; then
    if MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysqladmin -h 127.0.0.1 -P "$DB_PORT" -u "$REMOTE_HIGH_PRV_USER" ping > /dev/null 2>&1; then
        MYSQL_CLIENT_HOST="127.0.0.1"
    fi
fi
CONN_OPTS="-h $MYSQL_CLIENT_HOST -P $DB_PORT -u $DB_USER"
HIGH_CONN_OPTS="-h $MYSQL_CLIENT_HOST -P $DB_PORT -u $REMOTE_HIGH_PRV_USER -D mysql"

wait_for_mysql_ready() {
    local opts="$1"
    local timeout="${2:-60}"
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        if mysqladmin $opts ping > /dev/null 2>&1 && mysql $opts -e "SELECT 1" > /dev/null 2>&1; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    echo "Error: MySQL is not ready after ${timeout}s." >&2
    mysqladmin $opts ping >&2 || true
    return 1
}

mysql_admin_scalar() {
    local expr="$1"
    MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysql $ADMIN_OPTS -N -B -e "SELECT @@GLOBAL.${expr};" | xargs
}

set_global_if_possible() {
    local param="$1"
    local value="$2"
    local err_output
    local cmd_exit

    set +e
    err_output=$(MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysql $ADMIN_OPTS -e "SET GLOBAL ${param} = ${value};" 2>&1)
    cmd_exit=$?
    set -e

    if [ "$cmd_exit" -ne 0 ]; then
        echo "   Warning: Failed to set ${param}=${value} - $(echo "$err_output" | tr '\n' ' ')" >&2
        return 1
    fi
    echo "   ${param}=${value}"
}

available_memory_bytes() {
    local mem_bytes
    if [ -f /sys/fs/cgroup/memory.max ]; then
        mem_bytes=$(cat /sys/fs/cgroup/memory.max)
        if [ "$mem_bytes" != "max" ]; then
            echo "$mem_bytes"
            return 0
        fi
    fi
    if [ -f /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
        mem_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        if [ "$mem_bytes" -lt 9223372036854771712 ]; then
            echo "$mem_bytes"
            return 0
        fi
    fi
    awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo
}

target_buffer_pool_size_bytes() {
    local mem_bytes
    local half_mem
    local one_gib=$((1024 * 1024 * 1024))
    local four_gib=$((4 * 1024 * 1024 * 1024))

    mem_bytes=$(available_memory_bytes)
    half_mem=$((mem_bytes / 2))
    if [ "$half_mem" -lt "$one_gib" ]; then
        echo "$half_mem"
    elif [ "$half_mem" -gt "$four_gib" ]; then
        echo "$four_gib"
    else
        echo "$half_mem"
    fi
}

restore_import_tuning() {
    if [ "${IMPORT_TUNING_APPLIED:-0}" != "1" ]; then
        return 0
    fi

    echo "-- Restoring MySQL import tuning parameters ..."
    export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"
    set_global_if_possible "innodb_flush_log_at_trx_commit" "$ORIGINAL_INNODB_FLUSH_LOG_AT_TRX_COMMIT" || true
    set_global_if_possible "sync_binlog" "$ORIGINAL_SYNC_BINLOG" || true
    set_global_if_possible "innodb_redo_log_capacity" "$ORIGINAL_INNODB_REDO_LOG_CAPACITY" || true
    set_global_if_possible "innodb_buffer_pool_size" "$ORIGINAL_INNODB_BUFFER_POOL_SIZE" || true
    set_global_if_possible "local_infile" "$ORIGINAL_LOCAL_INFILE" || true
    IMPORT_TUNING_APPLIED=0
    trap - EXIT
    export MYSQL_PWD="$DB_PASSWORD"
    return 0
}

apply_import_tuning() {
    echo "-- Applying temporary MySQL import tuning parameters ..."
    export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"

    ORIGINAL_INNODB_FLUSH_LOG_AT_TRX_COMMIT=$(mysql_admin_scalar "innodb_flush_log_at_trx_commit")
    ORIGINAL_SYNC_BINLOG=$(mysql_admin_scalar "sync_binlog")
    ORIGINAL_INNODB_REDO_LOG_CAPACITY=$(mysql_admin_scalar "innodb_redo_log_capacity")
    ORIGINAL_INNODB_BUFFER_POOL_SIZE=$(mysql_admin_scalar "innodb_buffer_pool_size")
    ORIGINAL_LOCAL_INFILE=$(mysql_admin_scalar "local_infile")
    TARGET_INNODB_BUFFER_POOL_SIZE=$(target_buffer_pool_size_bytes)
    IMPORT_TUNING_APPLIED=1
    trap restore_import_tuning EXIT

    set_global_if_possible "innodb_flush_log_at_trx_commit" "2" || true
    set_global_if_possible "sync_binlog" "0" || true
    set_global_if_possible "innodb_redo_log_capacity" "$((1024 * 1024 * 1024))" || true
    if [ "$TARGET_INNODB_BUFFER_POOL_SIZE" -gt "$ORIGINAL_INNODB_BUFFER_POOL_SIZE" ]; then
        set_global_if_possible "innodb_buffer_pool_size" "$TARGET_INNODB_BUFFER_POOL_SIZE" || true
    fi
    set_global_if_possible "local_infile" "ON" || true

    export MYSQL_PWD="$DB_PASSWORD"
    return 0
}

log_mysql_processlist() {
    echo "-- MySQL processlist snapshot for troubleshooting:" >&2
    MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysql $ADMIN_OPTS -e \
        "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 500) AS INFO_PREFIX FROM information_schema.PROCESSLIST ORDER BY TIME DESC;" >&2 || true
}

rewrite_mysql_source_to_database() {
    local source_file="$1"
    local db_name="$2"

    awk -v db_name="$db_name" '
        BEGIN { IGNORECASE = 1 }
        /^[[:space:]]*CREATE DATABASE/ {
            sub(/`[^`]+`/, "`" db_name "`")
            print
            next
        }
        /^[[:space:]]*USE[[:space:]]+`[^`]+`;[[:space:]]*$/ {
            print "USE `" db_name "`;"
            next
        }
        { print }
    ' "$source_file"
}

emit_mysql_import_stream() {
    local source_file="$1"
    local db_name="$2"

    printf "%s\n" "SET SESSION AUTOCOMMIT=0;"
    printf "%s\n" "SET SESSION UNIQUE_CHECKS=0;"
    printf "%s\n" "SET SESSION FOREIGN_KEY_CHECKS=0;"
    rewrite_mysql_source_to_database "$source_file" "$db_name"
    printf "%s\n" "COMMIT;"
    printf "%s\n" "SET SESSION FOREIGN_KEY_CHECKS=1;"
    printf "%s\n" "SET SESSION UNIQUE_CHECKS=1;"
    printf "%s\n" "SET SESSION AUTOCOMMIT=1;"
}

run_mysql_import() {
    local source_file="$1"
    local source_label="$2"
    local timeout_seconds="${MYSQL_IMPORT_TIMEOUT_SECONDS:-21600}"
    local cmd_exit

    echo "   Import timeout: ${timeout_seconds}s"
    set +e
    if command -v timeout > /dev/null 2>&1; then
        emit_mysql_import_stream "$source_file" "$DB_NAME" | timeout "$timeout_seconds" mysql $CONN_OPTS "$DB_NAME"
        pipeline_status=("${PIPESTATUS[@]}")
        if [ "${pipeline_status[0]}" -ne 0 ]; then
            cmd_exit="${pipeline_status[0]}"
        else
            cmd_exit="${pipeline_status[1]}"
        fi
    else
        emit_mysql_import_stream "$source_file" "$DB_NAME" | mysql $CONN_OPTS "$DB_NAME"
        pipeline_status=("${PIPESTATUS[@]}")
        if [ "${pipeline_status[0]}" -ne 0 ]; then
            cmd_exit="${pipeline_status[0]}"
        else
            cmd_exit="${pipeline_status[1]}"
        fi
    fi
    set -e

    if [ "$cmd_exit" -eq 124 ]; then
        echo "Error: ${source_label} import timed out after ${timeout_seconds}s." >&2
        log_mysql_processlist
        exit 1
    fi
    if [ "$cmd_exit" -ne 0 ]; then
        echo "Error: ${source_label} import failed with exit code ${cmd_exit}." >&2
        log_mysql_processlist
        exit 1
    fi
}

mysql_string_literal() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\'/\\\'}"
    printf "'%s'" "$value"
}

mysql_identifier() {
    local value="$1"
    value="${value//\`/\`\`}"
    printf "\`%s\`" "$value"
}

emit_mysql_load_data_stream() {
    local load_dir="$1"
    local manifest_file="$load_dir/manifest.tsv"
    local table_name
    local relative_file
    local row_count
    local data_file
    local table_identifier
    local data_literal
    local is_header=1

    printf "%s\n" "SET SESSION AUTOCOMMIT=0;"
    printf "%s\n" "SET SESSION UNIQUE_CHECKS=0;"
    printf "%s\n" "SET SESSION FOREIGN_KEY_CHECKS=0;"

    while IFS=$'\t' read -r table_name relative_file row_count; do
        if [ "$is_header" -eq 1 ]; then
            is_header=0
            continue
        fi
        if [ -z "$table_name" ] || [ -z "$relative_file" ]; then
            continue
        fi
        data_file="$load_dir/$relative_file"
        if [ ! -f "$data_file" ]; then
            echo "Error: LOAD DATA file not found: $data_file" >&2
            return 1
        fi
        table_identifier=$(mysql_identifier "$table_name")
        data_literal=$(mysql_string_literal "$(readlink -f "$data_file")")
        printf "LOAD DATA LOCAL INFILE %s INTO TABLE %s CHARACTER SET utf8mb4 FIELDS TERMINATED BY %s ESCAPED BY %s LINES TERMINATED BY %s;\n" \
            "$data_literal" "$table_identifier" "'\\t'" "'\\\\'" "'\\n'"
    done < "$manifest_file"

    printf "%s\n" "COMMIT;"
    printf "%s\n" "SET SESSION FOREIGN_KEY_CHECKS=1;"
    printf "%s\n" "SET SESSION UNIQUE_CHECKS=1;"
    printf "%s\n" "SET SESSION AUTOCOMMIT=1;"
}

run_mysql_load_data_files() {
    local load_dir="$1"
    local timeout_seconds="${MYSQL_IMPORT_TIMEOUT_SECONDS:-21600}"
    local cmd_exit

    echo "   LOAD DATA timeout: ${timeout_seconds}s"
    set +e
    if command -v timeout > /dev/null 2>&1; then
        emit_mysql_load_data_stream "$load_dir" | timeout "$timeout_seconds" mysql --local-infile=1 $CONN_OPTS "$DB_NAME"
        pipeline_status=("${PIPESTATUS[@]}")
        if [ "${pipeline_status[0]}" -ne 0 ]; then
            cmd_exit="${pipeline_status[0]}"
        else
            cmd_exit="${pipeline_status[1]}"
        fi
    else
        emit_mysql_load_data_stream "$load_dir" | mysql --local-infile=1 $CONN_OPTS "$DB_NAME"
        pipeline_status=("${PIPESTATUS[@]}")
        if [ "${pipeline_status[0]}" -ne 0 ]; then
            cmd_exit="${pipeline_status[0]}"
        else
            cmd_exit="${pipeline_status[1]}"
        fi
    fi
    set -e

    if [ "$cmd_exit" -eq 124 ]; then
        echo "Error: LOAD DATA import timed out after ${timeout_seconds}s." >&2
        log_mysql_processlist
        exit 1
    fi
    if [ "$cmd_exit" -ne 0 ]; then
        echo "Error: LOAD DATA import failed with exit code ${cmd_exit}." >&2
        log_mysql_processlist
        exit 1
    fi
}

run_load_data_instance() {
    local load_dir="$1"

    if [ ! -f "$load_dir/manifest.tsv" ] || [ ! -f "$load_dir/schema.sql" ]; then
        echo "Error: Invalid LOAD DATA instance directory: $load_dir" >&2
        exit 1
    fi

    echo "-- Importing LOAD DATA instance from $load_dir ..."
    echo "   Ensuring '$DB_NAME' privileges for user $DB_USER..."
    export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"
    mysql $ADMIN_OPTS \
        -e "GRANT ALL PRIVILEGES ON \`$DB_NAME\`.* TO '$DB_USER'@'%';" || true
    export MYSQL_PWD="$DB_PASSWORD"

    apply_import_tuning
    echo "   Loading schema ..."
    run_mysql_import "$load_dir/schema.sql" "LOAD DATA schema $load_dir/schema.sql"
    echo "   Loading table data via LOAD DATA LOCAL INFILE ..."
    run_mysql_load_data_files "$load_dir"
    if [ -s "$load_dir/post_load.sql" ]; then
        echo "   Building secondary indexes and foreign keys ..."
        run_mysql_import "$load_dir/post_load.sql" "LOAD DATA post-load DDL $load_dir/post_load.sql"
    fi
    restore_import_tuning
    echo "✅ LOAD DATA instance imported."
}

# =============================================
# 0. TERMINATE ACTIVE CONNECTIONS (Robust)
# =============================================
echo "-- Checking active connections to database '$DB_NAME' ..."

export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"

ADMIN_OPTS=$(echo "$HIGH_CONN_OPTS" | sed -E 's/(\s+-D\s+\S+)|(\s+--database=\S+)//g')

echo "Executing: mysql $ADMIN_OPTS -N -B -e \"SELECT count(*) FROM information_schema.PROCESSLIST WHERE DB = '$DB_NAME';\"" >&2

set +e
ACTIVE_CONN_OUTPUT=$(mysql $ADMIN_OPTS -N -B -e "SELECT count(*) FROM information_schema.PROCESSLIST WHERE DB = '$DB_NAME';" 2>&1)
STATUS=$?
set -e

if [ $STATUS -ne 0 ]; then
    echo "❌ Failed to execute query for active connection count." >&2
    echo "Error message:" >&2
    echo "$ACTIVE_CONN_OUTPUT" >&2
    exit 1
fi

ACTIVE_CONN_COUNT=$(echo "$ACTIVE_CONN_OUTPUT" | xargs)

if [ -z "$ACTIVE_CONN_COUNT" ]; then
    echo "❌ Failed to get active connection count (empty result). Please check if mysql database is accessible." >&2
    exit 1
fi

echo "🔢 Current active connections: $ACTIVE_CONN_COUNT"

echo "-- Terminating all active connections to database '$DB_NAME' ..."
mysql $ADMIN_OPTS -N -B -e "SELECT CONCAT('KILL ', ID, ';') FROM information_schema.PROCESSLIST WHERE DB = '$DB_NAME' AND ID <> CONNECTION_ID();" | mysql $ADMIN_OPTS > /dev/null 2>&1 || true

echo "✅ Active connections terminated."

export MYSQL_PWD="$DB_PASSWORD"

if ! mysql $CONN_OPTS -e "SELECT 1" > /dev/null 2>&1; then
    echo "Error: Cannot connect to remote MySQL at $DB_HOST:$DB_PORT as $DB_USER"
    exit 1
fi
echo "✅ Connected to remote MySQL."

# =============================================
# 1. CREATE DATABASE IF NOT EXISTS
# =============================================
DB_EXISTS=$(mysql -h "$MYSQL_CLIENT_HOST" -P "$DB_PORT" -u "$DB_USER" -N -B -e \
    "SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='$DB_NAME'")

if [ "$DB_EXISTS" == "1" ]; then
    mysql -h "$MYSQL_CLIENT_HOST" -P "$DB_PORT" -u "$DB_USER" \
        -e "DROP DATABASE \`$DB_NAME\`;"
    echo "✅ Existing database $DB_NAME dropped."
fi

mysql -h "$MYSQL_CLIENT_HOST" -P "$DB_PORT" -u "$DB_USER" \
    -e "CREATE DATABASE \`$DB_NAME\`;"
echo "✅ Database $DB_NAME created."


# =============================================
# 2. Apply configuration parameters from JSON
# =============================================
echo "-- Applying configuration parameters from Default Backup ..."

LOCAL_CONFIG_DIR="$TMP_DIR/Local_Instance"
DATA_DIR="/var/lib/mysql"
REMOTE_CONFIG_DIR="$TMP_DIR/RDS"
MYSQL_TMPDIR="$TMP_DIR/mysql_tmp"

MYSQL_CONF="/etc/mysql/mysql.conf.d/dbdiag.cnf"
MYSQL_CNF="/etc/mysql/my.cnf"


if [ "$DB_HOST" = "127.0.0.1" ] || [ "$DB_HOST" = "localhost" ]; then
    echo "Local instance detected. Copying configuration files from $LOCAL_CONFIG_DIR ..."

    mkdir -p "$LOCAL_CONFIG_DIR"
    mkdir -p "$REMOTE_CONFIG_DIR"
    mkdir -p "$MYSQL_TMPDIR"
    chmod 755 "$LOCAL_CONFIG_DIR" "$REMOTE_CONFIG_DIR"
    chmod 1777 "$MYSQL_TMPDIR"

    cp "$MYSQL_CONF" "$LOCAL_CONFIG_DIR/dbdiag.cnf.bak"
    cp "$MYSQL_CNF" "$LOCAL_CONFIG_DIR/my.cnf.bak"
    chmod 644 "$LOCAL_CONFIG_DIR/dbdiag.cnf.bak" "$LOCAL_CONFIG_DIR/my.cnf.bak"

    echo "✅ Local configuration files restored. Restart MySQL if needed."
    MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysqladmin $ADMIN_OPTS shutdown > /dev/null 2>&1 || true
    sleep 3
    rm -f /var/run/mysqld/mysqld.sock /var/run/mysqld/mysqld.sock.lock \
          /var/run/mysqld/mysqlx.sock /var/run/mysqld/mysqlx.sock.lock \
          /var/run/mysqld/mysqld.pid 2>/dev/null || true
    mysqld --defaults-file="$MYSQL_CONF" --user=mysql --datadir="$DATA_DIR" --tmpdir="$MYSQL_TMPDIR" </dev/null >/dev/null 2>&1 &
    export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"
    wait_for_mysql_ready "$ADMIN_OPTS" 60
    mysql $ADMIN_OPTS -N -B -e "SHOW VARIABLES WHERE Variable_name IN ('tmpdir', 'innodb_tmpdir');" >&2 || true
else
    echo "REMOTE CONNECTION needs to support initialization via remote superuser"

    CONFIG_JSON="$REMOTE_CONFIG_DIR/configurations.json"

    if ! command -v jq > /dev/null 2>&1; then
        echo "Error: 'jq' is required to parse JSON config."
        exit 1
    fi

    jq -r '.[] | "\(.name) \(.setting)"' "$CONFIG_JSON" | while read -r param value; do
        if [ -z "$value" ] || [ "$value" = "null" ]; then
            echo "   Skipping $param (no valid setting)"
            continue
        fi
        echo "   Setting $param = $value"

        set +e
        err_output=$(mysql $CONN_OPTS -e "SET PERSIST $param = '$value';" 2>&1)
        cmd_exit=$?
        set -e

        if [ $cmd_exit -ne 0 ]; then
            error_msg=$(echo "$err_output" | grep -i "error" | head -1 | sed -E 's/^ERROR[[:space:]]+[0-9]+[[:space:]]+\([^)]*\):[[:space:]]*//')
            if [ -z "$error_msg" ]; then
                error_msg="$err_output"
            fi
            echo "   Warning: Failed to set $param - $error_msg"
        fi
    done

    echo "✅ Configuration parameters applied. Reloading MySQL configuration..."

    MYSQL_PWD=$REMOTE_HIGH_PRV_PASSWORD mysqladmin $ADMIN_OPTS reload
    echo "✅ Configuration applied and reloaded."
fi

# =============================================
# 3. Recreate database (DROP + CREATE)
# =============================================
echo "-- Recreating database '$DB_NAME' ..."
export MYSQL_PWD="$DB_PASSWORD"
if ! mysql $CONN_OPTS -e "DROP DATABASE IF EXISTS \`$DB_NAME\`;" > /dev/null; then
    echo "Error: Failed to drop database '$DB_NAME'." >&2
    exit 1
fi
if ! mysql $CONN_OPTS -e "CREATE DATABASE \`$DB_NAME\`;" > /dev/null; then
    echo "Error: Failed to create database '$DB_NAME'." >&2
    exit 1
fi
echo "✅ Database '$DB_NAME' created."

echo "-- Preparing target database '$DB_NAME' (MySQL schema parameter is ignored) ..."
mysql $CONN_OPTS \
    -e "USE \`$DB_NAME\`;" > /dev/null 2>&1

echo "✅ Target database '$DB_NAME' is ready for import."

# =============================================
# 4. DATA IMPORT
# =============================================
cd "$INSTANCE_DIR" || { echo "Error: Cannot cd into $INSTANCE_DIR"; exit 1; }

if [ "$DB_TYPE" = "load_data" ] || { [ "$DB_TYPE" = "dump" ] && [ -f "${DB_NAME}.load/manifest.tsv" ]; }; then
    LOAD_DIR="${DB_NAME}.load"
    run_load_data_instance "$LOAD_DIR"
elif [ "$DB_TYPE" = "dump" ]; then
    DUMP_FILE="${DB_NAME}.dump"
    if [ -f "$DUMP_FILE" ]; then
        echo "-- Importing dump file $DUMP_FILE using mysql ..."

        echo "   Ensuring '$DB_NAME' privileges for user $DB_USER..."
        export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"
        mysql $ADMIN_OPTS \
            -e "GRANT ALL PRIVILEGES ON \`$DB_NAME\`.* TO '$DB_USER'@'%';" || true
        export MYSQL_PWD="$DB_PASSWORD"
        apply_import_tuning

        echo "   Rewriting any dump database declaration to target database '$DB_NAME' during import..."
        run_mysql_import "$DUMP_FILE" "Dump file $DUMP_FILE"
        restore_import_tuning
        echo "✅ Dump imported."
    else
        SQL_FILE="${DB_NAME}.sql"
        if [ -f "$SQL_FILE" ]; then
            echo "-- Importing SQL file $SQL_FILE using mysql ..."
            apply_import_tuning
            echo "   Rewriting any SQL database declaration to target database '$DB_NAME' during import..."
            run_mysql_import "$SQL_FILE" "SQL file $SQL_FILE"
            restore_import_tuning
            echo "✅ SQL file imported."
        else
            echo "Error: No dump or SQL file found for database $DB_NAME (expected ${DB_NAME}.dump or ${DB_NAME}.sql)"
            exit 1
        fi
    fi

else
    echo "Error: Invalid database type '$DB_TYPE'. Use 'dump' for now."
    exit 1
fi

echo "✅ MySQL initialization completed successfully."
''',
    'Docker/Ubuntu_PG/init_dbms_postgresql.sh': r'''#!/bin/bash
set -Eeuo pipefail

REMOTE_IP="127.0.0.1"
REMOTE_PORT="5432"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"
PATH_VALUE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --remote_ip|-h) REMOTE_IP="$2"; shift 2 ;;
        --remote_port|-port) REMOTE_PORT="$2"; shift 2 ;;
        --user|-u) DB_USER="$2"; shift 2 ;;
        --password|-pwd) DB_PASSWORD="$2"; shift 2 ;;
        --path_value|-path) PATH_VALUE="$2"; shift 2 ;;
        --tmp_value|-tmp) shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$PATH_VALUE" ]]; then
    echo "Usage: $0 -path <PATH_VALUE>" >&2
    exit 1
fi

# docker_func.py invokes this entry point after docker run. The initialized
# cluster and fixed accounts are part of the image; only the daemon starts here.
if ! pg_isready -h "$REMOTE_IP" -p "$REMOTE_PORT" > /dev/null 2>&1; then
    rm -f /var/run/postgresql/.s.PGSQL.* /tmp/.s.PGSQL.* 2>/dev/null || true
    su -s /bin/bash postgres -c \
        "/usr/lib/postgresql/16/bin/pg_ctl -D /var/lib/postgresql/16/main -l /var/lib/postgresql/16/postgres.log start"
fi

deadline=$((SECONDS + 120))
until pg_isready -h "$REMOTE_IP" -p "$REMOTE_PORT" > /dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
        echo "PostgreSQL startup timed out after 120s" >&2
        exit 1
    fi
    sleep 1
done

PGPASSWORD=root psql -v ON_ERROR_STOP=1 -h "$REMOTE_IP" -p "$REMOTE_PORT" -U root -d postgres -c "SELECT 1" > /dev/null
PGPASSWORD="$DB_PASSWORD" psql -v ON_ERROR_STOP=1 -h "$REMOTE_IP" -p "$REMOTE_PORT" -U "$DB_USER" -d postgres -c "SELECT 1" > /dev/null
PGPASSWORD=monitor psql -v ON_ERROR_STOP=1 -h "$REMOTE_IP" -p "$REMOTE_PORT" -U monitor -d postgres -c "SELECT 1" > /dev/null

echo "PostgreSQL base image is ready."
''',
    'Docker/Ubuntu_MySQL/init_dbms_mysql.sh': r'''#!/bin/bash
set -Eeuo pipefail

REMOTE_IP="127.0.0.1"
REMOTE_PORT="3306"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"
PATH_VALUE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --remote_ip|-h) REMOTE_IP="$2"; shift 2 ;;
        --remote_port|-port) REMOTE_PORT="$2"; shift 2 ;;
        --user|-u) DB_USER="$2"; shift 2 ;;
        --password|-pwd) DB_PASSWORD="$2"; shift 2 ;;
        --path_value|-path) PATH_VALUE="$2"; shift 2 ;;
        --tmp_value|-tmp) shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$PATH_VALUE" ]]; then
    echo "Usage: $0 -path <PATH_VALUE>" >&2
    exit 1
fi

# docker_func.py invokes this entry point after docker run. The initialized
# datadir, static configuration, and fixed service accounts are in the image.
if ! MYSQL_PWD=root mysqladmin -h "$REMOTE_IP" -P "$REMOTE_PORT" -uroot ping --silent > /dev/null 2>&1; then
    rm -f /var/run/mysqld/mysqld.sock /var/run/mysqld/mysqld.sock.lock \
        /var/run/mysqld/mysqlx.sock /var/run/mysqld/mysqlx.sock.lock \
        /var/run/mysqld/mysqld.pid 2>/dev/null || true
    mysqld --defaults-file=/etc/mysql/mysql.conf.d/dbdiag.cnf \
        --user=mysql --datadir=/var/lib/mysql --daemonize
fi

deadline=$((SECONDS + 120))
until MYSQL_PWD=root mysqladmin -h "$REMOTE_IP" -P "$REMOTE_PORT" -uroot ping --silent > /dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
        echo "MySQL startup timed out after 120s" >&2
        exit 1
    fi
    sleep 1
done

MYSQL_PWD=root mysql -h "$REMOTE_IP" -P "$REMOTE_PORT" -uroot -e "SELECT 1" > /dev/null
MYSQL_PWD="$DB_PASSWORD" mysql -h "$REMOTE_IP" -P "$REMOTE_PORT" -u"$DB_USER" -e "SELECT 1" > /dev/null
MYSQL_PWD=monitor mysql -h "$REMOTE_IP" -P "$REMOTE_PORT" -umonitor -e "SELECT 1" > /dev/null

echo "MySQL base image is ready."
''',
    'Scripts/fast_reset_database_pg.sh': r'''#!/bin/sh
set -euo pipefail

# =============================================================================
# Fast baseline snapshot / reset for PostgreSQL (container-local golden clone).
#
# Instead of DROP + CREATE + re-importing the multi-GB dump on every LLM turn
# (what initialize_database_pg.sh does), this script:
#
#   --mode snapshot : clone the CURRENT ready-state database (already imported +
#                     data-modification applied) into a golden template database
#                     using `CREATE DATABASE <golden> TEMPLATE <db>` -- a fast,
#                     file-level copy performed once per case.
#   --mode reset    : restore the working database to that golden baseline via
#                     `DROP DATABASE <db>; CREATE DATABASE <db> TEMPLATE <golden>`
#                     (seconds, no text re-parse), then scrub session/role-level
#                     residue (ALTER ROLE ... RESET ALL, terminate leftover
#                     background holder sessions). This yields a bit-for-bit
#                     identical pre-injection baseline every turn.
#
# The golden database lives INSIDE the per-worker container, so parallel workers
# never share or contend on it. On any failure this script exits non-zero and the
# Python caller falls back to the original full re-initialization path.
# =============================================================================

DB_HOST="127.0.0.1"
DB_PORT="5432"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"
DB_SCHEMA="diagnosis"

REMOTE_HIGH_PRV_USER="root"
REMOTE_HIGH_PRV_PASSWORD="root"

DB_NAME=""
GOLDEN_NAME=""
MODE=""

usage() {
    echo "Usage: $0 --mode <snapshot|reset> --database_name <dbname> --golden_name <golden> [--host <host>] [--port <port>] [--database_schema <schema>] [--user <user>] [--password <pwd>]"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --mode)            MODE="$2"; shift 2 ;;
        --host)            DB_HOST="$2"; shift 2 ;;
        --port)            DB_PORT="$2"; shift 2 ;;
        --database_name)   DB_NAME="$2"; shift 2 ;;
        --golden_name)     GOLDEN_NAME="$2"; shift 2 ;;
        --database_schema) DB_SCHEMA="$2"; shift 2 ;;
        --user)            DB_USER="$2"; shift 2 ;;
        --password)        DB_PASSWORD="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

[ -z "$MODE" ] && { echo "Error: --mode is required (snapshot|reset)."; usage; }
[ "$MODE" != "snapshot" ] && [ "$MODE" != "reset" ] && { echo "Error: invalid --mode '$MODE'."; usage; }
[ -z "$DB_NAME" ] && { echo "Error: --database_name is required."; usage; }
[ -z "$GOLDEN_NAME" ] && { echo "Error: --golden_name is required."; usage; }

# Admin connection (superuser) targets the maintenance database `postgres` so it
# is never attached to $DB_NAME / $GOLDEN_NAME while we drop or clone them.
ADMIN_OPTS="-h $DB_HOST -p $DB_PORT -U $REMOTE_HIGH_PRV_USER -d postgres"

admin_psql() {
    PGPASSWORD="$REMOTE_HIGH_PRV_PASSWORD" psql $ADMIN_OPTS -X -q "$@"
}

# Terminate every backend attached to the given database (except ourselves) so a
# DROP/CREATE ... TEMPLATE never fails with "database is being accessed by other
# users". This also reaps any leftover background holder sessions started by the
# previous turn's anomaly injection (FOR UPDATE holders, connection floods, ...).
terminate_backends() {
    target_db="$1"
    admin_psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$target_db' AND pid <> pg_backend_pid();" > /dev/null 2>&1 || true
}

# Robustly drop a database even when a previous turn's injection left runaway
# backends that keep RE-CONNECTING (the race that intermittently produced
# "database is being accessed by other users" / "database does not exist"):
#   1. ALTER DATABASE ... ALLOW_CONNECTIONS false blocks ALL new connections
#      (owner included), so terminated backends cannot immediately reconnect.
#   2. Retry terminate + `DROP DATABASE ... WITH (FORCE)` (PostgreSQL 13+ kills
#      any stragglers atomically as part of the drop).
#   3. Fall back to a plain terminate + DROP for older servers lacking FORCE.
# Every step tolerates a non-existent target (guarded by `|| true` / IF EXISTS).
drop_database_forcefully() {
    target_db="$1"
    admin_psql -c "ALTER DATABASE $target_db WITH ALLOW_CONNECTIONS false;" > /dev/null 2>&1 || true

    attempt=0
    while [ $attempt -lt 5 ]; do
        terminate_backends "$target_db"
        if admin_psql -c "DROP DATABASE IF EXISTS $target_db WITH (FORCE);" > /dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    # Last resort for servers without WITH (FORCE): terminate then plain DROP.
    terminate_backends "$target_db"
    admin_psql -c "DROP DATABASE IF EXISTS $target_db;" > /dev/null
}

if ! admin_psql -c "SELECT 1;" > /dev/null 2>&1; then
    echo "Error: cannot connect to PostgreSQL at $DB_HOST:$DB_PORT as $REMOTE_HIGH_PRV_USER."
    exit 1
fi

if [ "$MODE" = "snapshot" ]; then
    echo "-- [fast-reset] Creating golden baseline '$GOLDEN_NAME' from '$DB_NAME' ..."
    # Drop any stale golden robustly, then clone from the source. CREATE ...
    # TEMPLATE requires NO active sessions on the source db, so briefly block new
    # connections + terminate stragglers on the source during the copy, then
    # re-open it. (Snapshot runs before any injection, so the source is normally
    # already idle; this only hardens against leftover holders.)
    drop_database_forcefully "$GOLDEN_NAME"
    echo "-- [fast-reset] Blocking new source connections and terminating existing backends ..."
    admin_psql -c "ALTER DATABASE $DB_NAME WITH ALLOW_CONNECTIONS false;" > /dev/null 2>&1 || true
    terminate_backends "$DB_NAME"
    create_status=0
    echo "-- [fast-reset] Cloning PostgreSQL template '$DB_NAME' into '$GOLDEN_NAME' ..."
    admin_psql -c "CREATE DATABASE $GOLDEN_NAME TEMPLATE $DB_NAME OWNER $DB_USER;" > /dev/null || create_status=$?
    # Always re-open the source db regardless of the clone outcome.
    admin_psql -c "ALTER DATABASE $DB_NAME WITH ALLOW_CONNECTIONS true;" > /dev/null 2>&1 || true
    if [ "$create_status" -ne 0 ]; then
        echo "Error: failed to create golden baseline '$GOLDEN_NAME' from '$DB_NAME'."
        exit 1
    fi
    echo "✅ [fast-reset] Golden baseline '$GOLDEN_NAME' created."
    exit 0
fi

# MODE = reset
echo "-- [fast-reset] Restoring '$DB_NAME' from golden baseline '$GOLDEN_NAME' ..."

# Verify the golden template exists; if it does not, fail so the caller falls
# back to the full re-initialization path rather than silently doing nothing.
GOLDEN_EXISTS=$(admin_psql -tAc "SELECT 1 FROM pg_database WHERE datname='$GOLDEN_NAME';")
if [ "$GOLDEN_EXISTS" != "1" ]; then
    echo "Error: golden baseline '$GOLDEN_NAME' does not exist; cannot fast-reset."
    exit 1
fi

# 1) Scrub role-level GUC residue (e.g. ALTER ROLE ... SET enable_indexscan/
#    work_mem from a previous injection) BEFORE recreating the db. Role settings
#    live in pg_db_role_setting keyed by role and survive a database drop.
admin_psql -c "ALTER ROLE $DB_USER RESET ALL;" > /dev/null 2>&1 || true

# 2) Drop the working db robustly (blocks reconnects + WITH (FORCE) + retries so
#    runaway injection backends that keep re-connecting cannot wedge the DROP),
#    then re-clone it from the golden template -- a fast file-level copy.
drop_database_forcefully "$DB_NAME"
admin_psql -c "CREATE DATABASE $DB_NAME TEMPLATE $GOLDEN_NAME OWNER $DB_USER;" > /dev/null
# The freshly created db inherits datallowconn=true from the template, but be
# explicit in case the golden itself was ever marked no-connect.
admin_psql -c "ALTER DATABASE $DB_NAME WITH ALLOW_CONNECTIONS true;" > /dev/null 2>&1 || true

# 3) Re-apply database-level settings that TEMPLATE cloning does not carry over
#    (pg_db_role_setting entries are keyed by datid, so they are not copied).
admin_psql -c "ALTER DATABASE $DB_NAME SET search_path TO $DB_SCHEMA, public;" > /dev/null 2>&1 || true

echo "✅ [fast-reset] Database '$DB_NAME' restored to golden baseline."
exit 0
''',
    'Scripts/fast_reset_database_mysql.sh': r'''#!/bin/bash
set -euo pipefail

# =============================================================================
# Fast baseline snapshot / reset for MySQL (container-local golden clone).
#
# MySQL has no `CREATE DATABASE ... TEMPLATE`, so the fast analog of PostgreSQL's
# template clone is an ENGINE-INTERNAL table copy: `CREATE TABLE LIKE` +
# `INSERT ... SELECT`. Data stays inside InnoDB and never round-trips through the
# client text parser, so this is dramatically cheaper than re-running LOAD DATA /
# re-sourcing the dump (what initialize_database_mysql.sh does) on every turn.
#
#   --mode snapshot : clone the CURRENT ready-state schema (imported +
#                     data-modification applied) into a golden schema, once.
#   --mode reset    : DROP the working schema, recreate it, and clone every table
#                     back from the golden schema; then scrub GLOBAL/session
#                     residue and kill leftover background holder sessions so the
#                     next turn starts from an identical pre-injection baseline.
#
# The golden schema lives INSIDE the per-worker container -> no cross-worker
# contention. Views are unsupported by this fast path; if any exist the script
# exits non-zero so the Python caller falls back to full re-initialization.
# =============================================================================

DB_HOST="127.0.0.1"
DB_PORT="3306"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"

REMOTE_HIGH_PRV_USER="root"
REMOTE_HIGH_PRV_PASSWORD="root"

DB_NAME=""
GOLDEN_NAME=""
MODE=""

usage() {
    echo "Usage: $0 --mode <snapshot|reset> --database_name <dbname> --golden_name <golden> [--host <host>] [--port <port>] [--user <user>] [--password <pwd>]"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --mode)          MODE="$2"; shift 2 ;;
        --host)          DB_HOST="$2"; shift 2 ;;
        --port)          DB_PORT="$2"; shift 2 ;;
        --database_name) DB_NAME="$2"; shift 2 ;;
        --golden_name)   GOLDEN_NAME="$2"; shift 2 ;;
        --database_schema) shift 2 ;;   # accepted for arg parity; ignored for MySQL
        --user)          DB_USER="$2"; shift 2 ;;
        --password)      DB_PASSWORD="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

[ -z "$MODE" ] && { echo "Error: --mode is required (snapshot|reset)."; usage; }
[ "$MODE" != "snapshot" ] && [ "$MODE" != "reset" ] && { echo "Error: invalid --mode '$MODE'."; usage; }
[ -z "$DB_NAME" ] && { echo "Error: --database_name is required."; usage; }
[ -z "$GOLDEN_NAME" ] && { echo "Error: --golden_name is required."; usage; }

MYSQL_CLIENT_HOST="$DB_HOST"
if [ "$MYSQL_CLIENT_HOST" = "localhost" ]; then
    MYSQL_CLIENT_HOST="127.0.0.1"
fi
ADMIN_OPTS="-h $MYSQL_CLIENT_HOST -P $DB_PORT -u $REMOTE_HIGH_PRV_USER"

admin_mysql() {
    MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysql $ADMIN_OPTS "$@"
}

# Backtick-quote an identifier (defensive against schema/table names with quirks).
qident() {
    printf '`%s`' "${1//\`/\`\`}"
}

if ! admin_mysql -e "SELECT 1;" > /dev/null 2>&1; then
    echo "Error: cannot connect to MySQL at $MYSQL_CLIENT_HOST:$DB_PORT as $REMOTE_HIGH_PRV_USER."
    exit 1
fi

# Reap leftover, non-system sessions on a schema (background holders / floods).
# Looped a few times because an injection flood may keep RE-CONNECTING: a single
# pass can miss sessions that reconnect right after being killed. Also kills
# sessions opened by the working user that are not currently bound to the schema
# (e.g. connected with no default DB), which a plain `DB = '$target_db'` filter
# would otherwise leave holding connection slots.
kill_schema_sessions() {
    target_db="$1"
    pass=0
    while [ $pass -lt 5 ]; do
        kill_stmts=$(admin_mysql -N -B -e "
            SELECT GROUP_CONCAT(CONCAT('KILL ', id, ';') SEPARATOR ' ')
            FROM information_schema.PROCESSLIST
            WHERE (DB = '$target_db' OR (USER = '$DB_USER' AND COMMAND <> 'Sleep'))
              AND ID <> CONNECTION_ID();" 2>/dev/null)
        if [ -z "$kill_stmts" ] || [ "$kill_stmts" = "NULL" ]; then
            return 0
        fi
        admin_mysql -e "$kill_stmts" > /dev/null 2>&1 || true
        pass=$((pass + 1))
        sleep 1
    done
}

# DROP a schema robustly, reaping reconnecting holder sessions between attempts
# so a leftover injection flood cannot keep the schema busy indefinitely.
drop_schema_forcefully() {
    target_db="$1"
    attempt=0
    while [ $attempt -lt 5 ]; do
        kill_schema_sessions "$target_db"
        if admin_mysql -e "DROP DATABASE IF EXISTS $(qident "$target_db");" > /dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    # Final attempt surfaces the real error (and aborts under set -e) so the
    # Python caller falls back to a full re-initialization.
    admin_mysql -e "DROP DATABASE IF EXISTS $(qident "$target_db");"
}

# Abort the fast path if the source schema contains views (CREATE TABLE LIKE
# cannot reproduce them); the caller then does a full re-init instead.
assert_no_views() {
    src="$1"
    view_count=$(admin_mysql -N -B -e "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '$src';")
    if [ "${view_count:-0}" -ne 0 ]; then
        echo "Error: schema '$src' contains $view_count view(s); fast path unsupported."
        exit 1
    fi
}

# Clone every base table from $1 (source schema) into $2 (destination schema),
# using engine-internal copies. Destination schema must already exist and be
# empty. Each table is committed independently and emits progress so the outer
# synthesis watchdog can distinguish a large, active copy from a stalled step.
# FOREIGN_KEY_CHECKS is disabled so parent/child insertion order is free.
clone_tables() {
    src="$1"
    dst="$2"

    mapfile -t tables < <(admin_mysql -N -B -e "
        SELECT TABLE_NAME FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = '$src' AND TABLE_TYPE = 'BASE TABLE';")

    total="${#tables[@]}"
    if [ "$total" -eq 0 ]; then
        echo "Error: source schema '$src' has no base tables to clone." >&2
        return 1
    fi

    index=0
    for tbl in "${tables[@]}"; do
        [ -z "$tbl" ] && continue
        index=$((index + 1))
        sq=$(qident "$src")
        dq=$(qident "$dst")
        tq=$(qident "$tbl")
        echo "-- [fast-reset] Cloning table ${index}/${total} '${tbl}' from '${src}' to '${dst}' ..."
        admin_mysql <<SQL
SET SESSION FOREIGN_KEY_CHECKS=0;
SET SESSION UNIQUE_CHECKS=0;
CREATE TABLE ${dq}.${tq} LIKE ${sq}.${tq};
INSERT INTO ${dq}.${tq} SELECT * FROM ${sq}.${tq};
SQL
        echo "-- [fast-reset] Completed table ${index}/${total} '${tbl}'."
    done
}

if [ "$MODE" = "snapshot" ]; then
    echo "-- [fast-reset] Creating golden baseline '$GOLDEN_NAME' from '$DB_NAME' ..."
    assert_no_views "$DB_NAME"
    drop_schema_forcefully "$GOLDEN_NAME"
    admin_mysql -e "CREATE DATABASE $(qident "$GOLDEN_NAME");"
    clone_tables "$DB_NAME" "$GOLDEN_NAME"
    echo "✅ [fast-reset] Golden baseline '$GOLDEN_NAME' created."
    exit 0
fi

# MODE = reset
echo "-- [fast-reset] Restoring '$DB_NAME' from golden baseline '$GOLDEN_NAME' ..."

GOLDEN_EXISTS=$(admin_mysql -N -B -e "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$GOLDEN_NAME';")
if [ "${GOLDEN_EXISTS:-0}" -eq 0 ]; then
    echo "Error: golden baseline '$GOLDEN_NAME' does not exist; cannot fast-reset."
    exit 1
fi

# 1) Reap leftover holder sessions on the working schema (looped + retried DROP so
#    a reconnecting injection flood cannot wedge it), then recreate it.
drop_schema_forcefully "$DB_NAME"
admin_mysql -e "CREATE DATABASE $(qident "$DB_NAME");"
# Restore the working user's privileges on the freshly recreated schema.
admin_mysql -e "GRANT ALL PRIVILEGES ON $(qident "$DB_NAME").* TO '$DB_USER'@'%';" 2>/dev/null || true
admin_mysql -e "FLUSH PRIVILEGES;" 2>/dev/null || true

# 2) Clone all tables back from the golden baseline (engine-internal copy).
clone_tables "$GOLDEN_NAME" "$DB_NAME"

echo "✅ [fast-reset] Database '$DB_NAME' restored to golden baseline."
exit 0
''',
}

_ASSET_ROOT = Path(_docker_TMP_ROOT) / "filter_runtime_0a161ce576c16b44"
_FILTER_OPTIONS = {}


def _prepare_runtime_assets():
    """Publish complete assets atomically into the container-visible staging mount."""
    for relative, content in _EMBEDDED_ASSETS.items():
        path = _ASSET_ROOT / relative
        data = content.encode()
        if path.is_file() and path.read_bytes() == data:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".publish_")
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(data)
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    return _ASSET_ROOT


def _embedded_evaluator(sample_config, sample_files):
    variant = _FILTER_OPTIONS.get("evaluator_variant", "auto")
    if variant == "auto":
        original = sample_config.get("anomaly_eval_path") or sample_files.get("anomaly_eval.py")
        if original and Path(original).is_file():
            variant = "reproduction" if "--success-criterion" in Path(original).read_text(errors="replace") else "synthetic"
        else:
            variant = "reproduction"
    name = "anomaly_eval_rep.py" if variant == "reproduction" else "anomaly_eval.py"
    return str(_ASSET_ROOT / "Scripts" / name)


def _validate_standalone_config(config, input_directory):
    for field in ("parallelism", "median_runs", "workload_validation_rounds",
                  "workload_validation_timeout_seconds", "docker_init_timeout_seconds",
                  "database_init_timeout_seconds", "database_init_parallelism"):
        value = config.get(field, 1)
        if type(value) is not int or value < 1:
            raise ValueError(f"{field} must be a positive integer")
    if config.get("workload_validation_rounds", 3) < 2:
        raise ValueError("workload_validation_rounds must be at least 2")
    docker_port = config.get("docker_port", _legacy_DEFAULT_DOCKER_PORT)
    if type(docker_port) is not int or not 1 <= docker_port <= 65535:
        raise ValueError("docker_port must be an integer between 1 and 65535")
    last_port = docker_port + (config.get("parallelism", 1) - 1) * _legacy_WORKER_PORT_STRIDE
    if last_port > 65535:
        raise ValueError("The worker host-port range exceeds 65535")
    docker_name = config.get("docker_name", _legacy_DEFAULT_DOCKER_NAME)
    if not isinstance(docker_name, str) or not _util_re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", docker_name):
        raise ValueError("docker_name must be a nonempty Docker-compatible name")
    start_id = config.get("start_id", _legacy_START_ID)
    end_id = config.get("end_id", _legacy_END_ID)
    if type(start_id) is not int or type(end_id) is not int or not 0 <= start_id < end_id:
        raise ValueError("start_id/end_id must be integers with 0 <= start_id < end_id")
    if config.get("evaluator_variant", "auto") not in ("auto", "synthetic", "reproduction"):
        raise ValueError("evaluator_variant must be auto, synthetic or reproduction")
    if config.get("success_criterion", "AUTO") not in _legacy_VALID_SUCCESS_CRITERIA:
        raise ValueError("Invalid success_criterion")
    if "anomaly_type" in config and config["anomaly_type"] not in _legacy_VALID_ANOMALY_TYPES:
        raise ValueError("anomaly_type must be OLAP, OLTP or BOTH")
    for field in ("success_threshold_percent", "min_abs_delta_seconds"):
        value = config.get(field, 0)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{field} must be a number")
        if not 0 <= value < float("inf"):
            raise ValueError(f"{field} must be finite and nonnegative")
    key_info = load_json(os.path.join(input_directory, "key_information.json"), None)
    if not isinstance(key_info, list) or not key_info:
        raise ValueError("key_information.json must be a nonempty list")
    all_ids = [_legacy_parse_sample_id(case, index) for index, case in enumerate(key_info)]
    if len(all_ids) != len(set(all_ids)):
        raise ValueError("Duplicate sample IDs")
    _FILTER_OPTIONS.clear()
    _FILTER_OPTIONS.update(config)
    samples, _ = _legacy_build_target_samples(key_info, logging.getLogger(__name__))
    if not samples:
        raise ValueError("No samples selected; check start_id/end_id")
    resolve_sample_context.sample_base_directory = input_directory
    ids = [s[1] for s in samples]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate sample IDs")
    for sample in samples:
        resolve_sample_context(sample, str(PROJECT_ROOT))
    return len(samples)


# Public reusable observations for the Agentic filtering entry.
kill_tmux_sessions_by_prefix = _docker_kill_tmux_sessions_by_prefix
remove_containers_by_prefix = _docker_remove_containers_by_prefix
workload_log_observation = _legacy_workload_log_observation


if __name__ == "__main__":
    if _health_sys.argv[1:] == ["--worker"]:
        _health_worker_main()
    else:
        main()
