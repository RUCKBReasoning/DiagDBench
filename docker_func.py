import collections
import functools
import threading
from utils import split_executable_sql
import os
import subprocess
import shlex
import time
import re
# NOTE: `from utils import *` below re-exports utils's `from datetime import
# datetime` (utils has no __all__), which would REBIND a bare `import datetime`
# name to the datetime CLASS and break `datetime.datetime.now()`. Import the
# module under an alias that the star-import cannot shadow.
import datetime as datetime_module
import logging
import statistics

from utils import *

# Optional process-local admission control for foreground database I/O.
# DB_DIAG_IO_MAX_CONCURRENCY=0 disables it. Queueing precedes each phase
# timeout; nested calls share the same admission without deadlocking.
_LOG = logging.getLogger('DB_DIAG.docker_func.io_admission')


class IOAdmission:
    def __init__(self, limit):
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError('I/O concurrency must be a nonnegative integer')
        self.limit = limit
        self._condition = threading.Condition()
        self._local = threading.local()
        self._queue = collections.deque()
        self._active = 0
        self._sequence = 0
        self._peak = 0

    def wrap(self, function):
        @functools.wraps(function)
        def admitted(*args, **kwargs):
            if self.limit == 0 or getattr(self._local, 'depth', 0):
                return function(*args, **kwargs)
            started = time.monotonic()
            with self._condition:
                self._sequence += 1
                ticket = self._sequence
                self._queue.append(ticket)
                if self._active >= self.limit or self._queue[0] != ticket:
                    _LOG.info('[RESOURCE][IO] waiting ticket=%s phase=%s active=%s limit=%s',
                              ticket, function.__name__, self._active, self.limit)
                try:
                    while self._queue[0] != ticket or self._active >= self.limit:
                        self._condition.wait()
                except BaseException:
                    self._queue.remove(ticket)
                    self._condition.notify_all()
                    raise
                self._queue.popleft()
                self._active += 1
                self._peak = max(self._peak, self._active)
                self._local.depth = 1
                active = self._active
                self._condition.notify_all()
            _LOG.info('[RESOURCE][IO] acquired ticket=%s phase=%s wait_seconds=%.3f active=%s limit=%s',
                      ticket, function.__name__, time.monotonic() - started, active, self.limit)
            try:
                return function(*args, **kwargs)
            finally:
                with self._condition:
                    self._local.depth = 0
                    self._active -= 1
                    active = self._active
                    self._condition.notify_all()
                _LOG.info('[RESOURCE][IO] released ticket=%s phase=%s active=%s limit=%s',
                          ticket, function.__name__, active, self.limit)
        return admitted


def configured_limit():
    raw = os.environ.get('DB_DIAG_IO_MAX_CONCURRENCY', '0')
    try:
        result = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError('DB_DIAG_IO_MAX_CONCURRENCY must be a nonnegative integer') from exc
    if result < 0:
        raise ValueError('DB_DIAG_IO_MAX_CONCURRENCY must be a nonnegative integer')
    return result


ADMISSION = IOAdmission(configured_limit())
io_heavy_phase = ADMISSION.wrap

# Retain the existing local decorator name for database initialization.
_dbdiag_io_heavy_phase = io_heavy_phase


try:
    from psycopg2 import errors
except ImportError:  # pragma: no cover - optional dependency at orchestration time
    class _Psycopg2ErrorsFallback:
        class QueryCanceled(Exception):
            pass

    errors = _Psycopg2ErrorsFallback()

logger = logging.getLogger(f"DB_DIAG.{__name__}")

# Use the canonical on-disk path, not a symlinked alias such as
# ``/home/... -> /data00/home/...``. Docker bind-mounts against the canonical
# host path, and inside the container the mounted destination is that canonical
# path as well. If later commands are constructed with the symlink alias
# (``/home/...``), `docker exec` inside the container cannot find the mounted
# files and database initialization fails with ``No such file or directory``.
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
# Keep the default project-local temporary directory, while allowing a host
# with a separate Docker disk to place container-visible staging files there.
TMP_ROOT = os.path.realpath(
    os.environ.get("DB_DIAG_TMP_ROOT", os.path.join(os.path.dirname(BASE_DIR), "TMP"))
)

# Static hang-detection thresholds. These bound the three operations that can
# otherwise block forever (Docker container init, recording merged-workload
# timing, collecting the user observation), turning a silent hang into a
# bounded failure that the shell driver's per-step retry re-runs from the
# failed checkpoint. The execution ceilings are 2x the prior 600s default;
# the connection-establishment timeout is left at its own small value.
DOCKER_INIT_TIMEOUT = 1200
SQL_EXEC_TIMEOUT = 1200
DB_CONNECT_TIMEOUT = 30
# Per-invocation ceiling for a single ``docker rm -f``. Kept separate from
# ``DB_CONNECT_TIMEOUT`` (which only bounds DB connects) so a slow daemon-side
# teardown -- e.g. many concurrent syntheses tearing down quota-backed
# (``--storage-opt size=..``) containers at once -- has enough head-room to
# finish instead of being killed at 30s and retried forever.
DOCKER_RM_CMD_TIMEOUT = 300
# Overall ceiling for confirming a container is fully removed. High-concurrency
# teardown of large quota-backed containers can take minutes, so this bounds the
# synchronous wait generously; it only blocks this long when a removal genuinely
# never completes.
DOCKER_REMOVE_TIMEOUT = 1800
DOCKER_POLL_INTERVAL = 2
# Number of CONSECUTIVE confirmed-absent probes required before a container is
# treated as fully removed. Guards against a name that briefly disappears mid
# transition being mistaken for a completed removal.
DOCKER_ABSENT_STABLE_CHECKS = 3
# ``docker ps`` bounds / retries. Under heavy parallel load ``docker ps`` itself
# can be slow or transiently fail; retrying (instead of treating one failed
# probe as "container gone") is what prevents a stale container from silently
# keeping its name and causing the next ``docker run`` to hit a 125 conflict.
DOCKER_PS_TIMEOUT = 60
DOCKER_PS_RETRIES = 3
DOCKER_PS_RETRY_INTERVAL = 2
# Length of one confirmation slice inside ``force_remove_container``. If the name
# is not confirmed released within a slice, ``docker rm -f`` is re-issued (covers
# the case where a prior client-side timeout killed the CLI before the daemon
# acted) until the overall ``DOCKER_REMOVE_TIMEOUT`` is reached.
DOCKER_RM_CONFIRM_SLICE = 60
# Overall ceiling (seconds) for waiting until a target container NAME is free
# before issuing ``docker run``. Even with timestamped unique names a just-torn-
# down name can linger briefly in the daemon's registry; we poll until it is
# confirmed absent, and only give up (reporting failure) after this budget. Set
# per the requirement to 3600s.
DOCKER_NAME_FREE_TIMEOUT = 3600
# Poll interval (seconds) while waiting for a container name to become free.
DOCKER_NAME_FREE_POLL_INTERVAL = 10
# Golden-baseline snapshots may need to copy multi-gigabyte database objects
# under concurrent synthesis load. Keep their budget separate from ordinary
# initialization and execution timeouts. Operators can lower or raise this
# independently for a constrained environment.
try:
    FAST_RESET_SNAPSHOT_TIMEOUT_SECONDS = max(
        60,
        int(os.environ.get("FAST_RESET_SNAPSHOT_TIMEOUT_SECONDS", "21600")),
    )
except ValueError:
    FAST_RESET_SNAPSHOT_TIMEOUT_SECONDS = 21600
FAST_RESET_HEARTBEAT_INTERVAL_SECONDS = 600

create_container_str = f'docker run --cpus={{}} -m {{}}g --memory-swap {{}}g --storage-opt size={{}}G -p {{}}:5432 -v {BASE_DIR}:{BASE_DIR} -v {TMP_ROOT}:{TMP_ROOT} --name {{}} -d pg_diag'
init_container_str = f'docker exec {{}} bash {BASE_DIR}/Docker/Ubuntu_PG/init_dbms_postgresql.sh -h {{}} -port {{}} -path {BASE_DIR} -tmp {TMP_ROOT}'
mysql_create_container_str = f'docker run --cpus={{}} -m {{}}g --memory-swap {{}}g --storage-opt size={{}}G -p {{}}:3306 -v {BASE_DIR}:{BASE_DIR} -v {TMP_ROOT}:{TMP_ROOT} --name {{}} -d mysql_diag'
mysql_init_container_str = f'docker exec {{}} bash {BASE_DIR}/Docker/Ubuntu_MySQL/init_dbms_mysql.sh -h {{}} -port {{}} -path {BASE_DIR} -tmp {TMP_ROOT}'


def _docker_resource_value(value):
    value = str(value).strip()
    for suffix in ("GB", "gb", "G", "g"):
        if value.endswith(suffix):
            return value[:-len(suffix)]
    return value


def build_container_config(dbms, system_hardware_config, host_port, system_user_config):
    return {
        "dbms": dbms,
        "cpus": _docker_resource_value(system_hardware_config["CPU"]),
        "memory": _docker_resource_value(system_hardware_config["Memory"]),
        "memory_swap": _docker_resource_value(system_hardware_config["Memory + Swap"]),
        "storage": _docker_resource_value(system_hardware_config["Storage"]),
        "host_port": host_port,
        "db_host": system_user_config["DB_HOST"],
        "db_port": system_user_config["DB_PORT"],
    }


def _container_config_values(container_config):
    if isinstance(container_config, dict):
        return (
            normalize_dbms(container_config.get("dbms")),
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


def build_sql_file_command(dbms, system_user_config, sql_file_path, database_name=None):
    dbms_kind = normalize_dbms(dbms)
    db_name = database_name or system_user_config["DB_NAME"]
    if dbms_kind == "mysql":
        inner_command = (
            f"MYSQL_PWD={shlex.quote(str(system_user_config['DB_PASSWORD']))} "
            f"mysql -h {shlex.quote(str(system_user_config['DB_HOST']))} "
            f"-P {shlex.quote(str(system_user_config['DB_PORT']))} "
            f"-u {shlex.quote(str(system_user_config['DB_USER']))} "
            f"{shlex.quote(str(db_name))} < {shlex.quote(str(sql_file_path))}"
        )
        return ["bash", "-lc", inner_command]

    inner_command = (
        f"PGPASSWORD={shlex.quote(str(system_user_config['DB_PASSWORD']))} "
        f"psql -h {shlex.quote(str(system_user_config['DB_HOST']))} "
        f"-p {shlex.quote(str(system_user_config['DB_PORT']))} "
        f"-U {shlex.quote(str(system_user_config['DB_USER']))} "
        f"-d {shlex.quote(str(db_name))} "
        f"-f {shlex.quote(str(sql_file_path))}"
    )
    return ["bash", "-lc", inner_command]

def initialize_shared_tmp_directory(base_dir, dbms):
    tmp_root = TMP_ROOT
    last_error = None
    tmp_image = "mysql_diag" if normalize_dbms(dbms) == "mysql" else "pg_diag"

    try:
        tmp_dbms_dir = os.path.join(tmp_root, dbms)
        tmp_timestamp_dir = os.path.join(tmp_dbms_dir, time.strftime("%Y%m%d_%H%M%S"))
        docker_command = [
            "docker", "run", "--rm",
            "-v", f"{tmp_root}:{tmp_root}",
            tmp_image,
            "bash", "-lc",
            f"mkdir -p {shlex.quote(tmp_dbms_dir)} {shlex.quote(tmp_timestamp_dir)} && chmod 1777 {shlex.quote(tmp_root)} && chmod 777 {shlex.quote(tmp_dbms_dir)} {shlex.quote(tmp_timestamp_dir)}"
        ]

        try:
            os.makedirs(tmp_root, exist_ok=True)
        except PermissionError:
            logger.warning(f"[DOCKER FUNC][WARNING] PermissionError when creating TMP root directory: {tmp_root}. Attempting to prepare permissions via Docker command.")
            subprocess.run(docker_command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
       
        try:
            os.makedirs(tmp_dbms_dir, exist_ok=True)
        except PermissionError:
            subprocess.run(docker_command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        try:
            os.makedirs(tmp_timestamp_dir, exist_ok=True)
        except PermissionError:
            subprocess.run(docker_command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if not os.access(tmp_dbms_dir, os.W_OK):
            subprocess.run(docker_command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not os.access(tmp_timestamp_dir, os.W_OK):
            subprocess.run(docker_command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not os.access(tmp_dbms_dir, os.W_OK):
            logger.error(f"[DOCKER FUNC][ERROR] Shared TMP directory is not writable after permission preparation: {tmp_dbms_dir}")
            exit()
        if not os.access(tmp_timestamp_dir, os.W_OK):
            logger.error(f"[DOCKER FUNC][ERROR] Shared TMP timestamp directory is not writable after permission preparation: {tmp_timestamp_dir}")
            exit()

        return tmp_timestamp_dir
    except (PermissionError, OSError, subprocess.CalledProcessError) as e:
        last_error = e

    logger.error(f"[DOCKER FUNC][ERROR] Failed to prepare shared TMP directory for DBMS {dbms}. Last error: {last_error}")
    exit()

def wait_for_log_file_done(path, timeout=10, heartbeat_message=None): # "__DONE__" in lines
    start = time.time()
    last_heartbeat = start
    while time.time() - start < timeout:
        if os.path.exists(path):
            with open(path, 'r') as file :
                lines = file.readlines()
                lines = [line.strip() for line in lines]
            if "__DONE__" in lines : return True
        if heartbeat_message and time.time() - last_heartbeat >= FAST_RESET_HEARTBEAT_INTERVAL_SECONDS:
            logger.info(heartbeat_message)
            last_heartbeat = time.time()
        time.sleep(0.1)
    return False

def wait_for_log_file_signal(path, timeout, ready_pattern=None):
    """Wait for command completion or an optional explicit readiness signal."""
    compiled_ready = re.compile(ready_pattern) if ready_pattern else None
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="replace") as file:
                content = file.read()
            if "__DONE__" in content.splitlines():
                return "completed"
            if compiled_ready and compiled_ready.search(content):
                return "ready"
        time.sleep(0.1)
    return "timeout"

EXIT_CODE_SUFFIX = ".exit"

def build_exit_code_capture(inner_command_str, log_path):
    """Build a shell snippet that runs `inner_command_str`, tees its combined
    output to `log_path`, and records the command's true exit code.

    The exit code is taken from `${PIPESTATUS[0]}` so that the status reflects
    the actual command rather than the `tee` at the end of the pipe. The exit
    code is written to a SEPARATE sentinel file (`log_path + EXIT_CODE_SUFFIX`),
    keeping the original log content untouched for callers that read it back.
    The `__DONE__` marker is still appended to `log_path` so that the existing
    `wait_for_log_file_done` completion detection keeps working unchanged.
    """
    exit_code_path = log_path + EXIT_CODE_SUFFIX
    # Wrap the whole pipeline in a host-side bash so PIPESTATUS is available and
    # is evaluated immediately after the docker command finishes.
    snippet = (
        f"bash -c {shlex.quote(inner_command_str + f' 2>&1 | tee {shlex.quote(log_path)}; echo ${{PIPESTATUS[0]}} > {shlex.quote(exit_code_path)}')}"
        f"; echo \"__DONE__\" >> {shlex.quote(log_path)}"
    )
    return snippet, exit_code_path

def read_captured_exit_code(exit_code_path, timeout=10):
    """Read the in-container command's exit code from the sentinel file.

    Returns the integer exit code on success, or None when the file is missing,
    empty or unparsable within `timeout` seconds (which the caller should treat
    as a failure / indeterminate result).
    """
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(exit_code_path):
            try:
                with open(exit_code_path, 'r') as file:
                    content = file.read().strip()
                if content != "":
                    return int(content)
            except (ValueError, OSError):
                # File present but not yet fully written / not an integer.
                pass
        time.sleep(0.1)
    return None

def reset_captured_log(log_path):
    """Remove a previous run's log and exit-code sentinel before a new dispatch.

    The exec helpers reuse the same `log_path` (and its `.exit` sentinel) across
    multi-turn retries. Because the in-container command is launched
    asynchronously via `tmux send-keys`, the poller could otherwise observe the
    PREVIOUS run's `__DONE__` marker and stale exit code instantly, return before
    the new command finishes, and let the caller tear the container down while an
    import is still in flight (the SIGKILL -> exit code 137 false report).
    Clearing both files forces the poller to wait for the genuine new completion.
    """
    for path in (log_path, log_path + EXIT_CODE_SUFFIX):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning(f"[DOCKER FUNC][WARNING] Could not reset stale log file '{path}': {' '.join(str(e).split())}")

def tmux_has_session(session_name):
    result = subprocess.run(
        ["tmux", "has-session", "-t", session_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    return result.returncode == 0

def tmux_has_window(session_name, window_name):
    result = subprocess.run(
        ["tmux", "list-windows", "-t", session_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
    )
    if result.returncode != 0:
        return False

    for line in result.stdout.splitlines():
        if line.split(":")[1].strip().startswith(window_name):
            return True
    return False

def tmux_window_running(session_name, window_name):
    try:
        result = subprocess.check_output(
            ["tmux", "list-panes", "-t", f"{session_name}:{window_name}", "-F", "#{pane_current_command}"],
            stderr=subprocess.DEVNULL
        ).decode().strip()

        # 常见 shell
        shells = {"bash", "zsh", "sh", "fish"}

        return result not in shells
    except subprocess.CalledProcessError:
        return False
    
def interrupt_tmux_window(session_name, window_name):
    logger.info("[DOCKER FUNC][INTERRUPT THE CURRENT TMUX WINDOW]")

    if not tmux_has_session(session_name):
        subprocess.run(
            ["tmux", "new-session", "-d", "-s", session_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    if not tmux_has_window(session_name, window_name):
        subprocess.run(
            ["tmux", "new-window", "-d", "-t", session_name, "-n", window_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    
    subprocess.run(
        [
            "tmux", "send-keys",
            "-t", f"{session_name}:{window_name}",
            "C-c",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    subprocess.run([
        "tmux","send-keys",
        "-t",f"{session_name}:{window_name}",
        "q"
    ])

    subprocess.run([
        "tmux","send-keys",
        "-t",f"{session_name}:{window_name}",
        "Enter"
    ])

    subprocess.run([
        "tmux","send-keys",
        "-t",f"{session_name}:{window_name}",
        "C-c"
    ])

    logger.info("[DOCKER FUNC][INTERRUPT SENT TO THE TMUX WINDOW]")

def reset_tmux_window(session_name, window_name):
    logger.info("[DOCKER FUNC][RESET THE CURRENT TMUX WINDOW]")

    if not tmux_has_session(session_name):
        subprocess.run(
            ["tmux", "new-session", "-d", "-s", session_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    if not tmux_has_window(session_name, window_name):
        subprocess.run(
            ["tmux", "new-window", "-d", "-t", session_name, "-n", window_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    
    subprocess.run(
        ["tmux","kill-window","-t",f"{session_name}:{window_name}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    subprocess.run(
        ["tmux","new-window","-t",session_name,"-n",window_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

def kill_tmux_session(session_name):
    """Kill the whole tmux session ``session_name`` if it exists.

    Because ``session_name`` tracks the (now timestamped, unique) container name,
    this must be called whenever the container name changes (e.g. on a per-turn
    recreation) so the OLD session — which carried the previous container's
    ``docker exec`` workload — does not linger and accumulate across turns.
    """
    if not tmux_has_session(session_name):
        return
    subprocess.run(
        ["tmux", "kill-session", "-t", session_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

def kill_tmux_sessions_by_prefix(base_name):
    """Kill every tmux session whose name equals ``base_name`` or starts with
    ``base_name + '_'`` (our timestamped-name convention).

    Symmetric to ``remove_containers_by_prefix``: used for end-of-sample / startup
    cleanup so lingering per-turn sessions of THIS worker are reaped without
    touching other parallel workers' sessions (their numeric prefixes differ).
    """
    result = subprocess.run(
        ["tmux", "list-sessions", "-F", "#{session_name}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
    )
    if result.returncode != 0 or not result.stdout:
        return
    prefix = f"{base_name}_"
    for name in result.stdout.splitlines():
        name = name.strip()
        if name == base_name or name.startswith(prefix):
            kill_tmux_session(name)

def _docker_list_container_names(all_containers=False):
    cmd = ["docker", "ps", "--format", "{{.Names}}"]
    if all_containers:
        cmd.insert(2, "-a")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        timeout=DOCKER_PS_TIMEOUT,
    )
    return result.stdout.strip().splitlines()

def _probe_container_presence(container_name, all_containers=True):
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
    for attempt in range(1, DOCKER_PS_RETRIES + 1):
        try:
            return container_name in _docker_list_container_names(all_containers=all_containers)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            last_error = e
            logger.warning(
                f"[DOCKER FUNC][WARNING] `docker ps` probe for {container_name} "
                f"failed (attempt {attempt}/{DOCKER_PS_RETRIES}) [{e}]"
            )
            if attempt < DOCKER_PS_RETRIES:
                time.sleep(DOCKER_PS_RETRY_INTERVAL)

    logger.error(
        f"[DOCKER FUNC][ERROR] Could not determine presence of container "
        f"{container_name} after {DOCKER_PS_RETRIES} attempts [{last_error}]"
    )
    return None

def _is_docker_run_command(cmd_list):
    return len(cmd_list) >= 2 and cmd_list[0] == "docker" and cmd_list[1] == "run"

def _is_name_conflict_error(error):
    stderr = error.stderr or ""
    return (
        error.returncode == 125
        and "Conflict. The container name" in stderr
        and "is already in use by container" in stderr
    )

def wait_for_container_removed(
    container_name,
    timeout=DOCKER_REMOVE_TIMEOUT,
    stable_checks=1,
):
    """Block until ``container_name`` is CONFIRMED removed or ``timeout`` elapses.

    Only a positive ``docker ps`` confirmation of absence
    (``is_container_confirmed_absent``) counts toward ``stable_checks``. A failed
    or timed-out probe is treated as "still unknown" and resets the streak, so we
    never declare a container removed just because ``docker ps`` was unreachable.
    """
    start_time = time.time()
    absent_checks = 0
    while time.time() - start_time < timeout:
        if is_container_confirmed_absent(container_name):
            absent_checks += 1
            if absent_checks >= stable_checks:
                logger.info(
                    f"[DOCKER FUNC] Container {container_name} confirmed removed "
                    f"({absent_checks}/{stable_checks} stable checks)."
                )
                return True
        else:
            absent_checks = 0
        time.sleep(DOCKER_POLL_INTERVAL)

    logger.error(
        f"[DOCKER FUNC][ERROR] Container name still exists (or presence "
        f"unconfirmed) after waiting {timeout}s: {container_name}"
    )
    return False

def force_remove_container(
    container_name,
    timeout=DOCKER_REMOVE_TIMEOUT,
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
    if is_container_confirmed_absent(container_name):
        if wait_when_absent:
            return wait_for_container_removed(
                container_name,
                timeout=timeout,
                stable_checks=DOCKER_ABSENT_STABLE_CHECKS,
            )
        return True

    deadline = time.time() + timeout
    rm_attempts = 0
    while time.time() < deadline:
        rm_attempts += 1
        logger.info(
            f"[DOCKER FUNC] Removing container {container_name} "
            f"(attempt {rm_attempts}) ..."
        )
        try:
            subprocess.run(
                ["docker", "rm", "-f", container_name],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=DOCKER_RM_CMD_TIMEOUT,
            )
        except subprocess.TimeoutExpired as e:
            # The CLI was killed at DOCKER_RM_CMD_TIMEOUT, but the daemon may
            # still be tearing the container down. Do NOT give up: fall through
            # to confirm-and-retry so we keep waiting for the real completion.
            logger.warning(
                f"[DOCKER FUNC][WARNING] Removing Container {container_name} "
                f"timed out after {DOCKER_RM_CMD_TIMEOUT}s; will re-confirm and "
                f"retry [{e}]"
            )
        except subprocess.CalledProcessError as e:
            stderr = e.stderr or ""
            if "No such container" in stderr:
                # Already gone from the daemon's perspective; still confirm via
                # docker ps to honour the stable-checks contract.
                return wait_for_container_removed(
                    container_name,
                    timeout=max(timeout, DOCKER_RM_CONFIRM_SLICE),
                    stable_checks=DOCKER_ABSENT_STABLE_CHECKS,
                )
            if "removal of container" in stderr and "is already in progress" in stderr:
                logger.info(
                    f"[DOCKER FUNC] Container removal already in progress: "
                    f"{container_name}; waiting for it to complete."
                )
            else:
                logger.warning(
                    f"[DOCKER FUNC][WARNING] Removing Container {container_name} "
                    f"failed [{e}]"
                )

        # After issuing the removal, wait one confirmation slice for the daemon
        # to actually release the name. If confirmed absent within the slice we
        # are done; otherwise loop and re-issue ``docker rm -f``.
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        slice_timeout = min(DOCKER_RM_CONFIRM_SLICE, remaining)
        if wait_for_container_removed(
            container_name,
            timeout=slice_timeout,
            stable_checks=DOCKER_ABSENT_STABLE_CHECKS,
        ):
            return True

    logger.error(
        f"[DOCKER FUNC][ERROR] Failed to confirm removal of container "
        f"{container_name} within {timeout}s after {rm_attempts} attempt(s)."
    )
    return False

def is_container_running(container_name):
    presence = _probe_container_presence(container_name, all_containers=False)
    # A failed/indeterminate probe (None) is reported as "not running" to keep
    # the historical boolean contract for callers that only gate a start on it.
    return presence is True

def is_container_confirmed_absent(container_name):
    """Return True only when ``docker ps`` positively confirms the name is gone.

    An indeterminate probe (``docker ps`` failing/timing out) returns False so
    the caller keeps waiting/retrying instead of racing ahead to ``docker run``
    and hitting a name conflict.
    """
    return _probe_container_presence(container_name, all_containers=True) is False

def make_unique_container_name(base_name):
    """Return a per-creation UNIQUE container name derived from ``base_name``.

    A microsecond-precision timestamp suffix (``%Y%m%d_%H%M%S_%f``) is appended
    so that two reinitializations within the same wall-clock second still get
    distinct names. Because names are never reused, a slow daemon-side teardown of
    the PREVIOUS container can no longer collide with the NEXT ``docker run`` (the
    root cause of the intermittent ``RC 125`` name conflicts under heavy parallel
    load). ``base_name`` (e.g. ``6_pg_syn_ano``) remains the stable PREFIX used by
    ``remove_containers_by_prefix`` for orphan sweeps.
    """
    timestamp = datetime_module.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return f"{base_name}_{timestamp}"

def list_containers_by_prefix(base_name):
    """Return the names of all existing containers whose name starts with
    ``base_name`` followed by our timestamp separator (``base_name + '_'``), plus
    an exact match on ``base_name`` itself (covers legacy fixed-name containers).

    The ``base_name + '_'`` guard prevents a shorter prefix from accidentally
    matching an unrelated container that merely shares a leading substring
    (e.g. base ``6_pg_syn_ano`` must not match ``60_pg_syn_ano_...``). A failed/
    timed-out ``docker ps`` is surfaced as an empty list so the caller does not
    mistake an unreachable daemon for "no orphans present".
    """
    for attempt in range(1, DOCKER_PS_RETRIES + 1):
        try:
            names = _docker_list_container_names(all_containers=True)
            prefix = f"{base_name}_"
            return [n for n in names if n == base_name or n.startswith(prefix)]
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(
                f"[DOCKER FUNC][WARNING] `docker ps` prefix scan for {base_name} "
                f"failed (attempt {attempt}/{DOCKER_PS_RETRIES}) [{e}]"
            )
            if attempt < DOCKER_PS_RETRIES:
                time.sleep(DOCKER_PS_RETRY_INTERVAL)
    logger.error(
        f"[DOCKER FUNC][ERROR] Could not list containers with prefix "
        f"{base_name} after {DOCKER_PS_RETRIES} attempts."
    )
    return []

def remove_containers_by_prefix(base_name):
    """Force-remove EVERY container sharing ``base_name`` as prefix and confirm
    each is gone. Used to reap orphans left behind by a previous crashed/killed
    run before a fresh start, and as a defensive sweep. Because each worker's
    ``base_name`` carries a unique numeric prefix (``0_``, ``6_`` ...), this only
    ever touches the calling worker's own containers and never interferes with
    other parallel workers.

    Returns True if all matched containers were confirmed removed (or none were
    present), False if at least one could not be confirmed gone.
    """
    stale = list_containers_by_prefix(base_name)
    if not stale:
        return True

    logger.info(
        f"[DOCKER FUNC] Prefix sweep for '{base_name}': found "
        f"{len(stale)} container(s) to remove: {stale}"
    )
    all_removed = True
    for name in stale:
        if not force_remove_container(name, wait_when_absent=True):
            logger.error(
                f"[DOCKER FUNC][ERROR] Prefix sweep failed to confirm removal "
                f"of container {name}"
            )
            all_removed = False
    return all_removed

def wait_for_container_name_free(container_name, timeout=DOCKER_NAME_FREE_TIMEOUT):
    """Block until ``container_name`` is CONFIRMED absent (free to reuse) or the
    ``timeout`` budget is exhausted.

    Implements requirement (1): before creating a container, if a container with
    the target name still exists, sleep and re-check; only after the name remains
    present beyond ``timeout`` (default 3600s) do we log an error and report
    failure to the caller (which surfaces it as a bounded failure rather than a
    silent hang). Returns True as soon as the name is confirmed free.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_container_confirmed_absent(container_name):
            return True
        logger.info(
            f"[DOCKER FUNC] Container name '{container_name}' still in use; "
            f"waiting {DOCKER_NAME_FREE_POLL_INTERVAL}s before re-checking "
            f"(elapsed {int(time.time() - start_time)}s / {timeout}s)."
        )
        time.sleep(DOCKER_NAME_FREE_POLL_INTERVAL)

    logger.error(
        f"[DOCKER FUNC][ERROR] Container name '{container_name}' is STILL in use "
        f"after waiting {timeout}s; aborting creation to avoid a name conflict."
    )
    return False

def docker_container_init(container_name, container_config) :
    if not force_remove_container(container_name):
        logger.error(f"[DOCKER FUNC][ERROR] Pre-cleaning Container {container_name}")
        return False

    dbms_kind, cpus, mem_limit, mem_swap, storage_size, port, db_host, db_port = _container_config_values(container_config)

    logger.info(f"[DOCKER FUNC] Docker Container ``{container_name}'' Initialization.")
    if dbms_kind == "mysql":
        docker_commands = [
            mysql_create_container_str.format(cpus, mem_limit, mem_swap, storage_size, port, container_name),
            mysql_init_container_str.format(container_name, db_host, db_port)
        ]
    else:
        docker_commands = [
            create_container_str.format(cpus, mem_limit, mem_swap, storage_size, port, container_name),
            init_container_str.format(container_name, db_host, db_port)
        ]
    for command_index, docker_command in enumerate(docker_commands):
        command_stage = "container creation" if command_index == 0 else "DBMS bootstrap"
        cmd_list = shlex.split(docker_command)
        if command_index == 1:
            from Baselines.container_boundary import prepare_bootstrap
            prepare_bootstrap(container_name, cmd_list)
        if _is_docker_run_command(cmd_list):
            from Baselines.container_boundary import isolated_docker_command
            cmd_list = isolated_docker_command(cmd_list, container_config)
            # Requirement (1): before ``docker run``, make sure the target name is
            # genuinely free. First force-remove any leftover holding this exact
            # name, then poll until the name is CONFIRMED absent. If it is still in
            # use after DOCKER_NAME_FREE_TIMEOUT (3600s), abort with an error rather
            # than racing into a guaranteed RC 125 conflict.
            if not force_remove_container(container_name):
                logger.error(f"[DOCKER FUNC][ERROR] Pre-cleaning Container {container_name}")
                return False
            if not wait_for_container_name_free(container_name):
                logger.error(
                    f"[DOCKER FUNC][ERROR] Aborting creation: container name "
                    f"{container_name} did not become free within "
                    f"{DOCKER_NAME_FREE_TIMEOUT}s."
                )
                return False

        attempts = 0
        while True :
            try:
                _ = subprocess.run(cmd_list, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=DOCKER_INIT_TIMEOUT)
                break

            except subprocess.TimeoutExpired as e:
                # A hung `docker run` / init script no longer blocks forever:
                # report failure so the caller's retry path can clean up and
                # the shell driver can re-run this step from the checkpoint.
                logger.error(
                    f"[DOCKER FUNC][ERROR] {command_stage} timed out after "
                    f"{DOCKER_INIT_TIMEOUT}s for container `{container_name}`: "
                    f"{' '.join(cmd_list)} [{str(e)}]"
                )
                if _is_docker_run_command(cmd_list):
                    force_remove_container(container_name, wait_when_absent=True)
                return False

            except subprocess.CalledProcessError as e:
                logger.error(
                    f"[DOCKER FUNC][ERROR] {command_stage} failed for container "
                    f"`{container_name}` (attempt {attempts + 1}/3): "
                    f"{' '.join(cmd_list)}"
                )
                logger.error(f"[DOCKER FUNC][ERROR] [Return Code]: {e.returncode}")
                if e.stdout and e.stdout.strip() != "" : logger.error(f"[DOCKER FUNC][ERROR][STDOUT]:\n{e.stdout}")
                if e.stderr and e.stderr.strip() != "" : logger.error(f"[DOCKER FUNC][ERROR][STDERR]:\n{e.stderr}")

                if _is_docker_run_command(cmd_list) and _is_name_conflict_error(e):
                    logger.warning(
                        f"[DOCKER FUNC][WARNING] Docker name conflict detected "
                        f"for {container_name}; force removing stale container."
                    )
                    if not force_remove_container(container_name, wait_when_absent=True):
                        return False

                # Never block on interactive stdin in a batch/tmux run (which
                # would hang forever). Retry a bounded number of times, then
                # surface failure to the caller's existing retry logic.
                attempts += 1
                if attempts >= 3 :
                    logger.error(
                        f"[DOCKER FUNC][ERROR] {command_stage} permanently failed "
                        f"for container `{container_name}` after {attempts} attempts."
                    )
                    return False
                time.sleep(5)
                continue
    
    # Check Current Container Status
    if is_container_running(container_name):
        return True
    logger.error(
        f"[DOCKER FUNC][ERROR] Container `{container_name}` completed initialization "
        "commands but is not running."
    )
    return False

def docker_container_del(container_name):
    return force_remove_container(container_name)

def docker_container_reinit(container_name, container_config):
    # logger.info(f"Reinitialization the Container ``{container_name}''.")
    del_status = docker_container_del(container_name)
    if not del_status : 
        logger.error(f"[DOCKER FUNC][ERROR] Deleting Container {container_name}")
        exit()
    init_status = docker_container_init(container_name, container_config)
    if not init_status :
        logger.error(f"[DOCKER FUNC][ERROR] Initializing Container {container_name}]")
        exit()

def recreate_container_with_new_name(old_container_name, base_name, container_config):
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
        kill_tmux_session(old_container_name)
        if not force_remove_container(old_container_name, wait_when_absent=True):
            logger.error(
                f"[DOCKER FUNC][ERROR] Failed to confirm removal of previous "
                f"container {old_container_name} before recreation."
            )
            # Continue anyway: the new unique name will not collide, and a later
            # prefix sweep / final cleanup will reap the leftover.

    # 2) Create under a fresh unique name (never reused -> no name conflict).
    new_container_name = make_unique_container_name(base_name)
    init_status = docker_container_init(new_container_name, container_config)
    if not init_status:
        logger.error(
            f"[DOCKER FUNC][ERROR] Initializing new container {new_container_name}"
        )
        return False, new_container_name
    return True, new_container_name

POSTGRES_INIT_ERROR_PATTERN = re.compile(
    r"^(?:psql:[^:]+:\d+:\s*)?(?:ERROR|FATAL|PANIC):",
    re.MULTILINE,
)


def database_init_log_errors(status_log, dbms):
    """Return PostgreSQL client errors that may be masked by psql exit status 0."""
    if not os.path.isfile(status_log) or normalize_dbms(dbms) == "mysql":
        return []
    try:
        with open(status_log, "r", encoding="utf-8", errors="replace") as file:
            content = file.read()
    except OSError as exc:
        return [f"unable to read initialization log: {exc}"]
    return [
        line.strip()
        for line in content.splitlines()
        if POSTGRES_INIT_ERROR_PATTERN.match(line)
    ]


@_dbdiag_io_heavy_phase
def docker_container_init_db(container_name, init_db_config, session_name="docker_test"):
    # Import-only scheduling budget; workload and score deadlines are unchanged.
    floor = int(os.environ.get("DB_DIAG_DATABASE_INIT_TIMEOUT_SECONDS", "0"))
    if floor < 0:
        raise ValueError("DB_DIAG_DATABASE_INIT_TIMEOUT_SECONDS must be nonnegative")
    if floor:
        init_db_config = dict(init_db_config, timeout=max(init_db_config['timeout'], floor))
    base_dir = os.path.realpath(init_db_config['base_dir'])
    dbms_kind = normalize_dbms(init_db_config.get("dbms"))
    if dbms_kind == "mysql":
        init_db_script_path = os.path.join(base_dir, 'Scripts', 'initialize_database_mysql.sh')
        instance_directory = os.path.join(base_dir, 'Database_Instances', 'mysql_instances')
        default_port = 3306
    else:
        init_db_script_path = os.path.join(base_dir, 'Scripts', 'initialize_database_pg.sh')
        instance_directory = os.path.join(base_dir, 'Database_Instances', 'postgresql_instances')
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
    tmp_directory = os.path.realpath(
        init_db_config.get('tmp_directory', os.path.join(base_dir, 'TMP'))
    )

    init_db_str = f"bash {init_db_script_path} --host {db_host} --port {db_port} --database_name {db_name} --database_type {db_type} --database_schema {db_schema} --instance_dir {instance_directory} --tmp_dir {tmp_directory} --user {db_user} --password {db_password}"
    # print(init_db_str)

    init_db_cmd = init_db_str.split()
    if dbms_kind == "mysql":
        # docker exec does not inherit the host's environment. Forward only the
        # explicit import-session options; SQL/evaluation sessions keep defaults.
        import_env = []
        for key in ("DB_DIAG_MYSQL_IMPORT_DDL_BUFFER_BYTES", "DB_DIAG_MYSQL_IMPORT_READ_THREADS"):
            value = os.environ.get(key)
            if value is not None:
                if not value.isascii() or not value.isdigit() or len(value) > 10:
                    raise ValueError(f"{key} must be a nonnegative integer")
                import_env.append(f"{key}={value}")
        if import_env:
            init_db_cmd = ["env", *import_env, *init_db_cmd]
    status_log = os.path.join(init_db_config['output_dir'], "init_db_log.txt")
    from Baselines.container_boundary import prepare_bootstrap
    prepare_bootstrap(container_name, init_db_cmd)
    docker_status = run_docker_exec_status(container_name, init_db_cmd, status_log, timeout = init_db_config['timeout'] + 120, session_name = session_name)
    init_errors = database_init_log_errors(status_log, dbms_kind)
    if docker_status and init_errors:
        logger.error(
            "[DOCKER FUNC][ERROR] Database initialization emitted "
            f"{len(init_errors)} PostgreSQL error(s) despite exit status 0. "
            f"First errors: {init_errors[:3]}"
        )
        docker_status = False
    logger.info(f"[DOCKER FUNC] Database Initialization Status: {'Success' if docker_status else 'Failure'}")
    if docker_status:
        from Baselines.container_boundary import prepare_database_socket_access
        prepare_database_socket_access(container_name)
    
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

def _fast_reset_db_params(init_db_config):
    """Extract the DB connection parameters shared by snapshot/reset from an
    ``init_db_config`` (the same dict passed to ``docker_container_init_db``)."""
    dbms = init_db_config.get("dbms")
    dbms_kind = normalize_dbms(dbms)
    default_port = 3306 if dbms_kind == "mysql" else 5432
    db_host = init_db_config.get("db_host", init_db_config.get("pg_host", "localhost"))
    db_port = init_db_config.get("db_port", init_db_config.get("pg_port", default_port))
    db_name = init_db_config.get("db_name", "tpch_1")
    db_schema = init_db_config.get("db_schema", "diagnosis")
    if dbms_kind == "mysql":
        db_schema = db_name
    db_user = init_db_config.get("db_user", "diagnosis")
    db_password = init_db_config.get("password", "diagnosis")
    base_dir = init_db_config["base_dir"]
    golden_name = derive_golden_db_name(db_name)
    return dbms, base_dir, db_name, golden_name, db_host, db_port, db_user, db_password, db_schema


def docker_container_build_baseline_snapshot(container_name, init_db_config, session_name="docker_test"):
    """Create the container-local golden baseline clone of the CURRENT ready-state
    database (imported + data-modification already applied).

    Call this ONCE per sample, immediately after the initial data modification
    succeeds. Returns True on success; on any failure returns False (the caller
    should then simply keep using the legacy full re-initialization path and skip
    fast reset for this sample).
    """
    dbms, base_dir, db_name, golden_name, db_host, db_port, db_user, db_password, db_schema = \
        _fast_reset_db_params(init_db_config)

    cmd = build_fast_reset_command(
        base_dir, dbms, "snapshot", db_name, golden_name,
        db_host, db_port, db_user, db_password, db_schema,
    )
    status_log = os.path.join(init_db_config["output_dir"], "fast_reset_snapshot_log.txt")
    timeout = max(
        init_db_config.get("timeout", 3600) + 120,
        FAST_RESET_SNAPSHOT_TIMEOUT_SECONDS,
    )
    logger.info(
        f"[DOCKER FUNC] Building golden baseline `{golden_name}` from `{db_name}` "
        f"in container `{container_name}` ..."
    )
    docker_status = run_docker_exec_status(
        container_name,
        cmd,
        status_log,
        timeout=timeout,
        session_name=session_name,
        heartbeat_message=(
            f"[DOCKER FUNC] Golden baseline `{golden_name}` is still running "
            f"in container `{container_name}`; waiting up to {timeout}s."
        ),
    )
    logger.info(f"[DOCKER FUNC] Golden Baseline Snapshot Status: {'Success' if docker_status else 'Failure'}")
    return docker_status


def docker_container_fast_reset(container_name, init_db_config, session_name="docker_test"):
    """Restore the working database to the golden baseline INSIDE the existing
    container (no container teardown, no dump re-import).

    Returns True on success. On failure returns False so the caller can fall back
    to ``recreate_container_with_new_name`` + ``docker_container_init_db`` +
    data-modification replay (the original, always-correct path).
    """
    dbms, base_dir, db_name, golden_name, db_host, db_port, db_user, db_password, db_schema = \
        _fast_reset_db_params(init_db_config)

    cmd = build_fast_reset_command(
        base_dir, dbms, "reset", db_name, golden_name,
        db_host, db_port, db_user, db_password, db_schema,
    )
    status_log = os.path.join(init_db_config["output_dir"], "fast_reset_log.txt")
    timeout = init_db_config.get("timeout", 3600) + 120
    logger.info(
        f"[DOCKER FUNC] Fast-resetting `{db_name}` from golden baseline "
        f"`{golden_name}` in container `{container_name}` ..."
    )
    docker_status = run_docker_exec_status(container_name, cmd, status_log, timeout=timeout, session_name=session_name)
    logger.info(f"[DOCKER FUNC] Fast Reset Status: {'Success' if docker_status else 'Failure'}")
    return docker_status


def run_docker_exec_observation(container_name, command, obser_log, timeout=60,
                                session_name="docker_test", window_name="observation",
                                ready_pattern=None):
    if tmux_window_running(session_name, window_name) or not tmux_has_session(session_name) or not tmux_has_window(session_name, window_name):
        reset_tmux_window(session_name, window_name)

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
    command_str = " ".join([shlex.quote(c) for c in command])

    # Wrap with exit-code capture: the real exit code of the `docker exec`
    # command is recorded to a sentinel file via ${PIPESTATUS[0]}, so a failing
    # in-container command is detected even though `tee`/send-keys return 0. The
    # exit code goes to a separate file, leaving obser_log content intact for the
    # callers that read it back.
    from Baselines.container_boundary import observation_argv
    inner_command_str = shlex.join(observation_argv(container_name, command))
    docker_cmd, exit_code_path = build_exit_code_capture(inner_command_str, obser_log)
    reset_captured_log(obser_log)

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
        result = subprocess.run(commands, capture_output=True, text=True, check=True, timeout=timeout)

        # Return the combined output (stdout + stderr)
        # output += f"Script executed successfully, with the output as follows:\n{stdout}\n\n"
    
        obser_state = wait_for_log_file_signal(
            obser_log, timeout + 60, ready_pattern=ready_pattern
        )

        output = ""
        if obser_state == "timeout":
            logger.warning(f"[DOCKER FUNC][ERROR] Observation log file did not complete within the expected time frame of {timeout + 60} seconds.")
            output += f"[Error in Script Exec]: Command timed out after {str(timeout + 60)} seconds\n"
            return False, output
        if obser_state == "ready":
            logger.info(
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
        exit_code = read_captured_exit_code(exit_code_path)
        if exit_code is None :
            logger.error(f"[DOCKER FUNC][ERROR] Could not determine exit code for command: {obser_log}")
            output += f"[Error in Script Exec]: Could not determine command exit code\n"
            return False, output
        if exit_code != 0 :
            logger.error(f"[DOCKER FUNC][ERROR] Command failed with exit code {exit_code}: {obser_log}")
            output += f"[Error in Script Exec]: Command failed with exit code {exit_code}\n"
            return False, output

        return True, output
    
    except subprocess.TimeoutExpired as e:
        stdout = to_text(e.stdout)
        stderr = to_text(e.stderr)
        # Timeout handling
        output += f"[Error in Script Exec]: Command timed out after {timeout} seconds [{str(e)}]\n"
        if stdout.strip():
            output += "[Partial Output]\n" + stdout + "\n"
        if stderr.strip():
            output += "[Error output]\n" + stderr + "\n"
        
        return False, output
    
    except subprocess.CalledProcessError as e:
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

def run_docker_exec_status(container_name, command, status_log, timeout=1200, backend=False, session_name="docker_test", window_name="status", heartbeat_message=None):
    if tmux_window_running(session_name, window_name) or not tmux_has_session(session_name) or not tmux_has_window(session_name, window_name):
        reset_tmux_window(session_name, window_name)

    # Build the in-container command and wrap it so the REAL exit code of the
    # docker command (via ${PIPESTATUS[0]}) is captured to a sentinel file. The
    # tmux send-keys subprocess only reports the status of send-keys itself, so
    # the exit code file is the only reliable signal of in-container failure.
    inner_command_str = " ".join(["docker", "exec", container_name]).strip() + " " + " ".join(command).strip()
    docker_command, exit_code_path = build_exit_code_capture(inner_command_str, status_log)
    reset_captured_log(status_log)
    try:
        commands = ["tmux", "send-keys", "-t", f"{session_name}:{window_name}", docker_command, "C-m"]
        if backend : process = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        else : result = subprocess.run(commands, capture_output=True, text=True, check=True, timeout=timeout)

        # First make sure the command actually finished (the __DONE__ marker).
        status_state = wait_for_log_file_done(
            status_log,
            timeout + 60,
            heartbeat_message=heartbeat_message,
        )
        if not status_state :
            logger.error(f"[DOCKER FUNC][ERROR] Command did not complete within {timeout + 60} seconds: {status_log}")
            return False

        # Then verify the captured exit code: a missing/non-zero code means the
        # in-container command failed even though the log reached __DONE__.
        exit_code = read_captured_exit_code(exit_code_path)
        if exit_code is None :
            logger.error(f"[DOCKER FUNC][ERROR] Could not determine exit code for command: {status_log}")
            return False
        if exit_code != 0 :
            logger.error(f"[DOCKER FUNC][ERROR] Command failed with exit code {exit_code}: {status_log}")
            return False

        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"[DOCKER FUNC][ERROR] Error in Script Execution [{str(e)}]")

        return False

    except subprocess.TimeoutExpired as e:
        # The send-keys dispatch (or synchronous wait) exceeded the timeout.
        logger.error(f"[DOCKER FUNC][ERROR] Command timed out after {timeout} seconds [{str(e)}]")
        return False

    except Exception as e:
        # Any other unforeseen error must be treated as a failure, never success.
        logger.error(f"[DOCKER FUNC][ERROR] Unexpected error in Script Execution [{str(e)}]")
        return False

def _mysql_table_identifier(table_name):
    return f"`{str(table_name).replace('`', '``')}`"


def get_mysql_db_info(db_config, table_names):
    mysql_db_config = db_config.copy()
    mysql_db_config['port'] = mysql_db_config['host_port']
    mysql_db_config['dbms'] = 'mysql'
    schema = effective_schema_name(mysql_db_config, "mysql")

    create_table_statements, primary_key_constraints, foreign_key_constraints, create_index_statements = [], [], [], []
    for table_name in table_names:
        create_rows = mysql_execute_sql(f"SHOW CREATE TABLE {_mysql_table_identifier(table_name)};", mysql_db_config)
        if create_rows:
            create_table_statements.append(create_rows[0][1] + ";")

        primary_key_constraints.extend(
            [
                f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (`{column}`);"
                for (column,) in mysql_execute_sql(
                    """
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE table_schema = %s
                      AND table_name = %s
                      AND constraint_name = 'PRIMARY'
                    ORDER BY ordinal_position;
                    """,
                    mysql_db_config,
                    (schema, table_name)
                ) or []
            ]
        )

        foreign_key_constraints.extend(
            [
                (
                    f"ALTER TABLE `{table_name}` ADD CONSTRAINT `{constraint_name}` "
                    f"FOREIGN KEY (`{column_name}`) REFERENCES `{referenced_table}` (`{referenced_column}`);"
                )
                for constraint_name, column_name, referenced_table, referenced_column in mysql_execute_sql(
                    """
                    SELECT constraint_name, column_name, referenced_table_name, referenced_column_name
                    FROM information_schema.key_column_usage
                    WHERE table_schema = %s
                      AND table_name = %s
                      AND referenced_table_name IS NOT NULL
                    ORDER BY constraint_name, ordinal_position;
                    """,
                    mysql_db_config,
                    (schema, table_name)
                ) or []
            ]
        )

        create_index_statements.extend(
            [
                (
                    f"CREATE {'UNIQUE ' if non_unique == 0 else ''}INDEX `{index_name}` "
                    f"ON `{table_name}` (`{column_name}`);"
                )
                for index_name, non_unique, column_name in mysql_execute_sql(
                    """
                    SELECT index_name, non_unique, column_name
                    FROM information_schema.statistics
                    WHERE table_schema = %s
                      AND table_name = %s
                      AND index_name <> 'PRIMARY'
                    ORDER BY index_name, seq_in_index;
                    """,
                    mysql_db_config,
                    (schema, table_name)
                ) or []
            ]
        )

    return create_table_statements, primary_key_constraints, foreign_key_constraints, create_index_statements


def get_db_info(db_config, table_names, docker_container_name):
    if normalize_dbms(db_config.get("dbms")) == "mysql":
        return get_mysql_db_info(db_config, table_names)

    # get information each table (including CREATE TABLE, FOREIGN KEY, PRIMARY KEY, etc.)
    psycopg_db_config = db_config.copy()
    psycopg_db_config['port'] = psycopg_db_config['host_port']
    psycopg_db_config['dbms'] = 'pg'

    create_table_statements, primary_key_constraints, foreign_key_constraints, create_index_statements = [], [], [], []
    for table_name in table_names:
        command = 'docker exec -e PGPASSWORD={} {} pg_dump -s -h {} -p {} -U {} -d {} -t "{}.{}" --schema-only'.format(
            db_config['password'],
            docker_container_name,
            db_config['host'],
            db_config['port'],
            db_config['user'],
            db_config['db_name'],
            db_config['schema'],
            table_name
        )

        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        output, error = process.communicate()

        if error is None:
            database_dump = output.decode()
            filtered_lines = [line for line in database_dump.split("\n") 
                if not (line.strip() == "" or line.startswith("SET") or line.startswith("--") or line.startswith("SELECT") or line.startswith("pg_dump") or line.startswith("\\restrict ") or line.startswith("\\unrestrict"))]
            database_dump = "\n".join(filtered_lines)

            statements = database_dump.split(";")
            for stmt in statements:
                if stmt.upper().startswith("CREATE TABLE"):
                    create_table_statements.append(stmt.strip().replace(f"{db_config['schema']}.", "") + ";")
                elif "PRIMARY KEY" in stmt.upper():
                    primary_key_constraints.append(stmt.strip().replace(f"{db_config['schema']}.", "") + ";")
                elif "FOREIGN KEY" in stmt.upper():
                    foreign_key_constraints.append(stmt.strip().replace(f"{db_config['schema']}.", "") + ";")
        else:
            print(f"Error getting DDL for table {table_name}: {error}")

        get_indexes_sql = "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{}';".format(table_name)
        # print(psycopg_db_config['port'])
        for res in pg_execute_sql(get_indexes_sql, psycopg_db_config):
            create_index_statements.append(res[1].replace(f"{db_config['schema']}.", ""))

    create_table_statements = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in create_table_statements]
    primary_key_constraints = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in primary_key_constraints]
    foreign_key_constraints = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in foreign_key_constraints]
    create_index_statements = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in create_index_statements]

    return create_table_statements, primary_key_constraints, foreign_key_constraints, create_index_statements

def obtain_database_schema(db_config, schema_path, docker_container_name):
    dbms_kind = normalize_dbms(db_config.get("dbms"))
    query_db_config = db_config.copy()
    query_db_config['port'] = query_db_config['host_port']
    query_db_config['dbms'] = dbms_kind

    table_names = get_table_names_from_db(query_db_config, dbms_kind)
    create_table_statements, primary_key_statements, foreign_key_statements, create_index_statements = get_db_info(db_config, table_names, docker_container_name)
    
    db_schema = dict()
    data_types = []
    table_info_list = []
    for table_name in table_names:
        table_info = dict()
        table_info["table"] = table_name
        if dbms_kind == "mysql":
            table_identifier = _mysql_table_identifier(table_name)
            rows = mysql_execute_sql(f"SELECT COUNT(*) FROM {table_identifier};", query_db_config)[0][0]
            columns_and_types = mysql_execute_sql(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s;
                """,
                query_db_config,
                (effective_schema_name(query_db_config, dbms_kind), table_name)
            )
        else:
            rows = pg_execute_sql(f"SELECT COUNT(*) FROM {table_name};", query_db_config)[0][0]
            columns_and_types = pg_execute_sql(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s;",
                query_db_config,
                (table_name,)
            )
        table_info["rows"] = rows
        table_info["columns"] = []
        for (column, data_type) in columns_and_types:
            data_type = data_type.upper()
            
            table_info["columns"].append(
                {
                    "name": column,
                    "type": data_type
                }
            )
            data_types.append(data_type)
        
        table_ddl = None
        for ddl in create_table_statements:
            normalized_ddl = ddl.lower().replace("`", "")
            if "create table {} ".format(table_name).lower() in normalized_ddl:
                table_ddl = ddl
                break
        
        if table_ddl is None:
            logger.warning(f"[DOCKER FUNC][WARNING] DDL statement for table {table_name} not found.")
            # raise ValueError("can not find ddl for table {}".format(table_name))

        table_info["ddl"] = table_ddl

        table_info_list.append(table_info)

    db_schema["table_info"] = table_info_list
    db_schema["primary_key_statements"] = primary_key_statements
    db_schema["foreign_key_statements"] = foreign_key_statements
    db_schema["create_index_statements"] = create_index_statements

    # f"./data/{db_name}_schema.json"
    abs_path = os.path.abspath(schema_path)
    # print(abs_path)
    if not os.path.exists(os.path.dirname(abs_path)) : os.makedirs(os.path.dirname(abs_path))
    
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(db_schema, f, indent=2, ensure_ascii=False)
    
    return db_schema

def _serialize_sql_execution_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(key): _serialize_sql_execution_value(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_sql_execution_value(item) for item in value]
    return str(value)

def _extract_sql_execution_result(cursor):
    status_message = getattr(cursor, "statusmessage", "")
    row_count = cursor.rowcount

    if cursor.description is not None:
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        serialized_rows = [
            [_serialize_sql_execution_value(value) for value in row]
            for row in rows
        ]
        return {
            "result_type": "rows",
            "status_message": status_message,
            "row_count": row_count,
            "columns": columns,
            "rows": serialized_rows
        }

    return {
        "result_type": "command",
        "status_message": status_message,
        "row_count": row_count
    }

def _connect_for_workload(db_config, dbms_kind):
    if dbms_kind == "mysql":
        if mysql_connector is None:
            raise ImportError("mysql-connector-python is required for MySQL workload execution.")
        conn = mysql_connector.connect(
            host=db_config["host"],
            port=int(db_config["host_port"]),
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["db_name"],
            connection_timeout=DB_CONNECT_TIMEOUT,
            use_pure=True
        )
        conn.autocommit = True
        return conn

    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["host_port"],
        user=db_config["user"],
        password=db_config["password"],
        dbname=db_config["db_name"],
        connect_timeout=DB_CONNECT_TIMEOUT,
        **({"client_encoding": db_config["client_encoding"]}
           if db_config.get("client_encoding") else {}),
    )
    conn.autocommit = True
    return conn


def _prepare_workload_session(cursor, db_config, dbms_kind, timeout_seconds):
    if dbms_kind == "mysql":
        try:
            cursor.execute(f"SET SESSION max_execution_time = {int(timeout_seconds * 1000)};")
        except Exception as e:
            logger.warning(f"[DOCKER FUNC][WARNING] Failed to set MySQL max_execution_time: {e}")
        return

    if db_config.get("schema"):
        cursor.execute(f"SET search_path TO {db_config['schema']};")
    cursor.execute(f"SET statement_timeout = '{timeout_seconds}s';")


def execute_sql_file_with_timing(sql_file_path, db_config, exec_num=1):
    dbms_kind = normalize_dbms(db_config.get("dbms"))
    with open(sql_file_path, 'r') as file:
        sql_content = file.read()
    sql_statements = split_executable_sql(sql_content, dbms_kind)

    results = []

    for sql in sql_statements:
        exec_time_list = []
        error_flag = False
        sql_execution_result = None
        for _ in range(exec_num) :
            conn = None
            cursor = None
            try:
                conn = _connect_for_workload(db_config, dbms_kind)
                cursor = conn.cursor()
                _prepare_workload_session(cursor, db_config, dbms_kind, SQL_EXEC_TIMEOUT)

                start_time = time.perf_counter()
                cursor.execute(sql)
                sql_execution_result = _extract_sql_execution_result(cursor)
                end_time = time.perf_counter()
                exec_time_list.append(end_time - start_time)
            
            except Exception as e:
                results.append({
                    "sql": sql,
                    "execution_time": -1,
                    "success": False,
                    "error": str(e),
                    "result": None
                })
                error_flag = True
                break
            finally:
                if cursor:
                    close_cursor_safely(cursor, conn, logger)
                if conn:
                    conn.close()
        
        if error_flag : continue

        results.append({
            "sql": sql,
            "execution_time": statistics.median(exec_time_list),
            "success": True,
            "error": None,
            "result": sql_execution_result
        })

    return results


def is_workload_timeout_error(exc):
    """Recognize DB timeout/cancel codes without confusing ordinary SQL errors."""
    code = getattr(exc, "pgcode", None) or getattr(exc, "sqlstate", None)
    return (
        code == "57014"
        or (code == "55P03" and "lock timeout" in str(exc).lower())
        or getattr(exc, "errno", None) in (1205, 3024, 1969)
    )


def execute_sql_file_in_session_with_timing(sql_file_path, db_config, exec_num=1,
                                          *, timeout_seconds=None, result_extractor=None):
    """Execute each full SQL-file run in one connection and capture statement latency.

    Unlike the legacy `execute_sql_file_with_timing`, this evaluator-oriented
    variant preserves explicit transactions, session-level settings, temporary
    objects and locks across statements. When `exec_num` is greater than one, the
    complete file is repeated in a fresh session and per-statement median latency
    is returned. Existing callers retain the 1200s per-statement behavior.
    Supplying timeout_seconds opts into evaluator behavior: matching server
    timeout and immediate return on a DB timeout (the caller bounds the file).
    """
    if timeout_seconds is not None:
        import math
        if (isinstance(timeout_seconds, bool)
                or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
            raise ValueError("timeout_seconds must be positive and finite.")
    dbms_kind = normalize_dbms(db_config.get("dbms"))
    with open(sql_file_path, 'r', encoding="utf-8") as file:
        sql_content = file.read()
    sql_statements = split_executable_sql(sql_content, dbms_kind)

    exec_num = max(1, int(exec_num))
    run_results = []
    for _ in range(exec_num):
        conn = None
        cursor = None
        current_run = []
        try:
            conn = _connect_for_workload(db_config, dbms_kind)
            cursor = conn.cursor()
            _prepare_workload_session(
                cursor, db_config, dbms_kind,
                SQL_EXEC_TIMEOUT if timeout_seconds is None else timeout_seconds,
            )

            for sql in sql_statements:
                start_time = time.perf_counter()
                try:
                    cursor.execute(sql)
                    sql_execution_result = (
                        result_extractor(cursor, sql) if result_extractor is not None
                        else _extract_sql_execution_result(cursor)
                    )
                    elapsed = time.perf_counter() - start_time
                    capture_overhead = sql_execution_result.pop("_capture_overhead_seconds", 0.0)
                    current_run.append({
                        "sql": sql,
                        "execution_time": max(0.0, elapsed - capture_overhead),
                        "success": True,
                        "error": None,
                        "result": sql_execution_result,
                    })
                except Exception as e:
                    current_run.append({
                        "sql": sql,
                        "execution_time": -1,
                        "success": False,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "result": None,
                    })
                    if timeout_seconds is not None and is_workload_timeout_error(e):
                        current_run[-1]["timed_out"] = True
                        current_run[-1]["sqlstate"] = getattr(e, "pgcode", None)
                        return current_run
        finally:
            if cursor:
                close_cursor_safely(cursor, conn, logger)
            if conn:
                conn.close()
        run_results.append(current_run)

    if exec_num == 1:
        return run_results[0]

    results = []
    for statement_index, sql in enumerate(sql_statements):
        attempts = [run[statement_index] for run in run_results]
        successful = [item for item in attempts if item["success"]]
        representative = successful[-1] if successful else attempts[-1]
        results.append({
            "sql": sql,
            "execution_time": (
                statistics.median(item["execution_time"] for item in successful)
                if len(successful) == len(attempts)
                else -1
            ),
            "success": len(successful) == len(attempts),
            "error": next(
                (item["error"] for item in attempts if not item["success"]),
                None,
            ),
            "result": representative["result"] if len(successful) == len(attempts) else None,
        })
    return results

def check_workload_timeout(workload_sql, db_config, timeout_seconds=1200):
    dbms_kind = normalize_dbms(db_config.get("dbms"))
    error_state = False
    execution_results = {
        "success": True,
        "total_execution_time": 0.0,
        "executed_sql_count": 0,
        "failed_sql": None,
        "failed_sql_index": None,
        "failed_reason": None,
        "timeout": False,
        "sql_results": []
    }

    conn = None
    cursor = None

    try:
        conn = _connect_for_workload(db_config, dbms_kind)
        cursor = conn.cursor()
        _prepare_workload_session(cursor, db_config, dbms_kind, timeout_seconds)

        sql_list = split_executable_sql(workload_sql, dbms_kind)
        sql_list = [sql.strip() for sql in sql_list if sql.strip()]

        total_start = time.perf_counter()
        for idx, sql in enumerate(sql_list):
            current_sql_result = {
                "sql_index": idx + 1,
                "sql": sql,
                "success": True,
                "execution_time": 0.0,
                "error": None,
                "timeout": False
            }

            try:
                start_time = time.perf_counter()
                cursor.execute(sql)
                if dbms_kind == "mysql":
                    consume_cursor_results(cursor, conn)
                end_time = time.perf_counter()

                execution_time = (end_time - start_time)
                current_sql_result["execution_time"] = execution_time
                execution_results["executed_sql_count"] += 1

            except errors.QueryCanceled as e:
                error_state = True

                current_sql_result["success"] = False
                current_sql_result["timeout"] = True
                current_sql_result["error"] = str(e)

                execution_results["success"] = False
                execution_results["timeout"] = True

                execution_results["failed_sql"] = sql
                execution_results["failed_sql_index"] = idx + 1
                execution_results["failed_reason"] = str(e)

                logger.warning(f"[DOCKER FUNC][WARNING][CHECK VERIFIED WORKLOAD TIMEOUT] SQL Statement TIMEOUT [{idx+1}/{len(sql_list)}]")

                execution_results["sql_results"].append(current_sql_result)

                break

            except Exception as e:
                error_state = True

                current_sql_result["success"] = False
                current_sql_result["error"] = str(e)

                execution_results["success"] = False

                execution_results["failed_sql"] = sql
                execution_results["failed_sql_index"] = idx + 1
                execution_results["failed_reason"] = str(e)

                execution_results["sql_results"].append(current_sql_result)

                logger.error(f"[DOCKER FUNC][ERROR][CHECK VERIFIED WORKLOAD TIMEOUT] SQL Execution Failed [{idx+1}/{len(sql_list)}]: {str(e)}")

                break

            execution_results["sql_results"].append(current_sql_result)

            current_time = time.perf_counter()

            if current_time - total_start > timeout_seconds:
                error_state = True

                execution_results["success"] = False
                execution_results["timeout"] = True
                execution_results["failed_reason"] = f"Workload execution timeout after {timeout_seconds} seconds"

                logger.warning(f"[DOCKER FUNC][WARNING][CHECK VERIFIED WORKLOAD TIMEOUT] Workload Execution TIMEOUT after {timeout_seconds} seconds.")
                break

        total_end = time.perf_counter()

        execution_results["total_execution_time"] = total_end - total_start

    except Exception as e:
        logger.error(f"[DOCKER FUNC][ERROR][CHECK VERIFIED WORKLOAD TIMEOUT] Workload Execution Error: {str(e)}")

        error_state = True

        execution_results["success"] = False
        execution_results["failed_reason"] = str(e)

    finally:
        if cursor : close_cursor_safely(cursor, conn, logger)
        if conn : conn.close()

    return error_state, execution_results

if __name__ == "__main__":
    exit()
