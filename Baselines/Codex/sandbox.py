"""Controller-only Docker isolation. No command allowlist is a security boundary.

Database initialization/injection remains in Baselines.Codex.react. Afterwards,
the common boundary exposes only the sanitized sample and agent-owned scratch.
SQL file APIs see the same sealed filesystem as Bash. Controllers retain their
private view without resetting the fault. Failure never falls back unsealed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import time
import uuid


class IsolationError(RuntimeError):
    """The sample cannot safely expose agent-controlled execution."""


class DatabaseActionLimitError(RuntimeError):
    """One database action was reclaimed without invalidating the sealed sample."""


def command(args: list[str], *, data: str | None = None, timeout: int = 45) -> str:
    result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        # Do not put paths, environment variables or credentials in tool errors.
        raise IsolationError(f"Controller operation failed ({Path(args[0]).name}, exit {result.returncode}).")
    return result.stdout


def inspect_container(name: str) -> dict:
    return json.loads(command(["docker", "inspect", name]))[0]


# Database backends and parallel/autovacuum workers share the server executable
# and are forked by the server itself, never by the agent's action. Anomaly
# injectors may also leave long-lived workers that spawn short-lived children
# while the agent runs. Neither is an agent-controlled escape. A process the
# agent orphaned to dodge the per-action execution token instead reparents to
# the container init (ppid 1); only that case stays fail-closed.
DB_SERVER_BINARIES = ("postgres", "postmaster", "mysqld", "mariadbd")


ESCAPE_FILTER = r"""
import json,os,sys
suspects=json.loads(sys.argv[1])
before=set(json.loads(sys.argv[2]))
server=set(json.loads(sys.argv[3]))
def info(pid):
    base="/proc/"+pid
    exe=os.path.basename(os.readlink(base+"/exe").removesuffix(" (deleted)"))
    ppid=open(base+"/stat").read().rsplit(")",1)[1].split()[1]
    return exe,ppid
live={}
for pid in suspects:
    try:live[pid]=info(pid)
    except (FileNotFoundError,ProcessLookupError,PermissionError):continue
roots={p for p in before if p!="1"}
allowed=set()
changed=True
while changed:
    changed=False
    for pid,(exe,ppid) in live.items():
        if pid in allowed:continue
        if exe in server or ppid in roots or ppid in allowed:
            allowed.add(pid);changed=True
print(json.dumps([[pid,live[pid][0],live[pid][1]] for pid in live if pid not in allowed]))
"""


class DatabaseSandbox:
    def __init__(self, container: str, project_root: Path, owner_prefix: str):
        self.container = container
        self.project_root = project_root.resolve()
        self.owner_prefix = owner_prefix
        self.identity: str | None = None
        # Shared Claude metadata still reads this compatibility field.
        self.hidden: list[str] = []
        self.sealed = False

    def _identity(self) -> dict:
        info = inspect_container(self.container)
        if self.identity and info["Id"] != self.identity:
            raise IsolationError("Sample container identity changed.")
        if not info["State"]["Running"]:
            raise IsolationError("Sample container is not running.")
        return info

    def seal(self) -> dict:
        from Baselines.container_boundary import seal
        if not self.container.startswith(self.owner_prefix + "_"):
            raise IsolationError("Container does not belong to this Codex run.")
        state = seal(self.container)
        self.identity = state["container_id"]
        # Preserve Codex's existing no-external-network boundary. Native SQL
        # baselines retain their original transport in the shared harness.
        for network in self._identity()["NetworkSettings"]["Networks"]:
            if network != "none":
                command(["docker", "network", "disconnect", network, self.container])
        self.sealed = True
        self.verify()
        return {"sealed": True, "container_id": self.identity,
                "boundary_version": state["version"], "hidden_mount_count": 0,
                "runtime_scratch": [], "external_network": False, "source_references": False}

    def verify(self) -> None:
        from Baselines.container_boundary import verify
        if not self.sealed:
            raise IsolationError("Database tools require a sealed sample.")
        if any(n != "none" for n in self._identity()["NetworkSettings"]["Networks"]):
            raise IsolationError("External network was reattached.")
        verify(self.container)

    def _execution_processes(self, token: str, *, terminate: bool) -> list[int]:
        script = r"""
import json,os,signal,sys,time
needle=("CODEX_EXECUTION_ID="+sys.argv[1]).encode()
mine={os.getpid(),os.getppid()}
def found():
    rows=[]
    for name in os.listdir("/proc"):
        if not name.isdigit() or int(name) in mine:continue
        try:
            if needle in open("/proc/"+name+"/environ","rb").read().split(b"\0"):
                rows.append(int(name))
        except (FileNotFoundError,ProcessLookupError,PermissionError):continue
    return rows
rows=found()
if sys.argv[2]=="1":
    for sig in (signal.SIGTERM,signal.SIGKILL):
        for pid in rows:
            try:os.kill(pid,sig)
            except ProcessLookupError:pass
        time.sleep(0.25)
        rows=found()
        if not rows:break
print(json.dumps(rows))
"""
        from Baselines.container_boundary import _namespace_prefix
        return json.loads(command(_namespace_prefix(self.container) + [
            "python3", "-c", script, token, "1" if terminate else "0"]))

    def _process_snapshot(self) -> dict[str, str]:
        script = r"""
import json,os
mine={os.getpid(),os.getppid()}
rows={}
for name in os.listdir("/proc"):
    if not name.isdigit() or int(name) in mine:continue
    try:
        fields=open("/proc/"+name+"/stat").read().rsplit(")",1)[1].split()
        if fields[0]=="Z":continue
        rows[name]=fields[19]
    except (FileNotFoundError,ProcessLookupError,PermissionError):continue
print(json.dumps(rows))
"""
        return json.loads(command([
            "docker", "exec", "-w", "/", self.container, "python3", "-c", script,
        ]))

    def _filter_escaped(self, untracked: list[str], before: dict[str, str]) -> list[list[str]]:
        """Reduce newly seen PIDs to genuine agent escapes.

        Database-server forks and children of pre-existing (anomaly) workers are
        legitimate; a process the agent orphaned past the execution token shows
        up reparented to the container init and is reported here.
        """
        if not untracked:
            return []
        from Baselines.container_boundary import _namespace_prefix
        return json.loads(command(_namespace_prefix(self.container) + [
            "python3", "-c",
            ESCAPE_FILTER, json.dumps(list(untracked)), json.dumps(list(before)),
            json.dumps(list(DB_SERVER_BINARIES)),
        ]))

    def _stop_sample(self, message: str) -> None:
        self.sealed = False
        command(["docker", "rm", "-f", self.container])
        raise IsolationError(message)

    def execute(self, script: str, log: Path, timeout: int) -> dict:
        self.verify()
        before = self._process_snapshot()
        token = uuid.uuid4().hex
        # stdin/stdout are pipes: no output or action directory is mounted back.
        from Baselines.container_boundary import agent_prefix
        args = agent_prefix(self.container) + [
                "env", "CODEX_EXECUTION_ID=" + token,
                "timeout", "--signal=TERM", "--kill-after=5s", str(timeout), "bash", "-s"]
        result = bounded_process(args, script, log, timeout + 10)
        remaining = self._execution_processes(token, terminate=True)
        if remaining:
            self._stop_sample("Database action cleanup could not reclaim all owned processes; sample stopped.")
        deadline = time.monotonic() + 5
        escaped: list[list[str]] = []
        while True:
            after = self._process_snapshot()
            untracked = sorted(
                pid for pid, started in after.items()
                if before.get(pid) != started
            )
            escaped = self._filter_escaped(untracked, before)
            if not escaped or time.monotonic() >= deadline:
                break
            time.sleep(0.25)
        if escaped:
            self._stop_sample(
                "Database action left an untracked process; sample stopped."
            )
        if result["terminated"] or result["returncode"] in (124, 137):
            self.verify()
            raise DatabaseActionLimitError(
                "Database action exceeded its resource/time budget; that action was terminated "
                "and the sealed sample remains available."
            )
        return result


def bounded_process(args: list[str], script: str, log: Path, timeout: int,
                    max_bytes: int = 16 * 1024 * 1024) -> dict:
    """Stream all output to a private log; stop instead of silently truncating."""
    log.parent.mkdir(parents=True, exist_ok=True)
    count, terminated, reason = 0, False, None
    with tempfile.TemporaryFile() as source, log.open("xb") as target:
        source.write(script.encode("utf-8"))
        source.seek(0)
        process = subprocess.Popen(args, stdin=source, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            with selectors.DefaultSelector() as poll:
                poll.register(process.stdout, selectors.EVENT_READ)
                deadline = time.monotonic() + timeout
                while poll.get_map():
                    if time.monotonic() >= deadline:
                        terminated, reason = True, "timeout"
                        break
                    for key, _ in poll.select(min(0.25, max(0, deadline - time.monotonic()))):
                        chunk = os.read(key.fileobj.fileno(), 65536)
                        if not chunk:
                            poll.unregister(key.fileobj)
                            continue
                        if count + len(chunk) > max_bytes:
                            terminated, reason = True, "output_limit"
                            break
                        target.write(chunk)
                        count += len(chunk)
                    if terminated:
                        break
                if terminated:
                    os.killpg(process.pid, signal.SIGKILL)
                code = process.wait(timeout=10)
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
            process.stdout.close()
    return {"returncode": code, "bytes": count, "terminated": terminated,
            "complete": not terminated, "termination_reason": reason}


def analysis_execute(workspace: Path, script: str, language: str, timeout: int,
                     log: Path, image: str = "pg_diag") -> dict:
    """Read-only evidence mount; ephemeral 64 MiB scratch; no host credentials."""
    name = "codex_analysis_" + uuid.uuid4().hex
    args = [
        "docker", "run", "--rm", "-i", "--name", name, "--label", "codex.analysis=1",
        "--network", "none", "--read-only", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges:true", "--pids-limit", "64",
        "--memory", "256m", "--memory-swap", "256m", "--cpus", "1",
        "--ulimit", "nofile=128:128", "--ulimit", "fsize=16777216:16777216",
        "--user", f"{os.getuid()}:{os.getgid()}",
        "--mount", f"type=bind,source={workspace},target=/workspace,readonly",
        "--tmpfs", "/scratch:rw,nosuid,nodev,noexec,size=67108864,mode=1777",
        "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=16777216,mode=1777",
        "--workdir", "/scratch", "--entrypoint", "/usr/bin/env", image,
        "-i", "PATH=/opt/venv/bin:/usr/bin:/bin", "HOME=/scratch",
        "TMPDIR=/scratch", "PYTHONDONTWRITEBYTECODE=1",
    ]
    args += ["python3", "-I", "-"] if language == "python" else ["bash", "--noprofile", "--norc", "-s"]
    try:
        return bounded_process(args, script, log, timeout)
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)
