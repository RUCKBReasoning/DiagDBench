"""Controller-only Docker isolation. No command allowlist is a security boundary.

Database initialization/injection remains in Baselines.Codex.driver. Only after it
finishes, hide host bind mounts in that SAME container, reject outstanding source
references, and disconnect external networks. SQL file APIs then see the same
sealed filesystem as Bash. Failure aborts the sample, never falls back unsealed.
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


# Executed by the trusted host controller before the agent starts. Reject source descriptors
# and alternate mount namespaces instead of killing injection workers or silently
# changing their behavior. /proc itself remains available for normal diagnostics.
RUNTIME_SCRATCH_AUDIT = r"""
import os,sys,json
roots=json.loads(sys.argv[1])
leader=sys.argv[2]
pidns=os.readlink("/proc/"+leader+"/ns/pid")
def under(path, root):
    return path==root or path.startswith(root+"/")
rows=[]
for pid in os.listdir("/proc"):
    if not pid.isdigit():continue
    base="/proc/"+pid
    try:
        if os.readlink(base+"/ns/pid")!=pidns:continue
        exe=os.path.basename(os.readlink(base+"/exe").removesuffix(" (deleted)"))
        if exe not in ("mysqld","mariadbd"):continue
        args=open(base+"/cmdline","rb").read().decode(errors="replace").split("\0")
        tmpdirs=[]
        for i,arg in enumerate(args):
            if arg.startswith("--tmpdir="):tmpdirs.append(arg.split("=",1)[1])
            elif arg=="--tmpdir" and i+1<len(args):tmpdirs.append(args[i+1])
        start=open(base+"/stat").read().rsplit(")",1)[1].split()[19]
        for path in tmpdirs:
            real=os.path.realpath(path)
            if os.path.isabs(real) and any(under(real,r) and real!=r for r in roots):
                rows.append({"pid":pid,"start":start,"path":real})
    except (FileNotFoundError,ProcessLookupError,PermissionError):continue
print(json.dumps(rows))
"""


REFERENCE_AUDIT = r"""
import os,sys,json
roots=json.loads(sys.argv[1])
leader=sys.argv[2]
allowed=json.loads(sys.argv[3])
pidns=os.readlink("/proc/"+leader+"/ns/pid")
def sensitive(path):
    path=path.removesuffix(" (deleted)")
    return any(path==r or path.startswith(r+"/") for r in roots)
def approved(pid,name,target):
    row=allowed.get(pid)
    if not row or not name.startswith("fd/"):return False
    try:start=open("/proc/"+pid+"/stat").read().rsplit(")",1)[1].split()[19]
    except (FileNotFoundError,ProcessLookupError,PermissionError):return False
    target=target.removesuffix(" (deleted)")
    return start==row["start"] and any(target==p or target.startswith(p+"/") for p in row["paths"])
own=os.readlink("/proc/"+leader+"/ns/mnt")
bad=[]
for pid in os.listdir("/proc"):
    if not pid.isdigit():continue
    base="/proc/"+pid
    try:
        if os.readlink(base+"/ns/pid")!=pidns:continue
        if os.readlink(base+"/ns/mnt")!=own:
            bad.append([pid,"mount_namespace"]);continue
        names=[base+"/cwd",base+"/root",base+"/exe"]
        names.extend(base+"/fd/"+f for f in os.listdir(base+"/fd"))
        for name in names:
            try: target=os.readlink(name)
            except FileNotFoundError: continue
            label="/".join(name.rsplit("/",2)[-2:])
            if sensitive(target) and not approved(pid,label,target):bad.append([pid,label])
        with open(base+"/maps") as f:
            if any(any(r+"/" in row for r in roots) for row in f):
                bad.append([pid,"memory_mapping"])
    except (FileNotFoundError,ProcessLookupError):continue
    except PermissionError:bad.append([pid,"unverifiable"])
print(json.dumps({"ok":not bad,"references":bad}))
"""


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
        self.hidden: list[str] = []
        self.runtime_scratch: list[str] = []
        self.reference_exceptions: dict[str, dict] = {}
        self.sealed = False

    def _identity(self) -> dict:
        info = inspect_container(self.container)
        if self.identity and info["Id"] != self.identity:
            raise IsolationError("Sample container identity changed.")
        if not info["State"]["Running"]:
            raise IsolationError("Sample container is not running.")
        return info

    def _references(self) -> dict:
        info = self._identity()
        privilege = [] if os.geteuid() == 0 else ["sudo", "-n"]
        result = json.loads(command(
            privilege + [sys.executable, "-c", REFERENCE_AUDIT, json.dumps(self.hidden),
                         str(info["State"]["Pid"]), json.dumps(self.reference_exceptions)]
        ))
        if not result["ok"]:
            raise IsolationError(
                "Isolation blocked: a live process retains a source file, directory, "
                "mapping, or an unverifiable mount namespace. The anomaly was not reset. "
                + json.dumps(result["references"])
            )
        return result

    def _discover_runtime_scratch(self) -> None:
        info = self._identity()
        privilege = [] if os.geteuid() == 0 else ["sudo", "-n"]
        rows = json.loads(command(
            privilege + [sys.executable, "-c", RUNTIME_SCRATCH_AUDIT,
                         json.dumps(self.hidden), str(info["State"]["Pid"])]
        ))
        by_pid: dict[str, dict] = {}
        for row in rows:
            path = str(Path(row["path"]))
            if not any(path.startswith(root + "/") for root in self.hidden):
                raise IsolationError("Unsafe database runtime scratch path.")
            self.runtime_scratch.append(path)
            entry = by_pid.setdefault(row["pid"], {"start": row["start"], "paths": []})
            entry["paths"].append(path)
        self.runtime_scratch = sorted(set(self.runtime_scratch))
        self.reference_exceptions = by_pid

    def _mount(self, *args: str) -> None:
        info = self._identity()
        privilege = [] if os.geteuid() == 0 else ["sudo", "-n"]
        command(privilege + [
            "nsenter", "-t", str(info["State"]["Pid"]), "-m", "--root", "--wd=/", "--",
            *args,
        ])

    def _hide_sources(self) -> None:
        for root in self.hidden:
            self._mount("/bin/mount", "-t", "tmpfs", "-o",
                        "rw,nosuid,nodev,noexec,size=1048576", "codex_hidden", root)
        for index, target in enumerate(self.runtime_scratch):
            source = f"/run/codex_runtime/{index}"
            self._mount("/bin/mkdir", "-p", source, target)
            self._mount("/bin/chmod", "1777", source, target)
            self._mount("/bin/mount", "--bind", source, target)
            self._mount("/bin/mount", "-o", "remount,bind,rw,nosuid,nodev,noexec", source, target)
        for root in self.hidden:
            self._mount("/bin/mount", "-o", "remount,ro,nosuid,nodev,noexec", root)

    def seal(self) -> dict:
        if not self.container.startswith(self.owner_prefix + "_"):
            raise IsolationError("Container does not belong to this Codex run.")
        info = self._identity()
        self.identity = info["Id"]
        hc = info["HostConfig"]
        if (hc.get("Privileged") or hc.get("PidMode") or hc.get("IpcMode") == "host"
                or hc.get("CapAdd") or hc.get("Devices") or hc.get("VolumesFrom")
                or hc.get("NetworkMode") == "host"
                or any("unconfined" in x for x in hc.get("SecurityOpt") or [])):
            raise IsolationError("Unexpected container privileges; refusing to expose tools.")
        # Image ENV may contain DB settings, but never a model/API credential.
        for value in info["Config"].get("Env") or []:
            key = value.partition("=")[0].upper()
            if any(word in key for word in ("API_KEY", "ACCESS_TOKEN", "AUTH_TOKEN", "SECRET")):
                raise IsolationError("Unexpected secret in database container environment.")
        binds = [m for m in info["Mounts"] if m["Type"] == "bind"]
        if not any(Path(m["Source"]).resolve() == self.project_root for m in binds):
            raise IsolationError("Expected original project mount is missing.")
        destinations = sorted({m["Destination"].rstrip("/") for m in binds}, key=len)
        for dst in destinations:
            if not dst.startswith("/") or dst in {"/", "/usr", "/bin", "/sbin", "/etc", "/proc", "/sys", "/dev", "/var"}:
                raise IsolationError("Unsafe host bind mount layout.")
        self.hidden = [d for d in destinations if not any(d.startswith(p + "/") for p in destinations if p != d)]
        self._discover_runtime_scratch()
        self._references()
        # Enter only this container's mount namespace. No host mount is changed.
        # MySQL's configured tmpdir can live below a source bind. Recreate only
        # that empty runtime path on the container layer before making the
        # source-covering tmpfs read-only.
        self._hide_sources()
        for network in info["NetworkSettings"]["Networks"]:
            if network != "none":
                command(["docker", "network", "disconnect", network, self.container])
        self._references()
        self.sealed = True
        self.verify()
        return {"sealed": True, "container_id": self.identity, "hidden_mount_count": len(self.hidden),
                "external_network": False, "source_references": False,
                "runtime_scratch": self.runtime_scratch}

    def verify(self) -> None:
        if not self.sealed:
            raise IsolationError("Database tools require a sealed sample.")
        info = self._identity()
        if any(n != "none" for n in info["NetworkSettings"]["Networks"]):
            raise IsolationError("External network was reattached.")
        check = r"""
import json,os,sys
roots=json.loads(sys.argv[1])
scratch=json.loads(sys.argv[2])
rows=open("/proc/self/mountinfo").read().splitlines()
for root in roots:
    matches=[r for r in rows if r.split()[4]==root]
    # A tmpfs overmount must be read-only and contain only structural
    # directories leading to explicitly approved runtime scratch mounts.
    assert matches and any(" - tmpfs codex_hidden " in r and "ro" in r.split()[5].split(",") for r in matches)
    for name in os.listdir(root):
        path=root+"/"+name
        assert any(s==path or s.startswith(path+"/") for s in scratch)
for path in scratch:
    matches=[r for r in rows if r.split()[4]==path]
    assert matches and any("rw" in r.split()[5].split(",") and "noexec" in r.split()[5].split(",") for r in matches)
    probe=path+"/.codex_probe"
    open(probe,"w").write("sealed")
    os.unlink(probe)
status=dict(x.split(":",1) for x in open("/proc/self/status") if ":" in x)
assert not (int(status["CapBnd"].strip(),16)&(1<<21))
print("sealed")
"""
        command(["docker", "exec", "-w", "/", self.container, "python3", "-c", check,
                 json.dumps(self.hidden), json.dumps(self.runtime_scratch)])

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
        return json.loads(command([
            "docker", "exec", "-w", "/", self.container, "python3", "-c", script,
            token, "1" if terminate else "0",
        ]))

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
        return json.loads(command([
            "docker", "exec", "-u", "0", "-w", "/", self.container, "python3", "-c",
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
        args = ["docker", "exec", "-i", "-u", "0", "-w", "/", self.container,
                "env", "PAGER=cat", "PSQL_PAGER=cat", "BASH_ENV=/dev/null",
                "CODEX_EXECUTION_ID=" + token,
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
