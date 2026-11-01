"""Trusted sample staging and filesystem boundary for diagnosis and recovery.

No project/output/TMP bind is exposed. Trusted injection retains a private mount
namespace so hiding its controllers cannot accidentally heal the incident.
Model-controlled commands run without OS privilege; DB credentials are unchanged.

Only the harness reads the external sample source. Complete samples are staged
at <project>/DiagDBench/<sample_id>; diagnosis receives four public files only.
Staging preserves SQL, injection content, diagnosis prompts and scoring.
"""
from __future__ import annotations

from contextvars import ContextVar
import fcntl
import functools
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import stat
import subprocess
import sys
import threading
import time
import logging
import uuid

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_NAMES = frozenset({
    "config.json", "initialization_workload.sql",
    "user_phenomenon_description.txt", "verified_workload.sql",
})
PUBLIC_KEYS = frozenset({"config", "initialization", "phenomenon", "verified"})
CURRENT_STAGE = ContextVar("dbdiag_sample_stage", default=None)

VERSION = "2026-10-29-db-only-v2"
ROOT = Path(__file__).resolve().parents[1]
LABEL = "io.diagdb.boundary"
AGENT_UID = 65532
VAULT = "/run/.dbdiag_control"
_locks = {}
_guard = threading.Lock()


class BoundaryError(RuntimeError):
    """Infrastructure failure: never convert into a scored diagnosis failure."""


# Trusted setup/probes only. Model SQL/action and scoring deadlines are untouched.
BOUNDARY_OPERATION_TIMEOUT = 180
BOUNDARY_READ_ONLY_ATTEMPTS = 2


def _run(args, *, data=None, timeout=BOUNDARY_OPERATION_TIMEOUT,
         retry_readonly=False, operation="trusted boundary operation"):
    attempts = BOUNDARY_READ_ONLY_ATTEMPTS if retry_readonly else 1
    for attempt in range(1, attempts + 1):
        try:
            result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            if attempt < attempts:
                logging.getLogger(__name__).warning(
                    "%s timed out after %ss; retrying read-only probe (%s/%s)",
                    operation, timeout, attempt + 1, attempts)
                time.sleep(2)
                continue
            raise BoundaryError(
                f"Boundary infrastructure operation could not complete: {operation}; "
                f"TimeoutExpired after {attempt} attempt(s), {timeout}s per attempt") from exc
        except OSError as exc:
            raise BoundaryError("Boundary infrastructure operation could not complete: "
                                + operation + "; " + type(exc).__name__) from exc
        if result.returncode:
            raise BoundaryError("Boundary operation failed: " + operation + ": "
                                + Path(args[0]).name + ": " + result.stderr[-1800:])
        return result.stdout


def read_only_probe(args, *, operation):
    """Retry only explicitly trusted, non-mutating boundary inspection commands.

    A failed invariant/nonzero command never retries or permits an action. Setup,
    mounts, injection, submitted scripts and SQL must not use this function.
    """
    return _run(args, retry_readonly=True, operation=operation)


def info(container):
    try:
        return json.loads(read_only_probe(["docker", "inspect", container], operation="container identity inspect"))[0]
    except (ValueError, IndexError) as exc:
        raise BoundaryError("Cannot verify container identity.") from exc


def managed(container):
    return info(container)["Config"].get("Labels", {}).get(LABEL) == VERSION


def validate_container_privileges(metadata):
    host = metadata["HostConfig"]
    if (host.get("Privileged") or host.get("PidMode") or host.get("IpcMode") == "host"
            or host.get("CapAdd") or host.get("Devices") or host.get("VolumesFrom")
            or host.get("NetworkMode") == "host"
            or any("unconfined" in x for x in host.get("SecurityOpt") or [])):
        raise BoundaryError("Unexpected container privileges.")
    for value in metadata["Config"].get("Env") or []:
        key = value.partition("=")[0].upper()
        if any(word in key for word in ("API_KEY", "ACCESS_TOKEN", "AUTH_TOKEN", "SECRET")):
            raise BoundaryError("Model/API secrets are forbidden in database containers.")


def _state_path(container):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,180}", container):
        raise BoundaryError("Unsafe container name.")
    directory = ROOT.parent / ".dbdiag_boundary_state"
    if directory.is_symlink():
        raise BoundaryError("Boundary state directory must not be a symlink.")
    directory.mkdir(mode=0o700, exist_ok=True)
    return directory / (container + ".json")


def _read(container):
    p = _state_path(container)
    return json.loads(p.read_text()) if p.is_file() else {}


def _write(container, state):
    p = _state_path(container)
    temporary = p.with_name("." + p.name + "." + uuid.uuid4().hex)
    temporary.write_text(json.dumps(state))
    temporary.chmod(0o600)
    temporary.replace(p)


def _exec(container, code, payload=None, *, privileged=False,
          timeout=BOUNDARY_OPERATION_TIMEOUT, retry_readonly=False,
          operation="trusted boundary setup"):
    args = (_namespace_prefix(container) if privileged else ["docker", "exec", "-i", container])
    wrapped = "import sys; sys.argv=['boundary',sys.stdin.read()]; exec(" + repr(code) + ")"
    args += ["python3", "-c", wrapped]
    return _run(args, data=json.dumps(payload), timeout=timeout,
                retry_readonly=retry_readonly, operation=operation)


HOST_ENTER = r"""
import json,os,pathlib,sys
p=json.loads(sys.argv[1]);pid=int(p['pid'])
assert pid>1
target=pathlib.Path('/proc',str(pid),'cgroup').read_text()
rows=[line.split(':',2) for line in target.splitlines()]
for hierarchy,controllers,path in rows:
 if not controllers:
  dest=pathlib.Path('/sys/fs/cgroup')/path.lstrip('/')/'cgroup.procs'
 else:
  options=set(controllers.split(','))
  matches=[]
  for line in pathlib.Path('/proc/self/mountinfo').read_text().splitlines():
   left,right=line.split(' - ',1);r=right.split();l=left.split()
   if r[0]=='cgroup' and options<=set(r[2].split(',')):
    base=l[3].rstrip('/')
    if base and not (path==base or path.startswith(base+'/')):continue
    matches.append(pathlib.Path(l[4])/path[len(base):].lstrip('/')/'cgroup.procs')
  if len(matches)!=1:raise RuntimeError('Cannot resolve container cgroup hierarchy')
  dest=matches[0]
 dest.write_text(str(os.getpid()))
if pathlib.Path('/proc/self/cgroup').read_text()!=target:
 raise RuntimeError('Trusted namespace worker is not in the database cgroup')
args=['/usr/bin/nsenter','--target',str(pid),'--mount','--pid','--net','--ipc','--uts',
      '--root','--wd=/','--']+p['command']
os.execve(args[0],args,p['environment'])
"""


def _container_environment(metadata):
    """Use the image's interpreter search path, never the host/sudo environment.

    Namespace entry does not inherit Docker's configured environment. In
    particular, mysql_diag installs connectors in /opt/venv. Keep only generic
    runtime settings; database arguments are supplied explicitly by the harness.
    """
    allowed = {"PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "VIRTUAL_ENV"}
    environment = {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME": "/root",
    }
    for entry in metadata["Config"].get("Env") or []:
        key, separator, value = entry.partition("=")
        if separator and key in allowed:
            if "\0" in value:
                raise BoundaryError("Invalid container runtime environment.")
            environment[key] = value
    # Do not search a sample's cwd for trusted bootstrap executables.
    if any(not part.startswith("/") for part in environment["PATH"].split(":")):
        raise BoundaryError("Container runtime PATH must contain absolute directories only.")
    return environment


def _namespace_prefix(container):
    metadata = info(container)
    if not metadata["State"].get("Running"):
        raise BoundaryError("Cannot enter a stopped container.")
    # Supply the command through a tiny host-side argv trampoline. No project
    # Python module executes as host root, and the target cgroup is verified.
    privilege = [] if os.geteuid() == 0 else ["sudo", "-n", "--"]
    payload = {"pid": int(metadata["State"]["Pid"]),
               "environment": _container_environment(metadata)}
    trampoline = (
        "import json,sys; p=json.loads(sys.argv[1]); p['command']=sys.argv[2:]; "
        "sys.argv=['helper',json.dumps(p)]; exec(" + repr(HOST_ENTER) + ")"
    )
    return privilege + ["/usr/bin/python3", "-c", trampoline, json.dumps(payload)]


def scoped_run_command(command, config):
    """Keep resource flags unchanged; allow only a sample and RO generic data."""
    sample = config.get("_sample_staging_dir")
    if not sample:
        return command  # Synthesis/maintenance is not a diagnosis execution.
    sample = Path(sample)
    expected = ROOT.resolve() / "DiagDBench"
    if sample.parent != expected or sample.is_symlink() or not sample.is_dir():
        raise BoundaryError("Invalid per-sample staging directory.")
    result = []
    index = 0
    while index < len(command):
        value = command[index]
        if value in {"-v", "--volume", "--mount", "--tmpfs"}:
            index += 2
            continue
        if value.startswith(("--volume=", "--mount=", "--tmpfs=")):
            index += 1
            continue
        result.append(value)
        index += 1
    assets = ROOT.resolve() / "Database_Instances"
    if not assets.is_dir():
        raise BoundaryError("Generic database instances are unavailable.")
    flags = [
        "--label", LABEL + "=" + VERSION,
        "--label", "io.diagdb.sample=" + sample.name,
        "--mount", f"type=bind,source={sample},target={sample}",
        "--mount", f"type=bind,source={assets},target={assets},readonly",
    ]
    return result[:2] + flags + result[2:]


def copy_trusted_file(container, path):
    """Copy only an explicitly selected harness file, never a directory."""
    path = Path(path).resolve(strict=True)
    if not path.is_file():
        raise BoundaryError("Expected one regular harness file.")
    target = str(path)
    _run(["docker", "exec", container, "mkdir", "-p", str(path.parent)])
    _run(["docker", "cp", str(path), container + ":" + target], operation="trusted explicit file copy")
    return target


def prepare_bootstrap(container, command):
    if not managed(container):
        return
    for argument in command:
        p = Path(argument)
        if p.is_absolute() and p.is_file() and (
                str(p).startswith(str(ROOT / "Docker") + "/")
                or str(p).startswith(str(ROOT / "Scripts") + "/")):
            copy_trusted_file(container, p)


def prepare_database_socket_access(container):
    """Allow an unprivileged client to reach existing DB sockets before injection.

    Add directory search only, not data-file read permission or a DB OS group.
    Never run after injection: a permission fault must not be silently repaired.
    """
    if not managed(container):
        return
    _exec(container, r"""
import os,pathlib,stat
for name in ('/run/mysqld/mysqld.sock','/var/run/mysqld/mysqld.sock',
             '/var/run/postgresql/.s.PGSQL.5432'):
 p=pathlib.Path(name)
 if not p.exists() or not stat.S_ISSOCK(p.stat().st_mode):continue
 for parent in p.resolve().parents:
  mode=parent.stat().st_mode
  if not mode & stat.S_IXOTH:parent.chmod(stat.S_IMODE(mode)|stat.S_IXOTH)
""")
    metadata = info(container)
    _write(container, {"version": VERSION, "container_id": metadata["Id"],
                       "phase": "initialized", "before": json.loads(_exec(container, SNAPSHOT))})


SNAPSHOT = r"""
import json,os,stat,sys
roots=('/tmp','/var/tmp','/run','/root','/home','/opt')
out={}
for root in roots:
 if not os.path.isdir(root):continue
 for base,dirs,files in os.walk(root):
  dirs[:]=[d for d in dirs if not (base+'/'+d).startswith('/run/.dbdiag_control')]
  for name in dirs+files:
   p=base+'/'+name
   try:
    s=os.lstat(p)
    out[p]=[s.st_mode,s.st_ino,s.st_size,s.st_mtime_ns]
   except FileNotFoundError:pass
print(json.dumps(out))
"""


BOOTSTRAP = r"""
import json,os,pathlib,resource,shutil,subprocess,sys,time
p=json.loads(sys.argv[1])
# nsenter/sudo inherits host limits, not the configured Docker-exec limits.
# Restore the ordinary container's exact NOFILE pair while still in the fixed
# trusted bootstrap. No capability or resource allowance exceeds Docker exec.
resource.setrlimit(resource.RLIMIT_NOFILE, tuple(p['nofile']))
assert list(resource.getrlimit(resource.RLIMIT_NOFILE)) == p['nofile']
vault=pathlib.Path('/run/.dbdiag_control')
vault.mkdir(mode=0o700,exist_ok=False)
sample=vault/'sample'
shutil.copytree(p['sample'],sample,symlinks=False)
subprocess.run(['mount','--bind',str(sample),p['sample']],check=True)
holder=os.fork()
if holder==0:
 os.setsid()
 for fd in (0,1,2):
  try:os.close(fd)
  except OSError:pass
 # Do not retain the privileged bootstrap's inline Python in /proc/cmdline.
 os.execv('/bin/sleep',['sleep','infinity'])
(vault/'holder').write_text(str(holder))
# Trusted namespace setup ends here. Never give the injection more capability
# than an ordinary Docker exec; keep su/setpriv behavior needed by fixtures.
caps=p['capabilities']
args=['setpriv','--bounding-set='+caps,'--inh-caps=-all','--ambient-caps=-all','--']
os.execvp(args[0],args+p['command'])
"""


def injection_argv(container, command):
    with _guard:
        lock = _locks.setdefault(container, threading.RLock())
    with lock:
        metadata = info(container)
        if metadata["Config"].get("Labels", {}).get(LABEL) != VERSION:
            return ["docker", "exec", "-i", container, *command]
        state = _read(container)
        if (state and (state.get("phase") != "initialized"
                       or state.get("container_id") != metadata["Id"])):
            raise BoundaryError("Injection requires a new, unsealed container.")
        sample = ROOT.resolve() / "DiagDBench" / metadata["Config"]["Labels"]["io.diagdb.sample"]
        # Real runs snapshot before data_modification.sql as well as injection.
        # The fallback supports the synthetic boundary-only canary.
        before = state["before"] if state else json.loads(_exec(container, SNAPSHOT))
        # The ordinary container's exact capability bounding set is retained.
        caphex = _run(["docker", "exec", container, "sh", "-c",
                       "awk '/^CapBnd:/ {print $2}' /proc/self/status"]).strip()
        names = ["chown", "dac_override", "dac_read_search", "fowner", "fsetid", "kill",
                 "setgid", "setuid", "setpcap", "linux_immutable", "net_bind_service",
                 "net_broadcast", "net_admin", "net_raw", "ipc_lock", "ipc_owner",
                 "sys_module", "sys_rawio", "sys_chroot", "sys_ptrace", "sys_pacct",
                 "sys_admin", "sys_boot", "sys_nice", "sys_resource", "sys_time",
                 "sys_tty_config", "mknod", "lease", "audit_write", "audit_control",
                 "setfcap", "mac_override", "mac_admin", "syslog", "wake_alarm",
                 "block_suspend", "audit_read", "perfmon", "bpf", "checkpoint_restore"]
        capmask = int(caphex, 16)
        caps = "-all" + "".join(",+" + n for i, n in enumerate(names) if capmask & (1 << i))
        _write(container, {"version": VERSION, "container_id": metadata["Id"],
                           "phase": "injecting", "before": before, "sample": str(sample)})
        nofile = json.loads(_run(["docker", "exec", container, "python3", "-c",
            "import json,resource; print(json.dumps(resource.getrlimit(resource.RLIMIT_NOFILE)))"]))
        if (not isinstance(nofile, list) or len(nofile) != 2
                or any(isinstance(x, bool) or not isinstance(x, int) for x in nofile)):
            raise BoundaryError("Cannot verify ordinary Docker-exec file limits.")
        payload = {"sample": str(sample), "command": command, "capabilities": caps,
                   "nofile": nofile}
        # Only this fixed trusted bootstrap is privileged. It first creates a
        # private mount namespace, then drops back to the original cap set.
        return _namespace_prefix(container) + [
                "unshare", "--mount", "--propagation", "private", "--",
                "python3", "-c", BOOTSTRAP, json.dumps(payload)]


SEAL = r"""
import json,os,pathlib,signal,stat,subprocess,sys,time
p=json.loads(sys.argv[1]); vault=pathlib.Path('/run/.dbdiag_control')
leader=int((vault/'holder').read_text())
private_ns=os.readlink('/proc/'+str(leader)+'/ns/mnt')
main_ns=os.readlink('/proc/self/ns/mnt')
assert private_ns!=main_ns
before=p['before']; roots=('/tmp','/var/tmp','/run','/root','/home','/opt')
new=[]
for root in roots:
 if not os.path.isdir(root):continue
 for base,dirs,files in os.walk(root):
  dirs[:]=[d for d in dirs if not (base+'/'+d).startswith(str(vault))]
  for name in dirs+files:
   path=base+'/'+name
   if not path.startswith(str(vault)):
    try:
     st=os.lstat(path);mode=st.st_mode
     old=before.get(path)
     changed_file=(old is not None and stat.S_ISREG(mode)
                   and [mode,st.st_ino,st.st_size,st.st_mtime_ns]!=old)
     # DB sockets and PID files are live service state, not injection code.
     if ((old is None or changed_file) and
         (stat.S_ISREG(mode) or stat.S_ISDIR(mode) or stat.S_ISLNK(mode))):new.append(path)
    except FileNotFoundError:pass
new=sorted(new,key=lambda x:(len(pathlib.Path(x).parts),x))
tops=[]
for path in new:
 if not any(path.startswith(a+'/') for a in tops):tops.append(path)
# Do not move a live DB server's runtime scratch or data. Fail closed instead of
# changing the incident. Controllers in the private namespace retain their view.
for pid in os.listdir('/proc'):
 if not pid.isdigit():continue
 try:
  base='/proc/'+pid
  if os.readlink(base+'/ns/mnt')!=main_ns:continue
  exe=os.path.basename(os.readlink(base+'/exe'))
  if exe not in ('postgres','postmaster','mysqld','mariadbd'):continue
  refs=[base+'/cwd']+[base+'/fd/'+x for x in os.listdir(base+'/fd')]
  for ref in refs:
   try:target=os.readlink(ref).removesuffix(' (deleted)')
   except FileNotFoundError:continue
   if any(target==x or target.startswith(x+'/') for x in tops):
    raise RuntimeError('Database runtime references newly created private artifact: '+target)
 except (FileNotFoundError,ProcessLookupError):continue
stopped=[]
try:
 # Freeze only private-namespace controllers, not the DB or existing stopped
 # receivers. Re-scan to include children forked before their parent stopped.
 for _ in range(8):
  changed=False
  for pid in os.listdir('/proc'):
   if not pid.isdigit():continue
   try:
    if os.readlink('/proc/'+pid+'/ns/mnt')!=private_ns:continue
    state=open('/proc/'+pid+'/stat').read().rsplit(')',1)[1].split()[0]
    if state not in ('T','t'):
     os.kill(int(pid),signal.SIGSTOP);stopped.append(int(pid));changed=True
   except (FileNotFoundError,ProcessLookupError):continue
  if not changed:break
  time.sleep(.02)
 quarantine=vault/'quarantine';quarantine.mkdir()
 moved=[]
 for index,path in enumerate(tops):
  src=pathlib.Path(path)
  if not os.path.lexists(src):continue
  dest=quarantine/str(index)
  mode=src.lstat().st_mode
  if stat.S_ISLNK(mode):
   # Preserve the link inside the controller only by masking its parent is
   # preferred; a top-level link cannot be safely rebound as a regular file.
   raise RuntimeError('Unexpected top-level injection symlink: '+path)
  src.rename(dest)
  if stat.S_ISDIR(mode):src.mkdir(mode=0o700)
  else:src.touch(mode=0o600)
  subprocess.run(['nsenter','-t',str(leader),'-m','--','mount','--bind',str(dest),path],check=True)
  # Main/DB namespace gets no code: only empty non-readable placeholders.
  src.chmod(0)
  moved.append(path)
 # The entire vault is hidden from the DB namespace, including via SQL file
 # functions. The controller namespace continues to see its pinned source.
 subprocess.run(['mount','-t','tmpfs','-o','ro,nosuid,nodev,noexec,size=4096',
                 'dbdiag_private_hidden',str(vault)],check=True)
finally:
 for pid in stopped:
  try:os.kill(pid,signal.SIGCONT)
  except ProcessLookupError:pass
print(json.dumps({'leader':leader,'namespace':private_ns,'quarantined':moved}))
"""


def seal(container):
    """Close the full sample and all injection-generated ephemeral artifacts."""
    with _guard:
        lock = _locks.setdefault(container, threading.RLock())
    with lock:
        metadata = info(container)
        if metadata["Config"].get("Labels", {}).get(LABEL) != VERSION:
            raise BoundaryError("Diagnosis requires the current managed boundary.")
        validate_container_privileges(metadata)
        state = _read(container)
        if state.get("phase") == "sealed":
            verify(container)
            verify_database_file_boundary(container)
            return state
        if state.get("container_id") != metadata["Id"] or state.get("phase") != "injecting":
            raise BoundaryError("Missing successful isolated injection context.")
        outcome = json.loads(_exec(container, SEAL, state, privileged=True, timeout=90))
        sample = state["sample"]
        # Bootstrap code and generic raw data are not diagnosis inputs. The data
        # mount is detached only after imports finish; never change resource limits.
        code = r"""
import json,os,pathlib,shutil,subprocess,sys
p=json.loads(sys.argv[1])
for path in p['remove']:
 if os.path.isdir(path):shutil.rmtree(path)
asset=p['asset']
subprocess.run(['umount',asset],check=True)
subprocess.run(['mount','-o','remount,bind,ro',p['sample']],check=True)
work=pathlib.Path('/tmp/dbdiag_agent');work.mkdir(mode=0o700,exist_ok=True)
os.chown(work,65532,65532)
"""
        _exec(container, code, {
            "remove": [str(ROOT / "Docker"), str(ROOT / "Scripts")],
            "asset": str(ROOT.resolve() / "Database_Instances"), "sample": sample,
        }, privileged=True)
        state = {"version": VERSION, "container_id": metadata["Id"], "phase": "sealed",
                 "sample": sample, **outcome}
        _write(container, state)
        try:
            verify(container)
            verify_database_file_boundary(container)
        except BaseException:
            _write(container, dict(state, phase="blocked_isolation"))
            raise
        return state


def verify(container):
    metadata = info(container)
    state = _read(container)
    if state.get("phase") != "sealed" or state.get("container_id") != metadata["Id"]:
        raise BoundaryError("Unsealed or changed container.")
    validate_container_privileges(metadata)
    allowed = {state["sample"], str(ROOT.resolve() / "Database_Instances")}
    for mount in metadata.get("Mounts", []):
        if mount["Type"] == "bind" and mount["Destination"] not in allowed:
            raise BoundaryError("Unexpected host bind mount.")
    code = r"""
import json,os,pathlib,sys
p=json.loads(sys.argv[1])
expected={'config.json','initialization_workload.sql','user_phenomenon_description.txt','verified_workload.sql'}
assert set(os.listdir(p['sample']))==expected
assert not list(pathlib.Path('/run/.dbdiag_control').iterdir())
rows=open('/proc/self/mountinfo').read().splitlines()
assert any(r.split()[4]==p['sample'] and 'ro' in r.split()[5].split(',') for r in rows)
assert not any(r.split()[4]==p['asset'] for r in rows)
for path in p['quarantined']:
 if os.path.isdir(path):assert not list(pathlib.Path(path).iterdir())
 elif os.path.exists(path):assert os.path.getsize(path)==0
print('sealed')
"""
    _exec(container, code, dict(state, asset=str(ROOT.resolve() / "Database_Instances")),
          retry_readonly=True, operation="sealed sample and mount probe")
    # Check an actual reduced-privilege action cannot follow the controller's
    # /proc root/cwd. SQL backends run as distinct OS users, not container root.
    probe = r"""
import os,sys
assert os.geteuid()==65532
status=dict(l.split(':',1) for l in open('/proc/self/status') if ':' in l)
assert int(status['CapEff'].strip(),16)==0
assert int(status['CapBnd'].strip(),16)==0 and status['NoNewPrivs'].strip()=='1'
for name in ('root','cwd','environ','mem'):
 try:
  p='/proc/'+sys.argv[1]+'/'+name
  if name in ('root','cwd'):os.listdir(p)
  else:open(p,'rb').read(1)
 except (PermissionError,FileNotFoundError,ProcessLookupError):pass
 else:raise RuntimeError('Private controller process is accessible')
print('unprivileged')
"""
    read_only_probe(agent_prefix(container) + ["python3", "-c", probe, str(state["leader"])],
                    operation="unprivileged controller visibility probe")


def verify_database_file_boundary(container):
    """Check SQL server OS identities cannot use /proc aliases to private code."""
    state = _read(container)
    code = r"""
import json,os,pwd,sys
p=json.loads(sys.argv[1]);pidns=p['namespace']
private=[]
for pid in os.listdir('/proc'):
 if not pid.isdigit():continue
 try:
  if os.readlink('/proc/'+pid+'/ns/mnt')==pidns:private.append(pid)
 except (FileNotFoundError,PermissionError,ProcessLookupError):pass
identities={65532}
for name in ('postgres','mysql'):
 try:identities.add(pwd.getpwnam(name).pw_uid)
 except KeyError:pass
for uid in sorted(identities):
 child=os.fork()
 if child==0:
  os.setgroups([]);os.setgid(uid);os.setuid(uid)
  leaked=False
  for pid in private:
   for path in [p['sample']]+p['quarantined']:
    target='/proc/'+pid+'/root'+path
    try:
     if os.path.isdir(target):
      if os.listdir(target):leaked=True
     else:
      with open(target,'rb') as f:
       if f.read(1):leaked=True
    except (OSError,ValueError):pass
  os._exit(2 if leaked else 0)
 _,status=os.waitpid(child,0)
 if status:raise RuntimeError('SQL-server OS identity can follow a private controller root')
print('sql_file_boundary_verified')
"""
    _exec(container, code, state, privileged=True)


def agent_prefix(container):
    return ["docker", "exec", "-i", "-w", "/tmp/dbdiag_agent", container,
            "setpriv", "--reuid=65532", "--regid=65532", "--clear-groups",
            "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all",
            "--no-new-privs", "--", "env", "HOME=/tmp/dbdiag_agent",
            "PAGER=cat", "PSQL_PAGER=cat", "BASH_ENV=/dev/null"]


def action_argv(container, command):
    verify(container)
    mapped = []
    for argument in command:
        path = Path(argument)
        if path.is_absolute() and path.is_file() and ROOT.resolve() in path.resolve().parents:
            # The trusted harness chooses explicit script arguments. Never map
            # arbitrary paths occurring inside the model-generated shell text.
            suffix = path.suffix if path.suffix in {".sh", ".sql"} else ".txt"
            target = "/tmp/dbdiag_agent/input_" + uuid.uuid4().hex + suffix
            _run(["docker", "cp", str(path), container + ":" + target], operation="trusted explicit file copy")
            _run(["docker", "exec", container, "chown", "65532:65532", target], operation="action input owner setup")
            _run(["docker", "exec", container, "chmod", "600", target], operation="action input mode setup")
            mapped.append(target)
        else:
            mapped.append(argument)
    return agent_prefix(container) + mapped


def observation_argv(container, command):
    """Called only by the trusted harness, never by a model-selected tool name."""
    if not managed(container):
        return ["docker", "exec", "-i", container, *command]
    if any(Path(s).name == "anomaly_inject.sh" for s in command):
        return injection_argv(container, command)
    phase = _read(container).get("phase")
    if phase == "sealed":
        return action_argv(container, command)
    if phase not in {None, "initialized", "injecting"}:
        raise BoundaryError("A blocked boundary must never fall back to root execution.")
    prepare_bootstrap(container, command)
    return ["docker", "exec", "-i", container, *command]


# Sample staging and diagnosis visibility.

def _inside(path, root):
    return path == root or root in path.parents


def _regular(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("Sample input must be a regular, non-symlink file: " + str(path))


class SampleStage:
    def __init__(self, source, sample_id, logger=None):
        self.project = PROJECT_ROOT.resolve()
        self.source = Path(source).resolve(strict=True)
        self.sample_id = str(sample_id)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", self.sample_id):
            raise ValueError("Unsafe sample ID.")
        if _inside(self.source, self.project):
            raise ValueError("Original sample source must be outside the project bind mount.")
        self.root = self.project / "DiagDBench"
        self.path = self.root / self.sample_id
        self.logger = logger
        self.lock = None
        self.container = None
        self.entered = False

    def __enter__(self):
        for path in (self.root, self.path):
            if path.is_symlink():
                raise ValueError("Staging paths must not be symlinks: " + str(path))
            path.mkdir(mode=0o755, exist_ok=True)
        lock_root = self.project.parent / ".dbdiag_sample_locks"
        if lock_root.is_symlink():
            raise ValueError("Lock directory must not be a symlink.")
        lock_root.mkdir(mode=0o700, exist_ok=True)
        self.lock = os.open(lock_root / (self.sample_id + ".lock"),
                            os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.entered = True
            self.refresh()
        except BaseException:
            if self.entered:
                self.__exit__(*__import__("sys").exc_info())
            else:
                os.close(self.lock)
                self.lock = None
            raise
        return self

    def _remove(self, path):
        if path.parent != self.path or self.path.is_symlink():
            raise ValueError("Refusing removal outside the owned sample staging directory.")
        try:
            if path.is_symlink() or not path.is_dir():
                path.unlink(missing_ok=True)
            else:
                shutil.rmtree(path)
        except PermissionError:
            if not self.container:
                raise
            # Injection runs as container root and can create root-owned dirs.
            # Never apply this fallback to the source or an arbitrary host path.
            subprocess.run(["docker", "exec", self.container, "rm", "-rf", "--", str(path)],
                           check=True, capture_output=True, timeout=60)
        if os.path.lexists(path):
            raise RuntimeError("Staging cleanup did not remove " + str(path))

    def refresh(self):
        """Copy the complete source before each independent environment replay."""
        if not self.entered or self.path.is_symlink():
            raise RuntimeError("Staging lock not held or staging path changed.")
        # Reject links and special files rather than following them into secrets.
        for base, dirs, files in os.walk(self.source, followlinks=False):
            for name in dirs:
                if (Path(base) / name).is_symlink():
                    raise ValueError("Source sample contains a directory symlink.")
            for name in files:
                _regular(Path(base) / name)
        for name in PUBLIC_NAMES:
            _regular(self.source / name)
        for child in self.path.iterdir():
            self._remove(child)
        # Keep the directory inode: live Docker bind mounts refer to this inode.
        shutil.copytree(self.source, self.path, dirs_exist_ok=True, symlinks=False)
        if self.logger:
            self.logger.info("[SAMPLE ISOLATION] sample=%s complete copy staged at %s",
                             self.sample_id, self.path)

    def container_config(self, config):
        return dict(config, _sample_staging_dir=str(self.path))

    def sanitize(self, container=None, verify_container=False):
        try:
            return self._sanitize(container, verify_container)
        except BoundaryError:
            raise
        except Exception as exc:
            raise BoundaryError("Sample sanitization could not be verified: " + str(exc)) from exc

    def _sanitize(self, container=None, verify_container=False):
        """Fail closed: restore exactly the original four public regular files."""
        self.container = container or self.container
        if self.path.is_symlink() or self.root.is_symlink():
            raise RuntimeError("Staging directory was replaced by a symlink.")
        for child in list(self.path.iterdir()):
            if child.name not in PUBLIC_NAMES or child.is_symlink() or not child.is_file():
                self._remove(child)
        for name in PUBLIC_NAMES:
            source = self.source / name
            _regular(source)
            target = self.path / name
            if target.exists() and target.read_bytes() == source.read_bytes():
                continue
            if os.path.lexists(target):
                self._remove(target)
            shutil.copy2(source, target)
        names = {p.name for p in self.path.iterdir()}
        if names != PUBLIC_NAMES:
            raise RuntimeError("Sample whitelist validation failed.")
        for name in names:
            _regular(self.path / name)
        if verify_container:
            self.verify_container(container)
        if self.logger:
            self.logger.info("[SAMPLE ISOLATION] sample=%s sanitized; exact four-file whitelist verified%s",
                             self.sample_id, " in container" if verify_container else "")

    def verify_container(self, container):
        if not container:
            raise RuntimeError("A container is required for the diagnosis boundary check.")
        probe = read_only_probe(["docker", "inspect", container],
                                operation="sample staging container inspect")
        info = json.loads(probe)[0]
        mounts = info.get("Mounts", [])
        for mount in mounts:
            if mount.get("Type") == "bind":
                source = Path(mount["Source"]).resolve()
                if _inside(self.source, source):
                    raise RuntimeError("Original sample source is exposed by a Docker bind mount.")
        if not any(m.get("Type") == "bind" and m["Destination"] == str(self.path)
                   and Path(m["Source"]).resolve() == self.path for m in mounts):
            raise RuntimeError("Missing per-sample bind mount.")
        # Check the actual container view, not only the host-side deletion.
        script = ('find "$1" -mindepth 1 -maxdepth 1 -printf "%f\\n"; '
                  'printf "\\n--FILES--\\n"; '
                  'find "$2" -mindepth 1 -maxdepth 1 -type f -printf "%f\\n"; '
                  'printf "\\n--ALL--\\n"; '
                  'find "$2" -mindepth 1 -maxdepth 1 -printf "%f\\n"')
        probe = read_only_probe(["docker", "exec", container, "sh", "-c", script,
                                 "check-staging", str(self.root), str(self.path)],
                                operation="sample staging four-file whitelist probe")
        visible, rest = probe.split("\n--FILES--\n")
        files, all_entries = rest.split("\n--ALL--\n")
        if (set(visible.splitlines()) != {self.sample_id}
                or set(files.splitlines()) != PUBLIC_NAMES
                or set(all_entries.splitlines()) != PUBLIC_NAMES):
            raise RuntimeError("Container can see non-public or cross-sample staging files.")

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.entered:
                self.sanitize()
        finally:
            self.entered = False
            if self.lock is not None:
                fcntl.flock(self.lock, fcntl.LOCK_UN)
                os.close(self.lock)
                self.lock = None


def isolated_docker_command(command, container_config):
    """Mask other workers' complete samples without changing DB resource limits."""
    path = container_config.get("_sample_staging_dir") if isinstance(container_config, dict) else None
    if path is None:
        return command
    return scoped_run_command(command, container_config)


def staged_diagnosis(function):
    """Preserve the existing harness interface, including inspect.signature."""
    signature = inspect.signature(function)
    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        bound = signature.bind(*args, **kwargs)
        files = bound.arguments["files"]
        case = bound.arguments["case"]
        sid = case.get("anomaly_id", case.get("sample_id"))
        if sid is None:
            raise ValueError("Sample ID is required for isolated staging.")
        with SampleStage(Path(files["config"]).parent, sid, bound.arguments["logger"]) as stage:
            token = CURRENT_STAGE.set(stage)
            try:
                bound.arguments["files"] = {key: stage.path / Path(path).name for key, path in files.items()}
                bound.arguments["container_config"] = stage.container_config(bound.arguments["container_config"])
                return function(*bound.args, **bound.kwargs)
            finally:
                containers = bound.arguments.get("containers")
                if containers:
                    stage.container = containers[-1]
                CURRENT_STAGE.reset(token)
    return wrapped


def sanitize_before_diagnosis(files, container):
    stage = CURRENT_STAGE.get()
    if stage is None:
        raise RuntimeError("Diagnosis attempted without isolated sample staging.")
    stage.sanitize(container, verify_container=True)
    seal(container)
    # Do not pass private injection/modification path hints to a native adapter.
    return {key: value for key, value in files.items() if key in PUBLIC_KEYS}
