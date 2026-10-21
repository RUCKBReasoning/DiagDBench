"""Run react -> codex -> claude_code sequentially, supervising each process group."""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import hashlib
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "Output/2609270852"
CONFIGS = ROOT / "Configs/baselines/2609270857_run"
FRAMEWORKS = ("react", "codex", "claude_code")
ENTRYPOINTS = {
    "react": ROOT / "Baselines/react/react.py",
    "codex": ROOT / "Baselines/Codex/codex.py",
    "claude_code": ROOT / "Baselines/claude_code/claude_code.py",
}


def write_status(state):
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
    for port in range(start, start + 1000, 2):
        probes = []
        try:
            for value in (port, port + 1):
                probe = socket.socket()
                probes.append(probe)
                probe.bind(("0.0.0.0", value))
            if port != start:
                config["host_port"] = port
                config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n")
                print(f"Port pair changed to {port}, {port + 1}", flush=True)
            return
        except OSError:
            pass
        finally:
            for probe in probes:
                probe.close()
    raise RuntimeError("No free port pair available.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--framework", nargs="+", choices=FRAMEWORKS,
                        help="Run only these frameworks, preserving other framework statuses.")
    args = parser.parse_args()
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
        config = json.loads(config_path.read_text())
        assert config["parallelism_num"] == 2
        output = Path(config["output_base_dir"])
        output.mkdir(parents=True, exist_ok=True)
        fatal = output / "fatal_api_error.json"
        if fatal.exists():
            raise RuntimeError(f"Previous API failure marker exists: {fatal}; inspect before restarting.")
        ensure_ports(config, config_path)
        print(f"=== START {name}; parallelism=2; ports={config['host_port']},{config['host_port'] + 1} ===", flush=True)
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
