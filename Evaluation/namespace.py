"""Enter a DB network namespace, then drop privileges before trusted SQL runs.

The diagnosis/evaluation orchestrators remain the SSH user. Only system
nsenter/setpriv execute as root; no project Python code executes as root.
"""
import collections
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import threading


def worker_command(pid):
    pid = int(pid)
    if pid <= 1 or os.geteuid() == 0:
        raise ValueError("Namespace helper expects a valid target and non-root caller.")
    return [
        "/usr/bin/sudo", "-n", "--",
        "/usr/bin/nsenter", "--target", str(pid), "--net", "--",
        "/usr/bin/setpriv", "--reuid", str(os.getuid()), "--regid", str(os.getgid()),
        "--clear-groups", "--inh-caps=-all", "--ambient-caps=-all",
        "--bounding-set=-all", "--no-new-privs", "--",
        sys.executable, "-B", str(Path(__file__).with_name("namespace.py")), "--worker",
    ]


def run_worker(pid, request, timeout_seconds):
    process = subprocess.Popen(
        worker_command(pid), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, start_new_session=True,
        env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
    )
    messages = queue.Queue()
    stderr = collections.deque(maxlen=20)

    def reader():
        try:
            for line in process.stdout:
                messages.put(json.loads(line))
        except Exception as exc:
            messages.put({"kind": "error", "error": "Invalid worker protocol: " + str(exc)})
        finally:
            messages.put({"kind": "eof"})

    def err_reader():
        for line in process.stderr:
            stderr.append(line.rstrip())

    thread = threading.Thread(target=reader, daemon=True)
    error_thread = threading.Thread(target=err_reader, daemon=True)
    thread.start()
    error_thread.start()
    ready = None
    try:
        process.stdin.write(json.dumps(request) + "\n")
        process.stdin.close()
        try:
            ready = messages.get(timeout=30)
        except queue.Empty:
            raise RuntimeError("Namespace SQL worker startup exceeded 30 seconds.")
        if ready.get("kind") != "ready":
            error_thread.join(0.2)
            raise RuntimeError("Namespace SQL worker failed before ready: " +
                               str(ready.get("error", "")) + "; " + "\n".join(stderr))
        if ready.get("uid") != os.getuid() or ready.get("cap_eff") != "0000000000000000" \
                or ready.get("no_new_privs") != "1":
            raise RuntimeError("SQL worker privilege-drop verification failed.")
        if request.get("probe"):
            return ready
        try:
            result = messages.get(timeout=timeout_seconds)
        except queue.Empty:
            return [{
                "sql": "", "success": False, "execution_time": -1, "result": None,
                "timed_out": True, "error": f"Workload deadline exceeded ({timeout_seconds:.6f}s).",
            }]
        if result.get("kind") == "result":
            return result["result"]
        return [{"sql": "", "success": False, "execution_time": -1, "result": None,
                 "error": "SQL namespace worker failed: " + str(result.get("error", result))}]
    finally:
        # sudo's monitor may be root; the SQL Python process is ours and shares
        # this dedicated group. Kill the group so timed-out SQL is not orphaned.
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                process.wait(timeout=5)
        for pipe in (process.stdout, process.stderr):
            pipe.close()
        thread.join(1)
        error_thread.join(1)


def preflight():
    if os.geteuid() == 0:
        return {"ok": True, "transport": "original_root_worker"}
    ready = run_worker(os.getpid(), {"probe": True}, 10)
    return {"ok": True, "transport": "nsenter_then_unprivileged_sql",
            "sql_uid": ready["uid"], "cap_eff": ready["cap_eff"],
            "no_new_privs": ready["no_new_privs"]}



def connection_config(config, container):
    state = json.loads(subprocess.check_output(
        ["docker", "inspect", "--format", "{{json .State}}", container], text=True, timeout=15))
    pid = int(state.get("Pid", 0))
    if not state.get("Running") or pid <= 1:
        raise RuntimeError("Measurement requires a running local Docker container.")
    return dict(config, _network_namespace_pid=pid, host="127.0.0.1", host_port=int(config["port"]))


def emit(value):
    print(json.dumps(value, ensure_ascii=False), flush=True)


def worker_main():
    status = dict(line.split(":", 1) for line in Path("/proc/self/status").read_text().splitlines()
                  if ":" in line)
    cap = status["CapEff"].strip()
    nnp = status["NoNewPrivs"].strip()
    if os.geteuid() == 0 or cap != "0000000000000000" or nnp != "1":
        raise RuntimeError("Refusing to run project SQL code without privilege drop.")
    request = json.loads(sys.stdin.readline())
    if not request.get("probe"):
        # Import from the original evaluator: serialization, SQL execution,
        # time measurement and scoring semantics remain unchanged.
        from Evaluation import test_eval as evaluation
    emit({"kind": "ready", "uid": os.getuid(), "gid": os.getgid(),
          "groups": os.getgroups(), "cap_eff": cap, "no_new_privs": nnp,
          "network_namespace": os.readlink("/proc/self/ns/net")})
    if request.get("probe"):
        return
    config = dict(request["config"])
    config.pop("_network_namespace_pid", None)  # Already entered, do not setns again.
    result = evaluation.execute_measured_sql(
        request["sql_path"], config, request["timeout_seconds"])
    emit({"kind": "result", "result": result})


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="Unprivileged namespace SQL worker")
    parser.add_argument("--worker", action="store_true", required=True)
    parser.parse_args(argv)
    root = str(Path(__file__).resolve().parents[1])
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        worker_main()
    except BaseException as exc:
        emit({"kind": "error", "error": f"{type(exc).__name__}: {exc}"})
        raise SystemExit(1)


if __name__ == "__main__":
    main()
