"""Small real PG/MySQL fixtures, original container bootstrap/injection helpers.

No production sample is changed; test containers and private canaries are unique.
This does not claim to be a full benchmark/dataset reload.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import uuid

from docker_func import build_container_config, docker_container_init, force_remove_container
from Baselines.preparation import direct_injection_command
from Baselines.Codex import react
from Baselines.Codex.sandbox import (
    DatabaseActionLimitError,
    DatabaseSandbox,
    IsolationError,
    command,
    inspect_container,
)
from Baselines.Codex.tools import Tools
from Baselines.Codex.diagnosis_mcp import make_server


def free_port():
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


def fixture(dbms: str):
    root = react.PROJECT_ROOT
    prefix = "codex_integration"
    name = prefix + "_" + uuid.uuid4().hex
    private = Path(tempfile.mkdtemp(prefix="codex_fixture_", dir=root / "_Back_Up"))
    output = private / "output"
    output.mkdir()
    source = private / "raw_sample"
    source.mkdir()
    canary = source / "hidden_answer.txt"
    marker = "ORIGINAL_SAMPLE_CANARY_" + uuid.uuid4().hex
    canary.write_text(marker)
    user = {"DB_USER": "diagnosis", "DB_PASSWORD": "diagnosis", "DB_HOST": "localhost",
            "DB_PORT": "3306" if dbms == "mysql" else "5432"}
    hardware = {"CPU": 1, "Memory": 2, "Memory + Swap": 2, "Storage": 8}
    state = {"dbms": dbms, "checks": {}, "private_fixture": str(private)}
    try:
        config = build_container_config(dbms, hardware, free_port(), user)
        if not docker_container_init(name, config):
            raise RuntimeError("Original container initialization failed.")
        state["checks"]["original_container_bootstrap"] = True
        if dbms == "mysql":
            sql = ["mysql", "-h", "127.0.0.1", "-uroot", "-proot", "-N", "-e"]
            command(["docker", "exec", name, *sql,
                     "CREATE DATABASE codex_fixture; CREATE TABLE codex_fixture.probe(id INT PRIMARY KEY, v INT); "
                     "INSERT INTO codex_fixture.probe VALUES(1,10),(2,20);"])
            script = "# TIMEOUT THRESHOLD: 30\nmysql -h 127.0.0.1 -uroot -proot -e 'ALTER TABLE codex_fixture.probe ADD COLUMN injected INT DEFAULT 1;'\n"
            validation = "mysql -h 127.0.0.1 -uroot -proot -N -e 'SELECT SUM(v) FROM codex_fixture.probe;'"
            server_file_probe = "mysql -h 127.0.0.1 -uroot -proot -N -e \"SELECT LOAD_FILE('" + str(canary) + "') IS NULL;\""
            database = "codex_fixture"
        else:
            sql = ["env", "PGPASSWORD=root", "psql", "-h", "127.0.0.1", "-U", "root", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-Atc"]
            command(["docker", "exec", name, *sql,
                     "CREATE TABLE public.codex_probe(id INT PRIMARY KEY,v INT); INSERT INTO public.codex_probe VALUES(1,10),(2,20);"])
            script = "# TIMEOUT THRESHOLD: 30\nPGPASSWORD=root psql -h 127.0.0.1 -U root -d postgres -v ON_ERROR_STOP=1 -c 'ALTER TABLE public.codex_probe ADD COLUMN injected INT DEFAULT 1;'\n"
            validation = "PGPASSWORD=root psql -h 127.0.0.1 -U root -d postgres -Atc 'SELECT SUM(v) FROM public.codex_probe;'"
            server_file_probe = "PGPASSWORD=root psql -h 127.0.0.1 -U root -d postgres -Atc \"SELECT pg_read_file('" + str(canary) + "');\""
            database = "postgres"
        inject = source / "anomaly_inject.sh"
        inject.write_text(script)
        command(["docker", "exec", name, *direct_injection_command(inject, dbms, database, "public", user, 30)])
        state["checks"]["original_direct_injection_helper"] = True
        before = inspect_container(name)["Id"]
        sandbox = DatabaseSandbox(name, root, prefix)
        # A live injection worker could retain an open descriptor after hiding
        # the directory. Reject this state before granting any model tool.
        command(["docker", "exec", "-d", "-w", "/", name, "python3", "-c",
                 "import os,time; f=open(" + repr(str(canary)) + "); "
                 "open('/tmp/codex_fd_ready','w').write(str(os.getpid())); time.sleep(60)"])
        deadline = time.monotonic() + 10
        while True:
            try:
                held_pid = command(["docker", "exec", name, "cat", "/tmp/codex_fd_ready"]).strip()
                break
            except IsolationError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.1)
        try:
            sandbox.seal()
        except IsolationError as exc:
            assert "retains a source" in str(exc)
            state["checks"]["live_source_descriptor_fail_closed"] = True
        else:
            raise AssertionError("A live source descriptor was not rejected.")
        command(["docker", "exec", name, "kill", held_pid])
        command(["docker", "exec", name, "python3", "-c",
                 "import os,time; p='/proc/" + held_pid + "'; "
                 "\nfor _ in range(100):\n"
                 " if not os.path.exists(p): break\n"
                 " time.sleep(0.02)\n"])
        seal = sandbox.seal()
        state["checks"]["same_container_after_seal"] = inspect_container(name)["Id"] == before
        state["checks"]["sealed"] = seal["sealed"]
        analysis_image = json.loads(command(["docker", "image", "inspect", "pg_diag"]))[0]["Id"]
        context = {
            "output_dir": str(output), "project_root": str(root), "container_name": name,
            "owner_prefix": prefix, "max_turn_num": 20, "action_timeout": 60,
            "original_workload": "SELECT 1;", "seal": seal, "hidden_mounts": sandbox.hidden,
            "analysis_image": analysis_image, "transport_url": "http://127.0.0.1:1/v1",
            "transport_token": "test-only-unused", "enable_specialists": False,
        }
        tools = Tools(context)
        try:
            q = tools.invoke("db_execute", {"shell": "# TIMEOUT THRESHOLD: 10\n" + validation})
            assert "30" in q["text"]
            state["checks"]["live_database_query_after_seal"] = True
            raw = tools.invoke("db_execute", {"shell": "# TIMEOUT THRESHOLD: 10\ncat " + str(canary) + "\ncat /proc/1/root" + str(canary)})
            assert marker not in raw["text"] and raw["returncode"] != 0
            state["checks"]["source_and_proc_root_blocked"] = True
            sql_read = tools.invoke("db_execute", {"shell": "# TIMEOUT THRESHOLD: 10\n" + server_file_probe})
            assert marker not in sql_read["text"]
            assert (sql_read["returncode"] != 0 if dbms != "mysql" else "1" in sql_read["text"])
            state["checks"]["sql_server_file_read_blocked"] = True
            escape = tools.invoke("db_execute", {"shell": "# TIMEOUT THRESHOLD: 10\nunshare -Urnm true"})
            assert escape["returncode"] != 0
            state["checks"]["mount_namespace_escape_blocked"] = True
            entry = tools.invoke("workspace_write", {"path": "work/check.txt", "content": "analysis_evidence"})
            tools.invoke("workspace_patch", {"path": "work/check.txt", "old_text": "analysis_evidence",
                                            "new_text": "analysis_ready", "expected_sha256": entry["sha256"]})
            code = (
                "import os,socket\n"
                "assert open('/workspace/work/check.txt').read()=='analysis_ready'\n"
                "assert not os.path.exists(" + repr(str(canary)) + ")\n"
                "assert not os.path.exists('/var/run/docker.sock')\n"
                "assert not any('API_KEY' in k or 'TOKEN' in k for k in os.environ)\n"
                "try:\n open('/workspace/work/tamper','w'); raise AssertionError('writable')\n"
                "except OSError: pass\n"
                "s=socket.socket();s.settimeout(1)\n"
                "try:\n s.connect(('1.1.1.1',443));raise AssertionError('network')\n"
                "except OSError:pass\n"
                "print('ANALYSIS_ISOLATED')\n"
                "print('x'*50000)\n"
            )
            result = tools.invoke("analysis_execute", {"language": "python", "code": code, "timeout": 20})
            assert result["returncode"] == 0 and "ANALYSIS_ISOLATED" in result["text"]
            assert result["has_more"]
            page = tools.invoke("read_observation", {"observation_id": result["observation_id"], "offset": result["next_offset"]})
            assert page["bytes"] > 0
            state["checks"]["analysis_offline_readonly_no_source_no_secret"] = True
            state["checks"]["complete_log_pagination"] = True
            assert tools.invoke("docs_search", {"query": "lock", "dbms": dbms})["results"]
            state["checks"]["pinned_document_retrieval"] = True
            tools.invoke("plan_update", {"steps": [{"step": "Verify isolation", "status": "completed"}]})
            assert tools.invoke("plan_read", {})["steps"][0]["status"] == "completed"
            server, server_tools = make_server(context)
            schemas = asyncio.run(server.list_tools())
            assert len(schemas) == len(server_tools.enabled)
            server_tools.specialists.close()
            state["checks"]["mcp_tool_registration"] = True
            try:
                sandbox.execute("sleep 20\n", private / "timeout.log", 1)
            except DatabaseActionLimitError:
                sandbox.verify()
                assert inspect_container(name)["State"]["Running"]
                state["checks"]["timeout_preserves_sealed_sample"] = True
            else:
                raise AssertionError("Timed-out database action was not rejected.")
            after_timeout = tools.invoke("db_execute", {
                "shell": "# TIMEOUT THRESHOLD: 10\n" + validation,
            })
            assert "30" in after_timeout["text"]
            state["checks"]["database_query_after_timeout"] = True
            done = tools.invoke("db_finish_diagnosis", {"diagnosis_report": "Integration fixture validated.",
                                "solution_script": "# TIMEOUT THRESHOLD: 10\ntrue\n", "verified_workload": ""})
            assert done["status"] == "saved"
            assert (output / "optimized_verified_workload.sql").read_text() == "SELECT 1;"
            state["checks"]["original_finish_artifact_contract"] = True
            try:
                tools.invoke("db_execute", {"shell": "# TIMEOUT THRESHOLD: 10\ntrue"})
            except ValueError:
                state["checks"]["post_finish_execution_rejected"] = True
            else:
                raise AssertionError("Execution accepted after finish.")
            try:
                sandbox.execute(
                    "env -i sh -c 'sleep 20' >/dev/null 2>&1 &\n",
                    private / "untracked_process.log",
                    5,
                )
            except IsolationError:
                state["checks"]["untracked_process_fails_closed"] = True
            else:
                raise AssertionError("An untracked background process was not rejected.")
        finally:
            tools.specialists.close()
    finally:
        force_remove_container(name)
        state["remaining_test_container"] = bool(subprocess.check_output(
            ["docker", "ps", "-a", "--filter", "name=^/" + name + "$", "--format", "{{.ID}}"], text=True).strip())
        (private / "result.json").write_text(json.dumps(state, indent=2))
    print(json.dumps(state, indent=2))
    assert all(state["checks"].values()) and not state["remaining_test_container"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dbms", choices=["postgresql", "mysql"], required=True)
    fixture(parser.parse_args().dbms)
