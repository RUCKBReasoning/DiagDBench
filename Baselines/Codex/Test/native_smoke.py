"""Opt-in real model/CLI/MCP/specialist test. Uses an env-only model credential."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import tempfile
import uuid

from Baselines.Codex.agent import preflight, run_agent
from Baselines.Codex.sandbox import command
from Baselines.Codex.Test.integration import free_port
from Baselines.Codex import react
from Baselines.preparation import direct_injection_command
from docker_func import build_container_config, docker_container_init, force_remove_container

PROMPT = """This is an isolated tool-integration fixture, not a benchmark sample.
Database: PostgreSQL at localhost:5432, database postgres, administrator root/root.
Use these MCP tools to complete a smoke test:
1. plan_update one in_progress step. workspace_write work/note.txt with 'pending'.
2. workspace_read that file, then workspace_patch pending to ready using its hash.
3. analysis_execute Python reading /workspace/work/note.txt; assert ready; print ANALYSIS_OK.
4. db_execute Bash with # TIMEOUT THRESHOLD: 20 and PGPASSWORD=root psql -h 127.0.0.1 -U root -d postgres -Atc 'SELECT SUM(v) FROM public.codex_probe;'.
5. docs_search PostgreSQL lock. agent_spawn exactly one read-only specialist to read work/note.txt and independently report its contents. agent_status until completed.
6. plan_update completed. db_finish_diagnosis report the observed SUM and specialist outcome; solution_script '# TIMEOUT THRESHOLD: 10\\ntrue\\n', verified_workload empty.
Do not inspect host paths or raw samples. Call tools rather than describing them."""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    preflight(config)
    config.update(docker_name="codex_smoke", max_turn_num=8, action_timeout=30, agent_timeout=600)
    config["codex"] = {"max_aux_actions": 65, "max_model_calls": 35,
                           "max_model_tokens": 250000, "max_specialists": 1, "specialist_turns": 4}
    private = Path(tempfile.mkdtemp(prefix="codex_native_", dir=react.PROJECT_ROOT / "_Back_Up"))
    output = private / "output"
    output.mkdir()
    (private / "config.json").write_text(json.dumps(config, indent=2))
    initial = private / "init.sql"
    initial.write_text("SELECT 1;")
    verified = private / "verified.sql"
    verified.write_text("SELECT SUM(v) FROM public.codex_probe;")
    inject = private / "anomaly_inject.sh"
    inject.write_text("# TIMEOUT THRESHOLD: 20\nPGPASSWORD=root psql -h 127.0.0.1 -U root -d postgres -c 'ALTER TABLE public.codex_probe ADD COLUMN injected INT DEFAULT 1;'\n")
    user = {"DB_USER": "root", "DB_PASSWORD": "root", "DB_HOST": "localhost", "DB_PORT": "5432"}
    name = config["docker_name"] + "_" + uuid.uuid4().hex
    state = {"fixture": str(private), "checks": {}}
    logging.basicConfig(level=logging.INFO)
    try:
        db = build_container_config("postgresql", {"CPU": 1, "Memory": 2, "Memory + Swap": 2, "Storage": 8}, free_port(), user)
        assert docker_container_init(name, db)
        command(["docker", "exec", name, "env", "PGPASSWORD=root", "psql", "-h", "127.0.0.1",
                 "-U", "root", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
                 "CREATE TABLE public.codex_probe(id INT PRIMARY KEY,v INT); INSERT INTO public.codex_probe VALUES(1,10),(2,20);"])
        command(["docker", "exec", name, *direct_injection_command(inject, "postgresql", "postgres", "public", user, 20)])
        result = run_agent(PROMPT, config, name, output, {"initialization": initial, "verified": verified}, logging.getLogger("native_smoke"))
        events = [json.loads(s) for s in (output / "agent_control/tool_events.jsonl").read_text().splitlines()]
        called = {e["tool"] for e in events if e["phase"] == "result"}
        required = {"plan_update", "workspace_write", "workspace_read", "workspace_patch", "analysis_execute",
                    "db_execute", "docs_search", "agent_spawn", "agent_status", "db_finish_diagnosis"}
        state["checks"]["required_tools_called"] = required <= called
        state["checks"]["specialist_completed"] = any(e["tool"] == "agent_status" and e["phase"] == "result"
                                                      and e["result"]["status"] == "completed" for e in events)
        state["checks"]["specialist_read_tool"] = any(e["actor"] == "specialist_1" and e["tool"] == "workspace_read" for e in events)
        state["checks"]["native_finish"] = result["action"] == "finish"
        state["checks"]["ephemeral_context_removed"] = not (output / "agent_control/context.json").exists()
        state["model_usage"] = json.loads((output / "model_usage.json").read_text())
        state["success"] = all(state["checks"].values())
    except Exception as exc:
        state.update(success=False, error_type=type(exc).__name__)
        raise
    finally:
        force_remove_container(name)
        (private / "result.json").write_text(json.dumps(state, indent=2))
        print(json.dumps(state, indent=2))
    if not state["success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
