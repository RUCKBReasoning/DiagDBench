"""Shared MCP dispatcher, schemas, budgets, trusted audit and tool implementations."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import threading
import time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from . import react
from .schemas import ExecuteInput, FinishInput
from .knowledge import Knowledge
from .sandbox import DatabaseSandbox, IsolationError, analysis_execute
from .subagents import READ_TOOLS, Specialists
from .workspace import CONTROL_DIRECTORY, Workspace, page_file


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Empty(Input):
    pass


class ListInput(Input):
    offset: int = Field(0, ge=0, le=100000, description="Zero-based item offset.")
    limit: int = Field(30, ge=1, le=100, description="Maximum result count.")


class ReadInput(Input):
    path: str = Field(min_length=1, max_length=512, description="Relative inputs/ or work/ file path.")
    offset: int = Field(0, ge=0, le=64 * 1024 * 1024, description="Byte cursor returned by the previous page.")
    limit: int = Field(16000, ge=4, le=32000, description="Maximum bytes to return.")


class SearchInput(ListInput):
    query: str = Field(min_length=1, max_length=300, description="Literal case-insensitive search text.")


class WriteInput(Input):
    path: str = Field(min_length=1, max_length=512, description="Destination under work/.")
    content: str = Field(max_length=2 * 1024 * 1024, description="Complete UTF-8 file content.")
    expected_sha256: str | None = Field(None, pattern=r"^[a-f0-9]{64}$", description="Optional compare-and-swap SHA-256.")


class PatchInput(Input):
    path: str = Field(min_length=1, max_length=512, description="Existing work/ file.")
    old_text: str = Field(min_length=1, max_length=100000, description="Exact text that occurs once.")
    new_text: str = Field(max_length=100000, description="Replacement text.")
    expected_sha256: str = Field(pattern=r"^[a-f0-9]{64}$", description="SHA-256 from workspace_read.")


class AnalysisInput(Input):
    language: Literal["python", "bash"] = Field(description="Interpreter in the isolated analysis container.")
    code: str = Field(min_length=1, max_length=100000, description="Code; /workspace is read-only, /scratch is temporary.")
    timeout: int = Field(30, ge=1, le=120, description="Wall-clock limit in seconds.")


class ObservationInput(Input):
    observation_id: str = Field(pattern=r"^(db|analysis)_[0-9]{4}$", description="ID returned by an execution tool.")
    offset: int = Field(0, ge=0, le=64 * 1024 * 1024, description="Byte cursor; not a filesystem path.")
    limit: int = Field(16000, ge=4, le=32000, description="Maximum bytes in this page.")


class PlanStep(Input):
    step: str = Field(min_length=1, max_length=500, description="Concrete investigation or validation step.")
    status: Literal["pending", "in_progress", "completed"] = Field(description="Current state.")


class PlanInput(Input):
    steps: list[PlanStep] = Field(min_length=1, max_length=12, description="At most one step may be in progress.")


class DocsInput(Input):
    query: str = Field(min_length=1, max_length=300, description="Database concept, parameter or error to find.")
    dbms: Literal["all", "postgresql", "mysql"] = Field("all", description="Documentation family.")
    limit: int = Field(5, ge=1, le=8, description="Maximum matching pinned documents.")


class DocReadInput(Input):
    doc_id: str = Field(min_length=1, max_length=80, description="ID from docs_search.")
    offset: int = Field(0, ge=0, le=2000000, description="Character cursor.")
    limit: int = Field(16000, ge=1, le=32000, description="Maximum characters.")


class WebSearchInput(Input):
    query: str = Field(min_length=1, max_length=200, description="General database topic; searches official documentation only.")
    limit: int = Field(5, ge=1, le=10, description="Maximum versioned documentation links.")


class WebFetchInput(Input):
    url: str = Field(min_length=1, max_length=1000, description="HTTPS PostgreSQL 16 or MySQL 8.0 manual URL.")
    offset: int = Field(0, ge=0, le=2000000, description="Character offset.")
    limit: int = Field(16000, ge=1, le=32000, description="Maximum characters.")


class AgentInput(Input):
    task: str = Field(min_length=1, max_length=8000, description="Read-only specialist task; reference approved evidence IDs.")


class AgentStatusInput(Input):
    agent_id: str = Field(pattern=r"^specialist_[1-4]$", description="ID returned by agent_spawn.")
    wait_seconds: int = Field(0, ge=0, le=20, description="Optional bounded wait for completion.")


# name -> schema, description, read-only hint. Read-only annotations never replace
# the dispatcher actor policy or filesystem/container enforcement.
SPECS = {
    "db_execute": (ExecuteInput, "Execute Bash/SQL in the SAME initialized, injected and now sealed database container. Start with # TIMEOUT THRESHOLD: <seconds>. Returns an observation ID and first page.", False),
    "db_finish_diagnosis": (FinishInput, "Commit the same report/recovery/workload artifacts as Codex after validation. Requires a live inspection and no running specialists.", False),
    "workspace_list": (ListInput, "List only approved inputs/ and agent-created work/ files, with pagination.", True),
    "workspace_read": (ReadInput, "Read a file page and SHA-256 in the isolated evidence workspace; no raw samples, parent paths or symlinks.", True),
    "workspace_search": (SearchInput, "Search literal text in approved inputs/work files; returns file/line evidence and pagination.", True),
    "workspace_write": (WriteInput, "Create or replace a UTF-8 file under work/. Control files and inputs are not writable.", False),
    "workspace_patch": (PatchInput, "Apply one exact text replacement to a work/ file with a required SHA-256 precondition.", False),
    "analysis_execute": (AnalysisInput, "Run Python/Bash in a disposable, offline container. Read /workspace; compute in /scratch. No database access, host files, credentials or persistent writes.", False),
    "read_observation": (ObservationInput, "Page the complete captured log of a current-run db/analysis action by ID. Never reads initialization or injection logs.", True),
    "plan_update": (PlanInput, "Maintain the investigation plan; at most one step in progress. Does not execute actions.", False),
    "plan_read": (Empty, "Read the current investigation plan.", True),
    "docs_search": (DocsInput, "Search the pinned, SHA-256-verified PostgreSQL 16/MySQL 8.0 documentation snapshot.", True),
    "docs_read": (DocReadInput, "Read a pinned document by ID with character pagination and source provenance.", True),
    "web_search": (WebSearchInput, "Search live official PostgreSQL documentation; only version-16 manual links returned. General/benchmark web search is disabled.", True),
    "web_fetch": (WebFetchInput, "Read public official versioned PostgreSQL/MySQL documentation. Private IPs, redirects, arbitrary hosts and query URLs are blocked.", True),
    "agent_spawn": (AgentInput, "Start a bounded read-only specialist using the same model. It can inspect approved evidence/docs, but cannot execute, mutate, browse or delegate.", False),
    "agent_status": (AgentStatusInput, "Read a specialist result or wait at most 20 seconds. All specialists must finish before final submission.", True),
    "tools_status": (Empty, "Show enabled/disabled capabilities, shared tool budgets, observation IDs and specialist status.", True),
}


class Tools:
    def __init__(self, context: dict):
        self.context = context
        self.output = Path(context["output_dir"])
        self.private = self.output / CONTROL_DIRECTORY
        self.private.mkdir(mode=0o700, exist_ok=True)
        self.workspace = Workspace(self.output / "agent_workspace")
        self.knowledge = Knowledge(Path(__file__).with_name("docs"))
        self.lock = threading.RLock()
        self.finished = False
        self.broken = False
        self.db_actions = 0
        self.aux_actions = 0
        self.analysis_actions = 0
        self.logs = {}
        self.plan = []
        self.enabled = set(SPECS)
        if not context.get("enable_web_docs", True):
            self.enabled -= {"web_search", "web_fetch"}
        if not context.get("enable_specialists", True):
            self.enabled -= {"agent_spawn", "agent_status"}
        self.database = DatabaseSandbox(context["container_name"], Path(context["project_root"]), context["owner_prefix"])
        self.database.identity = context["seal"]["container_id"]
        self.database.hidden = context["hidden_mounts"]
        self.database.runtime_scratch = context["seal"].get("runtime_scratch", [])
        self.database.sealed = True
        self.specialists = Specialists(self, context.get("transport_url", ""), context.get("transport_token", ""),
                                       max_jobs=context.get("max_specialists", 4),
                                       max_turns=context.get("specialist_turns", 6))

    def audit(self, event: dict):
        with (self.private / "tool_events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"time": time.time(), **event}, ensure_ascii=False) + "\n")

    def function_schemas(self, allowed=None):
        selected = self.enabled if allowed is None else self.enabled & allowed
        return [{"type": "function", "name": name, "description": SPECS[name][1],
                 "parameters": {"type": "object", "properties": {"params": SPECS[name][0].model_json_schema()},
                                "required": ["params"], "additionalProperties": False}}
                for name in sorted(selected)]

    def invoke(self, name: str, arguments: dict, actor: str = "main") -> dict:
        if name not in self.enabled or (actor != "main" and name not in READ_TOOLS):
            raise ValueError("Tool is not allowed for this agent.")
        params = SPECS[name][0].model_validate(arguments)
        with self.lock:
            if self.finished or self.broken:
                raise ValueError("Diagnosis is finished or isolation failed; no more tools can run.")
            if name not in {"db_execute", "db_finish_diagnosis"}:
                if self.aux_actions >= self.context.get("max_aux_actions", 100):
                    raise ValueError("Shared auxiliary tool budget exhausted; submit current findings.")
                self.aux_actions += 1
            self.audit({"actor": actor, "tool": name, "arguments": params.model_dump(), "phase": "start"})
            if name != "agent_status":
                try:
                    result = self._call(name, params)
                    self.audit({"actor": actor, "tool": name, "phase": "result", "result": result})
                    return result
                except IsolationError:
                    self.broken = True
                    self.audit({"actor": actor, "tool": name, "phase": "isolation_failure"})
                    raise
                except Exception as exc:
                    self.audit({"actor": actor, "tool": name, "phase": "error", "error_type": type(exc).__name__})
                    raise
        # Never wait while holding the dispatch lock: specialists need it to read.
        result = self.specialists.status(params.agent_id, params.wait_seconds)
        with self.lock:
            self.audit({"actor": actor, "tool": name, "phase": "result", "result": result})
        return result

    def _call(self, name, p):
        if name == "db_execute":
            if self.db_actions >= self.context["max_turn_num"] - 1:
                raise ValueError("Database action budget exhausted; use db_finish_diagnosis.")
            timeout = react.timeout_from_script(p.shell, "Diagnostic script")
            if not 1 <= timeout <= self.context.get("action_timeout", 1800):
                raise ValueError("Script timeout exceeds the configured action budget.")
            self.db_actions += 1
            key = f"db_{self.db_actions:04d}"
            log = self.private / f"{key}.log"
            self.logs[key] = log
            result = self.database.execute(p.shell, log, timeout)
            return {"observation_id": key, **result, **page_file(log)}
        if name == "db_finish_diagnosis":
            if not self.db_actions:
                raise ValueError("Inspect the live database before finishing.")
            if self.specialists.active():
                raise ValueError("Wait for running specialists before submitting the diagnosis.")
            self.database.verify()
            action = {"action": "finish", "diagnosis_report.md": p.diagnosis_report,
                      "solution_script.sh": p.solution_script, "verified_workload.sql": p.verified_workload}
            react.save_finish(action, self.output, self.context["original_workload"])
            (self.output / "native_finish.json").write_text(json.dumps(action, ensure_ascii=False, indent=2))
            self.finished = True
            return {"status": "saved", "files": ["diagnosis_report.md", "solution_script.sh", "optimized_verified_workload.sql"]}
        if name == "workspace_list":
            items = self.workspace.inventory()
            return {"files": items[p.offset:p.offset+p.limit], "total": len(items),
                    "has_more": p.offset+p.limit < len(items),
                    "next_offset": p.offset+p.limit if p.offset+p.limit < len(items) else None}
        if name == "workspace_read":
            result = self.workspace.read(p.path, p.offset, p.limit)
            result["sha256"] = hashlib.sha256(self.workspace.path(p.path).read_bytes()).hexdigest()
            return result
        if name == "workspace_search":
            return self.workspace.search(p.query, p.offset, p.limit)
        if name == "workspace_write":
            return self.workspace.write(p.path, p.content, p.expected_sha256)
        if name == "workspace_patch":
            return self.workspace.patch(p.path, p.old_text, p.new_text, p.expected_sha256)
        if name == "analysis_execute":
            self.workspace.inventory()  # Reject unexpected links before the RO mount.
            self.analysis_actions += 1
            key = f"analysis_{self.analysis_actions:04d}"
            log = self.private / f"{key}.log"
            self.logs[key] = log
            result = analysis_execute(self.workspace.root.resolve(), p.code, p.language, p.timeout,
                                      log, self.context["analysis_image"])
            return {"observation_id": key, **result, **page_file(log)}
        if name == "read_observation":
            if p.observation_id not in self.logs:
                raise ValueError("Unknown current-run observation ID.")
            return {"observation_id": p.observation_id,
                    **page_file(self.logs[p.observation_id], p.offset, p.limit)}
        if name == "plan_update":
            if sum(s.status == "in_progress" for s in p.steps) > 1:
                raise ValueError("At most one plan step may be in progress.")
            self.plan = [s.model_dump() for s in p.steps]
            (self.private / "plan.json").write_text(json.dumps(self.plan, ensure_ascii=False, indent=2))
            return {"steps": self.plan}
        if name == "plan_read":
            return {"steps": self.plan}
        if name == "docs_search":
            return self.knowledge.search(p.query, p.limit, p.dbms)
        if name == "docs_read":
            return self.knowledge.read(p.doc_id, p.offset, p.limit)
        if name == "web_search":
            return self.knowledge.web_search(p.query, p.limit)
        if name == "web_fetch":
            return self.knowledge.web_fetch(p.url, p.offset, p.limit)
        if name == "agent_spawn":
            return self.specialists.spawn(p.task)
        if name == "tools_status":
            return {"enabled": sorted(self.enabled), "database_actions_used": self.db_actions,
                    "database_actions_limit": self.context["max_turn_num"] - 1,
                    "auxiliary_actions_used": self.aux_actions,
                    "auxiliary_actions_limit": self.context.get("max_aux_actions", 100),
                    "observations": sorted(self.logs),
                    "specialists": [self.specialists.status(k) for k in self.specialists.jobs],
                    "disabled": ["native_host_shell", "native_host_files",
                                 "raw_sample_access", "specialist_database_mutation", "recursive_delegation"],
                    "native_tools": self.context.get("native_tools", []),
                    "web_scope": (
                        "native_public_web_and_official_docs"
                        if "WebSearch" in self.context.get("native_tools", [])
                        else "official_versioned_documentation"
                    )}
        raise ValueError("Unknown tool.")
