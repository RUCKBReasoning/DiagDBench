"""Per-sample MCP tools for native Codex and Claude Code database agents."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, ConfigDict, Field

if __package__:
    from . import react
else:
    import react


class ExecuteInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    shell: str = Field(min_length=1, max_length=100000, description="Complete Bash script; first line: # TIMEOUT THRESHOLD: <seconds>.")


class FinishInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    diagnosis_report: str = Field(min_length=1, description="Markdown report: evidence, root cause, applied recovery and validation results.")
    solution_script: str = Field(min_length=1, description="Reusable Bash recovery script, first line # TIMEOUT THRESHOLD: <seconds>.")
    verified_workload: str = Field(
        default="",
        description=(
            "Optimized SQL only when it has a potential performance benefit and "
            "retains every original statement in exactly the same order, without "
            "adding, deleting, merging, splitting, replacing, or reordering statements. "
            "Sequential execution from the same initialized state must produce exactly "
            "the same observable result for every corresponding statement, including "
            "returned rows and values, duplicate counts, affected-row counts, and "
            "persistent database state. Any result mismatch is a diagnosis failure. "
            "Return empty to retain the original workload when exact equivalence cannot "
            "be guaranteed."
        ),
    )


class DiagnosisTools:
    def __init__(self, context):
        self.context = context
        self.output = Path(context["output_dir"])
        self.turn = 0
        self.finished = False
        self.lock = asyncio.Lock()
        self.logger = logging.getLogger("DB_DIAG.native")

    def record(self, event):
        with (self.output / "tool_events.jsonl").open("a", encoding="utf-8") as log:
            log.write(json.dumps(event, ensure_ascii=False) + "\n")

    async def execute(self, params: ExecuteInput) -> str:
        async with self.lock:
            if self.finished:
                raise ValueError("Diagnosis already finished.")
            if self.turn >= int(self.context["max_turn_num"]) - 1:
                raise ValueError("Action budget exhausted. Submit db_finish_diagnosis with current evidence and an honest recovery status.")
            timeout = react.timeout_from_script(params.shell, "Diagnostic script")
            if timeout <= 0 or timeout > int(self.context.get("action_timeout", 1800)):
                raise ValueError("Timeout must be between 1 and the configured action_timeout seconds.")
            self.turn += 1
            self.record({"turn": self.turn, "tool": "db_execute", "shell": params.shell})
            observation = await asyncio.to_thread(
                react.execute_action, self.context["container_name"], self.output,
                self.turn, {"action": "code", "shell": params.shell}, self.logger,
            )
            self.record({"turn": self.turn, "observation": observation})
            return observation

    async def finish(self, params: FinishInput) -> dict:
        async with self.lock:
            if self.turn == 0:
                raise ValueError("Inspect the live database through db_execute before finishing.")
            action = {
                "action": "finish", "diagnosis_report.md": params.diagnosis_report,
                "solution_script.sh": params.solution_script,
                "verified_workload.sql": params.verified_workload,
            }
            react.save_finish(action, self.output, self.context["original_workload"])
            (self.output / "native_finish.json").write_text(json.dumps(action, ensure_ascii=False, indent=2), encoding="utf-8")
            self.finished = True
            self.record({"tool": "db_finish_diagnosis", "turn": self.turn + 1})
            return {"status": "saved", "files": ["diagnosis_report.md", "solution_script.sh", "optimized_verified_workload.sql"]}


def make_server(context):
    tools = DiagnosisTools(context)
    server = FastMCP("diagnosis_mcp")

    @server.tool(annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": False})
    async def db_execute(params: ExecuteInput) -> str:
        """Run a diagnostic or recovery Bash script inside this sample's database container.

        Returns execution status and at most 20,000 characters of observations.
        Only the preassigned container is used; no host commands are executed.
        """
        return await tools.execute(params)

    @server.tool(annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False})
    async def db_finish_diagnosis(params: FinishInput) -> dict:
        """Save the diagnosis report, reusable recovery script and optional optimized SQL.

        Call after inspecting/recovering the database and validating the workload.
        State unresolved failures honestly. Returns saved artifact names.
        """
        return await tools.finish(params)

    return server


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", required=True, type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)  # stderr; stdout is reserved for MCP
    make_server(json.loads(args.context.read_text())).run(transport="stdio")
