"""Codex CLI configuration; all filesystem/DB tools use the scoped MCP server."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sys


def build_command(llm, workspace, context_path=None, *, transport_url=None, transport_token=None):
    executable = shutil.which(str(llm.get("codex_executable", "codex")))
    if not executable:
        raise RuntimeError("Codex CLI is not installed.")
    command = [
        executable, "exec", "--ephemeral", "--skip-git-repo-check",
        "--ignore-user-config", "--ignore-rules",
        "--sandbox", "read-only", "--color", "never", "--json",
        "--cd", str(workspace),
    ]
    env = os.environ.copy()
    env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost"
    env["DB_DIAG_CODEX_API_KEY"] = transport_token or llm["api_key"]
    settings = {
        "approval_policy": "never",
        "features.shell_tool": False, "features.unified_exec": False,
        "features.apply_patch_freeform": False, "features.multi_agent": False,
        "features.js_repl": False, "features.apps": False,
        "features.skills": False, "features.image_generation": False,
        # Chat gateways cannot execute Responses' native web_search tool.
        # Online search/fetch remain available through the scoped MCP server.
        "web_search": "disabled",
        "project_doc_max_bytes": 0,
        "model_provider": "db_diag",
        "model_providers.db_diag.name": "benchmark",
        "model_providers.db_diag.base_url": transport_url or llm["base_url"].rstrip("/"),
        "model_providers.db_diag.wire_api": "responses",
        "model_providers.db_diag.env_key": "DB_DIAG_CODEX_API_KEY",
        "model_providers.db_diag.request_max_retries": 0,
        "model_providers.db_diag.stream_max_retries": 0,
    }
    if context_path is not None:
        settings.update({
            "mcp_servers.diagnosis.command": sys.executable,
            "mcp_servers.diagnosis.args": [
                str(Path(__file__).with_name("diagnosis_mcp.py")), "--context", str(context_path),
            ],
            "mcp_servers.diagnosis.startup_timeout_sec": 60,
            "mcp_servers.diagnosis.tool_timeout_sec": 1900,
            "mcp_servers.diagnosis.required": True,
        })
    model = llm.get("codex_model") or llm.get("model_name")
    if model:
        command += ["--model", model]
    for key, value in settings.items():
        command += ["-c", key + "=" + json.dumps(value)]
    command += ["-"]
    return command, env
