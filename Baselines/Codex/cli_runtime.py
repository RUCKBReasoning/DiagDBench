from __future__ import annotations
import json
import os
import shutil
import sys
from pathlib import Path

def build_command(llm, workspace, context_path=None, *, transport_url=None, transport_token=None):
    """Use the configured Responses endpoint, without editing global Codex settings."""
    executable = shutil.which(str(llm.get("codex_executable", "codex")))
    if executable is None:
        raise RuntimeError("Codex CLI is not installed.")
    command = [
        executable, "exec", "--ephemeral", "--skip-git-repo-check",
        "--sandbox", "read-only", "--color", "never", "--json",
        "--cd", str(workspace),
    ]
    env = os.environ.copy()
    no_proxy = env.get("NO_PROXY", env.get("no_proxy", ""))
    env["NO_PROXY"] = env["no_proxy"] = no_proxy + ",127.0.0.1,localhost"
    provider = llm.get("provider", "codex")
    settings = {
        "approval_policy": "never",
        "features.shell_tool": False, "features.unified_exec": False,
        "features.apply_patch_freeform": False, "features.multi_agent": False,
        "web_search": "disabled", "project_doc_max_bytes": 0,
    }
    if provider != "codex_cli":
        command.append("--ignore-user-config")
        key = llm.get("codex_api_key") or llm.get("api_key") or os.environ.get(llm.get("api_key_env", "CODEX_API_KEY"))
        if key:
            env["DB_DIAG_CODEX_API_KEY"] = key
        settings.update({
            "model_provider": "db_diag",
            "model_providers.db_diag.name": str(provider),
            "model_providers.db_diag.base_url": llm["base_url"].rstrip("/"),
            "model_providers.db_diag.wire_api": "responses",
            "model_providers.db_diag.request_max_retries": 1,
            "model_providers.db_diag.stream_max_retries": 1,
        })
        if provider == "azure":
            # Match AzureOpenAI's base URL normalization used by llm_api.py.
            base = llm["base_url"].rstrip("/")
            if not base.endswith("/openai"):
                base += "/openai"
            settings["model_providers.db_diag.base_url"] = base
            settings["model_providers.db_diag.env_http_headers.api-key"] = "DB_DIAG_CODEX_API_KEY"
            settings["model_providers.db_diag.query_params.api-version"] = llm["api_version"]
            settings["model_providers.db_diag.requires_openai_auth"] = False
            no_proxy = env.get("NO_PROXY", env.get("no_proxy", ""))
            env["NO_PROXY"] = env["no_proxy"] = no_proxy + ",127.0.0.1,localhost"
        else:
            settings["model_providers.db_diag.env_key"] = "DB_DIAG_CODEX_API_KEY"
    model = llm.get("codex_model") or llm.get("model_name")
    if model:
        command.extend(["--model", model])
    if transport_url:
        # The local facade keeps upstream credentials/model in Python, while
        # Codex talks the native Responses protocol using an ephemeral token.
        settings = {k: v for k, v in settings.items() if not k.startswith("model_providers.db_diag.")}
        settings.update({
            "model_provider": "db_diag",
            "model_providers.db_diag.name": "configured_chat_gateway",
            "model_providers.db_diag.base_url": transport_url,
            "model_providers.db_diag.wire_api": "responses",
            "model_providers.db_diag.env_key": "DB_DIAG_CODEX_API_KEY",
            "model_providers.db_diag.request_max_retries": 0,
            "model_providers.db_diag.stream_max_retries": 0,
        })
        env["DB_DIAG_CODEX_API_KEY"] = transport_token
    if context_path is not None:
        settings.update({
            "mcp_servers.diagnosis.command": sys.executable,
            "mcp_servers.diagnosis.args": [str(Path(__file__).with_name("diagnosis_mcp.py")), "--context", str(context_path)],
            "mcp_servers.diagnosis.startup_timeout_sec": 60,
            "mcp_servers.diagnosis.tool_timeout_sec": 1900,
            "mcp_servers.diagnosis.required": True,
            "mcp_servers.diagnosis.enabled_tools": ["db_execute", "db_finish_diagnosis"],
            "mcp_servers.diagnosis.tools.db_execute.approval_mode": "approve",
            "mcp_servers.diagnosis.tools.db_finish_diagnosis.approval_mode": "approve",
        })
    for key, value in settings.items():
        command.extend(["-c", key + "=" + json.dumps(value)])
    command.append("-")
    return command, env
