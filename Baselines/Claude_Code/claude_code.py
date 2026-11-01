"""Database diagnosis baseline backed by the native Claude Agent SDK."""

from __future__ import annotations

import asyncio
import os
import json
import subprocess
import sys
from dataclasses import asdict, is_dataclass
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

try:
    from . import react
except ImportError:
    import react

if __package__:
    from . import native_agent
else:
    import native_agent


_PROXY_ENV_KEYS = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "all_proxy", "no_proxy",
)


def _openrouter_proxy_environment(llm):
    """Load only proxy variables from the configured scoped wrapper."""
    base = str(llm.get("base_url", ""))
    if urlparse(base).hostname != "openrouter.ai":
        return {}
    wrapper_value = llm.get(
        "openrouter_proxy_wrapper",
        "~/.local/bin/with-openrouter-proxy",
    )
    if not wrapper_value:
        return {}
    wrapper = Path(str(wrapper_value)).expanduser()
    if not wrapper.is_file() or not os.access(wrapper, os.X_OK):
        raise RuntimeError(f"OpenRouter proxy wrapper is unavailable or not executable: {wrapper}")
    probe = (
        "import json,os;"
        f"keys={_PROXY_ENV_KEYS!r};"
        "print(json.dumps({key:os.environ[key] for key in keys if os.environ.get(key)}))"
    )
    try:
        completed = subprocess.run(
            [str(wrapper), sys.executable, "-c", probe],
            text=True,
            capture_output=True,
            check=True,
            timeout=10,
        )
        values = json.loads(completed.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to load OpenRouter proxy environment from {wrapper}: {exc}") from exc
    proxy_env = {
        key: str(value)
        for key, value in values.items()
        if key in _PROXY_ENV_KEYS and value
    }
    if not (proxy_env.get("HTTPS_PROXY") or proxy_env.get("https_proxy")):
        raise RuntimeError(f"OpenRouter proxy wrapper did not provide HTTPS_PROXY: {wrapper}")
    return proxy_env


def build_options(llm, workspace, max_turns, context_path=None):
    from claude_agent_sdk import ClaudeAgentOptions, HookMatcher
    from Baselines.Codex.tools import SPECS

    timeout_seconds = float(llm.get("claude_api_timeout_seconds", 300))
    if not 0 < timeout_seconds < float("inf"):
        raise ValueError("claude_api_timeout_seconds must be positive and finite.")
    env = {
        "API_TIMEOUT_MS": str(max(1, int(timeout_seconds * 1000))),
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
        "CLAUDE_CODE_DISABLE_CLAUDE_MDS": "1",
        "CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS": "1",
        "CLAUDE_CODE_DISABLE_BACKGROUND_TASKS": "1",
    }
    env.update(_openrouter_proxy_environment(llm))
    key = llm.get("anthropic_api_key") or llm.get("api_key") or os.environ.get(llm.get("api_key_env", "ANTHROPIC_API_KEY"))
    if key:
        env["ANTHROPIC_API_KEY"] = key
        env["ANTHROPIC_AUTH_TOKEN"] = ""
    base = str(llm.get("anthropic_base_url") or llm.get("base_url", "")).rstrip("/")
    # Anthropic SDK appends /v1/messages itself; the supplied gateway URL uses /v1.
    if base.endswith("/v1"):
        base = base[:-3]
    if base:
        env["ANTHROPIC_BASE_URL"] = base
    servers = {}
    native_tools = []
    allowed = []
    if context_path is not None:
        # The controller context is not part of the agent-visible workspace.
        state = json.loads(Path(context_path).read_text())
        offered = set(SPECS) - {"agent_spawn", "agent_status"}
        if not state.get("enable_web_docs", True):
            offered -= {"web_search", "web_fetch"}
        native_tools = ["WebSearch", "WebFetch"]
        allowed = native_tools + ["mcp__diagnosis__" + name for name in sorted(offered)]
        servers["diagnosis"] = {
            "type": "stdio", "command": sys.executable,
            "args": [str(Path(__file__).with_name("diagnosis_mcp.py")), "--context", str(context_path)],
        }
        cli_home = Path(context_path).parent / "cli_home"
        cli_home.mkdir(mode=0o700, exist_ok=True)
        env["CLAUDE_CONFIG_DIR"] = str(cli_home)
        env["HOME"] = str(cli_home)
        runtime_dir = cli_home / "runtime"
        runtime_dir.mkdir(mode=0o700, exist_ok=True)
        env["XDG_RUNTIME_DIR"] = str(runtime_dir)

    async def guard_tool(data, tool_use_id, context):
        # PreToolUse runs even for allowed_tools: unknown/new SDK capabilities
        # cannot silently gain access to host files or a shell.
        del tool_use_id, context
        name = data.get("tool_name", "")
        if name not in allowed:
            return {"hookSpecificOutput": {
                "hookEventName": "PreToolUse", "permissionDecision": "deny",
                "permissionDecisionReason": "Only public research and sample-scoped MCP tools are permitted.",
            }}
        if name == "WebFetch":
            parsed = urlparse(str(data.get("tool_input", {}).get("url", "")))
            if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
                return {"hookSpecificOutput": {
                    "hookEventName": "PreToolUse", "permissionDecision": "deny",
                    "permissionDecisionReason": "WebFetch accepts public HTTP(S) documents only.",
                }}
        return {}

    return ClaudeAgentOptions(
        cwd=str(workspace), model=llm.get("claude_model") or llm["model_name"],
        system_prompt={"type": "preset", "preset": "claude_code", "append": native_agent.PROTOCOL},
        max_turns=max_turns, tools=native_tools, setting_sources=[], skills=[],
        mcp_servers=servers, strict_mcp_config=True,
        allowed_tools=allowed,
        disallowed_tools=["Bash", "Read", "Write", "Edit", "Glob", "Grep",
                          "NotebookEdit", "Agent", "Task", "Skill"],
        hooks={"PreToolUse": [HookMatcher(hooks=[guard_tool])]},
        permission_mode="dontAsk", env=env,
    )


@asynccontextmanager
async def _native_messages(prompt, options):
    """Own the SDK connection in the consuming task, including failure cleanup."""
    from claude_agent_sdk import ClaudeSDKClient

    async with ClaudeSDKClient(options=options) as client:
        await client.query(prompt)
        yield client.receive_response()


def _retry_error(data, options):
    status = data.get("error_status")
    if status is None:
        return (
            "Claude API transport error (retry cancelled): no HTTP response; "
            f"SDK error={data.get('error', 'unknown')}, "
            f"request timeout={options.env.get('API_TIMEOUT_MS', 'unset')} ms, "
            f"attempt={data.get('attempt')}. Check proxy connectivity or upstream latency."
        )
    return f"Claude API error (retry cancelled): HTTP {status}: {data.get('error')}"


def _check_system_error(message, options):
    if message.subtype == "api_retry":
        raise RuntimeError(_retry_error(message.data, options))
    if message.subtype == "model_refusal_no_fallback":
        raise RuntimeError(
            "Claude model refused the request: "
            f"category={message.data.get('api_refusal_category')}, "
            f"request_id={message.data.get('request_id')}"
        )


async def run_native_query(prompt, options, output):
    from claude_agent_sdk import AssistantMessage, ResultMessage, SystemMessage

    result_seen = False
    async with _native_messages(prompt, options) as messages:
        with (output / "native_events.jsonl").open("w", encoding="utf-8") as log:
            async for message in messages:
                log.write(json.dumps(asdict(message) if is_dataclass(message) else str(message), ensure_ascii=False, default=str) + "\n")
                log.flush()
                if isinstance(message, SystemMessage):
                    _check_system_error(message, options)
                if isinstance(message, AssistantMessage) and getattr(message, "error", None):
                    raise RuntimeError(f"Claude API error: {message.error}")
                if isinstance(message, ResultMessage):
                    result_seen = True
                    if message.is_error:
                        # Turn-budget exhaustion is not an API failure.
                        if message.subtype == "error_max_turns":
                            return
                        raise RuntimeError(f"Claude API/SDK error: {message.subtype}: {message.result}")
    if not result_seen:
        raise RuntimeError("Claude stream closed without a ResultMessage.")


def run_claude_agent(prompt, config, container, output, files, logger):
    context, workspace, prompt = native_agent.prepare(prompt, config, container, output, files)
    native_turns = int(config.get("max_turn_num", 20)) + int(
        config.get("claude_code", {}).get("max_aux_actions", 100)
    )
    options = build_options(config["llm_configuration"], workspace, native_turns, context)
    logger.info("[Claude Code] Starting native Agent SDK session with database MCP tools.")
    async def execute():
        await asyncio.wait_for(run_native_query(prompt, options, output), timeout=float(config.get("agent_timeout", 14400)))
    try:
        asyncio.run(execute())
    except Exception as exc:
        detail = str(exc)
        for name in ("api_key", "anthropic_api_key"):
            key = config["llm_configuration"].get(name)
            if key:
                detail = detail.replace(key, "<redacted>")
        native_agent.fatal_api_error(output, detail)
        raise RuntimeError(detail) from exc
    finally:
        context.unlink(missing_ok=True)
    return native_agent.finish_result(output)


SYSTEM_INSTRUCTION = """Act as the reasoning engine inside a controlled ReAct database
diagnosis loop. Do not inspect or modify local files and do not run commands yourself.
Use the supplied conversation and return exactly one <action> block matching the schema
specified by the original user prompt. The outer controller executes code actions and
returns their observations."""

DISALLOWED_TOOLS = [
    "Bash",
    "Edit",
    "Glob",
    "Grep",
    "Read",
    "WebFetch",
    "WebSearch",
    "Write",
]


def _conversation_prompt(messages: list[str]) -> str:
    transcript = []
    for index, message in enumerate(messages):
        role = "USER" if index % 2 == 0 else "ASSISTANT"
        transcript.append(f"===== {role} MESSAGE {index} =====\n{message}")
    return "\n\n".join(transcript)


async def _query_claude(prompt: str, llm_config: Dict[str, Any]) -> str:
    try:
        from claude_agent_sdk import AssistantMessage, ResultMessage, SystemMessage
    except ImportError as exc:
        raise RuntimeError(
            "Claude Agent SDK is unavailable. Install it with "
            "`python -m pip install claude-agent-sdk`."
        ) from exc

    options = build_options(llm_config, react.PROJECT_ROOT, int(llm_config.get("claude_max_turns", 1)))
    options.system_prompt = SYSTEM_INSTRUCTION
    options.disallowed_tools = DISALLOWED_TOOLS

    result = ""
    async with _native_messages(prompt, options) as messages:
        async for message in messages:
            if isinstance(message, SystemMessage):
                _check_system_error(message, options)
            if isinstance(message, AssistantMessage) and getattr(message, "error", None):
                raise RuntimeError(f"Claude API error: {message.error}")
            if isinstance(message, ResultMessage):
                if message.is_error:
                    raise RuntimeError(f"Claude API/SDK error: {message.subtype}: {message.result}")
                if message.result:
                    result = message.result
    if not result:
        raise RuntimeError("Claude Agent SDK returned no final result.")
    return result


def request_claude_code(
    messages: list[str],
    llm_config: Dict[str, Any],
    logger=None,
    tag: str = "[Claude Code Diagnosis]",
) -> str:
    model = llm_config.get("claude_model") or "configured default"
    if logger is not None:
        logger.info(f"{tag} Sending prompt through Claude Agent SDK (model=`{model}`) ...")
    response = asyncio.run(_query_claude(_conversation_prompt(messages), llm_config))
    if logger is not None:
        logger.info(f"{tag} Received Claude response ({len(response)} chars).")
    return response


if __name__ == "__main__":
    react.run(
        privileged=True,
        default_config=react.PROJECT_ROOT / "Configs" / "baselines" / "claude_code" / "example.json",
        description="Database diagnosis baseline backed by the native Claude Agent SDK.",
        agent_runner=run_claude_agent,
        workflow_type="claude_code",
    )
