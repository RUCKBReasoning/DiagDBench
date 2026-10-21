"""Reuse Codex's gateway in an isolated module; constrain tool/model budgets."""
from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import threading
import time


@contextmanager
def restricted_transport(llm: dict, output: Path, allowed: set[str], max_calls: int, max_tokens: int):
    path = Path(__file__).with_name("chat_transport.py")
    spec = importlib.util.spec_from_file_location("_codex_transport", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    original_definitions = module.tool_definitions
    original_completion = module.create_chat_completion
    lock = threading.Lock()
    usage = {"requests": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    definitions_log = output / "offered_tools.jsonl"

    def permitted(name: str, namespace: str | None) -> bool:
        # MCP's name on the native client and plain function names on our own
        # child client. No unrelated native tool is forwarded to the model.
        return ((namespace in (None, "mcp__diagnosis") and name in allowed)
                or (namespace is None and name.startswith("mcp__diagnosis__")
                    and name[len("mcp__diagnosis__"):] in allowed))

    def definitions(body):
        def filtered(tool):
            if tool.get("type") == "namespace":
                children = [c for c in tool.get("tools", []) if c.get("type") == "function"
                            and permitted(c.get("name", ""), tool["name"])]
                return dict(tool, tools=children) if children else None
            if tool.get("type") == "function" and permitted(tool.get("name", ""), None):
                return tool
            return None
        approved = [v for t in body.get("tools", []) if (v := filtered(t)) is not None]
        if body.get("tools") and not approved:
            raise ValueError("No approved Codex MCP tools were offered; aborting.")
        reduced = dict(body, tools=approved)
        result = original_definitions(reduced)
        with lock, definitions_log.open("a") as log:
            log.write(json.dumps({"time": time.time(), "approved": list(result[1])}) + "\n")
        return result

    def complete(client, request, settings, audit=None):
        with lock:
            if usage["requests"] >= max_calls or usage["total_tokens"] >= max_tokens:
                raise ValueError("Shared Codex model budget exhausted.")
            usage["requests"] += 1
        response = original_completion(client, request, settings, audit=audit)
        with lock:
            for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
                usage[key] += getattr(response.usage, key, 0) or 0
            (output / "model_usage.json").write_text(json.dumps(usage, indent=2))
        return response

    module.tool_definitions = definitions
    module.create_chat_completion = complete
    settings = dict(llm, codex_transport="chat_completions")
    settings.update(request_retry_times=min(3, int(settings.get("request_retry_times", 3))),
                    retry_times=min(4, int(settings.get("retry_times", 4))),
                    sdk_max_retries=0, timeout=min(300, float(settings.get("timeout", 300))),
                    codex_json_repair_retries=1)
    try:
        with module.configured_transport(settings, output) as transport:
            yield transport
    finally:
        (output / "model_usage.json").write_text(json.dumps(usage, indent=2))
