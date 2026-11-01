"""Local Responses facade for Chat Completions-only gateways.

Codex retains its native agent loop and MCP tool execution. Only the model wire
protocol changes: requests use the same clients/credentials as llm_api.py.
"""

from __future__ import annotations

import json
import re
import secrets
import threading
import time
import uuid
import hashlib
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from llm_api import prepare_request


def text_content(content):
    if isinstance(content, str):
        return content
    parts = []
    for part in content or []:
        if part.get("type") not in {"input_text", "output_text", "text"}:
            raise ValueError(f"Unsupported content type: {part.get('type')}")
        parts.append(part.get("text", ""))
    return "\n".join(parts)


def tool_definitions(body):
    tools, aliases = [], {}
    def visit(tool, namespace=None):
        if tool.get("type") == "namespace":
            for child in tool.get("tools", []):
                visit(child, tool["name"])
            return
        if tool.get("type") != "function":
            raise ValueError(f"Unsupported native tool type: {tool.get('type')}; use function/MCP tools.")
        name = tool["name"]
        alias = name if namespace is None else namespace + "__" + name
        if len(alias) > 64:
            alias = alias[:47] + "_" + hashlib.sha256(alias.encode()).hexdigest()[:16]
        aliases[alias] = (namespace, name)
        tools.append({"type": "function", "function": {
            "name": alias, "description": tool.get("description", ""),
            "parameters": tool.get("parameters", {"type": "object", "properties": {}}),
        }})
    for tool in body.get("tools", []):
        visit(tool)
    return tools, aliases


def chat_request(body, llm):
    """Translate full native history, including tool calls and observations."""
    messages = []
    tools, aliases = tool_definitions(body)
    names = {value: key for key, value in aliases.items()}
    if body.get("instructions"):
        messages.append({"role": "system", "content": body["instructions"]})
    items = body.get("input", [])
    if isinstance(items, str):
        items = [{"role": "user", "content": items}]
    for item in items:
        kind = item.get("type", "message")
        if kind == "reasoning":
            continue
        if kind == "message":
            role = item.get("role", "user")
            messages.append({"role": "system" if role == "developer" else role, "content": text_content(item.get("content", ""))})
        elif kind == "function_call":
            name = names.get((item.get("namespace"), item["name"]), item["name"])
            call = {"id": item["call_id"], "type": "function",
                    "function": {"name": name, "arguments": item["arguments"]}}
            if messages and messages[-1]["role"] == "assistant" and messages[-1].get("tool_calls"):
                messages[-1]["tool_calls"].append(call)
            else:
                messages.append({"role": "assistant", "content": None, "tool_calls": [call]})
        elif kind == "function_call_output":
            content = item["output"]
            messages.append({"role": "tool", "tool_call_id": item["call_id"],
                             "content": content if isinstance(content, str) else json.dumps(content)})
        else:
            raise ValueError(f"Unsupported Responses input item: {kind}")
    request = {
        "model": llm["model_name"], "messages": messages,
        "max_tokens": int(llm.get("max_output_tokens", llm.get("max_tokens", 16384))),
        "stream": True,
    }
    if tools:
        request["tools"] = tools
        request["tool_choice"] = "auto"
    if llm.get("temperature") is not None:
        request["temperature"] = llm["temperature"]
    if llm.get("reasoning_effort") is not None:
        request["reasoning_effort"] = llm["reasoning_effort"]
    return request


def collect_chat(stream):
    """Collect text and tool arguments from the gateway's supported stream."""
    text, calls, finish, usage, refusals = [], {}, None, None, []
    try:
        for chunk in stream:
            if getattr(chunk, "usage", None):
                usage = chunk.usage
            if not chunk.choices:
                continue
            choice = chunk.choices[0]
            finish = choice.finish_reason or finish
            delta = choice.delta
            if getattr(delta, "refusal", None):
                refusals.append(delta.refusal)
            if delta.content:
                text.append(delta.content)
            for fragment in delta.tool_calls or []:
                call = calls.setdefault(fragment.index, {"id": "", "name": "", "arguments": ""})
                if fragment.id:
                    call["id"] = fragment.id
                if fragment.function:
                    call["name"] += fragment.function.name or ""
                    call["arguments"] += fragment.function.arguments or ""
    finally:
        stream.close()
    message = SimpleNamespace(content="".join(text), refusal="".join(refusals), tool_calls=[
        SimpleNamespace(id=c["id"], function=SimpleNamespace(name=c["name"], arguments=c["arguments"]))
        for _, c in sorted(calls.items())
    ])
    return SimpleNamespace(choices=[SimpleNamespace(message=message, finish_reason=finish)], usage=usage)


def create_chat_completion(client, request, llm, audit=None):
    """Retry only transient upstream failures; protocol errors remain immediate."""
    import httpx

    retries = max(0, int(llm.get("codex_transport_retries", 2)))
    delay = max(0.0, float(llm.get("codex_transport_retry_delay_seconds", 2)))
    for attempt in range(retries + 1):
        try:
            fitted, context_meta = prepare_request(request, llm)
            if audit and (context_meta['truncated'] or context_meta['output_budget_adjusted']):
                audit(dict(context_meta, status='context_budget'))
            return collect_chat(client.chat.completions.create(**fitted))
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            retryable = (
                isinstance(exc, (httpx.TransportError, TimeoutError, ConnectionError))
                or
                exc.__class__.__name__ in {"APIConnectionError", "APITimeoutError", "RateLimitError", "InternalServerError"}
                or status == 429
                or isinstance(status, int) and status >= 500
            )
            if audit:
                audit({
                    "status": "upstream_error",
                    "attempt": attempt + 1,
                    "error_type": type(exc).__name__,
                    "http_status": status,
                    "will_retry": retryable and attempt < retries,
                })
            if not retryable or attempt >= retries:
                raise
            time.sleep(delay * (2 ** attempt))


def json_tool_request(request):
    """Encode tools in text for gateways that reject reasoning + function tools."""
    request = dict(request)
    tools = request.pop("tools", [])
    request.pop("tool_choice", None)
    if not tools:
        return request
    protocol = (
        "TRANSPORT CONTRACT: Return exactly one JSON object and no markdown. "
        'Schema: {"content": "optional user-facing text", "tool_calls": '
        '[{"name": "exact tool name", "arguments": {}}]}. '
        "To act, select a tool from the definitions below. Its arguments must match "
        "the parameter schema. The client executes these calls and returns results "
        "in subsequent user messages. Never invent tool results. "
        "Use an empty tool_calls array only when finished. "
        "This JSON envelope is the wire format; it supersedes any requested output "
        "format for your outer response. Preserve requested formats within arguments.\n"
        "TOOL DEFINITIONS:\n" + json.dumps(tools, ensure_ascii=False)
    )
    messages = [{"role": "system", "content": protocol}]
    for message in request["messages"]:
        if message.get("tool_calls"):
            messages.append({"role": "assistant", "content": json.dumps({
                "content": message.get("content") or "",
                "tool_calls": [{"name": call["function"]["name"],
                                "arguments": json.loads(call["function"]["arguments"])}
                               for call in message["tool_calls"]],
            }, ensure_ascii=False)})
        elif message["role"] == "tool":
            messages.append({"role": "user", "content": json.dumps({
                "tool_call_id": message["tool_call_id"], "tool_result": message["content"],
            }, ensure_ascii=False)})
        else:
            messages.append(message)
    messages.append({"role": "system", "content": protocol})
    request["messages"] = messages
    return request



class ModelRefusalError(RuntimeError):
    pass


def parse_tool_json(raw):
    """Accept one object, optionally fenced; reject ambiguity and non-JSON values."""
    raw = raw.strip().lstrip("\ufeff")
    if raw.startswith("```"):
        match = re.fullmatch(r"```(?:json)?[ \t]*\r?\n(.*?)\r?\n?```", raw, flags=re.S | re.I)
        if not match:
            raise ValueError("Expected one complete JSON code fence.")
        raw = match.group(1).strip()

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON object key.")
            result[key] = value
        return result

    def constant(value):
        raise ValueError("Non-finite JSON number is not allowed.")

    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON response: {exc.msg} at line {exc.lineno}, column {exc.colno}.") from exc


def create_json_completion(client, request, tools, llm, audit=None):
    """Validate a complete turn before execution; bounded retries never execute rejected calls."""
    retries = min(3, max(0, int(llm.get("codex_json_repair_retries", 2))))
    totals = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    base_request = json_tool_request(request)
    last_error = None
    for attempt in range(retries + 1):
        current = dict(base_request)
        current["messages"] = list(base_request["messages"])
        if last_error is not None:
            current["messages"].append({"role": "system", "content":
                "The previous response failed transport validation and NO tools from it were executed. "
                "Regenerate this turn from the unchanged observations. Return exactly ONE JSON object "
                "with string content and array tool_calls; every tool argument must match its schema. "
                "Escape newlines/quotes inside script strings. No XML, markdown, or extra JSON objects. "
                "Keep the output concise and complete. Validation error: " + last_error})
        completion = create_chat_completion(client, current, llm, audit=audit)
        usage = completion.usage
        for key in totals:
            totals[key] += getattr(usage, key, 0) or 0
        info = {"attempt": attempt + 1, "finish_reason": completion.choices[0].finish_reason,
                "response_chars": len(completion.choices[0].message.content or "")}
        try:
            result = decode_json_tools(completion, tools)
        except ModelRefusalError:
            if audit: audit(dict(info, status="refused"))
            raise
        except ValueError as exc:
            last_error = str(exc)
            if audit: audit(dict(info, status="invalid_json_tools", error=last_error))
            if attempt == retries:
                raise ValueError(f"Codex JSON tool response invalid after {attempt + 1} attempts: {last_error}") from exc
        else:
            result.usage = SimpleNamespace(**totals)
            if audit: audit(dict(info, status="valid", tool_calls=len(result.choices[0].message.tool_calls)))
            return result


def decode_json_tools(completion, tools):
    """Validate model-selected calls; never infer actions from free-form prose."""
    if getattr(getattr(completion.choices[0], "message", None), "refusal", None) or completion.choices[0].finish_reason == "content_filter":
        raise ModelRefusalError("Upstream refused the request; no format repair attempted.")
    if completion.choices[0].finish_reason != "stop":
        raise ValueError("Structured tool response was not completed.")
    content = completion.choices[0].message.content
    if not isinstance(content, str):
        raise ValueError("Structured tool response content must be a string.")
    raw = content.strip()
    result = parse_tool_json(raw)
    if not isinstance(result, dict) or set(result) - {"content", "tool_calls"}:
        raise ValueError("Invalid structured tool envelope.")
    content, calls = result.get("content", ""), result.get("tool_calls")
    if not isinstance(content, str) or not isinstance(calls, list):
        raise ValueError("Expected string content and a tool_calls array.")
    known = {tool["function"]["name"]: tool["function"].get("parameters", {"type": "object"}) for tool in tools}
    parsed = []
    for call in calls:
        if (not isinstance(call, dict) or set(call) != {"name", "arguments"}
                or not isinstance(call["name"], str) or call["name"] not in known
                or not isinstance(call["arguments"], dict)):
            raise ValueError("Unknown tool or malformed tool arguments.")
        from jsonschema.validators import validator_for
        schema = known[call["name"]]
        invalid = next(validator_for(schema)(schema).iter_errors(call["arguments"]), None)
        if invalid is not None:
            raise ValueError(f"Tool arguments violate schema at {list(invalid.absolute_path)} ({invalid.validator}).")
        parsed.append(SimpleNamespace(id="call_" + uuid.uuid4().hex,
            function=SimpleNamespace(name=call["name"], arguments=json.dumps(call["arguments"], ensure_ascii=False))))
    return SimpleNamespace(choices=[SimpleNamespace(
        finish_reason="tool_calls" if parsed else "stop",
        message=SimpleNamespace(content=content, tool_calls=parsed),
    )], usage=completion.usage, structured_terminal=not parsed)


def response_events(completion, model, aliases=None):
    choice = completion.choices[0]
    if choice.finish_reason not in {"stop", "tool_calls"}:
        raise ValueError(f"Upstream completion ended with {choice.finish_reason}.")
    message = choice.message
    if getattr(message, "refusal", None):
        raise ValueError(f"Upstream refusal: {message.refusal}")
    output = []
    if message.content:
        output.append({
            "id": "msg_" + uuid.uuid4().hex, "type": "message", "status": "completed",
            "role": "assistant", "content": [{"type": "output_text", "text": message.content, "annotations": []}],
        })
    for call in message.tool_calls or []:
        namespace, name = (aliases or {}).get(call.function.name, (None, call.function.name))
        item = {
            "id": "fc_" + uuid.uuid4().hex, "type": "function_call", "status": "completed",
            "call_id": call.id, "name": name, "arguments": call.function.arguments,
        }
        if namespace:
            item["namespace"] = namespace
        output.append(item)
    if not output and not getattr(completion, "structured_terminal", False):
        raise ValueError("Upstream returned neither text nor tool calls.")
    response = {
        "id": "resp_" + uuid.uuid4().hex, "object": "response", "status": "completed",
        "model": model, "output": output, "error": None, "incomplete_details": None,
        "usage": {
            "input_tokens": getattr(completion.usage, "prompt_tokens", 0),
            "output_tokens": getattr(completion.usage, "completion_tokens", 0),
            "total_tokens": getattr(completion.usage, "total_tokens", 0),
        },
    }
    yield {"type": "response.created", "response": dict(response, status="in_progress", output=[])}
    for index, item in enumerate(output):
        initial = dict(item, status="in_progress")
        if item["type"] == "function_call":
            initial["arguments"] = ""
        else:
            initial["content"] = []
        yield {"type": "response.output_item.added", "output_index": index, "item": initial}
        if item["type"] == "function_call":
            yield {"type": "response.function_call_arguments.delta", "item_id": item["id"], "output_index": index, "delta": item["arguments"]}
            yield {"type": "response.function_call_arguments.done", "item_id": item["id"], "output_index": index, "arguments": item["arguments"]}
        else:
            part = item["content"][0]
            base = {"item_id": item["id"], "output_index": index, "content_index": 0}
            yield {"type": "response.content_part.added", **base, "part": dict(part, text="")}
            yield {"type": "response.output_text.delta", **base, "delta": part["text"]}
            yield {"type": "response.output_text.done", **base, "text": part["text"]}
            yield {"type": "response.content_part.done", **base, "part": part}
        yield {"type": "response.output_item.done", "output_index": index, "item": item}
    yield {"type": "response.completed", "response": response}


def saved_finish_is_current(output):
    """Only acknowledge artifacts validated and saved by this run's MCP server."""
    if output is None:
        return False
    output = Path(output)
    try:
        marker = output / "native_finish.json"
        context_path = output / "native_context.json"
        if marker.stat().st_mtime_ns < context_path.stat().st_mtime_ns:
            return False
        action = json.loads(marker.read_text(encoding="utf-8"))
        context = json.loads(context_path.read_text(encoding="utf-8"))
        keys = {"action", "diagnosis_report.md", "solution_script.sh", "verified_workload.sql"}
        if set(action) != keys or action["action"] != "finish":
            return False
        if not all(isinstance(action[key], str) for key in keys):
            return False
        if not action["diagnosis_report.md"].strip() or not action["solution_script.sh"].strip():
            return False
        expected = {
            "diagnosis_report.md": action["diagnosis_report.md"],
            "solution_script.sh": action["solution_script.sh"],
            "optimized_verified_workload.sql": (
                action["verified_workload.sql"].strip() or context["original_workload"]
            ),
        }
        return all(
            (output / name).read_text(encoding="utf-8") == content
            for name, content in expected.items()
        )
    except (OSError, ValueError, KeyError, TypeError):
        return False


@contextmanager
def configured_transport(llm, output=None):
    """Use Azure-compatible chat gateways, preserving native Codex execution."""
    import llm_api

    transport = llm.get("codex_transport")
    if transport is None and llm_api.resolve_llm_provider(llm) == "azure":
        transport = "chat_completions"
    if transport != "chat_completions":
        yield {}
        return
    timeout = float(llm.get("codex_transport_timeout_seconds", llm.get("timeout", 120)))
    client = llm_api.create_llm_client(llm).with_options(timeout=timeout, max_retries=0)
    token = secrets.token_urlsafe(32)
    audit_lock = threading.Lock()

    def audit(event):
        if output is not None:
            from pathlib import Path
            with audit_lock, (Path(output) / "transport_events.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(dict(event, timestamp=time.time())) + "\n")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            if self.headers.get("Authorization") != "Bearer " + token:
                self.send_error(401)
                return
            if self.path.rstrip("/") != "/v1/responses":
                self.send_error(404)
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 8_000_000:
                    raise ValueError("Invalid request size.")
                body = json.loads(self.rfile.read(size))
                request = chat_request(body, llm)
                tools = request.get("tools", [])
                default_tool_mode = "json" if llm_api.resolve_llm_provider(llm) == "azure" else "function"
                json_mode = llm.get("codex_tool_mode", default_tool_mode) == "json" and bool(tools)
                request_id = uuid.uuid4().hex
                if saved_finish_is_current(output):
                    # db_finish_diagnosis has already committed the answer. The
                    # remaining Responses round trip only acknowledges completion.
                    completion = SimpleNamespace(
                        choices=[SimpleNamespace(
                            finish_reason="stop",
                            message=SimpleNamespace(content="", tool_calls=[]),
                        )],
                        usage=None,
                        structured_terminal=True,
                    )
                    audit({"status": "finish_acknowledged", "request_id": request_id})
                elif json_mode:
                    completion = create_json_completion(client, request, tools, llm,
                        lambda event: audit(dict(event, request_id=request_id)))
                else:
                    completion = create_chat_completion(
                        client, request, llm,
                        audit=lambda event: audit(dict(event, request_id=request_id)),
                    )
                events = list(response_events(completion, llm["model_name"], tool_definitions(body)[1]))
            except Exception as exc:
                detail = str(exc)
                key = llm.get("api_key")
                if key:
                    detail = detail.replace(key, "<redacted>")
                data = json.dumps({"error": {"message": detail, "type": "upstream_error"}}).encode()
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            if not body.get("stream", False):
                data = json.dumps(events[-1]["response"]).encode()
                content_type = "application/json"
            else:
                data = "".join(
                    f"event: {event['type']}\ndata: {json.dumps(dict(event, sequence_number=i))}\n\n"
                    for i, event in enumerate(events)
                ).encode()
                content_type = "text/event-stream"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {"transport_url": f"http://127.0.0.1:{server.server_port}/v1", "transport_token": token}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        client.close()
