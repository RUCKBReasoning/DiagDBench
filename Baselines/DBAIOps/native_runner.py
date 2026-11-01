"""Headless DiagDBench adapter for DBAIOps knowledge Q&A, not the full extension."""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import os
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from Baselines.input_contract import DIAGNOSIS_REQUIREMENTS
from llm_api import prepare_request
from Baselines.recovery_artifacts import (
    VERSION as RECOVERY_CONVERSION_VERSION, build_recovery_prompt, collect_public_evidence,
    conversion_metadata, effective_workload, validate_artifacts,
)


DBMS_DATASETS = {"pg": "2104", "mysql": "2102"}
UPSTREAM_RULES = (
    Path(__file__).resolve().parent
    / "DBAIOps_v1.1.1"
    / "js"
    / "popup"
    / "managers"
    / "parameter-rules-manager.js"
)
DBMS_NAMES = {"pg": "PG兼容生态", "mysql": "MySQL兼容"}


def load_precise_rule() -> dict[str, Any]:
    source = UPSTREAM_RULES.read_text(encoding="utf-8")
    match = re.search(
        r'"id":\s*"default-fast-search".*?'
        r'"similarity":\s*([0-9.]+).*?'
        r'"topN":\s*([0-9]+).*?'
        r'"temperature":\s*([0-9.]+).*?'
        r'"prompt":\s*("(?:\\.|[^"\\])*")',
        source,
        re.DOTALL,
    )
    if match is None:
        raise RuntimeError("Cannot load DBAIOps default-fast-search rule")
    return {
        "id": "default-fast-search",
        "similarity": float(match.group(1)),
        "topn": int(match.group(2)),
        "temperature": float(match.group(3)),
        "prompt": json.loads(match.group(4)),
    }


def read_text(path: Path, limit: int) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[:limit] + ("\n...[truncated]" if len(text) > limit else "")


def merged_workload(sample_dir: Path, limit: int) -> str:
    initialization = read_text(sample_dir / "initialization_workload.sql", limit).strip()
    verified = read_text(sample_dir / "verified_workload.sql", limit).strip()
    return (
        "-- Preparation phase: execute first\n"
        + initialization
        + "\n\n--- anomaly-verified workload phase boundary ---\n\n"
        + "-- Anomaly verification phase: execute second\n"
        + verified
    ).strip()


class PermanentHTTPError(RuntimeError):
    """A request error that must not be retried."""


class RetryableModelResponseError(RuntimeError):
    """Empty or truncated text; limited separately from network failures."""


def request_json(
    url: str,
    payload: dict[str, Any],
    api_key: str,
    *,
    api_version: str = "",
    timeout: int = 300,
    attempts: int = 1,
    retry_delay: float = 30,
    validate_response=None,
    response_attempts: int = 3,
    prepare_payload=None,
) -> dict[str, Any]:
    if attempts < 1 or timeout <= 0 or retry_delay < 0:
        raise ValueError("Invalid request retry budget")
    if api_version:
        separator = "&" if "?" in url else "?"
        url += separator + "api-version=" + urllib.parse.quote(api_version)
    invalid_responses = 0
    for attempt in range(1, attempts + 1):
        # Recompute after each output-budget change; knowledge-service payloads are untouched.
        outgoing = prepare_payload(payload) if prepare_payload is not None else payload
        request = urllib.request.Request(
            url,
            data=json.dumps(outgoing, ensure_ascii=False).encode("utf-8"),
            headers={
                "Authorization": "Bearer " + api_key,
                "api-key": api_key,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                data = json.loads(response.read().decode("utf-8", errors="replace"))
            if validate_response is not None:
                validate_response(data)
            return data
        except RetryableModelResponseError as exc:
            invalid_responses += 1
            detail = str(exc)
            if invalid_responses >= response_attempts:
                raise RuntimeError(
                    f"Model response invalid after {invalid_responses} responses: {detail}"
                ) from exc
            failure = exc
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = exc.read().decode("utf-8", errors="replace")[:1000]
            exc.close()
            detail = f"HTTP {status} from {url}: {body}".replace(api_key, "<redacted>") if api_key else f"HTTP {status} from {url}: {body}"
            if status not in {408, 429, 500, 502, 503, 504}:
                raise PermanentHTTPError(detail) from exc
            failure = exc
        except (urllib.error.URLError, TimeoutError, ConnectionError, ssl.SSLError,
                http.client.HTTPException) as exc:
            if isinstance(exc, ssl.SSLCertVerificationError) or isinstance(
                getattr(exc, "reason", None), ssl.SSLCertVerificationError
            ):
                raise PermanentHTTPError("TLS certificate verification failed") from exc
            detail = f"Cannot reach {url}: {exc}"
            if api_key:
                detail = detail.replace(api_key, "<redacted>")
            failure = exc
        if attempt == attempts:
            raise RuntimeError(f"Request failed after {attempts} attempts: {detail}") from failure
        logging.getLogger(__name__).warning(
            "Transient request failure (%s), attempt %s/%s; retrying in %ss",
            type(failure).__name__, attempt, attempts, retry_delay,
        )
        time.sleep(retry_delay)


def retrieve_knowledge(
    question: str,
    *,
    url: str,
    api_key: str,
    dataset_id: str,
    retries: int,
    similarity: float,
    topn: int,
    temperature: float,
    fallback_dir: Path | None = None,
    dbms: str = "",
) -> tuple[list[str], list[dict[str, Any]]]:
    # Keep the old keyword signature, but do not substitute a different method.
    if fallback_dir is not None:
        logging.getLogger(__name__).warning(
            "fallback_dir is deprecated and ignored; native DBAIOps uses only the remote knowledge service"
        )
    if retries < 1:
        raise ValueError("Knowledge request attempts must be positive")
    payload = {
        "question": question,
        "similarity": similarity,
        "topn": topn,
        "dataset_name": dataset_id,
        "temperature": temperature,
        "language": "中文",
        "isSupportImg": "true",
    }
    response = request_json(url, payload, api_key, attempts=retries, retry_delay=1)
    if not isinstance(response, dict) or str(response.get("status")) != "200":
        message = response.get("message", "unknown error") if isinstance(response, dict) else "invalid response"
        raise RuntimeError("DBAIOps knowledge service rejected the request: " + str(message))
    data = response.get("data")
    # streamChat requests the image-aware response schema. Do not reinterpret an
    # empty response as permission to use a different method's local documents.
    items = data.get("dataList") if isinstance(data, dict) else None
    if not isinstance(items, list) or not items:
        raise RuntimeError("DBAIOps knowledge service returned no matched documents; diagnosis stopped")
    return [item if isinstance(item, str) else _js_string(item) for item in items], [response]


def _js_string(value: Any) -> str:
    """Match the extension's String(item) conversion for JSON service values."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, dict):
        return "[object Object]"
    if isinstance(value, list):
        return ",".join("" if item is None else _js_string(item) for item in value)
    return str(value)


def build_native_messages(question, knowledge, dbms, rule):
    """Port callOllamaAPI's Chinese text-Q&A prompt construction.

    There is no browser page in this adapter; about:blank is a neutral placeholder.
    Benchmark evidence/safety constraints remain explicit additions at the end.
    """
    locale = json.loads((UPSTREAM_RULES.parents[3] / "i18n/locales/zhcn.json").read_text(encoding="utf-8"))
    general = locale["popup.system.aiAssistantPromptGeneral"]
    rule_prompt = rule["prompt"]
    if not re.search(r"[\u4e00-\u9fff]", question):
        instruction = (
            "如果用户的问题不是中文，请先将其翻译成中文仅用于检索和理解，但务必继续使用用户原始语言进行思考与推理，"
            "最终将思考和回答等一切文字请翻译成中文输出。"
        )
        general = instruction + "\n\n" + general
        rule_prompt = instruction + "\n\n" + rule_prompt
    system_prompt = (
        rule_prompt + "\n\n" + general + "\n\n"
        + locale["popup.system.knowledgeBasePrompt"].replace("{{name}}", DBMS_NAMES[dbms])
    )
    content = "\n\n".join(knowledge)
    if "相似度：" in content or "版本：" in content:
        context = locale["popup.context.kbQueryResult"] + ":\n" + content
    else:
        context = (locale["popup.context.pageUrl"] + ": about:blank\n"
                   + locale["popup.context.pageContent"] + ": " + content[:2000] + "...")
    context = (locale["popup.context.baseContent"] + ":\n\n" + context
               + "\n\n" + locale["popup.context.question"] + ":" + question)
    system_prompt += (
        "\n\n评测补充约束：区分事实、推断和待验证项；不得虚构指标、"
        "执行结果、异常注入方式或已完成的恢复操作。" + DIAGNOSIS_REQUIREMENTS
    )
    return [{"role": "system", "content": system_prompt}, {"role": "user", "content": context}]


def chat_endpoint(url: str, model: str, provider: str = "") -> str:
    """Resolve Azure deployment routes without changing OpenAI-compatible routes."""
    parts = urllib.parse.urlsplit(url)
    path = parts.path.rstrip("/")
    host = (parts.hostname or "").lower()
    azure = provider in {"azure", "azure_openai"}
    if provider not in {"aiskt", "ark", "openai", "openrouter", "openai_compatible"}:
        azure = azure or "/openai/deployments/" in path or "/modelhub/" in path
        azure = azure or host.endswith(".openai.azure.com") or host.endswith(".cognitiveservices.azure.com")
    if not path.endswith("/chat/completions"):
        if azure and "/openai/deployments/" not in path:
            if not path.endswith("/openai"):
                path += "/openai"
            path += "/deployments/" + urllib.parse.quote(model, safe="")
        path += "/chat/completions"
    return urllib.parse.urlunsplit(parts._replace(path=path))


def chat_completion(
    messages: list[dict[str, str]],
    *,
    url: str,
    api_key: str,
    model: str,
    api_version: str,
    temperature: float,
    max_tokens: int,
    provider: str = "",
    request_attempts: int = 1,
    request_delay: float = 30,
    request_timeout: int = 300,
    allow_context_truncation: bool = True,
) -> tuple[str, dict[str, Any]]:
    endpoint = chat_endpoint(url, model, provider)
    payload = {
        "model": model, "messages": messages, "max_tokens": max_tokens,
        "temperature": temperature, "stream": False,
    }

    def validate(response):
        choices = response.get("choices") or []
        first = choices[0] if choices else {}
        message = first.get("message") or {}
        if message.get("refusal") or first.get("finish_reason") == "content_filter":
            raise RuntimeError("Model refused the response; no automatic response retry")
        content = message.get("content") or response.get("response") or response.get("content") or ""
        if first.get("finish_reason") == "length" or not isinstance(content, str) or not content.strip():
            previous_limit = payload["max_tokens"]
            payload["max_tokens"] = max(previous_limit, min(32768, previous_limit * 2))
            summary = {
                "finish_reason": first.get("finish_reason"),
                "usage": response.get("usage"),
                "previous_max_tokens": previous_limit, "next_max_tokens": payload["max_tokens"],
            }
            logging.getLogger(__name__).warning("Incomplete model text: %s", json.dumps(summary))
            raise RetryableModelResponseError(
                "Model response was empty or truncated (finish_reason=%s)" % first.get("finish_reason")
            )
    response = request_json(
        endpoint,
        payload,
        api_key,
        api_version=api_version,
        attempts=max(3, request_attempts),
        retry_delay=request_delay,
        timeout=request_timeout,
        validate_response=validate,
        prepare_payload=lambda value: prepare_request(value, {
            'model_name': model, 'allow_context_truncation': allow_context_truncation,
        })[0],
    )
    choices = response.get("choices") or []
    content = choices[0].get("message", {}).get("content", "") if choices else ""
    content = content or response.get("response") or response.get("content") or ""
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("Model response did not contain text")
    return content.strip(), response


def extract_object(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", text):
        try:
            value, _ = decoder.raw_decode(text[match.start():])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise ValueError("Model response does not contain a JSON object")


def validate_action(action: dict[str, Any]) -> dict[str, str]:
    expected = {
        "action", "diagnosis_report.md", "solution_script.sh",
        "verified_workload.sql",
    }
    if not isinstance(action, dict) or set(action) != expected or action.get("action") != "finish":
        raise ValueError("Artifact response does not match the finish schema")
    for key in expected - {"action"}:
        if not isinstance(action[key], str):
            raise ValueError(f"{key} must be a string")
    if not action["diagnosis_report.md"].strip():
        raise ValueError("Diagnosis report is empty")
    validate_artifacts({key: action[key] for key in ("solution_script.sh", "verified_workload.sql")})
    return {key: str(action[key]) for key in expected}


def normalize_artifact(action: dict[str, Any], native_report: str) -> dict[str, str]:
    """Wrap two-field artifacts (or legacy finish responses) with the frozen report."""
    if not isinstance(action, dict):
        raise ValueError("Artifact response must be an object")
    declared = action.get("action")
    if declared is None:
        declared = "finish"
    if not isinstance(declared, str) or declared.strip().lower() != "finish":
        raise ValueError(
            "Artifact action must be 'finish'; regenerate final artifacts, not an intermediate action"
        )
    if "solution_script.sh" not in action:
        raise ValueError("Missing required solution_script.sh; generate the recovery script")
    workload = action.get("verified_workload.sql")
    normalized = {
        "action": "finish",
        # The native diagnosis is authoritative, including when the model omits
        # or rewrites this field. Extra commentary fields are not finish fields.
        "diagnosis_report.md": native_report,
        "solution_script.sh": action["solution_script.sh"],
        "verified_workload.sql": "" if workload is None else workload,
    }
    return validate_action(normalized)


def atomic_write(path: Path, content: str, mode: int | None = None) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    if mode is not None:
        temporary.chmod(mode)
    temporary.replace(path)


def generate_artifact_action(
    args: argparse.Namespace, prompt: str, native_report: str, output_dir: Path,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Bounded schema repair; preserve every returned response before validation."""
    repairs = getattr(args, "artifact_retries", 2)
    if not 0 <= repairs <= 3:
        raise ValueError("artifact_retries must be between 0 and 3")
    attempts = []
    correction = ""
    for index in range(repairs + 1):
        record = {"attempt": index + 1, "status": "requesting"}
        attempts.append(record)
        path = output_dir / "artifact_attempts.json"
        atomic_write(path, json.dumps(attempts, ensure_ascii=False, indent=2) + "\n")
        try:
            text, response = chat_completion(
                [{"role": "user", "content": prompt + correction}],
                url=args.model_url, api_key=args.model_api_key, model=args.model,
                api_version=args.api_version, temperature=args.temperature,
                max_tokens=args.max_tokens,
                provider=getattr(args, "provider", ""),
                request_attempts=getattr(args, "model_request_attempts", 1),
                request_delay=getattr(args, "model_request_delay", 30),
                request_timeout=getattr(args, "model_request_timeout", 300),
                allow_context_truncation=False,
            )
        except Exception as exc:
            # Transport errors remain errors, not schema corrections.
            detail = str(exc).replace(args.model_api_key, "<redacted>") if args.model_api_key else str(exc)
            record.update(status="request_failed", error=detail)
            atomic_write(path, json.dumps(attempts, ensure_ascii=False, indent=2) + "\n")
            raise
        record.update(status="received", raw_text=text, response=response)
        atomic_write(path, json.dumps(attempts, ensure_ascii=False, indent=2) + "\n")
        try:
            for choice in response.get("choices", []):
                if choice.get("finish_reason") == "content_filter":
                    raise ValueError("Artifact model response was filtered")
                if choice.get("finish_reason") == "length":
                    raise ValueError("Artifact model response was not completed")
            action = normalize_artifact(extract_object(text), native_report)
        except (ValueError, subprocess.SubprocessError) as exc:
            record.update(status="invalid", error=str(exc))
            atomic_write(path, json.dumps(attempts, ensure_ascii=False, indent=2) + "\n")
            if index == repairs:
                raise ValueError(
                    f"Artifact validation failed after {index + 1} attempts: {exc}; see {path}"
                ) from exc
            correction = (
                "\n\nThe previous artifact response failed validation. No generated script "
                "was executed. Keep the native diagnosis unchanged and regenerate one complete "
                "JSON object with exactly solution_script.sh and "
                "verified_workload.sql. Include a valid Bash script starting with "
                "'# TIMEOUT THRESHOLD: <positive integer at most 1800>'. Use installed psql/mysql "
                "clients for guarded target-database recovery, not an advisory-only refusal. Any non-empty optimized "
                "workload must retain exactly the same statements in exactly the same "
                "order and produce exactly the same sequential execution results as the "
                "original workload; do not add, delete, merge, split, replace, or reorder "
                "statements. Any result mismatch is a diagnosis failure. Return empty SQL "
                "unless exact equivalence and a potential performance benefit can be "
                "guaranteed. Do not return code/continue/tool actions. Output JSON only.\n"
                "Validation error: " + str(exc)
            )
            continue
        record["status"] = "valid"
        atomic_write(path, json.dumps(attempts, ensure_ascii=False, indent=2) + "\n")
        return action, response
    raise AssertionError("unreachable")


def run(args: argparse.Namespace) -> None:
    if getattr(args, "local_knowledge_dir", None) is not None:
        logging.getLogger(__name__).warning(
            "--local-knowledge-dir is deprecated and ignored; no automatic knowledge fallback is enabled"
        )
    sample_dir = args.sample_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    limit = args.context_chars
    workload = merged_workload(sample_dir, limit)
    symptoms = read_text(sample_dir / "user_phenomenon_description.txt", limit)
    snapshot = read_text(args.snapshot, limit) if args.snapshot.is_file() else ""
    rule = load_precise_rule()
    question = f"""目标 DBMS：{args.dbms}

## 用户观测到的异常现象
{symptoms}

## Anomaly-verified workload
{workload}

## 当前只读运行时观测
{snapshot}

请基于知识库和以上可观测证据诊断根因。不得假设未提供的异常定义、注入方式或标准答案。"""
    knowledge, knowledge_responses = retrieve_knowledge(
        question,
        url=args.knowledge_url,
        api_key=args.knowledge_api_key,
        dataset_id=args.dataset_id,
        retries=args.retries,
        similarity=rule["similarity"],
        topn=rule["topn"],
        temperature=rule["temperature"],
    )
    native_messages = build_native_messages(question, knowledge, args.dbms, rule)
    native_report, diagnosis_response = chat_completion(
        native_messages,
        url=args.model_url,
        api_key=args.model_api_key,
        model=args.model,
        api_version=args.api_version,
        # Upstream callOllamaAPI applies the selected rule to the model as well.
        temperature=rule["temperature"],
        max_tokens=args.max_tokens,
        provider=getattr(args, "provider", ""),
        request_attempts=getattr(args, "model_request_attempts", 1),
        request_delay=getattr(args, "model_request_delay", 30),
        request_timeout=getattr(args, "model_request_timeout", 300),
    )
    # Canonical native output is frozen once; the converter never rewrites it.
    native_report = native_report.rstrip() + "\n"
    # Persist the native method's output before the fallible evaluation adapter.
    atomic_write(output_dir / "native_diagnosis_report.md", native_report)
    trace = {
        "method": "DBAIOps headless knowledge-Q&A adapter",
        "upstream_commit": "86fa8bfaa32eb55f227370285ec09ae4b7f32ee9",
        "adapter_scope": "Chinese text Q&A; no browser/AWR/inspection/image rendering",
        "dataset_id": args.dataset_id,
        "model_temperature": rule["temperature"],
        "artifact_temperature": args.temperature,
        "parameter_rule": {key: value for key, value in rule.items() if key != "prompt"},
        "native_messages": native_messages,
        "knowledge_items": knowledge,
        "knowledge_responses": knowledge_responses,
        "knowledge_source": "remote_dbaiops",
        "diagnosis_response": diagnosis_response,
        "artifact_status": "pending",
        "recovery_conversion_version": RECOVERY_CONVERSION_VERSION,
    }
    atomic_write(output_dir / "native_trace.json", json.dumps(trace, ensure_ascii=False, indent=2) + "\n")
    try:
        public_evidence = collect_public_evidence(sample_dir, args.dbms)
        artifact_prompt = build_recovery_prompt({
            "native_diagnosis_report": native_report, "public_evidence": public_evidence,
        }, DIAGNOSIS_REQUIREMENTS)
        atomic_write(output_dir / "conversion_prompt.txt", artifact_prompt, 0o600)
        action, artifact_response = generate_artifact_action(
            args, artifact_prompt, native_report, output_dir,
        )
        original_verified = public_evidence["public_inputs"]["verified_workload.sql"]
        optimized = effective_workload(action["verified_workload.sql"], original_verified)
        metadata = conversion_metadata(
            "DBAIOps", native_report, action, original_verified, artifact_prompt, public_evidence,
        )
        atomic_write(output_dir / "diagnosis_report.md", action["diagnosis_report.md"])
        atomic_write(output_dir / "solution_script.sh", action["solution_script.sh"], 0o700)
        atomic_write(output_dir / "optimized_verified_workload.sql", optimized)
        atomic_write(
            output_dir / "conversion_meta.json",
            json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", 0o600,
        )
        atomic_write(
            output_dir / "artifact_action.json",
            json.dumps(action, ensure_ascii=False, indent=2) + "\n",
        )
    except Exception:
        trace["artifact_status"] = "failed"
        atomic_write(output_dir / "native_trace.json", json.dumps(trace, ensure_ascii=False, indent=2) + "\n")
        raise
    trace.update(artifact_status="completed", artifact_response=artifact_response)
    atomic_write(output_dir / "native_trace.json", json.dumps(trace, ensure_ascii=False, indent=2) + "\n")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--dbms", choices=sorted(DBMS_DATASETS), required=True)
    parser.add_argument("--dataset-id", default="")
    parser.add_argument("--knowledge-url", required=True)
    parser.add_argument("--knowledge-api-key", required=True)
    parser.add_argument("--local-knowledge-dir", type=Path, default=None,
                        help="Deprecated compatibility option; ignored (remote knowledge only)")
    parser.add_argument("--model-url", required=True)
    parser.add_argument("--model-api-key", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--provider", default=os.getenv("OPENAI_PROVIDER", ""))
    parser.add_argument("--api-version", default="")
    parser.add_argument("--temperature", type=float, default=0.7)
    parser.add_argument("--max-tokens", type=int, default=8192)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--model-request-attempts", type=int, default=int(os.getenv("DBAIOPS_MODEL_REQUEST_ATTEMPTS", "3")))
    parser.add_argument("--model-request-delay", type=float, default=float(os.getenv("DBAIOPS_MODEL_REQUEST_DELAY", "30")))
    parser.add_argument("--model-request-timeout", type=int, default=int(os.getenv("DBAIOPS_MODEL_REQUEST_TIMEOUT", "300")))
    parser.add_argument(
        "--artifact-retries", type=int, choices=range(4),
        default=int(os.getenv("DBAIOPS_ARTIFACT_RETRIES", "2")),
        help="Additional artifact schema repair attempts (0-3; default 2)",
    )
    parser.add_argument("--context-chars", type=int, default=50000)
    args = parser.parse_args(argv)
    args.dataset_id = args.dataset_id or DBMS_DATASETS[args.dbms]
    return args


def main() -> int:
    run(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
