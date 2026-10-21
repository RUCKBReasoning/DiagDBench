"""Run DBAIOps' open-source knowledge-Q&A flow for one evaluation sample."""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import os
import re
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


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
) -> dict[str, Any]:
    if attempts < 1 or timeout <= 0 or retry_delay < 0:
        raise ValueError("Invalid request retry budget")
    if api_version:
        separator = "&" if "?" in url else "?"
        url += separator + "api-version=" + urllib.parse.quote(api_version)
    invalid_responses = 0
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
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
) -> tuple[list[str], list[dict[str, Any]]]:
    payload = {
        "question": question,
        "similarity": similarity,
        "topn": topn,
        "dataset_name": dataset_id,
        "temperature": temperature,
        "language": "中文",
        "isSupportImg": "true",
    }
    attempts = []
    for attempt in range(1, retries + 1):
        try:
            response = request_json(url, payload, api_key)
            attempts.append(response)
            if str(response.get("status")) != "200":
                raise RuntimeError(
                    "DBAIOps knowledge service rejected the request: "
                    + str(response.get("message", "unknown error"))
                )
            data = response.get("data") or {}
            items = data.get("dataList") if isinstance(data, dict) else data
            if not isinstance(items, list) or not items:
                raise RuntimeError("DBAIOps knowledge service returned no matched documents")
            return [item if isinstance(item, str) else json.dumps(item, ensure_ascii=False)
                    for item in items], attempts
        except PermanentHTTPError:
            raise
        except RuntimeError:
            if attempt == retries:
                raise
            time.sleep(min(attempt, 3))
    raise AssertionError("unreachable")


def chat_endpoint(url: str, model: str, provider: str = "") -> str:
    """Resolve Azure deployment routes without changing OpenAI-compatible routes."""
    parts = urllib.parse.urlsplit(url)
    path = parts.path.rstrip("/")
    host = (parts.hostname or "").lower()
    azure = provider in {"azure", "azure", "azure_openai"}
    if not provider:
        azure = host.endswith(".openai.azure.com")
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
    if set(action) != expected or action.get("action") != "finish":
        raise ValueError("Artifact response does not match the finish schema")
    for key in expected - {"action"}:
        if not isinstance(action[key], str):
            raise ValueError(f"{key} must be a string")
    if not action["diagnosis_report.md"].strip():
        raise ValueError("Diagnosis report is empty")
    script_lines = action["solution_script.sh"].splitlines()
    first_line = script_lines[0].strip() if script_lines else ""
    if re.fullmatch(r"# TIMEOUT THRESHOLD: [1-9][0-9]*", first_line) is None:
        raise ValueError("Solution script has no valid timeout header")
    checked = subprocess.run(
        ["bash", "-n"], input=action["solution_script.sh"], text=True,
        capture_output=True, check=False, timeout=10,
    )
    if checked.returncode:
        raise ValueError(f"Invalid Bash: {checked.stderr.strip()}")
    return {key: str(action[key]) for key in expected}


def normalize_artifact(action: dict[str, Any], native_report: str) -> dict[str, str]:
    """Fill deterministic fields, then apply the unchanged finish validation."""
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
        "diagnosis_report.md": native_report.rstrip() + "\n",
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
        except ValueError as exc:
            record.update(status="invalid", error=str(exc))
            atomic_write(path, json.dumps(attempts, ensure_ascii=False, indent=2) + "\n")
            if index == repairs:
                raise ValueError(
                    f"Artifact validation failed after {index + 1} attempts: {exc}; see {path}"
                ) from exc
            correction = (
                "\n\nThe previous artifact response failed validation. No generated script "
                "was executed. Keep the native diagnosis unchanged and regenerate one complete "
                "JSON object with action='finish', diagnosis_report.md, solution_script.sh, "
                "verified_workload.sql. Include a valid Bash script starting with "
                "'# TIMEOUT THRESHOLD: <positive integer>'. Any non-empty optimized "
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
    knowledge_text = "\n\n".join(knowledge)
    system_prompt = (
        rule["prompt"]
        + "\n\n你是一个智能问答助手，基于用户提供的内容来回答问题。"
        "请提供准确、有用的回答。"
        + f"\n\n请结合{DBMS_NAMES[args.dbms]}的专业知识来回答问题。"
        + "\n\n评测补充约束：区分事实、推断和待验证项；不得虚构指标、"
        "执行结果、异常注入方式或已完成的恢复操作。"
    )
    native_user_prompt = (
        "基于以下内容回答问题:\n\n"
        "知识库查询结果:\n"
        + knowledge_text
        + "\n\n问题:"
        + question
    )
    native_report, diagnosis_response = chat_completion(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": native_user_prompt},
        ],
        url=args.model_url,
        api_key=args.model_api_key,
        model=args.model,
        api_version=args.api_version,
        # Retrieval rules configure the knowledge service, not the chosen LLM.
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        provider=getattr(args, "provider", ""),
        request_attempts=getattr(args, "model_request_attempts", 1),
        request_delay=getattr(args, "model_request_delay", 30),
        request_timeout=getattr(args, "model_request_timeout", 300),
    )
    # Persist the native method's output before the fallible evaluation adapter.
    atomic_write(output_dir / "native_diagnosis_report.md", native_report.rstrip() + "\n")
    trace = {
        "method": "DBAIOps open-source knowledge-Q&A flow",
        "dataset_id": args.dataset_id,
        "model_temperature": args.temperature,
        "parameter_rule": {key: value for key, value in rule.items() if key != "prompt"},
        "native_messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": native_user_prompt},
        ],
        "knowledge_items": knowledge,
        "knowledge_responses": knowledge_responses,
        "diagnosis_response": diagnosis_response,
        "artifact_status": "pending",
    }
    atomic_write(output_dir / "native_trace.json", json.dumps(trace, ensure_ascii=False, indent=2) + "\n")
    artifact_prompt = f"""The following Markdown report is the frozen output of the
DBAIOps knowledge-Q&A diagnosis flow. Preserve the diagnosis exactly and only compile
the evaluation artifacts.

Return one JSON object with exactly: action, diagnosis_report.md,
solution_script.sh, verified_workload.sql. action must be "finish".
diagnosis_report.md must be exactly the native report below.
The Bash script must start with '# TIMEOUT THRESHOLD: <positive integer>', be
non-interactive, use DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD, and avoid broad
or destructive actions when the report lacks evidence.

For `verified_workload.sql`, treat the original workload as an ordered sequence.
A non-empty optimized workload must have a concrete potential performance benefit,
retain exactly the same number of SQL statements in exactly the same order, and
preserve each statement's semantics and order-sensitive logic. Starting from the
same initialized database state, sequential execution must produce exactly the
same observable result for every corresponding statement, including returned rows
and values, duplicate counts, affected-row counts, and persistent database state.
Do not add, delete, merge, split, replace, or reorder statements. Any execution-
result mismatch is a diagnosis failure. Return an empty optimized workload unless
exact result equivalence can be guaranteed.

## Native report
{native_report}

## Anomaly-verified workload
{workload}
"""
    try:
        action, artifact_response = generate_artifact_action(
            args, artifact_prompt, native_report, output_dir,
        )
    except Exception:
        trace["artifact_status"] = "failed"
        atomic_write(output_dir / "native_trace.json", json.dumps(trace, ensure_ascii=False, indent=2) + "\n")
        raise
    original_verified = (sample_dir / "verified_workload.sql").read_text(encoding="utf-8").strip()
    optimized = action["verified_workload.sql"].strip() or original_verified
    atomic_write(output_dir / "diagnosis_report.md", action["diagnosis_report.md"])
    atomic_write(output_dir / "solution_script.sh", action["solution_script.sh"], 0o700)
    atomic_write(
        output_dir / "optimized_verified_workload.sql",
        optimized + ("\n" if optimized else ""),
    )
    atomic_write(
        output_dir / "artifact_action.json",
        json.dumps(action, ensure_ascii=False, indent=2) + "\n",
    )
    trace.update(artifact_status="completed", artifact_response=artifact_response)
    atomic_write(output_dir / "native_trace.json", json.dumps(trace, ensure_ascii=False, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--dbms", choices=sorted(DBMS_DATASETS), required=True)
    parser.add_argument("--dataset-id", default="")
    parser.add_argument("--knowledge-url", required=True)
    parser.add_argument("--knowledge-api-key", required=True)
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
    args = parser.parse_args()
    args.dataset_id = args.dataset_id or DBMS_DATASETS[args.dbms]
    run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
