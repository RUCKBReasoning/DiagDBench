"""Convert a native DB-GPT diagnosis into ReAct-compatible output artifacts."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any
from benchmark_policy import DIAGNOSIS_REQUIREMENTS
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from llm_api import prepare_request
from Baselines.recovery_artifacts import (
    build_recovery_prompt, collect_public_evidence, conversion_metadata,
    effective_workload, read_utf8, validate_artifacts,
)


TIMEOUT_LINE = re.compile(r"# TIMEOUT THRESHOLD: ([1-9][0-9]*)")
GENERATED_KEYS = {"solution_script.sh", "verified_workload.sql"}
SAMPLE_EVIDENCE_FILES = (
    "config.json",
    "user_phenomenon_description.txt",
)


def read_text(path: Path, limit: int | None = None) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    if limit is not None and len(text) > limit:
        return text[:limit] + "\n...[truncated]"
    return text


def atomic_write(path: Path, content: str, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    if mode is not None:
        temporary.chmod(mode)
    temporary.replace(path)


def extract_json_object(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", text):
        try:
            value, _ = decoder.raw_decode(text[match.start():])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise ValueError("Model response does not contain a valid JSON object.")


def validate_generated_fields(value: dict[str, Any]) -> dict[str, str]:
    return validate_artifacts(value)


def native_report_from_result(result: dict[str, Any]) -> str:
    report = result.get("report")
    if not isinstance(report, str) or not report.strip():
        raise ValueError("DB-GPT result does not contain a non-empty Markdown `report`.")
    return report


def original_workload(sample_dir: Path) -> str:
    return read_utf8(sample_dir / "verified_workload.sql")


def collect_evidence(sample_dir: Path, snapshot_path: Path | None, limit: int) -> str:
    """Retain the old signature; recovery evidence now follows the explicit allowlist.

    snapshot_path and limit remain accepted for caller compatibility. Snapshots
    are diagnosis inputs, not recovery authority; complete public SQL is never
    silently truncated. The model context budget rejects oversized requests.
    """
    return json.dumps(collect_public_evidence(sample_dir, "PostgreSQL"), ensure_ascii=False)


def build_prompt(native_report: str, evidence: str) -> str:
    try:
        public = json.loads(evidence)
    except (ValueError, TypeError):
        # Preserve the public build_prompt(report, evidence_text) calling form.
        public = evidence
    return build_recovery_prompt({
        "native_diagnosis_report": native_report, "public_evidence": public,
    }, DIAGNOSIS_REQUIREMENTS)


def request_completion(
    prompt: str,
    *,
    api_key: str,
    model: str,
    api_base: str,
    api_version: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
) -> tuple[str, str]:
    try:
        import openai

        if api_version:
            client = openai.AzureOpenAI(
                api_key=api_key,
                api_version=api_version,
                azure_endpoint=api_base,
                timeout=timeout,
            )
        else:
            client = openai.OpenAI(
                api_key=api_key,
                base_url=api_base,
                timeout=timeout,
            )
        request, _ = prepare_request({
            'model':model,
            'messages':[{'role':'system','content':'Produce machine-readable database recovery artifacts.'},
                        {'role':'user','content':prompt}],
            'temperature':temperature, 'max_tokens':max_tokens, 'stream':False,
        }, {'model_name':model, 'allow_context_truncation': False})
        response = client.chat.completions.create(**request)

    except Exception as exc:
        raise RuntimeError(f"Artifact API request failed: {exc}") from exc

    raw = response.model_dump_json()
    if response.choices and response.choices[0].finish_reason in {"length", "content_filter"}:
        raise RuntimeError("Artifact API response was incomplete or filtered")
    content = response.choices[0].message.content if response.choices else ""
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("Artifact API response did not contain text content.")
    return content, raw


def generate_artifacts(
    prompt: str,
    *,
    api_key: str,
    model: str,
    api_base: str,
    api_version: str = "",
    temperature: float = 0.0,
    max_tokens: int = 8192,
    timeout: int = 300,
    retry_times: int = 3,
) -> tuple[dict[str, str], list[str]]:
    errors = []
    current_prompt = prompt
    raw_responses = []
    for attempt in range(1, retry_times + 1):
        try:
            content, raw = request_completion(
                current_prompt,
                api_key=api_key,
                model=model,
                api_base=api_base,
                api_version=api_version,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=timeout,
            )
            raw_responses.append(raw)
            return validate_generated_fields(extract_json_object(content)), raw_responses
        except (ValueError, RuntimeError, json.JSONDecodeError, subprocess.SubprocessError) as exc:
            errors.append(f"attempt {attempt}: {exc}")
            current_prompt = (
                prompt
                + "\n\nYour previous response was invalid. Correct this validation error and "
                + "return the complete JSON object again:\n"
                + str(exc)
            )
            if attempt < retry_times:
                time.sleep(min(attempt, 3))
    raise RuntimeError("Unable to generate valid recovery artifacts: " + "; ".join(errors))


def save_artifacts(
    output_dir: Path,
    native_report: str,
    generated: dict[str, str],
    fallback_workload: str,
) -> dict[str, str]:
    generated = validate_generated_fields(generated)
    optimized_workload = effective_workload(generated["verified_workload.sql"], fallback_workload)
    action = {
        "action": "finish",
        "diagnosis_report.md": native_report,
        "solution_script.sh": generated["solution_script.sh"],
        "verified_workload.sql": generated["verified_workload.sql"],
    }
    atomic_write(output_dir / "diagnosis_report.md", native_report)
    atomic_write(output_dir / "solution_script.sh", generated["solution_script.sh"], 0o700)
    atomic_write(
        output_dir / "optimized_verified_workload.sql",
        optimized_workload,
    )
    atomic_write(
        output_dir / "artifact_action.json",
        json.dumps(action, ensure_ascii=False, indent=2) + "\n",
    )
    return action


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--diagnosis-result", type=Path, required=True)
    parser.add_argument("--sample-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--model", default=os.getenv("DB_GPT_OPENAI_MODEL", "gpt-4-0613"))
    parser.add_argument(
        "--api-base",
        default=os.getenv("DB_GPT_OPENAI_API_BASE", "https://api.openai.com/v1"),
    )
    parser.add_argument("--api-version", default=os.getenv("DB_GPT_OPENAI_API_VERSION", ""))
    parser.add_argument(
        "--temperature",
        type=float,
        default=float(os.getenv(
            "DB_GPT_ARTIFACT_TEMPERATURE",
            os.getenv("DB_GPT_OPENAI_TEMPERATURE", "1"),
        )),
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=int(os.getenv("DB_GPT_ARTIFACT_MAX_TOKENS", "8192")),
    )
    parser.add_argument(
        "--retry-times",
        type=int,
        default=int(os.getenv("DB_GPT_ARTIFACT_RETRY_TIMES", "3")),
    )
    parser.add_argument(
        "--context-chars",
        type=int,
        default=int(os.getenv("MAX_CONTEXT_CHARS", "50000")),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY is required for artifact generation.")
    if args.retry_times < 1:
        raise SystemExit("--retry-times must be at least 1.")

    result = json.loads(read_text(args.diagnosis_result))
    native_report = native_report_from_result(result)
    evidence = collect_evidence(args.sample_dir, args.snapshot, args.context_chars)
    prompt = build_prompt(native_report, evidence)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write(args.output_dir / "conversion_prompt.txt", prompt, 0o600)
    generated, raw_responses = generate_artifacts(
        prompt,
        api_key=api_key,
        model=args.model,
        api_base=args.api_base,
        api_version=args.api_version,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        retry_times=args.retry_times,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write(
        args.output_dir / "artifact_raw_responses.json",
        json.dumps(raw_responses, ensure_ascii=False, indent=2) + "\n",
    )
    # Reuse the exact SQL supplied to the converter, not a second filesystem read.
    public_evidence = json.loads(evidence)
    fallback_workload = public_evidence["public_inputs"]["verified_workload.sql"]
    save_artifacts(args.output_dir, native_report, generated, fallback_workload)
    metadata = conversion_metadata(
        "DB-GPT", native_report, generated, fallback_workload, prompt, public_evidence,
    )
    atomic_write(args.output_dir / "conversion_meta.json", json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", 0o600)
    print(f"Structured artifacts written to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
