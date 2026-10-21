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
    if set(value) != GENERATED_KEYS:
        raise ValueError(
            "Expected exactly `solution_script.sh` and `verified_workload.sql`."
        )
    if not all(isinstance(value[key], str) for key in GENERATED_KEYS):
        raise ValueError("Both generated artifact fields must be strings.")

    script = value["solution_script.sh"]
    first_line = script.splitlines()[0].strip() if script.splitlines() else ""
    if TIMEOUT_LINE.fullmatch(first_line) is None:
        raise ValueError(
            "`solution_script.sh` must start with "
            "`# TIMEOUT THRESHOLD: <positive integer>`."
        )
    checked = subprocess.run(
        ["bash", "-n"],
        input=script,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    if checked.returncode:
        raise ValueError(f"Recovery script has invalid Bash syntax: {checked.stderr.strip()}")
    return {
        "solution_script.sh": script.rstrip() + "\n",
        "verified_workload.sql": value["verified_workload.sql"].strip(),
    }


def native_report_from_result(result: dict[str, Any]) -> str:
    report = result.get("report")
    if not isinstance(report, str) or not report.strip():
        raise ValueError("DB-GPT result does not contain a non-empty Markdown `report`.")
    return report.strip() + "\n"


def original_workload(sample_dir: Path) -> str:
    for name in ("verified_workload.sql", "merged_workload.sql"):
        path = sample_dir / name
        if path.is_file():
            return read_text(path).strip()
    return ""


def collect_evidence(sample_dir: Path, snapshot_path: Path | None, limit: int) -> str:
    sections = []
    remaining = max(0, limit)
    candidates = [(name, sample_dir / name) for name in SAMPLE_EVIDENCE_FILES]
    if snapshot_path is not None:
        candidates.append(("database_snapshot.json", snapshot_path))
    for name, path in candidates:
        if path.is_file() and remaining:
            content = read_text(path, remaining)
            sections.append(f"## {name}\n{content}")
            remaining = max(0, remaining - len(content))
    if remaining:
        initialization = read_text(sample_dir / "initialization_workload.sql")
        verified = original_workload(sample_dir)
        workload = (
            "-- Preparation phase: execute first\n"
            + initialization.strip()
            + "\n\n--- anomaly-verified workload phase boundary ---\n\n"
            + "-- Anomaly verification phase: execute second\n"
            + verified.strip()
        )
        sections.append(f"## anomaly-verified workload\n{workload[:remaining]}")
    return "\n\n".join(sections)


def build_prompt(native_report: str, evidence: str) -> str:
    return f"""You are a database recovery artifact compiler.

The diagnosis below was produced by DB-GPT's native multi-agent workflow. Do not
change its root cause, invent observations, or claim that recovery was executed.
Convert only its supported recommendations into a conservative, reusable Bash
recovery script and an optional optimized verification workload.

Requirements:
1. Return exactly one JSON object with exactly these string fields:
   "solution_script.sh" and "verified_workload.sql".
2. The first line of the Bash script must be:
   # TIMEOUT THRESHOLD: <positive integer seconds>
3. The script must be non-interactive, valid Bash, use PostgreSQL client commands,
   consume DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD environment variables,
   set PGPASSWORD without exposing it on the command line, and use ON_ERROR_STOP.
4. Add bounded prechecks and postchecks. Refuse destructive or broad actions when
   the diagnosis does not identify a sufficiently specific target.
5. Do not restart the DBMS, remove containers, modify the host OS, or terminate
   unrelated sessions.
6. When DB-GPT only recommends investigation or lacks evidence for an automatic
   repair, generate a safe inspection-only script that exits nonzero with an
   explanation instead of guessing.
7. Treat the original verification workload as an ordered sequence. Set
   "verified_workload.sql" to a non-empty optimized workload only when the DB-GPT
   diagnosis supports a rewrite with a concrete potential performance benefit.
   The rewrite must retain exactly the same number of SQL statements in exactly
   the same order and preserve every statement's semantics and order-sensitive
   logic. Starting from the same initialized database state, sequential execution
   must produce exactly the same observable result for every corresponding
   statement, including returned rows and values, duplicate counts, affected-row
   counts, and persistent database state. Do not add, delete, merge, split,
   replace, or reorder statements. Any execution-result mismatch is a diagnosis
   failure. If exact result equivalence cannot be guaranteed, return "".
8. Output JSON only, without Markdown fences or commentary.

# Native DB-GPT Markdown report
{native_report}

# Supplied sample evidence
{evidence}
"""


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
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "Produce machine-readable database recovery artifacts.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
            stream=False,
        )
    except Exception as exc:
        raise RuntimeError(f"Artifact API request failed: {exc}") from exc

    raw = response.model_dump_json()
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
        except (ValueError, RuntimeError, json.JSONDecodeError) as exc:
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
    optimized_workload = generated["verified_workload.sql"] or fallback_workload
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
        optimized_workload.rstrip() + ("\n" if optimized_workload else ""),
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
    generated, raw_responses = generate_artifacts(
        build_prompt(native_report, evidence),
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
    save_artifacts(
        args.output_dir,
        native_report,
        generated,
        original_workload(args.sample_dir),
    )
    print(f"Structured artifacts written to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
