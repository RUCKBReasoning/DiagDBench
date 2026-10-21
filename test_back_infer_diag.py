import argparse
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
import json
import logging
import os
import re
import shutil
from typing import Any, Dict, List, Optional, Tuple

from llm_api import get_llm_response
from utils import extract_json_object, resolve_path_from_base


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(
    BASE_DIR, "Configs", "back_infer_diag", "config.json"
)
REQUIRED_SAMPLE_FILES = (
    "user_phenomenon_description.txt",
    "merged_workload.sql",
    "anomaly_description.json",
    "anomaly_inject.sh",
)
DEFAULT_RETRY_TIMES = 24
MAX_RETRY_TIMES = 24
DEFAULT_PARALLELISM = 1
REPORT_NAME = "diagnosis_report.md"
OUTPUT_REPORT_NAME = "gold_diag_report_en.md"
REQUIRED_REPORT_SECTIONS = (
    "Executive Summary",
    "Incident Context",
    "Symptoms and Impact",
    "Evidence and Diagnostic Reasoning",
    "Root Cause",
    "Recovery Actions",
    "Recovery Validation",
    "Preventive and Monitoring Recommendations",
    "Evidence Limitations",
)


diag_report_prompt_template = r"""
# ROLE AND OBJECTIVE

You are a senior database administrator producing a gold-standard English post-incident diagnosis report for a synthetic database anomaly.

Perform backward diagnosis inference. Treat the user workload and observed phenomenon as the problem, and treat the anomaly definition and injection script as the authoritative answer describing the injected root cause. Reconstruct how an expert DBA would diagnose the incident, recover the database service, and validate recovery.

# INPUT EVIDENCE

## User-Observed Phenomenon
{USER_PHENOMENON_DESCRIPTION}

## User-Executed Workload
{MERGED_WORKLOAD}

## Anomaly Definition
{ANOMALY_DESCRIPTION}

## Anomaly Injection Code
{ANOMALY_INJECTION_SCRIPT}

# EVIDENCE DISCIPLINE

1. Ground the root cause, affected objects, commands, and recovery method strictly in the anomaly definition and injection code. Never invent a different cause or unsupported incident fact.
2. Do not claim that unprovided diagnostics, commands, metrics, or validation results were actually observed. Label technically justified but unprovided details as expected evidence, inferred behavior, or a recommended validation.
3. Refuse hallucination: omit details that cannot be supported by the four supplied artifacts. Do not use external knowledge to override the injection code.
4. Reason only as far as needed to connect injection, workload, symptoms, diagnosis, recovery, and validation. Avoid speculative branches and overthinking.
5. Do not reveal hidden chain-of-thought. Return only the requested report payload.

# REPORT REQUIREMENTS

Generate exactly one professional English Markdown report. Write it as a completed expert diagnosis and recovery narrative while preserving the evidence limitations above. Keep every recommendation specific to this sample.

Use these section headings:
- Executive Summary
- Incident Context
- Symptoms and Impact
- Evidence and Diagnostic Reasoning
- Root Cause
- Recovery Actions
- Recovery Validation
- Preventive and Monitoring Recommendations
- Evidence Limitations

The diagnostic reasoning must provide a concise causal sequence from the injected action to the workload-visible symptom. Recovery actions must reverse or neutralize the concrete injection behavior safely. Recovery validation must explain how to rerun the supplied workload and verify that the symptom is gone without fabricating measured values.

# OUTPUT FORMAT (STRICT)

Return exactly one valid JSON object inside `<action>` and `</action>` tags. Do not add Markdown fences, reasoning tags, or any other text. JSON newlines must be escaped. The `action` value must be `finish`.

<action>
{{
  "action": "finish",
  "diagnosis_report.md": "<the complete English Markdown diagnosis report>"
}}
</action>
"""


CORRECTION_PROMPT_TEMPLATE = r"""
Your previous response was rejected because it did not satisfy the required output contract:
{ERROR}

Regenerate the answer using the original evidence. Return only:
<action>
{{
  "action": "finish",
  "diagnosis_report.md": "<a complete English Markdown report containing every required section>"
}}
</action>
Do not include reasoning tags, Markdown fences, commentary, or unsupported facts.
"""


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as file:
        return file.read()


def first_present(mapping: Dict[str, Any], keys: Tuple[str, ...]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None and value != "":
            return value
    return None


def normalize_anomaly_definition(raw_definition: Dict[str, Any]) -> Dict[str, str]:
    nested_definition = raw_definition.get("anomaly_def")
    definition = (
        nested_definition if isinstance(nested_definition, dict) else raw_definition
    )
    name = first_present(
        definition, ("Name", "name", "anomaly_name", "AnomalyName")
    )
    description = first_present(
        definition,
        ("Description", "description", "anomaly_description", "AnomalyDescription"),
    )
    if not name or not description:
        raise ValueError("Anomaly definition must contain a name and description")

    anomaly_type = str(
        first_present(definition, ("Type", "type", "anomaly_type")) or "AUTO"
    ).upper()
    if anomaly_type not in {"OLAP", "OLTP", "BOTH"}:
        anomaly_type = "AUTO"

    success_criterion = str(
        first_present(
            definition,
            ("SuccessCriterion", "success_criterion", "successCriterion"),
        )
        or "AUTO"
    ).upper()
    return {
        "anomaly_name": str(name),
        "anomaly_description": str(description),
        "anomaly_type": anomaly_type,
        "success_criterion": success_criterion,
    }


def atomic_write_json(path: str, value: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary_path = f"{path}.tmp.{os.getpid()}"
    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=4)
        file.write("\n")
    os.replace(temporary_path, path)


def generate_key_information(
    input_directory: str, expected_sample_count: Optional[int] = None
) -> List[Dict[str, Any]]:
    sample_ids = sorted(
        int(name)
        for name in os.listdir(input_directory)
        if name.isdigit() and os.path.isdir(os.path.join(input_directory, name))
    )
    expected_ids = list(range(len(sample_ids)))
    if sample_ids != expected_ids:
        raise ValueError(
            "Numeric sample directories must be contiguous from 0; "
            f"found {len(sample_ids)} directories ending at "
            f"{sample_ids[-1] if sample_ids else 'none'}"
        )
    if expected_sample_count is not None and len(sample_ids) != expected_sample_count:
        raise ValueError(
            f"Expected {expected_sample_count} samples, found {len(sample_ids)}"
        )

    key_information = []
    for sample_id in sample_ids:
        sample_directory = os.path.join(input_directory, str(sample_id))
        config_path = os.path.join(sample_directory, "config.json")
        definition_path = os.path.join(
            sample_directory, "anomaly_description.json"
        )
        instance_config = read_json(config_path)
        raw_definition = read_json(definition_path)
        if not isinstance(instance_config, dict):
            raise TypeError(f"Sample config must be an object: {config_path}")
        if not isinstance(raw_definition, dict):
            raise TypeError(
                f"Anomaly definition must be an object: {definition_path}"
            )

        normalized = normalize_anomaly_definition(raw_definition)
        key_information.append(
            {
                "anomaly_id": sample_id,
                "dbms": instance_config.get("dbms"),
                "database_name": instance_config.get("database_name"),
                "database_schema_name": instance_config.get(
                    "database_schema_name"
                ),
                **normalized,
            }
        )

    output_path = os.path.join(input_directory, "key_information.json")
    atomic_write_json(output_path, key_information)
    return key_information


def resolve_sample_directory(input_directory: str, sample: Dict[str, Any]) -> str:
    sample_id = str(sample["anomaly_id"])
    if not sample_id or sample_id in {".", ".."} or os.path.basename(sample_id) != sample_id:
        raise ValueError(f"Invalid anomaly_id: {sample_id!r}")

    sample_directory = os.path.join(input_directory, sample_id)
    if not os.path.isdir(sample_directory):
        raise FileNotFoundError(f"Sample directory not found: {sample_directory}")
    return sample_directory


def load_sample_inputs(sample_directory: str) -> Dict[str, str]:
    inputs = {}
    for filename in REQUIRED_SAMPLE_FILES:
        path = os.path.join(sample_directory, filename)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Required sample file not found: {path}")
        inputs[filename] = read_text(path)
    return inputs


def build_prompt(sample_directory: str) -> str:
    sample_inputs = load_sample_inputs(sample_directory)

    return diag_report_prompt_template.format(
        USER_PHENOMENON_DESCRIPTION=sample_inputs["user_phenomenon_description.txt"],
        MERGED_WORKLOAD=sample_inputs["merged_workload.sql"],
        ANOMALY_DESCRIPTION=sample_inputs["anomaly_description.json"],
        ANOMALY_INJECTION_SCRIPT=sample_inputs["anomaly_inject.sh"],
    )


def validate_english_report(report: str) -> str:
    normalized_report = report.strip()
    if not normalized_report:
        raise ValueError(f"{REPORT_NAME} must be a non-empty string")
    if re.search(r"[\u3400-\u4dbf\u4e00-\u9fff]", normalized_report):
        raise ValueError(f"{REPORT_NAME} must be written only in English")
    missing_sections = [
        section
        for section in REQUIRED_REPORT_SECTIONS
        if not re.search(
            rf"^#+\s+(?:\d+[.)]\s*)?{re.escape(section)}\s*$",
            normalized_report,
            flags=re.MULTILINE | re.IGNORECASE,
        )
    ]
    if missing_sections:
        raise ValueError(
            f"{REPORT_NAME} is missing required sections: {missing_sections}"
        )
    return normalized_report + "\n"


def extract_diagnosis_reports(response: str) -> Dict[str, str]:
    action_match = re.search(
        r"<action>\s*(.*?)\s*</action>",
        response,
        flags=re.DOTALL | re.IGNORECASE,
    )
    action_content = action_match.group(1) if action_match else response
    payload = json.loads(extract_json_object(action_content))
    if not isinstance(payload, dict):
        raise TypeError("LLM response JSON must be an object")

    required_fields = {"action", REPORT_NAME}
    if set(payload) != required_fields:
        raise ValueError(
            f"LLM response fields must be {sorted(required_fields)}; "
            f"actual fields: {sorted(payload)}"
        )
    if payload["action"] != "finish":
        raise ValueError(
            f"LLM response action must be finish, got {payload['action']!r}"
        )
    report = payload[REPORT_NAME]
    if not isinstance(report, str):
        raise TypeError(f"{REPORT_NAME} must be a string")
    return {REPORT_NAME: validate_english_report(report)}


def append_reasoning_response(
    output_path: str, attempt: int, response: str
) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "a", encoding="utf-8") as file:
        file.write(f"======= LLM Inference Attempt {attempt} =======\n")
        file.write(response)
        if not response.endswith("\n"):
            file.write("\n")
        file.write("==============================================\n\n")


def infer_diagnosis_reports(
    prompt: str,
    llm_configuration: Dict[str, Any],
    reasoning_responses_path: str,
) -> Dict[str, str]:
    retry_times = min(
        MAX_RETRY_TIMES,
        max(1, int(llm_configuration.get("retry_times", DEFAULT_RETRY_TIMES))),
    )
    last_error = None
    messages = [prompt]
    for attempt in range(1, retry_times + 1):
        response = get_llm_response(messages, llm_configuration)
        append_reasoning_response(reasoning_responses_path, attempt, response)
        try:
            return extract_diagnosis_reports(response)
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            last_error = error
            logging.warning(
                "Invalid LLM output on attempt %s/%s: %s",
                attempt,
                retry_times,
                error,
            )
            messages = [
                prompt,
                response,
                CORRECTION_PROMPT_TEMPLATE.format(ERROR=str(error)),
            ]
    raise RuntimeError(
        f"Failed to obtain a valid diagnosis report after {retry_times} attempts: {last_error}"
    )


def write_report(output_path: str, report: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    temporary_path = output_path + ".tmp"
    with open(temporary_path, "w", encoding="utf-8") as file:
        file.write(report)
    os.replace(temporary_path, output_path)


def existing_report_is_valid(report_path: str) -> bool:
    if not os.path.isfile(report_path):
        return False
    try:
        validate_english_report(read_text(report_path))
        return True
    except (OSError, ValueError):
        return False


def process_sample(
    index: int,
    total: int,
    sample: Dict[str, Any],
    input_directory: str,
    output_directory: str,
    llm_configuration: Dict[str, Any],
) -> Dict[str, Any]:
    sample_id = str(sample["anomaly_id"])
    sample_directory = resolve_sample_directory(input_directory, sample)
    sample_output_directory = os.path.join(output_directory, sample_id)
    report_path = os.path.join(sample_output_directory, OUTPUT_REPORT_NAME)
    status_path = os.path.join(sample_output_directory, "status.json")
    os.makedirs(sample_output_directory, exist_ok=True)

    if existing_report_is_valid(report_path):
        result = {
            "sample_id": int(sample_id),
            "status": "skipped",
            "report_path": report_path,
        }
        atomic_write_json(status_path, result)
        logging.info("[%s/%s][sample=%s] Valid report exists; skipped", index, total, sample_id)
        return result

    atomic_write_json(
        status_path,
        {"sample_id": int(sample_id), "status": "running"},
    )
    try:
        prompt = build_prompt(sample_directory)
        reports = infer_diagnosis_reports(
            prompt,
            dict(llm_configuration),
            os.path.join(sample_output_directory, "reasoning_responses.txt"),
        )
        write_report(report_path, reports[REPORT_NAME])
        result = {
            "sample_id": int(sample_id),
            "status": "completed",
            "report_path": report_path,
        }
        atomic_write_json(status_path, result)
        logging.info("[%s/%s][sample=%s] Wrote %s", index, total, sample_id, report_path)
        return result
    except Exception as error:
        atomic_write_json(
            status_path,
            {
                "sample_id": int(sample_id),
                "status": "failed",
                "error": str(error),
            },
        )
        raise


def prepare_output_directory(output_directory: str, config_path: str) -> str:
    os.makedirs(output_directory, exist_ok=True)
    config_snapshot_path = os.path.join(output_directory, "_run_config.json")
    if os.path.abspath(config_path) != os.path.abspath(config_snapshot_path):
        shutil.copyfile(config_path, config_snapshot_path)
    return output_directory


def run(config_path: str) -> int:
    config = read_json(config_path)
    if not isinstance(config, dict):
        raise TypeError("The configuration file must contain a JSON object")

    input_directory = resolve_path_from_base(config.get("input_directory"), BASE_DIR)
    output_directory = resolve_path_from_base(
        config.get("output_directory"), BASE_DIR
    )
    llm_configuration = config.get("llm_configuration")
    if not input_directory or not os.path.isdir(input_directory):
        raise NotADirectoryError(f"Input directory not found: {input_directory}")
    if not output_directory:
        raise ValueError("output_directory is required")
    if not isinstance(llm_configuration, dict):
        raise ValueError("llm_configuration must be a JSON object")

    parallelism = max(
        1, int(config.get("parallelism", DEFAULT_PARALLELISM))
    )
    expected_sample_count_value = config.get("expected_sample_count")
    expected_sample_count = (
        int(expected_sample_count_value)
        if expected_sample_count_value is not None
        else None
    )
    samples = generate_key_information(input_directory, expected_sample_count)
    for sample in samples:
        sample_directory = resolve_sample_directory(input_directory, sample)
        load_sample_inputs(sample_directory)

    output_directory = prepare_output_directory(output_directory, config_path)
    total = len(samples)
    logging.info(
        "Validated %s samples from %s with immutable IDs %s..%s",
        total,
        input_directory,
        samples[0]["anomaly_id"] if samples else "none",
        samples[-1]["anomaly_id"] if samples else "none",
    )
    logging.info("Output directory: %s", output_directory)
    logging.info("Parallel workers: %s", parallelism)

    results = []
    failures = []
    future_bindings: Dict[Future, Tuple[int, int]] = {}
    with ThreadPoolExecutor(
        max_workers=parallelism, thread_name_prefix="back-infer"
    ) as executor:
        for index, sample in enumerate(samples, start=1):
            sample_id = int(sample["anomaly_id"])
            future = executor.submit(
                process_sample,
                index,
                total,
                sample,
                input_directory,
                output_directory,
                llm_configuration,
            )
            future_bindings[future] = (index, sample_id)

        for future in as_completed(future_bindings):
            index, sample_id = future_bindings[future]
            try:
                results.append(future.result())
            except Exception as error:
                failure = {
                    "sample_id": sample_id,
                    "status": "failed",
                    "error": str(error),
                }
                failures.append(failure)
                results.append(failure)
                logging.exception(
                    "[%s/%s][sample=%s] Failed", index, total, sample_id
                )
            atomic_write_json(
                os.path.join(output_directory, "run_summary.json"),
                {
                    "total": total,
                    "finished": len(results),
                    "completed": sum(
                        result["status"] == "completed" for result in results
                    ),
                    "skipped": sum(
                        result["status"] == "skipped" for result in results
                    ),
                    "failed": len(failures),
                    "results": sorted(results, key=lambda item: item["sample_id"]),
                },
            )

    if failures:
        logging.error("Failed samples: %s", [item["sample_id"] for item in failures])
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate gold diagnosis reports through backward LLM inference."
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to the configuration JSON file",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = resolve_path_from_base(args.config, BASE_DIR)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    return run(config_path)


if __name__ == "__main__":
    raise SystemExit(main())
