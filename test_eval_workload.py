#!/usr/bin/env python3
"""Re-score optimized workloads with a strict LLM semantic-equivalence gate.

This evaluator is intentionally offline with respect to the database. It reuses
the diagnosis score, execution timings, error state, and initialization evidence
already produced by test_eval.py. Only workload semantic equivalence is judged
again by an LLM.
"""

import argparse
import copy
import json
import logging
import math
import os
import re
import statistics
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


DIAGNOSIS_SCORE_WEIGHT = 0.30
PERFORMANCE_SCORE_WEIGHT = 0.70
FULL_SCORE_RATIO = 0.20
DEFAULT_ZERO_SCORE_RATIO = 8.0
SCORE_PRECISION = 6
SEMANTIC_DETAIL_FILENAME = "workload_semantic_judge_detail.json"
SCORE_DETAIL_FILENAME = "score_detail.json"
COMPARISON_DETAIL_FILENAME = "workload_result_comparison_detail.json"


def round_score(value):
    return None if value is None else round(float(value), SCORE_PRECISION)


def read_json(path):
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def read_text(path):
    with open(path, "r", encoding="utf-8") as file:
        return file.read()


def write_json_atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_path = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            json.dump(value, file, indent=4, ensure_ascii=False)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary_path, path)
    except Exception:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise


def natural_key(value):
    return [
        int(part) if part.isdigit() else part.lower()
        for part in re.split(r"(\d+)", str(value))
    ]


def resolve_path(value, config_directory):
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = config_directory / path
    return path.resolve()


def require_directory(path, field):
    if not path.is_dir():
        raise ValueError(f"{field} is not a directory: {path}")


def resolve_llm_configuration(config, config_directory):
    value = config.get("judge_llm_configuration")
    if not isinstance(value, dict) or not value:
        raise ValueError("judge_llm_configuration must be a non-empty object.")

    if "config_path" in value:
        source_path = resolve_path(value["config_path"], config_directory)
        source = read_json(source_path)
        config_key = value.get("config_key", "judge_llm_configuration")
        try:
            resolved = source[config_key]
        except KeyError as exc:
            raise ValueError(
                f"LLM configuration key `{config_key}` is missing from {source_path}."
            ) from exc
        if not isinstance(resolved, dict):
            raise ValueError(
                f"LLM configuration `{config_key}` in {source_path} must be an object."
            )
        resolved = dict(resolved)
        resolved.update(value.get("overrides") or {})
    else:
        resolved = dict(value)

    required = (
        "base_url",
        "api_key",
        "model_name",
        "temperature",
        "retry_times",
    )
    missing = [
        key for key in required
        if key not in resolved or resolved[key] is None or resolved[key] == ""
    ]
    if missing:
        raise ValueError(f"judge_llm_configuration is missing: {missing}")
    resolved.setdefault("max_output_tokens", 4096)
    resolved.setdefault("stream", False)
    return resolved


def load_runtime_config(config_path):
    config_path = Path(config_path).expanduser().resolve()
    config = read_json(config_path)
    config_directory = config_path.parent
    path_fields = (
        "anomaly_sample_directory",
        "diagnosis_method_directory",
        "diagnosis_evaluation_directory",
        "output_directory",
    )
    for field in path_fields:
        if field not in config:
            raise ValueError(f"Missing required configuration field: {field}")
        config[field] = resolve_path(config[field], config_directory)

    for field in path_fields[:-1]:
        require_directory(config[field], field)
    if config["output_directory"] == config["diagnosis_evaluation_directory"]:
        raise ValueError(
            "output_directory must differ from diagnosis_evaluation_directory."
        )

    repetitions = config.get("judge_repetitions", 3)
    if isinstance(repetitions, bool) or not isinstance(repetitions, int):
        raise ValueError("judge_repetitions must be an odd positive integer.")
    if repetitions <= 0 or repetitions % 2 == 0:
        raise ValueError("judge_repetitions must be an odd positive integer.")
    parallelism = config.get("parallelism_num", 8)
    if isinstance(parallelism, bool) or not isinstance(parallelism, int):
        raise ValueError("parallelism_num must be a positive integer.")
    if parallelism <= 0:
        raise ValueError("parallelism_num must be a positive integer.")

    config["judge_repetitions"] = repetitions
    config["parallelism_num"] = parallelism
    config["_config_path"] = config_path
    config["_llm_configuration"] = resolve_llm_configuration(
        config, config_directory
    )
    return config


def find_sample_directory(root, sample_id):
    direct = root / sample_id
    if direct.is_dir():
        return direct
    candidates = sorted(
        (path for path in root.glob(f"{sample_id}_*") if path.is_dir()),
        key=lambda path: natural_key(path.name),
    )
    if candidates:
        return candidates[0]
    raise FileNotFoundError(
        f"Cannot find sample `{sample_id}` under anomaly_sample_directory {root}."
    )


def extract_json_object(response):
    text = str(response).strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fenced:
        text = fenced.group(1)
    start = text.find("{")
    if start < 0:
        raise ValueError("Judge response contains no JSON object.")
    decoder = json.JSONDecoder()
    parsed, _ = decoder.raw_decode(text[start:])
    if not isinstance(parsed, dict):
        raise ValueError("Judge response must be a JSON object.")
    return parsed


def compact_statement(statement):
    return {
        key: statement.get(key)
        for key in (
            "statement_index",
            "sql_preview",
            "success",
            "error",
            "result_type",
            "status_message",
            "row_count",
            "columns",
            "result_fingerprint",
        )
    }


def representative_statements(result_comparison, side):
    runs = result_comparison.get(f"{side}_comparison_runs") or []
    if not runs:
        return []
    result_detail = runs[0].get("result_detail") or {}
    return [
        compact_statement(statement)
        for statement in result_detail.get("statements") or []
    ]


def compact_comparison_evidence(comparison):
    optimized = comparison.get("optimized_performance") or comparison
    result = optimized.get("result_comparison") or {}
    return {
        "warning": (
            "Execution evidence is supporting context only. Matching outputs do "
            "not prove semantic equivalence, and differing volatile/time values "
            "do not by themselves prove semantic difference."
        ),
        "comparison_mode": comparison.get("comparison_mode"),
        "historical_results_equivalent": optimized.get(
            "results_equivalent", comparison.get("results_equivalent")
        ),
        "baseline_statements": representative_statements(result, "baseline"),
        "optimized_statements": representative_statements(result, "recovered"),
        "initialization_comparison": optimized.get("initialization_comparison"),
        "performance_comparison": optimized.get("performance_comparison"),
    }


def build_judge_prompt(
    sample_id,
    dbms,
    original_workload,
    optimized_workload,
    comparison_evidence,
):
    evidence_json = json.dumps(
        comparison_evidence, ensure_ascii=False, indent=2
    )
    return f"""
You are a strict database-workload semantic-equivalence judge.

Determine whether OPTIMIZED_WORKLOAD preserves the complete user intent and
observable behavior of ORIGINAL_WORKLOAD for DBMS `{dbms}`. This is not merely
result-set equivalence. Judge the full ordered workload.

Mandatory standard:
1. Every original statement behavior must be preserved: reads, returned columns
   and rows, ordering, duplicate behavior, DDL/DML effects, session/transaction
   state, locks, errors, procedure calls, and other side effects.
2. The optimized workload must not add any executable operation or observable
   behavior absent from the original. Even an apparently harmless `DO 0`,
   `SELECT 1`, duplicate SET, extra probe, extra transaction boundary, or extra
   metadata operation makes it non-equivalent.
3. The optimized workload must not remove an operation or behavior. Combining
   statements is allowed only when all original behaviors, ordering constraints,
   intermediate states observable to the workload, error behavior, and final
   effects are strictly preserved.
4. Comments, whitespace, aliases that do not alter the output contract, and
   provably equivalent syntax are not differences.
5. Volatile values such as current timestamps, random values, generated IDs, and
   concurrency-sensitive observations may differ across executions. Use SQL
   semantics, not literal runtime values, to judge those cases.
6. Historical execution comparison is supporting evidence only. Do not declare
   equivalence merely because one execution produced matching fingerprints.
7. If equivalence cannot be established strictly, return score 0.

Return exactly one JSON object and no Markdown:
{{
  "semantic_equivalent": true,
  "score": 1,
  "reason": "specific comparison of preserved or changed user intent",
  "preserved_operations": ["..."],
  "added_operations": [],
  "removed_operations": [],
  "changed_semantics": []
}}

`score` must be exactly 1 when semantic_equivalent is true and exactly 0 when it
is false. List every material added, removed, or changed behavior you identify.

===== SAMPLE ID =====
{sample_id}

===== ORIGINAL_WORKLOAD =====
{original_workload}

===== OPTIMIZED_WORKLOAD =====
{optimized_workload}

===== HISTORICAL EXECUTION EVIDENCE =====
{evidence_json}
""".strip()


def parse_judgement(response):
    parsed = extract_json_object(response)
    equivalent = parsed.get("semantic_equivalent")
    score = parsed.get("score")
    if not isinstance(equivalent, bool):
        raise ValueError("semantic_equivalent must be a boolean.")
    if isinstance(score, bool) or score not in (0, 1):
        raise ValueError("score must be integer 0 or 1.")
    if score != int(equivalent):
        raise ValueError("score conflicts with semantic_equivalent.")
    reason = parsed.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("reason must be a non-empty string.")

    result = {
        "semantic_equivalent": equivalent,
        "score": score,
        "reason": reason.strip(),
    }
    for field in (
        "preserved_operations",
        "added_operations",
        "removed_operations",
        "changed_semantics",
    ):
        value = parsed.get(field, [])
        if not isinstance(value, list) or not all(
            isinstance(item, str) for item in value
        ):
            raise ValueError(f"{field} must be an array of strings.")
        result[field] = value
    return result


def call_judge(prompt, llm_configuration):
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from llm_api import get_llm_response

    return get_llm_response([prompt], dict(llm_configuration))


def judge_semantics(
    prompt,
    llm_configuration,
    repetitions,
    logger,
    sample_id,
    judge_function=call_judge,
):
    judgements = []
    failures = []
    max_attempts = repetitions * 2
    for attempt in range(1, max_attempts + 1):
        if len(judgements) >= repetitions:
            break
        try:
            logger.info(
                "[Sample ID: %s] [EVAL][SEMANTIC JUDGE] Requesting vote %d/%d "
                "(attempt %d/%d)",
                sample_id,
                len(judgements) + 1,
                repetitions,
                attempt,
                max_attempts,
            )
            response = judge_function(prompt, llm_configuration)
            judgement = parse_judgement(response)
            judgement["attempt"] = attempt
            judgement["raw_response"] = response
            judgements.append(judgement)
        except Exception as exc:
            error = " ".join(str(exc).split())
            failures.append({"attempt": attempt, "error": error})
            logger.warning(
                "[Sample ID: %s] [EVAL][SEMANTIC JUDGE][WARNING] Invalid "
                "attempt %d/%d: %s",
                sample_id,
                attempt,
                max_attempts,
                error,
            )
    if len(judgements) < repetitions:
        raise RuntimeError(
            f"Only {len(judgements)} valid semantic judgements obtained; "
            f"{repetitions} required. Failures: {failures}"
        )

    one_votes = sum(item["score"] for item in judgements)
    final_score = int(one_votes > repetitions // 2)
    winning = [item for item in judgements if item["score"] == final_score]
    return {
        "status": "scored",
        "score": final_score,
        "semantic_equivalent": bool(final_score),
        "vote_counts": {
            "equivalent": one_votes,
            "not_equivalent": repetitions - one_votes,
        },
        "voting_reason": " | ".join(item["reason"] for item in winning),
        "judgements": judgements,
        "failed_attempts": failures,
    }


def historical_equivalence(score_detail, comparison):
    optimized = comparison.get("optimized_performance") or comparison
    value = optimized.get("results_equivalent")
    if value is None:
        value = score_detail.get("optimized_results_equivalent")
    if value is None:
        value = score_detail.get("results_equivalent")
    return value


def comparison_disposition(historical, semantic_score):
    semantic = bool(semantic_score)
    if historical is False and semantic:
        return "corrected_from_0_to_1"
    if historical is True and not semantic:
        return "corrected_from_1_to_0"
    if historical is None:
        return "no_historical_execution_score"
    return "unchanged_1" if semantic else "unchanged_0"


def compute_performance_score(
    functional_gate,
    baseline_time,
    recovered_time,
    zero_score_ratio,
    failure_reasons,
):
    ratio = None
    if (
        baseline_time is not None
        and baseline_time > 0
        and recovered_time is not None
        and recovered_time >= 0
    ):
        ratio = recovered_time / baseline_time

    zero_reasons = list(failure_reasons)
    if not functional_gate and not zero_reasons:
        zero_reasons.append("functional_gate_failed")
    if baseline_time is None or baseline_time <= 0:
        zero_reasons.append("invalid_baseline_time")
    if recovered_time is None or recovered_time < 0:
        zero_reasons.append("recovered_time_unavailable")
    if ratio is not None and ratio >= zero_score_ratio:
        zero_reasons.append("execution_ratio_at_or_above_8x")
    zero_reasons = list(dict.fromkeys(zero_reasons))

    detail = {
        "functional_gate": bool(functional_gate),
        "execution_time_ratio": ratio,
        "score_band": "functional_failure",
        "zero_score_ratio": float(zero_score_ratio),
        "full_score_ratio": FULL_SCORE_RATIO,
        "zero_reasons": zero_reasons,
    }
    if zero_reasons:
        if functional_gate and ratio is not None:
            detail["score_band"] = "at_or_above_8x"
        detail["recovery_score"] = 0.0
        return 0.0, detail

    if ratio >= 1.0:
        score = 0.8 - (ratio - 1.0) * 0.7 / (zero_score_ratio - 1.0)
        score_band = "1x_to_8x"
    elif ratio >= FULL_SCORE_RATIO:
        score = 0.8 + (1.0 - ratio) * 0.16 / (1.0 - FULL_SCORE_RATIO)
        score_band = "0.2x_to_1x"
    else:
        score = 1.0
        score_band = "below_0.2x"
    score = round_score(min(1.0, max(0.0, score)))
    detail.update({"score_band": score_band, "recovery_score": score})
    return score, detail


def operational_failure_reasons(score_detail, semantic_score):
    old_reasons = (
        score_detail.get("optimized_recovery_failure_reasons")
        or score_detail.get("recovery_failure_reasons")
        or []
    )
    derived_reasons = {
        "functional_gate_failed",
        "result_mismatch",
        "semantic_mismatch",
        "invalid_baseline_time",
        "recovered_time_unavailable",
        "execution_ratio_at_or_above_8x",
    }
    reasons = [
        reason for reason in old_reasons if reason not in derived_reasons
    ]
    if not semantic_score:
        reasons.append("semantic_mismatch")
    return list(dict.fromkeys(reasons))


def update_score_detail(
    source,
    semantic_detail,
    comparison,
    optimized_workload_path,
    original_workload_path,
):
    updated = copy.deepcopy(source)
    semantic_score = semantic_detail["score"]
    historical = historical_equivalence(source, comparison)
    initialization = (
        source.get("initialization_comparison")
        or (comparison.get("optimized_performance") or comparison).get(
            "initialization_comparison"
        )
        or {}
    )
    initialization_compatible = initialization.get("compatible", True)
    recovered_error_free = source.get("recovered_error_free", True)
    functional_gate = bool(
        recovered_error_free and initialization_compatible and semantic_score
    )

    performance = (
        source.get("optimized_performance_comparison")
        or source.get("performance_comparison")
        or (comparison.get("optimized_performance") or comparison).get(
            "performance_comparison"
        )
        or {}
    )
    baseline_time = performance.get(
        "baseline_median_seconds", source.get("baseline_total_time")
    )
    recovered_time = performance.get(
        "recovered_median_seconds",
        source.get("optimized_recovered_total_time", source.get("recovered_total_time")),
    )
    previous_recovery_detail = (
        source.get("optimized_recovery_score_detail")
        or source.get("recovery_score_detail")
        or {}
    )
    zero_score_ratio = previous_recovery_detail.get(
        "zero_score_ratio", DEFAULT_ZERO_SCORE_RATIO
    )
    if (
        isinstance(zero_score_ratio, bool)
        or not isinstance(zero_score_ratio, (int, float))
        or not math.isfinite(zero_score_ratio)
        or zero_score_ratio <= 1
    ):
        zero_score_ratio = DEFAULT_ZERO_SCORE_RATIO

    failure_reasons = operational_failure_reasons(source, semantic_score)
    performance_score, recovery_detail = compute_performance_score(
        functional_gate,
        baseline_time,
        recovered_time,
        float(zero_score_ratio),
        failure_reasons,
    )
    diagnosis_score = source.get(
        "diagnosis_score", source.get("artifact_score")
    )
    mode = source.get("mode", "both")
    if mode == "solution":
        total_score = performance_score
    else:
        diagnosis_score = float(diagnosis_score or 0.0)
        total_score = round_score(
            diagnosis_score * DIAGNOSIS_SCORE_WEIGHT
            + performance_score * PERFORMANCE_SCORE_WEIGHT
        )

    disposition = comparison_disposition(historical, semantic_score)
    updated.update({
        "score": total_score,
        "recovery_score": performance_score,
        "performance_score": performance_score,
        "recovery_score_detail": recovery_detail,
        "recovery_failure_reasons": recovery_detail["zero_reasons"],
        "recovery_successful": functional_gate,
        "results_equivalent": bool(semantic_score),
        "functional_gate": functional_gate,
        "optimized": bool(
            functional_gate
            and recovery_detail["execution_time_ratio"] is not None
            and recovery_detail["execution_time_ratio"] <= 1
        ),
        "execution_time_ratio": recovery_detail["execution_time_ratio"],
        "baseline_total_time": baseline_time,
        "recovered_total_time": recovered_time,
        "optimized_score": total_score,
        "optimized_performance_score": performance_score,
        "optimized_recovery_score_detail": recovery_detail,
        "optimized_recovered_total_time": recovered_time,
        "optimized_functional_gate": functional_gate,
        "optimized_results_equivalent": bool(semantic_score),
        "optimized_recovery_failure_reasons": recovery_detail["zero_reasons"],
        "execution_results_equivalent": historical,
        "workload_semantic_consistency_score": semantic_score,
        "workload_semantic_equivalent": bool(semantic_score),
        "workload_semantic_score_disposition": disposition,
        "workload_semantic_reason": semantic_detail["voting_reason"],
        "workload_semantic_judge_detail_file": SEMANTIC_DETAIL_FILENAME,
        "original_verified_workload_path": str(original_workload_path),
        "optimized_verified_workload_path": str(optimized_workload_path),
    })
    return updated


def copy_unjudged_score(source, status, reason):
    updated = copy.deepcopy(source)
    updated.update({
        "workload_semantic_consistency_score": None,
        "workload_semantic_equivalent": None,
        "workload_semantic_score_disposition": status,
        "workload_semantic_reason": reason,
        "workload_semantic_judge_detail_file": SEMANTIC_DETAIL_FILENAME,
    })
    return updated


def score_log(logger, sample_id, sample_index, detail):
    logger.info(
        "[Sample ID: %s] [EVAL][SAMPLE %d][SCORE][Optimized Performance] "
        "sample_id=`%s` score=%.6f (recovery=%.6f, diagnosis=%s, "
        "performance=%.6f, functional_gate=%s, semantic_consistency=%s)",
        sample_id,
        sample_index,
        sample_id,
        float(detail.get("optimized_score", detail.get("score", 0.0)) or 0.0),
        float(detail.get("optimized_performance_score", 0.0) or 0.0),
        detail.get("diagnosis_score"),
        float(detail.get("performance_score", 0.0) or 0.0),
        detail.get("optimized_functional_gate", detail.get("functional_gate")),
        detail.get("workload_semantic_consistency_score"),
    )


def evaluate_sample(
    sample_id,
    sample_index,
    config,
    logger,
    overwrite=False,
    judge_function=call_judge,
):
    output_sample = config["output_directory"] / sample_id
    output_score_path = output_sample / SCORE_DETAIL_FILENAME
    output_semantic_path = output_sample / SEMANTIC_DETAIL_FILENAME
    if (
        not overwrite
        and output_score_path.is_file()
        and output_semantic_path.is_file()
    ):
        detail = read_json(output_score_path)
        score_log(logger, sample_id, sample_index, detail)
        return detail

    evaluation_sample = config["diagnosis_evaluation_directory"] / sample_id
    source_score_path = evaluation_sample / SCORE_DETAIL_FILENAME
    if not source_score_path.is_file():
        raise FileNotFoundError(f"Missing historical score: {source_score_path}")
    source_score = read_json(source_score_path)

    optimized_path = (
        config["diagnosis_method_directory"]
        / sample_id
        / "optimized_verified_workload.sql"
    )
    if not optimized_path.is_file() or not read_text(optimized_path).strip():
        reason = (
            "No non-empty optimized_verified_workload.sql was produced; "
            "the historical score is retained without an LLM semantic judgement."
        )
        semantic_detail = {
            "sample_id": sample_id,
            "status": "skipped_no_nonempty_optimized_workload",
            "score": None,
            "semantic_equivalent": None,
            "reason": reason,
        }
        updated = copy_unjudged_score(
            source_score,
            semantic_detail["status"],
            reason,
        )
        write_json_atomic(output_semantic_path, semantic_detail)
        write_json_atomic(output_score_path, updated)
        score_log(logger, sample_id, sample_index, updated)
        return updated

    anomaly_sample = find_sample_directory(
        config["anomaly_sample_directory"], sample_id
    )
    original_path = anomaly_sample / "verified_workload.sql"
    comparison_path = evaluation_sample / COMPARISON_DETAIL_FILENAME
    if not original_path.is_file():
        raise FileNotFoundError(f"Missing original workload: {original_path}")
    if not comparison_path.is_file():
        raise FileNotFoundError(
            f"Missing historical comparison detail: {comparison_path}"
        )

    original_workload = read_text(original_path)
    optimized_workload = read_text(optimized_path)
    comparison = read_json(comparison_path)
    dbms = source_score.get("dbms", "unknown")
    prompt = build_judge_prompt(
        sample_id,
        dbms,
        original_workload,
        optimized_workload,
        compact_comparison_evidence(comparison),
    )
    semantic_detail = judge_semantics(
        prompt,
        config["_llm_configuration"],
        config["judge_repetitions"],
        logger,
        sample_id,
        judge_function=judge_function,
    )
    historical = historical_equivalence(source_score, comparison)
    semantic_detail.update({
        "sample_id": sample_id,
        "judge_repetitions": config["judge_repetitions"],
        "historical_execution_equivalence_score": (
            None if historical is None else int(bool(historical))
        ),
        "score_disposition": comparison_disposition(
            historical, semantic_detail["score"]
        ),
        "original_verified_workload_path": str(original_path),
        "optimized_verified_workload_path": str(optimized_path),
        "historical_comparison_detail_path": str(comparison_path),
    })
    updated = update_score_detail(
        source_score,
        semantic_detail,
        comparison,
        optimized_path,
        original_path,
    )
    write_json_atomic(output_semantic_path, semantic_detail)
    write_json_atomic(output_score_path, updated)
    score_log(logger, sample_id, sample_index, updated)
    return updated


def discover_sample_ids(evaluation_directory):
    return sorted(
        (
            path.name
            for path in evaluation_directory.iterdir()
            if path.is_dir() and (path / SCORE_DETAIL_FILENAME).is_file()
        ),
        key=natural_key,
    )


def aggregate_results(details, config):
    scored = [
        detail for detail in details
        if detail.get("score") is not None
    ]
    semantic_scored = [
        detail for detail in details
        if detail.get("workload_semantic_consistency_score") is not None
    ]

    def average(field, source):
        values = [
            float(item[field])
            for item in source
            if item.get(field) is not None
        ]
        return round_score(statistics.mean(values)) if values else None

    dispositions = {}
    for detail in details:
        key = detail.get("workload_semantic_score_disposition", "unknown")
        dispositions[key] = dispositions.get(key, 0) + 1
    return {
        "total_sample_num": len(details),
        "scored_sample_num": len(scored),
        "semantic_judged_sample_num": len(semantic_scored),
        "semantic_skipped_sample_num": len(details) - len(semantic_scored),
        "judge_repetitions": config["judge_repetitions"],
        "parallelism_num": config["parallelism_num"],
        "overall_optimized_score": average("optimized_score", scored),
        "overall_diagnosis_score": average("diagnosis_score", scored),
        "overall_optimized_performance_score": average(
            "optimized_performance_score", scored
        ),
        "overall_workload_semantic_consistency_score": average(
            "workload_semantic_consistency_score", semantic_scored
        ),
        "semantic_score_dispositions": dispositions,
        "optimized_score_list": [
            item.get("optimized_score") for item in scored
        ],
        "workload_semantic_consistency_score_list": [
            item["workload_semantic_consistency_score"]
            for item in semantic_scored
        ],
        "sample_details": details,
    }


def sanitized_config(config):
    return {
        key: str(value) if isinstance(value, Path) else value
        for key, value in config.items()
        if not key.startswith("_")
    }


def setup_logger(output_directory):
    output_directory.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("DB_DIAG.eval_workload")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(message)s")
    file_handler = logging.FileHandler(
        output_directory / "log.txt", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def run(config, logger, selected_sample_ids=None, overwrite=False):
    sample_ids = discover_sample_ids(config["diagnosis_evaluation_directory"])
    if selected_sample_ids:
        selected = set(selected_sample_ids)
        unknown = sorted(selected.difference(sample_ids), key=natural_key)
        if unknown:
            raise ValueError(f"Unknown sample id(s): {unknown}")
        sample_ids = [sample_id for sample_id in sample_ids if sample_id in selected]

    logger.info(
        "[Sample ID: ALL] [MAIN][EVAL WORKLOAD] Starting %d sample(s); "
        "parallelism=%d, judge_repetitions=%d",
        len(sample_ids),
        config["parallelism_num"],
        config["judge_repetitions"],
    )
    details_by_id = {}
    failures = {}
    with ThreadPoolExecutor(
        max_workers=config["parallelism_num"]
    ) as executor:
        futures = {
            executor.submit(
                evaluate_sample,
                sample_id,
                index,
                config,
                logger,
                overwrite,
            ): sample_id
            for index, sample_id in enumerate(sample_ids)
        }
        for future in as_completed(futures):
            sample_id = futures[future]
            try:
                details_by_id[sample_id] = future.result()
            except Exception as exc:
                failures[sample_id] = " ".join(str(exc).split())
                logger.error(
                    "[Sample ID: %s] [EVAL][ERROR] Semantic workload "
                    "evaluation failed: %s",
                    sample_id,
                    failures[sample_id],
                )

    details = [
        details_by_id[sample_id]
        for sample_id in sample_ids
        if sample_id in details_by_id
    ]
    result = aggregate_results(details, config)
    result.update({
        "scheduled_sample_num": len(sample_ids),
        "failed_sample_num": len(failures),
        "failed_samples": [
            {"sample_id": sample_id, "error": failures[sample_id]}
            for sample_id in sample_ids
            if sample_id in failures
        ],
    })
    write_json_atomic(config["output_directory"] / "evaluation_result.json", result)
    write_json_atomic(
        config["output_directory"] / "config.json",
        sanitized_config(config),
    )
    logger.info(
        "[Sample ID: ALL] [MAIN][EVAL WORKLOAD] Completed %d sample(s); "
        "semantic_judged=%d, overall_score=%s",
        len(details),
        result["semantic_judged_sample_num"],
        result["overall_optimized_score"],
    )
    if failures:
        raise RuntimeError(
            f"{len(failures)} sample(s) failed. Re-run the same command to "
            "resume incomplete samples; see evaluation_result.json."
        )
    return result


def parse_args():
    default_config = Path(__file__).with_name("config.json")
    parser = argparse.ArgumentParser(
        description=(
            "Strict LLM-as-a-Judge semantic evaluation for optimized SQL "
            "workloads using existing test_eval.py artifacts."
        )
    )
    parser.add_argument("--config", default=str(default_config))
    parser.add_argument(
        "--sample-ids",
        nargs="+",
        help="Evaluate only the listed sample IDs.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-judge samples that already have complete output.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_runtime_config(args.config)
    logger = setup_logger(config["output_directory"])
    run(
        config,
        logger,
        selected_sample_ids=args.sample_ids,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()
