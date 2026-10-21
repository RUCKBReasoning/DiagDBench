#!/usr/bin/env python3
"""Diagnose each sample with a baseline method, then score it immediately.

Per sample (output layout `<output_root_dir>/<sample_id>/`):
  1. baseline/       target method diagnosis + recovery (Baselines/codex driver)
  2. eval/           test_eval.py scoring in a freshly created evaluation container
                     (the diagnosis containers are removed first)
  3. eval_workload/  test_eval_workload.py semantic-consistency re-scoring

Every stage is resumable: a stage is skipped when its terminal artifact exists
and is newer than the artifact of the stage it depends on. Incomplete outputs
are archived to `<sample_id>/_attempts/` instead of being deleted.
"""

import argparse
import importlib
import inspect
import json
import logging
import os
import queue
import statistics
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import test_eval as te  # noqa: E402
import test_eval_workload as tew  # noqa: E402
from Baselines.codex import react  # noqa: E402
from llm_api import FatalAPIQuotaError  # noqa: E402
from utils import set_up_logger  # noqa: E402


DEFAULT_CONFIG = BASE_DIR / "Configs" / "test_baseline_eval" / "config_codex_common.json"
METHOD_MODULES = {
    "codex_pro": "Baselines.Codex_Pro.agent",
    "codex": "Baselines.codex.agent",
}
METHOD_DISPLAY_NAMES = {"codex_pro": "Codex Pro", "codex": "Codex"}
SCORE_DETAIL = tew.SCORE_DETAIL_FILENAME
RUN_ID = te.RUN_LEVEL_SAMPLE_ID

# Interfaces this pipeline calls directly; checked at start-up so an upstream
# signature change fails fast instead of mid-run.
EXPECTED_SIGNATURES = (
    (react._run_case_body, (
        "case_index", "total_cases", "case", "config", "privileged", "output_dir",
        "logger", "base_docker_name", "container_config", "retries", "init_timeout",
        "dbms", "database_name", "schema_name", "user_config", "hardware", "files",
        "llm_requester", "agent_runner", "containers",
    )),
    (te.evaluate_single_sample, (
        "i", "anomaly_pair", "shared", "base_docker_name", "worker_host_port",
        "logger", "timeout_times", "median_runs",
    )),
    (te.resolve_solution_artifact_paths, (
        "solution_script_directory", "sample_id", "case_index",
    )),
)
METHOD_SIGNATURES = {
    "run_agent": ("prompt", "config", "container", "output", "files", "logger"),
    "validate_config": ("config",),
    "preflight": ("config",),
}


def project_path(value):
    path = Path(os.path.expanduser(str(value)))
    return Path(os.path.realpath(path if path.is_absolute() else BASE_DIR / path))


def one_line(value):
    return " ".join(str(value).split())


def timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")


def read_json_if_file(path):
    try:
        return tew.read_json(path) if Path(path).is_file() else None
    except (OSError, json.JSONDecodeError):
        return None


def newer(path, reference):
    """True when `path` is at least as new as `reference` (or no reference)."""
    return not Path(reference).exists() or (
        Path(path).stat().st_mtime_ns >= Path(reference).stat().st_mtime_ns
    )


def archive(path, sample_root, label):
    path = Path(path)
    if not os.path.lexists(path):
        return None
    target = sample_root / "_attempts" / f"{label}_{timestamp()}"
    target.parent.mkdir(parents=True, exist_ok=True)
    os.replace(path, target)
    return target


def redact(value):
    if isinstance(value, dict):
        return {
            key: ("***" if "api_key" in key.lower() and key != "api_key_env"
                  and isinstance(item, str) and item else redact(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def mean(values):
    values = [float(value) for value in values if value is not None]
    return te.round_score(statistics.mean(values)) if values else None


def check_signatures(method_module):
    checks = list(EXPECTED_SIGNATURES) + [
        (getattr(method_module, name), names) for name, names in METHOD_SIGNATURES.items()
    ]
    for function, names in checks:
        actual = tuple(inspect.signature(function).parameters)
        if actual[:len(names)] != names:
            raise RuntimeError(
                f"Interface changed: {function.__module__}.{function.__qualname__}"
                f"{actual}; this pipeline expects {names}. Adapt test_baseline_eval.py."
            )


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
def load_config(config_path):
    config_path = project_path(config_path)
    raw = tew.read_json(config_path)
    for section in ("baseline", "eval", "eval_workload"):
        if not isinstance(raw.get(section), dict):
            raise ValueError(f"Config section `{section}` must be an object.")
    parallelism = raw.get("parallelism_num", 1)
    if isinstance(parallelism, bool) or not isinstance(parallelism, int) or parallelism <= 0:
        raise ValueError("parallelism_num must be a positive integer.")

    key_info_path = project_path(raw["key_information_path"])
    output_root = project_path(raw["output_root_dir"])

    # ---- baseline (diagnosis method) -----------------------------------
    diagnosis = dict(raw["baseline"])
    workflow = str(diagnosis.get("workflow_type", "")).lower()
    if workflow not in METHOD_MODULES:
        raise ValueError(f"Unsupported workflow_type `{workflow}`; known: {sorted(METHOD_MODULES)}.")
    method_module = importlib.import_module(METHOD_MODULES[workflow])
    check_signatures(method_module)
    api_key_source = diagnosis.pop("api_key_source", None)
    retries = diagnosis.pop("sample_retry_times", 0)
    if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
        raise ValueError("baseline.sample_retry_times must be a non-negative integer.")
    diagnosis.update(
        key_information_path=str(key_info_path),
        workflow_type=workflow,
        parallelism_num=parallelism,
        llm_configuration=react.diagnosis_llm_config(diagnosis.get("llm_configuration", {})),
    )
    method_module.validate_config(diagnosis)
    react.validate_config(diagnosis, True)
    inject_model_key(diagnosis["llm_configuration"], api_key_source)

    # ---- eval (test_eval.py) --------------------------------------------
    evaluation = dict(raw["eval"])
    mode = te.evaluation_mode(evaluation)
    timeout_times = float(evaluation.get("timeout_times", te.RECOVERY_ZERO_SCORE_RATIO))
    if timeout_times != te.RECOVERY_ZERO_SCORE_RATIO:
        raise ValueError(f"eval.timeout_times must be exactly {te.RECOVERY_ZERO_SCORE_RATIO:g}.")
    median_runs = max(1, int(evaluation.get("median_runs", 1)))
    comparison_mode = str(evaluation.get(
        "result_comparison_mode", te.DEFAULT_RESULT_COMPARISON_MODE)).strip().lower()
    if comparison_mode not in te.RESULT_COMPARISON_MODES:
        raise ValueError(f"eval.result_comparison_mode must be one of {sorted(te.RESULT_COMPARISON_MODES)}.")
    if mode != "report" and comparison_mode == "strict" and median_runs < 2:
        raise ValueError("strict result comparison requires eval.median_runs >= 2.")
    positive = {
        "baseline_workload_timeout_seconds": te.DEFAULT_BASELINE_WORKLOAD_TIMEOUT_SECONDS,
        "initialization_workload_timeout_seconds": te.DEFAULT_INITIALIZATION_WORKLOAD_TIMEOUT_SECONDS,
    }
    for field, default in positive.items():
        value = evaluation.get(field, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ValueError(f"eval.{field} must be a positive number.")
        positive[field] = float(value)
    eval_retries = evaluation.get("sample_retry_times", te.DEFAULT_SAMPLE_RETRY_TIMES)
    if isinstance(eval_retries, bool) or not isinstance(eval_retries, int) or eval_retries < 0:
        raise ValueError("eval.sample_retry_times must be a non-negative integer.")
    eval_shared = {
        "mode": mode,
        "_cleanup_worker_prefix": True,
        "initialization_workload_timeout_seconds": positive["initialization_workload_timeout_seconds"],
        "anomaly_directory": str(key_info_path.parent),
        # Artifacts are mapped to `<root>/<sid>/baseline` by `baseline_artifact_paths`.
        "solution_script_directory": str(output_root),
        "output_base_dir": None,
        "base_dir": str(BASE_DIR),
        "result_comparison_mode": comparison_mode,
        "ground_truth_diagnosis": evaluation.get("ground_truth_diagnosis", te.DEFAULT_GROUND_TRUTH_DIAGNOSIS),
        "judge_llm_config": te.resolve_judge_llm_configuration(evaluation) if mode != "solution" else None,
        "judge_repetitions": evaluation.get("judge_repetitions", te.DEFAULT_JUDGE_REPETITIONS),
        "baseline_workload_timeout_seconds": positive["baseline_workload_timeout_seconds"],
        "superuser_config": dict(evaluation.get("superuser_config") or te.DEFAULT_SUPERUSER_CONFIG),
    }

    # ---- eval_workload (test_eval_workload.py) --------------------------
    workload = dict(raw["eval_workload"])
    repetitions = workload.get("judge_repetitions", 3)
    if isinstance(repetitions, bool) or not isinstance(repetitions, int) or repetitions <= 0 or repetitions % 2 == 0:
        raise ValueError("eval_workload.judge_repetitions must be an odd positive integer.")

    ports = {
        "diagnosis": int(diagnosis.get("host_port", 5427)),
        "eval": int(evaluation.get("host_port", 5426)),
    }
    if abs(ports["diagnosis"] - ports["eval"]) < parallelism:
        raise ValueError("baseline.host_port and eval.host_port port windows overlap.")
    return {
        "_config_path": config_path,
        "_raw": raw,
        "key_information_path": key_info_path,
        "output_root": output_root,
        "parallelism_num": parallelism,
        "method_module": method_module,
        "method_name": METHOD_DISPLAY_NAMES.get(workflow, workflow),
        "diagnosis": diagnosis,
        "diagnosis_retries": retries,
        "diagnosis_docker_name": str(diagnosis["docker_name"]),
        "eval_docker_name": str(evaluation.get("docker_name", "evaluation")),
        "ports": ports,
        "eval_shared": eval_shared,
        "eval_retries": eval_retries,
        "timeout_times": timeout_times,
        "median_runs": median_runs,
        "workload_llm": tew.resolve_llm_configuration(workload, config_path.parent),
        "workload_repetitions": repetitions,
    }


def inject_model_key(llm_configuration, source):
    """Export the method's model key to its env variable; never log or persist it."""
    env_name = llm_configuration.get("api_key_env")
    if not env_name or os.environ.get(env_name):
        return
    if not isinstance(source, dict) or not source.get("config_path"):
        raise ValueError(f"Set `{env_name}` or provide baseline.api_key_source.config_path.")
    source_path = project_path(source["config_path"])
    value = tew.read_json(source_path).get(source.get("config_key", "llm_configuration"), {})
    key = value.get("api_key") if isinstance(value, dict) else None
    if not key:
        raise ValueError(f"No api_key found in {source_path} for `{env_name}`.")
    os.environ[env_name] = key


def baseline_artifact_paths(solution_script_directory, sample_id, case_index):
    """Replacement for test_eval.resolve_solution_artifact_paths (pure, thread-safe)."""
    directory = os.path.join(solution_script_directory, str(sample_id), "baseline")
    return {
        "directory": directory,
        "solution_script": os.path.join(directory, "solution_script.sh"),
        "optimized_workload": os.path.join(directory, "optimized_verified_workload.sql"),
        "diagnosis_report": os.path.join(directory, "diagnosis_report.md"),
    }


# --------------------------------------------------------------------------- #
# Score formulas for the per-sample log
# --------------------------------------------------------------------------- #
def number(value, digits=6):
    return "N/A" if value is None else f"{float(value):.{digits}f}"


def seconds(value):
    return "N/A" if value is None else f"{float(value):.6f}s"


def diagnosis_formula(judge_detail, diagnosis_score):
    if diagnosis_score is None:
        return "diagnosis=N/A"
    judgements = (judge_detail or {}).get("judgements") or []
    if not judgements:
        return f"diagnosis={number(diagnosis_score)} (no Judge vote recorded)"
    votes = []
    for index, item in enumerate(judgements, 1):
        dimensions = item.get("dimension_scores") or {}
        terms = "+".join(
            f"{weight:.2f}×{number(dimensions.get(name), 4)}"
            for name, weight in te.DIAGNOSIS_JUDGE_DIMENSIONS.items()
        )
        votes.append(f"J{index}={terms}={number(item.get('score'))}")
    return (
        "diagnosis=median(J_k), J_k=0.45×root_cause+0.25×evidence+0.20×remediation"
        f"+0.10×safety: median[{'; '.join(votes)}]={number(diagnosis_score)}"
    )


def recovery_formula(performance_score, detail, baseline_time, recovered_time, reason_code=None):
    if performance_score is None:
        return "recovery=N/A"
    if not detail:
        return f"recovery={number(performance_score)} (no recovery detail; reason_code={reason_code})"
    ratio = detail.get("execution_time_ratio")
    band = detail.get("score_band")
    zero_ratio = float(detail.get("zero_score_ratio", te.RECOVERY_ZERO_SCORE_RATIO))
    full_ratio = float(detail.get("full_score_ratio", te.RECOVERY_FULL_SCORE_RATIO))
    zero_reasons = detail.get("zero_reasons") or []
    if zero_reasons:
        expression = f"0 [zero_reasons={zero_reasons}]"
    elif band == "below_0.2x":
        expression = f"1 [r<{full_ratio:g}]"
    elif band == "0.2x_to_1x":
        expression = f"0.8+(1-{number(ratio)})×0.16/{1 - full_ratio:g}"
    elif band == "1x_to_8x":
        expression = f"0.8-({number(ratio)}-1)×0.7/{zero_ratio - 1:g}"
    else:
        expression = str(band)
    return (
        f"recovery={expression}={number(performance_score)} "
        f"(r=recovered/baseline={seconds(recovered_time)}/{seconds(baseline_time)}"
        f"={number(ratio)}, band={band}, functional_gate={detail.get('functional_gate')})"
    )


def total_formula(score, diagnosis_score, performance_score, mode):
    if score is None:
        return "score=N/A"
    if mode == "solution":
        return f"score=recovery={number(score)}"
    if mode == "report":
        return f"score=diagnosis={number(score)}"
    return (
        f"score={te.DIAGNOSIS_SCORE_WEIGHT:.2f}×diagnosis+{te.RECOVERY_SCORE_WEIGHT:.2f}×recovery"
        f"={te.DIAGNOSIS_SCORE_WEIGHT:.2f}×{number(diagnosis_score)}"
        f"+{te.RECOVERY_SCORE_WEIGHT:.2f}×{number(performance_score)}={number(score)}"
    )


def score_groups(eval_detail, workload_detail):
    """Extract the three requested score groups from the two score_detail files."""
    groups = {}
    if eval_detail:
        groups["Original Workload"] = (eval_detail, {
            "score": eval_detail.get("original_score"),
            "performance": eval_detail.get("original_performance_score"),
            "recovery_detail": eval_detail.get("original_recovery_score_detail"),
            "recovered_time": eval_detail.get("original_recovered_total_time"),
        })
        groups["Optimized Workload"] = (eval_detail, {
            "score": eval_detail.get("optimized_score", eval_detail.get("score")),
            "performance": eval_detail.get("optimized_performance_score"),
            "recovery_detail": eval_detail.get("optimized_recovery_score_detail"),
            "recovered_time": eval_detail.get(
                "optimized_recovered_total_time", eval_detail.get("recovered_total_time")),
        })
    if workload_detail:
        groups["Consistency Updated"] = (workload_detail, {
            "score": workload_detail.get("optimized_score", workload_detail.get("score")),
            "performance": workload_detail.get("optimized_performance_score"),
            "recovery_detail": workload_detail.get("optimized_recovery_score_detail"),
            "recovered_time": workload_detail.get(
                "optimized_recovered_total_time", workload_detail.get("recovered_total_time")),
            "semantic": workload_detail.get("workload_semantic_consistency_score"),
            "disposition": workload_detail.get("workload_semantic_score_disposition"),
        })
    result = {}
    for name, (source, group) in groups.items():
        group.update(
            diagnosis=source.get("diagnosis_score"),
            baseline_time=source.get("baseline_total_time"),
            mode=source.get("mode", "both"),
            reason_code=source.get("reason_code"),
        )
        result[name] = group
    return result


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
class Pipeline:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.root = config["output_root"]
        self.method = config["method_module"]
        self.cases = list(react.validate_input_data(config["diagnosis"]))
        self.anomaly_root = config["key_information_path"].parent
        self.stop_event = threading.Event()
        self.fatal_marker = self.root / "fatal_api_error.json"

    # ---- stage 1: diagnosis -----------------------------------------------
    def run_diagnosis(self, index, case, output_dir, logger, base_docker_name, host_port):
        """Same preparation as react.run_case, but writing into `output_dir`."""
        config = self.config["diagnosis"]
        _, sample_dir = react.find_sample_directory(self.anomaly_root, case)
        files = react.required_files(sample_dir)
        sample_config = react.load_json_file(files["config"])
        for name in ("actions", "logs", "reasoning_process"):
            (output_dir / name).mkdir(parents=True, exist_ok=True)

        dbms = sample_config.get("dbms", case.get("dbms", config.get("dbms", "PostgreSQL 16.11")))
        database_name = str(sample_config.get("database_name", case["database_name"]))
        schema_name = str(sample_config.get("database_schema_name", case.get(
            "database_schema_name", config.get("database_schema_name", "diagnosis"))))
        if react.normalize_dbms(dbms) == "mysql":
            schema_name = database_name
        user_config = dict(react.DEFAULT_DB_USER)
        user_config.update(config.get("system_user_config") or {})
        user_config.update(sample_config.get("system_user_config") or {})
        user_config["DB_NAME"] = database_name
        user_config["DB_PORT"] = "3306" if react.normalize_dbms(dbms) == "mysql" else "5432"
        hardware = dict(react.DEFAULT_HARDWARE)
        hardware.update(config.get("system_hardware_config") or {})
        hardware.update(sample_config.get("system_hardware_config") or {})
        container_config = react.build_container_config(dbms, hardware, host_port, user_config)
        retries = int(config.get("docker_retry_times", 10))
        init_timeout = int(config.get(
            "database_init_timeout", react.DEFAULT_TIMEOUTS.get(database_name, 3600)))

        containers = []
        try:
            react._run_case_body(
                index, len(self.cases), case, config, True, output_dir, logger,
                base_docker_name, container_config, retries, init_timeout,
                dbms, database_name, schema_name, user_config, hardware, files,
                react.request_llm, self.method.run_agent, containers,
            )
        finally:
            for container_name in dict.fromkeys(containers):
                logger.info(f"[BASELINE][CLEANUP] Reclaiming container `{container_name}` and tmux session ...")
                react.kill_tmux_session(container_name)
                react.force_remove_container(container_name)

    def stage_baseline(self, index, case, sample_id, logger, base_docker_name, host_port):
        sample_root = self.root / sample_id
        output_dir = sample_root / "baseline"
        status_path = output_dir / "status.json"
        previous = read_json_if_file(status_path)
        if previous and previous.get("status") in react.TERMINAL_SAMPLE_STATUSES:
            logger.info(f"[BASELINE][RESUME] Terminal status=`{previous['status']}` exists; skipping diagnosis.")
            return previous
        attempts = self.config["diagnosis_retries"] + 1
        for attempt in range(1, attempts + 1):
            if self.stop_event.is_set() or self.fatal_marker.exists():
                raise RuntimeError("Fatal API marker exists; diagnosis not started.")
            # Codex_Pro refuses an existing `pro_control`; every attempt starts clean.
            moved = archive(output_dir, sample_root, "baseline")
            if moved:
                logger.warning(f"[BASELINE][RESUME] Archived incomplete diagnosis output to `{moved}`.")
            output_dir.mkdir(parents=True)
            logger.info(
                f"[BASELINE][SAMPLE {index}] Starting {self.config['method_name']} diagnosis "
                f"(attempt `{attempt}/{attempts}`, prefix `{base_docker_name}`, port {host_port}) ..."
            )
            try:
                self.run_diagnosis(index, case, output_dir, logger, base_docker_name, host_port)
                status = {"status": "completed", "attempt": attempt}
            except FatalAPIQuotaError as exc:
                tew.write_json_atomic(status_path, {"status": "interrupted_fatal_api", "error": one_line(exc)})
                raise
            except Exception as exc:  # noqa: BLE001 - a failed diagnosis is still scored.
                status = {"status": "failed", "attempt": attempt, "error": one_line(exc)}
                logger.error(f"[BASELINE][ERROR] Diagnosis attempt `{attempt}/{attempts}` failed: {status['error']}")
            tew.write_json_atomic(status_path, status)
            if status["status"] == "completed" or attempt == attempts:
                logger.info(f"[BASELINE][SAMPLE {index}] Diagnosis status=`{status['status']}`; output `{output_dir}`.")
                return status
            logger.warning(f"[BASELINE][RETRY] Retrying diagnosis attempt `{attempt + 1}/{attempts}`.")

    # ---- stage 2: test_eval ------------------------------------------------
    def terminal_eval_detail(self, sample_id, output_dir, logger):
        score_path = output_dir / SCORE_DETAIL
        detail = read_json_if_file(score_path)
        if (
            not isinstance(detail, dict)
            or str(detail.get("sample_id")) != sample_id
            or detail.get("mode") != self.config["eval_shared"]["mode"]
            or detail.get("status") not in te.TERMINAL_EVALUATION_STATUSES
            or (detail["status"] == "completed" and detail.get("score") is None)
        ):
            return None
        if not newer(score_path, self.root / sample_id / "baseline" / "status.json"):
            logger.warning("[EVAL][RESUME] Diagnosis artifacts are newer than the evaluation; re-evaluating.")
            return None
        if te.database_init_log_errors(str(output_dir / "init_db_log.txt"), detail.get("dbms")):
            return None
        return detail

    def stage_eval(self, index, case, sample_id, logger, diagnosis_prefix, eval_prefix, eval_port):
        sample_root = self.root / sample_id
        output_dir = sample_root / "eval"
        detail = self.terminal_eval_detail(sample_id, output_dir, logger)
        if detail is not None:
            logger.info(f"[EVAL][RESUME] Terminal evaluation status=`{detail['status']}` exists; skipping evaluation.")
            return detail
        # test_eval writes `<output_base_dir>/<sid>`; stage it, then move to `<sid>/eval`.
        stage_root = sample_root / "_eval_stage"
        for path, label in ((output_dir, "eval"), (stage_root, "eval_stage")):
            moved = archive(path, sample_root, label)
            if moved:
                logger.warning(f"[EVAL][RESUME] Archived incomplete evaluation output to `{moved}`.")

        logger.info(f"[EVAL][CLEANUP] Removing diagnosis containers with prefix `{diagnosis_prefix}` before evaluation ...")
        te.cleanup_evaluation_prefix(diagnosis_prefix)
        shared = dict(self.config["eval_shared"], output_base_dir=str(stage_root))
        retries = self.config["eval_retries"]
        for attempt in range(retries + 1):
            record = te.evaluate_single_sample(
                index, case, shared, eval_prefix, eval_port, self.logger,
                self.config["timeout_times"], median_runs=self.config["median_runs"],
            )
            if not record["is_aborted"]:
                break
            if attempt < retries:
                moved = archive(stage_root / sample_id, sample_root, "eval_aborted")
                logger.warning(
                    f"[EVAL][SAMPLE {index}][RETRY] Evaluation aborted; retrying attempt "
                    f"`{attempt + 2}/{retries + 1}` (previous output `{moved}`)."
                )
        os.replace(stage_root / sample_id, output_dir)
        stage_root.rmdir()
        return tew.read_json(output_dir / SCORE_DETAIL)

    # ---- stage 3: test_eval_workload --------------------------------------
    def stage_eval_workload(self, index, case, sample_id):
        """test_eval_workload.evaluate_sample with this pipeline's explicit paths."""
        sample_root = self.root / sample_id
        eval_dir = sample_root / "eval"
        output_dir = sample_root / "eval_workload"
        score_path = output_dir / SCORE_DETAIL
        semantic_path = output_dir / tew.SEMANTIC_DETAIL_FILENAME
        source_path = eval_dir / SCORE_DETAIL
        if score_path.is_file() and semantic_path.is_file() and newer(score_path, source_path):
            self.logger.info(
                f"[Sample ID: {sample_id}] [EVAL WORKLOAD][RESUME] Semantic score exists; skipping re-judgement."
            )
            return tew.read_json(score_path)
        moved = archive(output_dir, sample_root, "eval_workload")
        if moved:
            self.logger.warning(
                f"[Sample ID: {sample_id}] [EVAL WORKLOAD][RESUME] Archived stale output to `{moved}`."
            )
        source_score = tew.read_json(source_path)
        optimized_path = sample_root / "baseline" / "optimized_verified_workload.sql"
        if not optimized_path.is_file() or not tew.read_text(optimized_path).strip():
            reason = (
                "No non-empty optimized_verified_workload.sql was produced; "
                "the historical score is retained without an LLM semantic judgement."
            )
            semantic_detail = {
                "sample_id": sample_id, "status": "skipped_no_nonempty_optimized_workload",
                "score": None, "semantic_equivalent": None, "reason": reason,
            }
            updated = tew.copy_unjudged_score(source_score, semantic_detail["status"], reason)
        else:
            _, anomaly_sample = react.find_sample_directory(self.anomaly_root, case)
            original_path = anomaly_sample / "verified_workload.sql"
            comparison_path = eval_dir / tew.COMPARISON_DETAIL_FILENAME
            if not comparison_path.is_file():
                raise FileNotFoundError(f"Missing evaluation comparison detail: {comparison_path}")
            comparison = tew.read_json(comparison_path)
            prompt = tew.build_judge_prompt(
                sample_id, source_score.get("dbms", "unknown"),
                tew.read_text(original_path), tew.read_text(optimized_path),
                tew.compact_comparison_evidence(comparison),
            )
            semantic_detail = tew.judge_semantics(
                prompt, self.config["workload_llm"], self.config["workload_repetitions"],
                self.logger, sample_id,
            )
            historical = tew.historical_equivalence(source_score, comparison)
            semantic_detail.update({
                "sample_id": sample_id,
                "judge_repetitions": self.config["workload_repetitions"],
                "historical_execution_equivalence_score": None if historical is None else int(bool(historical)),
                "score_disposition": tew.comparison_disposition(historical, semantic_detail["score"]),
                "original_verified_workload_path": str(original_path),
                "optimized_verified_workload_path": str(optimized_path),
                "historical_comparison_detail_path": str(comparison_path),
            })
            updated = tew.update_score_detail(
                source_score, semantic_detail, comparison, optimized_path, original_path,
            )
        tew.write_json_atomic(semantic_path, semantic_detail)
        tew.write_json_atomic(score_path, updated)
        tew.score_log(self.logger, sample_id, index, updated)
        return updated

    # ---- per-sample orchestration -----------------------------------------
    def log_scores(self, index, sample_id, logger, eval_detail, workload_detail):
        judge_detail = read_json_if_file(self.root / sample_id / "eval" / "diagnosis_judge_detail.json")
        groups = score_groups(eval_detail, workload_detail)
        for name, group in groups.items():
            tag = f"[EVAL][SAMPLE {index}][FINAL SCORE][{name}]"
            extra = ""
            if "semantic" in group:
                extra = f" (semantic_consistency={group['semantic']}, disposition={group['disposition']})"
            logger.info(f"{tag} {total_formula(group['score'], group['diagnosis'], group['performance'], group['mode'])}{extra}")
            logger.info(f"{tag}[Diagnosis] {diagnosis_formula(judge_detail, group['diagnosis'])}")
            logger.info(f"{tag}[Recovery] " + recovery_formula(
                group["performance"], group["recovery_detail"], group["baseline_time"],
                group["recovered_time"], group["reason_code"],
            ))
        return groups

    def process_sample(self, index, case, slot):
        sample_id = te.anomaly_sample_id(case)
        logger = te._sample_logger(self.logger, sample_id)
        diagnosis_prefix = f"{self.config['diagnosis_docker_name']}_w{slot}"
        eval_prefix = f"{self.config['eval_docker_name']}_w{slot}"
        diagnosis_port = self.config["ports"]["diagnosis"] + slot
        eval_port = self.config["ports"]["eval"] + slot
        summary = {"sample_id": sample_id, "index": index}
        logger.info(
            f"[PIPELINE][SAMPLE {index}] == START == slot={slot}, diagnosis=`{diagnosis_prefix}`:{diagnosis_port}, "
            f"evaluation=`{eval_prefix}`:{eval_port}"
        )
        try:
            baseline_status = self.stage_baseline(index, case, sample_id, logger, diagnosis_prefix, diagnosis_port)
            summary["baseline_status"] = baseline_status["status"]
            eval_detail = self.stage_eval(index, case, sample_id, logger, diagnosis_prefix, eval_prefix, eval_port)
            summary["eval_status"] = eval_detail.get("status")
            workload_detail = None
            if eval_detail.get("status") in te.TERMINAL_EVALUATION_STATUSES:
                workload_detail = self.stage_eval_workload(index, case, sample_id)
                summary["eval_workload_status"] = workload_detail.get("workload_semantic_score_disposition")
            else:
                summary["error"] = f"evaluation status `{eval_detail.get('status')}`: {eval_detail.get('error')}"
                logger.error(f"[EVAL WORKLOAD][SKIP] Evaluation not terminal; no consistency update: {summary['error']}")
            groups = self.log_scores(index, sample_id, logger, eval_detail, workload_detail)
            for name, group in groups.items():
                key = name.lower().replace(" workload", "").replace(" ", "_")
                summary[f"{key}_score"] = group["score"]
                summary[f"{key}_performance_score"] = group["performance"]
            summary["diagnosis_score"] = eval_detail.get("diagnosis_score")
            summary["workload_semantic_consistency_score"] = (workload_detail or {}).get(
                "workload_semantic_consistency_score")
        except FatalAPIQuotaError as exc:
            self.stop_event.set()
            marker = {"status": "fatal_api_error", "sample_id": sample_id, "error": one_line(exc)}
            tew.write_json_atomic(self.fatal_marker, marker)
            logger.critical(f"[PIPELINE][FATAL] Fatal API quota error; no new sample will start: {marker['error']}")
            summary["error"] = marker["error"]
        except Exception as exc:  # noqa: BLE001 - one bad sample must not abort the run.
            summary["error"] = one_line(exc)
            logger.error(f"[PIPELINE][ERROR] Sample pipeline failed: {summary['error']}")
        finally:
            for prefix in (diagnosis_prefix, eval_prefix):
                try:
                    te.cleanup_evaluation_prefix(prefix)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"[PIPELINE][CLEANUP][WARNING] Prefix `{prefix}` cleanup failed: {one_line(exc)}")
        logger.info(f"[PIPELINE][SAMPLE {index}] == DONE == {json.dumps(summary, ensure_ascii=False)}")
        return summary

    def run(self, selected_ids=None):
        if self.fatal_marker.exists():
            raise RuntimeError(f"{self.fatal_marker} exists; archive it after fixing the API quota.")
        # Point test_eval at `<root>/<sid>/baseline` instead of `<root>/<sid>`.
        te.resolve_solution_artifact_paths = baseline_artifact_paths
        cases = [
            (index, case) for index, case in enumerate(self.cases)
            if not selected_ids or te.anomaly_sample_id(case) in selected_ids
        ]
        parallelism = self.config["parallelism_num"]
        run_logger = te._sample_logger(self.logger, RUN_ID)
        run_logger.info(
            f"[MAIN][PIPELINE] Method `{self.config['method_name']}`; {len(cases)} sample(s); "
            f"parallelism={parallelism}; output `{self.root}`."
        )
        # Each worker owns a stable slot: `{docker_name}_w{slot}` prefixes plus
        # `host_port + slot` for both the diagnosis and the evaluation container.
        slot_queue = queue.Queue()
        for slot in range(parallelism):
            slot_queue.put(slot)

        def worker(index, case):
            slot = slot_queue.get()
            try:
                if self.stop_event.is_set():
                    return {"sample_id": te.anomaly_sample_id(case), "index": index,
                            "error": "skipped after fatal API error"}
                return self.process_sample(index, case, slot)
            finally:
                slot_queue.put(slot)

        summaries = {}
        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            futures = {executor.submit(worker, index, case): index for index, case in cases}
            for future in as_completed(futures):
                summaries[futures[future]] = future.result()
        ordered = [summaries[index] for index, _ in cases]
        result = self.aggregate(ordered)
        tew.write_json_atomic(self.root / "evaluation_result.json", result)
        for name in ("original", "optimized", "consistency_updated"):
            run_logger.info(
                f"[MAIN][PIPELINE][{name}] Overall Score : {number(result[f'overall_{name}_score'])} "
                f"(performance={number(result[f'overall_{name}_performance_score'])})"
            )
        run_logger.info(
            f"[MAIN][PIPELINE] Overall Diagnosis Score : {number(result['overall_diagnosis_score'])}; "
            f"Overall Semantic Consistency : {number(result['overall_workload_semantic_consistency_score'])}"
        )
        run_logger.info(
            f"[MAIN][PIPELINE] Failed Samples : {result['failed_sample_num']} / {len(ordered)}; "
            f"Result Saved To : {self.root / 'evaluation_result.json'}"
        )
        if self.stop_event.is_set():
            raise SystemExit(42)
        if result["failed_sample_num"]:
            raise SystemExit(1)
        return result

    def aggregate(self, summaries):
        failed = [item for item in summaries if item.get("error")]
        result = {
            "method": self.config["method_name"],
            "total_sample_num": len(summaries),
            "failed_sample_num": len(failed),
            "failed_samples": [{"sample_id": item["sample_id"], "error": item["error"]} for item in failed],
            "overall_diagnosis_score": mean(item.get("diagnosis_score") for item in summaries),
            "overall_workload_semantic_consistency_score": mean(
                item.get("workload_semantic_consistency_score") for item in summaries),
        }
        for name in ("original", "optimized", "consistency_updated"):
            result[f"overall_{name}_score"] = mean(item.get(f"{name}_score") for item in summaries)
            result[f"overall_{name}_performance_score"] = mean(
                item.get(f"{name}_performance_score") for item in summaries)
        for stage in ("baseline_status", "eval_status", "eval_workload_status"):
            counts = {}
            for item in summaries:
                counts[str(item.get(stage))] = counts.get(str(item.get(stage)), 0) + 1
            result[f"{stage}_counts"] = counts
        result["sample_summaries"] = summaries
        return result


def setup_logger(output_root):
    output_root.mkdir(parents=True, exist_ok=True)
    log_path = output_root / "log.txt"
    if log_path.exists():  # set_up_logger truncates; keep the previous run's log.
        backup = output_root / "_logs" / f"log_{timestamp()}.txt"
        backup.parent.mkdir(exist_ok=True)
        os.replace(log_path, backup)
    set_up_logger(str(log_path))
    return logging.getLogger("DB_DIAG.test_baseline_eval")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Baseline diagnosis followed by test_eval and test_eval_workload scoring, per sample."
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--sample-ids", nargs="+", help="Process only the listed sample IDs.")
    parser.add_argument("--dry-run", action="store_true", help="Validate config, samples and interfaces only.")
    parser.add_argument("--preflight", action="store_true", help="Run the method preflight and exit.")
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)
    if args.preflight:
        print(json.dumps(config["method_module"].preflight(config["diagnosis"]), indent=2, default=str))
        return
    if args.dry_run:
        cases = list(react.validate_input_data(config["diagnosis"]))
        parallelism = config["parallelism_num"]
        ports = config["ports"]
        print(f"[DRY RUN] Method `{config['method_name']}` validated; {len(cases)} sample(s).")
        print(f"[DRY RUN] Output root: {config['output_root']}")
        print(f"[DRY RUN] Diagnosis containers `{config['diagnosis_docker_name']}_w<0..{parallelism - 1}>`, "
              f"ports {ports['diagnosis']}-{ports['diagnosis'] + parallelism - 1}")
        print(f"[DRY RUN] Evaluation containers `{config['eval_docker_name']}_w<0..{parallelism - 1}>`, "
              f"ports {ports['eval']}-{ports['eval'] + parallelism - 1}")
        return
    config["method_module"].preflight(config["diagnosis"])
    logger = setup_logger(config["output_root"])
    tew.write_json_atomic(config["output_root"] / "config.json", redact(config["_raw"]))
    run_logger = te._sample_logger(logger, RUN_ID)
    run_logger.info("[MAIN][Baseline Diagnosis + Evaluation] == START ==")
    run_logger.info(f"[MAIN] Config: {config['_config_path']}")
    Pipeline(config, logger).run(set(args.sample_ids or []))


if __name__ == "__main__":
    main()

