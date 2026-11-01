#!/usr/bin/env python3
"""Unified diagnosis and reconstructed semantic-Judge evaluation.

Method selection is instance-local. All methods share one scoring engine and
write <sample>/baseline and <sample>/eval without import-time global patches.
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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from Evaluation import test_eval as te  # noqa: E402
from Evaluation.test_eval import read_json, write_json_atomic
from Baselines.Codex import react  # noqa: E402
from llm_api import FatalAPIQuotaError  # noqa: E402
from Baselines.preparation import PreparationError, preparation_failure
from utils import set_up_logger  # noqa: E402


DEFAULT_CONFIG = BASE_DIR / "Configs" / "evaluation" / "codex" / "example.json"
METHOD_MODULES = {
    "codex": "Baselines.Codex.agent",
    "react": "Evaluation.method_adapters",
    "db_gpt": "Evaluation.method_adapters",
    "dbaiops": "Evaluation.method_adapters",
}
METHOD_DISPLAY_NAMES = {"codex": "Codex", "react": "ReAct", "db_gpt": "DB-GPT", "dbaiops": "DBAIOps"}
SCORE_DETAIL = te.SCORE_DETAIL_FILENAME
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
        return read_json(path) if Path(path).is_file() else None
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


def check_signatures(method_module, driver=None):
    driver = driver or react
    checks = [(driver._run_case_body, EXPECTED_SIGNATURES[0][1]), *EXPECTED_SIGNATURES[1:]]
    checks.extend((getattr(method_module, name), names) for name, names in METHOD_SIGNATURES.items())
    for function, names in checks:
        actual = tuple(inspect.signature(function).parameters)
        if actual[:len(names)] != names:
            raise RuntimeError(f"Incompatible interface: {function.__module__}.{function.__qualname__}{actual}; expected {names}.")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
def load_config(config_path):
    config_path = project_path(config_path)
    raw = read_json(config_path)
    for section in ("baseline", "eval"):
        if not isinstance(raw.get(section), dict):
            raise ValueError(f"Config section `{section}` must be an object.")
    parallelism = raw.get("parallelism_num", 1)
    if type(parallelism) is not int or parallelism <= 0:
        raise ValueError("parallelism_num must be a positive integer.")
    key_info_path = project_path(raw["key_information_path"])
    output_root = project_path(raw["output_root_dir"])
    diagnosis = dict(raw["baseline"])
    workflow = str(diagnosis.get("workflow_type", "")).lower()
    if workflow not in METHOD_MODULES:
        raise ValueError(f"Unsupported workflow_type `{workflow}`; known: {sorted(METHOD_MODULES)}.")
    method_module = importlib.import_module(METHOD_MODULES[workflow])
    driver = react if workflow == "codex" else importlib.import_module("Baselines.ReAct.react")
    check_signatures(method_module, driver)
    api_key_source = diagnosis.pop("api_key_source", None)
    retries = diagnosis.pop("sample_retry_times", 0)
    if type(retries) is not int or retries < 0:
        raise ValueError("baseline.sample_retry_times must be non-negative.")
    diagnosis.update(key_information_path=str(key_info_path), workflow_type=workflow, parallelism_num=parallelism,
                     llm_configuration=driver.diagnosis_llm_config(diagnosis.get("llm_configuration", {})))
    method_module.validate_config(diagnosis)
    driver.validate_config(diagnosis, True)
    inject_model_key(diagnosis["llm_configuration"], api_key_source)
    evaluation = dict(raw["eval"])
    from Evaluation.measurement import validate_measurement_runs
    median_runs = validate_measurement_runs(evaluation.get("median_runs", 3))
    eval_retries = evaluation.get("sample_retry_times", te.DEFAULT_SAMPLE_RETRY_TIMES)
    if type(eval_retries) is not int or eval_retries < 0:
        raise ValueError("eval.sample_retry_times must be non-negative.")
    shared = te.build_evaluation_shared(evaluation, base_dir=BASE_DIR)
    shared.update(anomaly_directory=str(key_info_path.parent), solution_script_directory=str(output_root),
                  output_base_dir=None, workflow_type=workflow, artifact_resolver=baseline_artifact_paths)
    ports = {"diagnosis": int(diagnosis.get("host_port", 5427)), "eval": int(evaluation.get("host_port", 5426))}
    if any(port < 1 or port + parallelism - 1 > 65535 for port in ports.values()):
        raise ValueError("Worker port ranges must be within 1..65535.")
    if abs(ports["diagnosis"] - ports["eval"]) < parallelism:
        raise ValueError("Diagnosis/evaluation worker port ranges overlap.")
    continuation_passes = raw.get("continuation_passes", 0)
    if type(continuation_passes) is not int or not 0 <= continuation_passes <= 3:
        raise ValueError("continuation_passes must be an integer between 0 and 3")
    if continuation_passes and not raw.get("native_output_root"):
        raise ValueError("continuation_passes requires explicit native_output_root")
    native_output_root = None
    if raw.get("native_output_root"):
        if workflow not in {"db_gpt", "dbaiops"}:
            raise ValueError("native_output_root is supported only for DB-GPT and DBAIOps")
        from Evaluation.recovery_reassessment import validate_roots
        native_output_root = validate_roots(project_path(raw["native_output_root"]), output_root)
        shared["native_output_root"] = str(native_output_root)
    diagnosis_prefix = str(diagnosis["docker_name"])
    eval_prefix = str(evaluation.get("docker_name", "evaluation"))
    if diagnosis_prefix == eval_prefix:
        raise ValueError("Diagnosis and evaluation require distinct container prefixes.")
    return {"_config_path": config_path, "_raw": raw, "key_information_path": key_info_path,
            "output_root": output_root, "native_output_root": native_output_root,
            "continuation_passes": continuation_passes, "parallelism_num": parallelism,
            "method_module": method_module, "driver": driver,
            "method_name": METHOD_DISPLAY_NAMES[workflow], "diagnosis": diagnosis,
            "diagnosis_retries": retries, "diagnosis_docker_name": diagnosis_prefix,
            "eval_docker_name": eval_prefix, "ports": ports, "eval_shared": shared,
            "eval_retries": eval_retries, "timeout_times": te.RECOVERY_ZERO_SCORE_RATIO, "median_runs": median_runs}


def inject_model_key(llm_configuration, source):
    """Export the method's model key to its env variable; never log or persist it."""
    env_name = llm_configuration.get("api_key_env")
    if llm_configuration.get("api_key") or not env_name or os.environ.get(env_name):
        return
    if not isinstance(source, dict) or not source.get("config_path"):
        raise ValueError(f"Set `{env_name}` or provide baseline.api_key_source.config_path.")
    source_path = project_path(source["config_path"])
    value = read_json(source_path).get(source.get("config_key", "llm_configuration"), {})
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
# Per-sample logging helpers
# --------------------------------------------------------------------------- #
def number(value, digits=6):
    return "N/A" if value is None else f"{float(value):.{digits}f}"


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
class Pipeline:

    def __init__(self, config, logger):
        self.config, self.logger = config, logger
        self.root = config["output_root"]
        self.method = config["method_module"]
        self.driver = config.get("driver", react)
        self.agent_runner = None if config["diagnosis"]["workflow_type"] == "react" else self.method.run_agent
        self.cases = list(self.driver.validate_input_data(config["diagnosis"]))
        self.anomaly_root = config["key_information_path"].parent
        self.stop_event = threading.Event()
        self.fatal_marker = self.root / "fatal_api_error.json"

    def run_diagnosis(self, index, case, output_dir, logger, base_docker_name, host_port):
        """Same preparation as react.run_case, but writing into `output_dir`."""
        react = self.driver
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
                react.request_llm, self.agent_runner, containers,
            )
        finally:
            for container_name in dict.fromkeys(containers):
                logger.info(f"[BASELINE][CLEANUP] Reclaiming container `{container_name}` and tmux session ...")
                react.kill_tmux_session(container_name)
                react.force_remove_container(container_name)

    def stage_baseline(self, index, case, sample_id, logger, base_docker_name, host_port):
        react = self.driver
        sample_root = self.root / sample_id
        output_dir = sample_root / "baseline"
        status_path = output_dir / "status.json"
        if self.config["diagnosis"]["workflow_type"] == "db_gpt":
            _, sample = react.find_sample_directory(self.anomaly_root, case)
            dbms = react.load_json_file(sample / "config.json").get("dbms", case.get("dbms"))
            if react.normalize_dbms(dbms) != "pg":
                status = {"status": "skipped_unsupported_dbms", "dbms": dbms}
                write_json_atomic(status_path, status)
                return status
        if self.config.get("native_output_root"):
            from Evaluation.recovery_reassessment import prepare_sample
            return prepare_sample(self, case, sample_id, logger)
        previous = read_json_if_file(status_path)
        if previous and previous.get("status") in react.TERMINAL_SAMPLE_STATUSES and not preparation_failure(previous):
            logger.info(f"[BASELINE][RESUME] Terminal status=`{previous['status']}` exists; skipping diagnosis.")
            return previous
        attempts = self.config["diagnosis_retries"] + 1
        for attempt in range(1, attempts + 1):
            if self.stop_event.is_set() or self.fatal_marker.exists():
                raise RuntimeError("Fatal API marker exists; diagnosis not started.")
            # Every attempt needs fresh private agent state.
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
                from Baselines.container_boundary import VERSION as boundary_version
                status = {"status": "completed", "attempt": attempt,
                          "boundary_version": boundary_version, "diagnosis_policy": "database_only"}
            except __import__("Baselines.container_boundary", fromlist=["BoundaryError"]).BoundaryError as exc:
                write_json_atomic(status_path, {"status": "blocked_isolation", "error": one_line(exc)})
                raise
            except FatalAPIQuotaError as exc:
                write_json_atomic(status_path, {"status": "interrupted_fatal_api", "error": one_line(exc)})
                raise
            except PreparationError as exc:
                status = {"status": "preparation_failed", "attempt": attempt,
                          "failure_phase": exc.phase, "retryable": True, "error": one_line(exc)}
                logger.error("[BASELINE][PREPARATION_FAILED] %s: %s", exc.phase, status["error"])
            except Exception as exc:  # noqa: BLE001 - a failed diagnosis is still scored.
                status = {"status": "failed", "attempt": attempt, "error": one_line(exc)}
                logger.error(f"[BASELINE][ERROR] Diagnosis attempt `{attempt}/{attempts}` failed: {status['error']}")
            write_json_atomic(status_path, status)
            if status["status"] == "completed" or attempt == attempts:
                logger.info(f"[BASELINE][SAMPLE {index}] Diagnosis status=`{status['status']}`; output `{output_dir}`.")
                return status
            logger.warning(f"[BASELINE][RETRY] Retrying diagnosis attempt `{attempt + 1}/{attempts}`.")

    def terminal_eval_detail(self, sample_id, output_dir, logger):
        from Evaluation.measurement import protocol_matches
        if preparation_failure(read_json_if_file(self.root / sample_id / "baseline/status.json")):
            return None
        # The score's sample ID is the dataset ID, not the final directory name.
        detail = read_json_if_file(output_dir / SCORE_DETAIL)
        if (not isinstance(detail, dict) or detail.get("scoring_version") != te.SCORING_VERSION
                or detail.get("score_channel") != te.SCORE_CHANNEL or detail.get("mode") != "both"
                or str(detail.get("sample_id")) != str(sample_id)
                or detail.get("status") not in te.TERMINAL_EVALUATION_STATUSES):
            return None
        if detail["status"] == "completed" and (detail.get("score") is None
                or detail.get("semantic_judge", {}).get("score") is None):
            return None
        if not newer(output_dir / SCORE_DETAIL, self.root / sample_id / "baseline/status.json"):
            return None
        if not protocol_matches(detail, self.config["eval_shared"], self.config["median_runs"], te.protocol_digest):
            return None
        case = next((c for c in self.cases if te.anomaly_sample_id(c) == str(sample_id)), None)
        if case is None:
            return None
        _, sample = self.driver.find_sample_directory(self.anomaly_root, case)
        if detail.get("input_digest") != te.input_digest(
                sample, self.root / sample_id / "baseline", self.config["eval_shared"]["ground_truth_diagnosis"]):
            return None
        sample_dbms = self.driver.load_json_file(sample / "config.json").get("dbms", case.get("dbms"))
        unsupported = (self.config["diagnosis"]["workflow_type"] == "db_gpt"
                       and self.driver.normalize_dbms(sample_dbms) != "pg")
        if ((detail["status"] == "not_evaluated") != unsupported
                or (unsupported and detail.get("reason_code") != "unsupported_dbms")):
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
        shared = dict(self.config["eval_shared"], output_base_dir=str(stage_root), artifact_resolver=baseline_artifact_paths)
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
        if self.config.get("native_output_root"):
            from Evaluation.recovery_reassessment import VERSION as reassessment_version
            write_json_atomic(output_dir / "reassessment_provenance.json", {
                "version": reassessment_version, "scoring_version": te.SCORING_VERSION,
                "score_channel": te.SCORE_CHANNEL, "fresh_environment": True,
                "native_reports_not_regenerated": True,
            })
        return read_json(output_dir / SCORE_DETAIL)

    def log_scores(self, index, sample_id, logger, eval_detail, workload_detail=None):
        logger.info("[EVAL][SAMPLE %s][%s] score=%s diagnosis=%s recovery=%s semantic=%s",
                    index, te.SCORE_CHANNEL, eval_detail.get("score"), eval_detail.get("diagnosis_score"),
                    eval_detail.get("recovery_score"), eval_detail.get("workload_semantic_consistency_score"))

    def process_sample(self, index, case, slot):
        sample_id = te.anomaly_sample_id(case)
        logger = te._sample_logger(self.logger, sample_id)
        diagnosis_prefix = f"{self.config['diagnosis_docker_name']}_w{slot}"
        eval_prefix = f"{self.config['eval_docker_name']}_w{slot}"
        summary = {"sample_id": sample_id, "index": index}
        try:
            status = self.stage_baseline(index, case, sample_id, logger, diagnosis_prefix,
                                         self.config["ports"]["diagnosis"] + slot)
            summary["baseline_status"] = status["status"]
            if preparation_failure(status):
                summary.update(preparation_failed=True, eval_status="not_evaluated_preparation_failed")
                raise PreparationError(status.get("failure_phase", "preparation"), status.get("error", "Preparation failed"))
            detail = self.stage_eval(index, case, sample_id, logger, diagnosis_prefix, eval_prefix,
                                     self.config["ports"]["eval"] + slot)
            summary.update(eval_status=detail["status"], is_not_evaluated=detail["status"] == "not_evaluated",
                           evaluation=detail)
            if detail["status"] not in te.TERMINAL_EVALUATION_STATUSES:
                summary["error"] = f"Evaluation status {detail['status']}: {detail.get('error', '')}"
            self.log_scores(index, sample_id, logger, detail)
        except FatalAPIQuotaError as exc:
            self.stop_event.set()
            write_json_atomic(self.fatal_marker, {"status": "fatal_api_error", "sample_id": sample_id, "error": one_line(exc)})
            summary["error"] = one_line(exc)
        except Exception as exc:
            summary["error"] = one_line(exc)
            logger.error("[PIPELINE] Sample failed: %s", exc)
        finally:
            for prefix in (diagnosis_prefix, eval_prefix):
                try:
                    te.cleanup_evaluation_prefix(prefix)
                except Exception as exc:
                    logger.warning("[PIPELINE] Cleanup failed for %s: %s", prefix, exc)
        return summary

    def run(self, selected_ids=None):
        if self.fatal_marker.exists():
            raise RuntimeError(f"{self.fatal_marker} exists; resolve the API error before resuming.")
        cases = [(i, case) for i, case in enumerate(self.cases)
                 if not selected_ids or te.anomaly_sample_id(case) in selected_ids]
        slots = queue.Queue()
        for slot in range(self.config["parallelism_num"]):
            slots.put(slot)
        def worker(index, case):
            slot = slots.get()
            try:
                if self.stop_event.is_set():
                    return {"sample_id": te.anomaly_sample_id(case), "index": index,
                            "error": "Not started after fatal API error"}
                return self.process_sample(index, case, slot)
            finally:
                slots.put(slot)
        with ThreadPoolExecutor(max_workers=self.config["parallelism_num"]) as pool:
            summaries = list(pool.map(lambda row: worker(*row), cases))
        result = self.aggregate(summaries)
        write_json_atomic(self.root / "evaluation_result.json", result)
        self.logger.info("[PIPELINE][%s] overall score=%s, failed=%s/%s", te.SCORE_CHANNEL,
                         result["overall_score"], result["failed_sample_num"], len(summaries))
        if self.stop_event.is_set():
            raise SystemExit(42)
        if result["failed_sample_num"]:
            raise SystemExit(1)
        return result

    def aggregate(self, summaries):
        details = []
        for item in summaries:
            if item.get("preparation_failed"):
                continue
            detail = item.get("evaluation") or {
                "sample_id": item["sample_id"], "status": "aborted", "error": item.get("error")}
            details.append(detail)
        result = te.aggregate_scores(details)
        result.update(method=self.config["method_name"], requested_sample_num=len(summaries),
                      preparation_failed_sample_num=sum(bool(s.get("preparation_failed")) for s in summaries),
                      failed_sample_num=sum(bool(s.get("error")) for s in summaries),
                      measurement_median_runs=self.config["median_runs"], sample_summaries=summaries)
        result.pop("sample_details")
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run a diagnosis method and reconstructed semantic-Judge evaluation.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--method", choices=sorted(METHOD_MODULES), help="Require this method to match the configuration.")
    parser.add_argument("--sample-ids", nargs="+")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Validate config, samples and call interfaces only.")
    mode.add_argument("--preflight", action="store_true", help="Check runtime prerequisites without evaluating samples.")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    if args.method and args.method != config["diagnosis"]["workflow_type"]:
        raise ValueError("--method does not match baseline.workflow_type.")
    if args.dry_run:
        cases = list(config["driver"].validate_input_data(config["diagnosis"]))
        print(f"[DRY RUN] Method `{config['method_name']}` validated; {len(cases)} sample(s).")
        print(f"[DRY RUN] Scoring channel: {te.SCORE_CHANNEL}; output: {config['output_root']}")
        return
    from Evaluation.namespace import preflight as measurement_preflight
    # Report reassessment does not initialize native models, embeddings or KBs.
    method_check = ({"mode": "native_report_reassessment", "native_runtime_required": False}
                    if config.get("native_output_root") else config["method_module"].preflight(config["diagnosis"]))
    check = {"method": method_check, "measurement": measurement_preflight()}
    if args.preflight:
        print(json.dumps(check, indent=2, default=str))
        return
    logger = setup_logger(config["output_root"])
    write_json_atomic(config["output_root"] / "config.json", redact(config["_raw"]))
    pipeline = Pipeline(config, logger)
    if config.get("native_output_root"):
        from Evaluation.recovery_reassessment import run_reassessment
        run_reassessment(pipeline, set(args.sample_ids or []))
    else:
        pipeline.run(set(args.sample_ids or []))


if __name__ == "__main__":
    main()
