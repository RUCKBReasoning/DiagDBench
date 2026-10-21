import json
import os
import time
import re
import hashlib
import shlex
import logging
import statistics
import math
import multiprocessing
import threading
import subprocess
import shutil
import queue as queue_module
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import argparse

from utils import *
from docker_func import *

# Docker-container database-initialization timeouts (seconds), kept in lockstep
# with `test_syn_ano.py`'s TIMEOUTS so the reproduction / synthesis and the
# evaluation share the SAME per-database initialization budget. An under-sized
# budget was a source of spurious "database initialization timeout" aborts, so
# the value here MUST match the data-synthesis framework.
TIMEOUTS = {'tpch_1' : 1500, 'imdb' : 4500, 'tpcds_1' : 1800, 'tpch_10' : 11800}
DEFAULT_DB_INIT_TIMEOUT = 3600
RETRY_TIMES = 10

# Timeout (seconds) for applying the sample's data-modification SQL inside the
# container; mirrors `test_syn_ano.run_data_modification_step`.
DATA_MODIFICATION_TIMEOUT = 600
# Fallback timeout when a script does not declare `# TIMEOUT THRESHOLD: <n>`.
DEFAULT_SCRIPT_TIMEOUT = 600

# Floating-point rounding precision used when normalizing result values so that
# trivial numeric noise (e.g. 1.0000001 vs 1.0) does not break equivalence checks.
RESULT_FLOAT_PRECISION = 6
RESULT_COMPARISON_MODES = frozenset({"strict", "common"})
DEFAULT_RESULT_COMPARISON_MODE = "common"
DEFAULT_GROUND_TRUTH_DIAGNOSIS = "anomaly_description.json"
DEFAULT_JUDGE_REPETITIONS = 3
RECOVERY_ZERO_SCORE_RATIO = 8.0
RECOVERY_FULL_SCORE_RATIO = 0.2
RECOVERY_WATCHDOG_RATIO = 10.0
RECOVERY_MIN_TIMEOUT_SECONDS = 600.0
DEFAULT_INITIALIZATION_WORKLOAD_TIMEOUT_SECONDS = 3600.0
DEFAULT_BASELINE_WORKLOAD_TIMEOUT_SECONDS = 3600
DEFAULT_SAMPLE_RETRY_TIMES = 2
DEFAULT_SUPERUSER_CONFIG = {
    "DB_USER": "root",
    "DB_PASSWORD": "root",
}
SCORE_PRECISION = 6
DIAGNOSIS_SCORE_WEIGHT = 0.30
RECOVERY_SCORE_WEIGHT = 0.70

# Sentinel sample id used for run-level / aggregate log lines that are not tied
# to any single sample (e.g. the overall start banner and the final summary).
RUN_LEVEL_SAMPLE_ID = "ALL"


class _SampleLogAdapter(logging.LoggerAdapter):
    """Prefix every log record with the current sample's id.

    Evaluation runs many samples concurrently (see `parallelism_num`), so their
    log lines interleave freely in the shared log file. Wrapping the logger in
    this adapter guarantees that EVERY line emitted while evaluating a sample --
    including lines produced by the helper functions it calls -- begins with
    `[Sample ID: <id>]`, making each line unambiguously attributable to one
    sample when inspecting the results.
    """

    def process(self, msg, kwargs) :
        return f"[Sample ID: {self.extra['sample_id']}] {msg}", kwargs


def _sample_logger(logger, sample_id) :
    """Return a logger that prefixes every line with `[Sample ID: <sample_id>]`.

    Unwraps an already-adapted logger first so re-scoping (run-level -> sample)
    never double-prefixes a line.
    """
    base = logger.logger if isinstance(logger, logging.LoggerAdapter) else logger
    return _SampleLogAdapter(base, {"sample_id": sample_id})


def anomaly_sample_id(anomaly_pair):
    return str(anomaly_pair.get('sample_id', anomaly_pair.get('anomaly_id')))


def find_anomaly_sample_directory(anomaly_directory, anomaly_pair):
    sample_id = anomaly_sample_id(anomaly_pair)
    database_name = str(anomaly_pair.get('database_name', ""))
    candidates = [
        os.path.join(anomaly_directory, sample_id),
        os.path.join(anomaly_directory, f"{sample_id}_{database_name}"),
    ]
    candidates.extend(
        os.path.join(anomaly_directory, name)
        for name in sorted(os.listdir(anomaly_directory))
        if name.startswith(f"{sample_id}_")
    )
    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate
    raise FileNotFoundError(f"Cannot find sample directory for sample `{sample_id}` under {anomaly_directory}.")


def load_sample_config(sample_directory):
    """Read the DBMS / hardware / database-user settings from the sample's own
    `config.json`.

    These settings describe the environment each sample was synthesized in and
    are the single source of truth for the evaluation container. There is no
    built-in default: if the sample directory has no `config.json`, or the file
    omits any of the required fields, we abort the evaluation for that sample
    rather than silently guessing an environment that may not match the sample.
    """
    config_path = os.path.join(sample_directory, "config.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Sample `config.json` not found under {sample_directory}. "
            "DBMS / hardware / database-user settings must be provided per-sample."
        )
    with open(config_path, "r", encoding="utf-8") as file:
        sample_config = json.load(file)

    required_fields = ("dbms", "database_name", "system_hardware_config", "system_user_config")
    missing = [field for field in required_fields if not sample_config.get(field)]
    if missing:
        raise ValueError(
            f"Sample `config.json` at {config_path} is missing required field(s): {missing}."
        )

    dbms = sample_config["dbms"]
    database_name = sample_config["database_name"]
    database_schema_name = sample_config.get("database_schema_name", "diagnosis")
    if normalize_dbms(dbms) == "mysql":
        database_schema_name = database_name

    system_user_config = dict(sample_config["system_user_config"])
    system_user_config["DB_NAME"] = database_name
    system_hardware_config = dict(sample_config["system_hardware_config"])

    return {
        "dbms": dbms,
        "database_name": database_name,
        "database_schema_name": database_schema_name,
        "system_user_config": system_user_config,
        "system_hardware_config": system_hardware_config,
        "db_type": resolve_db_type(sample_config, dbms),
    }


def evaluation_mode(config):
    mode = config.get("mode", "both")
    if mode not in ("both", "report", "solution"):
        raise ValueError("mode must be both, report or solution.")
    return mode


def resolve_judge_llm_configuration(config):
    if "judge_llm_configuration" not in config:
        raise ValueError(
            "Evaluation config must explicitly provide the required "
            "`judge_llm_configuration` field."
        )
    judge_config = config["judge_llm_configuration"]
    if not isinstance(judge_config, dict) or not judge_config:
        raise ValueError(
            "judge_llm_configuration must be a non-empty JSON object."
        )
    if "config_path" in judge_config or "config_key" in judge_config:
        raise ValueError(
            "judge_llm_configuration must inline the complete LLM "
            "configuration; `config_path`/`config_key` references are not allowed."
        )

    required_fields = (
        "base_url",
        "api_key",
        "model_name",
        "temperature",
        "retry_times",
    )
    missing = [
        field for field in required_fields
        if field not in judge_config
        or judge_config[field] is None
        or (
            isinstance(judge_config[field], str)
            and not judge_config[field].strip()
        )
    ]
    if missing:
        raise ValueError(
            "judge_llm_configuration is missing required field(s): "
            f"{missing}."
        )
    if (
        isinstance(judge_config["temperature"], bool)
        or not isinstance(judge_config["temperature"], (int, float))
    ):
        raise ValueError(
            "judge_llm_configuration.temperature must be numeric."
        )
    if (
        isinstance(judge_config["retry_times"], bool)
        or not isinstance(judge_config["retry_times"], int)
        or judge_config["retry_times"] <= 0
    ):
        raise ValueError(
            "judge_llm_configuration.retry_times must be a positive integer."
        )
    return dict(judge_config)


def resolve_solution_artifact_paths(solution_script_directory, sample_id, case_index):
    candidate_dirs = [
        os.path.join(solution_script_directory, str(sample_id)),
        os.path.join(solution_script_directory, str(case_index)),
    ]
    for directory in candidate_dirs:
        solution_script = os.path.join(directory, "solution_script.sh")
        optimized_workload = os.path.join(directory, "optimized_verified_workload.sql")
        diagnosis_report = os.path.join(directory, "diagnosis_report.md")
        if os.path.exists(solution_script) or os.path.exists(optimized_workload) or os.path.exists(diagnosis_report):
            return {
                "directory": directory,
                "solution_script": solution_script,
                "optimized_workload": optimized_workload,
                "diagnosis_report": diagnosis_report,
            }
    return {
        "directory": candidate_dirs[0],
        "solution_script": os.path.join(candidate_dirs[0], "solution_script.sh"),
        "optimized_workload": os.path.join(candidate_dirs[0], "optimized_verified_workload.sql"),
        "diagnosis_report": os.path.join(candidate_dirs[0], "diagnosis_report.md"),
    }


def parse_timeout_threshold(script_text, default_seconds=DEFAULT_SCRIPT_TIMEOUT):
    match = re.search(r"#\s*TIMEOUT THRESHOLD:\s*(\d+)", script_text or "")
    if not match:
        return default_seconds
    return int(match.group(1))

def parse_injection_ready_pattern(script_text):
    """Return an opt-in readiness regex without changing legacy scripts."""
    directive = re.search(
        r"(?mi)^\s*#\s*EVALUATION READY PATTERN:\s*(.+?)\s*$",
        script_text or "",
    )
    if directive:
        pattern = directive.group(1)
        re.compile(pattern)
        return pattern
    if re.search(r"(?i)anomaly injection active", script_text or ""):
        return re.escape("Anomaly injection active")
    return None


def round_score(value):
    return None if value is None else round(float(value), SCORE_PRECISION)


def build_script_command(script_path, db_config, dbms):
    """Wrap an in-container shell script (anomaly injection / solution script) so
    it runs with the same clean database-client environment that
    `anomaly_eval.build_db_env` provides: PG* / MYSQL_PWD / DB_* variables and,
    for PostgreSQL, a search_path pointing at the sample schema.

    Both the injection and the solution scripts reference these variables
    (`$PGHOST`, `${DB_USER:-$PGUSER}`, ...), so they MUST be exported before the
    script runs; otherwise the client cannot connect.
    """
    dbms_kind = normalize_dbms(dbms)
    exports = {
        "PGHOST": db_config["host"],
        "PGPORT": db_config["port"],
        "PGUSER": db_config["user"],
        "PGPASSWORD": db_config["password"],
        "PGDATABASE": db_config["db_name"],
        "MYSQL_PWD": db_config["password"],
        "DB_HOST": db_config["host"],
        "DB_PORT": db_config["port"],
        "DB_USER": db_config["user"],
        "DB_NAME": db_config["db_name"],
        "DB_SCHEMA": db_config["schema"],
        "DB_PASSWORD": db_config["password"],
        "DBMS": dbms,
    }
    if dbms_kind != "mysql":
        exports["PGOPTIONS"] = f"-c search_path={db_config['schema']}"
    export_str = " ".join(f"{key}={shlex.quote(str(value))}" for key, value in exports.items())
    inner_command = f"export {export_str}; bash {shlex.quote(str(script_path))}"
    return ["bash", "-lc", inner_command]


def build_recovery_db_config(db_config, superuser_config=None):
    """Use DB superuser credentials without changing connection routing."""
    credentials = dict(DEFAULT_SUPERUSER_CONFIG)
    credentials.update(superuser_config or {})
    user = credentials.get("DB_USER")
    password = credentials.get("DB_PASSWORD")
    if not isinstance(user, str) or not user:
        raise ValueError("superuser_config.DB_USER must be a non-empty string.")
    if not isinstance(password, str) or not password:
        raise ValueError("superuser_config.DB_PASSWORD must be a non-empty string.")

    recovery_config = dict(db_config)
    recovery_config["user"] = user
    recovery_config["password"] = password
    return recovery_config


def cleanup_evaluation_container(container_name):
    """Attempt both cleanup operations even if tmux fails; never assume removal."""
    try:
        kill_tmux_session(container_name)
    finally:
        if not force_remove_container(container_name):
            raise RuntimeError(f"Container cleanup not confirmed: {container_name}")


def cleanup_evaluation_prefix(base_name):
    """Strict discovery: a failed Docker listing must not mean 'no containers'."""
    result = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        capture_output=True, text=True, check=True, timeout=60,
    )
    names = [
        name for name in result.stdout.splitlines()
        if name == base_name or name.startswith(base_name + "_")
    ]
    for name in names:
        cleanup_evaluation_container(name)
    kill_tmux_sessions_by_prefix(base_name)


def initialize_sample_environment(base_docker_name, container_config, init_db_config, dbms,
                                  system_user_config, data_modification_path, database_name,
                                  output_dir, logger, stage_label, previous_docker_name=None):
    """Bring the evaluation container to the clean, pre-injection baseline state
    and return the FINAL (unique) container name that was created.

    This single entry point encapsulates the whole environment-preparation
    sequence so callers do not have to repeat it: it (1) creates a fresh Docker
    container under a UNIQUE name, (2) initializes the database from its
    dump / load-data source, and (3) applies the sample's data-modification SQL.

    Container names are never reused: `make_unique_container_name` appends a
    microsecond timestamp to `base_docker_name`, and each retry moves to a brand
    new name via `recreate_container_with_new_name` (mirroring `test_syn_ano.py`).
    This is what makes it safe to (a) initialize the SAME sample twice (baseline
    then pre-injection) without a name clash, and (b) run many samples in parallel
    -- distinct `base_docker_name` prefixes plus unique timestamps guarantee no
    two live containers ever share a name. When `previous_docker_name` is given
    (the second initialization of a sample) it is torn down as part of creating
    the new container, so no orphan container is leaked.

    Every filesystem path handed to `docker exec` here (the data-modification
    SQL, the init-db script's base_dir, ...) MUST be canonical (symlink
    resolved), because the container bind-mounts the canonical host path;
    passing a symlinked alias makes the file invisible inside the container.
    Raises RuntimeError on any unrecoverable failure.
    """
    # Register ownership BEFORE docker run, so even a partially-created container
    # is recoverable when creation, DB loading or data modification raises.
    owned_names = set()
    if previous_docker_name is not None:
        owned_names.add(previous_docker_name)
    transferred = False
    docker_name = None
    try:
        if previous_docker_name is not None:
            cleanup_evaluation_container(previous_docker_name)
            owned_names.remove(previous_docker_name)
        for db_attempt in range(RETRY_TIMES):
            for attempt in range(RETRY_TIMES):
                docker_name = make_unique_container_name(base_docker_name)
                owned_names.add(docker_name)
                logger.info(f"[EVAL][{stage_label}] Initializing Docker Container `{docker_name}` ...")
                if docker_container_init(docker_name, container_config) is True:
                    break
                cleanup_evaluation_container(docker_name)
                owned_names.remove(docker_name)
                if attempt + 1 < RETRY_TIMES:
                    time.sleep(5)
            else:
                raise RuntimeError(f"Container initialization failed for {base_docker_name}.")

            logger.info(f"[EVAL][{stage_label}] Initializing Database `{database_name}` in Docker Container `{docker_name}`...")
            if docker_container_init_db(docker_name, init_db_config, session_name=docker_name) is True:
                logger.info(f"[EVAL][{stage_label}] Database `{database_name}` Initialization Succeeded.")
                break
            cleanup_evaluation_container(docker_name)
            owned_names.remove(docker_name)
            if db_attempt + 1 < RETRY_TIMES:
                time.sleep(5)
        else:
            raise RuntimeError(f"Database initialization failed for {database_name}.")

        logger.info(f"[EVAL][{stage_label}] Applying Data Modification If Needed...")
        if data_modification_path is None or not os.path.exists(data_modification_path):
            raise RuntimeError(f"Data modification SQL not found: {data_modification_path}")
        cmd = build_sql_file_command(dbms, system_user_config, data_modification_path, database_name)
        obser_log = os.path.join(output_dir, "data_modification_log.txt")
        state, obser_output = run_docker_exec_observation(
            docker_name, cmd, obser_log, DATA_MODIFICATION_TIMEOUT, session_name=docker_name
        )
        if state is False:
            raise RuntimeError(f"Data modification failed. See log: {obser_log}\n{obser_output}")
        logger.info(f"[EVAL][{stage_label}] Data Modification Succeeded. Log: {obser_log}")
        transferred = True
        return docker_name
    finally:
        if not transferred:
            cleanup_errors = []
            for name in sorted(owned_names):
                try:
                    cleanup_evaluation_container(name)
                except Exception as exc:
                    cleanup_errors.append(f"{name}: {exc}")
            if cleanup_errors:
                raise RuntimeError("Initialization cleanup failed: " + "; ".join(cleanup_errors))


DIAGNOSIS_JUDGE_DIMENSIONS = {
    "root_cause_accuracy": 0.45,
    "evidence_consistency": 0.25,
    "remediation_effectiveness": 0.20,
    "operational_safety": 0.10,
}


def _read_text_if_exists(path):
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as file:
        return file.read().strip()


def _bounded_score(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Judge field `{label}` must be numeric.")
    value = float(value)
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"Judge field `{label}` must be within [0, 1].")
    return value


def build_diagnosis_judge_prompt(sample_directory, ground_truth_path, diagnosis_report_path):
    anomaly_injection_path = os.path.join(
        sample_directory, "anomaly_inject.sh"
    )
    evidence_files = {
        "user_phenomenon_description.txt": os.path.join(
            sample_directory, "user_phenomenon_description.txt"
        ),
        "initialization_workload.sql": os.path.join(
            sample_directory, "initialization_workload.sql"
        ),
        "verified_workload.sql": os.path.join(
            sample_directory, "verified_workload.sql"
        ),
        "data_modification.sql": os.path.join(
            sample_directory, "data_modification.sql"
        ),
    }
    evidence_sections = []
    for name, path in evidence_files.items():
        evidence_sections.append(
            f"\n===== SAMPLE INPUT: {name} =====\n{_read_text_if_exists(path)}"
        )

    ground_truth = _read_text_if_exists(ground_truth_path)
    anomaly_injection = _read_text_if_exists(anomaly_injection_path)
    diagnosis_report = _read_text_if_exists(diagnosis_report_path)
    return f"""
You are an impartial database-diagnosis evaluation judge. Score the candidate
diagnosis report strictly against the supplied ground-truth diagnosis and the
original anomaly-sample inputs.

Mandatory evaluation rules:
1. Score fairly and impartially. Every score and reason must be justified with
   concrete evidence by explicitly mapping the candidate diagnosis report to the
   supplied anomaly scenario, ground-truth definition and injection mechanism.
   Apply the same standard regardless of report style, verbosity or method name.
2. Treat `{os.path.basename(ground_truth_path)}` as the conceptual reference
   answer and `anomaly_inject.sh` as the authoritative operational reference for
   how the anomaly was actually constructed. Verify every claimed symptom,
   causal mechanism, root cause and remediation against both references. Do not
   reward merely using similar database terminology.
3. Inspect the concrete commands, SQL, configuration changes, processes,
   transactions, locks and resource pressure created by `anomaly_inject.sh`.
   Reward a report only when it correctly connects those injected actions to the
   diagnosed database behavior. Penalize diagnoses or remedies that contradict
   or ignore the actual injection method.
4. The candidate report may reason only from the supplied reference material and
   sample inputs. Root-cause deductions must have an explicit evidence chain.
   Penalize fabricated metrics, sessions, timestamps, objects, causal links or
   runtime observations that are not supported by these inputs or the references.
5. Distinguish a confirmed root cause from a hypothesis. Penalize unjustified
   certainty and remedies that do not address the injected anomaly mechanism.
6. For operational safety, assume the requesting user and the database diagnosis
   agent are authorized DBAs who may directly handle the user-related processes
   and transactions involved in this sample. Do not penalize an action merely
   because it cancels/terminates an involved process or commits/rolls back an
   involved transaction. Still evaluate target validation, scope control,
   unintended data loss, reversibility and whether the action addresses the
   correct process or transaction.
7. Judge report quality, not report length or writing style. Use scores in [0, 1].

Dimensions and weights:
- root_cause_accuracy (0.45): identifies the concrete injected root cause and
  explains the correct causal chain.
- evidence_consistency (0.25): all material claims are grounded in the reference
  and sample inputs; no fabrication or contradiction.
- remediation_effectiveness (0.20): actions directly recover the anomaly and are
  technically executable and relevant.
- operational_safety (0.10): actions are scoped, validated and avoid unrelated or
  destructive side effects under the DBA-authorization assumption above.

Return exactly one JSON object and no Markdown fences:
{{
  "root_cause_accuracy": {{"score": 0.0, "reason": "..."}},
  "evidence_consistency": {{"score": 0.0, "reason": "..."}},
  "remediation_effectiveness": {{"score": 0.0, "reason": "..."}},
  "operational_safety": {{"score": 0.0, "reason": "..."}},
  "summary": "..."
}}

===== GROUND TRUTH: {os.path.basename(ground_truth_path)} =====
{ground_truth}

===== AUTHORITATIVE ANOMALY INJECTION SCRIPT: anomaly_inject.sh =====
{anomaly_injection}
{''.join(evidence_sections)}

===== CANDIDATE DIAGNOSIS REPORT =====
{diagnosis_report}
""".strip()


def compute_diagnosis_artifact_score(
    sample_directory,
    diagnosis_report_path,
    ground_truth_filename,
    judge_llm_config,
    logger,
    repetitions=DEFAULT_JUDGE_REPETITIONS,
):
    if not os.path.exists(diagnosis_report_path):
        return {
            "score": 0.0,
            "status": "missing_diagnosis_report",
            "ground_truth_filename": ground_truth_filename,
            "dimension_scores": {
                key: 0.0 for key in DIAGNOSIS_JUDGE_DIMENSIONS
            },
            "judgements": [],
        }

    if os.path.basename(ground_truth_filename) != ground_truth_filename:
        raise ValueError("ground_truth_diagnosis must be a filename, not a path.")
    ground_truth_path = os.path.join(sample_directory, ground_truth_filename)
    if not os.path.exists(ground_truth_path):
        raise FileNotFoundError(
            f"Ground-truth diagnosis not found: {ground_truth_path}"
        )
    if not judge_llm_config:
        raise ValueError(
            "Evaluation config must provide the required "
            "`judge_llm_configuration` for LLM-as-a-Judge scoring."
        )

    from llm_api import get_llm_response

    prompt = build_diagnosis_judge_prompt(
        sample_directory, ground_truth_path, diagnosis_report_path
    )
    llm_config = dict(judge_llm_config)
    llm_config["temperature"] = 0
    llm_config.setdefault("max_output_tokens", 4096)
    llm_config.setdefault("stream", False)

    repetitions = max(1, int(repetitions))
    judgements = []
    failures = []
    max_attempts = repetitions * 2
    for attempt in range(1, max_attempts + 1):
        if len(judgements) >= repetitions:
            break
        try:
            logger.info(
                f"[EVAL][DIAGNOSIS JUDGE] Requesting judgement "
                f"`{len(judgements) + 1}/{repetitions}` (attempt `{attempt}/{max_attempts}`) ..."
            )
            response = get_llm_response([prompt], llm_config)
            parsed = json.loads(extract_json_object(response))
            dimension_scores = {}
            reasons = {}
            for dimension, weight in DIAGNOSIS_JUDGE_DIMENSIONS.items():
                dimension_value = parsed.get(dimension)
                if not isinstance(dimension_value, dict):
                    raise ValueError(
                        f"Judge response field `{dimension}` must be an object."
                    )
                dimension_scores[dimension] = _bounded_score(
                    dimension_value.get("score"), f"{dimension}.score"
                )
                dimension_scores[dimension] = round_score(
                    dimension_scores[dimension]
                )
                reasons[dimension] = str(dimension_value.get("reason", "")).strip()
            weighted_score = round_score(sum(
                dimension_scores[name] * weight
                for name, weight in DIAGNOSIS_JUDGE_DIMENSIONS.items()
            ))
            judgements.append({
                "attempt": attempt,
                "score": weighted_score,
                "dimension_scores": dimension_scores,
                "reasons": reasons,
                "summary": str(parsed.get("summary", "")).strip(),
                "raw_response": response,
            })
        except Exception as exc:
            failures.append({"attempt": attempt, "error": str(exc)})
            logger.warning(
                f"[EVAL][DIAGNOSIS JUDGE][WARNING] Invalid judgement attempt "
                f"`{attempt}/{max_attempts}`: {exc}"
            )

    if len(judgements) < repetitions:
        raise RuntimeError(
            f"Only {len(judgements)} valid diagnosis judgements were obtained; "
            f"{repetitions} required. Failures: {failures}"
        )

    dimension_medians = {
        dimension: round_score(statistics.median(
            item["dimension_scores"][dimension] for item in judgements
        ))
        for dimension in DIAGNOSIS_JUDGE_DIMENSIONS
    }
    score = round_score(statistics.median(item["score"] for item in judgements))
    return {
        "score": score,
        "status": "scored",
        "ground_truth_filename": ground_truth_filename,
        "ground_truth_path": ground_truth_path,
        "dimension_scores": dimension_medians,
        "judgements": judgements,
        "failed_attempts": failures,
    }


def compute_recovery_effect_score(
    functional_gate,
    original_time,
    recovered_time,
    baseline_times,
    timeout_times,
    failure_reasons=None,
):
    zero_score_ratio = float(timeout_times)
    if not math.isfinite(zero_score_ratio) or zero_score_ratio != RECOVERY_ZERO_SCORE_RATIO:
        raise ValueError(
            f"timeout_times must be exactly {RECOVERY_ZERO_SCORE_RATIO:g}; "
            "it is the fixed recovery-score zero boundary."
        )
    execution_time_ratio = None
    if (
        original_time is not None
        and original_time > 0
        and recovered_time is not None
        and recovered_time >= 0
    ):
        execution_time_ratio = recovered_time / original_time

    zero_reasons = list(failure_reasons or [])
    if not functional_gate and not zero_reasons:
        zero_reasons.append("functional_gate_failed")
    if original_time is None or original_time <= 0:
        zero_reasons.append("invalid_baseline_time")
    if recovered_time is None or recovered_time < 0:
        zero_reasons.append("recovered_time_unavailable")
    if (
        execution_time_ratio is not None
        and execution_time_ratio >= zero_score_ratio
    ):
        zero_reasons.append("execution_ratio_at_or_above_8x")

    detail = {
        "functional_gate": bool(functional_gate),
        "execution_time_ratio": execution_time_ratio,
        "score_band": "functional_failure",
        "zero_score_ratio": float(timeout_times),
        "full_score_ratio": RECOVERY_FULL_SCORE_RATIO,
        "zero_reasons": list(dict.fromkeys(zero_reasons)),
    }
    if detail["zero_reasons"]:
        if functional_gate and execution_time_ratio is not None:
            detail["score_band"] = "at_or_above_8x"
        detail["recovery_score"] = 0.0
        return 0.0, detail

    if execution_time_ratio >= 1.0:
        recovery_score = 0.8 - (
            (execution_time_ratio - 1.0) * 0.7
            / (zero_score_ratio - 1.0)
        )
        score_band = "1x_to_8x"
    elif execution_time_ratio >= RECOVERY_FULL_SCORE_RATIO:
        recovery_score = 0.8 + (
            (1.0 - execution_time_ratio) * 0.16
            / (1.0 - RECOVERY_FULL_SCORE_RATIO)
        )
        score_band = "0.2x_to_1x"
    else:
        recovery_score = 1.0
        score_band = "below_0.2x"
    detail.update({
        "execution_time_ratio": execution_time_ratio,
        "score_band": score_band,
        "recovery_score": round_score(recovery_score),
    })
    return round_score(min(1.0, max(0.0, recovery_score))), detail


def compute_sample_score(
    diagnosis_judge_detail,
    functional_gate,
    original_time,
    recovered_time,
    baseline_times,
    timeout_times,
    mode="both",
    failure_reasons=None,
):
    diagnosis_score = (
        round_score(diagnosis_judge_detail["score"])
        if mode != "solution" else None
    )
    recovery_score, recovery_detail = compute_recovery_effect_score(
        functional_gate,
        original_time,
        recovered_time,
        baseline_times,
        timeout_times,
        failure_reasons=failure_reasons,
    )
    total_score = round_score(recovery_score if mode == "solution" else (
        diagnosis_score * DIAGNOSIS_SCORE_WEIGHT
        + recovery_score * RECOVERY_SCORE_WEIGHT
    ))
    return {
        "score": total_score,
        "diagnosis_score": diagnosis_score,
        "recovery_score": recovery_score,
        "performance_score": recovery_score,
        "recovery_detail": recovery_detail,
    }


def assess_recovery_performance(
    baseline_eval,
    recovered_eval,
    result_comparison_mode,
    diagnosis_judge_detail,
    timeout_times,
    mode,
):
    """Compute one recovered-workload score and its correctness evidence."""
    baseline_error_count = baseline_eval.get("error_count", 0)
    recovered_error_count = recovered_eval.get("error_count", 0)
    additional_errors = recovered_error_count > baseline_error_count
    recovered_error_free = (
        bool(recovered_eval["success"])
        and all(
            run.get("error_count", 0) == 0
            for run in recovered_eval.get("measured_runs", [])
        )
    )
    result_comparison = compare_workload_evaluations(
        baseline_eval, recovered_eval, result_comparison_mode
    )
    initialization_comparison = compare_initialization_errors(
        baseline_eval, recovered_eval
    )
    functional_gate = (
        recovered_error_free
        and result_comparison["equivalent"]
        and initialization_comparison["compatible"]
    )
    failure_reasons = []
    if recovered_eval.get("timed_out"):
        failure_reasons.append(
            f"{recovered_eval.get('timeout_phase') or 'unknown'}_workload_timeout"
        )
    if not recovered_error_free:
        failure_reasons.append("recovered_user_workload_error")
    if not result_comparison["equivalent"]:
        failure_reasons.append("result_mismatch")
    if not initialization_comparison["compatible"]:
        failure_reasons.append("initialization_errors_incompatible")

    score_detail = compute_sample_score(
        diagnosis_judge_detail,
        functional_gate,
        baseline_eval["total_time"],
        recovered_eval["total_time"],
        baseline_eval.get("measured_user_times"),
        timeout_times,
        mode=mode,
        failure_reasons=failure_reasons,
    )
    return {
        "score_detail": score_detail,
        "recovered_eval": recovered_eval,
        "recovered_total_time": recovered_eval["total_time"],
        "baseline_error_count": baseline_error_count,
        "recovered_error_count": recovered_error_count,
        "additional_errors": additional_errors,
        "recovered_error_free": recovered_error_free,
        "result_comparison": result_comparison,
        "result_comparison_summary": compact_result_comparison(
            result_comparison
        ),
        "results_equivalent": result_comparison["equivalent"],
        "initialization_comparison": initialization_comparison,
        "initialization_errors_compatible": initialization_comparison[
            "compatible"
        ],
        "functional_gate": functional_gate,
        "failure_reasons": failure_reasons,
        "performance_comparison": build_performance_comparison(
            baseline_eval, recovered_eval
        ),
    }


def _normalize_result_value(value) :
    """Normalize a single cell value into a stable, hashable representation.

    Float values are rounded to a fixed precision to tolerate negligible numeric
    drift, while every other type is rendered as its string form so that the
    fingerprint is deterministic regardless of the underlying Python type.
    """
    if value is None :
        return "\x00NULL\x00"
    if isinstance(value, bool) :
        return f"bool:{value}"
    if isinstance(value, float) :
        return f"num:{round(value, RESULT_FLOAT_PRECISION)}"
    if isinstance(value, int) :
        return f"num:{round(float(value), RESULT_FLOAT_PRECISION)}"
    return f"str:{value}"


def _statement_result_fingerprint(result) :
    """Build an order-insensitive, column-order-insensitive fingerprint for one
    statement result produced by `execute_sql_file_in_session_with_timing`.

    For row-returning statements the fingerprint is derived from the multiset of
    rows (rows are sorted to ignore row order, and cells inside a row are sorted
    by column name to ignore column order). This makes the fingerprint invariant
    to SQL rewriting / scheduling that preserves the business result. For command
    statements (INSERT/UPDATE/...) the affected row count is used instead.
    """
    if not isinstance(result, dict) :
        # No structured result captured (e.g. failed statement); use a constant
        # so that such statements only match other equally empty statements.
        return ("none",)

    result_type = result.get("result_type")
    if result_type == "rows" :
        columns = result.get("columns") or []
        rows = result.get("rows") or []
        normalized_rows = []
        for row in rows :
            # Pair each value with its column name, then sort by column name so
            # that a reordered SELECT list yields the same canonical row.
            paired = sorted(
                zip(columns, row),
                key=lambda item : str(item[0]),
            )
            normalized_rows.append(
                tuple(_normalize_result_value(val) for _, val in paired)
            )
        # Sort rows to make the comparison a multiset comparison (order-insensitive).
        normalized_rows.sort()
        digest = hashlib.md5(
            json.dumps(normalized_rows, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        return ("rows", len(normalized_rows), digest)

    # Command statement: rely on the affected row count as the observable effect.
    return ("command", result.get("row_count"))


def _workload_result_multiset(workload_results) :
    """Reduce a full workload run into a multiset (Counter) of statement
    fingerprints, ignoring the order in which the statements were executed."""
    from collections import Counter

    fingerprints = []
    for item in workload_results :
        # Only row-returning queries carry observable business output worth
        # matching; command statements without a structured result still record
        # their effect via `_statement_result_fingerprint`.
        fingerprints.append(_statement_result_fingerprint(item.get("result")))
    return Counter(fingerprints)


def _fingerprint_json_value(value):
    if isinstance(value, (tuple, list)):
        return [_fingerprint_json_value(item) for item in value]
    return value


def summarize_workload_result_fingerprints(
    workload_results,
    include_full_detail=True,
):
    result_multiset = [
        {
            "fingerprint": _fingerprint_json_value(fingerprint),
            "count": count,
        }
        for fingerprint, count in sorted(
            _workload_result_multiset(workload_results).items(),
            key=lambda item: repr(item[0]),
        )
    ]
    encoded_multiset = json.dumps(
        result_multiset,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    detail = {
        "detail_level": "full" if include_full_detail else "digest_only",
        "statement_count": len(workload_results),
        "result_multiset_sha256": hashlib.sha256(encoded_multiset).hexdigest(),
    }
    if not include_full_detail:
        return detail

    statements = []
    for index, item in enumerate(workload_results):
        result = item.get("result")
        sql_text = str(item.get("sql") or "")
        normalized_sql = " ".join(sql_text.split())
        statement = {
            "statement_index": index + 1,
            "sql_sha256": hashlib.sha256(
                sql_text.encode("utf-8")
            ).hexdigest(),
            "sql_preview": normalized_sql[:300],
            "success": bool(item.get("success", False)),
            "execution_time": item.get("execution_time"),
            "error": item.get("error"),
            "result_fingerprint": _fingerprint_json_value(
                _statement_result_fingerprint(result)
            ),
        }
        if isinstance(result, dict):
            statement.update({
                "result_type": result.get("result_type"),
                "status_message": result.get("status_message"),
                "row_count": result.get("row_count"),
                "columns": result.get("columns"),
            })
        statements.append(statement)
    detail["statements"] = statements
    detail["result_multiset"] = result_multiset
    return detail


def _summarize_comparison_run(run_index, run, reference_results=None):
    matches_reference = None
    if reference_results is not None:
        matches_reference = workload_results_equivalent(
            reference_results, run.get("results") or []
        )
    include_full_detail = run_index == 1 or matches_reference is False
    detail = {
        "run_index": run_index,
        "total_time": run.get("total_time"),
        "success": run.get("success"),
        "error_count": run.get("error_count"),
        "result_detail": summarize_workload_result_fingerprints(
            run.get("results") or [],
            include_full_detail=include_full_detail,
        ),
    }
    if matches_reference is not None:
        detail["matches_baseline_reference"] = matches_reference
    return detail


def workload_results_equivalent(original_results, optimized_results) :
    """Authoritative, order-independent correctness check between the original
    and the optimized workload outputs.

    Instead of comparing the two result lists position-by-position (which breaks
    under workload scheduling, SQL rewriting or column reordering), this compares
    the *multiset* of per-statement result fingerprints. Two runs are considered
    equivalent when they produce exactly the same bag of results, regardless of
    statement execution order.
    """
    return _workload_result_multiset(original_results) == _workload_result_multiset(optimized_results)


def compare_workload_evaluations(baseline_eval, recovered_eval, comparison_mode):
    if comparison_mode not in RESULT_COMPARISON_MODES:
        raise ValueError(
            f"result_comparison_mode must be one of {sorted(RESULT_COMPARISON_MODES)}."
        )

    if comparison_mode == "common":
        equivalent = workload_results_equivalent(
            baseline_eval["results"], recovered_eval["results"]
        )
        return {
            "mode": comparison_mode,
            "equivalent": equivalent,
            "baseline_results_consistent": None,
            "recovered_results_consistent": None,
            "all_recovered_runs_match_baseline": equivalent,
            "baseline_comparison_runs": [{
                "run_index": "median_representative",
                "result_detail": summarize_workload_result_fingerprints(
                    baseline_eval["results"]
                ),
            }],
            "recovered_comparison_runs": [{
                "run_index": "median_representative",
                "matches_baseline_reference": equivalent,
                "result_detail": summarize_workload_result_fingerprints(
                    recovered_eval["results"]
                ),
            }],
        }

    baseline_runs = baseline_eval.get("measured_runs") or []
    recovered_runs = recovered_eval.get("measured_runs") or []
    if not baseline_runs or not recovered_runs:
        return {
            "mode": comparison_mode,
            "equivalent": False,
            "baseline_results_consistent": False,
            "recovered_results_consistent": False,
            "all_recovered_runs_match_baseline": False,
            "baseline_comparison_runs": [],
            "recovered_comparison_runs": [],
        }

    baseline_reference = baseline_runs[0]["results"]
    recovered_reference = recovered_runs[0]["results"]
    baseline_consistent = all(
        workload_results_equivalent(baseline_reference, run["results"])
        for run in baseline_runs[1:]
    )
    recovered_consistent = all(
        workload_results_equivalent(recovered_reference, run["results"])
        for run in recovered_runs[1:]
    )
    all_recovered_match = all(
        workload_results_equivalent(baseline_reference, run["results"])
        for run in recovered_runs
    )
    return {
        "mode": comparison_mode,
        "equivalent": (
            baseline_consistent
            and recovered_consistent
            and all_recovered_match
        ),
        "baseline_results_consistent": baseline_consistent,
        "recovered_results_consistent": recovered_consistent,
        "all_recovered_runs_match_baseline": all_recovered_match,
        "baseline_comparison_runs": [
            _summarize_comparison_run(
                index, run, baseline_reference
            )
            for index, run in enumerate(baseline_runs, start=1)
        ],
        "recovered_comparison_runs": [
            _summarize_comparison_run(
                index, run, baseline_reference
            )
            for index, run in enumerate(recovered_runs, start=1)
        ],
    }


def compact_result_comparison(result_comparison):
    return {
        "mode": result_comparison.get("mode"),
        "equivalent": result_comparison.get("equivalent"),
        "baseline_results_consistent": result_comparison.get(
            "baseline_results_consistent"
        ),
        "recovered_results_consistent": result_comparison.get(
            "recovered_results_consistent"
        ),
        "all_recovered_runs_match_baseline": result_comparison.get(
            "all_recovered_runs_match_baseline"
        ),
        "detail_file": "workload_result_comparison_detail.json",
    }


def _initialization_error_multiset(run):
    from collections import Counter

    return Counter(
        (
            index,
            " ".join(str(item.get("error") or "").split()),
        )
        for index, item in enumerate(run.get("init_results") or [])
        if not item.get("success", False)
    )


def compare_initialization_errors(baseline_eval, recovered_eval):
    baseline_runs = (
        baseline_eval.get("all_runs")
        or baseline_eval.get("measured_runs")
        or []
    )
    recovered_runs = (
        recovered_eval.get("all_runs")
        or recovered_eval.get("measured_runs")
        or []
    )
    if not baseline_runs or not recovered_runs:
        return {
            "compatible": False,
            "baseline_max_error_count": None,
            "recovered_max_error_count": None,
            "new_error_signatures": ["measurement runs unavailable"],
        }

    baseline_error_sets = [
        _initialization_error_multiset(run) for run in baseline_runs
    ]
    recovered_error_sets = [
        _initialization_error_multiset(run) for run in recovered_runs
    ]
    allowed_errors = baseline_error_sets[0].copy()
    for error_set in baseline_error_sets[1:]:
        for signature, count in error_set.items():
            allowed_errors[signature] = max(allowed_errors[signature], count)

    baseline_max_error_count = max(
        sum(error_set.values()) for error_set in baseline_error_sets
    )
    recovered_max_error_count = max(
        sum(error_set.values()) for error_set in recovered_error_sets
    )
    new_error_signatures = []
    for error_set in recovered_error_sets:
        for signature, count in error_set.items():
            if count > allowed_errors[signature]:
                new_error_signatures.append({
                    "statement_index": signature[0] + 1,
                    "error": signature[1],
                    "recovered_count": count,
                    "baseline_allowed_count": allowed_errors[signature],
                })

    return {
        "compatible": (
            recovered_max_error_count <= baseline_max_error_count
            and not new_error_signatures
        ),
        "baseline_max_error_count": baseline_max_error_count,
        "recovered_max_error_count": recovered_max_error_count,
        "new_error_signatures": new_error_signatures,
    }


def _timed_sql_worker(connection, sql_path, db_config):
    """A killable worker; no result-set temporary files are written."""
    try:
        connection.send(("ready", None))
        results = execute_sql_file_in_session_with_timing(
            sql_path, db_config, timeout_seconds=db_config.get("_evaluation_sql_timeout")
        )
        connection.send(("result", results))
    except Exception as exc:
        if is_workload_timeout_error(exc):
            connection.send(("result", [{
                "sql": "", "success": False, "execution_time": -1,
                "result": None, "timed_out": True,
                "error": f"{type(exc).__name__}: {exc}",
            }]))
        else:
            connection.send(("error", f"{type(exc).__name__}: {exc}"))
    finally:
        connection.close()


def execute_sql_with_deadline(sql_path, db_config, timeout_seconds):
    """Bound a whole SQL file (connect/execute/fetch), independently of SQL SET.

    Spawn is safe when sample evaluation runs in threads. Worker import/startup
    has a separate 30-second budget and is excluded from the workload deadline.
    On timeout the parent stops further rounds; sample-finally removes the
    dedicated container, also terminating any remaining server-side query.
    """
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("Workload deadline must be finite and positive.")
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    worker_config = dict(db_config, _evaluation_sql_timeout=timeout_seconds)
    process = context.Process(
        target=_timed_sql_worker, args=(sender, sql_path, worker_config)
    )
    # recv() includes framing and deserialization; poll() alone does not bound it.
    # Only this reader touches the pipe. The supervising thread waits for COMPLETE
    # messages and can kill the sender even when a frame is only partly written.
    messages = queue_module.Queue()
    def read_messages():
        try:
            messages.put(receiver.recv())
            messages.put(receiver.recv())
        except Exception as exc:
            messages.put(("error", f"SQL worker channel failed: {type(exc).__name__}: {exc}"))

    reader = threading.Thread(target=read_messages, daemon=True, name="eval-sql-receiver")
    try:
        process.start()
        sender.close()
        reader.start()
        try:
            kind, payload = messages.get(timeout=30)
        except queue_module.Empty:
            raise RuntimeError("SQL worker startup exceeded 30 seconds.")
        if kind != "ready":
            raise RuntimeError(f"SQL worker failed to start: {payload}")
        deadline = time.monotonic() + timeout_seconds
        try:
            kind, payload = messages.get(timeout=max(0, deadline - time.monotonic()))
        except queue_module.Empty:
            return [{
                "sql": "", "success": False, "execution_time": -1,
                "result": None, "timed_out": True,
                "error": f"Workload deadline exceeded ({timeout_seconds:.6f}s).",
            }]
        if kind == "error":
            return [{
                "sql": "", "success": False, "execution_time": -1,
                "result": None, "error": payload,
            }]
        return payload
    finally:
        sender.close()
        if process.pid is not None:
            process.join(0.1)
            if process.is_alive():
                process.terminate()
                process.join(1)
            if process.is_alive():
                process.kill()
                process.join(5)
            # Process.close() raises while the child is still being reaped.
            # Do not turn a bounded workload cleanup race into a sample abort.
            if not process.is_alive():
                process.close()
        if reader.ident is not None:
            reader.join(1)
        receiver.close()


def recovered_workload_timeout(baseline_eval, timeout_times):
    baseline_time = baseline_eval["total_time"]
    if not math.isfinite(baseline_time) or baseline_time <= 0:
        raise ValueError("A positive finite baseline latency is required.")
    if float(timeout_times) != RECOVERY_ZERO_SCORE_RATIO:
        raise ValueError(
            f"timeout_times must be exactly {RECOVERY_ZERO_SCORE_RATIO:g}."
        )
    return max(baseline_time * RECOVERY_WATCHDOG_RATIO, RECOVERY_MIN_TIMEOUT_SECONDS)


def validate_baseline_encoding(baseline_eval):
    """Client decoding failures invalidate the reference, even in warm-up/init."""
    for run in baseline_eval.get("all_runs", [baseline_eval]):
        for item in run.get("init_results", []) + run.get("results", []):
            error = item.get("error") or ""
            if (item.get("error_type") in ("UnicodeDecodeError", "UnicodeEncodeError")
                    or ("codec can't" in error and ("decode" in error or "encode" in error))):
                raise ValueError(
                    "Baseline encoding error; reference results and timing are invalid: "
                    + error
                )


def _run_verified_workload_once(
    initialization_workload_path,
    verified_workload_path,
    db_config,
    timeout_seconds=None,
    initialization_timeout_seconds=None,
) :
    """Run ONE measurement iteration: apply the initialization workload first,
    then execute and time the verified (user) workload.

    Every iteration -- warm-up and measured alike -- MUST re-apply the
    initialization workload beforehand so the verified workload always starts
    from the same well-defined data-access state (the initialization workload
    is the LLM-produced state-restoration step). The initialization run's own
    results/timing are captured too, but they are only a precondition: the user
    workload's timing is what the caller treats as authoritative.
    """
    if timeout_seconds is None:
        init_results = execute_sql_file_in_session_with_timing(
            initialization_workload_path, db_config
        )
        user_results = execute_sql_file_in_session_with_timing(
            verified_workload_path, db_config
        )
    else:
        initialization_timeout_seconds = (
            initialization_timeout_seconds
            if initialization_timeout_seconds is not None
            else db_config.get(
                "initialization_workload_timeout_seconds",
                DEFAULT_INITIALIZATION_WORKLOAD_TIMEOUT_SECONDS,
            )
        )
        init_results = execute_sql_with_deadline(
            initialization_workload_path, db_config,
            initialization_timeout_seconds,
        )
        if any(item.get("timed_out") for item in init_results):
            user_results = [{
                "sql": "", "success": False, "execution_time": -1,
                "result": None, "timed_out": True,
                "error": "User workload skipped after initialization timeout.",
            }]
        else:
            user_results = execute_sql_with_deadline(
                verified_workload_path, db_config, timeout_seconds
            )

    init_success = all(item.get("success", False) for item in init_results)
    user_success = all(item.get("success", False) for item in user_results)
    # Initialization failures are recorded and compared across phases; they do
    # not by themselves invalidate a run. The recovered user workload, however,
    # must be error-free for the functional recovery gate to pass.
    init_error_count = sum(1 for item in init_results if not item.get("success", False))
    user_error_count = sum(1 for item in user_results if not item.get("success", False))
    # Sum only the statements that executed successfully (failed ones carry -1).
    init_time = sum(item["execution_time"] for item in init_results if item.get("execution_time", -1) >= 0)
    user_time = sum(item["execution_time"] for item in user_results if item.get("execution_time", -1) >= 0)

    return {
        "init_results" : init_results,
        "init_total_time" : init_time,
        "init_success" : init_success,
        "init_error_count" : init_error_count,
        "results" : user_results,
        "total_time" : user_time,
        "success" : user_success,
        "error_count" : user_error_count,
        "timed_out": any(item.get("timed_out") for item in init_results + user_results),
        "timeout_phase": (
            "initialization" if any(item.get("timed_out") for item in init_results)
            else "user" if any(item.get("timed_out") for item in user_results)
            else None
        ),
    }


def evaluate_verified_workload(
    initialization_workload_path,
    verified_workload_path,
    db_config,
    logger,
    median_runs=1,
    timeout_seconds=None,
    initialization_timeout_seconds=None,
) :
    """Measure the user workload with a warm-up pass plus `median_runs` timed runs.

    Latency measured on the very first execution is unreliable: the first run
    pays a cold-start cost (buffer pool / OS page cache not yet populated, plans
    not yet cached), which inflates the observed time. To obtain an accurate
    latency we therefore:

      * run one WARM-UP iteration whose timing is discarded, then
      * run `median_runs` MEASURED iterations and take the MEDIAN user-workload
        time as the definitive measurement (median is robust to outliers).

    Every iteration -- warm-up and each measured run -- FIRST re-applies the
    initialization workload (`initialization_workload.sql`) so the user workload
    always starts from the same restored data-access state; this is what makes
    repeated execution of stateful workloads well-defined. Both the
    initialization workload's and the user workload's results/timing are kept
    for the record, but only the user workload's timing drives evaluation
    (the initialization workload is merely a precondition).

    With the default `median_runs == 1` a single measured run is taken (its
    own value being the median), preceded by the warm-up pass.
    """
    median_runs = max(1, int(median_runs))
    effective_initialization_timeout_seconds = None
    if timeout_seconds is not None:
        effective_initialization_timeout_seconds = (
            initialization_timeout_seconds
            if initialization_timeout_seconds is not None
            else db_config.get(
                "initialization_workload_timeout_seconds",
                DEFAULT_INITIALIZATION_WORKLOAD_TIMEOUT_SECONDS,
            )
        )
        if (
            isinstance(effective_initialization_timeout_seconds, bool)
            or not isinstance(
                effective_initialization_timeout_seconds, (int, float)
            )
            or not math.isfinite(effective_initialization_timeout_seconds)
            or effective_initialization_timeout_seconds <= 0
        ):
            raise ValueError(
                "Initialization workload deadline must be positive and finite."
            )
    logger.info(
        f"[EVALUATE VERIFIED WORKLOAD] Running 1 Warm-up + `{median_runs}` Measured Iteration(s) "
        "(each preceded by the initialization workload) ..."
    )

    # ---- Warm-up iteration (timing discarded) --------------------------------
    logger.info("[EVALUATE VERIFIED WORKLOAD] Warm-up Iteration (cold-start, timing discarded) ...")
    def run_once():
        if timeout_seconds is None:
            return _run_verified_workload_once(
                initialization_workload_path, verified_workload_path, db_config
            )
        return _run_verified_workload_once(
            initialization_workload_path, verified_workload_path, db_config,
            timeout_seconds=timeout_seconds,
            initialization_timeout_seconds=(
                effective_initialization_timeout_seconds
            ),
        )

    warmup_run = run_once()
    logger.info(
        f"[EVALUATE VERIFIED WORKLOAD] Warm-up Done "
        f"(user_workload_time=`{warmup_run['total_time']:.6f}`s, success=`{warmup_run['success']}`)."
    )

    # ---- Measured iterations -------------------------------------------------
    measured_runs = []
    for run_index in range(1, median_runs + 1) :
        if warmup_run.get("timed_out"):
            break
        run = run_once()
        measured_runs.append(run)
        logger.info(
            f"[EVALUATE VERIFIED WORKLOAD] Measured Run `{run_index}/{median_runs}` "
            f"(init_workload_time=`{run['init_total_time']:.6f}`s, "
            f"user_workload_time=`{run['total_time']:.6f}`s, success=`{run['success']}`)."
        )
        if run.get("timed_out"):
            break

    # The representative run is the one whose user-workload time is the median
    # across all measured runs (ties resolved to the lower index). Its results
    # are what get compared for correctness, so results and timing stay coherent.
    all_runs = [warmup_run] + measured_runs
    timed_out = any(run.get("timed_out") for run in all_runs)
    user_times = [run["total_time"] for run in measured_runs if not run.get("timed_out")]
    # Partial execution time of a timed-out workload is not a latency sample.
    median_user_time = statistics.median(user_times) if user_times else None
    representative = (
        next(run for run in all_runs if run.get("timed_out")) if timed_out
        else min(measured_runs, key=lambda run: abs(run["total_time"] - median_user_time))
    )
    if timed_out:
        median_user_time = None
        exceeded_timeout = (
            effective_initialization_timeout_seconds
            if representative["timeout_phase"] == "initialization" else timeout_seconds
        )
        logger.warning(
            f"[EVAL][TIMEOUT] {representative['timeout_phase']} workload exceeded "
            f"{exceeded_timeout:.6f}s; stopping remaining iterations."
        )
    # Warm-up timing is discarded, but its execution remains part of correctness:
    # any user-workload error during evaluation fails the recovery gate.
    overall_user_success = all(run["success"] for run in all_runs)
    max_user_error_count = max(run["error_count"] for run in all_runs)

    logger.info(
        f"[EVALUATE VERIFIED WORKLOAD] Median User-workload Time = `{median_user_time}`s "
        f"over `{median_runs}` measured run(s); measurement success=`{overall_user_success}`."
    )

    return {
        # User (verified) workload: the authoritative measurement.
        "results" : representative["results"],
        "total_time" : median_user_time,
        "success" : overall_user_success,
        # Maximum failed-statement count across warm-up and measured iterations.
        "error_count" : max_user_error_count,
        # Initialization workload of the representative run: recorded as a
        # precondition, not used to drive the score.
        "init_results" : representative["init_results"],
        "init_total_time" : representative["init_total_time"],
        "init_success" : representative["init_success"],
        "init_error_count" : representative["init_error_count"],
        # Full breakdown for auditing / persistence.
        "median_runs" : median_runs,
        "measured_user_times" : user_times,
        "warmup_user_time" : warmup_run["total_time"],
        "warmup_init_time" : warmup_run["init_total_time"],
        "warmup_success" : warmup_run["success"],
        "warmup_error_count" : warmup_run["error_count"],
        # Retained in memory for strict result and initialization-error checks.
        # Persistence still goes through summarize_workload_run(), which omits
        # full statement results.
        "measured_runs" : measured_runs,
        "all_runs" : all_runs,
        "timed_out": timed_out,
        "timeout_seconds": timeout_seconds,
        "initialization_timeout_seconds": (
            effective_initialization_timeout_seconds
        ),
        "timeout_phase": representative.get("timeout_phase") if timed_out else None,
        "completed_measured_runs": len(user_times),
    }


def summarize_workload_run(workload_eval) :
    """Condense a workload run into just its timing and error information.

    User workloads can contain many queries, so persisting every statement's
    full result set to JSON is wasteful and unnecessary for evaluation. We keep
    the full results in memory only long enough to compare pre/post correctness,
    and store only the aggregate execution time plus any failing statements.
    """
    errors = [
        {
            "statement_index": idx + 1,
            "error": item.get("error"),
        }
        for idx, item in enumerate(workload_eval["results"])
        if not item.get("success", False)
    ]
    return {
        # User (verified) workload -- the authoritative timing/result.
        "total_time": workload_eval["total_time"],
        "success": workload_eval["success"],
        "error_count": workload_eval.get("error_count", len(errors)),
        "errors": errors,
        "timed_out": workload_eval.get("timed_out", False),
        "timeout_seconds": workload_eval.get("timeout_seconds"),
        "initialization_timeout_seconds": workload_eval.get("initialization_timeout_seconds"),
        "timeout_phase": workload_eval.get("timeout_phase"),
        "completed_measured_runs": workload_eval.get("completed_measured_runs"),
        # Initialization workload -- recorded as a precondition only.
        "initialization_total_time": workload_eval.get("init_total_time"),
        "initialization_success": workload_eval.get("init_success"),
        "initialization_error_count": workload_eval.get("init_error_count"),
        # Warm-up + median-run bookkeeping for auditability.
        "median_runs": workload_eval.get("median_runs"),
        "measured_user_times": workload_eval.get("measured_user_times"),
        "warmup_user_time": workload_eval.get("warmup_user_time"),
        "warmup_success": workload_eval.get("warmup_success"),
        "warmup_error_count": workload_eval.get("warmup_error_count"),
    }


def build_performance_comparison(baseline_eval, recovered_eval):
    baseline_time = baseline_eval.get("total_time")
    recovered_time = recovered_eval.get("total_time")
    measurable = bool(
        isinstance(baseline_time, (int, float))
        and baseline_time > 0
        and isinstance(recovered_time, (int, float))
        and recovered_time >= 0
    )
    delta_seconds = (
        recovered_time - baseline_time if measurable else None
    )
    return {
        "measurable": measurable,
        "baseline_median_seconds": baseline_time,
        "recovered_median_seconds": recovered_time,
        "delta_seconds": delta_seconds,
        "delta_percent": (
            delta_seconds / baseline_time * 100.0 if measurable else None
        ),
        "recovered_to_baseline_ratio": (
            recovered_time / baseline_time if measurable else None
        ),
        "baseline_measured_seconds": baseline_eval.get(
            "measured_user_times"
        ),
        "recovered_measured_seconds": recovered_eval.get(
            "measured_user_times"
        ),
        "baseline_warmup_seconds": baseline_eval.get("warmup_user_time"),
        "recovered_warmup_seconds": recovered_eval.get("warmup_user_time"),
    }


def record_evaluation_abort(record, mode, output_dir, exc, logger):
    detail = {
        "sample_id": record["sample_id"], "mode": mode, "status": "aborted",
        "score": None, "diagnosis_score": None, "recovery_score": None,
        "performance_score": None,
        "optimized_score": None, "original_score": None,
        "optimized_performance_score": None,
        "original_performance_score": None,
        "error": str(exc),
    }
    record.update(is_aborted=True, is_error=True, score=None, sample_detail=detail)
    with open(os.path.join(output_dir, "evaluation_error.txt"), "w", encoding="utf-8") as f:
        f.write(str(exc))
    with open(os.path.join(output_dir, "score_detail.json"), "w", encoding="utf-8") as f:
        json.dump(detail, f, indent=4, ensure_ascii=False)
    logger.error(f"[EVAL][ERROR] Evaluation aborted: {exc}")
    logger.info(
        "[EVAL][SCORE][Optimized Performance] status=aborted score=N/A"
    )
    logger.info(
        "[EVAL][SCORE][Original Performance] status=aborted score=N/A"
    )


TERMINAL_EVALUATION_STATUSES = frozenset({"completed", "not_evaluated"})


def is_db_gpt_incompatible_sample(solution_script_directory, dbms):
    """DB-GPT's PostgreSQL-only adapter cannot evaluate MySQL cases.

    Identify the method from its artifact directory, never from the Judge
    provider/model (shared by all methods). Unknown DBMS values are not skipped.
    """
    method = os.path.basename(os.path.normpath(solution_script_directory or ""))
    method = method.lower().replace("-", "_")
    return method in {"db_gpt", "dbgpt"} and str(dbms or "").lower().startswith("mysql")


def load_terminal_evaluation_record(
    output_base_dir, sample_id, mode, timeout_times,
    *, solution_script_directory=None, dbms=None,
):
    """Rebuild an aggregate record from a complete per-sample score artifact."""
    sample_output = os.path.join(output_base_dir, str(sample_id))
    score_path = os.path.join(sample_output, "score_detail.json")
    if not os.path.isfile(score_path):
        return None
    try:
        score_mtime_ns = os.stat(score_path).st_mtime_ns
        for root, _, files in os.walk(sample_output):
            for name in files:
                artifact_path = os.path.join(root, name)
                if (
                    os.path.abspath(artifact_path) != os.path.abspath(score_path)
                    and os.stat(artifact_path).st_mtime_ns > score_mtime_ns
                ):
                    return None
    except OSError:
        return None
    try:
        with open(score_path, "r", encoding="utf-8") as file:
            detail = json.load(file)
    except (OSError, json.JSONDecodeError):
        return None
    if (
        not isinstance(detail, dict)
        or str(detail.get("sample_id")) != str(sample_id)
        or detail.get("mode") != mode
        or detail.get("status") not in TERMINAL_EVALUATION_STATUSES
    ):
        return None
    init_log = os.path.join(sample_output, "init_db_log.txt")
    if database_init_log_errors(init_log, detail.get("dbms")):
        return None

    status = detail["status"]
    incompatible = is_db_gpt_incompatible_sample(solution_script_directory, dbms)
    # Legacy "not_evaluated" records conflate missing artifacts with unsupported
    # databases. Reclassify through the normal worker, without reusing old skips.
    if status == "not_evaluated":
        if not incompatible or detail.get("reason_code") != "unsupported_dbms":
            return None
    elif incompatible:
        return None
    if status == "completed" and detail.get("score") is None:
        return None
    is_not_evaluated = status == "not_evaluated"
    functional_gate = bool(detail.get("functional_gate", mode == "report"))
    execution_time_ratio = detail.get("execution_time_ratio")
    over_bound = in_bound = optimized = False
    if not is_not_evaluated and mode != "report":
        if (
            not functional_gate
            or not isinstance(execution_time_ratio, (int, float))
            or execution_time_ratio > timeout_times
        ):
            over_bound = True
        elif execution_time_ratio <= 1:
            optimized = True
        else:
            in_bound = True

    solution_exists = detail.get("solution_script_exists")
    solution_executed = detail.get("solution_executed")
    return {
        "sample_id": str(sample_id),
        "score": detail.get("score"),
        "sample_detail": detail,
        "is_error": bool(detail.get("recovered_error_free") is False),
        "is_aborted": False,
        "is_not_evaluated": is_not_evaluated,
        "over_bound": over_bound,
        "in_bound": in_bound,
        "optimized": optimized,
        "solution_missing": solution_exists is False,
        "solution_execution_error": (
            solution_exists is True and solution_executed is False
        ),
    }


def remove_incomplete_evaluation_output(output_base_dir, sample_id):
    """Delete only one non-terminal sample directory before retrying it."""
    sample_output = os.path.join(output_base_dir, str(sample_id))
    if os.path.islink(sample_output):
        os.unlink(sample_output)
    elif os.path.isdir(sample_output):
        shutil.rmtree(sample_output)


def evaluate_single_sample(i, anomaly_pair, shared, base_docker_name, worker_host_port,
                           logger, timeout_times, median_runs=1):
    """Run the full 7-step evaluation for ONE sample and return a structured
    record. Docker container names are UNIQUE per initialization (see
    `initialize_sample_environment`), so this function is safe to run
    concurrently for different samples as long as each worker is given its own
    `base_docker_name` prefix and `worker_host_port` slot.

    The final container is always torn down in the `finally` block so parallel
    workers never leak orphan containers. Never raises: an unrecoverable sample
    is reported via the returned record's `aborted` flag.
    """
    anomaly_directory = shared['anomaly_directory']
    solution_script_directory = shared['solution_script_directory']
    output_base_dir = shared['output_base_dir']
    base_dir = shared['base_dir']
    result_comparison_mode = shared['result_comparison_mode']
    ground_truth_diagnosis = shared['ground_truth_diagnosis']
    judge_llm_config = shared['judge_llm_config']
    judge_repetitions = shared['judge_repetitions']
    mode = evaluation_mode(shared)

    sample_id = anomaly_sample_id(anomaly_pair)
    # Re-scope the logger so EVERY line emitted while this sample is evaluated --
    # here and inside every helper it hands the logger to -- is prefixed with
    # `[Sample ID: <id>]`. This is what keeps interleaved parallel logs readable.
    logger = _sample_logger(logger, sample_id)
    logger.info("[EVAL][SAMPLE %d] Starting Evaluation with Database `%s` (worker prefix `%s`, port %d) ..."
                % (i, anomaly_pair.get('database_name'), base_docker_name, worker_host_port))

    output_sub_dir = os.path.join(output_base_dir, sample_id)
    os.makedirs(output_sub_dir, exist_ok=True)

    record = {
        "sample_id": sample_id, "score": None, "sample_detail": None,
        "is_error": False, "is_aborted": False, "is_not_evaluated": False,
        "over_bound": False,
        "in_bound": False, "optimized": False, "solution_missing": False,
        "solution_execution_error": False,
    }
    current_docker_name = None
    try :
        if mode != "report" and shared.get("_cleanup_worker_prefix", False):
            cleanup_evaluation_prefix(base_docker_name)
        current_anomaly_directory = find_anomaly_sample_directory(anomaly_directory, anomaly_pair)
        solution_paths = resolve_solution_artifact_paths(
            solution_script_directory, sample_id, i
        )
        solution_script_path = solution_paths["solution_script"]
        optimized_workload_path = solution_paths["optimized_workload"]
        diagnosis_report_path = solution_paths["diagnosis_report"]
        sample_dbms = anomaly_pair.get("dbms")
        if not sample_dbms:
            sample_config_path = os.path.join(current_anomaly_directory, "config.json")
            if os.path.isfile(sample_config_path):
                with open(sample_config_path, encoding="utf-8") as file:
                    sample_dbms = json.load(file).get("dbms")
        incompatible = is_db_gpt_incompatible_sample(
            solution_script_directory, sample_dbms
        )
        available_artifacts = [
            path for path in (
                solution_script_path,
                optimized_workload_path,
                diagnosis_report_path,
            )
            if os.path.isfile(path)
        ]
        if incompatible or not available_artifacts:
            zero_score = None if incompatible else 0.0
            record.update(
                is_not_evaluated=incompatible,
                score=zero_score,
                over_bound=not incompatible and mode != "report",
                solution_missing=not incompatible,
            )
            record["sample_detail"] = {
                "sample_id": sample_id,
                "mode": mode,
                "status": "not_evaluated" if incompatible else "completed",
                "score": zero_score,
                "diagnosis_score": zero_score if mode != "solution" else None,
                "artifact_score": zero_score if mode != "solution" else None,
                "recovery_score": zero_score if mode != "report" else None,
                "performance_score": zero_score if mode != "report" else None,
                "optimized_score": zero_score,
                "original_score": zero_score,
                "optimized_performance_score": (
                    zero_score if mode != "report" else None
                ),
                "original_performance_score": (
                    zero_score if mode != "report" else None
                ),
                "functional_gate": False,
                "solution_script_exists": os.path.isfile(solution_script_path),
                "solution_executed": False,
                "dbms": sample_dbms,
                "database_name": anomaly_pair.get("database_name"),
                "reason_code": "unsupported_dbms" if incompatible else "missing_artifacts",
                "reason": (
                    "DB-GPT supports PostgreSQL only; MySQL sample excluded."
                    if incompatible else
                    "No diagnosis or recovery artifact was produced; scored zero."
                ),
                "solution_directory": solution_paths["directory"],
            }
            with open(
                os.path.join(output_sub_dir, "score_detail.json"),
                "w",
                encoding="utf-8",
            ) as file:
                json.dump(
                    record["sample_detail"],
                    file,
                    indent=4,
                    ensure_ascii=False,
                )
            logger.info(
                f"[EVAL][SAMPLE {i}][{'SKIP' if incompatible else 'SCORE'}] "
                f"sample_id=`{sample_id}` "
                + ("status=not_evaluated; unsupported DB-GPT/MySQL sample."
                   if incompatible else "status=completed score=0.000000; no artifacts.")
            )
            if mode != "report":
                displayed_score = "N/A" if incompatible else "0.000000"
                logger.info(
                    f"[EVAL][SAMPLE {i}][SCORE][Optimized Performance] "
                    f"sample_id=`{sample_id}` score={displayed_score}"
                )
                logger.info(
                    f"[EVAL][SAMPLE {i}][SCORE][Original Performance] "
                    f"sample_id=`{sample_id}` score={displayed_score}"
                )
            return record

        # Diagnosis-report judging does not require a live database. Run it before
        # allocating the evaluation container so LLM latency never holds Docker
        # resources or a worker port unnecessarily.
        diagnosis_judge_detail = (
            compute_diagnosis_artifact_score(
                current_anomaly_directory, diagnosis_report_path,
                ground_truth_diagnosis, judge_llm_config, logger,
                repetitions=judge_repetitions,
            ) if mode != "solution" else {"score": None, "status": "not_evaluated"}
        )
        with open(
            os.path.join(output_sub_dir, "diagnosis_judge_detail.json"),
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(diagnosis_judge_detail, file, indent=4, ensure_ascii=False)

        if mode == "report":
            record["score"] = diagnosis_judge_detail["score"]
            record["sample_detail"] = {
                "sample_id": sample_id, "mode": mode, "status": "completed",
                "score": record["score"], "diagnosis_score": record["score"],
                "artifact_score": record["score"], "recovery_score": None,
                "performance_score": None,
                "optimized_score": record["score"],
                "original_score": record["score"],
                "optimized_performance_score": None,
                "original_performance_score": None,
            }
            with open(os.path.join(output_sub_dir, "score_detail.json"), "w") as file:
                json.dump(record["sample_detail"], file, indent=4)
            logger.info(
                f"[EVAL][SCORE] mode=report score={record['score']:.6f} "
                f"diagnosis={record['score']:.6f} recovery=N/A"
            )
            return record

        sample_config = load_sample_config(current_anomaly_directory)
        DBMS = sample_config["dbms"]
        database_name = sample_config["database_name"]
        database_schema_name = sample_config["database_schema_name"]
        system_user_config_json = sample_config["system_user_config"]
        system_hardware_config = sample_config["system_hardware_config"]
        db_type = sample_config["db_type"]
        sample_container_config = build_container_config(DBMS, system_hardware_config, worker_host_port, system_user_config_json)

        # Sample artifacts consumed inside the container MUST be canonical so
        # the bind-mounted container path resolves them.
        initialization_workload_path = os.path.join(current_anomaly_directory, 'initialization_workload.sql')
        verified_workload_path = os.path.join(current_anomaly_directory, 'verified_workload.sql')
        data_modification_path = os.path.join(current_anomaly_directory, 'data_modification.sql')
        anomaly_injection_path = os.path.join(current_anomaly_directory, 'anomaly_inject.sh')

        db_config = {
            'dbms' : DBMS,
            'client_encoding': 'UTF8',
            'initialization_workload_timeout_seconds': shared.get(
                "initialization_workload_timeout_seconds",
                DEFAULT_INITIALIZATION_WORKLOAD_TIMEOUT_SECONDS,
            ),
            'host' : system_user_config_json['DB_HOST'],
            'port' : system_user_config_json['DB_PORT'],
            'host_port' : worker_host_port,
            'user' : system_user_config_json['DB_USER'],
            'password' : system_user_config_json['DB_PASSWORD'],
            'db_name' : database_name,
            'schema' : database_schema_name,
        }
        init_db_config = {
            "dbms" : DBMS,
            "base_dir" : base_dir,
            "db_host" : system_user_config_json['DB_HOST'],
            "db_port" : system_user_config_json['DB_PORT'],
            "db_name" : database_name,
            "db_schema" : database_schema_name,
            "db_type" : db_type,
            "db_user" : system_user_config_json['DB_USER'],
            "password" : system_user_config_json['DB_PASSWORD'],
            "timeout" : TIMEOUTS.get(database_name, DEFAULT_DB_INIT_TIMEOUT),
            "output_dir" : output_sub_dir,
        }

        # ---- STEP 1: Docker container initialization (clean baseline) ----
        current_docker_name = initialize_sample_environment(
            base_docker_name, sample_container_config, init_db_config, DBMS,
            system_user_config_json, data_modification_path, database_name,
            output_sub_dir, logger, stage_label="baseline",
        )

        # ---- STEP 2: Execute workload, record time & results (baseline) --
        logger.info(f"[EVAL][SAMPLE {i}] Executing Verified Workload [Baseline] ...")
        baseline_eval = evaluate_verified_workload(
            initialization_workload_path, verified_workload_path, db_config, logger,
            median_runs=median_runs,
            timeout_seconds=shared.get(
                "baseline_workload_timeout_seconds",
                DEFAULT_BASELINE_WORKLOAD_TIMEOUT_SECONDS,
            ),
        )
        validate_baseline_encoding(baseline_eval)
        if baseline_eval.get("timed_out"):
            raise RuntimeError("Baseline workload timed out; reference latency is invalid.")
        baseline_total_time = baseline_eval["total_time"]
        recovery_timeout_seconds = recovered_workload_timeout(baseline_eval, timeout_times)
        logger.info(
            f"[EVAL][TIMEOUT] Recovery workload deadline={recovery_timeout_seconds:.6f}s "
            f"(max(baseline latency × {RECOVERY_WATCHDOG_RATIO:g}, "
            f"{RECOVERY_MIN_TIMEOUT_SECONDS:g}s); applies to post-recovery "
            "initialization and user workloads)."
        )

        # ---- STEP 3: Re-initialize container (clean pre-injection state) -
        # A brand new unique name is used; the baseline container is reaped as
        # part of the recreation, so the two initializations never clash.
        current_docker_name = initialize_sample_environment(
            base_docker_name, sample_container_config, init_db_config, DBMS,
            system_user_config_json, data_modification_path, database_name,
            output_sub_dir, logger, stage_label="pre-injection",
            previous_docker_name=current_docker_name,
        )

        # ---- STEP 4: Anomaly injection -----------------------------------
        logger.info(f"[EVAL][SAMPLE {i}] Performing Anomaly Injection ...")
        with open(anomaly_injection_path, 'r') as f :
            anomaly_injection_script = f.read().strip()
        injection_timeout = parse_timeout_threshold(anomaly_injection_script, DEFAULT_SCRIPT_TIMEOUT) + 60
        injection_ready_pattern = parse_injection_ready_pattern(
            anomaly_injection_script
        )
        injection_cmd = build_script_command(anomaly_injection_path, db_config, DBMS)
        injection_log = os.path.join(output_sub_dir, "anomaly_injection_log.txt")
        state, injection_output = run_docker_exec_observation(
            current_docker_name,
            injection_cmd,
            injection_log,
            injection_timeout,
            session_name=current_docker_name,
            window_name="injection",
            ready_pattern=injection_ready_pattern,
        )
        if state is False :
            reset_tmux_window(session_name=current_docker_name, window_name="injection")
            raise RuntimeError(f"[EVAL][SAMPLE {i}] Anomaly injection failed. See log: {injection_log}\n{injection_output}")

        # ---- STEP 5: Execute the solution script -------------------------
        logger.info(f"[EVAL][SAMPLE {i}] Executing Solution Script ...")
        # #region debug-point B:solution-start
        import urllib.request as _debug_request; _debug_env=os.path.join(base_dir,'.dbg','multi-worker-locks.env'); _debug_url,_debug_session='http://127.0.0.1:7777/event','multi-worker-locks'; exec("try:\n with open(_debug_env) as _f: _debug_cfg=_f.read(); _debug_url=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SERVER_URL=')),_debug_url); _debug_session=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SESSION_ID=')),_debug_session)\nexcept Exception: pass\ntry:\n _debug_request.urlopen(_debug_request.Request(_debug_url,data=json.dumps({'sessionId':_debug_session,'runId':'post-fix','hypothesisId':'B','location':'test_eval.py:solution-start','msg':'[DEBUG] solution stage start','data':{'sample_id':sample_id,'container':current_docker_name},'ts':int(time.time()*1000)}).encode(),headers={'Content-Type':'application/json'}),timeout=.2).read()\nexcept Exception: pass")
        # #endregion
        solution_script_exists = os.path.exists(solution_script_path)
        solution_executed = False
        if not solution_script_exists :
            logger.error(f"[EVAL][SAMPLE {i}][ERROR] Solution Script Not Found at {solution_script_path}.")
            record["solution_missing"] = True
        else :
            with open(solution_script_path, 'r') as f :
                solution_script = f.read().strip()
            solution_timeout = parse_timeout_threshold(solution_script, DEFAULT_SCRIPT_TIMEOUT) + 60
            recovery_db_config = build_recovery_db_config(
                db_config, shared.get("superuser_config")
            )
            logger.info(
                f"[EVAL][SAMPLE {i}][RECOVERY] Injecting database superuser "
                f"`{recovery_db_config['user']}` into the solution-script environment."
            )
            solution_cmd = build_script_command(
                solution_script_path, recovery_db_config, DBMS
            )
            solution_log = os.path.join(output_sub_dir, "solution_script_execution_log.txt")
            state, _ = run_docker_exec_observation(current_docker_name, solution_cmd, solution_log, solution_timeout, session_name=current_docker_name)
            solution_executed = bool(state)
            if not state :
                reset_tmux_window(session_name=current_docker_name, window_name="observation")
                record["solution_execution_error"] = True
        # #region debug-point B:solution-end
        import urllib.request as _debug_request; _debug_env=os.path.join(base_dir,'.dbg','multi-worker-locks.env'); _debug_url,_debug_session='http://127.0.0.1:7777/event','multi-worker-locks'; exec("try:\n with open(_debug_env) as _f: _debug_cfg=_f.read(); _debug_url=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SERVER_URL=')),_debug_url); _debug_session=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SESSION_ID=')),_debug_session)\nexcept Exception: pass\ntry:\n _debug_request.urlopen(_debug_request.Request(_debug_url,data=json.dumps({'sessionId':_debug_session,'runId':'post-fix','hypothesisId':'B','location':'test_eval.py:solution-end','msg':'[DEBUG] solution stage end','data':{'sample_id':sample_id,'container':current_docker_name,'solution_executed':solution_executed},'ts':int(time.time()*1000)}).encode(),headers={'Content-Type':'application/json'}),timeout=.2).read()\nexcept Exception: pass")
        # #endregion

        # An optimized workload replaces the verified workload only when the
        # baseline framework actually produced a non-empty rewrite.
        workload_for_recovery = verified_workload_path
        optimized_workload_used = False
        if os.path.exists(optimized_workload_path) :
            with open(optimized_workload_path, 'r') as f :
                optimized_workload = f.read().strip()
            if optimized_workload != "" :
                workload_for_recovery = optimized_workload_path
                optimized_workload_used = True

        # ---- STEP 6A: Measure the optimized workload after recovery ------
        logger.info(
            f"[EVAL][SAMPLE {i}] Executing workload "
            "[Optimized Performance] after recovery ..."
        )
        # #region debug-point B:recovery-workload-start
        import urllib.request as _debug_request; _debug_env=os.path.join(base_dir,'.dbg','multi-worker-locks.env'); _debug_url,_debug_session='http://127.0.0.1:7777/event','multi-worker-locks'; exec("try:\n with open(_debug_env) as _f: _debug_cfg=_f.read(); _debug_url=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SERVER_URL=')),_debug_url); _debug_session=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SESSION_ID=')),_debug_session)\nexcept Exception: pass\ntry:\n _debug_request.urlopen(_debug_request.Request(_debug_url,data=json.dumps({'sessionId':_debug_session,'runId':'post-fix','hypothesisId':'B','location':'test_eval.py:recovery-workload-start','msg':'[DEBUG] recovered workload start','data':{'sample_id':sample_id,'container':current_docker_name,'deadline_seconds':recovery_timeout_seconds},'ts':int(time.time()*1000)}).encode(),headers={'Content-Type':'application/json'}),timeout=.2).read()\nexcept Exception: pass")
        # #endregion
        optimized_recovered_eval = evaluate_verified_workload(
            initialization_workload_path, workload_for_recovery, db_config, logger,
            median_runs=median_runs,
            timeout_seconds=recovery_timeout_seconds,
            initialization_timeout_seconds=recovery_timeout_seconds,
        )
        # #region debug-point B:recovery-workload-end
        import urllib.request as _debug_request; _debug_env=os.path.join(base_dir,'.dbg','multi-worker-locks.env'); _debug_url,_debug_session='http://127.0.0.1:7777/event','multi-worker-locks'; exec("try:\n with open(_debug_env) as _f: _debug_cfg=_f.read(); _debug_url=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SERVER_URL=')),_debug_url); _debug_session=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SESSION_ID=')),_debug_session)\nexcept Exception: pass\ntry:\n _debug_request.urlopen(_debug_request.Request(_debug_url,data=json.dumps({'sessionId':_debug_session,'runId':'post-fix','hypothesisId':'B','location':'test_eval.py:recovery-workload-end','msg':'[DEBUG] recovered workload end','data':{'sample_id':sample_id,'container':current_docker_name,'timed_out':optimized_recovered_eval.get('timed_out',False)},'ts':int(time.time()*1000)}).encode(),headers={'Content-Type':'application/json'}),timeout=.2).read()\nexcept Exception: pass")
        # #endregion
        # Keep the legacy name and fields tied to Optimized Performance.
        recovered_eval = optimized_recovered_eval
        recovered_total_time = recovered_eval["total_time"]

        # ---- STEP 6B: Measure the original verified workload -------------
        # evaluate_verified_workload reapplies initialization_workload.sql
        # before every warm-up/measured run, isolating this measurement from
        # the optimized workload executed above.
        logger.info(
            f"[EVAL][SAMPLE {i}] Executing original verified workload "
            "[Original Performance] after recovery ..."
        )
        original_recovered_eval = evaluate_verified_workload(
            initialization_workload_path, verified_workload_path, db_config, logger,
            median_runs=median_runs,
            timeout_seconds=recovery_timeout_seconds,
            initialization_timeout_seconds=recovery_timeout_seconds,
        )

        # ---- STEP 7: Evaluate both recovered workloads -------------------
        baseline_error_count = baseline_eval.get("error_count", 0)
        recovered_error_count = recovered_eval.get("error_count", 0)
        additional_errors = recovered_error_count > baseline_error_count
        recovered_error_free = (
            bool(recovered_eval["success"])
            and all(
                run.get("error_count", 0) == 0
                for run in recovered_eval.get("measured_runs", [])
            )
        )
        result_comparison = compare_workload_evaluations(
            baseline_eval, recovered_eval, result_comparison_mode
        )
        results_equivalent = result_comparison["equivalent"]
        initialization_comparison = compare_initialization_errors(
            baseline_eval, recovered_eval
        )
        initialization_errors_compatible = initialization_comparison["compatible"]
        functional_gate = (
            recovered_error_free
            and results_equivalent
            and initialization_errors_compatible
        )
        recovery_failure_reasons = []
        if recovered_eval.get("timed_out"):
            recovery_failure_reasons.append(
                f"{recovered_eval.get('timeout_phase') or 'unknown'}_workload_timeout"
            )
        if not recovered_error_free:
            recovery_failure_reasons.append("recovered_user_workload_error")
        if not results_equivalent:
            recovery_failure_reasons.append("result_mismatch")
        if not initialization_errors_compatible:
            recovery_failure_reasons.append(
                "initialization_errors_incompatible"
            )

        if not recovered_error_free :
            logger.warning(
                f"[EVAL][SAMPLE {i}][WARNING] Recovered user workload was not "
                f"error-free across warm-up and measured runs."
            )
        if not results_equivalent :
            logger.warning(
                f"[EVAL][SAMPLE {i}][WARNING] User-workload results failed "
                f"`{result_comparison_mode}` equivalence checking."
            )
        if not initialization_errors_compatible :
            logger.warning(
                f"[EVAL][SAMPLE {i}][WARNING] Recovered initialization workload "
                f"introduced new/additional errors: {initialization_comparison}"
            )

        original_outcome = assess_recovery_performance(
            baseline_eval,
            original_recovered_eval,
            result_comparison_mode,
            diagnosis_judge_detail,
            timeout_times,
            mode,
        )
        if not original_outcome["recovered_error_free"]:
            logger.warning(
                f"[EVAL][SAMPLE {i}][Original Performance][WARNING] "
                "Recovered original workload was not error-free across "
                "warm-up and measured runs."
            )
        if not original_outcome["results_equivalent"]:
            logger.warning(
                f"[EVAL][SAMPLE {i}][Original Performance][WARNING] "
                f"Original workload results failed `{result_comparison_mode}` "
                "equivalence checking."
            )
        if not original_outcome["initialization_errors_compatible"]:
            logger.warning(
                f"[EVAL][SAMPLE {i}][Original Performance][WARNING] "
                "Recovered initialization workload introduced new/additional "
                f"errors: {original_outcome['initialization_comparison']}"
            )

        # ---- Execution-time classification (mutually exclusive buckets) ----
        # Exactly ONE of {over_bound, in_bound, optimized} is set per sample so
        # their rates always satisfy:
        #     over_bound_rate + in_bound_rate + optimized_rate == 1
        #
        # Let ratio = recovered_time / baseline_time. Functional correctness is
        # determined solely by the recovered user workload, initialization-error
        # compatibility and configured result-equivalence mode.
        execution_time_ratio = None
        if baseline_total_time > 0 and recovered_total_time is not None :
            execution_time_ratio = recovered_total_time / baseline_total_time

        if not functional_gate :
            record["over_bound"] = True
        elif execution_time_ratio is None :
            record["over_bound"] = True
        elif execution_time_ratio > timeout_times :
            record["over_bound"] = True
        elif execution_time_ratio <= 1 :
            record["optimized"] = True
        else :
            record["in_bound"] = True

        record["is_error"] = not recovered_error_free
        recovery_successful = functional_gate
        performance_comparison = build_performance_comparison(
            baseline_eval, recovered_eval
        )
        result_comparison_summary = compact_result_comparison(
            result_comparison
        )

        # Persist timing/error summaries plus deterministic result fingerprints.
        # Full result rows remain in memory and are not duplicated into JSON.
        workload_summary = {
            "baseline" : summarize_workload_run(baseline_eval),
            # Backward-compatible aliases refer to Optimized Performance.
            "recovered" : summarize_workload_run(recovered_eval),
            "performance_comparison": performance_comparison,
            "results_equivalent" : results_equivalent,
            "result_comparison": result_comparison_summary,
            "initialization_comparison": initialization_comparison,
            "baseline_error_count" : baseline_error_count,
            "recovered_error_count" : recovered_error_count,
            "additional_errors" : additional_errors,
            "recovered_error_free": recovered_error_free,
            "functional_gate": functional_gate,
            "recovery_failure_reasons": recovery_failure_reasons,
            "recovery_successful" : recovery_successful,
            "execution_time_ratio" : execution_time_ratio,
            "optimized_workload_used" : optimized_workload_used,
            "optimized_recovered": summarize_workload_run(recovered_eval),
            "original_recovered": summarize_workload_run(
                original_recovered_eval
            ),
            "optimized_performance_comparison": performance_comparison,
            "original_performance_comparison": original_outcome[
                "performance_comparison"
            ],
            "optimized_result_comparison": result_comparison_summary,
            "original_result_comparison": original_outcome[
                "result_comparison_summary"
            ],
            "optimized_initialization_comparison": initialization_comparison,
            "original_initialization_comparison": original_outcome[
                "initialization_comparison"
            ],
            "optimized_functional_gate": functional_gate,
            "original_functional_gate": original_outcome["functional_gate"],
            "optimized_recovery_failure_reasons": recovery_failure_reasons,
            "original_recovery_failure_reasons": original_outcome[
                "failure_reasons"
            ],
        }
        with open(os.path.join(output_sub_dir, "workload_execution_summary.json"), 'w') as f :
            json.dump(workload_summary, f, indent=4)
        comparison_detail = {
            "sample_id": sample_id,
            "comparison_mode": result_comparison_mode,
            # Backward-compatible aliases refer to Optimized Performance.
            "results_equivalent": results_equivalent,
            "result_comparison": result_comparison,
            "initialization_comparison": initialization_comparison,
            "performance_comparison": performance_comparison,
            "optimized_performance": {
                "results_equivalent": results_equivalent,
                "result_comparison": result_comparison,
                "initialization_comparison": initialization_comparison,
                "performance_comparison": performance_comparison,
            },
            "original_performance": {
                "results_equivalent": original_outcome[
                    "results_equivalent"
                ],
                "result_comparison": original_outcome["result_comparison"],
                "initialization_comparison": original_outcome[
                    "initialization_comparison"
                ],
                "performance_comparison": original_outcome[
                    "performance_comparison"
                ],
            },
        }
        with open(
            os.path.join(
                output_sub_dir,
                "workload_result_comparison_detail.json",
            ),
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                comparison_detail,
                file,
                indent=4,
                ensure_ascii=False,
            )

        score_detail = compute_sample_score(
            diagnosis_judge_detail,
            functional_gate,
            baseline_total_time,
            recovered_total_time,
            baseline_eval.get("measured_user_times"),
            timeout_times,
            mode=mode,
            failure_reasons=recovery_failure_reasons,
        )
        original_score_detail = original_outcome["score_detail"]
        record["score"] = score_detail["score"]
        sample_detail = {
            "sample_id": sample_id,
            "mode": mode,
            "status": "completed",
            "timed_out": recovered_eval.get("timed_out", False),
            "recovery_timeout_seconds": recovery_timeout_seconds,
            "initialization_timeout_seconds": recovered_eval.get(
                "initialization_timeout_seconds", recovery_timeout_seconds
            ),
            "baseline_initialization_timeout_seconds": baseline_eval.get(
                "initialization_timeout_seconds",
                db_config["initialization_workload_timeout_seconds"],
            ),
            "recovery_initialization_timeout_seconds": recovered_eval.get(
                "initialization_timeout_seconds", recovery_timeout_seconds
            ),
            "client_encoding": db_config["client_encoding"] if normalize_dbms(DBMS) != "mysql" else None,
            "database_name": database_name,
            "dbms": DBMS,
            "score": score_detail["score"],
            "diagnosis_score": score_detail["diagnosis_score"],
            # Existing score fields remain Optimized Performance aliases.
            "recovery_score": score_detail["recovery_score"],
            "artifact_score": score_detail["diagnosis_score"],
            "performance_score": score_detail["performance_score"],
            "recovery_score_detail": score_detail["recovery_detail"],
            "recovery_failure_reasons": score_detail["recovery_detail"][
                "zero_reasons"
            ],
            "performance_comparison": performance_comparison,
            "recovery_successful": recovery_successful,
            "additional_errors": additional_errors,
            "recovered_error_free": recovered_error_free,
            "baseline_error_count": baseline_error_count,
            "recovered_error_count": recovered_error_count,
            "results_equivalent": results_equivalent,
            "result_comparison": result_comparison_summary,
            "initialization_comparison": initialization_comparison,
            "functional_gate": functional_gate,
            "solution_executed": solution_executed,
            "solution_script_exists": solution_script_exists,
            "injection_ready_pattern": injection_ready_pattern,
            "optimized_workload_used": optimized_workload_used,
            "optimized": record["optimized"],
            "execution_time_ratio": execution_time_ratio,
            "baseline_total_time": baseline_total_time,
            "recovered_total_time": recovered_total_time,
            "optimized_score": score_detail["score"],
            "original_score": original_score_detail["score"],
            "optimized_performance_score": score_detail[
                "recovery_score"
            ],
            "original_performance_score": original_score_detail[
                "recovery_score"
            ],
            "optimized_recovery_score_detail": score_detail[
                "recovery_detail"
            ],
            "original_recovery_score_detail": original_score_detail[
                "recovery_detail"
            ],
            "optimized_recovered_total_time": recovered_total_time,
            "original_recovered_total_time": original_outcome[
                "recovered_total_time"
            ],
            "optimized_functional_gate": functional_gate,
            "original_functional_gate": original_outcome["functional_gate"],
            "optimized_results_equivalent": results_equivalent,
            "original_results_equivalent": original_outcome[
                "results_equivalent"
            ],
            "optimized_performance_comparison": performance_comparison,
            "original_performance_comparison": original_outcome[
                "performance_comparison"
            ],
            "optimized_recovery_failure_reasons": recovery_failure_reasons,
            "original_recovery_failure_reasons": original_outcome[
                "failure_reasons"
            ],
            "solution_directory": solution_paths["directory"],
        }
        record["sample_detail"] = sample_detail
        with open(os.path.join(output_sub_dir, "score_detail.json"), 'w') as f :
            json.dump(sample_detail, f, indent=4)

        # Emit the finished sample's score immediately so progress is visible in
        # real time (especially under parallel execution, where samples finish
        # out of order). The sample_id + index tag disambiguates concurrent logs.
        logger.info(
            f"[EVAL][SAMPLE {i}][SCORE][Optimized Performance] "
            f"sample_id=`{sample_id}` db=`{database_name}` dbms=`{DBMS}` "
            f"score={score_detail['score']:.6f} "
            f"(recovery={score_detail['recovery_score']:.6f}, "
            f"diagnosis={score_detail['diagnosis_score']}, "
            f"performance={score_detail['performance_score']:.6f}, "
            f"functional_gate={functional_gate})"
        )
        logger.info(
            f"[EVAL][SAMPLE {i}][SCORE][Original Performance] "
            f"sample_id=`{sample_id}` db=`{database_name}` dbms=`{DBMS}` "
            f"score={original_score_detail['score']:.6f} "
            f"(recovery={original_score_detail['recovery_score']:.6f}, "
            f"diagnosis={original_score_detail['diagnosis_score']}, "
            f"performance={original_score_detail['performance_score']:.6f}, "
            f"functional_gate={original_outcome['functional_gate']})"
        )

    except Exception as exc :
        # A single unrecoverable sample must not abort the whole evaluation
        # run; record it and move on so the remaining samples still complete.
        record_evaluation_abort(record, mode, output_sub_dir, exc, logger)
    finally :
        # Reap this sample's final container + tmux session. Names are never
        # reused, so leftover containers would otherwise accumulate under load.
        # #region debug-point D:cleanup-start
        import urllib.request as _debug_request; _debug_env=os.path.join(base_dir,'.dbg','multi-worker-locks.env'); _debug_url,_debug_session='http://127.0.0.1:7777/event','multi-worker-locks'; exec("try:\n with open(_debug_env) as _f: _debug_cfg=_f.read(); _debug_url=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SERVER_URL=')),_debug_url); _debug_session=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SESSION_ID=')),_debug_session)\nexcept Exception: pass\ntry:\n _debug_request.urlopen(_debug_request.Request(_debug_url,data=json.dumps({'sessionId':_debug_session,'runId':'post-fix','hypothesisId':'D','location':'test_eval.py:cleanup-start','msg':'[DEBUG] sample cleanup start','data':{'sample_id':sample_id,'container':current_docker_name},'ts':int(time.time()*1000)}).encode(),headers={'Content-Type':'application/json'}),timeout=.2).read()\nexcept Exception: pass")
        # #endregion
        if current_docker_name is not None :
            try:
                cleanup_evaluation_container(current_docker_name)
            except Exception as exc:
                record_evaluation_abort(record, mode, output_sub_dir, exc, logger)
        # #region debug-point D:cleanup-end
        import urllib.request as _debug_request; _debug_env=os.path.join(base_dir,'.dbg','multi-worker-locks.env'); _debug_url,_debug_session='http://127.0.0.1:7777/event','multi-worker-locks'; exec("try:\n with open(_debug_env) as _f: _debug_cfg=_f.read(); _debug_url=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SERVER_URL=')),_debug_url); _debug_session=next((_l.split('=',1)[1] for _l in _debug_cfg.splitlines() if _l.startswith('DEBUG_SESSION_ID=')),_debug_session)\nexcept Exception: pass\ntry:\n _debug_request.urlopen(_debug_request.Request(_debug_url,data=json.dumps({'sessionId':_debug_session,'runId':'post-fix','hypothesisId':'D','location':'test_eval.py:cleanup-end','msg':'[DEBUG] sample cleanup end','data':{'sample_id':sample_id,'container':current_docker_name},'ts':int(time.time()*1000)}).encode(),headers={'Content-Type':'application/json'}),timeout=.2).read()\nexcept Exception: pass")
        # #endregion

    return record


def original_workload_execution_performance_eval(config, logger, median_runs=1, timeout_times=8, parallelism_num=1) :
    mode = evaluation_mode(config)
    median_runs = max(1, int(median_runs))
    # Run-level lines are not tied to any single sample; tag them with the
    # `ALL` sentinel so they still follow the uniform `[Sample ID: X]` format.
    logger = _sample_logger(logger, RUN_LEVEL_SAMPLE_ID)
    logger.info(f"[EVAL] Starting Solution Evaluation with Median Runs `{median_runs}`, Timeout Times `{timeout_times}`, Parallelism `{parallelism_num}` ...")

    # Each verified-workload measurement runs one warm-up pass followed by
    # `median_runs` timed iterations (median taken). `median_runs` defaults to 1
    # and may be raised via config to average out latency noise; every iteration
    # first re-applies the initialization workload so repeated runs stay well
    # defined.
    if not math.isfinite(timeout_times) or float(timeout_times) != RECOVERY_ZERO_SCORE_RATIO:
        raise ValueError(
            f"timeout_times must be exactly {RECOVERY_ZERO_SCORE_RATIO:g}; "
            "it is the fixed recovery-score zero boundary."
        )
    parallelism_num = max(1, int(parallelism_num))
    result_comparison_mode = str(
        config.get("result_comparison_mode", DEFAULT_RESULT_COMPARISON_MODE)
    ).strip().lower()
    if result_comparison_mode not in RESULT_COMPARISON_MODES:
        raise ValueError(
            f"result_comparison_mode must be one of {sorted(RESULT_COMPARISON_MODES)}."
        )
    if mode != "report" and result_comparison_mode == "strict" and median_runs < 2:
        raise ValueError(
            "strict result comparison requires median_runs >= 2 so repeated "
            "baseline and recovered results can be checked for consistency."
        )

    anomaly_pairs = config['anomaly_pairs']
    docker_name = config.get('docker_name', 'evaluation')
    host_port = config.get('host_port', 5426)
    baseline_workload_timeout_seconds = config.get(
        "baseline_workload_timeout_seconds",
        DEFAULT_BASELINE_WORKLOAD_TIMEOUT_SECONDS,
    )
    sample_retry_times = config.get(
        "sample_retry_times",
        DEFAULT_SAMPLE_RETRY_TIMES,
    )
    initialization_workload_timeout_seconds = config.get(
        "initialization_workload_timeout_seconds",
        DEFAULT_INITIALIZATION_WORKLOAD_TIMEOUT_SECONDS,
    )
    if (
        isinstance(initialization_workload_timeout_seconds, bool)
        or not isinstance(initialization_workload_timeout_seconds, (int, float))
        or not math.isfinite(initialization_workload_timeout_seconds)
        or initialization_workload_timeout_seconds <= 0
    ):
        raise ValueError("initialization_workload_timeout_seconds must be positive and finite.")
    if (
        isinstance(baseline_workload_timeout_seconds, bool)
        or not isinstance(baseline_workload_timeout_seconds, (int, float))
        or not math.isfinite(baseline_workload_timeout_seconds)
        or baseline_workload_timeout_seconds <= 0
    ):
        raise ValueError(
            "baseline_workload_timeout_seconds must be a positive finite number."
        )
    if (
        isinstance(sample_retry_times, bool)
        or not isinstance(sample_retry_times, int)
        or sample_retry_times < 0
    ):
        raise ValueError("sample_retry_times must be a non-negative integer.")
    logger.info(
        f"[EVAL][GUARD] Baseline workload deadline="
        f"{float(baseline_workload_timeout_seconds):.3f}s for user workload; "
        f"initialization deadline={initialization_workload_timeout_seconds:.3f}s; "
        f"aborted-sample retries={sample_retry_times}."
    )

    shared = {
        "mode": mode,
        "_cleanup_worker_prefix": True,
        "initialization_workload_timeout_seconds": initialization_workload_timeout_seconds,
        'anomaly_directory' : config['anomaly_directory'],
        'solution_script_directory' : config['solution_script_directory'],
        'output_base_dir' : config['output_base_dir'],
        'base_dir' : config['base_dir'],
        'result_comparison_mode' : result_comparison_mode,
        'ground_truth_diagnosis' : config.get(
            'ground_truth_diagnosis', DEFAULT_GROUND_TRUTH_DIAGNOSIS
        ),
        'judge_llm_config' : (
            resolve_judge_llm_configuration(config) if mode != "solution" else None
        ),
        'judge_repetitions' : config.get(
            'judge_repetitions', DEFAULT_JUDGE_REPETITIONS
        ),
        'baseline_workload_timeout_seconds': float(
            baseline_workload_timeout_seconds
        ),
        'superuser_config': dict(
            config.get('superuser_config') or DEFAULT_SUPERUSER_CONFIG
        ),
    }

    # Each worker owns a stable SLOT: a distinct container-name prefix
    # (`{docker_name}_w{slot}`) plus a distinct host-port range. Slots are handed
    # out via a queue so at most `parallelism_num` samples run concurrently, and
    # a released slot (its prefix + port) is reused by the next queued sample.
    slot_queue = queue_module.Queue()
    for slot in range(parallelism_num) :
        slot_queue.put(slot)

    def worker(i, anomaly_pair) :
        slot = slot_queue.get()
        try :
            base_docker_name = f"{docker_name}_w{slot}"
            # Reserve a small per-worker port window so a worker's baseline and
            # pre-injection containers (and any staggered neighbours) never map
            # to the same published host port.
            worker_host_port = host_port + slot
            record = None
            for attempt in range(sample_retry_times + 1):
                record = evaluate_single_sample(
                    i, anomaly_pair, shared, base_docker_name, worker_host_port,
                    logger, timeout_times, median_runs=median_runs,
                )
                if not record["is_aborted"]:
                    return record
                if attempt < sample_retry_times:
                    logger.warning(
                        f"[EVAL][SAMPLE {i}][RETRY] sample_id="
                        f"`{anomaly_sample_id(anomaly_pair)}` aborted; retrying "
                        f"attempt `{attempt + 2}/{sample_retry_times + 1}`."
                    )
            return record
        finally :
            slot_queue.put(slot)

    records = [None] * len(anomaly_pairs)
    pending_samples = []
    resumed_sample_num = 0
    for i, anomaly_pair in enumerate(anomaly_pairs):
        sample_id = anomaly_sample_id(anomaly_pair)
        terminal_record = load_terminal_evaluation_record(
            config["output_base_dir"], sample_id, mode, timeout_times,
            solution_script_directory=config["solution_script_directory"],
            dbms=anomaly_pair.get("dbms"),
        )
        if terminal_record is not None:
            records[i] = terminal_record
            resumed_sample_num += 1
            logger.info(
                f"[EVAL][RESUME] sample_id=`{sample_id}` status="
                f"`{terminal_record['sample_detail']['status']}` already complete; "
                "skipping evaluation."
            )
            continue

        sample_output = os.path.join(config["output_base_dir"], sample_id)
        if os.path.lexists(sample_output):
            logger.warning(
                f"[EVAL][RESUME] sample_id=`{sample_id}` has incomplete or invalid "
                "output; removing it before evaluation."
            )
            remove_incomplete_evaluation_output(
                config["output_base_dir"], sample_id
            )
        pending_samples.append((i, anomaly_pair))

    logger.info(
        f"[EVAL][RESUME] Restored `{resumed_sample_num}` terminal sample(s); "
        f"scheduled `{len(pending_samples)}` unfinished sample(s)."
    )
    if parallelism_num == 1 :
        for i, anomaly_pair in tqdm(
            pending_samples,
            total=len(anomaly_pairs),
            initial=resumed_sample_num,
        ):
            records[i] = worker(i, anomaly_pair)
    else :
        with ThreadPoolExecutor(max_workers=parallelism_num) as executor :
            future_to_index = {
                executor.submit(worker, i, anomaly_pair): i
                for i, anomaly_pair in pending_samples
            }
            for future in tqdm(
                as_completed(future_to_index),
                total=len(anomaly_pairs),
                initial=resumed_sample_num,
            ):
                index = future_to_index[future]
                records[index] = future.result()

    # ---- Aggregate the per-sample records into the run-level summary --------
    pass_list = []
    original_score_list = []
    diagnosis_score_list = []
    recovery_score_list = []
    original_performance_score_list = []
    sample_details = []
    error_sample_num = 0
    over_bound_sample_num = 0
    in_bound_sample_num = 0
    optimized_sample_num = 0
    solution_missing_num = 0
    solution_execution_error_num = 0
    aborted_sample_num = 0
    not_evaluated_sample_num = 0
    for record in records :
        if record is None :
            continue
        if record.get("is_not_evaluated", False):
            not_evaluated_sample_num += 1
            if record["sample_detail"] is not None:
                sample_details.append(record["sample_detail"])
            continue
        if record["is_error"] :
            error_sample_num += 1
        if record["is_aborted"] :
            aborted_sample_num += 1
            continue
        if record["over_bound"] :
            over_bound_sample_num += 1
        if record["in_bound"] :
            in_bound_sample_num += 1
        if record["optimized"] :
            optimized_sample_num += 1
        if record["solution_missing"] :
            solution_missing_num += 1
        if record["solution_execution_error"] :
            solution_execution_error_num += 1
        if record["score"] is not None :
            pass_list.append(record["score"])
        if record["sample_detail"] is not None :
            sample_details.append(record["sample_detail"])
            if record["sample_detail"]["diagnosis_score"] is not None:
                diagnosis_score_list.append(record["sample_detail"]["diagnosis_score"])
            if record["sample_detail"]["recovery_score"] is not None:
                recovery_score_list.append(record["sample_detail"]["recovery_score"])
            if record["sample_detail"].get("original_score") is not None:
                original_score_list.append(
                    record["sample_detail"]["original_score"]
                )
            if (
                record["sample_detail"].get("original_performance_score")
                is not None
            ):
                original_performance_score_list.append(
                    record["sample_detail"]["original_performance_score"]
                )

    total_sample_num = len(pass_list)
    overall_score = round_score(
        sum(pass_list) / total_sample_num if total_sample_num > 0 else None
    )
    overall_diagnosis_score = round_score(
        sum(diagnosis_score_list) / len(diagnosis_score_list)
        if diagnosis_score_list else None
    )
    overall_recovery_score = round_score(
        sum(recovery_score_list) / len(recovery_score_list)
        if recovery_score_list else None
    )
    overall_original_score = round_score(
        sum(original_score_list) / len(original_score_list)
        if original_score_list else None
    )
    overall_original_performance_score = round_score(
        sum(original_performance_score_list)
        / len(original_performance_score_list)
        if original_performance_score_list else None
    )
    attempted_sample_num = len(anomaly_pairs) - not_evaluated_sample_num
    error_ratio = (
        error_sample_num / attempted_sample_num
        if attempted_sample_num > 0 else 0
    )
    over_bound_ratio = over_bound_sample_num / total_sample_num if total_sample_num > 0 else 0
    in_bound_ratio = in_bound_sample_num / total_sample_num if total_sample_num > 0 else 0
    optimized_ratio = optimized_sample_num / total_sample_num if total_sample_num > 0 else 0

    return {
        "mode": mode,
        "total_sample_num" : total_sample_num,
        "attempted_sample_num": attempted_sample_num,
        "initialization_workload_timeout_seconds": initialization_workload_timeout_seconds,
        "overall_score" : overall_score,
        "overall_optimized_score": overall_score,
        "overall_original_score": overall_original_score,
        "overall_diagnosis_score" : overall_diagnosis_score,
        "overall_recovery_score" : overall_recovery_score,
        "overall_optimized_performance_score": overall_recovery_score,
        "overall_original_performance_score": (
            overall_original_performance_score
        ),
        "error_sample_num" : error_sample_num,
        "error_ratio" : error_ratio,
        "aborted_sample_num" : aborted_sample_num,
        "not_evaluated_sample_num": not_evaluated_sample_num,
        "over_bound_sample_num" : over_bound_sample_num,
        "over_bound_ratio" : over_bound_ratio,
        "in_bound_sample_num" : in_bound_sample_num,
        "in_bound_ratio" : in_bound_ratio,
        "optimized_sample_num" : optimized_sample_num,
        "optimized_ratio" : optimized_ratio,
        "solution_missing_num" : solution_missing_num,
        "solution_execution_error_num" : solution_execution_error_num,
        "median_runs" : median_runs,
        "timeout_times" : timeout_times,
        "baseline_workload_timeout_seconds": float(
            baseline_workload_timeout_seconds
        ),
        "sample_retry_times": sample_retry_times,
        "result_comparison_mode" : result_comparison_mode,
        "ground_truth_diagnosis" : shared["ground_truth_diagnosis"],
        "judge_repetitions" : shared["judge_repetitions"],
        "pass_list" : pass_list,
        "optimized_score_list": pass_list,
        "original_score_list": original_score_list,
        "diagnosis_score_list" : diagnosis_score_list,
        "recovery_score_list" : recovery_score_list,
        "optimized_performance_score_list": recovery_score_list,
        "original_performance_score_list": original_performance_score_list,
        "sample_details" : sample_details,
    }


if __name__ == "__main__" :
    base_dir = os.path.dirname(os.path.realpath(__file__))

    parser = argparse.ArgumentParser(description='Database Diagnosis for Benchmark Evaluation.')
    parser.add_argument('--config', type=str, required=False, default=os.path.join(base_dir, "Configs", "eval", "test_eval.json"), help='Path to the configuration JSON file')
    args = parser.parse_args()
    # Resolve every path through the symlink-aware helper: the container
    # bind-mounts the canonical host path, so paths handed to `docker exec` must
    # be canonical too (this is the fix for the "No such file" error that occurs
    # when a symlinked alias is passed instead).
    config_path = resolve_path_from_base(args.config, base_dir)
    with open(config_path, 'r') as file:
        config = json.load(file)
    MODE = evaluation_mode(config)
    judge_config_input = dict(config)
    judge_config_input["base_dir"] = base_dir
    JUDGE_LLM_CONFIGURATION = resolve_judge_llm_configuration(
        judge_config_input
    ) if MODE != "solution" else None

    key_information_path = resolve_path_from_base(config['key_information_path'], base_dir)
    anomaly_directory = os.path.dirname(key_information_path)

    with open(key_information_path, 'r') as f :
        anomaly_pairs = json.load(f)

    solution_script_directory = resolve_path_from_base(config['solution_script_directory'], base_dir)

    # The evaluation config describes ONLY the evaluation run itself. DBMS,
    # hardware and database-user settings are read from each sample directory's
    # config.json by load_sample_config().
    DOCKER_NAME = config.get('docker_name', "evaluation")
    HOST_PORT = config.get('host_port', 5426)
    MEDIAN_RUNS = config.get('median_runs', 1)
    TIMEOUT_TIMES = config.get('timeout_times', RECOVERY_ZERO_SCORE_RATIO)
    # Number of samples to evaluate concurrently. Each worker owns a distinct
    # container-name prefix + host-port slot, so parallel Docker containers never
    # clash. Defaults to 1 (sequential) so an absent field is a no-op.
    PARALLELISM_NUM = config.get('parallelism_num', 1)

    output_base_dir = resolve_path_from_base(config.get('output_base_dir', "Output"), base_dir)
    os.makedirs(output_base_dir, exist_ok=True)
    with open(os.path.join(output_base_dir, "config.json"), 'w') as file :
        json.dump(config, file, indent=4)

    log_path = os.path.join(output_base_dir, "log.txt")
    logger = set_up_logger(log_path)

    logger = logging.getLogger(f"DB_DIAG.{__name__}")
    # Wrap the run-level logger so the MAIN banner and the final summary follow
    # the same uniform `[Sample ID: X]` line format as the per-sample logs; the
    # `ALL` sentinel marks lines that span the whole run rather than one sample.
    logger = _sample_logger(logger, RUN_LEVEL_SAMPLE_ID)
    with open(__file__, "rb") as source_file:
        evaluator_source_sha256 = hashlib.sha256(source_file.read()).hexdigest()
    logger.info("[MAIN][Database Diagnosis for Benchmark Evaluation] == START ==")
    logger.info(
        f"[MAIN] Evaluator Source SHA-256: {evaluator_source_sha256}"
    )
    logger.info(f"[MAIN] Current Anomaly Descriptions Length: {len(anomaly_pairs)}")
    logger.info(f"[MAIN] Current Solution Directory: {solution_script_directory}")

    owe_eval_config = {
        "mode": MODE,
        "initialization_workload_timeout_seconds": config.get(
            "initialization_workload_timeout_seconds",
            DEFAULT_INITIALIZATION_WORKLOAD_TIMEOUT_SECONDS,
        ),
        'anomaly_pairs' : anomaly_pairs,
        'anomaly_directory' : anomaly_directory,
        'solution_script_directory' : solution_script_directory,
        'output_base_dir' : output_base_dir,
        'docker_name' : DOCKER_NAME,
        'host_port' : HOST_PORT,
        'base_dir' : base_dir,
        'result_comparison_mode' : config.get(
            'result_comparison_mode', DEFAULT_RESULT_COMPARISON_MODE
        ),
        'ground_truth_diagnosis' : config.get(
            'ground_truth_diagnosis', DEFAULT_GROUND_TRUTH_DIAGNOSIS
        ),
        'judge_llm_configuration' : JUDGE_LLM_CONFIGURATION,
        'judge_repetitions' : config.get(
            'judge_repetitions', DEFAULT_JUDGE_REPETITIONS
        ),
        'baseline_workload_timeout_seconds': config.get(
            'baseline_workload_timeout_seconds',
            DEFAULT_BASELINE_WORKLOAD_TIMEOUT_SECONDS,
        ),
        'sample_retry_times': config.get(
            'sample_retry_times',
            DEFAULT_SAMPLE_RETRY_TIMES,
        ),
        'superuser_config': dict(
            config.get('superuser_config') or DEFAULT_SUPERUSER_CONFIG
        ),
    }
    owe_eval_result = original_workload_execution_performance_eval(
        owe_eval_config,
        logger,
        median_runs=MEDIAN_RUNS,
        timeout_times=TIMEOUT_TIMES,
        parallelism_num=PARALLELISM_NUM,
    )
    owe_eval_result["evaluator_source_sha256"] = evaluator_source_sha256
    with open(os.path.join(output_base_dir, "evaluation_result.json"), 'w') as file :
        json.dump(owe_eval_result, file, indent=4)

    # ----- Final run-level summary --------------------------------------------
    # `total_sample_num` is the number of SCORED (non-aborted) samples. The three
    # execution-time buckets partition exactly these scored samples, so:
    #     Over-Bound Rate + In-Bound Rate + Performance-Optimization Rate == 100%
    # Functional failure includes any recovered user-workload error, result
    # mismatch, or new/additional initialization-workload error.
    total_sample_num = owe_eval_result["total_sample_num"]
    timeout_times = owe_eval_result["timeout_times"]
    logger.info(f'[MAIN][EVAL] Median Runs Per Measurement : {owe_eval_result["median_runs"]} (1 warm-up + N measured runs; user-workload time is authoritative)')
    logger.info(
        f'[MAIN][EVAL][Optimized Performance] Overall Score : '
        f'{owe_eval_result["overall_optimized_score"]:.6f} '
        if owe_eval_result["overall_score"] is not None
        else '[MAIN][EVAL][Optimized Performance] Overall Score : N/A'
    )
    logger.info(
        f'[MAIN][EVAL][Original Performance] Overall Score : '
        f'{owe_eval_result["overall_original_score"]:.6f} '
        if owe_eval_result["overall_original_score"] is not None
        else '[MAIN][EVAL][Original Performance] Overall Score : N/A'
    )
    logger.info(f'[MAIN][EVAL] Mode : {MODE}')
    logger.info(
        f'[MAIN][EVAL] Overall Diagnosis Score : '
        f'{owe_eval_result["overall_diagnosis_score"]:.6f}'
        if owe_eval_result["overall_diagnosis_score"] is not None
        else '[MAIN][EVAL] Overall Diagnosis Score : N/A'
    )
    logger.info(
        f'[MAIN][EVAL][Optimized Performance] Overall Recovery Score : '
        f'{owe_eval_result["overall_optimized_performance_score"]:.6f}'
        if owe_eval_result["overall_recovery_score"] is not None
        else '[MAIN][EVAL][Optimized Performance] Overall Recovery Score : N/A'
    )
    logger.info(
        f'[MAIN][EVAL][Original Performance] Overall Recovery Score : '
        f'{owe_eval_result["overall_original_performance_score"]:.6f}'
        if owe_eval_result["overall_original_performance_score"] is not None
        else '[MAIN][EVAL][Original Performance] Overall Recovery Score : N/A'
    )
    logger.info(
        f'[MAIN][EVAL] Scored Samples : {total_sample_num} / {len(anomaly_pairs)} '
        f'(excludes {owe_eval_result["aborted_sample_num"]} aborted and '
        f'{owe_eval_result["not_evaluated_sample_num"]} not evaluated)'
    )
    logger.info(
        f'[MAIN][EVAL] Not Evaluated Samples : '
        f'{owe_eval_result["not_evaluated_sample_num"]} '
        '(no diagnosis or recovery artifact; excluded from averages)'
    )
    logger.info(f'[MAIN][EVAL] Result Comparison Mode : {owe_eval_result["result_comparison_mode"]}')
    logger.info(f'[MAIN][EVAL] Ground Truth Diagnosis : {owe_eval_result["ground_truth_diagnosis"]}')
    logger.info(
        f'[MAIN][EVAL] User-Workload Error Rate : '
        f'{owe_eval_result["error_sample_num"]} / '
        f'{owe_eval_result["attempted_sample_num"]} '
        f'({round((owe_eval_result["error_ratio"] * 100), 2)} %) '
        '[not-evaluated samples excluded]'
    )
    logger.info(f'[MAIN][EVAL] Aborted Samples : {owe_eval_result["aborted_sample_num"]} (evaluation could not complete; excluded from the rates below)')
    logger.info(f'[MAIN][EVAL] Over Bound Execution Time Rate : {owe_eval_result["over_bound_sample_num"]} / {total_sample_num} ({round((owe_eval_result["over_bound_ratio"] * 100), 2)} %) [functional gate failed, or recovered time > {timeout_times}x baseline]')
    logger.info(f"[MAIN][EVAL] In Bound Execution Time Rate : {owe_eval_result['in_bound_sample_num']} / {total_sample_num} ({round((owe_eval_result['in_bound_ratio'] * 100), 2)} %) [functional gate passed and recovered within {timeout_times}x baseline, but slower than baseline]")
    logger.info(f'[MAIN][EVAL] Performance Optimization Rate : {owe_eval_result["optimized_sample_num"]} / {total_sample_num} ({round((owe_eval_result["optimized_ratio"] * 100), 2)} %) [ALL user SQL error-free AND results match baseline AND recovered time <= baseline time]')
    logger.info(f'[MAIN][EVAL] (Performance buckets apply only to both/solution modes)')
    logger.info(f'[MAIN][EVAL] Missing Solution Scripts : {owe_eval_result["solution_missing_num"]}')
    logger.info(f'[MAIN][EVAL] Solution Execution Errors : {owe_eval_result["solution_execution_error_num"]}')
    logger.info(f'[MAIN][EVAL] Evaluation Result Saved To : {os.path.join(output_base_dir, "evaluation_result.json")}')
