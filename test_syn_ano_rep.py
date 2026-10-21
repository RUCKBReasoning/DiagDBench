import openai
import json
import os
import time
import shutil
import re
import shlex
from tqdm import tqdm
import argparse
import logging

from prompt_template import *
from utils import *
from llm_api import *
from docker_func import *

# python test_syn_ano_rep.py --config ./Configs/syn_ano_rep/example_mysql.json
#
# Online Diagnosis Incident Anomaly REPRODUCTION synthesizer.
#
# This entry point mirrors the design of `test_syn_ano.py` (unique-named Docker
# containers, tmux-based in-container execution, ReAct multi-turn LLM inference,
# per-sample output directory, defensive container sweeps) but drives the whole
# pipeline from REAL Online Diagnosis Incident records instead of pre-synthesized anomaly
# definitions. For every record the LLM runs a single, unified multi-action
# ReAct loop (sql / bash / python exploration, then a `finish`) that produces the
# seven LLM-authored artifacts; the harness then deterministically derives
# `merged_workload.sql`, `merged_workload_performance.json`, and copies the
# reproduction-oriented `anomaly_eval_rep.py` in as `anomaly_eval.py`, yielding
# the exact same 10-file sample layout that `test_extract_key_info.py` expects.
#
# NOTE: This file intentionally reuses `docker_func.py`, `llm_api.py`,
# `utils.py`, and `prompt_template.py` WITHOUT modifying them, and it never
# touches the existing anomaly-synthesis code paths.

openai.api_key = os.environ.get("OPENAI_API_KEY", "")
openai.base_url = os.environ.get("OPENAI_BASE_URL", "https://api.example.com/v1")
model_name = os.environ.get("LLM_MODEL", "your-chat-model")
temperature = 1

GB_Configurations = ["Memory", "Memory + Swap", "Storage"]

LOG_NAME = os.path.basename(__file__).split('.')[0]

# The reproduction pipeline targets a single OLAP instance (tpch_1) by default;
# these timeouts mirror `test_syn_ano.py` so DB initialization behaves the same.
TIMEOUTS = {'tpch_1': 1500, 'imdb': 4500, 'tpcds_1': 1800, 'tpch_10': 11800}
RETRY_TIMES = 10
FAKE_DATABASE_IP = "8888.8888.8888.8888"

# Encodings tried when reading a single Incident record CSV (the filtered samples
# are written as utf-8-sig by the preprocessing step, but stay tolerant).
ENCODINGS_TO_TRY = ["utf-8-sig", "utf-8", "gbk", "gb2312", "latin-1"]


def resolve_fast_reset_enabled(dbms_kind, config):
    """Match the established synthesis/revision fast-reset policy."""
    requested = bool(config.get("fast_reset_enabled", True))
    if dbms_kind != "mysql":
        return requested
    return requested and bool(
        config.get("mysql_fast_reset_experimental_enabled", False)
    )


# The subset of Incident CSV columns surfaced to the LLM, with readable labels.
INCIDENT_FIELD_LABELS = [
    ("name", "Record Title"),
    ("level", "Severity Level"),
    ("LLM预测表象分类", "Surface Symptom Category (表象分类)"),
    ("LLM预测根因分类", "Root Cause Category (根因分类)"),
    ("LLM预测类别分析", "Category Analysis (类别分析)"),
    ("责任方", "Responsible Party (责任方)"),
    ("summary", "Incident Summary (诊断事件摘要)"),
    ("category_by_customer", "Customer Impact (客户影响分类)"),
    ("fault_locator_msg", "Diagnostic Evidence (定位证据)"),
    ("msg", "Reported Symptom (原始问题片段)"),
]


def read_incident_sample_csv(path):
    """Read a single-row Incident record CSV into an ordered dict of column->value.

    The filtered samples produced by ``preprocess_incident_samples.py`` contain a
    header row plus one data row. Encoding is auto-detected with a small
    fallback list so a stray non-utf8 export does not abort the whole run.
    """
    import csv

    last_error = None
    for encoding in ENCODINGS_TO_TRY:
        try:
            with open(path, "r", encoding=encoding, newline="") as fh:
                reader = csv.DictReader(fh)
                rows = list(reader)
            if not rows:
                raise ValueError(f"Incident sample CSV has no data row: {path}")
            return rows[0]
        except (UnicodeDecodeError, UnicodeError) as exc:
            last_error = exc
            continue
    raise RuntimeError(f"Failed to decode Incident sample CSV {path}: {last_error}")


def format_incident_ticket_info(row):
    """Render incident evidence, excluding tracking identifiers and raw snapshots."""
    structured_fields = {
        "summary": [
            ("phenomenon", "Phenomenon (异常现象)"),
            ("effect_users", "Affected Users (影响范围)"),
            ("progress", "Investigation Progress (处理经过)"),
            ("conclusion", "Conclusion (排查结论)"),
        ],
        "category_by_customer": [
            ("category", "Category (分类)"),
            ("category_analysis", "Analysis (分析)"),
        ],
        "fault_locator_msg": [
            ("msg", "Key Message (关键消息)"),
            ("analysis", "Analysis (定位分析)"),
            ("locator_type", "Method (定位方法)"),
        ],
    }

    def decode(value):
        for _ in range(2):
            if not isinstance(value, str):
                break
            stripped = value.strip()
            if not stripped.startswith(("{", "[", '"')):
                break
            try:
                value = json.loads(stripped)
            except (ValueError, TypeError):
                break
        return value

    def text(value):
        if value is None or isinstance(value, (dict, list)):
            return ""
        value = str(value).replace("\r\n", "\n").replace("\r", "\n")
        value = re.sub(r"https?://[^\s<>\"']+", "[link omitted]", value)
        value = re.sub(r"\b(?:oc|om|ou)_[A-Za-z0-9]+\b", "[id omitted]", value)
        value = re.sub(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b", "[email omitted]", value)
        return value.strip() if value.strip() not in ("", "null", "None", "未提及") else ""

    def append_scalar(destination, label, value, indent=""):
        content = text(value)
        if not content:
            return
        parts = content.splitlines()
        destination.append(f"{indent}- {label}: {parts[0]}")
        destination.extend(f"{indent}  {part}" for part in parts[1:])

    mapped_export = bool(row.get("_filter_category") and row.get("_source_file"))
    summary = decode(row.get("summary"))
    has_summary = isinstance(summary, dict) and any(
        text(summary.get(key)) for key, _ in structured_fields["summary"]
    )
    lines = []
    for key, label in INCIDENT_FIELD_LABELS:
        # New exports concatenate the same summary/locator data into this adapter
        # field. Render the original sections instead of repeating that entire text.
        if key == "LLM预测类别分析" and mapped_export and has_summary:
            continue
        if key == "LLM预测根因分类" and mapped_export:
            # The mapped export leaves the legacy root-cause column empty. Surface
            # its non-empty rule-selected category as the single root-cause field
            # instead of rendering a separate reproduction-candidate category.
            value = decode(row.get("_filter_category_name"))
        else:
            value = decode(row.get(key))
        if mapped_export and key == "LLM预测表象分类":
            label = "Surface Symptom Category (诊断事件类型规则映射，非独立LLM预测)"
        if key in structured_fields and isinstance(value, (dict, list)):
            records = value if isinstance(value, list) else [value]
            children = []
            for record in records:
                if not isinstance(record, dict):
                    continue
                for field, field_label in structured_fields[key]:
                    append_scalar(children, field_label, record.get(field), indent="  ")
            if children:
                lines.append(f"- {label}:")
                lines.extend(children)
        elif key not in structured_fields and isinstance(value, (dict, list)):
            # Classification columns can contain {"category": score} or a label list.
            # Only scalar category values are rendered; nested metadata is excluded.
            values = list(value.keys()) if isinstance(value, dict) else value
            append_scalar(lines, label, ", ".join(filter(None, (text(v) for v in values))))
        else:
            append_scalar(lines, label, value)
    if not lines:
        return "- (No structured record fields available.)"
    return "\n".join(lines)


def load_reproduction_skills(skills_dir, logger):
    """Load exactly the four authoritative Incident reproduction skills."""
    if not skills_dir or not os.path.isdir(skills_dir):
        raise RuntimeError(f"[MAIN][ERROR] Skills directory not found: {skills_dir}")

    required = [
        "incident_category_to_anomaly.md",
        "reproduction_action_playbook.md",
        "fast_environment_reset.md",
        "output_contract.md",
    ]
    missing = [
        filename for filename in required
        if not os.path.isfile(os.path.join(skills_dir, filename))
    ]
    if missing:
        raise RuntimeError(
            "[MAIN][ERROR] Missing required Incident reproduction skill(s): "
            + ", ".join(missing)
        )
    blocks = []
    for index, filename in enumerate(required, 1):
        with open(os.path.join(skills_dir, filename), "r", encoding="utf-8") as fh:
            content = fh.read().strip()
        if not content:
            raise RuntimeError(
                f"[MAIN][ERROR] Required Incident reproduction skill is empty: {filename}"
            )
        blocks.append(
            f"===== INCIDENT SKILL {index}/{len(required)}: {filename} =====\n{content}"
        )
    logger.info(
        f"[MAIN] Loaded {len(required)} required reproduction skill file(s): {required}"
    )
    return "\n\n".join(blocks)


def build_incident_sample_list(config, base_dir, logger):
    """Build the ordered list of Incident record samples to reproduce.

    Precedence:
      1. ``incident_sample_path`` (single explicit CSV) overrides everything.
      2. otherwise enumerate ``{id}.csv`` files under ``incident_samples_dir``,
         ordered by numeric id (matching the preprocessing naming).
    Optional filters ``sample_id`` (pick one id) and ``max_samples`` (cap count)
    are applied afterwards. Each entry is ``(sample_id:int, absolute_path)``.
    """
    single_path = config.get("incident_sample_path", None)
    if single_path:
        single_path = resolve_path_from_base(single_path, base_dir)
        if not os.path.exists(single_path):
            raise RuntimeError(f"[MAIN][ERROR] incident_sample_path does not exist: {single_path}")
        stem = os.path.splitext(os.path.basename(single_path))[0]
        sample_id = int(stem) if stem.isdigit() else 0
        logger.info(f"[MAIN] Using single explicit Incident sample: {single_path} (id={sample_id})")
        return [(sample_id, single_path)]

    samples_dir = resolve_path_from_base(
        config.get("incident_samples_dir", "external_inputs/incident"),
        base_dir,
    )
    if not os.path.isdir(samples_dir):
        raise RuntimeError(f"[MAIN][ERROR] incident_samples_dir does not exist: {samples_dir}")

    entries = []
    for filename in os.listdir(samples_dir):
        stem, ext = os.path.splitext(filename)
        if ext.lower() == ".csv" and stem.isdigit():
            entries.append((int(stem), os.path.join(samples_dir, filename)))
    entries.sort(key=lambda item: item[0])

    if not entries:
        raise RuntimeError(f"[MAIN][ERROR] No numeric `<id>.csv` samples found under {samples_dir}")

    sample_id = config.get("sample_id", None)
    if sample_id is not None:
        wanted = {int(sample_id)} if not isinstance(sample_id, (list, tuple)) else {int(s) for s in sample_id}
        entries = [item for item in entries if item[0] in wanted]
        if not entries:
            raise RuntimeError(f"[MAIN][ERROR] Requested sample_id {sample_id} not found under {samples_dir}")

    max_samples = config.get("max_samples", None)
    if max_samples is not None:
        entries = entries[: int(max_samples)]

    logger.info(f"[MAIN] Incident sample directory: {samples_dir} | Selected samples: {len(entries)}")
    return entries


def build_incontainer_db_env_prefix(system_user_config, db_name, db_schema, dbms):
    """Build a shell export prefix so bash/python exploration actions can reach
    the in-container database via standard client environment variables.

    Exploration actions run INSIDE the container (localhost:5432). Credentials are
    exposed via env vars (never on the command line) exactly like the runtime the
    injection script will later see under ``anomaly_eval.py``.
    """
    host = str(system_user_config["DB_HOST"])
    port = str(system_user_config["DB_PORT"])
    user = str(system_user_config["DB_USER"])
    password = str(system_user_config["DB_PASSWORD"])
    exports = {
        "PGHOST": host,
        "PGPORT": port,
        "PGUSER": user,
        "PGPASSWORD": password,
        "PGDATABASE": db_name,
        "MYSQL_PWD": password,
        "DB_HOST": host,
        "DB_PORT": port,
        "DB_USER": user,
        "DB_PASSWORD": password,
        "DB_NAME": db_name,
        "DB_SCHEMA": db_schema,
        "DBMS": dbms,
    }
    if normalize_dbms(dbms) != "mysql":
        exports["PGOPTIONS"] = f"-c search_path={db_schema}"
    prefix = " ".join(f"export {k}={shlex.quote(v)};" for k, v in exports.items())
    return prefix


def build_reproduction_baseline_snapshot(
    docker_name, init_db_config, output_dir, fast_reset_state, logger, reason
):
    """Capture the imported, pre-candidate database used by reproduction."""
    if not fast_reset_state or not fast_reset_state.get("enabled"):
        return False

    try:
        snapshot_config = dict(init_db_config)
        snapshot_config["output_dir"] = output_dir
        snapshot_ok = docker_container_build_baseline_snapshot(
            docker_name, snapshot_config, session_name=docker_name
        )
        fast_reset_state["snapshot_ready"] = bool(snapshot_ok)
        if snapshot_ok:
            logger.info(
                f"[Anomaly Reproduction] Raw imported baseline captured [{reason}]; "
                "candidate resets will reuse the current container."
            )
        else:
            logger.warning(
                f"[Anomaly Reproduction][WARNING] Raw baseline capture failed [{reason}]; "
                "using full container rebuilds."
            )
        return bool(snapshot_ok)
    except Exception as exc:
        fast_reset_state["snapshot_ready"] = False
        logger.warning(
            f"[Anomaly Reproduction][WARNING] Raw baseline capture raised [{reason}] "
            f"[{exc}]; using full container rebuilds."
        )
        return False


def reinitialize_clean_environment(docker_name, base_docker_name, container_config, init_db_config,
                                   dbms, system_user_config, data_modification_path,
                                   output_dir, logger, tag, fast_reset_state=None,
                                   force_full_rebuild=False):
    """Restore the raw imported state, then apply the current candidate shaping.

    The preferred path restores the working database from the container-local
    raw golden baseline. A failed/unsafe fast reset transparently falls back to
    recreating the container and re-importing the database. The raw baseline is
    captured before ``data_modification.sql`` because that file may change on
    every refinement turn.
    """
    db_name = init_db_config['db_name']
    if (not force_full_rebuild
            and fast_reset_state
            and fast_reset_state.get("enabled")
            and fast_reset_state.get("snapshot_ready")):
        logger.info(
            f"[Anomaly Reproduction] -- Fast-Resetting Clean Environment [{tag}] --"
        )
        try:
            reset_config = dict(init_db_config)
            reset_config["output_dir"] = output_dir
            if docker_container_fast_reset(
                docker_name, reset_config, session_name=docker_name
            ):
                apply_data_modification_if_needed(
                    docker_name, dbms, system_user_config, data_modification_path,
                    db_name, output_dir, logger,
                    log_filename=f"data_modification_{tag}_log.txt",
                )
                logger.info(
                    f"[Anomaly Reproduction] Fast reset and candidate data shaping "
                    f"succeeded [{tag}]."
                )
                return docker_name
            logger.warning(
                f"[Anomaly Reproduction][WARNING] Fast reset failed [{tag}]; "
                "falling back to a full container rebuild."
            )
        except Exception as exc:
            logger.warning(
                f"[Anomaly Reproduction][WARNING] Fast reset raised [{tag}] [{exc}]; "
                "falling back to a full container rebuild."
            )
        fast_reset_state["snapshot_ready"] = False
    elif force_full_rebuild:
        logger.info(
            f"[Anomaly Reproduction] -- Forcing Full Container Rebuild [{tag}] "
            "(prior evaluation timed out) --"
        )
        if fast_reset_state:
            fast_reset_state["snapshot_ready"] = False

    logger.info(f"[Anomaly Reproduction] -- Re-initializing Clean Docker Environment [{tag}] --")
    initialization_times = 0
    docker_init_status = False
    while initialization_times < RETRY_TIMES:
        docker_init_status, docker_name = recreate_container_with_new_name(docker_name, base_docker_name, container_config)
        if docker_init_status is True:
            logger.info(f"[MAIN] Docker Container ``{docker_name}`` Initialized Successfully.")
            break
        logger.error(f"[MAIN][ERROR] Error initializing Docker Container ``{docker_name}''. Retrying ({initialization_times}/{RETRY_TIMES})...")
        initialization_times += 1
        time.sleep(5)
    if docker_init_status is False:
        raise RuntimeError(f"[MAIN][ERROR] Docker Container `{docker_name}` initialization failed after {RETRY_TIMES} retries.")

    initialization_times = 0
    docker_status = False
    while initialization_times < RETRY_TIMES:
        docker_status = docker_container_init_db(docker_name, init_db_config, session_name=docker_name)
        if docker_status is True:
            logger.info(f"[MAIN] Database `{db_name}` Initialization in Docker Container `{docker_name}` Succeeded.")
            break
        logger.error(f"[MAIN][ERROR] Database `{db_name}` Initialization in `{docker_name}` Failed. Retrying ({initialization_times}/{RETRY_TIMES})...")
        initialization_times += 1
        time.sleep(5)
        _, docker_name = recreate_container_with_new_name(docker_name, base_docker_name, container_config)
    if docker_status is False:
        raise RuntimeError(f"[MAIN][ERROR] Database `{db_name}` initialization in `{docker_name}` failed after {RETRY_TIMES} retries.")

    # Capture the raw import before applying the candidate. Each Refine may
    # change data_modification.sql, so embedding it in the golden baseline would
    # restore a stale candidate on the next turn.
    build_reproduction_baseline_snapshot(
        docker_name, init_db_config, output_dir, fast_reset_state, logger,
        reason=f"full rebuild {tag}",
    )

    apply_data_modification_if_needed(
        docker_name, dbms, system_user_config, data_modification_path, db_name, output_dir, logger,
        log_filename=f"data_modification_{tag}_log.txt"
    )
    return docker_name


def apply_data_modification_if_needed(docker_name, dbms, system_user_config, data_modification_path,
                                      database_name, output_dir, logger, log_filename="data_modification_log.txt"):
    """Apply the LLM-authored one-time data-shaping SQL, if it has executable
    content. Comment-only / empty files are skipped (no-op)."""
    if not data_modification_path or not os.path.exists(data_modification_path):
        logger.info("[MAIN] No data modification file present; skipping data shaping.")
        return
    with open(data_modification_path, "r", encoding="utf-8") as fh:
        content = fh.read()
    meaningful = "\n".join(
        line for line in content.splitlines() if line.strip() and not line.strip().startswith("--")
    ).strip()
    if meaningful == "":
        logger.info("[MAIN] Data modification file is empty/comment-only; skipping data shaping.")
        return

    logger.info("[MAIN] Applying one-time data modification SQL to the clean instance...")
    cmd = build_sql_file_command(dbms, system_user_config, data_modification_path, database_name)
    obser_log = os.path.join(output_dir, log_filename)
    state, obser_output = run_docker_exec_observation(docker_name, cmd, obser_log, 600, session_name=docker_name)
    if state is False:
        reset_tmux_window(session_name=docker_name, window_name="observation")
        raise RuntimeError(f"[MAIN][ERROR] Data Modification Failed. See log: {obser_log}. Output: {obser_output}")
    logger.info(f"[MAIN] Data Modification Succeeded. Log saved to: {obser_log}")


def parse_timeout_threshold(script_text, default=600):
    """Extract the integer `# TIMEOUT THRESHOLD: <n>` from a shell script's first
    line; fall back to ``default`` when absent/unparseable."""
    if "# TIMEOUT THRESHOLD: " in script_text:
        try:
            return int(script_text.split("# TIMEOUT THRESHOLD: ")[-1].strip().split()[0].strip())
        except (ValueError, IndexError):
            return default
    return default


def extract_anomaly_type(anomaly_description_str):
    """Best-effort extraction of the anomaly ``Type`` (OLAP/OLTP/BOTH) from the
    LLM-authored ``anomaly_description.json`` string for the eval hint."""
    try:
        obj = json.loads(anomaly_description_str)
        value = str(obj.get("Type", "BOTH")).strip().upper().replace(" ", "")
        if value in {"OLAP", "OLTP", "BOTH"}:
            return value
        if value in {"OLAP+OLTP", "OLTP+OLAP"}:
            return "BOTH"
    except (ValueError, TypeError):
        pass
    return "BOTH"


# LLM-declared manifestation categories. These MUST stay in sync with
# ``anomaly_eval_rep.py`` (``--success-criterion``) so the value extracted here is
# understood verbatim by the authoritative evaluator. ``AUTO`` preserves the
# historical broadened OR judgement when the record does not declare a category.
SUCCESS_CRITERION_PERFORMANCE = "PERFORMANCE_DEGRADATION"
SUCCESS_CRITERION_BLOCKING = "BLOCKING_TIMEOUT"
SUCCESS_CRITERION_ERROR = "EXECUTION_ERROR"
SUCCESS_CRITERION_AUTO = "AUTO"
VALID_SUCCESS_CRITERIA = frozenset(
    {
        SUCCESS_CRITERION_PERFORMANCE,
        SUCCESS_CRITERION_BLOCKING,
        SUCCESS_CRITERION_ERROR,
        SUCCESS_CRITERION_AUTO,
    }
)


def normalize_success_criterion_value(value):
    """Normalize a free-text success-criterion to one of the known categories.

    Mirrors ``anomaly_eval_rep.normalize_success_criterion`` (canonical
    underscore form + a small synonym set) so the value passed on the command
    line is always one the evaluator recognizes. Unknown / missing values degrade
    to ``AUTO`` (historical broadened OR judgement) instead of aborting the run.
    """
    normalized = str(value or "").strip().upper().replace(" ", "_").replace("-", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    normalized = normalized.strip("_")
    if normalized in VALID_SUCCESS_CRITERIA:
        return normalized
    performance_aliases = {"PERFORMANCE", "DEGRADATION", "SLOWDOWN", "LATENCY", "PERFORMANCE_DEGRADE", "PERF_DEGRADATION"}
    blocking_aliases = {"BLOCKING", "BLOCKED", "TIMEOUT", "BLOCK", "HANG", "BLOCKED_UNTIL_TIMEOUT", "BLOCK_TIMEOUT"}
    error_aliases = {"ERROR", "EXECUTION_FAILURE", "FAILURE", "EXEC_ERROR", "WORKLOAD_ERROR", "EXECUTION_ERRORS"}
    if normalized in performance_aliases:
        return SUCCESS_CRITERION_PERFORMANCE
    if normalized in blocking_aliases:
        return SUCCESS_CRITERION_BLOCKING
    if normalized in error_aliases:
        return SUCCESS_CRITERION_ERROR
    return SUCCESS_CRITERION_AUTO


def extract_success_criterion(anomaly_description_str):
    """Extract the LLM-declared ``SuccessCriterion`` from the authored
    ``anomaly_description.json`` string.

    This is the manifestation category (performance degradation / blocking
    timeout / execution error) the LLM chose from the Incident record; it selects
    which authoritative success signal ``anomaly_eval_rep.py`` uses. A missing or
    unrecognized value degrades to ``AUTO`` so a noisy classification never aborts
    the evaluation.
    """
    try:
        obj = json.loads(anomaly_description_str)
        return normalize_success_criterion_value(obj.get("SuccessCriterion", ""))
    except (ValueError, TypeError):
        return SUCCESS_CRITERION_AUTO


def normalize_anomaly_description_json(raw_value):
    """Return a valid JSON string for anomaly_description.json.

    The LLM may emit either a JSON string or (occasionally) a nested object; both
    are normalized to a compact, valid JSON string with the required keys. The
    ``SuccessCriterion`` key (the LLM-declared manifestation category) is
    preserved and normalized to a known category so the evaluator can consume it.
    """
    try:
        obj = raw_value if isinstance(raw_value, dict) else json.loads(raw_value)
    except (ValueError, TypeError):
        # Not valid JSON: wrap the raw text into a minimal valid structure.
        return json.dumps(
            {
                "Name": "Online Diagnosis Incident Reproduced Anomaly",
                "Description": str(raw_value),
                "Type": "BOTH",
                "SuccessCriterion": SUCCESS_CRITERION_AUTO,
            },
            ensure_ascii=False,
            indent=2,
        )
    obj.setdefault("Name", "Online Diagnosis Incident Reproduced Anomaly")
    obj.setdefault("Description", "")
    obj.setdefault("Type", "BOTH")
    # Normalize whatever the LLM declared (or AUTO when the key is absent) so the
    # persisted artifact and the value passed to the evaluator always agree.
    obj["SuccessCriterion"] = normalize_success_criterion_value(obj.get("SuccessCriterion", ""))
    return json.dumps(obj, ensure_ascii=False, indent=2)


def run_finish_validation(json_response, docker_name, config_opt, turn_idx, logger):
    """Materialize the finish artifacts, deterministically derive the harness
    files, copy the reproduction eval script in, and run the authoritative
    anomaly evaluation. Returns ``(success: bool, observation: str, docker_name)``.

    On success the sample directory contains the full 10-file layout. On failure
    the returned observation is fed back to the LLM for another refinement turn.

    ``turn_idx`` is the ReAct turn index; it is only used to emit the
    watchdog-recognized ``Executing Script with Timeout Threshold <N> at Turn <i>``
    log line so the automation shell's custom-timeout detector temporarily relaxes
    its stall timeout during the (potentially very long) in-container evaluation.
    """
    system_opt = config_opt['system_opt']
    anomaly_eval_opt = config_opt['anomaly_eval_opt']
    llm_opt = config_opt['llm_opt']

    output_base_dir = system_opt['output_base_dir']
    base_docker_name = system_opt['base_docker_name']
    container_config = system_opt['container_config']
    init_db_config = system_opt['init_db_config']
    system_user_config = system_opt['system_user_config']
    dbms = system_opt['dbms']
    db_config = system_opt['db_config']
    db_name = init_db_config['db_name']
    db_schema = anomaly_eval_opt['schema']
    median_runs = config_opt['median_runs']

    # 1) Persist the seven LLM-authored artifacts verbatim (with light JSON
    #    normalization for anomaly_description.json).
    for key, value in json_response.items():
        if key == "action":
            continue
        target_path = os.path.join(output_base_dir, key)
        if key == "anomaly_description.json":
            value = normalize_anomaly_description_json(value)
        with open(target_path, "w", encoding="utf-8") as fh:
            fh.write(value)

    initialization_workload_path = os.path.join(output_base_dir, "initialization_workload.sql")
    verified_workload_path = os.path.join(output_base_dir, "verified_workload.sql")
    data_modification_path = os.path.join(output_base_dir, "data_modification.sql")
    anomaly_inject_path = os.path.join(output_base_dir, "anomaly_inject.sh")
    anomaly_description_path = os.path.join(output_base_dir, "anomaly_description.json")

    # 2) Deterministically build merged_workload.sql (init + verified).
    with open(initialization_workload_path, "r", encoding="utf-8") as fh:
        initialization_workload = fh.read()
    with open(verified_workload_path, "r", encoding="utf-8") as fh:
        verified_workload = fh.read()
    merged_workload = initialization_workload.strip() + "\n\n" + verified_workload.strip() + "\n"
    merged_workload_path = os.path.join(output_base_dir, "merged_workload.sql")
    with open(merged_workload_path, "w", encoding="utf-8") as fh:
        fh.write(merged_workload)

    # 3) Reinitialize a CLEAN instance and apply the one-time data modification so
    #    the measured baseline is independent of exploratory drift.
    docker_name = reinitialize_clean_environment(
        docker_name, base_docker_name, container_config, init_db_config,
        dbms, system_user_config, data_modification_path, output_base_dir, logger,
        tag="finish", fast_reset_state=system_opt.get("fast_reset_state"),
    )

    # 4) Time the merged workload on the clean+modified instance (host-side).
    logger.info("[MAIN] Recording Merged Workload Performance on Clean Instance...")
    merged_workload_results = execute_sql_file_with_timing(merged_workload_path, db_config)
    with open(os.path.join(output_base_dir, "merged_workload_performance.json"), "w", encoding="utf-8") as fh:
        json.dump(merged_workload_results, fh, indent=4)
    merged_workload_total_exec_time = sum(
        max(0.0, result['execution_time'])
        for result in merged_workload_results
        if result.get('success') and isinstance(result.get('execution_time'), (int, float))
    )
    logger.info(f"[MAIN] Merged Workload Total Execution Time on Clean Instance: {merged_workload_total_exec_time} seconds.")

    # Reject a sample whose merged workload cannot even run on the normal state:
    # such artifacts are not internally consistent and cannot be reproduced.
    if any((not result.get('success', False)) for result in merged_workload_results):
        failed = [r.get('sql', '')[:120] for r in merged_workload_results if not r.get('success', False)]
        observation = ("The merged workload (initialization + verified) FAILED to execute cleanly on the "
                       "NORMAL database state. The reproduction sample must run cleanly before injection. "
                       f"Failing statements (truncated): {failed}. Please fix initialization_workload.sql / "
                       "verified_workload.sql / data_modification.sql and finish again.")
        return False, observation, docker_name

    # 5) Copy the reproduction-oriented evaluation script in as anomaly_eval.py.
    #    The authoritative evaluation below runs THIS copied reproduction-oriented
    #    script (it accepts --min-abs-delta-seconds / --anomaly-type /
    #    --success-criterion), NOT the protected legacy Scripts/anomaly_eval.py.
    evaluated_anomaly_eval_path = os.path.join(output_base_dir, "anomaly_eval.py")
    shutil.copy(anomaly_eval_opt['anomaly_eval_source_path'], evaluated_anomaly_eval_path)

    # 6) Run the authoritative anomaly evaluation inside the container.
    with open(anomaly_inject_path, "r", encoding="utf-8") as fh:
        anomaly_inject_text = fh.read()
    with open(anomaly_description_path, "r", encoding="utf-8") as fh:
        anomaly_description_text = fh.read()
    anomaly_type = extract_anomaly_type(anomaly_description_text)
    # The LLM-declared manifestation category selects the authoritative success
    # signal (see anomaly_eval_rep.py); a missing/unknown value degrades to AUTO.
    success_criterion = extract_success_criterion(anomaly_description_text)

    timeout_threshold = parse_timeout_threshold(anomaly_inject_text, default=600)
    timeout_threshold += median_runs * 2.5 * merged_workload_total_exec_time
    if timeout_threshold > 86400:
        logger.warning(f"[MAIN][WARNING] Timeout Threshold {timeout_threshold}s exceeds 24 hours. Capping to 86400s.")
    timeout_threshold = min(timeout_threshold, 86400)

    anomaly_eval_command_list = [
        "python",
        evaluated_anomaly_eval_path,
        "--dbms", str(anomaly_eval_opt.get('dbms', 'PostgreSQL 16.11')),
        "--script-path", anomaly_inject_path,
        "--verified-workload-path", verified_workload_path,
        "--initialization-workload-path", initialization_workload_path,
        "--host", anomaly_eval_opt['host'],
        "--port", str(anomaly_eval_opt['port']),
        "--user", anomaly_eval_opt['user'],
        "--database", anomaly_eval_opt['database'],
        "--schema", anomaly_eval_opt['schema'],
        "--password", anomaly_eval_opt['password'],
        "--timeout", str(timeout_threshold),
        "--median-runs", str(median_runs),
        "--success-threshold-percent", str(anomaly_eval_opt['success_threshold_percent']),
        "--min-abs-delta-seconds", str(anomaly_eval_opt['min_abs_delta_seconds']),
        "--anomaly-type", anomaly_type,
        "--success-criterion", success_criterion,
    ]
    obser_log = os.path.join(output_base_dir, "finish_eval_log.txt")
    # Emit the exact phrase the automation watchdog recognizes so it relaxes its
    # stall timeout for the duration of this (possibly very long) in-container
    # evaluation window; see parse_active_custom_timeout in the *_ano_syn_rep.sh
    # scripts. The window is considered closed once a subsequent progress marker
    # (e.g. "LLM Inference Completed") appears in log.txt.
    logger.info(
        f"[Anomaly Reproduction] - Executing Script with Timeout Threshold {timeout_threshold} at Turn {turn_idx}..."
    )
    logger.info(
        f"[MAIN] Running authoritative anomaly evaluation (timeout {timeout_threshold}s, "
        f"type {anomaly_type}, success criterion {success_criterion})..."
    )
    state, obser_output = run_docker_exec_observation(
        docker_name, anomaly_eval_command_list, obser_log, timeout_threshold, session_name=docker_name
    )

    if state is True:
        logger.info("[MAIN] Authoritative anomaly evaluation SUCCEEDED. Sample is complete.")
        observation = "Reproduction sample validated successfully; anomaly evaluation reported success.\n"
        observation += prune_obser_log(obser_log)
        return True, observation, docker_name

    if "Command timed out" in obser_output:
        fast_reset_state = system_opt.get("fast_reset_state")
        if fast_reset_state is not None:
            fast_reset_state["force_full_rebuild"] = True
        observation = ("The anomaly evaluation flow timed out at the orchestration level: the whole evaluation "
                       "did not finish within the allotted budget, so the TIMEOUT THRESHOLD in anomaly_inject.sh "
                       "is likely too small. Increase it and finish again. Output as follows:\n")
    else:
        observation = ("The anomaly evaluation did NOT confirm a reproduced anomaly (the verified workload did not "
                       "degrade/block/error enough after injection). Strengthen the anomaly (e.g. hold locks longer, "
                       "increase contention/skew, or exhaust more resources) and finish again. Output as follows:\n")
    observation += prune_obser_log(obser_log)
    if obser_output != "":
        observation += f"\n\n-- COMMENTS :\n{obser_output}"

    reset_tmux_window(session_name=docker_name, window_name="observation")
    return False, observation, docker_name


def dispatch_exploration_action(json_response, action_kind, docker_name, config_opt, turn_idx, logger):
    """Execute one exploratory action (sql/bash/python) inside the live container
    and return the observation string. Exploration state accumulates in the same
    container so the LLM can prototype multi-step logic; the authoritative finish
    always reinitializes a clean instance, so drift never affects final artifacts.
    """
    system_opt = config_opt['system_opt']
    output_base_dir = system_opt['output_base_dir']
    system_user_config = system_opt['system_user_config']
    dbms = system_opt['dbms']
    db_name = system_opt['init_db_config']['db_name']
    db_schema = config_opt['anomaly_eval_opt']['schema']

    output_sub_dir = os.path.join(output_base_dir, f"{turn_idx}_multi_turn")
    os.makedirs(output_sub_dir, exist_ok=True)
    obser_log = os.path.join(output_sub_dir, "obser_log.txt")

    env_prefix = build_incontainer_db_env_prefix(system_user_config, db_name, db_schema, dbms)

    if action_kind == "sql":
        query_path = os.path.join(output_sub_dir, "action.sql")
        with open(query_path, "w", encoding="utf-8") as fh:
            fh.write(json_response['query'])
        cmd = build_sql_file_command(dbms, system_user_config, query_path, db_name)
        timeout = config_opt['explore_sql_timeout']
        state, obser_output = run_docker_exec_observation(docker_name, cmd, obser_log, timeout, session_name=docker_name)

    elif action_kind == "bash":
        script_path = os.path.join(output_sub_dir, "action.sh")
        with open(script_path, "w", encoding="utf-8") as fh:
            fh.write(json_response['shell'])
        timeout = min(parse_timeout_threshold(json_response['shell'], default=config_opt['explore_bash_timeout']), 86400)
        inner = f"{env_prefix} bash {shlex.quote(script_path)}"
        cmd = ["bash", "-lc", inner]
        state, obser_output = run_docker_exec_observation(docker_name, cmd, obser_log, timeout, session_name=docker_name)

    else:  # python
        script_path = os.path.join(output_sub_dir, "action.py")
        with open(script_path, "w", encoding="utf-8") as fh:
            fh.write(json_response['code'])
        timeout = config_opt['explore_python_timeout']
        inner = f"{env_prefix} python3 {shlex.quote(script_path)}"
        cmd = ["bash", "-lc", inner]
        state, obser_output = run_docker_exec_observation(docker_name, cmd, obser_log, timeout, session_name=docker_name)

    if state is True:
        observation = f"The {action_kind} action executed successfully, with the output as follows:\n"
    else:
        if "Command timed out" in obser_output:
            observation = (f"The {action_kind} action timed out. If this was an intentional long-running holder "
                           f"launched in the background, detach it (e.g. nohup ... &) so it persists without blocking; "
                           f"otherwise reduce its runtime. Output as follows:\n")
        else:
            observation = f"The {action_kind} action failed, with the output as follows:\n"
        reset_tmux_window(session_name=docker_name, window_name="observation")

    observation += prune_obser_log(obser_log)
    if obser_output != "":
        observation += f"\n\n-- COMMENTS :\n{obser_output}"
    return observation


def llm_infer_reproduce(prompt, config_opt, logger):
    """Unified multi-action ReAct loop for one Incident record.

    Each turn the LLM emits exactly one action:
      - ``sql`` / ``bash`` / ``python``: exploratory execution in the container.
      - ``finish``: emit the seven artifacts; the harness then validates and
        materializes the full 10-file sample.
    A ``finish`` is only accepted once the authoritative evaluation confirms the
    anomaly; otherwise the failure observation is fed back for further turns.
    Returns ``(finish_synthesized: bool, docker_name)``.
    """
    logger.info("[Anomaly Reproduction] Starting Unified Multi-Action ReAct Inference...")
    llm_opt = config_opt['llm_opt']
    system_opt = config_opt['system_opt']
    MAX_TURN_NUM = config_opt['max_turn_num']

    docker_name = system_opt['docker_name']
    output_mes_path = system_opt['output_mes_path']
    output_base_dir = system_opt['output_base_dir']
    system_user_config = system_opt['system_user_config']

    messages = [prompt]
    response = ""
    finish_synthesized = False

    for i in range(MAX_TURN_NUM):
        if i == MAX_TURN_NUM - 1:
            logger.info(f"[Anomaly Reproduction] -- ReAct Turn {i} (MAX TURN NUMBER) --")
        else:
            logger.info(f"[Anomaly Reproduction] -- ReAct Turn {i} --")

        output_sub_dir = os.path.join(output_base_dir, f"{i}_multi_turn")
        os.makedirs(output_sub_dir, exist_ok=True)

        cnt = 0
        reasoning_responses = ""
        json_response = ""
        action_kind = None
        matched_mess = False
        while cnt < llm_opt['retry_times']:
            response = get_llm_response(messages, llm_opt)
            reasoning_responses += response + "\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
            with open(os.path.join(output_sub_dir, "reasoning_responses.txt"), "w", encoding="utf-8") as fh:
                fh.write(reasoning_responses)

            pure_response = response.split('<action>')[-1].split('</action>')[0].strip().replace(FAKE_DATABASE_IP, system_user_config['DB_HOST'])
            try:
                json_str = extract_json_object(pure_response)
                json_response = json.loads(json_str)
            except (ValueError, json.JSONDecodeError) as exc:
                logger.warning(f"[Anomaly Reproduction][WARNING] Failed to extract valid JSON action: {exc} --> Retrying ({cnt}/{llm_opt['retry_times']})...")
                cnt += 1
                continue

            keys = json_response.keys() if isinstance(json_response, dict) else set()
            action_val = json_response.get('action') if isinstance(json_response, dict) else None
            sql_state = (keys == {'action', 'query'} and action_val == 'sql')
            bash_state = (keys == {'action', 'shell'} and action_val == 'bash' and "# TIMEOUT THRESHOLD: " in str(json_response.get('shell', '')))
            python_state = (keys == {'action', 'code'} and action_val == 'python')
            finish_state = (
                action_val == 'finish'
                and keys == {
                    'action', 'anomaly_description.json', 'user_phenomenon_description.txt',
                    'initialization_workload.sql', 'verified_workload.sql', 'data_modification.sql',
                    'data_feature_summarization.txt', 'anomaly_inject.sh',
                }
                and "# TIMEOUT THRESHOLD: " in str(json_response.get('anomaly_inject.sh', ''))
            )

            if sql_state:
                action_kind = "sql"
            elif bash_state:
                action_kind = "bash"
            elif python_state:
                action_kind = "python"
            elif finish_state:
                action_kind = "finish"

            if action_kind is not None:
                matched_mess = True
                break
            logger.warning(f"[Anomaly Reproduction][WARNING] Unrecognized action format --> Retrying ({cnt}/{llm_opt['retry_times']})...")
            cnt += 1

        if matched_mess is False:
            logger.warning("[Anomaly Reproduction][WARNING] LLM did not produce a valid action within retry budget.")
        messages.append(response)
        logger.info(f"[Anomaly Reproduction] -- LLM Inference Finish [Turn {i}] --")

        # The very first turn may not `finish` (must explore/validate first).
        if action_kind == "finish" and i == 0:
            observation = ("A `finish` action is not allowed in the first turn. Please first explore the database and "
                           "validate that your anomaly injection reproduces the record symptom, then finish.")
            messages.append(observation)
            continue

        if action_kind in ("sql", "bash", "python"):
            observation = dispatch_exploration_action(json_response, action_kind, docker_name, config_opt, i, logger)
            messages.append(observation)
            logger.info(f"[Anomaly Reproduction] -- Observation Appended [Turn {i}] --")
            continue

        if action_kind == "finish":
            logger.info("[Anomaly Reproduction] - FINISH action received; validating reproduction sample -")
            success, observation, docker_name = run_finish_validation(json_response, docker_name, config_opt, i, logger)
            # Persist the running message trace after every finish attempt.
            messages_json = {f"message_{idx}": message for idx, message in enumerate(messages)}
            with open(output_mes_path, "w", encoding="utf-8") as fh:
                json.dump(messages_json, fh, indent=4, ensure_ascii=False)
            if success:
                finish_synthesized = True
                break
            messages.append(observation)
            logger.warning(f"[Anomaly Reproduction][WARNING] Finish validation failed at Turn {i}; feeding back for refinement.")
            fast_reset_state = system_opt.get("fast_reset_state")
            force_full_rebuild = bool(
                fast_reset_state and fast_reset_state.pop("force_full_rebuild", False)
            )
            docker_name = reinitialize_clean_environment(
                docker_name,
                system_opt["base_docker_name"],
                system_opt["container_config"],
                system_opt["init_db_config"],
                system_opt["dbms"],
                system_opt["system_user_config"],
                os.path.join(output_base_dir, "data_modification.sql"),
                output_base_dir,
                logger,
                tag=f"refine_{i}",
                fast_reset_state=fast_reset_state,
                force_full_rebuild=force_full_rebuild,
            )
            continue

        # Unparseable action this turn: reinitialize a clean container so the next
        # exploration turn starts from a consistent state, then continue.
        observation = ("Your response did not contain a valid action. Emit exactly one of the sql/bash/python/finish "
                       "actions in the required JSON format.")
        messages.append(observation)

    if not finish_synthesized:
        logger.error(f"[Anomaly Reproduction][ERROR][== SYNTHESIS FAILED ==] Max Turn Number ({MAX_TURN_NUM}) reached without a validated `finish`.")

    return finish_synthesized, docker_name


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description='Online Diagnosis Incident Database Anomaly Reproduction Synthesis')
    parser.add_argument('--config', type=str, required=False,
                        default=os.path.join(base_dir, "Configs/syn_ano_rep/config.json"),
                        help='Path to the configuration JSON file')
    args = parser.parse_args()
    config_path = resolve_path_from_base(args.config, base_dir)
    with open(config_path, 'r', encoding="utf-8") as file:
        config = json.load(file)

    # Database configuration (reproduction targets a single instance by default).
    db_names = config.get("database_names", ['tpch_1'])
    DB_NAME = db_names[0]
    database_schema_dir = resolve_path_from_base(config.get('database_schema_directory', "Data/Schema"), base_dir)

    MAX_TURN_NUM = config.get('max_turn_num', 24)
    # Number of MEASURED runs (median taken, preceded by one warm-up pass).
    # Defaults to 1 and may be raised via config to smooth out latency noise;
    # downstream anomaly_eval_rep.py re-applies the initialization workload before
    # every run so a repeated run always starts from the same restored state.
    median_runs = max(1, int(config.get('median_runs', 1)))

    # LLM configuration
    llm_opt = config.get('llm_configuration', None)
    if llm_opt is None:
        llm_opt = {
            "base_url": openai.base_url,
            "api_key": openai.api_key,
            "model_name": model_name,
            "temperature": temperature,
            "retry_times": 10,
        }

    # Docker configuration
    BASE_DOCKER_NAME = config.get('docker_name', "pg_syn_ano_rep")
    DOCKER_NAME = BASE_DOCKER_NAME
    output_root_dir = resolve_path_from_base(config.get('output_base_dir', "Output"), base_dir)
    output_base_dir = obtain_output_dir(os.path.abspath(__file__), output_root_dir)
    if not os.path.exists(output_base_dir):
        os.makedirs(output_base_dir)
    with open(os.path.join(output_base_dir, "config.json"), 'w', encoding="utf-8") as file:
        json.dump(config, file, indent=4, ensure_ascii=False)

    DBMS = config.get('dbms', "PostgreSQL 16.11")
    DBMS_KIND = normalize_dbms(DBMS)
    DB_TYPE = resolve_db_type(config, DBMS)
    FAST_RESET_ENABLED = resolve_fast_reset_enabled(DBMS_KIND, config)

    SYSTEM_HARDWARE_CONFIG = config.get('system_hardware_config', None)
    if SYSTEM_HARDWARE_CONFIG is None:
        SYSTEM_HARDWARE_CONFIG = {"CPU": 8, "Memory": 8, "Memory + Swap": 16, "Storage": 64}
    HOST_PORT = config.get('host_port', 25200)
    for GB_Config in GB_Configurations:
        SYSTEM_HARDWARE_CONFIG[f"{GB_Config}"] = f"{SYSTEM_HARDWARE_CONFIG[GB_Config]}GB"

    SYSTEM_USER_CONFIG_JSON = config.get('system_user_config', None)
    if SYSTEM_USER_CONFIG_JSON is None:
        SYSTEM_USER_CONFIG_JSON = {
            "DB_USER": "diagnosis",
            "DB_PASSWORD": "diagnosis",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_NAME": "",
        }

    container_config = build_container_config(DBMS, SYSTEM_HARDWARE_CONFIG, HOST_PORT, SYSTEM_USER_CONFIG_JSON)
    SYSTEM_HARDWARE_CONFIG_STR = str(SYSTEM_HARDWARE_CONFIG)

    # Evaluation thresholds (reproduction-oriented judgement, see anomaly_eval_rep.py).
    SUCCESS_THRESHOLD_PERCENT = config.get('success_threshold_percent', 20.0)
    MIN_ABS_DELTA_SECONDS = config.get('min_abs_delta_seconds', 0.0)

    # Exploration action timeouts.
    EXPLORE_SQL_TIMEOUT = int(config.get('explore_sql_timeout', 600))
    EXPLORE_BASH_TIMEOUT = int(config.get('explore_bash_timeout', 900))
    EXPLORE_PYTHON_TIMEOUT = int(config.get('explore_python_timeout', 900))

    log_path = os.path.join(output_base_dir, "log.txt")
    logger = set_up_logger(log_path)
    logger = logging.getLogger(f"DB_DIAG.{__name__}")
    logger.info("[MAIN][Online Diagnosis Incident Anomaly Reproduction Synthesis] == START ==")

    # Load curated reproduction skills for the prompt.
    skills_dir = resolve_path_from_base(config.get('skills_directory', "Skills"), base_dir)
    SKILLS = load_reproduction_skills(skills_dir, logger)

    # Build the ordered Incident sample list.
    incident_samples = build_incident_sample_list(config, base_dir, logger)

    # Reap orphan containers/sessions from a previously crashed run of this worker.
    logger.info(f"[MAIN] Sweeping stale containers/sessions with prefix '{BASE_DOCKER_NAME}' before start ...")
    remove_containers_by_prefix(BASE_DOCKER_NAME)
    kill_tmux_sessions_by_prefix(BASE_DOCKER_NAME)

    DB_SCHEMA_NAME = config.get('database_schema_name', "diagnosis")
    if DBMS_KIND == "mysql":
        DB_SCHEMA_NAME = DB_NAME
    PROMPT_DB_SCHEMA_NAME = MYSQL_SCHEMA_PROMPT_VALUE if DBMS_KIND == "mysql" else DB_SCHEMA_NAME

    inference_times = []
    for sample_id, sample_path in tqdm(incident_samples, total=len(incident_samples), desc="Incident Anomaly Reproduction"):
        try:
            incident_row = read_incident_sample_csv(sample_path)
            INCIDENT_TICKET_INFO = format_incident_ticket_info(incident_row)
            logger.info(f"[MAIN] Processing Incident Sample {sample_id} : {str(incident_row.get('name', ''))[:120]}")

            output_dir = os.path.join(output_base_dir, f"{sample_id}_{DB_NAME}")
            os.makedirs(output_dir, exist_ok=True)
            # Preserve provenance: store the raw record row alongside the sample.
            with open(os.path.join(output_dir, "incident_source.json"), "w", encoding="utf-8") as fh:
                json.dump({"sample_id": sample_id, "sample_path": sample_path, "record": incident_row}, fh, indent=4, ensure_ascii=False)

            # Docker container initialization (fresh unique name per sample).
            initialization_times = 0
            docker_init_status = False
            DOCKER_NAME = make_unique_container_name(BASE_DOCKER_NAME)
            fast_reset_state = {
                "enabled": FAST_RESET_ENABLED,
                "snapshot_ready": False,
            }
            logger.info(f"[MAIN] Initializing Docker Container [{DOCKER_NAME}] ...")
            while initialization_times < RETRY_TIMES:
                docker_init_status = docker_container_init(DOCKER_NAME, container_config)
                if docker_init_status is True:
                    logger.info(f"[MAIN] Docker Container ``{DOCKER_NAME}`` Initialized Successfully.")
                    break
                logger.error(f"[MAIN][ERROR] Error initializing ``{DOCKER_NAME}''. Retrying ({initialization_times}/{RETRY_TIMES})...")
                initialization_times += 1
                time.sleep(5)
                docker_init_status, DOCKER_NAME = recreate_container_with_new_name(DOCKER_NAME, BASE_DOCKER_NAME, container_config)
                if docker_init_status is True:
                    logger.info(f"[MAIN] Docker Container ``{DOCKER_NAME}`` Initialized Successfully.")
                    break
            if docker_init_status is False:
                raise RuntimeError(f"[MAIN][ERROR] Docker Container `{DOCKER_NAME}` initialization failed after {RETRY_TIMES} retries.")

            # Initialize database in container.
            logger.info("[MAIN] Initializing Database in Docker Container...")
            tmp_dbms_name = DBMS.replace(" ", "_")
            tmp_directory = initialize_shared_tmp_directory(base_dir, tmp_dbms_name)
            init_db_config = {
                "dbms": DBMS,
                "base_dir": base_dir,
                "db_host": SYSTEM_USER_CONFIG_JSON['DB_HOST'],
                "db_port": SYSTEM_USER_CONFIG_JSON['DB_PORT'],
                "db_name": DB_NAME,
                "db_type": DB_TYPE,
                "db_schema": DB_SCHEMA_NAME,
                "db_user": SYSTEM_USER_CONFIG_JSON['DB_USER'],
                "password": SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
                "timeout": TIMEOUTS.get(DB_NAME, 3600),
                "output_dir": output_dir,
                "tmp_directory": tmp_directory,
            }
            initialization_times = 0
            docker_status = False
            while initialization_times < RETRY_TIMES:
                docker_status = docker_container_init_db(DOCKER_NAME, init_db_config, session_name=DOCKER_NAME)
                if docker_status is True:
                    logger.info(f"[MAIN] Database `{DB_NAME}` Initialization in `{DOCKER_NAME}` Succeeded.")
                    break
                logger.error(f"[MAIN][ERROR] Database `{DB_NAME}` Initialization in `{DOCKER_NAME}` Failed. Retrying ({initialization_times}/{RETRY_TIMES})...")
                initialization_times += 1
                time.sleep(5)
                _, DOCKER_NAME = recreate_container_with_new_name(DOCKER_NAME, BASE_DOCKER_NAME, container_config)
            if docker_status is False:
                raise RuntimeError(f"[MAIN][ERROR] Database `{DB_NAME}` initialization in `{DOCKER_NAME}` failed after {RETRY_TIMES} retries.")

            build_reproduction_baseline_snapshot(
                DOCKER_NAME,
                init_db_config,
                output_dir,
                fast_reset_state,
                logger,
                reason="initial import",
            )

            # Obtain the base database schema for the prompt (pre-modification).
            db_config = {
                'host': SYSTEM_USER_CONFIG_JSON['DB_HOST'],
                'port': SYSTEM_USER_CONFIG_JSON['DB_PORT'],
                'host_port': HOST_PORT,
                'user': SYSTEM_USER_CONFIG_JSON['DB_USER'],
                'password': SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
                'db_name': DB_NAME,
                'schema': DB_SCHEMA_NAME,
                'dbms': DBMS,
            }
            database_schema_path = os.path.join(database_schema_dir, f"{DB_NAME}_schema.json")
            if os.path.exists(database_schema_path):
                with open(database_schema_path, 'r', encoding="utf-8") as file:
                    database_schema = json.load(file)
            else:
                database_schema = obtain_database_schema(db_config, database_schema_path, DOCKER_NAME)

            DB_SCHEMA = ""
            DB_ROWS = ""
            for table_info in database_schema['table_info']:
                DB_ROWS += f"- Table {table_info['table']} with {table_info['rows']} rows.\n"
                DB_SCHEMA += table_info["ddl"] + "\n\n"

            # Build the reproduction prompt with a de-identified DB host.
            SYSTEM_USER_CONFIG_JSON['DB_NAME'] = DB_NAME
            FAKE_SYSTEM_USER_CONFIG_JSON = SYSTEM_USER_CONFIG_JSON.copy()
            FAKE_SYSTEM_USER_CONFIG_JSON['DB_HOST'] = FAKE_DATABASE_IP
            input_information = synthesize_anomaly_reproduction_prompt_template.format(
                MAX_TURN_NUM=MAX_TURN_NUM,
                SYSTEM_HARDWARE_CONFIG=SYSTEM_HARDWARE_CONFIG_STR,
                SYSTEM_USER_CONFIG=FAKE_SYSTEM_USER_CONFIG_JSON,
                DBMS=DBMS,
                DB_NAME=DB_NAME,
                DB_SCHEMA_NAME=PROMPT_DB_SCHEMA_NAME,
                DB_SCHEMA=DB_SCHEMA,
                DB_ROWS=DB_ROWS,
                INCIDENT_TICKET_INFO=INCIDENT_TICKET_INFO,
                SKILLS=SKILLS,
            )

            system_opt = {
                'docker_name': DOCKER_NAME,
                'base_docker_name': BASE_DOCKER_NAME,
                'container_config': container_config,
                'init_db_config': init_db_config,
                'system_user_config': SYSTEM_USER_CONFIG_JSON,
                'dbms': DBMS,
                'db_config': db_config,
                'docker_main_dir': base_dir,
                'output_mes_path': os.path.join(output_dir, "llm_messages.json"),
                'output_base_dir': output_dir,
                'fast_reset_state': fast_reset_state,
            }
            anomaly_eval_opt = {
                # The reproduction-oriented evaluator is copied into each sample
                # directory as anomaly_eval.py and executed from there (it accepts
                # --min-abs-delta-seconds / --anomaly-type / --success-criterion).
                # The protected legacy Scripts/anomaly_eval.py is intentionally NOT
                # invoked here.
                "anomaly_eval_source_path": os.path.join(base_dir, 'Scripts', 'anomaly_eval_rep.py'),
                "host": SYSTEM_USER_CONFIG_JSON['DB_HOST'],
                "port": SYSTEM_USER_CONFIG_JSON['DB_PORT'],
                "user": SYSTEM_USER_CONFIG_JSON['DB_USER'],
                "password": SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
                "database": SYSTEM_USER_CONFIG_JSON['DB_NAME'],
                "schema": DB_SCHEMA_NAME,
                "dbms": DBMS,
                "success_threshold_percent": SUCCESS_THRESHOLD_PERCENT,
                "min_abs_delta_seconds": MIN_ABS_DELTA_SECONDS,
            }
            config_opt = {
                "system_opt": system_opt,
                "llm_opt": llm_opt,
                "anomaly_eval_opt": anomaly_eval_opt,
                "max_turn_num": MAX_TURN_NUM,
                "median_runs": median_runs,
                "explore_sql_timeout": EXPLORE_SQL_TIMEOUT,
                "explore_bash_timeout": EXPLORE_BASH_TIMEOUT,
                "explore_python_timeout": EXPLORE_PYTHON_TIMEOUT,
            }

            # Copy the eval script in as anomaly_eval.py so the sample directory
            # holds it even when reference is helpful during the loop; the finish
            # validation re-copies to guarantee it is present on success.
            shutil.copy(anomaly_eval_opt['anomaly_eval_source_path'], os.path.join(output_dir, "anomaly_eval.py"))

            start_time = time.time()
            finish_synthesized, DOCKER_NAME = llm_infer_reproduce(input_information, config_opt, logger)
            endtime = time.time()
            inference_times.append(endtime - start_time)

            if not finish_synthesized:
                logger.error(f"[MAIN][== SYNTHESIS FAILED ==] Incident sample {sample_id} did not yield a validated reproduction.")

            # Delete the sample's container and tmux session.
            initialization_times = 0
            while initialization_times < RETRY_TIMES:
                if docker_container_del(DOCKER_NAME) is True:
                    break
                logger.error(f"[MAIN][ERROR] Error deleting ``{DOCKER_NAME}''. Retrying ({initialization_times}/{RETRY_TIMES})...")
                initialization_times += 1
                time.sleep(5)
            logger.info(f"[MAIN] Docker Container ``{DOCKER_NAME}`` Deleted.")
            kill_tmux_session(DOCKER_NAME)

        except Exception as exc:
            # Never let a single noisy Incident record abort the whole batch.
            logger.error(f"[MAIN][ERROR][== SYNTHESIS FAILED ==] Incident sample {sample_id} raised an error: {exc}")
            try:
                docker_container_del(DOCKER_NAME)
                kill_tmux_session(DOCKER_NAME)
            except Exception:
                pass
            continue

    # Defensive sweep of any leftover container/session before reporting done.
    remove_containers_by_prefix(BASE_DOCKER_NAME)
    kill_tmux_sessions_by_prefix(BASE_DOCKER_NAME)

    logger.info("[MAIN] LLM Inference Completed, with inference times : {}".format(str(inference_times)))
