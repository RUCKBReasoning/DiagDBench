import os
import json
import shutil
import argparse

from utils import normalize_dbms, resolve_path_from_base, resolve_db_type

# ============================================================================
# Key-information extraction for the Online Diagnosis Incident anomaly REPRODUCTION pipeline.
#
# Why a dedicated ``_rep`` variant (instead of reusing test_extract_key_info.py)
# --------------------------------------------------------------------------
# ``test_extract_key_info.py`` assumes a FLAT synthesized-anomaly layout:
#
#     <synthesized_anomaly_directory>/
#         config.json                 <- one shared config at the root
#         <idx>_<db>/                 <- one sample per sub-directory
#         ...
#
# The reproduction pipeline (test_syn_ano_rep.py, driven by *_ano_syn_rep.sh)
# instead writes a TWO-LEVEL layout, one timestamped run directory per sample:
#
#     <reproduction_summary_directory>/            (e.g. Output/0_ano_syn_rep)
#         test_syn_ano_rep_<timestamp>/
#             config.json             <- per-run config lives HERE, not at root
#             log.txt
#             <idx>_<db>/             <- the actual sample core files
#         test_syn_ano_rep_<timestamp>/
#             ...
#
# On top of that the reproduction flow differs in three ways that make the
# legacy extractor unsafe here:
#   1. Some run directories are INCOMPLETE (only ``incident_source.json`` was
#      written before the run was aborted); the legacy extractor would crash on
#      ``shutil.copyfile`` of a missing core file.
#   2. The same ``<idx>`` can appear under several timestamped runs (a retried
#      record); ``anomaly_id`` must stay unique in ``key_information.json``.
#   3. The authoritative evaluator is the reproduction-oriented
#      ``Scripts/anomaly_eval_rep.py`` (it understands --success-criterion /
#      --anomaly-type / --min-abs-delta-seconds), NOT the legacy
#      ``Scripts/anomaly_eval.py``.
#   4. The legacy extractor calls ``exit(0)`` (not ``continue``) the first time a
#      sample's database is outside the allow-list, silently truncating the run.
#
# This script is therefore invoked ONCE PER SAMPLE by the shell driver
# (2607201127_rep_eval*.sh), so a single malformed / incomplete sample can be
# skipped without aborting the whole traversal. Each successful invocation:
#   * copies the 9 LLM-authored core files + the reproduction evaluator (as
#     ``anomaly_eval.py``) into ``<output_directory>/<idx>_<db>/``;
#   * writes the decoupled per-sample ``config.json`` consumed downstream by
#     unified ``test_filter_syn_data.py``;
#   * appends / updates this sample's entry in the aggregate
#     ``<output_directory>/key_information.json`` (dedup by ``anomaly_id``; a
#     later timestamped run overwrites an earlier one).
#
# Example (single sample):
#   python test_extract_key_info_rep.py \
#       --sample-dir Output/0_ano_syn_rep/test_syn_ano_rep_260717201836/1_tpch_1 \
#       --run-config Output/0_ano_syn_rep/test_syn_ano_rep_260717201836/config.json \
#       --output-directory Output/0_key_info_rep
# ============================================================================


# The 9 LLM-authored core files every complete reproduction sample must contain.
# ``anomaly_eval.py`` is deliberately NOT in this list: the sample ships its own
# copy, but we always overwrite it with the canonical reproduction evaluator so
# the filtering stage never runs a stale evaluator.
CORE_FILE_NAMES = [
    'anomaly_description.json',
    'user_phenomenon_description.txt',
    'initialization_workload.sql',
    'verified_workload.sql',
    'merged_workload.sql',
    'merged_workload_performance.json',
    'data_modification.sql',
    'data_feature_summarization.txt',
    'anomaly_inject.sh',
]

# LLM-declared manifestation categories. These MUST stay in sync with
# ``anomaly_eval_rep.py`` (``--success-criterion``) and ``test_syn_ano_rep.py``
# so the value recorded here is understood verbatim by the evaluator. ``AUTO``
# preserves the historical broadened-OR judgement when the record does not
# declare a category.
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

    Mirrors ``test_syn_ano_rep.normalize_success_criterion_value`` (canonical
    underscore form + a small synonym set) so the value we persist is always one
    the evaluator recognizes. Unknown / missing values degrade to ``AUTO``.
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


def normalize_anomaly_type_value(value):
    """Best-effort extraction of the anomaly ``Type`` (OLAP / OLTP / BOTH).

    Mirrors ``test_syn_ano_rep.extract_anomaly_type`` so the value recorded in
    ``key_information.json`` matches what the evaluator expects (``--anomaly-type``).
    """
    normalized = str(value or "BOTH").strip().upper().replace(" ", "")
    if normalized in {"OLAP", "OLTP", "BOTH"}:
        return normalized
    if normalized in {"OLAP+OLTP", "OLTP+OLAP"}:
        return "BOTH"
    return "BOTH"


def parse_sample_dir_name(sample_dir_name):
    """Split a ``<idx>_<db>`` sample directory name into ``(anomaly_id, db_name)``.

    Returns ``None`` when the name is not in the expected ``<digits>_<db>`` form
    (e.g. ``test_syn_ano_rep_<ts>`` run directories are rejected here).
    """
    parts = sample_dir_name.split("_", 1)
    if len(parts) != 2 or not parts[0].isdigit():
        return None
    return int(parts[0]), parts[1]


def load_run_config(run_config_path):
    """Load the per-run ``config.json`` that sits alongside the sample directory.

    The reproduction run directory stores exactly one ``config.json`` (see the
    module docstring). Absence is a hard error because the decoupled per-sample
    config we emit relies on its ``dbms`` / ``system_user_config`` / thresholds.
    """
    if not os.path.isfile(run_config_path):
        raise FileNotFoundError(
            f"Reproduction run config not found: {run_config_path}"
        )
    with open(run_config_path, "r", encoding="utf-8") as file:
        return json.load(file)


def resolve_schema_name(dbms, db_name, run_config):
    """Resolve the schema name exactly as the reproduction flow did.

    ``test_syn_ano_rep.py`` forces ``DB_SCHEMA_NAME = DB_NAME`` for MySQL (the
    per-database schema) and uses the configured ``database_schema_name``
    (default ``diagnosis``) for PostgreSQL. Replicating this here keeps the
    evaluator's ``--schema`` identical to the one that validated the sample.
    """
    if normalize_dbms(dbms) == "mysql":
        return db_name
    return run_config.get("database_schema_name", "diagnosis")


def build_sample_config(dbms, db_name, database_schema_name, run_config):
    """Build the decoupled per-sample ``config.json`` for the filtering stage.

    Carries every field ``test_filter_syn_data.py`` needs to reconstruct the
    exact evaluation the reproduction performed: DBMS, database / schema names,
    hardware + user configs, the reproduction-judgement thresholds, and the
    ``db_type`` (dump vs load_data) used to initialize the container database.
    """
    sample_config = {
        "dbms": dbms,
        "database_name": db_name,
        "database_schema_name": database_schema_name,
        "db_type": resolve_db_type(run_config, dbms),
        "system_hardware_config": run_config.get("system_hardware_config", {}),
        "system_user_config": run_config.get("system_user_config", {}),
        # Reproduction-oriented judgement knobs (see anomaly_eval_rep.py). Kept in
        # the per-sample config so the filtering stage judges each sample with the
        # SAME thresholds the reproduction used.
        "success_threshold_percent": run_config.get("success_threshold_percent", 20.0),
        "min_abs_delta_seconds": run_config.get("min_abs_delta_seconds", 0.0),
    }
    return sample_config


def read_anomaly_description(anomaly_description_path):
    """Load ``anomaly_description.json`` returning ``{}`` on any read/parse error."""
    if not os.path.exists(anomaly_description_path):
        return {}
    try:
        with open(anomaly_description_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return {}


def upsert_key_information(key_info_path, entry):
    """Insert or replace ``entry`` in the aggregate ``key_information.json``.

    The file is a JSON list of per-sample dicts keyed by ``anomaly_id``. Because
    the shell driver walks the timestamped run directories in ascending order, a
    later (more recent) run for the same ``anomaly_id`` naturally overwrites an
    earlier one here. The list is kept sorted by ``anomaly_id`` for stable,
    human-readable output. Read-modify-write is safe under the serial per-sample
    invocation the shell driver uses.
    """
    key_information = []
    if os.path.isfile(key_info_path):
        try:
            with open(key_info_path, "r", encoding="utf-8") as file:
                loaded = json.load(file)
            if isinstance(loaded, list):
                key_information = loaded
        except (OSError, json.JSONDecodeError):
            # A corrupt aggregate is rebuilt from scratch rather than aborting the
            # whole traversal; every already-extracted sample is re-appended on
            # its own next run if needed.
            key_information = []

    key_information = [
        item for item in key_information
        if item.get("anomaly_id") != entry["anomaly_id"]
    ]
    key_information.append(entry)
    key_information.sort(key=lambda item: item.get("anomaly_id", 0))

    with open(key_info_path, "w", encoding="utf-8") as file:
        json.dump(key_information, file, indent=4, ensure_ascii=False)


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(
        description="Key-information extraction for a SINGLE Online Diagnosis Incident anomaly "
                    "reproduction sample (two-level output layout)."
    )
    parser.add_argument(
        '--sample-dir', type=str, required=True,
        help="Path to one reproduction sample directory named '<idx>_<db>' "
             "(e.g. .../test_syn_ano_rep_<ts>/1_tpch_1).",
    )
    parser.add_argument(
        '--run-config', type=str, required=False, default=None,
        help="Path to the per-run config.json sitting in the sample's parent "
             "(timestamped) directory. Defaults to '<parent_of_sample>/config.json'.",
    )
    parser.add_argument(
        '--output-directory', type=str, required=True,
        help="Destination key-info directory (e.g. Output/0_key_info_rep). The "
             "extracted sample is written to '<output-directory>/<idx>_<db>' and "
             "its entry appended to '<output-directory>/key_information.json'.",
    )
    args = parser.parse_args()

    sample_dir = resolve_path_from_base(args.sample_dir, base_dir)
    output_directory = resolve_path_from_base(args.output_directory, base_dir)

    # ---- Validate the sample directory & its naming. ----
    if not os.path.isdir(sample_dir):
        print(f"[EXTRACT_REP][ERROR] Sample directory does not exist: {sample_dir}")
        return 2

    sample_dir_name = os.path.basename(os.path.normpath(sample_dir))
    parsed = parse_sample_dir_name(sample_dir_name)
    if parsed is None:
        print(f"[EXTRACT_REP][ERROR] Sample directory name is not '<idx>_<db>': "
              f"{sample_dir_name}. Skipping.")
        return 2
    anomaly_id, db_name = parsed

    # ---- Reject incomplete samples (missing any core file). ----
    # Aborted reproduction runs contain only ``incident_source.json``; those are
    # skipped so a partial sample never enters the filtering workload.
    missing_files = [
        file_name for file_name in CORE_FILE_NAMES
        if not os.path.isfile(os.path.join(sample_dir, file_name))
    ]
    if missing_files:
        print(f"[EXTRACT_REP][WARNING] Sample `{sample_dir_name}` is incomplete "
              f"(missing: {missing_files}). Skipping.")
        return 3

    # ---- Locate & load the per-run config.json. ----
    if args.run_config:
        run_config_path = resolve_path_from_base(args.run_config, base_dir)
    else:
        run_config_path = os.path.join(os.path.dirname(os.path.normpath(sample_dir)), "config.json")
    try:
        run_config = load_run_config(run_config_path)
    except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
        print(f"[EXTRACT_REP][ERROR] Failed to load run config for sample "
              f"`{sample_dir_name}`: {exc}. Skipping.")
        return 2

    dbms = run_config.get("dbms", "")
    database_schema_name = resolve_schema_name(dbms, db_name, run_config)

    # ---- Resolve the canonical reproduction evaluator. ----
    anomaly_eval_rep_path = os.path.join(base_dir, "Scripts", "anomaly_eval_rep.py")
    if not os.path.isfile(anomaly_eval_rep_path):
        print(f"[EXTRACT_REP][ERROR] Reproduction evaluator not found: "
              f"{anomaly_eval_rep_path}. Aborting.")
        return 2

    # ---- Prepare the destination sample directory. ----
    os.makedirs(output_directory, exist_ok=True)
    current_output_directory = os.path.join(output_directory, sample_dir_name)
    os.makedirs(current_output_directory, exist_ok=True)

    # ---- Copy the 9 core files + the reproduction evaluator as anomaly_eval.py. ----
    for file_name in CORE_FILE_NAMES:
        shutil.copyfile(
            os.path.join(sample_dir, file_name),
            os.path.join(current_output_directory, file_name),
        )
    shutil.copyfile(
        anomaly_eval_rep_path,
        os.path.join(current_output_directory, "anomaly_eval.py"),
    )

    # ---- Write the decoupled per-sample config.json. ----
    sample_config = build_sample_config(dbms, db_name, database_schema_name, run_config)
    with open(os.path.join(current_output_directory, "config.json"), "w", encoding="utf-8") as file:
        json.dump(sample_config, file, indent=4, ensure_ascii=False)

    # ---- Build & upsert this sample's key-information entry. ----
    anomaly = read_anomaly_description(os.path.join(sample_dir, "anomaly_description.json"))
    entry = {
        'anomaly_id': anomaly_id,
        'dbms': dbms,
        'database_name': db_name,
        'database_schema_name': database_schema_name,
        'anomaly_name': anomaly.get('Name', ''),
        'anomaly_description': anomaly.get('Description', ''),
        'anomaly_type': normalize_anomaly_type_value(anomaly.get('Type', 'BOTH')),
        # LLM-declared manifestation category driving the authoritative success
        # signal in anomaly_eval_rep.py; consumed by test_filter_syn_data.py.
        'success_criterion': normalize_success_criterion_value(anomaly.get('SuccessCriterion', '')),
    }
    key_info_path = os.path.join(output_directory, "key_information.json")
    upsert_key_information(key_info_path, entry)

    print(f"[EXTRACT_REP] Extracted sample `{sample_dir_name}` (anomaly_id={anomaly_id}, "
          f"dbms={dbms}, schema={database_schema_name}) -> {current_output_directory}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
