#!/usr/bin/env python3
"""Revise generated database-anomaly samples from per-sample review suggestions.

Design summary (mirrors the project conventions established by
``test_syn_ano.py`` and reuses the shared Docker / LLM / evaluation helpers):

* Only samples that HAVE a matching ``NNN.txt`` review file under the
  ``Human_Review/<dataset>`` directory are revised by the LLM. Every other
  sample is copied through to the output directory unchanged so the output is a
  complete, self-contained dataset.
* Sample IDs are extracted from the three-digit review file name (``001.txt`` ->
  ``1``) and are strictly validated as integers that match the file name.
* The curated ``fast_environment_reset.md`` and ``ano_revision_skill.md`` skill
  cards are loaded through the same pattern as
  ``test_syn_ano.load_synthesis_skills`` and their guidance is de-duplicated
  against the prompt template so a rule is stated exactly once.
* Initialization and verified workloads are handled as INDEPENDENT fields (no
  immutable separator marker). ``merge_workloads`` optionally folds them into a
  single verified workload when an anomaly needs a combined workload.
* A revision may touch ANOMALY_INFO, DB_MODIFICATION, ANOMALY_INJECTION_SCRIPT,
  INITIALIZATION_WORKLOAD, VERIFIED_WORKLOAD, and PHENOMENON_DESCRIPTION. The
  final sample is written into a structured output sub-directory.
* Logging follows ``utils.set_up_logger``: one clean overall log per dataset
  plus per-sample process reasoning files. There are no separate worker logs.

The revision loop is a multi-turn think -> action -> observation ReAct process.
Before every observation, the database is restored to a clean golden baseline
or fully rebuilt if fast reset is unavailable or unsafe. A sample is published
only after a successful evaluation followed by a valid Finish action. Failed
samples receive configurable independent revision attempts (default: two);
after exhaustion the original sample is copied through. Sample-level work is parallelized across
worker processes, each pinned to a unique Docker port and container prefix.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import logging
import multiprocessing as mp
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

import docker_func  # noqa: E402
from Filter import test_filter_syn_data as filter_helpers  # noqa: E402
import utils  # noqa: E402


DEFAULT_CONFIG_PATH = PROJECT_DIR / "Configs" / "revision" / "example.json"

# Per-dataset raw sample files live one level below the sample directory so the
# revised copy can preserve the sibling ``anomaly_eval_log`` / ``health_report``
# directories untouched.
RAW_SAMPLE_SUBDIR = "raw_sample"

# Curated skill cards injected into the prompt via ``{SKILLS}``. The reset skill
# comes first because it defines the execution discipline shared with
# ``test_syn_ano.py``; unknown files are ignored for this flow.
REVISION_SKILL_FILES = [
    "fast_environment_reset.md",
    "ano_revision_skill.md",
]
MAX_REVISION_ATTEMPTS = 2

DATABASE_INIT_TIMEOUTS = {
    "tpch_1": 1500,
    "imdb": 4500,
    "tpcds_1": 1800,
    "tpch_10": 11800,
    "sqlbrain_ap": 36000,
    "sqlbrain_tp": 14400,
}
DEFAULT_DATABASE_INIT_TIMEOUT_SECONDS = 3600
OBSERVATION_MAX_CHARS = 20000

GB_CONFIG_KEYS = ("Memory", "Memory + Swap", "Storage")

REQUIRED_SAMPLE_FILES = (
    "anomaly_description.json",
    "anomaly_eval.py",
    "anomaly_inject.sh",
    "config.json",
    "data_feature_summarization.txt",
    "data_modification.sql",
    "initialization_workload.sql",
    "merged_workload.sql",
    "merged_workload_performance.json",
    "user_phenomenon_description.txt",
    "verified_workload.sql",
)

# The complete set of fields the LLM returns. ``ACTION`` is control metadata; the
# remaining keys are the revisable sample content (spec point 5).
OUTPUT_KEYS = (
    "ACTION",
    "ANOMALY_INFO",
    "DB_MODIFICATION",
    "ANOMALY_INJECTION_SCRIPT",
    "INITIALIZATION_WORKLOAD",
    "VERIFIED_WORKLOAD",
    "PHENOMENON_DESCRIPTION",
)
CONTENT_KEYS = tuple(key for key in OUTPUT_KEYS if key != "ACTION")

SUCCESS_MARKERS = (
    "Anomaly successfully simulated and verified.",
    "Anomaly successfully reproduced and verified.",
)
TIMEOUT_PATTERN = re.compile(
    r"^\s*#\s*TIMEOUT THRESHOLD:\s*(\d+)\s*$", re.MULTILINE
)
REQUIRED_SAMPLE_CONFIG_KEYS = {
    "dbms",
    "database_name",
    "database_schema_name",
    "system_hardware_config",
    "system_user_config",
}
REQUIRED_SYSTEM_USER_CONFIG_KEYS = {
    "DB_USER",
    "DB_PASSWORD",
    "DB_HOST",
    "DB_PORT",
}

# Generic revision guidance candidates. Any bullet whose meaning is already
# covered by the curated skill card is dropped by ``dedupe_prompt_guidance`` so a
# rule is never stated twice (spec point 3). Only rules NOT present in the skill
# survive and are rendered into the prompt.
CANDIDATE_PROMPT_GUIDANCE = (
    "The injection must use plausible database-level operations. Do not destroy "
    "containers, restart the DBMS, attack the host, or use OS stress tools.",
    "ANOMALY_INJECTION_SCRIPT must be a complete executable shell script that "
    "contains `# TIMEOUT THRESHOLD: <integer seconds>` near the top, after an "
    "optional shebang.",
    "Keep initialization, verification, and injection as separate artifacts and "
    "preserve the business intent of each workload.",
    "Validate the exact SQL and error-code semantics before retaining any "
    "user-facing claim in the phenomenon description.",
    "Empty strings mean retain the latest value; a Refine action must make at "
    "least one real change.",
    "Finish normally uses empty strings to retain the last successfully "
    "evaluated candidate; non-empty Finish fields must exactly match it.",
)


# NOTE: no INITIALIZATION / VERIFIED WORKLOAD separator marker is used anymore
# (spec point 4). The two workloads are independent named fields; the optional
# ``merge_workloads`` control decides whether they are presented and stored as a
# single combined verified workload.
REVISION_PROMPT_TEMPLATE = r"""
# TASK OVERVIEW

You are a professional database administrator with extensive experience in
database operations, workload design, and production incident reproduction.

Revise the supplied anomaly sample strictly according to REVISION_SUGGESTION.
The revised sample must also pass the anomaly evaluator. You may revise any of
the following fields: ANOMALY_INFO, DB_MODIFICATION, ANOMALY_INJECTION_SCRIPT,
INITIALIZATION_WORKLOAD, VERIFIED_WORKLOAD, and PHENOMENON_DESCRIPTION. Do not
introduce goals, claims, or semantics outside the supplied revision suggestion
and target anomaly.

Use a multi-turn think -> action -> observation process. A Refine action is
executed by the harness and its evaluator output is returned as the next
observation. Finish is accepted only after a Refine action has passed.

# EXECUTION RULES

{PROMPT_GUIDANCE}
- ANOMALY_INFO must be a string containing the complete valid JSON document to
  store as anomaly_description.json.
- {WORKLOAD_FIELD_RULE}
- Return exactly the keys specified in OUTPUT FORMAT. Do not add keys.
- Do not output prose outside the <think> and <action> blocks.

# DATABASE-CENTRIC GUIDANCE

{SKILLS}

# INPUT INFORMATION

## System Hardware Configuration
{SYSTEM_HARDWARE_CONFIG}

## System User Configuration
{SYSTEM_USER_CONFIG}

## DBMS
{DBMS}

## Database Instance

### Database Name
{DB_NAME}

### Schema Name
{DB_SCHEMA_NAME}

### Database DDL
{DB_SCHEMA}

### Row Count
{DB_ROWS}

### Data Feature Summary
{DATA_FEATURE_SUMMARY}

## Current Data Modification
{DB_MODIFICATION}

## Target Anomaly Information
{ANOMALY_INFO}

## Current Anomaly Injection Script
{ANOMALY_INJECTION_SCRIPT}

## Current Initialization Workload
{INITIALIZATION_WORKLOAD}

## Current Verified Workload
{VERIFIED_WORKLOAD}

## Current User-Observed Phenomenon
{PHENOMENON_DESCRIPTION}

## Revision Suggestion
{REVISION_SUGGESTION}

# OUTPUT FORMAT

Use ACTION `Refine` to test a candidate and ACTION `Finish` after a successful
observation. Every field value must be a JSON string.

<think>
Concise reasoning about the requested correction and previous observation.
</think>
<action>
```json
{OUTPUT_SKELETON}
```
</action>
"""


class RevisionError(RuntimeError):
    """A recoverable per-sample revision failure."""


# ---------------------------------------------------------------------------
# Small IO helpers
# ---------------------------------------------------------------------------
def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def resolve_path(value: Any, base: Path = PROJECT_DIR) -> Path:
    if value is None or not str(value).strip():
        raise ValueError("A required path configuration is empty.")
    path = Path(str(value)).expanduser()
    return path.resolve() if path.is_absolute() else (base / path).resolve()


def get_llm_response(messages: List[str], llm_config: Dict[str, Any]) -> str:
    try:
        import llm_api
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "LLM dependencies are unavailable. Install the project requirements "
            f"from {PROJECT_DIR / 'requirements.txt'}."
        ) from exc
    return llm_api.get_llm_response(messages, llm_config)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def revision_attempt_limit(config: Dict[str, Any]) -> int:
    value = config.get("max_revision_attempts", MAX_REVISION_ATTEMPTS)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 10:
        raise ValueError("max_revision_attempts must be an integer from 1 to 10.")
    return value


def load_config(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    config.setdefault("skills_directory", str(PROJECT_DIR / "Skills"))
    source = config.get("llm_configuration_file")
    if source:
        with resolve_path(source).open("r", encoding="utf-8") as handle:
            source_config = json.load(handle)
        options = dict(
            source_config.get("llm_configuration")
            or source_config.get("judge_llm_configuration")
            or {}
        )
        overrides = config.get("llm_configuration", {})
        if not isinstance(overrides, dict):
            raise ValueError("llm_configuration must be a JSON object.")
        options.update(overrides)
        config["llm_configuration"] = options
    required = {
        "skills_directory",
        "docker_name",
        "docker_port",
        "db_type",
        "max_turn_num",
        "llm_configuration",
    }
    missing = sorted(required - set(config))
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")
    if int(config["max_turn_num"]) < 2:
        raise ValueError("max_turn_num must be at least 2 (Refine then Finish).")
    config["max_revision_attempts"] = revision_attempt_limit(config)
    llm_config = config["llm_configuration"]
    if not isinstance(llm_config, dict):
        raise ValueError("llm_configuration must be a JSON object.")
    env_name = llm_config.get("api_key_env")
    if env_name and not llm_config.get("api_key"):
        llm_config["api_key"] = os.environ.get(str(env_name), "")
    if not llm_config.get("api_key"):
        raise ValueError(
            "llm_configuration.api_key is empty and api_key_env is not set "
            "in the current environment."
        )
    return config


# ---------------------------------------------------------------------------
# Spec point 2: sample-id extraction and validation
# ---------------------------------------------------------------------------
SUGGESTION_NAME_PATTERN = re.compile(r"^(\d{3})\.txt$")


def extract_sample_id(filename: str) -> int:
    """Return the integer sample id encoded by a three-digit ``NNN.txt`` name.

    Raises ``ValueError`` for any name that does not match the strict
    ``<three digits>.txt`` convention or whose integer parse does not round-trip
    back to the original three-digit token (spec point 2).
    """
    match = SUGGESTION_NAME_PATTERN.match(filename)
    if not match:
        raise ValueError(
            f"Suggestion file '{filename}' does not match the NNN.txt convention."
        )
    token = match.group(1)
    sample_id = int(token)
    if f"{sample_id:03d}" != token:
        raise ValueError(
            f"Suggestion file '{filename}' does not round-trip to its id "
            f"({sample_id})."
        )
    return sample_id


def discover_suggestions(
    revision_directory: Path, logger: logging.Logger
) -> Tuple[Dict[int, Path], List[str]]:
    """Map validated sample ids to their review-suggestion txt files.

    Malformed file names are logged and skipped rather than aborting the batch.
    """
    mapping: Dict[int, Path] = {}
    skipped: List[str] = []
    if not revision_directory.is_dir():
        logger.warning(
            "[MAIN][WARNING] Review directory not found: %s", revision_directory
        )
        return mapping, skipped
    for path in sorted(revision_directory.glob("*.txt")):
        try:
            sample_id = extract_sample_id(path.name)
        except ValueError as exc:
            logger.error("[MAIN][ERROR] %s", exc)
            skipped.append(path.name)
            continue
        if sample_id in mapping:
            logger.error(
                "[MAIN][ERROR] Duplicate sample id %d from '%s' (already mapped "
                "to '%s'); skipping.",
                sample_id,
                path.name,
                mapping[sample_id].name,
            )
            skipped.append(path.name)
            continue
        mapping[sample_id] = path
    return mapping, skipped


# ---------------------------------------------------------------------------
# Spec point 3: skill loading, version control, and prompt de-duplication
# ---------------------------------------------------------------------------
def load_revision_skills(
    skills_directory: Path, logger: logging.Logger
) -> Tuple[str, List[Dict[str, Any]]]:
    """Load curated revision skill cards and a version manifest.

    Mirrors ``test_syn_ano.load_synthesis_skills``: the folder is optional, files
    are wrapped in labelled blocks, and a missing skill only omits guidance. In
    addition, a manifest recording each file's sha256, size, and mtime is
    returned so the exact skill content used by a run is traceable.
    """
    manifest: List[Dict[str, Any]] = []
    if not skills_directory.is_dir():
        logger.warning(
            "[MAIN][WARNING] Skills directory not found: %s. Proceeding without "
            "curated skills.",
            skills_directory,
        )
        return "(No curated revision skills are available for this run.)", manifest

    available = sorted(
        name for name in os.listdir(skills_directory)
        if name.lower().endswith(".md")
    )
    ordered = [name for name in REVISION_SKILL_FILES if name in available]
    if not ordered:
        logger.warning(
            "[MAIN][WARNING] No revision skill files found under %s.",
            skills_directory,
        )
        return "(No curated revision skills are available for this run.)", manifest

    blocks = []
    for filename in ordered:
        path = skills_directory / filename
        raw = path.read_bytes()
        content = raw.decode("utf-8").strip()
        blocks.append(f"===== SKILL FILE: {filename} =====\n{content}")
        manifest.append(
            {
                "file": filename,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
                "modified_at": time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(path.stat().st_mtime)
                ),
            }
        )
    logger.info(
        "[MAIN] Loaded %d revision skill file(s): %s", len(ordered), ordered
    )
    for record in manifest:
        logger.debug(
            "[MAIN] Skill '%s' sha256=%s bytes=%d",
            record["file"],
            record["sha256"][:12],
            record["bytes"],
        )
    return "\n\n".join(blocks), manifest


def _normalize_tokens(text: str) -> set:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def dedupe_prompt_guidance(
    guidance: Iterable[str],
    skills_text: str,
    logger: logging.Logger,
    threshold: float = 0.6,
) -> str:
    """Drop guidance bullets whose meaning is already covered by the skill card.

    A bullet is treated as redundant when the Jaccard token overlap between the
    bullet and any single skill sentence reaches ``threshold``. Surviving bullets
    are rendered as a markdown list; removed bullets are logged for traceability
    (spec point 3).
    """
    skill_sentences = [
        segment.strip()
        for segment in re.split(r"(?<=[.!?])\s+|\n", skills_text)
        if segment.strip()
    ]
    skill_token_sets = [_normalize_tokens(sentence) for sentence in skill_sentences]

    kept: List[str] = []
    removed: List[str] = []
    for bullet in guidance:
        bullet_tokens = _normalize_tokens(bullet)
        if not bullet_tokens:
            continue
        redundant = False
        for skill_tokens in skill_token_sets:
            if not skill_tokens:
                continue
            overlap = len(bullet_tokens & skill_tokens)
            union = len(bullet_tokens | skill_tokens)
            if union and overlap / union >= threshold:
                redundant = True
                break
        (removed if redundant else kept).append(bullet)

    if removed:
        logger.info(
            "[MAIN] De-duplicated %d prompt guidance line(s) already covered by "
            "the skill card.",
            len(removed),
        )
        for bullet in removed:
            logger.debug("[MAIN] Dropped redundant guidance: %s", bullet)
    if not kept:
        return "- Follow the DATABASE-CENTRIC GUIDANCE skill card below."
    return "\n".join(f"- {bullet}" for bullet in kept)


# ---------------------------------------------------------------------------
# Sample loading and validation
# ---------------------------------------------------------------------------
def raw_sample_dir(sample_directory: Path) -> Path:
    return sample_directory / RAW_SAMPLE_SUBDIR


def load_sample_config(raw_dir: Path) -> Dict[str, Any]:
    config_path = raw_dir / "config.json"
    sample_config = json.loads(read_text(config_path))
    if not isinstance(sample_config, dict):
        raise ValueError(f"Sample config must be a JSON object: {config_path}")

    missing = sorted(REQUIRED_SAMPLE_CONFIG_KEYS - set(sample_config))
    if missing:
        raise ValueError(
            f"Sample config {config_path} is missing required keys: {missing}"
        )

    hardware = sample_config["system_hardware_config"]
    required_hardware = {"CPU", "Memory", "Memory + Swap", "Storage"}
    if not isinstance(hardware, dict):
        raise ValueError(f"system_hardware_config must be an object: {config_path}")
    missing_hardware = sorted(required_hardware - set(hardware))
    if missing_hardware:
        raise ValueError(
            f"Sample config {config_path} is missing hardware keys: "
            f"{missing_hardware}"
        )

    user_config = sample_config["system_user_config"]
    if not isinstance(user_config, dict):
        raise ValueError(f"system_user_config must be an object: {config_path}")
    missing_user = sorted(REQUIRED_SYSTEM_USER_CONFIG_KEYS - set(user_config))
    if missing_user:
        raise ValueError(
            f"Sample config {config_path} is missing user config keys: "
            f"{missing_user}"
        )
    return sample_config


def verify_sample_files(raw_dir: Path) -> None:
    missing = [name for name in REQUIRED_SAMPLE_FILES if not (raw_dir / name).is_file()]
    if missing:
        raise RevisionError(f"Sample is missing required files: {missing}")


def load_candidate(raw_dir: Path) -> Dict[str, str]:
    """Read the current sample content into the independent revisable fields."""
    anomaly_json = json.loads(read_text(raw_dir / "anomaly_description.json"))
    return {
        "ANOMALY_INFO": json.dumps(anomaly_json, ensure_ascii=False, indent=2),
        "DB_MODIFICATION": read_text(raw_dir / "data_modification.sql"),
        "ANOMALY_INJECTION_SCRIPT": read_text(raw_dir / "anomaly_inject.sh"),
        "INITIALIZATION_WORKLOAD": read_text(raw_dir / "initialization_workload.sql"),
        "VERIFIED_WORKLOAD": read_text(raw_dir / "verified_workload.sql"),
        "PHENOMENON_DESCRIPTION": read_text(
            raw_dir / "user_phenomenon_description.txt"
        ),
    }


def validate_candidate(candidate: Dict[str, str]) -> None:
    script = candidate["ANOMALY_INJECTION_SCRIPT"]
    match = TIMEOUT_PATTERN.search(script)
    if not match or int(match.group(1)) <= 0:
        raise RevisionError(
            "ANOMALY_INJECTION_SCRIPT must contain "
            "`# TIMEOUT THRESHOLD: <positive integer>`."
        )
    if not candidate["VERIFIED_WORKLOAD"].strip():
        raise RevisionError("VERIFIED_WORKLOAD must not be empty.")
    try:
        anomaly_info = json.loads(candidate["ANOMALY_INFO"])
    except json.JSONDecodeError as exc:
        raise RevisionError(f"ANOMALY_INFO is not valid JSON: {exc}") from exc
    if not isinstance(anomaly_info, dict):
        raise RevisionError("ANOMALY_INFO must encode a JSON object.")


def merge_action(
    current: Dict[str, str], action: Dict[str, str], merge_workloads: bool
) -> Tuple[Dict[str, str], List[str]]:
    candidate = copy.deepcopy(current)
    changed = []
    for key in CONTENT_KEYS:
        value = action[key]
        if value and value != candidate[key]:
            candidate[key] = value
            changed.append(key)
    if merge_workloads:
        # In merged mode the LLM edits VERIFIED_WORKLOAD only; the initialization
        # content is treated as part of that single workload.
        candidate["INITIALIZATION_WORKLOAD"] = ""
    validate_candidate(candidate)
    return candidate, sorted(changed)


def output_skeleton(merge_workloads: bool) -> str:
    skeleton = {"ACTION": "Refine"}
    for key in CONTENT_KEYS:
        if merge_workloads and key == "INITIALIZATION_WORKLOAD":
            continue
        skeleton[key] = ""
    return json.dumps(skeleton, ensure_ascii=False, indent=2)


def parse_llm_action(response: str, merge_workloads: bool) -> Dict[str, str]:
    action_text = response
    if "<action>" in response:
        action_text = response.split("<action>", 1)[1].split("</action>", 1)[0]
    payload = json.loads(utils.extract_json_object(action_text))
    expected = set(OUTPUT_KEYS)
    if merge_workloads:
        expected.discard("INITIALIZATION_WORKLOAD")
    if set(payload) != expected:
        raise RevisionError(f"LLM output keys must be exactly {sorted(expected)}.")
    if not all(isinstance(payload[key], str) for key in expected):
        raise RevisionError("Every LLM output value must be a string.")
    # Normalize back to the full key set so downstream logic is uniform.
    payload.setdefault("INITIALIZATION_WORKLOAD", "")
    normalized_action = payload["ACTION"].strip().lower()
    if normalized_action not in {"refine", "finish"}:
        raise RevisionError("ACTION must be Refine or Finish.")
    payload["ACTION"] = normalized_action.title()
    return payload


def write_candidate(raw_dir: Path, candidate: Dict[str, str], merge_workloads: bool) -> None:
    initialization = candidate["INITIALIZATION_WORKLOAD"]
    verified = candidate["VERIFIED_WORKLOAD"]
    write_text(raw_dir / "anomaly_inject.sh", candidate["ANOMALY_INJECTION_SCRIPT"])
    write_text(
        raw_dir / "user_phenomenon_description.txt",
        candidate["PHENOMENON_DESCRIPTION"],
    )
    write_text(raw_dir / "data_modification.sql", candidate["DB_MODIFICATION"])
    write_text(raw_dir / "initialization_workload.sql", initialization)
    write_text(raw_dir / "verified_workload.sql", verified)
    if merge_workloads or not initialization.strip():
        merged = verified
    else:
        merged = initialization.rstrip("\n") + "\n" + verified
    write_text(raw_dir / "merged_workload.sql", merged)
    write_json_atomic(
        raw_dir / "anomaly_description.json",
        json.loads(candidate["ANOMALY_INFO"]),
    )


def prepare_candidate_directory(
    sample_directory: Path,
    turn_directory: Path,
    candidate: Dict[str, str],
    merge_workloads: bool,
) -> Path:
    """Materialize a full candidate sample tree and return its raw_sample dir."""
    if turn_directory.exists():
        shutil.rmtree(turn_directory)
    shutil.copytree(sample_directory, turn_directory)
    candidate_raw = raw_sample_dir(turn_directory)
    write_candidate(candidate_raw, candidate, merge_workloads)
    return candidate_raw


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------
def schema_prompt_data(
    schema_directory: Optional[Path], database_name: str
) -> Tuple[str, str]:
    if schema_directory is None or not schema_directory.is_dir():
        return "(Schema file unavailable.)", "(Row counts unavailable.)"
    candidates = [
        schema_directory / f"{database_name}_schema.json",
        schema_directory / str(database_name) / "schema.json",
    ]
    candidates.extend(
        sorted(
            schema_directory.glob(f"*/{database_name}_schema.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    )
    schema = None
    for path in candidates:
        if path.is_file():
            schema = json.loads(read_text(path))
            break
    if not isinstance(schema, dict):
        return "(Schema file unavailable.)", "(Row counts unavailable.)"
    ddl_parts = []
    row_parts = []
    for table in schema.get("table_info", []):
        if not isinstance(table, dict):
            continue
        if table.get("ddl"):
            ddl_parts.append(str(table["ddl"]))
        if table.get("table") is not None:
            row_parts.append(
                f"- Table {table['table']} with {table.get('rows', 'unknown')} rows."
            )
    return "\n\n".join(ddl_parts), "\n".join(row_parts)


def build_prompt(
    schema_directory: Optional[Path],
    raw_dir: Path,
    sample_config: Dict[str, Any],
    candidate: Dict[str, str],
    revision_suggestion: str,
    skills: str,
    prompt_guidance: str,
    merge_workloads: bool,
) -> str:
    dbms = sample_config["dbms"]
    database_name = sample_config["database_name"]
    schema_name = sample_config["database_schema_name"]
    if utils.normalize_dbms(dbms) == "mysql":
        schema_name = database_name
    schema, rows = schema_prompt_data(schema_directory, database_name)
    user_config = sample_config["system_user_config"]
    safe_user_config = dict(user_config)
    if "DB_PASSWORD" in safe_user_config:
        safe_user_config["DB_PASSWORD"] = "[REDACTED]"

    if merge_workloads:
        initialization_display = (
            "[Merged into VERIFIED_WORKLOAD -- do not return this field.]"
        )
        workload_field_rule = (
            "INITIALIZATION_WORKLOAD and VERIFIED_WORKLOAD are merged for this "
            "sample: place all workload SQL in VERIFIED_WORKLOAD and omit "
            "INITIALIZATION_WORKLOAD."
        )
    else:
        initialization_display = candidate["INITIALIZATION_WORKLOAD"] or "(empty)"
        workload_field_rule = (
            "INITIALIZATION_WORKLOAD and VERIFIED_WORKLOAD are independent fields; "
            "keep initialization SQL and verification SQL in their own fields."
        )

    return REVISION_PROMPT_TEMPLATE.format(
        PROMPT_GUIDANCE=prompt_guidance,
        WORKLOAD_FIELD_RULE=workload_field_rule,
        SKILLS=skills,
        SYSTEM_HARDWARE_CONFIG=json.dumps(
            sample_config["system_hardware_config"], ensure_ascii=False, indent=2
        ),
        SYSTEM_USER_CONFIG=json.dumps(
            safe_user_config, ensure_ascii=False, indent=2
        ),
        DBMS=dbms,
        DB_NAME=database_name,
        DB_SCHEMA_NAME=schema_name,
        DB_SCHEMA=schema or "(No DDL found.)",
        DB_ROWS=rows or "(No row counts found.)",
        DATA_FEATURE_SUMMARY=read_text(raw_dir / "data_feature_summarization.txt"),
        DB_MODIFICATION=candidate["DB_MODIFICATION"] or "(empty)",
        ANOMALY_INFO=candidate["ANOMALY_INFO"],
        ANOMALY_INJECTION_SCRIPT=candidate["ANOMALY_INJECTION_SCRIPT"],
        INITIALIZATION_WORKLOAD=initialization_display,
        VERIFIED_WORKLOAD=candidate["VERIFIED_WORKLOAD"],
        PHENOMENON_DESCRIPTION=candidate["PHENOMENON_DESCRIPTION"],
        REVISION_SUGGESTION=revision_suggestion,
        OUTPUT_SKELETON=output_skeleton(merge_workloads),
    )


# ---------------------------------------------------------------------------
# Docker environment + evaluation
# ---------------------------------------------------------------------------
def _hardware_with_gb(hardware: Dict[str, Any]) -> Dict[str, Any]:
    prepared = dict(hardware)
    for key in GB_CONFIG_KEYS:
        value = prepared.get(key)
        if value is not None and not str(value).endswith("GB"):
            prepared[key] = f"{value}GB"
    return prepared


def resolve_fast_reset_enabled(dbms: str, config: Dict[str, Any]) -> bool:
    """Match ``test_syn_ano.py`` fast-reset policy.

    PostgreSQL uses the golden-baseline reset by default. MySQL uses full
    container rebuilds unless its experimental logical-clone path is explicitly
    enabled.
    """
    requested = bool(config.get("fast_reset_enabled", True))
    if utils.normalize_dbms(dbms) != "mysql":
        return requested
    return requested and bool(config.get("mysql_fast_reset_experimental_enabled", False))


def modification_sha256(candidate_raw_dir: Path) -> str:
    return hashlib.sha256(
        (candidate_raw_dir / "data_modification.sql").read_bytes()
    ).hexdigest()


def initialize_environment(
    config: Dict[str, Any],
    sample_config: Dict[str, Any],
    candidate_raw_dir: Path,
    process_directory: Path,
    container_name: Optional[str],
    base_prefix: Any = None,
    docker_port: Optional[int] = None,
    logger: Optional[logging.Logger] = None,
) -> Any:
    """Fully rebuild a clean baseline and capture a golden snapshot.

    This is the correctness fallback used by ``test_syn_ano.py``: recreate the
    container, initialize the database, re-apply data modification, then build a
    fresh golden baseline for later per-turn resets.
    """
    legacy_call = logger is None and isinstance(base_prefix, logging.Logger)
    if legacy_call:
        logger = base_prefix
        base_prefix = str(config["docker_name"])
        docker_port = int(config["docker_port"])
    if logger is None or base_prefix is None or docker_port is None:
        raise TypeError(
            "initialize_environment requires either the legacy 6-argument "
            "contract or base_prefix, docker_port, and logger."
        )

    dbms = sample_config["dbms"]
    database_name = sample_config["database_name"]
    if not database_name:
        raise RevisionError("database_name is empty.")
    schema_name = sample_config["database_schema_name"]
    if utils.normalize_dbms(dbms) == "mysql":
        schema_name = database_name
    user_config = copy.deepcopy(sample_config["system_user_config"])
    user_config["DB_NAME"] = database_name
    hardware = _hardware_with_gb(sample_config["system_hardware_config"])
    container_config = docker_func.build_container_config(
        dbms, hardware, int(docker_port), user_config
    )
    if container_name:
        docker_func.kill_tmux_session(container_name)
        docker_func.docker_container_del(container_name)
    container_name = docker_func.make_unique_container_name(base_prefix)
    container_name = filter_helpers._legacy_initialize_docker_container(
        container_name, base_prefix, container_config, logger
    )
    tmp_name = os.path.join(dbms.replace(" ", "_"), base_prefix)
    init_config = {
        "dbms": dbms,
        "base_dir": str(PROJECT_DIR),
        "db_host": user_config["DB_HOST"],
        "db_port": user_config["DB_PORT"],
        "db_name": database_name,
        "db_type": sample_config.get("db_type", config["db_type"]),
        "db_schema": schema_name,
        "db_user": user_config["DB_USER"],
        "password": user_config["DB_PASSWORD"],
        "timeout": DATABASE_INIT_TIMEOUTS.get(
            database_name, DEFAULT_DATABASE_INIT_TIMEOUT_SECONDS
        ),
        "output_dir": str(process_directory),
        "tmp_directory": docker_func.initialize_shared_tmp_directory(
            str(PROJECT_DIR), tmp_name
        ),
    }
    container_name = filter_helpers._legacy_initialize_database(
        container_name, base_prefix, init_config, container_config, database_name, logger
    )
    modification_path = candidate_raw_dir / "data_modification.sql"
    if modification_path.stat().st_size > 0 and not filter_helpers._legacy_run_data_modification(
        container_name,
        dbms,
        user_config,
        str(modification_path),
        database_name,
        str(process_directory),
        logger,
    ):
        raise RevisionError("data_modification.sql execution failed.")
    context = {
        "dbms": dbms,
        "database_name": database_name,
        "schema_name": schema_name,
        "user_config": user_config,
    }
    fast_reset_state = {
        "enabled": resolve_fast_reset_enabled(dbms, config),
        "snapshot_ready": False,
    }
    if fast_reset_state["enabled"]:
        try:
            snapshot_config = dict(init_config)
            snapshot_config["output_dir"] = str(process_directory)
            snapshot_ok = docker_func.docker_container_build_baseline_snapshot(
                container_name, snapshot_config, session_name=container_name
            )
            fast_reset_state["snapshot_ready"] = bool(snapshot_ok)
            if snapshot_ok:
                logger.info(
                    "[ENV] Golden baseline captured for subsequent observations."
                )
            else:
                logger.warning(
                    "[ENV][WARNING] Golden baseline capture failed; subsequent "
                    "observations will use full container rebuilds."
                )
        except Exception as exc:  # fast reset is an optimization, never a blocker
            logger.warning(
                "[ENV][WARNING] Golden baseline capture raised [%s]; subsequent "
                "observations will use full container rebuilds.",
                exc,
            )
    environment = {
        "container_config": container_config,
        "init_config": init_config,
        "fast_reset_state": fast_reset_state,
        "baseline_modification_sha256": modification_sha256(candidate_raw_dir),
    }
    if legacy_call:
        return container_name, container_config, init_config, context
    return container_name, context, environment


def restore_environment_for_observation(
    config: Dict[str, Any],
    sample_config: Dict[str, Any],
    candidate_raw_dir: Path,
    process_directory: Path,
    container_name: Optional[str],
    context: Optional[Dict[str, Any]],
    environment: Optional[Dict[str, Any]],
    base_prefix: str,
    docker_port: int,
    logger: logging.Logger,
    turn: int,
    force_full_rebuild: bool,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """Restore a clean database immediately before an Observation.

    This mirrors ``test_syn_ano.reinitialize_docker_environment_for_next_turn``:
    use the container-local golden baseline when it is valid, otherwise rebuild
    the container/database, replay data modification, and recapture a baseline.
    An orchestration timeout always forces a full rebuild to reap potentially
    runaway in-container processes.
    """
    modification_changed = (
        environment is not None
        and environment.get("baseline_modification_sha256")
        != modification_sha256(candidate_raw_dir)
    )
    state = environment.get("fast_reset_state", {}) if environment else {}
    can_fast_reset = (
        container_name
        and context
        and environment
        and not force_full_rebuild
        and not modification_changed
        and state.get("enabled")
        and state.get("snapshot_ready")
    )
    if can_fast_reset:
        logger.info(
            "[ENV] Fast-resetting current container to golden baseline before "
            "Observation [Turn %d].",
            turn,
        )
        try:
            reset_config = dict(environment["init_config"])
            reset_config["output_dir"] = str(process_directory)
            if docker_func.docker_container_fast_reset(
                container_name, reset_config, session_name=container_name
            ):
                logger.info(
                    "[ENV] Golden-baseline restore succeeded before Observation "
                    "[Turn %d].",
                    turn,
                )
                return container_name, context, environment
            logger.warning(
                "[ENV][WARNING] Fast reset failed before Observation [Turn %d]; "
                "falling back to full rebuild.",
                turn,
            )
        except Exception as exc:
            logger.warning(
                "[ENV][WARNING] Fast reset raised before Observation [Turn %d] "
                "[%s]; falling back to full rebuild.",
                turn,
                exc,
            )
    elif force_full_rebuild:
        logger.info(
            "[ENV] Forcing full rebuild before Observation [Turn %d] because the "
            "previous evaluation timed out.",
            turn,
        )
    elif modification_changed:
        logger.info(
            "[ENV] Data modification changed; rebuilding the baseline before "
            "Observation [Turn %d].",
            turn,
        )
    else:
        logger.info(
            "[ENV] Initializing a clean environment before Observation [Turn %d].",
            turn,
        )

    return initialize_environment(
        config,
        sample_config,
        candidate_raw_dir,
        process_directory,
        container_name,
        base_prefix,
        docker_port,
        logger,
    )


def timeout_for_candidate(candidate: Dict[str, str], raw_dir: Path, config: Dict[str, Any]) -> int:
    match = TIMEOUT_PATTERN.search(candidate["ANOMALY_INJECTION_SCRIPT"])
    if match is None:
        raise RevisionError(
            "Cannot extract TIMEOUT THRESHOLD from ANOMALY_INJECTION_SCRIPT."
        )
    script_timeout = int(match.group(1))
    median_runs = max(1, int(config.get("median_runs", 3)))
    performance = json.loads(read_text(raw_dir / "merged_workload_performance.json"))
    workload_seconds = sum(
        max(0.0, float(item.get("execution_time", 0.0)))
        for item in performance
        if isinstance(item, dict)
        and item.get("success")
        and isinstance(item.get("execution_time"), (int, float))
    )
    return script_timeout + int(median_runs * 2.5 * workload_seconds)


def evaluate_candidate(
    container_name: str,
    context: Dict[str, Any],
    candidate_raw_dir: Path,
    log_directory: Path,
    candidate: Dict[str, str],
    config: Dict[str, Any],
) -> Tuple[bool, str, bool]:
    timeout = timeout_for_candidate(candidate, candidate_raw_dir, config)
    evaluator = candidate_raw_dir / "anomaly_eval.py"
    command = [
        "python", str(evaluator),
        "--script-path", str(candidate_raw_dir / "anomaly_inject.sh"),
        "--verified-workload-path", str(candidate_raw_dir / "verified_workload.sql"),
        "--initialization-workload-path",
        str(candidate_raw_dir / "initialization_workload.sql"),
        "--host", str(context["user_config"]["DB_HOST"]),
        "--port", str(context["user_config"]["DB_PORT"]),
        "--user", str(context["user_config"]["DB_USER"]),
        "--database", str(context["database_name"]),
        "--schema", str(context["schema_name"]),
        "--password", str(context["user_config"]["DB_PASSWORD"]),
        "--timeout", str(timeout),
        "--median-runs", str(max(1, int(config.get("median_runs", 3)))),
    ]
    if filter_helpers._legacy_anomaly_eval_supports_flag(str(evaluator), "--dbms"):
        command[2:2] = ["--dbms", str(context["dbms"])]
    log_path = log_directory / "observation.log"
    state, comment = docker_func.run_docker_exec_observation(
        container_name, command, str(log_path), timeout, session_name=container_name
    )
    log_text = read_text(log_path) if log_path.exists() else ""
    marker_found = any(marker in log_text for marker in SUCCESS_MARKERS)
    passed = bool(state and marker_found)
    observation = (
        f"Evaluation {'PASSED' if passed else 'FAILED'} "
        f"(command_ok={state}, success_marker={marker_found}, timeout={timeout}s).\n"
        + utils.prune_obser_log(str(log_path), OBSERVATION_MAX_CHARS)
    )
    if comment:
        observation += "\n\nHarness comments:\n" + comment
    observation_timed_out = bool(
        not state and comment and "Command timed out" in comment
    )
    if not state:
        docker_func.reset_tmux_window(
            session_name=container_name, window_name="observation"
        )
    return passed, observation, observation_timed_out


# ---------------------------------------------------------------------------
# Publishing: structured output storage (spec point 5)
# ---------------------------------------------------------------------------
def publish_sample(
    sample_directory: Path,
    output_directory: Path,
    candidate: Dict[str, str],
    suggestion_path: Path,
    merge_workloads: bool,
) -> None:
    temporary = output_directory.with_name(output_directory.name + ".tmp")
    if temporary.exists():
        shutil.rmtree(temporary)
    shutil.copytree(sample_directory, temporary)
    write_candidate(raw_sample_dir(temporary), candidate, merge_workloads)
    shutil.copy2(suggestion_path, temporary / "revision_suggestion.txt")
    if output_directory.exists():
        shutil.rmtree(output_directory)
    os.replace(temporary, output_directory)


def copy_through_sample(sample_directory: Path, output_directory: Path) -> None:
    """Copy a sample without a review suggestion to the output unchanged."""
    temporary = output_directory.with_name(output_directory.name + ".tmp")
    if temporary.exists():
        shutil.rmtree(temporary)
    shutil.copytree(sample_directory, temporary)
    if output_directory.exists():
        shutil.rmtree(output_directory)
    os.replace(temporary, output_directory)


# ---------------------------------------------------------------------------
# Per-sample process logger (per-sample reasoning; not a "worker log")
# ---------------------------------------------------------------------------


def build_sample_logger(sample_id: int, log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"DB_DIAG.revision.sample_{sample_id}")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


# ---------------------------------------------------------------------------
# Per-sample revision loop
# ---------------------------------------------------------------------------
def run_revision_attempt(
    sample_id: int,
    attempt_index: int,
    sample_directory: Path,
    suggestion_path: Path,
    process_root: Path,
    updated_root: Path,
    schema_directory: Optional[Path],
    skills: str,
    prompt_guidance: str,
    config: Dict[str, Any],
    llm_config: Dict[str, Any],
    merge_workloads: bool,
    base_prefix: str,
    docker_port: int,
) -> Dict[str, Any]:
    raw_dir = raw_sample_dir(sample_directory)
    sample_process = (
        process_root / f"{sample_id:03d}" / f"revision_{attempt_index:02d}"
    )
    sample_process.mkdir(parents=True, exist_ok=True)
    logger = build_sample_logger(sample_id, sample_process / "sample.log")

    result: Dict[str, Any] = {
        "sample_id": sample_id,
        "attempt": attempt_index,
        "status": "failed",
        "turns": 0,
        "error": "",
        "updated_fields": [],
    }
    container_name: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    environment: Optional[Dict[str, Any]] = None
    previous_observation_timed_out = False
    messages: List[str] = []
    try:
        verify_sample_files(raw_dir)
        sample_config = load_sample_config(raw_dir)
        sample_config.setdefault("db_type", config["db_type"])
        candidate = load_candidate(raw_dir)
        original = copy.deepcopy(candidate)
        validate_candidate(candidate)
        prompt = build_prompt(
            schema_directory,
            raw_dir,
            sample_config,
            candidate,
            read_text(suggestion_path),
            skills,
            prompt_guidance,
            merge_workloads,
        )
        messages = [prompt]
        last_passed_candidate: Optional[Dict[str, str]] = None

        for turn in range(1, int(config["max_turn_num"]) + 1):
            turn_directory = sample_process / f"{turn:02d}_turn"
            turn_directory.mkdir(parents=True, exist_ok=True)
            action = None
            parse_errors: List[str] = []
            response = ""
            for attempt in range(1, int(llm_config.get("retry_times", 10)) + 1):
                response = get_llm_response(messages, llm_config)
                write_text(turn_directory / f"llm_response_{attempt:02d}.txt", response)
                try:
                    action = parse_llm_action(response, merge_workloads)
                    break
                except (RevisionError, ValueError, json.JSONDecodeError) as exc:
                    parse_errors.append(str(exc))
                    logger.debug("[TURN %d] parse attempt %d failed: %s", turn, attempt, exc)
            if action is None:
                raise RevisionError(
                    "No valid LLM action after retries: " + "; ".join(parse_errors)
                )
            messages.append(response)
            result["turns"] = turn
            write_json_atomic(turn_directory / "parsed_action.json", action)

            try:
                proposed, changed = merge_action(candidate, action, merge_workloads)
            except RevisionError as exc:
                logger.info("[TURN %d] candidate rejected by validation: %s", turn, exc)
                messages.append(f"Action rejected by validation: {exc}")
                continue

            if action["ACTION"] == "Finish":
                if last_passed_candidate is None:
                    messages.append(
                        "Finish rejected: no Refine candidate has passed the "
                        "anomaly evaluator. Return Refine and test a correction."
                    )
                    continue
                if proposed != last_passed_candidate:
                    messages.append(
                        "Finish rejected: it contains untested changes. Return "
                        "Finish with empty values or values exactly matching the "
                        "last passed candidate."
                    )
                    continue
                publish_sample(
                    sample_directory,
                    updated_root / f"{sample_id:03d}",
                    last_passed_candidate,
                    suggestion_path,
                    merge_workloads,
                )
                result["status"] = "passed"
                result["updated_fields"] = sorted(
                    key for key in CONTENT_KEYS
                    if last_passed_candidate[key] != original[key]
                )
                logger.info(
                    "[TURN %d] Finished. Updated fields: %s",
                    turn,
                    result["updated_fields"],
                )
                break

            if not changed:
                messages.append(
                    "Refine rejected: no field changed. Apply a concrete change "
                    "that follows the revision suggestion."
                )
                continue

            candidate = proposed
            candidate_directory = turn_directory / "Candidate_Sample"
            candidate_raw = prepare_candidate_directory(
                sample_directory, candidate_directory, candidate, merge_workloads
            )
            # Restore the current container to a deterministic baseline
            # immediately before EVERY Observation, then execute anomaly_eval.py.
            # A previous orchestration timeout forces full recreation so runaway
            # injection/evaluator processes cannot race a DB-only reset.
            container_name, context, environment = restore_environment_for_observation(
                config,
                sample_config,
                candidate_raw,
                turn_directory,
                container_name,
                context,
                environment,
                base_prefix,
                docker_port,
                logger,
                turn,
                previous_observation_timed_out,
            )
            passed, observation, previous_observation_timed_out = evaluate_candidate(
                container_name, context, candidate_raw, turn_directory, candidate, config
            )
            write_text(turn_directory / "observation_for_llm.txt", observation)
            logger.info("[TURN %d] Refine evaluated: passed=%s changed=%s", turn, passed, changed)
            if passed:
                last_passed_candidate = copy.deepcopy(candidate)
            messages.append(observation)
        else:
            result["error"] = "Maximum turn count reached without a valid Finish."

        if result["status"] != "passed" and not result["error"]:
            result["error"] = "Revision did not produce a publishable sample."
    except Exception as exc:  # noqa: BLE001 -- recorded per sample, batch continues
        logger.exception("Sample %d revision failed", sample_id)
        result["error"] = str(exc)
    finally:
        write_json_atomic(
            sample_process / "llm_messages.json",
            {f"message_{index}": value for index, value in enumerate(messages)},
        )
        if container_name:
            docker_func.kill_tmux_session(container_name)
            docker_func.docker_container_del(container_name)
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()
    return result


def process_sample(
    sample_id: int,
    sample_directory: Path,
    suggestion_path: Path,
    process_root: Path,
    updated_root: Path,
    schema_directory: Optional[Path],
    skills: str,
    prompt_guidance: str,
    config: Dict[str, Any],
    llm_config: Dict[str, Any],
    merge_workloads: bool,
    base_prefix: str,
    docker_port: int,
) -> Dict[str, Any]:
    """Retry independently up to the configured limit; stop on success."""
    max_attempts = revision_attempt_limit(config)
    attempts: List[Dict[str, Any]] = []
    for attempt_index in range(1, max_attempts + 1):
        logging.getLogger("DB_DIAG.revision").info(
            "[RETRY] Sample %03d: independent Revision %d/%d",
            sample_id, attempt_index, max_attempts,
        )
        cleanup_ok = docker_func.remove_containers_by_prefix(base_prefix)
        docker_func.kill_tmux_sessions_by_prefix(base_prefix)
        if cleanup_ok:
            result = run_revision_attempt(
                sample_id,
                attempt_index,
                sample_directory,
                suggestion_path,
                process_root,
                updated_root,
                schema_directory,
                skills,
                prompt_guidance,
                config,
                copy.deepcopy(llm_config),
                merge_workloads,
                base_prefix,
                docker_port,
            )
        else:
            result = {
                "sample_id": sample_id,
                "attempt": attempt_index,
                "status": "failed",
                "turns": 0,
                "error": (
                    f"Worker cleanup failed for prefix {base_prefix}; "
                    f"port {docker_port} was not reused."
                ),
                "updated_fields": [],
            }
        # Store a detached snapshot. Appending ``result`` itself and then adding
        # ``revision_attempts`` to it creates a circular object graph that
        # ``json.dumps`` cannot serialize.
        attempts.append(copy.deepcopy(result))
        if result["status"] == "passed":
            final_result = copy.deepcopy(result)
            final_result["revision_attempts"] = attempts
            final_result["attempts_used"] = attempt_index
            return final_result

    copy_through_sample(sample_directory, updated_root / f"{sample_id:03d}")
    return {
        "sample_id": sample_id,
        "status": "fallback_original",
        "turns": sum(item["turns"] for item in attempts),
        "error": f"All {max_attempts} independent revision attempts failed; original sample retained.",
        "updated_fields": [],
        "attempts_used": max_attempts,
        "revision_attempts": attempts,
    }


# ---------------------------------------------------------------------------
# Worker process: pulls samples from a shared queue with a pinned port/prefix
# ---------------------------------------------------------------------------
def worker_loop(
    worker_id: int,
    task_queue: "mp.Queue",
    result_queue: "mp.Queue",
    payload: Dict[str, Any],
) -> None:
    base_prefix = f"{payload['docker_name']}_w{worker_id}"
    docker_port = int(payload["docker_port_base"]) + worker_id
    # Sweep any orphan containers/sessions left by an earlier crashed run of this
    # worker slot before starting (matched by the per-worker unique prefix).
    docker_func.remove_containers_by_prefix(base_prefix)
    docker_func.kill_tmux_sessions_by_prefix(base_prefix)

    schema_directory = (
        Path(payload["schema_directory"]) if payload["schema_directory"] else None
    )
    try:
        while True:
            item = task_queue.get()
            if item is None:
                break
            sample_id, sample_dir, suggestion = item
            result = process_sample(
                sample_id,
                Path(sample_dir),
                Path(suggestion),
                Path(payload["process_root"]),
                Path(payload["updated_root"]),
                schema_directory,
                payload["skills"],
                payload["prompt_guidance"],
                payload["config"],
                payload["llm_config"],
                payload["merge_workloads"],
                base_prefix,
                docker_port,
            )
            result["worker_id"] = worker_id
            result_queue.put(result)
    finally:
        docker_func.remove_containers_by_prefix(base_prefix)
        docker_func.kill_tmux_sessions_by_prefix(base_prefix)


# ---------------------------------------------------------------------------
# Dataset driver
# ---------------------------------------------------------------------------
def process_dataset(
    dataset: str,
    raw_root: Path,
    review_root: Path,
    output_root: Path,
    config: Dict[str, Any],
    llm_config: Dict[str, Any],
    schema_directory: Optional[Path],
    workers: int,
    docker_port_base: int,
    merge_workloads: bool,
) -> Dict[str, Any]:
    dataset_raw = raw_root / dataset
    dataset_review = review_root / dataset
    dataset_out = output_root / dataset
    process_root = dataset_out / "_Process_Logs"
    updated_root = dataset_out
    process_root.mkdir(parents=True, exist_ok=True)
    updated_root.mkdir(parents=True, exist_ok=True)

    logger = utils.set_up_logger(str(dataset_out / "log.txt"))
    logger = logging.getLogger("DB_DIAG.revision")
    logger.info("[MAIN] ==== Dataset '%s' revision start ====", dataset)

    if not dataset_raw.is_dir():
        logger.error("[MAIN][ERROR] Raw data directory not found: %s", dataset_raw)
        return {"dataset": dataset, "error": f"missing raw dir: {dataset_raw}"}

    skills, skill_manifest = load_revision_skills(
        resolve_path(config["skills_directory"]), logger
    )
    prompt_guidance = dedupe_prompt_guidance(CANDIDATE_PROMPT_GUIDANCE, skills, logger)
    write_json_atomic(
        dataset_out / "skill_manifest.json",
        {"loaded_at": time.strftime("%Y-%m-%d %H:%M:%S"), "skills": skill_manifest},
    )

    suggestions, malformed = discover_suggestions(dataset_review, logger)

    sample_ids = sorted(
        int(entry.name)
        for entry in dataset_raw.iterdir()
        if entry.is_dir() and entry.name.isdigit()
    )
    to_revise = [sid for sid in sample_ids if sid in suggestions]
    to_copy = [sid for sid in sample_ids if sid not in suggestions]
    logger.info(
        "[MAIN] Dataset '%s': %d samples total, %d with review suggestions, "
        "%d copied through, %d malformed suggestion file(s).",
        dataset,
        len(sample_ids),
        len(to_revise),
        len(to_copy),
        len(malformed),
    )

    # Spec point 1: copy every no-suggestion sample through unchanged and log it.
    for sample_id in to_copy:
        source = dataset_raw / str(sample_id)
        logger.info(
            "[MAIN] Sample %03d has no review suggestion -- copying original "
            "sample through unchanged.",
            sample_id,
        )
        copy_through_sample(source, dataset_out / f"{sample_id:03d}")

    resumed_ids = []
    for sample_id in to_revise:
        published = dataset_out / f"{sample_id:03d}"
        published_raw = raw_sample_dir(published)
        published_suggestion = published / "revision_suggestion.txt"
        if (
            published_suggestion.is_file()
            and all((published_raw / name).is_file() for name in REQUIRED_SAMPLE_FILES)
            and published_suggestion.read_bytes() == suggestions[sample_id].read_bytes()
        ):
            resumed_ids.append(sample_id)

    pending_revisions = [sid for sid in to_revise if sid not in set(resumed_ids)]
    results: List[Dict[str, Any]] = [
        {
            "sample_id": sample_id,
            "status": "passed",
            "turns": 0,
            "error": "",
            "updated_fields": [],
            "attempts_used": 0,
            "revision_attempts": [],
            "resumed": True,
        }
        for sample_id in resumed_ids
    ]
    result_path = dataset_out / "revision_result.json"
    if result_path.is_file():
        previous = json.loads(result_path.read_text(encoding="utf-8"))
        previous_passed = {
            int(row["sample_id"]): row
            for row in previous.get("samples", [])
            if row.get("status") == "passed"
        }
        for index, row in enumerate(results):
            if row["sample_id"] in previous_passed:
                results[index] = copy.deepcopy(previous_passed[row["sample_id"]])
                results[index]["resumed"] = True
    if resumed_ids:
        logger.info(
            "[MAIN] Resuming %d previously published sample(s): %s",
            len(resumed_ids),
            ", ".join(f"{sample_id:03d}" for sample_id in resumed_ids),
        )

    if pending_revisions:
        write_json_atomic(
            result_path,
            {"completed": False, "dataset": dataset,
             "max_revision_attempts": revision_attempt_limit(config), "samples": results},
        )
        effective_workers = max(1, min(int(workers), len(pending_revisions)))
        payload = {
            "docker_name": str(config["docker_name"]),
            "docker_port_base": int(docker_port_base),
            "process_root": str(process_root),
            "updated_root": str(updated_root),
            "schema_directory": str(schema_directory) if schema_directory else "",
            "skills": skills,
            "prompt_guidance": prompt_guidance,
            "config": config,
            "llm_config": llm_config,
            "merge_workloads": merge_workloads,
        }
        ctx = mp.get_context("spawn")
        task_queue: "mp.Queue" = ctx.Queue()
        result_queue: "mp.Queue" = ctx.Queue()
        for sample_id in pending_revisions:
            task_queue.put(
                (
                    sample_id,
                    str(dataset_raw / str(sample_id)),
                    str(suggestions[sample_id]),
                )
            )
        for _ in range(effective_workers):
            task_queue.put(None)

        logger.info(
            "[MAIN] Revising %d sample(s) with %d parallel worker(s); "
            "up to %d independent revisions per sample.",
            len(pending_revisions),
            effective_workers,
            revision_attempt_limit(config),
        )
        processes = [
            ctx.Process(target=worker_loop, args=(wid, task_queue, result_queue, payload))
            for wid in range(effective_workers)
        ]
        for process in processes:
            process.start()
        for _ in range(len(pending_revisions)):
            result = result_queue.get()
            results.append(result)
            status = result["status"]
            (logger.info if status == "passed" else logger.warning)(
                "[MAIN] Sample %03d %s (attempts=%d, turns=%d, fields=%s)%s",
                result["sample_id"],
                status,
                result.get("attempts_used", 1),
                result["turns"],
                result.get("updated_fields", []),
                "" if status == "passed" else f", error={result['error']}",
            )
            write_json_atomic(
                result_path, {"completed": False, "dataset": dataset,
                              "max_revision_attempts": revision_attempt_limit(config),
                              "samples": results}
            )
        for process in processes:
            process.join()

    passed = sum(1 for item in results if item["status"] == "passed")
    double_revision_failed_ids = sorted(
        item["sample_id"]
        for item in results
        if item["status"] == "fallback_original"
    )
    summary = {
        "completed": True,
        "dataset": dataset,
        "total_samples": len(sample_ids),
        "revised": len(to_revise),
        "passed": passed,
        "failed": len(double_revision_failed_ids),
        "max_revision_attempts": revision_attempt_limit(config),
        "failed_sample_ids": double_revision_failed_ids,
        "original_retained_after_failure": len(double_revision_failed_ids),
        # Retained as compatibility aliases for existing result consumers.
        "double_revision_failed_ids": double_revision_failed_ids,
        "original_retained_after_double_failure": len(double_revision_failed_ids),
        "copied_through": len(to_copy),
        "malformed_suggestions": malformed,
        "merge_workloads": merge_workloads,
        "samples": results,
    }
    write_json_atomic(result_path, summary)
    logger.info(
        "[MAIN] ==== Dataset '%s' done: %d revised (%d passed, %d failed), "
        "%d copied through ====",
        dataset,
        len(to_revise),
        passed,
        len(to_revise) - passed,
        len(to_copy),
    )
    if double_revision_failed_ids:
        logger.error(
            "[MAIN][REVISION ATTEMPTS EXHAUSTED] All %d attempts failed for "
            "sample IDs: %s. Original sample data was retained in the output.",
            revision_attempt_limit(config),
            ", ".join(f"{sample_id:03d}" for sample_id in double_revision_failed_ids),
        )
    else:
        logger.info(
            "[MAIN] Revision attempts exhausted IDs: none."
        )
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Revise database anomaly samples from per-sample review "
        "suggestions."
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument(
        "--input-root",
        required=True,
        help="Eval-set root containing Raw_Data and Human_Review.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        required=True,
        help="Dataset sub-directory to process (repeatable, e.g. PG_Syn).",
    )
    parser.add_argument(
        "--output-root", required=True, help="Output root for revised datasets."
    )
    parser.add_argument("--raw-subdir", default="Raw_Data")
    parser.add_argument("--review-subdir", default="Human_Review")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--docker-port-base",
        type=int,
        default=None,
        help="First Docker host port; worker i uses base + i.",
    )
    parser.add_argument(
        "--merge-workloads",
        action="store_true",
        help="Merge initialization into the verified workload as one field.",
    )
    parser.add_argument("--schema-directory", default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)

    config = load_config(resolve_path(args.config))
    llm_config = copy.deepcopy(config["llm_configuration"])
    input_root = resolve_path(args.input_root)
    raw_root = input_root / args.raw_subdir
    review_root = input_root / args.review_subdir
    output_root = resolve_path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    schema_directory = None
    schema_value = args.schema_directory or config.get("database_schema_directory")
    if schema_value:
        candidate = (
            Path(str(schema_value)).expanduser()
            if args.schema_directory
            else resolve_path(schema_value)
        )
        if candidate.is_dir():
            schema_directory = candidate.resolve()

    docker_port_base = (
        args.docker_port_base
        if args.docker_port_base is not None
        else int(config["docker_port"])
    )
    merge_workloads = bool(args.merge_workloads or config.get("merge_workloads", False))

    exit_code = 0
    for dataset in args.dataset:  # datasets processed serially, in order
        summary = process_dataset(
            dataset,
            raw_root,
            review_root,
            output_root,
            config,
            llm_config,
            schema_directory,
            args.workers,
            docker_port_base,
            merge_workloads,
        )
        if summary.get("error") or summary.get("failed"):
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
