"""ReAct baseline for database diagnosis and recovery with superuser permissions."""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import re
import sys
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Tuple

from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from docker_func import (  # noqa: E402
    build_container_config,
    build_sql_file_command,
    docker_container_init,
    docker_container_init_db,
    force_remove_container,
    interrupt_tmux_window,
    kill_tmux_session,
    make_unique_container_name,
    recreate_container_with_new_name,
    reset_tmux_window,
    run_docker_exec_observation,
)
from prompt_template import agentic_llm_act_prompt_template, agentic_llm_report_prompt_template  # noqa: E402
from utils import extract_json_object, normalize_dbms, prune_obser_log, set_up_logger  # noqa: E402
from Baselines.input_contract import anomaly_verified_workload, render_diagnosis_prompt  # noqa: E402
from Baselines.runtime_policy import diagnosis_llm_config, direct_injection_command
from llm_api import FatalAPIQuotaError

DEFAULT_TIMEOUTS = {
    "tpch_1": 1500, "imdb": 4500, "tpcds_1": 1800,
    "tpch_10": 11800,
}
DEFAULT_HARDWARE = {"CPU": 16, "Memory": 16, "Memory + Swap": 32, "Storage": 256}
DEFAULT_DB_USER = {"DB_USER": "diagnosis", "DB_PASSWORD": "diagnosis", "DB_HOST": "localhost", "DB_PORT": "5432"}
TIMEOUT_PATTERN = re.compile(r"^\s*#\s*TIMEOUT THRESHOLD:\s*(\d+)\b", re.MULTILINE)


DEFAULT_METHOD = "Claude Code"
TERMINAL_SAMPLE_STATUSES = frozenset({"completed", "failed", "cancelled_after_api_error"})


def method_name(config: Dict[str, Any]) -> str:
    workflow = str(config.get("workflow_type") or "").lower()
    return {
        "react": "ReAct", "react_super": "ReAct", "react_common": "ReAct",
        "codex": "Codex", "claude_code": "Claude Code",
        "db_gpt": "DB-GPT", "dbgpt": "DB-GPT", "dbaiops": "DBAIOps",
    }.get(workflow, workflow or DEFAULT_METHOD)


def terminal_sample_result(output_base: Path, index: int, case: Dict[str, Any]) -> tuple[str, Dict[str, Any] | None]:
    sample_id = str(case.get("anomaly_id", index))
    status_path = output_base / sample_id / "status.json"
    if not status_path.is_file():
        return sample_id, None
    try:
        result = json.loads(read_text(status_path))
    except (OSError, json.JSONDecodeError):
        return sample_id, None
    if isinstance(result, dict) and result.get("status") in TERMINAL_SAMPLE_STATUSES:
        return sample_id, result
    return sample_id, None


def project_path(value: str | Path) -> Path:
    # Resolve symlinks to the canonical path. The evaluation container
    # bind-mounts the CANONICAL host path, so any sample file handed to
    # `docker exec` must be canonical too; passing a symlinked alias makes the
    # file invisible inside the container ("No such file or directory").
    path = Path(value)
    path = path if path.is_absolute() else PROJECT_ROOT / path
    return path.resolve()


def required_files(sample_dir: Path) -> Dict[str, Path]:
    files = {
        "phenomenon": sample_dir / "user_phenomenon_description.txt",
        "initialization": sample_dir / "initialization_workload.sql",
        "verified": sample_dir / "verified_workload.sql",
        "modification": sample_dir / "data_modification.sql",
        "inject": sample_dir / "anomaly_inject.sh",
        "config": sample_dir / "config.json",
    }
    missing = [str(path) for path in files.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError("Sample is incomplete; missing: " + ", ".join(missing))
    return files


def find_sample_directory(anomaly_root: Path, case: Dict[str, Any]) -> Tuple[str, Path]:
    sample_id = str(case.get("sample_id", case.get("anomaly_id", "")))
    if not sample_id:
        raise ValueError("Each key-information entry needs `sample_id` or `anomaly_id`.")

    exact = anomaly_root / sample_id
    if exact.is_dir():
        return sample_id, exact

    database = str(case.get("database_name", ""))
    named = anomaly_root / f"{sample_id}_{database}"
    if named.is_dir():
        return sample_id, named

    candidates = sorted(path for path in anomaly_root.glob(f"{sample_id}_*") if path.is_dir())
    if len(candidates) == 1:
        return sample_id, candidates[0]
    raise FileNotFoundError(f"Cannot identify sample directory for id `{sample_id}` under {anomaly_root}.")


def timeout_from_script(script: str, label: str) -> int:
    match = TIMEOUT_PATTERN.search(script)
    if match is None:
        raise ValueError(f"{label} must declare `# TIMEOUT THRESHOLD: <seconds>` in its first non-empty line.")
    return int(match.group(1))


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_json_file(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    return json.loads(read_text(path))


def validate_config(config: Dict[str, Any], requires_superuser: bool) -> None:
    required = {"key_information_path", "llm_configuration", "docker_name"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError("Missing required config fields: " + ", ".join(missing))

    llm_config = config["llm_configuration"]
    if not isinstance(llm_config, dict):
        raise ValueError("`llm_configuration` must be an object.")
    llm_missing = {"base_url", "api_key", "model_name", "temperature", "retry_times"} - llm_config.keys()
    if llm_missing:
        raise ValueError("Incomplete `llm_configuration`: " + ", ".join(sorted(llm_missing)))
    if int(llm_config["retry_times"]) < 1:
        raise ValueError("`llm_configuration.retry_times` must be at least 1.")

    if requires_superuser:
        superuser = config.get("superuser_config")
        if not isinstance(superuser, dict) or {"DB_USER", "DB_PASSWORD"} - superuser.keys():
            raise ValueError("Superuser mode requires `superuser_config.DB_USER` and `superuser_config.DB_PASSWORD`.")


def validate_input_data(config: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    key_info_path = project_path(config["key_information_path"])
    if not key_info_path.is_file():
        raise FileNotFoundError(f"`key_information_path` does not exist: {key_info_path}")
    cases = json.loads(read_text(key_info_path))
    if not isinstance(cases, list):
        raise ValueError("`key_information_path` must contain a JSON list.")
    root = key_info_path.parent
    for case in cases:
        if not isinstance(case, dict) or not case.get("database_name"):
            raise ValueError("Each key-information entry must be an object with `database_name`.")
        _, sample_dir = find_sample_directory(root, case)
        required_files(sample_dir)
    return cases


def build_prompt(
    privileged: bool,
    max_turns: int,
    hardware: Dict[str, Any],
    user_config: Dict[str, Any],
    superuser_config: Dict[str, Any] | None,
    files: Dict[str, Path],
) -> str:
    values = {
        "MAX_TURN_NUM": max_turns,
        "SYSTEM_ENVIRONMENT": str(hardware),
        "SYSTEM_USER_CONFIG": str(user_config),
        "ANOMALY_VERIFIED_WORKLOAD": anomaly_verified_workload(files),
        "ANOMALY_SYMPTOMS": read_text(files["phenomenon"]),
    }
    if privileged:
        values["SUPERUSER_CONFIG"] = str(superuser_config)
        return render_diagnosis_prompt(agentic_llm_act_prompt_template, values) + (
            f"\n## SUPERUSER CONFIGURATION:\n{superuser_config}\n"
            "You may perform targeted database recovery during code actions. "
            "Validate the workload after recovery before finishing.\n"
        )
    return render_diagnosis_prompt(agentic_llm_report_prompt_template, values)


def parse_action(response: str) -> Tuple[Dict[str, str] | None, str]:
    action_block = response.partition("<action>")[2].partition("</action>")[0].strip()
    if not action_block:
        return None, "Missing <action> block."
    try:
        action = json.loads(extract_json_object(action_block))
    except (ValueError, json.JSONDecodeError) as exc:
        return None, f"Invalid action JSON: {exc}"

    if not isinstance(action, dict):
        return None, "Action must be a JSON object."
    if set(action) == {"action", "shell"} and action.get("action") == "code" and isinstance(action["shell"], str):
        try:
            timeout_from_script(action["shell"], "Code action")
        except ValueError as exc:
            return None, str(exc)
        return action, ""

    finish_keys = {"action", "diagnosis_report.md", "solution_script.sh", "verified_workload.sql"}
    if set(action) == finish_keys and action.get("action") == "finish" and all(isinstance(action[key], str) for key in finish_keys - {"action"}):
        try:
            timeout_from_script(action["solution_script.sh"], "Solution script")
        except ValueError as exc:
            return None, str(exc)
        return action, ""
    return None, "Unexpected action schema."


def save_finish(action, output_dir, original_workload):
    """Validate and persist the common artifacts for all three frameworks."""
    parsed, reason = parse_action("<action>" + json.dumps(action) + "</action>")
    if parsed is None or parsed["action"] != "finish":
        raise ValueError(reason or "Expected finish action.")
    if not action["diagnosis_report.md"].strip():
        raise ValueError("Diagnosis report is empty.")
    checked = subprocess.run(
        ["bash", "-n"], input=action["solution_script.sh"],
        text=True, capture_output=True, timeout=10,
    )
    if checked.returncode:
        raise ValueError(f"Recovery script has invalid Bash syntax: {checked.stderr}")
    (output_dir / "diagnosis_report.md").write_text(action["diagnosis_report.md"], encoding="utf-8")
    script = output_dir / "solution_script.sh"
    script.write_text(action["solution_script.sh"], encoding="utf-8")
    script.chmod(0o700)
    (output_dir / "optimized_verified_workload.sql").write_text(
        action["verified_workload.sql"].strip() or original_workload, encoding="utf-8",
    )


def execute_action(
    container_name: str, output_dir: Path, turn: int, action: Dict[str, str], logger: logging.Logger
) -> str:
    shell = action["shell"]
    timeout = timeout_from_script(shell, "Code action") + 60
    action_path = output_dir / "actions" / f"{turn}_action.sh"
    action_path.write_text(shell, encoding="utf-8")
    log_path = output_dir / "logs" / f"{turn}_observation_log.txt"
    success, command_output = run_docker_exec_observation(
        container_name, ["bash", str(action_path)], str(log_path), timeout=timeout, session_name=container_name
    )
    if not success:
        interrupt_tmux_window(container_name, "observation")
    status = "successfully" if success else "with errors"
    observation = f"Script executed {status}. Output follows:\n{prune_obser_log(str(log_path), retain_length=20000)}"
    if command_output:
        observation += f"\n\n-- EXECUTION COMMENTS --\n{command_output}"
    logger.info(f"[{DEFAULT_METHOD} Diagnosis][Turn {turn}] Executed diagnosis action (code) : success=`{success}`.")
    return observation[:20000]


def request_llm(
    messages: list[str], llm_config: Dict[str, Any],
    logger: logging.Logger | None = None, tag: str = f"[{DEFAULT_METHOD} Diagnosis]",
) -> str:
    try:
        from llm_api import get_llm_response
    except ImportError as exc:
        raise RuntimeError(
            "LLM dependencies are unavailable. Install the project requirements with "
            "`python -m pip install -r requirements.txt` before running diagnosis."
        ) from exc
    # Log the request parameters WITHOUT leaking the API key: only model
    # / temperature and the conversation size are recorded.
    if logger is not None:
        logger.info(
            f"{tag} Sending LLM request (model=`{llm_config.get('model_name')}`, "
            f"temperature=`{llm_config.get('temperature')}`, "
            f"messages={len(messages)}) ..."
        )
    response = get_llm_response(messages, diagnosis_llm_config(llm_config))
    if logger is not None:
        logger.info(f"{tag} Received LLM response ({len(response)} chars).")
    return response


def run_case(
    case_index: int, total_cases: int, case: Dict[str, Any], config: Dict[str, Any],
    privileged: bool, output_base: Path, logger: logging.Logger,
    base_docker_name: str, worker_host_port: int,
    llm_requester: Callable[..., str] = request_llm,
    agent_runner=None,
) -> None:
    key_info_path = project_path(config["key_information_path"])
    sample_id, sample_dir = find_sample_directory(key_info_path.parent, case)
    files = required_files(sample_dir)
    sample_config = load_json_file(files["config"])
    output_dir = output_base / sample_id
    for name in ("actions", "logs", "reasoning_process"):
        (output_dir / name).mkdir(parents=True, exist_ok=True)

    dbms = sample_config.get("dbms", case.get("dbms", config.get("dbms", "PostgreSQL 16.11")))
    database_name = str(sample_config.get("database_name", case["database_name"]))
    schema_name = str(sample_config.get("database_schema_name", case.get("database_schema_name", config.get("database_schema_name", "diagnosis"))))
    if normalize_dbms(dbms) == "mysql":
        schema_name = database_name

    user_config = dict(DEFAULT_DB_USER)
    user_config.update(config.get("system_user_config") or {})
    user_config.update(sample_config.get("system_user_config") or {})
    user_config["DB_NAME"] = database_name
    user_config["DB_PORT"] = "3306" if normalize_dbms(dbms) == "mysql" else "5432"
    hardware = dict(DEFAULT_HARDWARE)
    hardware.update(config.get("system_hardware_config") or {})
    hardware.update(sample_config.get("system_hardware_config") or {})
    # Container names are NEVER reused: `make_unique_container_name` appends a
    # microsecond timestamp to the per-worker prefix (`base_docker_name`), and each
    # retry moves to a brand new name via `recreate_container_with_new_name`
    # (mirroring test_syn_ano.py / test_eval.py). Combined with the per-worker
    # `worker_host_port` slot this makes parallel runs collision-free -- no two
    # live containers ever share a name or a published host port.
    container_config = build_container_config(dbms, hardware, worker_host_port, user_config)
    retries = int(config.get("docker_retry_times", 10))
    init_timeout = int(config.get("database_init_timeout", DEFAULT_TIMEOUTS.get(database_name, 3600)))

    containers = []
    try:
        _run_case_body(
            case_index, total_cases, case, config, privileged, output_dir, logger,
            base_docker_name, container_config, retries, init_timeout,
            dbms, database_name, schema_name, user_config, hardware, files, llm_requester,
            agent_runner, containers,
        )
    finally:
        # Reap this sample's final container + tmux session. Names are never
        # reused, so leftover containers would otherwise accumulate under load.
        for container_name in dict.fromkeys(containers):
            logger.info(f"[{method_name(config)} Diagnosis][{case_index + 1}/{total_cases}] Reclaiming container `{container_name}` and tmux session ...")
            kill_tmux_session(container_name)
            force_remove_container(container_name)


def _run_case_body(
    case_index: int, total_cases: int, case: Dict[str, Any], config: Dict[str, Any],
    privileged: bool, output_dir: Path, logger: logging.Logger,
    base_docker_name: str, container_config: Dict[str, Any], retries: int, init_timeout: int,
    dbms: str, database_name: str, schema_name: str,
    user_config: Dict[str, Any], hardware: Dict[str, Any], files: Dict[str, Path],
    llm_requester: Callable[..., str] = request_llm,
    agent_runner=None, containers=None,
) -> str:
    """Perform the diagnosis for one sample and return the FINAL (unique) container
    name so the caller can tear it down. Raises on any unrecoverable failure."""
    tag = f"[{method_name(config)} Diagnosis][{case_index + 1}/{total_cases}]"
    mode = "superuser" if privileged else "ordinary-user"
    logger.info(
        f"{tag} == START == Diagnosing database `{database_name}` (dbms=`{dbms}`, schema=`{schema_name}`, "
        f"mode=`{mode}`) in a fresh container (prefix `{base_docker_name}`)."
    )
    container_name = None
    if containers is None:
        containers = []
    logger.info(f"{tag} [STEP 1/5] Initializing Docker Container (prefix `{base_docker_name}`) ...")
    for attempt in range(1, retries + 1):
        if container_name is None:
            container_name = make_unique_container_name(base_docker_name)
            containers.append(container_name)
            if docker_container_init(container_name, container_config):
                break
        else:
            init_ok, container_name = recreate_container_with_new_name(container_name, base_docker_name, container_config)
            containers.append(container_name)
            if init_ok:
                break
        logger.warning(f"{tag}[WARNING] Container initialization failed ({attempt}/{retries}).")
        if attempt == retries:
            raise RuntimeError(f"Unable to initialize Docker container `{container_name}`.")
        time.sleep(5)
    logger.info(f"{tag} Docker Container `{container_name}` Initialized Successfully.")

    db_type = config.get("db_type", "load_data" if normalize_dbms(dbms) == "mysql" else "dump")
    init_db_config = {
        "base_dir": str(PROJECT_ROOT), "dbms": dbms, "db_host": user_config["DB_HOST"],
        "db_port": user_config["DB_PORT"], "db_name": database_name, "db_schema": schema_name,
        "db_type": db_type, "db_user": user_config["DB_USER"], "password": user_config["DB_PASSWORD"],
        "timeout": init_timeout, "output_dir": str(output_dir),
    }
    logger.info(f"{tag} [STEP 2/5] Initializing Database `{database_name}` in Container `{container_name}` ...")
    for attempt in range(1, retries + 1):
        if docker_container_init_db(container_name, init_db_config, session_name=container_name):
            break
        logger.warning(f"{tag}[WARNING] Database initialization failed ({attempt}/{retries}).")
        if attempt == retries:
            reset_tmux_window(container_name, "status")
            raise RuntimeError(f"Unable to initialize database `{database_name}`.")
        time.sleep(5)
        # Recreate under a NEW unique name before the next database-init retry.
        _, container_name = recreate_container_with_new_name(container_name, base_docker_name, container_config)
        containers.append(container_name)
    logger.info(f"{tag} Database `{database_name}` Initialization Succeeded.")

    logger.info(f"{tag} [STEP 3/5] Applying Data Modification ...")
    modification_log = output_dir / "data_modification_log.txt"
    command = build_sql_file_command(dbms, user_config, str(files["modification"]), database_name)
    success, details = run_docker_exec_observation(container_name, command, str(modification_log), 600, session_name=container_name)
    if not success:
        logger.error(f"{tag}[ERROR] Data Modification Failed. Log: {modification_log}")
        raise RuntimeError(f"Data modification failed: {modification_log}\n{details}")
    logger.info(f"{tag} Data Modification Succeeded. Log: {modification_log}")

    logger.info(f"{tag} [STEP 4/5] Executing anomaly_inject.sh directly (prevalidated sample) ...")
    injection_timeout = int(config.get("direct_injection_timeout_seconds", 7200))
    injection_command = direct_injection_command(
        files["inject"], dbms, database_name, schema_name, user_config, injection_timeout
    )
    injection_log = output_dir / "anomaly_injection_log.txt"
    success, details = run_docker_exec_observation(
        container_name, injection_command, str(injection_log), injection_timeout + 30, session_name=container_name
    )
    if not success:
        logger.error(f"{tag}[ERROR] Anomaly Injection Failed. Log: {injection_log}")
        reset_tmux_window(container_name, "observation")
        raise RuntimeError(f"Anomaly injection failed: {injection_log}\n{details}")
    logger.info(f"{tag} Anomaly Injection Succeeded. Log: {injection_log}")

    if agent_runner is not None:
        logger.info(f"{tag} [STEP 5/5] Starting {method_name(config)} diagnosis workflow ...")
        if __package__:
            from .native_agent import build_prompt as build_native_prompt
        else:
            from native_agent import build_prompt as build_native_prompt
        prompt = build_native_prompt(
            int(config.get("max_turn_num", 20)), hardware, user_config,
            config.get("superuser_config"), files,
        )
        action = agent_runner(prompt, config, container_name, output_dir, files, logger)
        save_finish(action, output_dir, read_text(files["verified"]))
        logger.info(f"{tag} Diagnosis artifacts saved to `{output_dir}`.")
        return container_name
    prompt = build_prompt(
        privileged, int(config.get("max_turn_num", 20)), hardware, user_config,
        config.get("superuser_config"), files,
    )
    messages = [prompt]
    llm_config = dict(config["llm_configuration"])
    max_turn_num = int(config.get("max_turn_num", 20))
    logger.info(
        f"{tag} [STEP 5/5] Starting ReAct Diagnosis Loop (max_turn=`{max_turn_num}`, "
        f"model=`{llm_config.get('model_name')}`, retry_times=`{llm_config.get('retry_times')}`) ..."
    )
    finished = False
    for turn in range(max_turn_num):
        logger.info(f"{tag} [Turn {turn + 1}/{max_turn_num}] Requesting diagnosis action from LLM ...")
        history = []
        action = None
        for retry in range(int(llm_config["retry_times"])):
            response = llm_requester(messages, llm_config, logger=logger, tag=f"{tag}[Turn {turn + 1}]")
            history.append(f"\n=== Turn {turn}, retry {retry} ===\n{response}\n")
            (output_dir / "reasoning_process" / f"{turn}_reasoning_messages.txt").write_text("".join(history), encoding="utf-8")
            action, reason = parse_action(response)
            if action is not None:
                break
            logger.warning(f"{tag}[WARNING] Invalid LLM action at turn {turn + 1} ({retry + 1}/{llm_config['retry_times']}): {reason}")
        if action is None:
            logger.warning(f"{tag}[WARNING] No valid action parsed at turn {turn + 1}; re-prompting for a valid action JSON.")
            messages.append(response)
            messages.append("The previous response did not conform to the required action schema. Return a valid action JSON.")
            continue

        messages.append(response)
        logger.info(f"{tag} [Turn {turn + 1}/{max_turn_num}] Parsed action=`{action['action']}`.")
        if action["action"] == "finish":
            if turn == 0:
                messages.append("Inspect the live database with a code action before finishing.")
                continue
            try:
                save_finish(action, output_dir, read_text(files["verified"]))
            except ValueError as exc:
                messages.append(f"Invalid final artifacts: {exc}. Correct and finish again.")
                continue
            messages.append("Diagnosis completed.")
            finished = True
            logger.info(f"{tag} Diagnosis Finished at Turn {turn + 1}. Artifacts written to `{output_dir}`.")
            break
        messages.append(execute_action(container_name, output_dir, turn, action, logger))

    if not finished:
        logger.warning(f"{tag}[WARNING] Diagnosis loop reached max_turn=`{max_turn_num}` without a finish action.")

    (output_dir / f"{base_docker_name}_messages.json").write_text(
        json.dumps({f"message_{index}": message for index, message in enumerate(messages)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"{tag} == DONE == Container `{container_name}` ready for teardown.")
    if not finished:
        raise RuntimeError("Turn limit reached without valid diagnosis and recovery artifacts.")
    return container_name


def run(
    privileged: bool,
    default_config: Path,
    description: str,
    llm_requester: Callable[..., str] = request_llm,
    workflow_type: str | None = None,
    output_base_dir: str | Path | None = None,
    agent_runner=None,
) -> None:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", type=Path, default=default_config, help="Path to the workflow JSON configuration.")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration and input samples without Docker or LLM calls.")
    args = parser.parse_args()
    config_path = args.config.resolve()
    if not config_path.is_file():
        parser.error(f"Configuration file does not exist: {config_path}")
    try:
        config = json.loads(read_text(config_path))
        config["llm_configuration"] = diagnosis_llm_config(config.get("llm_configuration", {}))
        if workflow_type is not None:
            config["workflow_type"] = workflow_type
        if output_base_dir is not None:
            config.setdefault("output_base_dir", str(output_base_dir))
        validate_config(config, privileged)
        cases = list(validate_input_data(config))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    if args.dry_run:
        print(f"[{method_name(config)}] Validated {len(cases)} samples using {config_path}.")
        return

    output_base = project_path(config.get("output_base_dir", "Output"))
    output_base.mkdir(parents=True, exist_ok=True)
    (output_base / "config.json").write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    logger = set_up_logger(str(output_base / "log.txt"))

    parallelism_num = max(1, int(config.get("parallelism_num", 1)))
    docker_name = str(config["docker_name"])
    host_port = int(config.get("host_port", 5427))
    main_tag = f"[MAIN][{method_name(config)} Database Diagnosis Baseline]"
    logger.info(f"{main_tag} == START ==")
    logger.info(
        f"{main_tag} Mode: `{'superuser' if privileged else 'ordinary-user'}`, Samples: `{len(cases)}`, "
        f"Parallelism: `{parallelism_num}`, Docker Prefix: `{docker_name}`, Base Host Port: `{host_port}`."
    )
    logger.info(f"{main_tag} Output Base Directory: `{output_base}`.")

    # Each worker owns a stable SLOT: a distinct container-name prefix
    # (`{docker_name}_w{slot}`) plus a distinct published host port
    # (`host_port + slot`). Slots are handed out via a queue so at most
    # `parallelism_num` samples run concurrently, and a released slot (its prefix
    # + port) is reused by the next queued sample. This is what keeps parallel
    # Docker container creation / initialization / deletion collision-free.
    slot_queue = queue.Queue()
    results = {}
    pending_cases = []
    for index, case in enumerate(cases):
        sample_id, previous_result = terminal_sample_result(output_base, index, case)
        if previous_result is None:
            pending_cases.append((index, case))
            continue
        results[sample_id] = previous_result
        logger.info(
            f"{main_tag} Skipping sample {sample_id}: terminal status="
            f"`{previous_result['status']}` already exists."
        )
    logger.info(
        f"{main_tag} Scheduling `{len(pending_cases)}` sample(s); "
        f"`{len(results)}` terminal sample(s) will not be rerun."
    )
    for slot in range(parallelism_num):
        slot_queue.put(slot)

    def worker(index: int, case: Dict[str, Any]) -> None:
        slot = slot_queue.get()
        fatal_exit = False
        try:
            if (output_base / "fatal_api_error.json").exists():
                raise FatalAPIQuotaError("A prior OpenRouter HTTP 402 marker exists.")
            base_docker_name = f"{docker_name}_w{slot}"
            worker_host_port = host_port + slot
            run_case(
                index, len(cases), case, config, privileged, output_base, logger,
                base_docker_name, worker_host_port, llm_requester, agent_runner,
            )
            results[str(case.get("anomaly_id", index))] = {"status": "completed"}
        except FatalAPIQuotaError as exc:
            fatal_exit = True
            sample_id = str(case.get("anomaly_id", index))
            results[sample_id] = {"status": "interrupted_fatal_api", "error": str(exc)}
            marker = output_base / "fatal_api_error.json"
            temporary_marker = marker.with_suffix(".json.tmp")
            temporary_marker.write_text(json.dumps({
                "status": "fatal_api_error", "http_status": 402,
                "sample_id": sample_id, "error": str(exc),
            }, indent=2))
            temporary_marker.replace(marker)
            logger.critical(
                f"{main_tag}[FATAL] OpenRouter HTTP 402 for sample {sample_id}; "
                "terminating the entire baseline process without retry."
            )
        except Exception as exc:  # noqa: BLE001 - one bad sample must not abort the run.
            logger.error(f"{main_tag}[ERROR] Sample {index + 1}/{len(cases)} failed: {exc}")
            results[str(case.get("anomaly_id", index))] = {"status": "failed", "error": str(exc)}
        finally:
            sample_id = str(case.get("anomaly_id", index))
            status_path = output_base / sample_id / "status.json"
            status_path.parent.mkdir(parents=True, exist_ok=True)
            temporary_status_path = status_path.with_suffix(".json.tmp")
            temporary_status_path.write_text(
                json.dumps(results.get(sample_id, {"status": "interrupted"}), indent=2)
            )
            temporary_status_path.replace(status_path)
            slot_queue.put(slot)
            if fatal_exit:
                logging.shutdown()
                os._exit(42)

    if parallelism_num == 1:
        for index, case in tqdm(pending_cases, total=len(cases), initial=len(results)):
            worker(index, case)
    else:
        with ThreadPoolExecutor(max_workers=parallelism_num) as executor:
            futures = [executor.submit(worker, index, case) for index, case in pending_cases]
            for future in tqdm(as_completed(futures), total=len(cases), initial=len(results)):
                future.result()

    logger.info(f"{main_tag} == DONE == {len(cases)} sample(s) processed. Output: `{output_base}`.")
    (output_base / "summary.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    if any(value["status"] != "completed" for value in results.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    run(
        privileged=True,
        default_config=PROJECT_ROOT / "Configs" / "baselines" / "react.json",
        description="ReAct database diagnosis baseline with superuser recovery permissions.",
    )
