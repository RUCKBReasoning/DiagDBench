#!/usr/bin/env python3
"""V1 synthetic anomaly filtering with non-terminal workload observations.

Existing first-filter results complete their legacy follow-up stages before
new first-filter work begins. Samples that have not reached the first filter
run initialization before every verified workload, and record verified-workload
output consistency before anomaly injection, but workload consistency never
rejects a sample by itself.
"""

import argparse
import copy
import hashlib
import json
import logging
import os
import queue
import shutil
import tempfile
import threading

import docker_func
from utils import *
from docker_func import *

try:
    from Scripts.anomaly_judge import (
        judge_database_health,
        write_database_state_report,
    )
except Exception:
    judge_database_health = None
    write_database_state_report = None


V1_FIRST_RESULT_FILENAME = "0_filter_result.json"
V1_SECOND_RESULT_FILENAME = "1_filter_result.json"
V1_EVIDENCE_FILENAME = "v1_workload_consistency.json"
V1_FORMAT_VERSION = 5
DEFAULT_WORKLOAD_VALIDATION_ROUNDS = 3
DEFAULT_WORKLOAD_VALIDATION_TIMEOUT = 3600
SUCCESS_MARKERS = (
    "Anomaly successfully simulated and verified.",
    "Anomaly successfully reproduced and verified.",
)


def write_json_atomic(path, value):
    _v0legacy_write_json_atomic(path, value)


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return default


def normalize_ids(values):
    normalized = set()
    for value in values or []:
        try:
            normalized.add(int(value))
        except (TypeError, ValueError):
            continue
    return normalized


def split_sql_statements(sql_text):
    """Split on top-level semicolons without changing statement text or order."""
    statements = []
    statement_start = 0
    index = 0
    state = None
    dollar_quote = None
    length = len(sql_text)

    while index < length:
        character = sql_text[index]
        following = sql_text[index:index + 2]
        if state is None:
            if following == "--":
                state = "line_comment"
                index += 2
                continue
            if following == "/*":
                state = "block_comment"
                index += 2
                continue
            if character == "'":
                state = "single_quote"
            elif character == '"':
                state = "double_quote"
            elif character == "`":
                state = "backtick_quote"
            elif character == "$":
                delimiter_end = sql_text.find("$", index + 1)
                if delimiter_end != -1:
                    candidate = sql_text[index:delimiter_end + 1]
                    if candidate == "$$" or (
                            candidate[1:-1].replace("_", "a").isalnum()
                            and not candidate[1:-1][0].isdigit()):
                        dollar_quote = candidate
                        state = "dollar_quote"
                        index = delimiter_end + 1
                        continue
            elif character == ";":
                statements.append(sql_text[statement_start:index + 1])
                statement_start = index + 1
        elif state == "line_comment":
            if character == "\n":
                state = None
        elif state == "block_comment":
            if following == "*/":
                state = None
                index += 2
                continue
        elif state == "single_quote":
            if character == "'" and sql_text[index + 1:index + 2] == "'":
                index += 2
                continue
            if character == "'":
                state = None
        elif state == "double_quote":
            if character == '"' and sql_text[index + 1:index + 2] == '"':
                index += 2
                continue
            if character == '"':
                state = None
        elif state == "backtick_quote":
            if character == "`" and sql_text[index + 1:index + 2] == "`":
                index += 2
                continue
            if character == "`":
                state = None
        elif state == "dollar_quote":
            if sql_text.startswith(dollar_quote, index):
                state = None
                index += len(dollar_quote)
                continue
        index += 1

    if sql_text[statement_start:].strip():
        statements.append(sql_text[statement_start:])
    return statements


def create_marked_verified_workload(dbms, source_path):
    """Create a temporary workload that frames every SQL statement's output."""
    with open(source_path, "r", encoding="utf-8") as source_file:
        source_sql = source_file.read()

    statements = split_sql_statements(source_sql)
    marker_seed = hashlib.sha256(
        (os.path.abspath(source_path) + "\0" + source_sql).encode("utf-8")
    ).hexdigest()[:16]
    marker_prefix = "__DB_DIAG_WORKLOAD_%s_" % marker_seed
    is_mysql = normalize_dbms(dbms) == "mysql"
    fragments = []
    for statement_index, statement in enumerate(statements, start=1):
        begin_marker = "%sBEGIN_%04d__" % (marker_prefix, statement_index)
        end_marker = "%sEND_%04d__" % (marker_prefix, statement_index)
        if is_mysql:
            fragments.extend((
                "SELECT '%s' AS __db_diag_workload_marker__;\n" % begin_marker,
                statement,
                "\nSELECT '%s' AS __db_diag_workload_marker__;\n" % end_marker,
            ))
        else:
            fragments.extend((
                "\\echo %s\n" % begin_marker,
                statement,
                "\n\\echo %s\n" % end_marker,
            ))

    descriptor, marked_path = tempfile.mkstemp(
        prefix=".db_diag_verified_",
        suffix=".sql",
        dir=os.path.dirname(os.path.abspath(source_path)),
        text=True,
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as marked_file:
        marked_file.write("".join(fragments))
    return marked_path, marker_prefix, len(statements)


def parse_marked_verified_observation(observation, marker_prefix, statement_count):
    """Preserve SQL order while reducing each statement's output to a set."""
    begin_markers = {
        "%sBEGIN_%04d__" % (marker_prefix, index): index
        for index in range(1, statement_count + 1)
    }
    end_markers = {
        "%sEND_%04d__" % (marker_prefix, index): index
        for index in range(1, statement_count + 1)
    }
    statement_lines = {index: [] for index in range(1, statement_count + 1)}
    unframed_lines = []
    active_index = None
    seen_begin = set()
    seen_end = set()
    parse_complete = True

    for line in observation.get("output", []):
        matching_begin = next(
            (index for marker, index in begin_markers.items() if marker in line),
            None,
        )
        matching_end = next(
            (index for marker, index in end_markers.items() if marker in line),
            None,
        )
        if matching_begin is not None:
            if active_index is not None or matching_begin in seen_begin:
                parse_complete = False
            active_index = matching_begin
            seen_begin.add(matching_begin)
            continue
        if matching_end is not None:
            if active_index != matching_end or matching_end in seen_end:
                parse_complete = False
            active_index = None
            seen_end.add(matching_end)
            continue
        if active_index is None:
            unframed_lines.append(line)
        else:
            statement_lines[active_index].append(line)

    if active_index is not None:
        parse_complete = False
    expected_indexes = set(range(1, statement_count + 1))
    parse_complete = (
        parse_complete
        and seen_begin == expected_indexes
        and seen_end == expected_indexes
    )
    observation["statement_results"] = [
        {
            "statement_index": index,
            "output_set": sorted(set(statement_lines[index])),
        }
        for index in range(1, statement_count + 1)
    ]
    observation["unframed_output_set"] = sorted(set(unframed_lines))
    observation["marker_parse_complete"] = parse_complete
    return observation


def verified_comparison_view(observation):
    """Return only the verified-workload fields that define consistency."""
    return {
        "completed": observation.get("completed"),
        "exit_code": observation.get("exit_code"),
        "command_ok": observation.get("command_ok"),
        "marker_parse_complete": observation.get("marker_parse_complete"),
        "statement_results": observation.get("statement_results"),
        "unframed_output_set": observation.get("unframed_output_set"),
    }


class V1InitialResultStore:
    """Extend a copied legacy first-filter result with V1-only observations."""

    def __init__(self, path, valid_ids, legacy_result):
        self.path = path
        self.valid_ids = set(valid_ids)
        self._legacy_result = copy.deepcopy(legacy_result)
        self._legacy_passed = (
            normalize_ids(self._legacy_result.get("passed_sample_ids"))
            - self.valid_ids
        )
        self._legacy_failed = (
            normalize_ids(self._legacy_result.get("failed_sample_ids"))
            - self.valid_ids
        )
        self._legacy_passed -= self._legacy_failed
        raw = load_json(path, {})
        self._workload_passed = (
            normalize_ids(raw.get("workload_pass_ids")) & self.valid_ids
        )
        self._workload_failed = (
            normalize_ids(raw.get("workload_failed_ids")) & self.valid_ids
        )
        self._anomaly_passed = (
            normalize_ids(raw.get("passed_sample_ids")) & self.valid_ids
        )
        self._anomaly_failed = (
            normalize_ids(raw.get("failed_sample_ids")) & self.valid_ids
        )
        self._anomaly_passed -= self._anomaly_failed
        self._lock = threading.Lock()
        self.persist()

    def _result(self):
        result = copy.deepcopy(self._legacy_result)
        result.update({
            "format_version": V1_FORMAT_VERSION,
            "stage": "v1_first_filter",
            "workload_evidence_file": V1_EVIDENCE_FILENAME,
            "workload_pass_ids": sorted(self._workload_passed),
            "workload_failed_ids": sorted(self._workload_failed),
            "passed_sample_ids": sorted(
                self._legacy_passed | self._anomaly_passed
            ),
            "failed_sample_ids": sorted(
                self._legacy_failed | self._anomaly_failed
            ),
        })
        return result

    def persist(self):
        with self._lock:
            write_json_atomic(self.path, self._result())

    def record(self, sample_id, workload_passed, anomaly_passed):
        with self._lock:
            if workload_passed:
                self._workload_passed.add(sample_id)
                self._workload_failed.discard(sample_id)
            else:
                self._workload_failed.add(sample_id)
                self._workload_passed.discard(sample_id)
            if anomaly_passed:
                self._anomaly_passed.add(sample_id)
                self._anomaly_failed.discard(sample_id)
            else:
                self._anomaly_failed.add(sample_id)
                self._anomaly_passed.discard(sample_id)
            write_json_atomic(self.path, self._result())

    def completed_ids(self):
        with self._lock:
            return set(self._anomaly_passed) | set(self._anomaly_failed)

    def anomaly_passed_ids(self):
        with self._lock:
            return set(self._anomaly_passed)

    def anomaly_failed_ids(self):
        with self._lock:
            return set(self._anomaly_failed)

    def workload_passed_ids(self):
        with self._lock:
            return set(self._workload_passed)

    def workload_failed_ids(self):
        with self._lock:
            return set(self._workload_failed)


class V1SecondResultStore:
    """Persist V1 second-stage outcomes while retaining first-stage metadata."""

    def __init__(
            self, path, base_result, inherited_passed, inherited_failed,
            workload_passed, workload_failed, revalidation_candidates,
            non_included):
        self.path = path
        self._base_result = copy.deepcopy(base_result)
        self._passed = set(inherited_passed)
        self._failed = set(inherited_failed)
        self._passed -= self._failed
        self._workload_passed = set(workload_passed)
        self._workload_failed = set(workload_failed)
        self._candidates = set(revalidation_candidates)
        self._non_included = list(non_included)
        raw = load_json(path, {})
        self._health_statuses = raw.get(
            HEALTH_STATUS_RESULT_KEY,
            self._base_result.get(HEALTH_STATUS_RESULT_KEY, {}),
        )
        if not isinstance(self._health_statuses, dict):
            self._health_statuses = {}
        raw_passed = normalize_ids(raw.get("passed_sample_ids"))
        self._completed = (
            normalize_ids(raw.get("v1_second_completed_ids"))
            & self._candidates
        )
        for sample_id in self._completed:
            if sample_id in raw_passed:
                self._passed.add(sample_id)
                self._failed.discard(sample_id)
            else:
                self._failed.add(sample_id)
                self._passed.discard(sample_id)
        self._lock = threading.Lock()
        self.persist()

    def _result(self):
        result = copy.deepcopy(self._base_result)
        result.update({
            "format_version": V1_FORMAT_VERSION,
            "stage": "v1_second_filter",
            "workload_evidence_file": V1_EVIDENCE_FILENAME,
            "workload_pass_ids": sorted(self._workload_passed),
            "workload_failed_ids": sorted(self._workload_failed),
            "v1_second_completed_ids": sorted(self._completed),
            "passed_sample_ids": sorted(self._passed),
            "failed_sample_ids": sorted(self._failed),
            "non_included_sample_ids": sorted(self._non_included),
            HEALTH_STATUS_RESULT_KEY: self._health_statuses,
        })
        return result

    def record(self, sample_id, is_passed, health_status=None):
        with self._lock:
            if is_passed:
                self._passed.add(sample_id)
                self._failed.discard(sample_id)
            else:
                self._failed.add(sample_id)
                self._passed.discard(sample_id)
            self._completed.add(sample_id)
            if health_status is not None:
                self._health_statuses[str(sample_id)] = str(health_status)
            write_json_atomic(self.path, self._result())

    def completed_ids(self):
        with self._lock:
            return set(self._completed)

    def persist(self):
        with self._lock:
            write_json_atomic(self.path, self._result())

    def passed_ids(self):
        with self._lock:
            return set(self._passed)

    def failed_ids(self):
        with self._lock:
            return set(self._failed)


def run_anomaly_evaluation_compatible(
        docker_name, anomaly_eval_path, sample_files, system_user_config,
        dbms, database_name, database_schema_name, timeout_threshold, median_runs,
        is_rep, success_threshold_percent, min_abs_delta_seconds,
        anomaly_type, success_criterion, output_dir, logger):
    """Use both established evaluator success markers on every server."""
    command = [
        "python", anomaly_eval_path,
        "--script-path", sample_files["anomaly_inject.sh"],
        "--verified-workload-path", sample_files["verified_workload.sql"],
        "--initialization-workload-path", sample_files["initialization_workload.sql"],
        "--host", str(system_user_config["DB_HOST"]),
        "--port", str(system_user_config["DB_PORT"]),
        "--user", str(system_user_config["DB_USER"]),
        "--database", str(database_name),
        "--schema", str(database_schema_name),
        "--password", str(system_user_config["DB_PASSWORD"]),
        "--timeout", str(timeout_threshold),
        "--median-runs", str(median_runs),
    ]
    if anomaly_eval_supports_flag(anomaly_eval_path, "--dbms"):
        command[2:2] = ["--dbms", str(dbms)]
    if is_rep:
        command.extend([
            "--success-threshold-percent", str(success_threshold_percent),
            "--min-abs-delta-seconds", str(min_abs_delta_seconds),
            "--anomaly-type", str(anomaly_type),
            "--success-criterion", str(success_criterion),
        ])

    log_path = os.path.join(output_dir, ANOMALY_EVAL_LOG_NAME)
    command_ok, _ = run_docker_exec_observation(
        docker_name,
        build_filtered_anomaly_eval_command(command),
        log_path,
        timeout_threshold,
        session_name=docker_name,
    )
    marker_found = log_contains_any(log_path, SUCCESS_MARKERS)
    if command_ok and not marker_found:
        logger.error(
            "[V1][FIRST_FILTER] Evaluator exited successfully without a "
            "recognized success marker: %s", log_path
        )
    return command_ok and marker_found


# The inherited second-stage worker resolves this global in its own module.
run_anomaly_evaluation = run_anomaly_evaluation_compatible


def resolve_sample_context(sample_task, base_dir):
    """Resolve paths and configuration shared by precheck and evaluation."""
    _index, sample_id, sample_info = sample_task
    database_name = sample_info.get("database_name", "")
    sample_directory = find_sample_directory(
        resolve_sample_context.sample_base_directory, sample_id, database_name
    )
    if not os.path.isdir(sample_directory):
        raise ValueError("sample directory not found: %s" % sample_directory)

    sample_files = {
        name: os.path.join(sample_directory, name)
        for name in REQUIRED_SAMPLE_FILES
    }
    missing = [name for name, path in sample_files.items() if not os.path.exists(path)]
    if missing:
        raise ValueError("missing required sample files: %s" % missing)

    sample_config = load_sample_config(sample_directory)
    dbms = sample_config.get("dbms", "PostgreSQL 16.11")
    database_name = sample_config.get("database_name", database_name)
    schema_name = sample_config.get(
        "database_schema_name",
        sample_info.get("database_schema_name", "diagnosis"),
    )
    if not schema_name:
        schema_name = "diagnosis"
    user_config = sample_config.get(
        "system_user_config", default_user_config(dbms)
    )
    hardware_config = sample_config.get(
        "system_hardware_config", default_hardware_config()
    )
    anomaly_eval_path = resolve_path_from_base(
        sample_config.get("anomaly_eval_path", sample_files["anomaly_eval.py"]),
        base_dir,
    )
    if not anomaly_eval_path or not os.path.exists(anomaly_eval_path):
        raise ValueError("anomaly evaluator not found: %s" % anomaly_eval_path)

    return {
        "sample_id": sample_id,
        "sample_files": sample_files,
        "dbms": dbms,
        "database_name": database_name,
        "schema_name": schema_name,
        "user_config": user_config,
        "hardware_config": hardware_config,
        "db_type": resolve_db_type(sample_config, dbms),
        "anomaly_eval_path": anomaly_eval_path,
        "is_rep": anomaly_eval_supports_rep_flags(anomaly_eval_path),
        "success_threshold_percent": sample_config.get(
            "success_threshold_percent", DEFAULT_SUCCESS_THRESHOLD_PERCENT
        ),
        "min_abs_delta_seconds": sample_config.get(
            "min_abs_delta_seconds", DEFAULT_MIN_ABS_DELTA_SECONDS
        ),
        "anomaly_type": resolve_anomaly_type(
            sample_info, read_anomaly_description(sample_directory)
        ),
        "success_criterion": resolve_success_criterion(
            sample_info, read_anomaly_description(sample_directory)
        ),
    }


def run_workload_consistency_rounds(
        container_name, context, output_dir, rounds, timeout_seconds, logger):
    """Run initialization before verified SQL and compare per-statement results."""
    observations = []
    for round_number in range(1, rounds + 1):
        round_dir = os.path.join(output_dir, "v1_workload_round_%d" % round_number)
        os.makedirs(round_dir, exist_ok=True)
        round_result = {}
        for workload_name, file_name in (
            ("initialization", "initialization_workload.sql"),
            ("verified", "verified_workload.sql"),
        ):
            log_path = os.path.join(round_dir, "%s_workload.log" % workload_name)
            sql_path = context["sample_files"][file_name]
            marked_path = None
            marker_prefix = None
            statement_count = None
            if workload_name == "verified":
                marked_path, marker_prefix, statement_count = (
                    create_marked_verified_workload(context["dbms"], sql_path)
                )
                sql_path = marked_path
            try:
                command = build_sql_file_command(
                    context["dbms"],
                    context["user_config"],
                    sql_path,
                    context["database_name"],
                )
                command_ok, _ = run_docker_exec_observation(
                    container_name, command, log_path, timeout_seconds,
                    session_name=container_name,
                )
            finally:
                if marked_path is not None:
                    try:
                        os.remove(marked_path)
                    except OSError:
                        pass
            observation = workload_log_observation(log_path)
            observation["command_ok"] = bool(command_ok)
            if workload_name == "verified":
                parse_marked_verified_observation(
                    observation, marker_prefix, statement_count
                )
            round_result[workload_name] = observation
        write_json_atomic(
            os.path.join(round_dir, "round_result.json"), round_result
        )
        observations.append(round_result)
        logger.info(
            "[V1][WORKLOAD] Completed workload round %d/%d.",
            round_number, rounds,
        )
    verified_observations = [
        verified_comparison_view(item["verified"]) for item in observations
    ]
    return (
        all(
            item == verified_observations[0]
            for item in verified_observations[1:]
        ),
        observations,
    )


def process_v1_first_sample(
        sample_task, worker_base_name, worker_port, base_dir, output_directory,
        median_runs, workload_rounds, workload_timeout, result_store,
        isolate_resources, logger):
    """Record workload consistency, then always run the first anomaly filter."""
    sample_id = sample_task[1]
    output_dir = os.path.join(output_directory, str(sample_id))
    container_name = None
    workload_passed = False
    try:
        os.makedirs(output_dir, exist_ok=True)
        context = resolve_sample_context(sample_task, base_dir)
        container_config = build_container_config(
            context["dbms"], context["hardware_config"], worker_port,
            context["user_config"],
        )
        container_name = make_unique_container_name(worker_base_name)
        container_name = initialize_docker_container(
            container_name, worker_base_name, container_config, logger
        )
        tmp_dbms = context["dbms"].replace(" ", "_")
        if isolate_resources:
            tmp_dbms = os.path.join(tmp_dbms, worker_base_name)
        init_config = {
            "dbms": context["dbms"],
            "base_dir": base_dir,
            "db_host": context["user_config"]["DB_HOST"],
            "db_port": context["user_config"]["DB_PORT"],
            "db_name": context["database_name"],
            "db_type": context["db_type"],
            "db_schema": context["schema_name"],
            "db_user": context["user_config"]["DB_USER"],
            "password": context["user_config"]["DB_PASSWORD"],
            "timeout": DATABASE_INIT_TIMEOUT_SECONDS,
            "output_dir": output_dir,
            "tmp_directory": initialize_shared_tmp_directory(base_dir, tmp_dbms),
        }
        container_name = initialize_database(
            container_name, worker_base_name, init_config, container_config,
            context["database_name"], logger,
        )
        if not run_data_modification(
                container_name, context["dbms"], context["user_config"],
                context["sample_files"]["data_modification.sql"],
                context["database_name"], output_dir, logger):
            raise ValueError("data modification failed")

        workload_passed, observations = run_workload_consistency_rounds(
            container_name, context, output_dir, workload_rounds,
            workload_timeout, logger,
        )
        evidence = {
            "format_version": V1_FORMAT_VERSION,
            "sample_id": sample_id,
            "completed": True,
            "passed": workload_passed,
            "round_count": workload_rounds,
            "comparison_rule": (
                "initialization runs before every verified workload but is not "
                "compared; verified workload status must match, SQL statement "
                "order is preserved, and each statement's normalized output is "
                "compared as an unordered set in every round"
            ),
            "rounds": observations,
        }
        write_json_atomic(os.path.join(output_dir, V1_EVIDENCE_FILENAME), evidence)
        if not workload_passed:
            logger.warning(
                "[V1][WORKLOAD] Sample %s is inconsistent; continuing to "
                "anomaly evaluation.",
                sample_id,
            )

        timeout = calculate_dynamic_timeout_threshold(
            context["sample_files"]["anomaly_inject.sh"],
            context["sample_files"]["merged_workload_performance.json"],
            median_runs,
        )
        passed = run_anomaly_evaluation_compatible(
            container_name, context["anomaly_eval_path"], context["sample_files"],
            context["user_config"], context["dbms"], context["database_name"],
            context["schema_name"], timeout, median_runs, context["is_rep"],
            context["success_threshold_percent"], context["min_abs_delta_seconds"],
            context["anomaly_type"], context["success_criterion"], output_dir, logger,
        )
        result_store.record(sample_id, workload_passed, passed)
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[V1][FIRST_FILTER] Sample %s failed: %s", sample_id, exc)
        result_store.record(sample_id, workload_passed, False)
    finally:
        if container_name is not None:
            kill_tmux_session(container_name)
            docker_container_del(container_name)


def run_v1_first_workers(
        tasks, worker_count, base_dir, docker_name, docker_port, median_runs,
        workload_rounds, workload_timeout, output_directory, result_store,
        logger):
    isolate_resources = worker_count > 1
    prefixes = [
        "%s_v1_w%d" % (docker_name, index) if isolate_resources else "%s_v1" % docker_name
        for index in range(worker_count)
    ]
    for prefix in prefixes:
        remove_containers_by_prefix(prefix)
        kill_tmux_sessions_by_prefix(prefix)

    task_queue = queue.Queue()
    for task in tasks:
        task_queue.put(task)
    stop_event = threading.Event()
    fatal_error = {}
    fatal_lock = threading.Lock()

    def worker(index):
        while not stop_event.is_set():
            try:
                task = task_queue.get_nowait()
            except queue.Empty:
                return
            try:
                process_v1_first_sample(
                    task, prefixes[index], docker_port + index, base_dir,
                    output_directory, median_runs, workload_rounds, workload_timeout,
                    result_store, isolate_resources, logger,
                )
            except (RuntimeError, SystemExit) as exc:
                with fatal_lock:
                    fatal_error.setdefault("error", exc)
                stop_event.set()
                return

    threads = [
        threading.Thread(target=worker, args=(index,), name="v1-first-%d" % index)
        for index in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for prefix in prefixes:
        remove_containers_by_prefix(prefix)
        kill_tmux_sessions_by_prefix(prefix)
    if fatal_error:
        raise RuntimeError("[V1][FIRST_FILTER] Worker aborted: %s" % fatal_error["error"])


def load_stage_result(path):
    if not os.path.isfile(path):
        return set(), set()
    return load_existing_filter_result(path)


def persist_inherited_filter_result(
        result_path, inherited_path, passed, failed, non_included,
        health_statuses=None):
    """Write a stage result without discarding inherited JSON attributes."""
    inherited = load_json(inherited_path, {})
    current = load_json(result_path, {})
    result = copy.deepcopy(inherited)
    for key, value in current.items():
        result.setdefault(key, copy.deepcopy(value))
    inherited_health = result.get(HEALTH_STATUS_RESULT_KEY, {})
    if not isinstance(inherited_health, dict):
        inherited_health = {}
    current_health = current.get(HEALTH_STATUS_RESULT_KEY, {})
    if isinstance(current_health, dict):
        inherited_health.update(current_health)
    if health_statuses is not None:
        inherited_health.update(health_statuses)
    result.update({
        "passed_sample_ids": sorted(set(passed)),
        "failed_sample_ids": sorted(set(failed) - set(passed)),
        "non_included_sample_ids": sorted(non_included),
        HEALTH_STATUS_RESULT_KEY: inherited_health,
    })
    write_json_atomic(result_path, result)


def clean_incomplete_sample_outputs(stage_dir, incomplete_ids, stage_name, logger):
    """Remove only numeric sample directories without a completed stage result."""
    removed = 0
    for sample_id in sorted(set(incomplete_ids)):
        output_dir = os.path.join(stage_dir, str(sample_id))
        if os.path.isdir(output_dir):
            shutil.rmtree(output_dir)
            removed += 1
    if removed:
        logger.info(
            "[V1][%s] Removed %d incomplete sample output directorie(s).",
            stage_name, removed,
        )


def run_historical_second_stage(
        first_passed, first_failed, valid_ids, sample_by_id, second_path,
        second_dir, parallelism, base_dir, docker_name, docker_port, median_runs,
        input_directory, non_included, first_path, inherited_health_statuses,
        logger):
    """Retry historical failures while retaining the complete first-stage JSON."""
    retry_eligible = set(first_failed) & valid_ids
    second_passed = set(first_passed) & valid_ids
    second_failed = set()
    old_second_passed, old_second_failed = load_stage_result(second_path)
    completed_retries = (old_second_passed | old_second_failed) & retry_eligible
    for sample_id in completed_retries:
        if sample_id in old_second_passed:
            second_passed.add(sample_id)
            second_failed.discard(sample_id)
        else:
            second_failed.add(sample_id)
            second_passed.discard(sample_id)
    second_pending_ids = retry_eligible - completed_retries
    clean_incomplete_sample_outputs(
        second_dir, second_pending_ids, "SECOND_FILTER", logger
    )
    second_pending = [
        sample_by_id[sample_id] for sample_id in sorted(second_pending_ids)
    ]
    health_statuses = dict(inherited_health_statuses)
    health_statuses.update(load_health_statuses(second_path))
    second_store = SampleResultStore(
        second_path, second_passed, second_failed, non_included,
        health_statuses=health_statuses,
    )
    second_store.persist()
    logger.info(
        "[V1][SECOND_FILTER] Retrying %d historical anomaly-evaluation "
        "failure(s).",
        len(second_pending),
    )
    run_evaluation_workers(
        second_pending, parallelism, base_dir, "%s_v1_second" % docker_name,
        docker_port, median_runs, second_dir, input_directory, second_store,
        False, logger,
    )
    second_store.persist()
    completed_passed, completed_failed = load_stage_result(second_path)
    persist_inherited_filter_result(
        second_path, first_path, completed_passed, completed_failed,
        non_included, health_statuses=load_health_statuses(second_path),
    )
    return load_stage_result(second_path)


def run_historical_third_stage(
        second_passed, valid_ids, sample_by_id, third_path, third_dir,
        parallelism, base_dir, docker_name, docker_port, input_directory,
        workload_timeout, non_included, second_path, inherited_health_statuses,
        logger):
    """Run third filtering while retaining the complete second-stage JSON."""
    legacy_third_candidates = set(second_passed) & valid_ids
    old_third_passed, old_third_failed = load_stage_result(third_path)
    completed_third = (
        (old_third_passed | old_third_failed) & legacy_third_candidates
    )
    third_passed = old_third_passed & completed_third
    third_failed = old_third_failed & completed_third
    third_pending_ids = legacy_third_candidates - completed_third
    clean_incomplete_sample_outputs(
        third_dir, third_pending_ids, "THIRD_FILTER", logger
    )
    third_pending = [
        sample_by_id[sample_id] for sample_id in sorted(third_pending_ids)
    ]
    health_statuses = dict(inherited_health_statuses)
    health_statuses.update(load_health_statuses(third_path))
    third_store = SampleResultStore(
        third_path, third_passed, third_failed, non_included,
        health_statuses=health_statuses,
    )
    third_store.persist()
    logger.info(
        "[V1][THIRD_FILTER] Running legacy compatibility validation for %d "
        "historical passing sample(s).",
        len(third_pending),
    )
    run_third_filter_workers(
        third_pending, parallelism, base_dir, "%s_v1_legacy_third" % docker_name,
        docker_port, third_dir, input_directory, workload_timeout, third_store,
        logger,
    )
    third_store.persist()
    completed_passed, completed_failed = load_stage_result(third_path)
    persist_inherited_filter_result(
        third_path, second_path, completed_passed, completed_failed,
        non_included, health_statuses=load_health_statuses(third_path),
    )
    return load_stage_result(third_path)


def run_v1_second_stage(
        initial_failed, valid_ids, sample_by_id, result_path, output_directory,
        parallelism, base_dir, docker_name, docker_port, median_runs,
        input_directory, non_included, inherited_result, inherited_passed,
        inherited_failed, workload_passed, workload_failed, logger):
    """Retry V1 anomaly-evaluation failures in a result inheriting first output."""
    candidates = set(initial_failed) & valid_ids
    store = V1SecondResultStore(
        result_path, inherited_result, inherited_passed, inherited_failed,
        workload_passed, workload_failed, candidates, non_included,
    )
    pending_ids = candidates - store.completed_ids()
    clean_incomplete_sample_outputs(
        output_directory, pending_ids, "V1_SECOND_FILTER", logger
    )
    pending = [sample_by_id[sample_id] for sample_id in sorted(pending_ids)]
    logger.info(
        "[V1][V1_SECOND_FILTER] Retrying %d initial anomaly-evaluation "
        "failure(s).",
        len(pending),
    )
    run_evaluation_workers(
        pending, parallelism, base_dir, "%s_v1_second" % docker_name,
        docker_port, median_runs, output_directory, input_directory, store,
        False, logger,
    )
    store.persist()
    return store.passed_ids(), store.failed_ids()


def main():
    parser = argparse.ArgumentParser(
        description="Synthetic anomaly filtering V1 with workload consistency gate"
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--parallelism", type=int, default=None)
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = resolve_path_from_base(args.config, base_dir)
    config = load_json(config_path, None)
    if not isinstance(config, dict):
        raise RuntimeError("Invalid config file: %s" % config_path)

    input_directory = resolve_path_from_base(config.get("input_directory"), base_dir)
    output_directory = resolve_path_from_base(config.get("output_directory"), base_dir)
    if not input_directory or not os.path.isdir(input_directory):
        raise RuntimeError("Invalid input_directory: %s" % input_directory)
    if not output_directory:
        raise RuntimeError("Missing output_directory")
    os.makedirs(output_directory, exist_ok=True)
    resolve_sample_context.sample_base_directory = input_directory

    parallelism = args.parallelism or int(config.get("parallelism", 1))
    parallelism = max(1, int(parallelism))
    median_runs = max(1, int(config.get("median_runs", DEFAULT_MEDIAN_RUNS)))
    workload_rounds = max(
        2, int(config.get("workload_validation_rounds", DEFAULT_WORKLOAD_VALIDATION_ROUNDS))
    )
    workload_timeout = max(
        1, int(config.get(
            "workload_validation_timeout_seconds", DEFAULT_WORKLOAD_VALIDATION_TIMEOUT
        ))
    )
    DATABASE_INIT_TIMEOUT_SECONDS = max(
        1, int(config.get(
            "database_init_timeout_seconds", DEFAULT_DATABASE_INIT_TIMEOUT
        ))
    )
    configure_database_init_parallelism(min(
        parallelism,
        max(1, int(config.get("database_init_parallelism", parallelism))),
    ))
    docker_timeout = int(config.get(
        "docker_init_timeout_seconds", DEFAULT_DOCKER_INIT_TIMEOUT
    ))
    if docker_timeout > 0:
        docker_func.DOCKER_INIT_TIMEOUT = docker_timeout

    docker_name = str(config.get("docker_name", DEFAULT_DOCKER_NAME))
    docker_port = int(config.get("docker_port", DEFAULT_DOCKER_PORT))
    first_dir = os.path.join(output_directory, "first_filter")
    second_dir = os.path.join(output_directory, "second_filter")
    third_dir = os.path.join(output_directory, "third_filter")
    v1_first_dir = os.path.join(output_directory, "v1_first_filter")
    v1_second_dir = os.path.join(output_directory, "v1_second_filter")
    for path in (first_dir, second_dir, third_dir, v1_first_dir, v1_second_dir):
        os.makedirs(path, exist_ok=True)
    first_path = os.path.join(first_dir, "0_filter_result.json")
    second_path = os.path.join(second_dir, "1_filter_result.json")
    third_path = os.path.join(third_dir, "2_filter_results.json")
    v1_first_path = os.path.join(v1_first_dir, V1_FIRST_RESULT_FILENAME)
    v1_second_path = os.path.join(v1_second_dir, V1_SECOND_RESULT_FILENAME)

    logger = set_up_logger(os.path.join(output_directory, "log_v1.txt"))
    logger = logging.getLogger("DB_DIAG.%s" % __name__)
    key_info = load_json(os.path.join(input_directory, "key_information.json"), None)
    if not isinstance(key_info, list):
        raise RuntimeError("key_information.json is unavailable or invalid")
    samples, non_included = build_target_samples(key_info, logger)
    sample_by_id = {sample[1]: sample for sample in samples}
    valid_ids = set(sample_by_id)

    logger.info("[V1] Starting with %d target samples.", len(samples))
    logger.info(
        "[V1] Workload observation: %d rounds, %ss timeout; inconsistency "
        "does not reject anomaly evaluation.",
        workload_rounds, workload_timeout,
    )

    first_passed, first_failed = load_stage_result(first_path)
    first_passed &= valid_ids
    first_failed &= valid_ids
    first_completed = first_passed | first_failed
    logger.info(
        "[V1][HISTORICAL_FIRST] Reusing %d completed first-filter sample(s); "
        "%d sample(s) remain outside the historical first filter.",
        len(first_completed), len(valid_ids - first_completed),
    )

    # Complete all historical first-filter results before creating new V1 ones.
    logger.info(
        "[V1] Completing second/third filtering for %d existing first-filter "
        "sample(s) before V1 initial filtering.",
        len(first_completed),
    )
    second_passed, second_failed = run_historical_second_stage(
        first_passed, first_failed, valid_ids, sample_by_id, second_path,
        second_dir, parallelism, base_dir, docker_name, docker_port, median_runs,
        input_directory, non_included, first_path,
        load_health_statuses(first_path), logger,
    )
    third_passed, third_failed = run_historical_third_stage(
        second_passed, valid_ids, sample_by_id, third_path, third_dir,
        parallelism, base_dir, docker_name, docker_port, input_directory,
        workload_timeout, non_included, second_path,
        load_health_statuses(second_path), logger,
    )

    historical_candidates = second_passed & valid_ids
    historical_passed = third_passed & historical_candidates
    historical_failed = (
        (second_failed & valid_ids) | (third_failed & historical_candidates)
    )
    historical_failed -= historical_passed
    persist_inherited_filter_result(
        third_path, second_path, historical_passed, historical_failed,
        non_included, health_statuses=load_health_statuses(third_path),
    )
    legacy_third_result = load_json(third_path, {})
    third_completed = historical_passed | historical_failed
    v1_pending_ids = valid_ids - third_completed
    logger.info(
        "[V1][HISTORICAL_FINAL] third_filter now contains %d completed "
        "historical sample(s); %d sample(s) require V1 initial filtering.",
        len(third_completed), len(v1_pending_ids),
    )

    initial_store = V1InitialResultStore(
        v1_first_path, v1_pending_ids, legacy_third_result
    )
    v1_initial_pending_ids = v1_pending_ids - initial_store.completed_ids()
    clean_incomplete_sample_outputs(
        v1_first_dir, v1_initial_pending_ids, "V1_INITIAL_FILTER", logger
    )
    v1_initial_pending = [
        sample_by_id[sample_id] for sample_id in sorted(v1_initial_pending_ids)
    ]
    logger.info(
        "[V1][INITIAL_FILTER] Running workload observation plus anomaly "
        "evaluation for %d sample(s).",
        len(v1_initial_pending),
    )
    run_v1_first_workers(
        v1_initial_pending, parallelism, base_dir, docker_name, docker_port,
        median_runs, workload_rounds, workload_timeout, v1_first_dir, initial_store,
        logger,
    )
    initial_store.persist()
    v1_initial_failed = initial_store.anomaly_failed_ids()

    logger.info(
        "[V1] Initial workload/anomaly filtering is complete; retrying the "
        "initial anomaly-evaluation failure set.",
    )
    v1_first_result = load_json(v1_first_path, legacy_third_result)
    inherited_passed, inherited_failed = load_stage_result(v1_first_path)
    inherited_passed &= valid_ids
    inherited_failed &= valid_ids
    inherited_failed -= inherited_passed
    v1_second_passed, v1_second_failed = run_v1_second_stage(
        v1_initial_failed, v1_pending_ids, sample_by_id, v1_second_path,
        v1_second_dir, parallelism, base_dir, docker_name, docker_port, median_runs,
        input_directory, non_included, v1_first_result, inherited_passed,
        inherited_failed, initial_store.workload_passed_ids(),
        initial_store.workload_failed_ids(), logger,
    )

    final_passed = v1_second_passed & valid_ids
    final_failed = v1_second_failed & valid_ids
    final_failed -= final_passed
    missing = valid_ids - final_passed - final_failed
    if missing:
        raise RuntimeError("V1 final state is incomplete for sample IDs: %s" % sorted(missing))
    logger.info(
        "[V1] Finished. Passed: %d, failed: %d. Final result is in %s.",
        len(final_passed), len(final_failed),
        v1_second_path,
    )
    print(RUN_COMPLETION_MARKER, flush=True)



# ============================================================================
# Inlined from the former test_filter_syn_data_v0 module (secret-free). It
# relies on the `from utils import *` / `from docker_func import *` added at
# the top of this file. The two names that collide with this file's own
# top-level functions are prefixed `_v0legacy_` to preserve both behaviours.
# ============================================================================
LOG_NAME = os.path.basename(__file__).split('.')[0]
DEFAULT_CONFIG_PATH = "Configs/filter_syn_data/config.json"
START_ID = 0
END_ID = 3332
MAX_TIMEOUT_SECONDS = 86400
NEGATIVE_EXECUTION_TIME_FALLBACK = 10.0

# Number of times to retry Docker / database initialization before giving up on
# a sample.
RETRY_TIMES = 10

DEFAULT_MEDIAN_RUNS = 1
DEFAULT_DOCKER_NAME = "pg_filter_syn_data"
DEFAULT_DOCKER_PORT = 63002
DEFAULT_SUCCESS_THRESHOLD_PERCENT = 20.0
DEFAULT_MIN_ABS_DELTA_SECONDS = 0.0
DEFAULT_ANOMALY_TYPE = "BOTH"
DEFAULT_SUCCESS_CRITERION = "AUTO"
THIRD_FILTER_ROUNDS = 3
ANOMALY_SUCCESS_MARKERS = (
    "Anomaly successfully simulated and verified.",
    "Anomaly successfully reproduced and verified.",
)
ANOMALY_INJECTION_LOG_START = "========== Anomaly Injection Script: START =========="
ANOMALY_INJECTION_LOG_END = "========== Anomaly Injection Script: END =========="
ANOMALY_INJECTION_LOG_MAX_LINES = 2000
ANOMALY_INJECTION_OUTPUT_TRUNCATED = (
    "[FILTER_SYN_DATA] Anomaly injection command output truncated after "
    f"{ANOMALY_INJECTION_LOG_MAX_LINES} lines."
)
EVALUATOR_CLI_ERROR_MARKERS = (
    "unrecognized arguments:",
)
POST_INJECTION_HEALTH_REPORT_NAME = "post_injection_health_report.json"
HEALTH_STATUS_RESULT_KEY = "post_injection_health_status"
HEALTH_STATUS_NOT_COLLECTED = "not_collected"
HEALTH_STATUS_INDETERMINATE = "indeterminate"
REP_EVAL_MARKER_FLAG = "--success-criterion"
VALID_SUCCESS_CRITERIA = frozenset(
    {"PERFORMANCE_DEGRADATION", "BLOCKING_TIMEOUT", "EXECUTION_ERROR", "AUTO"}
)
VALID_ANOMALY_TYPES = frozenset({"OLAP", "OLTP", "BOTH"})

# Parallel-evaluation defaults. Each worker owns a dedicated container name and
# host port derived from these bases plus the worker index, so concurrent
# workers never collide on either resource.
DEFAULT_PARALLELISM = 1
# Spacing between per-worker host ports. Workers use
# ``docker_port + worker_index * PORT_STRIDE`` so a stray neighbouring port can
# never be claimed by two workers at once.
WORKER_PORT_STRIDE = 1

# Ceiling (seconds) for the `docker run` + in-container init step. Creating a
# container with `--storage-opt size=64G` provisions a quota-backed filesystem,
# which on some storage drivers / loaded hosts is far slower than the previous
# 1200s allowance and was timing out. This value overrides
# ``docker_func.DOCKER_INIT_TIMEOUT`` at runtime and can be tuned from the config
# file via ``docker_init_timeout_seconds``.
DEFAULT_DOCKER_INIT_TIMEOUT = 2400
# Database import is I/O-bound and must not inherit a sample's anomaly-workload
# timeout. A dedicated ceiling prevents valid bulk imports from being cut short.
DEFAULT_DATABASE_INIT_TIMEOUT = 3600
DATABASE_INIT_TIMEOUT_SECONDS = DEFAULT_DATABASE_INIT_TIMEOUT
DATABASE_INIT_SEMAPHORE = None

# Marker printed to STDOUT only after the whole batch truly finishes. The shell
# watchdog requires BOTH a zero exit status AND this marker before treating the
# run as complete, so a premature/abnormal exit (including any ``exit()`` call
# deep inside the imported helpers that would exit with status 0) is retried
# instead of being misreported as success.
RUN_COMPLETION_MARKER = "[FILTER_SYN_DATA][RUN_COMPLETED]"

# Every file a synthetic anomaly sample directory is expected to contain. Used
# to validate the sample before evaluation.
REQUIRED_SAMPLE_FILES = [
    'anomaly_description.json',
    'anomaly_eval.py',
    'user_phenomenon_description.txt',
    'initialization_workload.sql',
    'verified_workload.sql',
    'merged_workload.sql',
    'merged_workload_performance.json',
    'data_modification.sql',
    'data_feature_summarization.txt',
    'anomaly_inject.sh',
]

# Name of the per-sample evaluation log written by ``run_anomaly_evaluation``.
# It is the LAST artifact produced for a sample, and the in-container command
# wrapper (``docker_func.build_exit_code_capture``) appends the ``__DONE__``
# marker to it only after the command truly finishes, writing the command's
# real exit code to a sibling ``.exit`` sentinel just before that. So this pair
# is the authoritative signal of "the sample evaluation flow ran to completion".
ANOMALY_EVAL_LOG_NAME = "anomaly_eval_log.txt"
# Sentinel that stores the in-container exit code (see
# ``docker_func.EXIT_CODE_SUFFIX`` / ``build_exit_code_capture``). An exit code
# of 0 means the anomaly evaluation PASSED, mirroring the pass/fail decision in
# ``docker_func.run_docker_exec_observation``.
EVAL_EXIT_CODE_SUFFIX = ".exit"
# Completion marker appended by the in-container command wrapper once the
# evaluation command has finished (see ``docker_func.wait_for_log_file_done``).
LOG_COMPLETION_MARKER = "__DONE__"


def parse_sample_id(sample_info, fallback_id):
    """Return the integer ``anomaly_id`` of a sample, falling back to its index."""
    raw_id = sample_info.get('anomaly_id', fallback_id)
    try:
        return int(raw_id)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid anomaly_id `{raw_id}`; expected an integer-compatible value.")


def find_sample_directory(sample_base_directory, anomaly_id, database_name):
    """Locate a sample directory.

    The requested path is ``input_directory/<anomaly_id>``; for backward
    compatibility the ``<anomaly_id>_<database_name>`` naming scheme is also
    accepted. The first existing candidate is returned, otherwise the primary
    candidate is returned so the caller can report a meaningful "not found".
    """
    candidate_dirs = [
        os.path.join(sample_base_directory, str(anomaly_id)),
        os.path.join(sample_base_directory, f"{anomaly_id}_{database_name}"),
    ]
    for candidate_dir in candidate_dirs:
        if os.path.isdir(candidate_dir):
            return candidate_dir
    return candidate_dirs[0]


def default_user_config(dbms):
    """Return a sensible default database user configuration for the DBMS."""
    if normalize_dbms(dbms) == "mysql":
        return {
            "DB_USER": "diagnosis",
            "DB_PASSWORD": "diagnosis",
            "DB_HOST": "localhost",
            "DB_PORT": "3306",
            "DB_NAME": "",
        }
    return {
        "DB_USER": "diagnosis",
        "DB_PASSWORD": "diagnosis",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "",
    }


def default_hardware_config():
    """Return the default container hardware configuration.

    The filtering pipeline pins every sample container to a fixed footprint of
    4 CPU cores, 4 GB memory, 8 GB memory + swap and 64 GB storage so that the
    ``docker run`` invocation in ``docker_func.py`` matches the resources the
    anomaly evaluation expects.
    """
    return {
        "CPU": 4,
        "Memory": 4,
        "Memory + Swap": 8,
        "Storage": 64,
    }


def load_sample_config(sample_directory):
    """Load a per-sample ``config.json`` if present, otherwise return ``{}``."""
    sample_config_path = os.path.join(sample_directory, 'config.json')
    if not os.path.exists(sample_config_path):
        return {}
    with open(sample_config_path, 'r', encoding='utf-8') as file:
        return json.load(file)


def read_anomaly_description(sample_directory):
    """Load anomaly metadata used by reproduction-oriented evaluations."""
    path = os.path.join(sample_directory, 'anomaly_description.json')
    try:
        with open(path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return {}


def normalize_success_criterion_value(value):
    normalized = str(value or "").strip().upper().replace(" ", "_").replace("-", "_")
    normalized = "_".join(part for part in normalized.split("_") if part)
    return normalized if normalized in VALID_SUCCESS_CRITERIA else DEFAULT_SUCCESS_CRITERION


def normalize_anomaly_type_value(value):
    normalized = str(value or "").strip().upper().replace(" ", "")
    if normalized in VALID_ANOMALY_TYPES:
        return normalized
    if normalized in {"OLAP+OLTP", "OLTP+OLAP"}:
        return "BOTH"
    return DEFAULT_ANOMALY_TYPE


def resolve_success_criterion(sample_info, anomaly_description):
    return normalize_success_criterion_value(
        sample_info.get('success_criterion') or anomaly_description.get('SuccessCriterion')
    )


def resolve_anomaly_type(sample_info, anomaly_description):
    return normalize_anomaly_type_value(
        sample_info.get('anomaly_type') or anomaly_description.get('Type')
    )


def anomaly_eval_supports_flag(anomaly_eval_path, flag):
    """Return whether an anomaly evaluator source declares a CLI flag."""
    try:
        with open(anomaly_eval_path, 'r', encoding='utf-8', errors='replace') as file:
            return flag in file.read()
    except OSError:
        return False


def anomaly_eval_supports_rep_flags(anomaly_eval_path):
    """Detect whether this sample's anomaly evaluator accepts reproduction flags."""
    return anomaly_eval_supports_flag(anomaly_eval_path, REP_EVAL_MARKER_FLAG)


def build_filtered_anomaly_eval_command(
        anomaly_eval_command,
        injection_log_max_lines=ANOMALY_INJECTION_LOG_MAX_LINES):
    """Wrap an evaluator command and limit streamed injection-script output.

    Both evaluator variants delimit the child anomaly-injection stream with
    stable START/END log messages. The wrapper preserves evaluator logs and its
    exit status while retaining only the first configured number of lines inside
    that interval. Critical evaluator failures are retained even after the limit
    or when an exception prevents the END marker.
    """
    injection_log_max_lines = max(0, int(injection_log_max_lines))
    truncated_message = ANOMALY_INJECTION_OUTPUT_TRUNCATED
    if injection_log_max_lines != ANOMALY_INJECTION_LOG_MAX_LINES:
        truncated_message = (
            "[FILTER_SYN_DATA] Anomaly injection command output truncated after "
            f"{injection_log_max_lines} lines."
        )
    filter_program = "\n".join([
        "import sys",
        f"start = {ANOMALY_INJECTION_LOG_START!r}",
        f"end = {ANOMALY_INJECTION_LOG_END!r}",
        f"max_lines = {injection_log_max_lines!r}",
        f"truncated_message = {truncated_message!r}",
        "critical = (",
        "    'Evaluation flow failed:',",
        "    'Anomaly simulation or verification failed.',",
        "    'Anomaly reproduction or verification failed.',",
        "    'Execution was interrupted by the user.',",
        ")",
        "inside = False",
        "retained_lines = 0",
        "truncation_reported = False",
        "for line in sys.stdin:",
        "    if start in line:",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
        "        inside = True",
        "        retained_lines = 0",
        "        truncation_reported = False",
        "        continue",
        "    if end in line:",
        "        inside = False",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
        "        continue",
        "    if not inside:",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
        "        continue",
        "    if retained_lines < max_lines:",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
        "        retained_lines += 1",
        "        continue",
        "    if not truncation_reported:",
        "        sys.stdout.write(truncated_message + '\\n')",
        "        sys.stdout.flush()",
        "        truncation_reported = True",
        "    if any(marker in line for marker in critical):",
        "        sys.stdout.write(line)",
        "        sys.stdout.flush()",
    ])
    evaluator_shell = " ".join(
        shlex.quote(str(argument)) for argument in anomaly_eval_command
    )
    filter_shell = " ".join([
        "python",
        "-u",
        "-c",
        shlex.quote(filter_program),
    ])
    return [
        "bash",
        "-o",
        "pipefail",
        "-c",
        f"{evaluator_shell} 2>&1 | {filter_shell}",
    ]


def build_target_samples(key_information, logger):
    """Collect samples whose ``anomaly_id`` falls in the configured ID range.

    Returns a tuple of:
    - target_samples: ``[(idx, anomaly_id, sample_info), ...]``
    - non_included_sample_ids: IDs in ``[START_ID, END_ID)`` that do not appear
      in ``key_information.json``
    """
    target_samples = []
    seen_sample_ids = set()
    duplicate_sample_ids = set()

    for idx, sample_info in enumerate(key_information):
        try:
            anomaly_id = parse_sample_id(sample_info, idx)
        except ValueError as exc:
            logger.warning(f"[MAIN][WARNING] Skip invalid sample entry #{idx}: {exc}")
            continue

        if not START_ID <= anomaly_id < END_ID:
            continue

        if anomaly_id in seen_sample_ids:
            duplicate_sample_ids.add(anomaly_id)
        seen_sample_ids.add(anomaly_id)
        target_samples.append((idx, anomaly_id, sample_info))

    if duplicate_sample_ids:
        logger.warning(
            "[MAIN][WARNING] Duplicate sample IDs detected in the configured "
            f"range: {sorted(duplicate_sample_ids)}"
        )

    non_included_sample_ids = sorted(set(range(START_ID, END_ID)) - seen_sample_ids)
    return target_samples, non_included_sample_ids


def extract_numeric_value_from_name(name):
    """Extract the concatenated digits from a directory name."""
    digits = ''.join(char for char in name if char.isdigit())
    if digits == "":
        return None
    return int(digits)


def find_resume_start_id(output_directory):
    """Find the maximum numeric sample ID from existing output sub-directories."""
    if not os.path.isdir(output_directory):
        return None

    max_sample_id = None
    for entry_name in os.listdir(output_directory):
        entry_path = os.path.join(output_directory, entry_name)
        if not os.path.isdir(entry_path):
            continue
        numeric_value = extract_numeric_value_from_name(entry_name)
        if numeric_value is None:
            continue
        if max_sample_id is None or numeric_value > max_sample_id:
            max_sample_id = numeric_value
    return max_sample_id


def log_reached_completion_marker(log_path):
    """Return ``True`` only when the log's final line is ``__DONE__``.

    Mirrors ``docker_func.wait_for_log_file_done``: the in-container command
    wrapper appends this marker to the log only after the command truly
    finishes, so its presence means the corresponding pipeline stage ran to
    completion (as opposed to being killed part-way when the process died).
    """
    if not os.path.isfile(log_path):
        return False
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as file:
            lines = file.read().splitlines()
    except OSError:
        return False
    return bool(lines) and lines[-1].strip() == LOG_COMPLETION_MARKER


def directory_has_entries(path):
    """Return whether a stage directory exists and contains any artifact."""
    if not os.path.isdir(path):
        return False
    try:
        with os.scandir(path) as entries:
            return next(entries, None) is not None
    except OSError:
        return False


def select_resume_stage(first_filter_dir, second_filter_dir, third_filter_dir):
    """Select the highest filtering stage that has already started."""
    if directory_has_entries(third_filter_dir):
        return 3
    if directory_has_entries(second_filter_dir):
        return 2
    return 1


def log_contains(log_path, expected_text):
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as file:
            return expected_text in file.read()
    except OSError:
        return False


def log_contains_any(log_path, expected_texts):
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as file:
            content = file.read()
    except OSError:
        return False
    return any(expected_text in content for expected_text in expected_texts)


def read_log_exit_code(log_path):
    """Read the captured in-container exit code for a completed stage log.

    The exit code lives in the ``<log>.exit`` sentinel written by
    ``docker_func.build_exit_code_capture``. Returns the integer exit code, or
    ``None`` when the sentinel is missing / empty / unparsable (which the caller
    treats as "verdict indeterminate", i.e. not safely complete).
    """
    exit_code_path = log_path + EVAL_EXIT_CODE_SUFFIX
    if not os.path.isfile(exit_code_path):
        return None
    try:
        with open(exit_code_path, 'r', encoding='utf-8', errors='replace') as file:
            content = file.read().strip()
    except OSError:
        return None
    if content == "":
        return None
    try:
        return int(content)
    except ValueError:
        return None


def inspect_sample_completion(sample_output_dir):
    """Classify an existing per-sample output directory.

    A sample is considered COMPLETE only when ``anomaly_eval_log.txt`` ends with
    ``__DONE__`` and has a parseable exit code. A missing anomaly-evaluation log
    is incomplete even if an earlier preparation step failed.

    Returns ``(is_complete, is_passed)``. ``is_passed`` is only meaningful when
    ``is_complete`` is ``True``. Anything else is reported as incomplete so the
    caller re-evaluates the sample from scratch.
    """
    eval_log_path = os.path.join(sample_output_dir, ANOMALY_EVAL_LOG_NAME)
    if log_reached_completion_marker(eval_log_path):
        exit_code = read_log_exit_code(eval_log_path)
        if exit_code is None:
            return False, False
        if exit_code != 0 and log_contains_any(
            eval_log_path, EVALUATOR_CLI_ERROR_MARKERS
        ):
            return False, False
        return True, exit_code == 0 and log_contains_any(
            eval_log_path, ANOMALY_SUCCESS_MARKERS
        )

    return False, False


def read_health_report_status(report_path):
    """Return a persisted health status, or ``None`` when no valid report exists."""
    try:
        with open(report_path, 'r', encoding='utf-8') as file:
            report = json.load(file)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(report, dict):
        return None
    return str(
        report.get("operational_status") or HEALTH_STATUS_INDETERMINATE
    )


def reconcile_existing_results(output_directory, filter_result_path,
                               passed_sample_ids, failed_sample_ids,
                               non_included_sample_ids, valid_sample_ids, logger,
                               health_statuses=None,
                               require_health_report=False):
    """Audit every existing per-sample output directory before evaluation.

    For each sample sub-directory under ``output_directory``:

    - if the sample's evaluation ran to completion, trust the on-disk verdict
      and make ``filter_result.json`` agree with it (correcting a passed/failed
      mislabel or filling in a missing entry);
    - if the evaluation is incomplete (interrupted mid-run, e.g. a worker /
      whole process was killed), delete the partial sample directory and clear
      the sample from both result sets so it is re-evaluated from scratch.

    Directories whose numeric id is unparseable or falls outside the configured
    valid id set are left untouched (they are not part of this run's workload).
    Returns the number of directories removed. ``filter_result.json`` is
    rewritten once at the end so the persisted state matches the reconciled
    in-memory sets exactly.
    """
    if not os.path.isdir(output_directory):
        return 0

    removed_count = 0
    completed_count = 0
    corrected_count = 0
    audited_sample_ids = set()

    for entry_name in sorted(os.listdir(output_directory)):
        entry_path = os.path.join(output_directory, entry_name)
        if not os.path.isdir(entry_path):
            continue

        numeric_value = extract_numeric_value_from_name(entry_name)
        # Only audit directories that correspond to a real in-range sample id.
        # ``<anomaly_id>`` is the canonical layout; other directories are left
        # alone so unrelated artifacts are never deleted.
        if numeric_value is None or numeric_value not in valid_sample_ids:
            continue

        audited_sample_ids.add(numeric_value)
        is_complete, is_passed = inspect_sample_completion(entry_path)
        health_report_path = os.path.join(
            entry_path, POST_INJECTION_HEALTH_REPORT_NAME
        )
        if is_complete and require_health_report:
            eval_completed = log_reached_completion_marker(
                os.path.join(entry_path, ANOMALY_EVAL_LOG_NAME)
            )
            health_status = read_health_report_status(health_report_path)
            if eval_completed and health_status is None:
                is_complete = False
            elif health_statuses is not None:
                health_statuses[str(numeric_value)] = (
                    health_status or HEALTH_STATUS_NOT_COLLECTED
                )

        if not is_complete:
            # Interrupted / partial evaluation: drop the directory and reset the
            # sample to "not evaluated" so it is picked up again below.
            try:
                shutil.rmtree(entry_path)
                removed_count += 1
                logger.warning(
                    f"[MAIN][RECONCILE] Incomplete evaluation for sample "
                    f"`{numeric_value}` detected; removed `{entry_path}` and reset it "
                    f"to un-evaluated."
                )
            except OSError as exc:
                logger.error(
                    f"[MAIN][RECONCILE][ERROR] Failed to remove incomplete sample "
                    f"directory `{entry_path}`: {exc}. Resetting its result state anyway."
                )
            passed_sample_ids.discard(numeric_value)
            failed_sample_ids.discard(numeric_value)
            if health_statuses is not None:
                health_statuses[str(numeric_value)] = HEALTH_STATUS_NOT_COLLECTED
            continue

        # Complete sample: make the recorded verdict match the on-disk truth.
        completed_count += 1
        recorded_passed = numeric_value in passed_sample_ids
        recorded_failed = numeric_value in failed_sample_ids
        if is_passed:
            if not recorded_passed or recorded_failed:
                corrected_count += 1
                logger.info(
                    f"[MAIN][RECONCILE] Sample `{numeric_value}` verdict synced to "
                    f"PASSED from its completed evaluation log."
                )
            passed_sample_ids.add(numeric_value)
            failed_sample_ids.discard(numeric_value)
        else:
            if not recorded_failed or recorded_passed:
                corrected_count += 1
                logger.info(
                    f"[MAIN][RECONCILE] Sample `{numeric_value}` verdict synced to "
                    f"FAILED from its completed evaluation log."
                )
            failed_sample_ids.add(numeric_value)
            passed_sample_ids.discard(numeric_value)

    # Any in-range id still recorded as passed/failed but with NO backing output
    # directory can no longer be verified (the directory was never created or was
    # removed out-of-band). Reset it to un-evaluated so it is re-run rather than
    # trusted blindly, keeping filter_result.json consistent with on-disk state.
    orphaned_ids = (
        (passed_sample_ids | failed_sample_ids) & valid_sample_ids
    ) - audited_sample_ids
    for orphan_id in sorted(orphaned_ids):
        passed_sample_ids.discard(orphan_id)
        failed_sample_ids.discard(orphan_id)
        if health_statuses is not None:
            health_statuses[str(orphan_id)] = HEALTH_STATUS_NOT_COLLECTED
        logger.warning(
            f"[MAIN][RECONCILE] Sample `{orphan_id}` was recorded but has no output "
            f"directory; reset to un-evaluated for re-run."
        )

    # Rewrite the persisted result once so it matches the reconciled sets exactly.
    persist_filter_result(
        filter_result_path,
        passed_sample_ids,
        failed_sample_ids,
        non_included_sample_ids,
        health_statuses,
    )
    logger.info(
        f"[MAIN][RECONCILE] Completeness check done: {completed_count} complete, "
        f"{removed_count} incomplete removed, {corrected_count} verdict(s) corrected, "
        f"{len(orphaned_ids)} orphaned record(s) reset."
    )
    return removed_count


def load_existing_filter_result(filter_result_path):
    """Load an existing filter result file if present."""
    if not os.path.exists(filter_result_path):
        return set(), set()

    with open(filter_result_path, 'r', encoding='utf-8') as file:
        filter_result = json.load(file)

    passed_sample_ids = {
        int(sample_id)
        for sample_id in filter_result.get("passed_sample_ids", [])
    }
    failed_sample_ids = {
        int(sample_id)
        for sample_id in filter_result.get("failed_sample_ids", [])
    }
    return passed_sample_ids, failed_sample_ids


def load_health_statuses(filter_result_path):
    if not os.path.exists(filter_result_path):
        return {}
    try:
        with open(filter_result_path, 'r', encoding='utf-8') as file:
            result = json.load(file)
    except (OSError, json.JSONDecodeError):
        return {}
    statuses = result.get(HEALTH_STATUS_RESULT_KEY, {})
    if not isinstance(statuses, dict):
        return {}
    return {str(sample_id): str(status) for sample_id, status in statuses.items()}


def persist_filter_result(filter_result_path, passed_sample_ids, failed_sample_ids,
                          non_included_sample_ids, health_statuses=None):
    """Atomically persist the current in-progress filtering result."""
    if health_statuses is None:
        health_statuses = load_health_statuses(filter_result_path)
    filter_result = {
        "passed_sample_ids": sorted(passed_sample_ids),
        "failed_sample_ids": sorted(failed_sample_ids),
        "non_included_sample_ids": sorted(non_included_sample_ids),
        HEALTH_STATUS_RESULT_KEY: {
            str(sample_id): health_statuses[str(sample_id)]
            for sample_id in sorted(
                (str(key) for key in health_statuses),
                key=lambda value: (
                    (0, int(value)) if value.isdigit() else (1, value)
                ),
            )
        },
    }
    os.makedirs(os.path.dirname(filter_result_path), exist_ok=True)
    tmp_path = f"{filter_result_path}.tmp"
    with open(tmp_path, 'w', encoding='utf-8') as file:
        json.dump(filter_result, file, ensure_ascii=False, indent=4)
    os.replace(tmp_path, filter_result_path)


def update_sample_result(anomaly_id, is_passed, passed_sample_ids, failed_sample_ids):
    """Update the in-memory result sets for one sample."""
    if is_passed:
        passed_sample_ids.add(anomaly_id)
        failed_sample_ids.discard(anomaly_id)
        return

    failed_sample_ids.add(anomaly_id)
    passed_sample_ids.discard(anomaly_id)


def save_sample_result(filter_result_path, anomaly_id, is_passed,
                       passed_sample_ids, failed_sample_ids,
                       non_included_sample_ids, health_statuses,
                       health_status=None):
    """Update and persist one sample's filtering result."""
    update_sample_result(anomaly_id, is_passed, passed_sample_ids, failed_sample_ids)
    if health_status is not None:
        health_statuses[str(anomaly_id)] = str(health_status)
    persist_filter_result(
        filter_result_path,
        passed_sample_ids,
        failed_sample_ids,
        non_included_sample_ids,
        health_statuses,
    )


def extract_base_timeout_from_script(script_path):
    """Extract ``# TIMEOUT THRESHOLD: <seconds>`` from the anomaly script."""
    with open(script_path, 'r', encoding='utf-8') as file:
        for line in file:
            if "# TIMEOUT THRESHOLD:" not in line:
                continue
            timeout_str = line.split("# TIMEOUT THRESHOLD:")[-1].strip().split()[0].strip()
            return int(timeout_str)
    raise ValueError(f"Timeout threshold marker not found in anomaly script: {script_path}")


def calculate_merged_workload_execution_time(merged_workload_performance_path):
    """Sum execution time from ``merged_workload_performance.json``.

    When a record contains a negative execution time, treat it as an error case
    and use ``NEGATIVE_EXECUTION_TIME_FALLBACK`` seconds instead.
    """
    with open(merged_workload_performance_path, 'r', encoding='utf-8') as file:
        merged_workload_results = json.load(file)

    total_execution_time = 0.0
    for result in merged_workload_results:
        execution_time = result.get('execution_time', 0.0)
        if not isinstance(execution_time, (int, float)):
            continue
        if execution_time < 0:
            total_execution_time += NEGATIVE_EXECUTION_TIME_FALLBACK
        else:
            total_execution_time += execution_time
    return total_execution_time


def calculate_dynamic_timeout_threshold(anomaly_inject_path, merged_workload_performance_path, median_runs):
    """Compute the runtime timeout threshold for one sample.

    The formula mirrors ``test_syn_ano.py``:
    ``base_timeout + median_runs * 2.5 * merged_workload_total_exec_time``,
    capped at 24 hours.
    """
    base_timeout_threshold = extract_base_timeout_from_script(anomaly_inject_path)
    merged_workload_total_exec_time = calculate_merged_workload_execution_time(
        merged_workload_performance_path
    )
    timeout_threshold = base_timeout_threshold + median_runs * 2.5 * merged_workload_total_exec_time
    if timeout_threshold > MAX_TIMEOUT_SECONDS:
        logging.warning(
            "[MAIN][WARNING] Timeout Threshold %s seconds exceeds 24 hours. "
            "Capping to %s seconds.",
            timeout_threshold,
            MAX_TIMEOUT_SECONDS,
        )
    return min(timeout_threshold, MAX_TIMEOUT_SECONDS)


def initialize_docker_container(container_name, base_docker_name, container_config, logger):
    """Step 1: create the Docker container under a per-attempt UNIQUE name.

    ``container_name`` is a freshly generated unique name (see
    ``make_unique_container_name``). On every failed attempt we move to a BRAND
    NEW unique name via ``recreate_container_with_new_name`` instead of reusing
    the same name, so a slow daemon-side teardown of the previous attempt can no
    longer race the next ``docker run`` and trigger an ``RC 125`` name conflict
    (the root cause of the intermittent create-time waits under heavy parallel
    load). ``recreate_container_with_new_name`` reaps the previous container and
    its tmux session first, so no orphan accumulates.

    Neither this helper nor its callee relies on ``docker_container_reinit`` from
    ``docker_func`` -- that helper calls the builtin ``exit()`` on failure, which
    raises ``SystemExit(None)`` and would make the whole process terminate with
    status code 0 (the shell watchdog would then misread the premature exit as a
    successful evaluation). A genuine failure after every retry raises
    ``RuntimeError`` so the caller exits non-zero and the run is retried.

    Returns the live container name so the caller keeps tracking the container
    that is actually running.
    """
    logger.info(f"[MAIN] Initializing Docker Container [{container_name}] ...")
    initialization_times = 0
    while initialization_times < RETRY_TIMES:
        if docker_container_init(container_name, container_config) is True:
            logger.info(f"[MAIN] Docker Container `{container_name}` Initialized Successfully.")
            return container_name

        logger.error(
            f"[MAIN][ERROR] Error in Initialize Docker Container `{container_name}`. "
            f"Retrying ({initialization_times + 1}/{RETRY_TIMES})..."
        )
        initialization_times += 1
        time.sleep(30)
        # Tear the failed attempt down and recreate under a NEW unique name so the
        # next ``docker run`` starts from a clean, conflict-free namespace.
        docker_init_status, container_name = recreate_container_with_new_name(
            container_name, base_docker_name, container_config
        )
        if docker_init_status is True:
            logger.info(f"[MAIN] Docker Container `{container_name}` Initialized Successfully.")
            return container_name

    logger.error(f"[MAIN][ERROR] Error in Initialize Docker Container `{container_name}`. EXIT()")
    raise RuntimeError(f"[MAIN][ERROR] Docker Container `{container_name}` initialization failed after {RETRY_TIMES} retries.")


def initialize_database(container_name, base_docker_name, init_db_config, container_config, database_name, logger):
    """Step 2: initialize the target database inside the container.

    On each failed attempt the container is recreated under a NEW unique name via
    ``recreate_container_with_new_name`` (never reusing a name, and never calling
    ``docker_container_reinit`` -- whose internal ``exit()`` would abort the
    process with status 0 and be misreported as success). A genuine failure after
    every retry raises ``RuntimeError`` so the caller exits non-zero and the run
    is retried.

    Returns the live container name so the caller keeps tracking the container
    that is actually running after any mid-init recreation.
    """
    initialization_times = 0
    while initialization_times < RETRY_TIMES:
        semaphore = DATABASE_INIT_SEMAPHORE
        if semaphore is not None:
            logger.info(
                f"[MAIN][DB_INIT] Waiting for an import slot for `{database_name}` "
                f"in container `{container_name}`."
            )
            semaphore.acquire()
        try:
            initialized = docker_container_init_db(
                container_name, init_db_config, session_name=container_name
            )
        finally:
            if semaphore is not None:
                semaphore.release()

        if initialized is True:
            logger.info(f"[MAIN] Database `{database_name}` Initialization in Docker Container `{container_name}` Succeeded.")
            return container_name

        logger.error(f"[MAIN][ERROR] Database `{database_name}` Initialization in Docker Container `{container_name}` Failed. Retrying ({initialization_times + 1}/{RETRY_TIMES})...")
        initialization_times += 1
        time.sleep(5)
        # Recreate the container under a NEW unique name before the next DB init.
        _, container_name = recreate_container_with_new_name(container_name, base_docker_name, container_config)

    logger.error(f"[MAIN][ERROR] Database `{database_name}` Initialization in Docker Container `{container_name}` Failed. EXIT()")
    raise RuntimeError(f"[MAIN][ERROR] Database `{database_name}` initialization in Docker Container `{container_name}` failed after {RETRY_TIMES} retries.")


def configure_database_init_parallelism(limit):
    """Restrict only bulk database imports, leaving evaluation workers concurrent."""
    global DATABASE_INIT_SEMAPHORE
    DATABASE_INIT_SEMAPHORE = threading.BoundedSemaphore(limit) if limit > 0 else None


def run_data_modification(docker_name, dbms, system_user_config, data_modification_path, database_name, output_dir, logger):
    """Step 3: apply the sample's ``data_modification.sql`` inside the container.

    Mirrors ``test_syn_ano.py`` (L519-531). Returns ``True`` on success.
    """
    logger.info("[MAIN] Modifying Data in Docker Container if Needed...")
    if data_modification_path is None or not os.path.exists(data_modification_path):
        logger.warning("[MAIN][WARNING] Data Modification SQL File is not provided or does not exist.")
        return False

    cmd = build_sql_file_command(dbms, system_user_config, data_modification_path, database_name)
    obser_log = os.path.join(output_dir, "data_modification_log.txt")
    state, _ = run_docker_exec_observation(docker_name, cmd, obser_log, 600, session_name=docker_name)
    if state is False:
        logger.warning(f"[MAIN][WARNING] Data Modification Failed. Check the log file for details : {obser_log}.")
    return state


def run_anomaly_evaluation(docker_name, anomaly_eval_path, sample_files, system_user_config,
                           dbms, database_name, database_schema_name, timeout_threshold, median_runs,
                           is_rep, success_threshold_percent, min_abs_delta_seconds,
                           anomaly_type, success_criterion, output_dir, logger):
    """Step 4: execute the anomaly evaluation script inside the container.

    Mirrors the command construction used in ``test_syn_ano.py`` (L387-401).
    Optional flags are added only when the shipped evaluator declares them, so
    older per-sample ``anomaly_eval.py`` files remain runnable. A pass requires
    both exit code zero and the evaluator's explicit success sentence in the
    captured log.
    """
    temp_shell_script_path = sample_files['anomaly_inject.sh']
    verified_workload_path = sample_files['verified_workload.sql']
    initialization_workload_path = sample_files['initialization_workload.sql']

    anomaly_eval_command_list = [
        "python", anomaly_eval_path,
        "--script-path", temp_shell_script_path,
        "--verified-workload-path", verified_workload_path,
        "--initialization-workload-path", initialization_workload_path,
        "--host", str(system_user_config['DB_HOST']),
        "--port", str(system_user_config['DB_PORT']),
        "--user", str(system_user_config['DB_USER']),
        "--database", str(database_name),
        "--schema", str(database_schema_name),
        "--password", str(system_user_config['DB_PASSWORD']),
        "--timeout", str(timeout_threshold),
        "--median-runs", str(median_runs),
    ]

    if anomaly_eval_supports_flag(anomaly_eval_path, "--dbms"):
        anomaly_eval_command_list[2:2] = ["--dbms", str(dbms)]
    elif normalize_dbms(dbms) == "mysql":
        logger.warning(
            f"[MAIN][Filtering][WARNING] `{anomaly_eval_path}` does not declare "
            "`--dbms`; running without DBMS override. Ensure this evaluator is "
            "compatible with MySQL samples."
        )

    if is_rep:
        anomaly_eval_command_list.extend([
            "--success-threshold-percent", str(success_threshold_percent),
            "--min-abs-delta-seconds", str(min_abs_delta_seconds),
            "--anomaly-type", str(anomaly_type),
            "--success-criterion", str(success_criterion),
        ])

    filtered_eval_command = build_filtered_anomaly_eval_command(
        anomaly_eval_command_list
    )
    logger.info(f"[MAIN][Filtering] - Executing Anomaly Evaluation with Timeout Threshold {timeout_threshold} ...")
    logger.info(
        f"[MAIN][Filtering] The first {ANOMALY_INJECTION_LOG_MAX_LINES} lines "
        "of anomaly injection command output will be retained in the evaluation "
        "log; additional lines will be discarded."
    )
    obser_log = os.path.join(output_dir, ANOMALY_EVAL_LOG_NAME)
    command_ok, _ = run_docker_exec_observation(
        docker_name,
        filtered_eval_command,
        obser_log,
        timeout_threshold,
        session_name=docker_name,
    )
    marker_found = log_contains_any(obser_log, ANOMALY_SUCCESS_MARKERS)
    if not command_ok and log_contains_any(obser_log, EVALUATOR_CLI_ERROR_MARKERS):
        logger.error(
            f"[MAIN][Filtering][ERROR] Anomaly evaluator CLI argument mismatch "
            f"detected in `{obser_log}`. This is an evaluator/filter-script "
            "compatibility error, not an anomaly synthesis failure."
        )
    if command_ok and not marker_found:
        logger.error(
            f"[MAIN][Filtering] Evaluation exited successfully but none of "
            f"`{ANOMALY_SUCCESS_MARKERS}` was found in `{obser_log}`."
        )
    return command_ok and marker_found


def capture_post_injection_health(
        container_name, dbms, system_user_config, host_port,
        database_name, database_schema_name, output_dir, logger):
    """Capture advisory health after anomaly evaluation without changing verdict."""
    report_path = os.path.join(output_dir, POST_INJECTION_HEALTH_REPORT_NAME)
    report = None
    try:
        if judge_database_health is None:
            raise RuntimeError("Scripts.anomaly_judge could not be imported")
        db_config = {
            "dbms": dbms,
            "host": "localhost",
            "host_port": int(host_port),
            "user": system_user_config["DB_USER"],
            "password": system_user_config["DB_PASSWORD"],
            "db_name": database_name,
            "schema": database_schema_name,
        }
        report = judge_database_health(
            db_config,
            logger=logger,
            container_name=container_name,
        )
        if not isinstance(report, dict):
            raise ValueError("health probe returned a non-dictionary report")
    except Exception as exc:
        logger.warning(
            f"[MAIN][HEALTH][WARNING] Post-injection health collection failed: {exc}"
        )
        report = {
            "healthy": None,
            "operational_status": HEALTH_STATUS_INDETERMINATE,
            "collection_error": str(exc),
            "container_name": container_name,
            "database": database_name,
        }

    try:
        if write_database_state_report is not None:
            write_database_state_report(report, report_path)
        else:
            _v0legacy_write_json_atomic(report_path, report)
    except Exception as exc:
        logger.warning(
            f"[MAIN][HEALTH][WARNING] Failed to write `{report_path}`: {exc}"
        )
    return str(
        report.get("operational_status") or HEALTH_STATUS_INDETERMINATE
    )


class SampleResultStore:
    """Thread-safe store for the incremental filtering result.

    Every mutation funnels through :meth:`record` under a single lock so that
    concurrent workers can update the shared passed / failed sets and rewrite
    ``filter_result.json`` without racing or corrupting the file.
    """

    def __init__(self, filter_result_path, passed_sample_ids, failed_sample_ids,
                 non_included_sample_ids, health_statuses=None):
        self._filter_result_path = filter_result_path
        self._passed_sample_ids = set(passed_sample_ids)
        self._failed_sample_ids = set(failed_sample_ids)
        self._non_included_sample_ids = list(non_included_sample_ids)
        self._health_statuses = dict(
            health_statuses
            if health_statuses is not None
            else load_health_statuses(filter_result_path)
        )
        self._lock = threading.Lock()

    def record(self, anomaly_id, is_passed, health_status=None):
        """Update and atomically persist one sample's result."""
        with self._lock:
            save_sample_result(
                self._filter_result_path, anomaly_id, is_passed,
                self._passed_sample_ids, self._failed_sample_ids, self._non_included_sample_ids,
                self._health_statuses, health_status,
            )

    def completed_sample_ids(self):
        """Return the set of already-recorded (passed or failed) sample IDs."""
        with self._lock:
            return self._passed_sample_ids | self._failed_sample_ids

    def discard(self, anomaly_id):
        with self._lock:
            self._passed_sample_ids.discard(anomaly_id)
            self._failed_sample_ids.discard(anomaly_id)

    def persist(self):
        with self._lock:
            persist_filter_result(
                self._filter_result_path,
                self._passed_sample_ids,
                self._failed_sample_ids,
                self._non_included_sample_ids,
                self._health_statuses,
            )

    def counts(self):
        with self._lock:
            return len(self._passed_sample_ids), len(self._failed_sample_ids)


def process_single_sample(sample_task, worker_base_docker_name, worker_docker_port,
                          base_dir, output_directory, sample_base_directory,
                          median_runs, result_store, isolate_resources,
                          collect_health_report, logger):
    """Evaluate a single synthetic anomaly sample end-to-end.

    This is the per-sample body shared by the serial and parallel drivers.
    ``worker_base_docker_name`` is this worker's stable NAME PREFIX and
    ``worker_docker_port`` its dedicated host port, so concurrent workers never
    collide on either resource. The actual container for each sample is created
    under a per-sample UNIQUE name derived from that prefix (see
    ``make_unique_container_name``); names are never reused, so a slow daemon-side
    teardown of the previous sample's container can no longer race the next
    ``docker run`` and stall on an ``RC 125`` name conflict. The unique name
    doubles as the tmux session name, and it is force-removed together with its
    session in a ``finally`` block so the steady state stays at one
    container/session per worker (no orphan accumulates). Results are persisted
    through the thread-safe ``result_store``. A ``RuntimeError`` (Docker /
    database initialization exhausted its retries) is propagated so the caller
    can abort the whole run with a non-zero status; every other error is recorded
    as a sample failure and swallowed so the batch continues.
    """
    _idx, anomaly_id, sample_info = sample_task
    database_name = sample_info.get('database_name', "")
    database_schema_name = sample_info.get('database_schema_name', "")
    sample_dir_name = str(anomaly_id)

    # Per-sample output directory (holds the initialization / data modification
    # / anomaly evaluation logs for this sample).
    output_dir = os.path.join(output_directory, sample_dir_name)

    # Live container name for this sample. Assigned once the per-sample unique
    # name is generated below; kept as ``None`` until then so the ``finally``
    # cleanup is a no-op if we bail out before any container is created.
    container_name = None

    try:
        os.makedirs(output_dir, exist_ok=True)

        logger.info(f"[MAIN][Filtering] Processing Sample `{sample_dir_name}` "
                    f"({sample_info.get('anomaly_name', '')}) for Database `{database_name}` "
                    f"on Worker `{worker_base_docker_name}` (host port {worker_docker_port})")

        # -- Locate the sample directory : input_directory/<anomaly_id> --
        sample_directory = find_sample_directory(sample_base_directory, anomaly_id, database_name)
        if not os.path.isdir(sample_directory):
            logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` FAILED : sample directory not found: {sample_directory}")
            result_store.record(anomaly_id, False)
            return

        # -- Validate that every required file exists in the sample dir. --
        sample_files = {file_name: os.path.join(sample_directory, file_name) for file_name in REQUIRED_SAMPLE_FILES}
        missing_files = [file_name for file_name, file_path in sample_files.items() if not os.path.exists(file_path)]
        if missing_files:
            logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` FAILED : missing required sample files: {missing_files}")
            result_store.record(anomaly_id, False)
            return

        timeout_threshold = calculate_dynamic_timeout_threshold(
            sample_files['anomaly_inject.sh'],
            sample_files['merged_workload_performance.json'],
            median_runs,
        )
        logger.info(
            f"[MAIN][Filtering] Computed Dynamic Timeout Threshold for Sample "
            f"`{sample_dir_name}`: {timeout_threshold} seconds"
        )

        # -- Read the decoupled per-sample configuration. --
        sample_config = load_sample_config(sample_directory)
        DBMS = sample_config.get('dbms', "PostgreSQL 16.11")
        database_name = sample_config.get('database_name', database_name)
        DB_TYPE = resolve_db_type(sample_config, DBMS)
        DB_SCHEMA_NAME = sample_config.get(
            'database_schema_name',
            sample_info.get('database_schema_name', "diagnosis"),
        )
        database_schema_name = sample_config.get('database_schema_name', database_schema_name or DB_SCHEMA_NAME)
        if not database_schema_name:
            database_schema_name = DB_SCHEMA_NAME
        SYSTEM_USER_CONFIG_JSON = sample_config.get('system_user_config', default_user_config(DBMS))
        SYSTEM_HARDWARE_CONFIG = sample_config.get('system_hardware_config', default_hardware_config())

        # The anomaly evaluation script ships inside the sample directory.
        anomaly_eval_path = sample_config.get('anomaly_eval_path', sample_files['anomaly_eval.py'])
        anomaly_eval_path = resolve_path_from_base(anomaly_eval_path, base_dir)
        if not os.path.exists(anomaly_eval_path):
            logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` FAILED : anomaly evaluation script not found: {anomaly_eval_path}")
            result_store.record(anomaly_id, False)
            return

        is_rep = anomaly_eval_supports_rep_flags(anomaly_eval_path)
        anomaly_description = read_anomaly_description(sample_directory)
        success_threshold_percent = sample_config.get(
            'success_threshold_percent', DEFAULT_SUCCESS_THRESHOLD_PERCENT
        )
        min_abs_delta_seconds = sample_config.get(
            'min_abs_delta_seconds', DEFAULT_MIN_ABS_DELTA_SECONDS
        )
        anomaly_type = resolve_anomaly_type(sample_info, anomaly_description)
        success_criterion = resolve_success_criterion(sample_info, anomaly_description)
        logger.info(
            f"[MAIN][Filtering] Sample `{sample_dir_name}` uses "
            f"{'reproduction' if is_rep else 'legacy'} anomaly evaluation."
        )

        container_config = build_container_config(
            DBMS,
            SYSTEM_HARDWARE_CONFIG,
            worker_docker_port,
            SYSTEM_USER_CONFIG_JSON,
        )

        # ---- Step 1 : Docker container initialization. ----
        # Create under a per-sample UNIQUE name derived from this worker's stable
        # prefix. ``initialize_docker_container`` may move to a fresh unique name
        # on a retry, so track whatever it returns as the live container name.
        container_name = make_unique_container_name(worker_base_docker_name)
        container_name = initialize_docker_container(container_name, worker_base_docker_name, container_config, logger)

        # ---- Step 2 : Database initialization inside the container. ----
        logger.info(f"[MAIN] Initializing Database in Docker Container `{container_name}`...")
        tmp_dbms_name = DBMS.replace(" ", "_")
        # In parallel mode give each worker its own TMP sub-tree so concurrent
        # dump / load staging cannot clash on shared files; serial runs keep the
        # original single-level layout for backward compatibility. Keyed on the
        # STABLE worker prefix (not the per-sample unique name) so the sub-tree is
        # reused across samples instead of leaking one directory per sample.
        worker_tmp_dbms_name = os.path.join(tmp_dbms_name, worker_base_docker_name) if isolate_resources else tmp_dbms_name
        tmp_directory = initialize_shared_tmp_directory(base_dir, worker_tmp_dbms_name)
        init_db_config = {
            "dbms": DBMS,
            "base_dir": base_dir,
            "db_host": SYSTEM_USER_CONFIG_JSON['DB_HOST'],
            "db_port": SYSTEM_USER_CONFIG_JSON['DB_PORT'],
            "db_name": database_name,
            "db_type": DB_TYPE,
            "db_schema": DB_SCHEMA_NAME,
            "db_user": SYSTEM_USER_CONFIG_JSON['DB_USER'],
            "password": SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
            "timeout": DATABASE_INIT_TIMEOUT_SECONDS,
            "output_dir": output_dir,
            "tmp_directory": tmp_directory,
        }
        container_name = initialize_database(container_name, worker_base_docker_name, init_db_config, container_config, database_name, logger)

        # ---- Step 3 : Data modification execution. ----
        data_modification_path = sample_files['data_modification.sql']
        if run_data_modification(container_name, DBMS, SYSTEM_USER_CONFIG_JSON, data_modification_path,
                                 database_name, output_dir, logger) is False:
            logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` FAILED : data modification failed.")
            reset_tmux_window(session_name=container_name, window_name="observation")
            result_store.record(anomaly_id, False)
            return

        # ---- Step 4 : Anomaly evaluation code execution. ----
        state = run_anomaly_evaluation(
            container_name, anomaly_eval_path, sample_files, SYSTEM_USER_CONFIG_JSON,
            DBMS, database_name, database_schema_name, timeout_threshold, median_runs,
            is_rep, success_threshold_percent, min_abs_delta_seconds,
            anomaly_type, success_criterion, output_dir, logger,
        )

        health_status = None
        if collect_health_report:
            # Advisory only: this status is persisted independently and never
            # participates in the anomaly-evaluation pass/fail decision.
            health_status = capture_post_injection_health(
                container_name,
                DBMS,
                SYSTEM_USER_CONFIG_JSON,
                worker_docker_port,
                database_name,
                database_schema_name,
                output_dir,
                logger,
            )

        # Decide success / failure from the anomaly evaluation result.
        if state is True:
            logger.info(f"[MAIN][Filtering] Sample `{sample_dir_name}` PASSED the anomaly evaluation.")
        else:
            logger.error(f"[MAIN][Filtering] Sample `{sample_dir_name}` FAILED the anomaly evaluation.")
            reset_tmux_window(session_name=container_name, window_name="observation")
        result_store.record(
            anomaly_id, state is True, health_status=health_status
        )

    except Exception as exc:
        if isinstance(exc, RuntimeError):
            # Docker / database initialization failure : abort the whole run so
            # the shell watchdog restarts it from the resume checkpoint.
            raise
        # Non-initialization sample errors are recorded and the batch continues.
        logger.error(f"[MAIN][ERROR] Sample `{sample_dir_name}` raised an unexpected error during evaluation: {exc}")
        result_store.record(anomaly_id, False)
    finally:
        # Synchronously reap THIS sample's container + tmux session before moving
        # on. Because the next sample always gets a brand new unique name, this
        # teardown never blocks the next ``docker run``; it just keeps the steady
        # state at one container/session per worker so no orphan accumulates
        # (the daemon may finish the removal lazily in the background).
        if container_name is not None:
            kill_tmux_session(container_name)
            docker_container_del(container_name)


def run_evaluation_workers(target_samples, worker_count, base_dir, docker_name, docker_port,
                           median_runs, output_directory, sample_base_directory,
                           result_store, collect_health_report, logger):
    """Drive sample evaluation with ``worker_count`` concurrent workers.

    Samples are pulled from a shared queue; each worker owns a dedicated NAME
    PREFIX (``<docker_name>_w<index>``) and host port
    (``docker_port + index * WORKER_PORT_STRIDE``). Every sample is run in a
    container created under a per-sample UNIQUE name derived from that prefix, so
    concurrent workers never collide and a slow teardown never blocks the next
    ``docker run``. ``worker_count == 1`` keeps the original single-prefix / port
    for full backward compatibility. The first ``RuntimeError`` raised by any
    worker (initialization exhausted its retries) stops the remaining workers and
    is re-raised in the caller so the process exits non-zero and the run is
    retried.

    Before starting, and once every worker has drained the queue, each worker's
    stale containers / tmux sessions are swept by prefix so a previously
    crashed / killed run leaves no orphan behind.
    """
    isolate_resources = worker_count > 1

    # Per-worker stable name prefixes (identical to the naming used inside the
    # worker threads). Sweep any orphan containers / tmux sessions left by a
    # previously interrupted run BEFORE spawning workers, and again at the end.
    def worker_base_name(worker_index):
        return f"{docker_name}_w{worker_index}" if isolate_resources else docker_name

    worker_prefixes = [worker_base_name(worker_index) for worker_index in range(worker_count)]

    logger.info(f"[MAIN] Sweeping stale containers/sessions for worker prefixes {worker_prefixes} before start ...")
    for worker_prefix in worker_prefixes:
        remove_containers_by_prefix(worker_prefix)
        kill_tmux_sessions_by_prefix(worker_prefix)

    task_queue = queue.Queue()
    for sample_task in target_samples:
        task_queue.put(sample_task)

    progress_bar = tqdm(
        total=len(target_samples),
        desc="Filtering Synthetic Anomaly Samples",
    )
    progress_lock = threading.Lock()
    stop_event = threading.Event()
    fatal_error = {}
    fatal_lock = threading.Lock()

    def worker(worker_index):
        worker_base_docker_name = worker_base_name(worker_index)
        worker_docker_port = docker_port + worker_index * WORKER_PORT_STRIDE

        while not stop_event.is_set():
            try:
                sample_task = task_queue.get_nowait()
            except queue.Empty:
                return

            try:
                process_single_sample(
                    sample_task, worker_base_docker_name, worker_docker_port,
                    base_dir, output_directory, sample_base_directory,
                    median_runs, result_store, isolate_resources,
                    collect_health_report, logger,
                )
            except (RuntimeError, SystemExit) as exc:
                # RuntimeError  : Docker / database initialization exhausted its
                #                 retries.
                # SystemExit    : an ``exit()`` fired deep inside an imported
                #                 helper (e.g. TMP-permission failure in
                #                 ``initialize_shared_tmp_directory``). In a
                #                 worker thread that would otherwise kill the
                #                 thread silently and let the main thread report
                #                 a false success, so escalate it to fatal.
                with fatal_lock:
                    fatal_error.setdefault("error", exc)
                stop_event.set()
                return
            finally:
                with progress_lock:
                    progress_bar.update(1)

    threads = [
        threading.Thread(target=worker, args=(worker_index,), name=f"filter-worker-{worker_index}")
        for worker_index in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    try:
        for thread in threads:
            thread.join()
    finally:
        progress_bar.close()
        # Final sweep: once every worker has drained the queue (or the run was
        # aborted), reap any container / tmux session still lingering under each
        # worker prefix so a crashed / killed run leaves no orphan behind.
        logger.info(f"[MAIN] Sweeping stale containers/sessions for worker prefixes {worker_prefixes} after finish ...")
        for worker_prefix in worker_prefixes:
            remove_containers_by_prefix(worker_prefix)
            kill_tmux_sessions_by_prefix(worker_prefix)

    if "error" in fatal_error:
        error = fatal_error["error"]
        if isinstance(error, SystemExit):
            # A bare ``exit()`` carries code ``None`` -> exit status 0, which the
            # shell watchdog would misread as success. Re-raise as a RuntimeError
            # so the process fails loudly with a non-zero status instead.
            raise RuntimeError(
                f"[MAIN][ERROR] A worker aborted via exit()/SystemExit "
                f"(code={error.code}); treating the run as failed."
            )
        raise error


def reconcile_retry_stage(output_directory, candidate_ids, passed_sample_ids,
                          failed_sample_ids, logger):
    """Recover completed second-stage verdicts without touching inherited ones."""
    completed_ids = set()
    for anomaly_id in sorted(candidate_ids):
        sample_output_dir = os.path.join(output_directory, str(anomaly_id))
        if not os.path.isdir(sample_output_dir):
            continue
        is_complete, is_passed = inspect_sample_completion(sample_output_dir)
        if not is_complete:
            shutil.rmtree(sample_output_dir, ignore_errors=True)
            passed_sample_ids.discard(anomaly_id)
            failed_sample_ids.add(anomaly_id)
            continue
        completed_ids.add(anomaly_id)
        update_sample_result(
            anomaly_id, is_passed, passed_sample_ids, failed_sample_ids
        )
    logger.info(
        f"[MAIN][SECOND_FILTER] Recovered {len(completed_ids)} completed retry result(s)."
    )
    return completed_ids


def _v0legacy_write_json_atomic(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, 'w', encoding='utf-8') as file:
        json.dump(value, file, ensure_ascii=False, indent=4)
    os.replace(tmp_path, path)


def workload_log_observation(log_path):
    """Return stable command output, including warnings/errors but not timing."""
    exit_code = read_log_exit_code(log_path)
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as file:
            lines = file.read().splitlines()
    except OSError as exc:
        lines = [f"<LOG_READ_ERROR: {exc}>"]

    normalized_lines = []
    for line in lines:
        if line.strip() == LOG_COMPLETION_MARKER:
            continue
        # psql's optional \timing output is runtime noise, not a query result.
        if line.lstrip().startswith("Time:") and line.rstrip().endswith(" ms"):
            continue
        normalized_lines.append(line)
    return {
        "completed": log_reached_completion_marker(log_path),
        "exit_code": exit_code,
        "output": normalized_lines,
    }


def _v0legacy_run_workload_consistency_rounds(container_name, dbms, user_config,
                                    database_name, sample_files, output_dir,
                                    timeout_seconds, logger):
    """Run initialization + verified workload three times and compare output."""
    observations = []
    for round_number in range(1, THIRD_FILTER_ROUNDS + 1):
        round_dir = os.path.join(output_dir, f"round_{round_number}")
        os.makedirs(round_dir, exist_ok=True)
        round_result = {}
        for workload_name, file_name in (
            ("initialization", "initialization_workload.sql"),
            ("verified", "verified_workload.sql"),
        ):
            log_path = os.path.join(round_dir, f"{workload_name}_workload.log")
            command = build_sql_file_command(
                dbms, user_config, sample_files[file_name], database_name
            )
            command_ok, _ = run_docker_exec_observation(
                container_name,
                command,
                log_path,
                timeout_seconds,
                session_name=container_name,
            )
            observation = workload_log_observation(log_path)
            observation["command_ok"] = bool(command_ok)
            round_result[workload_name] = observation
        _v0legacy_write_json_atomic(os.path.join(round_dir, "round_result.json"), round_result)
        observations.append(round_result)
        logger.info(
            f"[MAIN][THIRD_FILTER] Completed workload round {round_number}/"
            f"{THIRD_FILTER_ROUNDS}."
        )

    is_consistent = all(item == observations[0] for item in observations[1:])
    return is_consistent, observations


def inspect_third_filter_completion(sample_output_dir):
    result_path = os.path.join(sample_output_dir, "third_filter_result.json")
    try:
        with open(result_path, 'r', encoding='utf-8') as file:
            result = json.load(file)
    except (OSError, json.JSONDecodeError):
        return False, False
    if result.get("completed") is not True:
        return False, False
    return True, result.get("passed") is True


def process_third_filter_sample(sample_task, worker_base_docker_name,
                                worker_docker_port, base_dir, output_directory,
                                sample_base_directory, workload_timeout,
                                result_store, isolate_resources, logger):
    """Validate one stage-two passing sample in a fresh healthy environment."""
    _idx, anomaly_id, sample_info = sample_task
    sample_dir_name = str(anomaly_id)
    output_dir = os.path.join(output_directory, sample_dir_name)
    container_name = None
    verdict_path = os.path.join(output_dir, "third_filter_result.json")

    try:
        os.makedirs(output_dir, exist_ok=True)
        database_name = sample_info.get('database_name', "")
        sample_directory = find_sample_directory(
            sample_base_directory, anomaly_id, database_name
        )
        sample_files = {
            file_name: os.path.join(sample_directory, file_name)
            for file_name in REQUIRED_SAMPLE_FILES
        }
        missing_files = [
            name for name, path in sample_files.items() if not os.path.exists(path)
        ]
        if missing_files:
            raise ValueError(f"missing required sample files: {missing_files}")

        sample_config = load_sample_config(sample_directory)
        dbms = sample_config.get('dbms', "PostgreSQL 16.11")
        database_name = sample_config.get('database_name', database_name)
        db_type = resolve_db_type(sample_config, dbms)
        schema_name = sample_config.get(
            'database_schema_name',
            sample_info.get('database_schema_name', "diagnosis"),
        )
        user_config = sample_config.get(
            'system_user_config', default_user_config(dbms)
        )
        hardware_config = sample_config.get(
            'system_hardware_config', default_hardware_config()
        )
        container_config = build_container_config(
            dbms, hardware_config, worker_docker_port, user_config
        )

        container_name = make_unique_container_name(worker_base_docker_name)
        container_name = initialize_docker_container(
            container_name, worker_base_docker_name, container_config, logger
        )
        tmp_dbms_name = dbms.replace(" ", "_")
        worker_tmp_name = (
            os.path.join(tmp_dbms_name, worker_base_docker_name)
            if isolate_resources else tmp_dbms_name
        )
        init_db_config = {
            "dbms": dbms,
            "base_dir": base_dir,
            "db_host": user_config['DB_HOST'],
            "db_port": user_config['DB_PORT'],
            "db_name": database_name,
            "db_type": db_type,
            "db_schema": schema_name,
            "db_user": user_config['DB_USER'],
            "password": user_config['DB_PASSWORD'],
            "timeout": DATABASE_INIT_TIMEOUT_SECONDS,
            "output_dir": output_dir,
            "tmp_directory": initialize_shared_tmp_directory(
                base_dir, worker_tmp_name
            ),
        }
        container_name = initialize_database(
            container_name, worker_base_docker_name, init_db_config,
            container_config, database_name, logger
        )

        # Data modification is part of the sample's healthy baseline, while the
        # anomaly injection is deliberately not executed in this stage.
        if not run_data_modification(
            container_name, dbms, user_config,
            sample_files['data_modification.sql'], database_name,
            output_dir, logger,
        ):
            raise ValueError("third-filter data modification failed")

        is_consistent, observations = _v0legacy_run_workload_consistency_rounds(
            container_name, dbms, user_config, database_name, sample_files,
            output_dir, workload_timeout, logger,
        )
        _v0legacy_write_json_atomic(verdict_path, {
            "sample_id": anomaly_id,
            "completed": True,
            "passed": is_consistent,
            "round_count": THIRD_FILTER_ROUNDS,
            "comparison_rule": (
                "initialization and verified workload exit status plus normalized "
                "stdout/stderr must match in all rounds"
            ),
            "rounds": observations,
        })
        result_store.record(anomaly_id, is_consistent)
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error(
            f"[MAIN][THIRD_FILTER][ERROR] Sample `{sample_dir_name}` failed: {exc}"
        )
        _v0legacy_write_json_atomic(verdict_path, {
            "sample_id": anomaly_id,
            "completed": True,
            "passed": False,
            "error": str(exc),
        })
        result_store.record(anomaly_id, False)
    finally:
        if container_name is not None:
            kill_tmux_session(container_name)
            docker_container_del(container_name)


def run_third_filter_workers(target_samples, worker_count, base_dir, docker_name,
                             docker_port, output_directory, sample_base_directory,
                             workload_timeout, result_store, logger):
    isolate_resources = worker_count > 1
    task_queue = queue.Queue()
    for task in target_samples:
        task_queue.put(task)
    worker_prefixes = [
        f"{docker_name}_third_w{index}" if isolate_resources else f"{docker_name}_third"
        for index in range(worker_count)
    ]
    for prefix in worker_prefixes:
        remove_containers_by_prefix(prefix)
        kill_tmux_sessions_by_prefix(prefix)

    fatal_error = {}
    fatal_lock = threading.Lock()
    progress = tqdm(total=len(target_samples), desc="Third-Level Filtering")

    def worker(index):
        while True:
            try:
                task = task_queue.get_nowait()
            except queue.Empty:
                return
            try:
                process_third_filter_sample(
                    task, worker_prefixes[index],
                    docker_port + index * WORKER_PORT_STRIDE,
                    base_dir, output_directory, sample_base_directory,
                    workload_timeout, result_store, isolate_resources, logger,
                )
            except (RuntimeError, SystemExit) as exc:
                with fatal_lock:
                    fatal_error.setdefault("error", exc)
                return
            finally:
                progress.update(1)

    threads = [
        threading.Thread(target=worker, args=(index,), name=f"third-filter-{index}")
        for index in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    progress.close()
    for prefix in worker_prefixes:
        remove_containers_by_prefix(prefix)
        kill_tmux_sessions_by_prefix(prefix)
    if fatal_error:
        raise RuntimeError(
            f"[MAIN][THIRD_FILTER] Worker aborted: {fatal_error['error']}"
        )


if __name__ == "__main__":
    main()
