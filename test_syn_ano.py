import openai
import json
import os
import time
import shutil
import re
from tqdm import tqdm
import argparse
import logging

from prompt_template import *
from utils import *
from llm_api import *
from docker_func import *

# python test_syn_ano.py --config ./Configs/syn_ano/example_config.json

openai.api_key = os.environ.get("OPENAI_API_KEY", "")
openai.base_url = os.environ.get("OPENAI_BASE_URL", "https://api.example.com/v1")
model_name = os.environ.get("LLM_MODEL", "your-chat-model")
temperature = 1

GB_Configurations = ["Memory", "Memory + Swap", "Storage"]

LOG_NAME = os.path.basename(__file__).split('.')[0]


def resolve_fast_reset_enabled(dbms_kind, config):
    """Allow logical golden clones only for explicitly controlled MySQL runs."""
    requested = bool(config.get("fast_reset_enabled", True))
    if dbms_kind != "mysql":
        return requested
    return requested and bool(
        config.get("mysql_fast_reset_experimental_enabled", False)
    )
# openai.base_url = "https://api.example.com/v1"

# model_name = "gemini-3-pro-preview"
# model_name = "gemini-3-pro"
# temperature = 0.8

olap_databases = ['tpch_1', 'tpch_10', 'tpcds_1', 'imdb']
oltp_databases = ['benchbase']

TIMEOUTS = {'tpch_1' : 240, 'imdb' : 480, 'tpcds_1' : 360, 'tpch_10' : 420} 
TIMEOUTS = {'tpch_1' : 1500, 'imdb' : 4500, 'tpcds_1' : 1800, 'tpch_10' : 11800}
RETRY_TIMES = 10
FAKE_DATABASE_IP = "8888.8888.8888.8888"

database_schema_mapping = {
}

# Curated skill card(s) injected into the anomaly-synthesis prompt via the
# `{SKILLS}` placeholder. `fast_environment_reset.md` is the one that governs the
# synthesis flow: it tells the LLM to keep every injection side effect INSIDE the
# working database so the per-turn golden-clone reset can undo it cheaply and
# reliably (the same discipline that prevents runaway backends / "database does
# not exist" residue after a turn). Ordered; unknown files are appended.
SYNTHESIS_SKILL_FILES = ["fast_environment_reset.md"]


def load_synthesis_skills(skills_dir, logger, preferred_order=None):
    """Concatenate the curated synthesis skill markdown files for the prompt.

    Mirrors ``test_syn_ano_rep.load_reproduction_skills``: the Skills folder is
    optional, files are included in a stable order, and each is wrapped in a
    labelled block. Returning a harmless placeholder (never raising) keeps the
    prompt build robust -- a missing skill only omits guidance, never blocks
    synthesis.
    """
    if preferred_order is None :
        preferred_order = SYNTHESIS_SKILL_FILES

    if not skills_dir or not os.path.isdir(skills_dir) :
        logger.warning(f"[MAIN][WARNING] Skills directory not found: {skills_dir}. Proceeding without curated skills.")
        return "(No curated synthesis skills are available for this run.)"

    available = sorted(f for f in os.listdir(skills_dir) if f.lower().endswith(".md"))
    # Only the synthesis-relevant skills are loaded (in preferred order); other
    # skill cards in the folder target the reproduction flow and are skipped.
    ordered = [f for f in preferred_order if f in available]

    if not ordered :
        logger.warning(f"[MAIN][WARNING] No synthesis skill files found under {skills_dir}. Proceeding without curated skills.")
        return "(No curated synthesis skills are available for this run.)"

    blocks = []
    for filename in ordered :
        with open(os.path.join(skills_dir, filename), "r", encoding="utf-8") as fh :
            content = fh.read().strip()
        blocks.append(f"===== SKILL FILE: {filename} =====\n{content}")
    logger.info(f"[MAIN] Loaded {len(ordered)} synthesis skill file(s): {ordered}")
    return "\n\n".join(blocks)


def run_data_modification_step(docker_name, dbms, system_user_config, data_modification_path,
                               database_name, output_dir, logger, log_filename="data_modification_log.txt"):
    """
    Apply the prepared data-modification SQL to the freshly initialized database.
    This step must be executed both before the first LLM turn and after every
    per-turn Docker/database re-initialization so each turn starts from the same
    pre-injection database state.
    """
    logger.info("[MAIN] Modifying Data in Docker Container if Needed...")
    if data_modification_path is None :
        logger.warning(f"[MAIN][WARNING] Data Modification Docker Path is not provided in the system options. EXIT()")
        raise RuntimeError("[MAIN][ERROR] Data Modification Docker Path is not provided in the system options.")
    if not os.path.exists(data_modification_path) :
        logger.warning(f"[MAIN][WARNING] Data Modification SQL File does not exist: {data_modification_path}. EXIT()")
        raise RuntimeError(f"[MAIN][ERROR] Data Modification SQL File does not exist: {data_modification_path}.")

    cmd = build_sql_file_command(dbms, system_user_config, data_modification_path, database_name)
    obser_log = os.path.join(output_dir, log_filename)
    state, obser_output = run_docker_exec_observation(docker_name, cmd, obser_log, 600, session_name=docker_name)
    if state is False :
        logger.warning(f"[MAIN][WARNING] Data Modification Failed. Check the log file for details : {obser_log}. EXIT()")
        if obser_output != "" :
            logger.warning(f"[MAIN][WARNING] Data Modification Command Output : {obser_output}")
        reset_tmux_window(session_name=docker_name, window_name="observation")
        raise RuntimeError(f"[MAIN][ERROR] Data Modification Failed. Check the log file for details : {obser_log}.")

    logger.info(f"[MAIN] Data Modification Succeeded. Log saved to: {obser_log}")


def reinitialize_docker_environment_for_next_turn(docker_name, base_docker_name, container_config, init_db_config,
                                                   dbms, system_user_config, data_modification_path,
                                                   output_dir, logger, turn_idx,
                                                   fast_reset_state=None, force_full_rebuild=False):
    """
    Reset the experimental environment so the next LLM turn starts from a clean
    but semantically IDENTICAL pre-injection baseline.

    Two paths, tried in order:

    1) FAST RESET (preferred): if a container-local golden baseline exists for
       this sample (``fast_reset_state['snapshot_ready']``), restore the working
       database from it IN PLACE via ``docker_container_fast_reset`` -- no
       container teardown, no dump re-import, no data-modification replay (the
       golden clone already embeds it). This is the seconds-scale path that
       eliminates the per-turn multi-GB reinitialization bottleneck.

    2) FULL REBUILD (fallback): the original behaviour -- recreate the container
       under a NEW unique name (reaping the old container + tmux session first),
       reinitialize the database from the dump, and re-apply the data
       modification. Used on the first turn before a snapshot exists, whenever
       fast reset is disabled, or whenever fast reset fails for any reason.

    ``force_full_rebuild`` forces path 2 even when a golden snapshot exists. The
    caller sets it after an ORCHESTRATION-LEVEL OBSERVATION TIMEOUT: on a timeout
    the anomaly-eval + injection processes are still running INSIDE the container
    (killing the tmux window only detaches the client), so a DB-only fast reset
    would race those runaway backends -- exactly the source of the intermittent
    "database does not exist" / "database is being accessed by other users"
    failures. A full ``docker rm -f`` rebuild atomically kills every in-container
    process, guaranteeing a clean baseline before the next turn.

    Returns the (possibly new) container name so the caller keeps tracking the
    live container across turns.
    """
    db_name = init_db_config['db_name']

    # ---- Path 1: fast reset from the container-local golden baseline ----------
    # Skipped when ``force_full_rebuild`` is set (e.g. after an observation
    # timeout) because a DB-only reset cannot reap runaway in-container processes.
    if (not force_full_rebuild
            and fast_reset_state and fast_reset_state.get('enabled')
            and fast_reset_state.get('snapshot_ready')):
        logger.info(f"[Anomaly Injection Synthesis] -- Fast-Resetting Environment [Turn {turn_idx}] (golden baseline) ...")
        try:
            init_db_config_turn = dict(init_db_config)
            init_db_config_turn['output_dir'] = output_dir
            if docker_container_fast_reset(docker_name, init_db_config_turn, session_name=docker_name):
                logger.info(f"[MAIN] Fast reset of `{db_name}` in container `{docker_name}` Succeeded [Turn {turn_idx}].")
                # No data-modification replay: the golden baseline already embeds
                # the applied data modification, guaranteeing an identical state.
                return docker_name
            logger.warning(
                f"[Anomaly Injection Synthesis][WARNING] Fast reset failed at Turn {turn_idx}; "
                f"falling back to full container rebuild."
            )
        except Exception as e:  # never let the fast path break the loop
            logger.warning(
                f"[Anomaly Injection Synthesis][WARNING] Fast reset raised at Turn {turn_idx} "
                f"[{e}]; falling back to full container rebuild."
            )
    elif force_full_rebuild:
        logger.info(
            f"[Anomaly Injection Synthesis] -- Forcing FULL container rebuild [Turn {turn_idx}] "
            f"(observation timeout: reaping runaway in-container processes) ..."
        )

    # ---- Path 2: original full container rebuild (fallback / first turn) -------
    logger.info(f"[Anomaly Injection Synthesis] -- Re-initializing Docker Container [Turn {turn_idx}]...")
    initialization_times = 0
    docker_init_status = False
    while initialization_times < RETRY_TIMES :
        # Tear down the current container + session and create a fresh unique name.
        docker_init_status, docker_name = recreate_container_with_new_name(docker_name, base_docker_name, container_config)
        if docker_init_status is True :
            logger.info(f"[MAIN] Docker Container ``{docker_name}`` Initialized Successfully.")
            break

        logger.error(f"[MAIN][ERROR] Error in Initialize Docker Container ``{docker_name}''. Retrying ({initialization_times}/{RETRY_TIMES})...")
        initialization_times += 1
        time.sleep(5)

    if docker_init_status is False :
        logger.error(f"[MAIN][ERROR] Error in Initialize Docker Container ``{docker_name}''. EXIT()")
        raise RuntimeError(f"[MAIN][ERROR] Docker Container `{docker_name}` initialization failed after {RETRY_TIMES} retries.")

    initialization_times = 0
    docker_status = False
    while initialization_times < RETRY_TIMES :
        docker_status = docker_container_init_db(docker_name, init_db_config, session_name=docker_name)
        if docker_status is True :
            logger.info(f"[MAIN] Database `{db_name}` Initialization in Docker Container `{docker_name}` Succeeded.")
            break
        else :
            logger.error(f"[MAIN][ERROR] Database `{db_name}` Initialization in Docker Container `{docker_name}` Failed. Retrying ({initialization_times}/{RETRY_TIMES})...")
            initialization_times += 1
            time.sleep(5)
            _, docker_name = recreate_container_with_new_name(docker_name, base_docker_name, container_config)

    if docker_status is False :
        logger.error(f"[MAIN][ERROR] Database `{db_name}` Initialization in Docker Container `{docker_name}` Failed. EXIT()")
        raise RuntimeError(f"[MAIN][ERROR] Database `{db_name}` initialization in Docker Container `{docker_name}` failed after {RETRY_TIMES} retries.")

    # Re-apply the prepared data modification after the clean reinitialization;
    # otherwise the next turn would run on an inconsistent baseline state.
    logger.info(f"[Anomaly Injection Synthesis] -- Re-applying Data Modification [Turn {turn_idx}] --")
    run_data_modification_step(
        docker_name,
        dbms,
        system_user_config,
        data_modification_path,
        db_name,
        output_dir,
        logger,
        log_filename="reinit_data_modification_log.txt"
    )

    # The container was rebuilt under a new name, so any previously captured
    # golden baseline is gone. Rebuild it here (best-effort) from the freshly
    # reinitialized + data-modified state so subsequent turns can fast-reset
    # again. A failure here only disables the fast path; correctness is unaffected.
    if fast_reset_state and fast_reset_state.get('enabled'):
        try:
            init_db_config_snap = dict(init_db_config)
            init_db_config_snap['output_dir'] = output_dir
            snapshot_ok = docker_container_build_baseline_snapshot(
                docker_name, init_db_config_snap, session_name=docker_name
            )
            fast_reset_state['snapshot_ready'] = bool(snapshot_ok)
            if not snapshot_ok :
                logger.warning(
                    f"[Anomaly Injection Synthesis][WARNING] Could not rebuild golden baseline "
                    f"after full rebuild at Turn {turn_idx}; fast reset stays disabled until next rebuild."
                )
        except Exception as e:
            fast_reset_state['snapshot_ready'] = False
            logger.warning(
                f"[Anomaly Injection Synthesis][WARNING] Golden baseline rebuild raised at Turn {turn_idx} [{e}]."
            )

    return docker_name


def llm_infer_multi(prompt, config_opt, logger, median_runs=1):  
    """
    Final Output Path : temp_anomaly.sh, temp_eval.py
    """  
    logger.info("[Anomaly Injection Synthesis] Starting Multi-turn LLM Inference for Anomaly Injection Synthesis...")
    llm_opt = config_opt['llm_opt']
    system_opt = config_opt['system_opt']
    anomaly_eval_opt = config_opt['anomaly_eval_opt']
    MAX_TURN_NUM = config_opt['max_turn_num']
    
    merged_workload_total_exec_time = anomaly_eval_opt['merged_workload_execution_time']
    # Number of MEASURED runs passed to anomaly_eval.py (median taken, preceded
    # by one warm-up pass). Defaults to 1 and may be raised to smooth out latency
    # noise; every run re-applies the initialization workload downstream so a
    # repeated run always starts from the same restored state.
    median_runs = max(1, int(median_runs))

    docker_name = system_opt['docker_name']
    base_docker_name = system_opt['base_docker_name']
    output_mes_path = system_opt['output_mes_path']
    output_base_dir = system_opt['output_base_dir']
    system_user_config = system_opt['system_user_config']
    dbms = system_opt['dbms']
    data_modification_path = system_opt['data_modification_path']
    
    init_db_config = system_opt['init_db_config']
    container_config = system_opt['container_config']

    # Fast-reset state (container reuse + golden clone). Shared, mutable dict so
    # the per-turn reinit helper can flip `snapshot_ready` off/on across turns.
    # MySQL defaults to disabled; PostgreSQL remains enabled unless configured
    # otherwise.
    fast_reset_state = system_opt.get('fast_reset_state') or {'enabled': True, 'snapshot_ready': False}

    temp_shell_script_path = os.path.join(output_base_dir, f"temp_anomaly.sh")

    messages = [prompt.replace("SCRIPT_PATH_PLACEHOLDER", temp_shell_script_path)]
    pure_response = ""
    finish_synthesized = False

    # Build the golden baseline ONCE up-front. The container handed to this
    # function already holds the ready state (dump imported + initial data
    # modification applied by the caller), so a snapshot taken here captures the
    # exact pre-injection baseline that every subsequent turn must return to.
    # Best-effort: on failure the fast path stays off and the loop transparently
    # uses the original full-rebuild reinitialization.
    if fast_reset_state.get('enabled') and not fast_reset_state.get('snapshot_ready'):
        try:
            init_db_config_snap = dict(init_db_config)
            init_db_config_snap['output_dir'] = output_base_dir
            snapshot_ok = docker_container_build_baseline_snapshot(
                docker_name, init_db_config_snap, session_name=docker_name
            )
            fast_reset_state['snapshot_ready'] = bool(snapshot_ok)
            if snapshot_ok :
                logger.info("[Anomaly Injection Synthesis] Golden baseline captured; per-turn fast reset ENABLED.")
            else :
                logger.warning("[Anomaly Injection Synthesis][WARNING] Golden baseline capture failed; using full-rebuild reset.")
        except Exception as e:
            fast_reset_state['snapshot_ready'] = False
            logger.warning(f"[Anomaly Injection Synthesis][WARNING] Golden baseline capture raised [{e}]; using full-rebuild reset.")

    for i in range(MAX_TURN_NUM) :
        if i == MAX_TURN_NUM - 1 : logger.info(f"[Anomaly Injection Synthesis] -- Multi-Turn LLM Reasoning at Turn {i} (MAX TURN NUMBER) --")
        else : logger.info(f"[Anomaly Injection Synthesis] -- Multi-Turn LLM Reasoning at Turn {i} --")

        output_sub_dir = os.path.join(output_base_dir, f"{i}_multi_turn")
        if not os.path.exists(output_sub_dir) : os.makedirs(output_sub_dir)
        shell_script_path = os.path.join(output_sub_dir, f"anomaly.sh")

        cnt = 0
        reasoning_responses = ""
        json_response = ""
        code_action_state = False
        finish_action_state = False
        matched_mess = False
        while cnt < llm_opt['retry_times'] : 
            response = get_llm_response(messages, llm_opt)

            reasoning_responses += response + "\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
            with open(os.path.join(output_sub_dir, f"reasoning_responses.txt"), 'w') as file :
                file.write(reasoning_responses)

            pure_response = response.split('<action>')[-1].split('</action>')[0].strip().replace(FAKE_DATABASE_IP, system_user_config['DB_HOST'])
            try:
                json_str = extract_json_object(pure_response)
                json_response = json.loads(json_str)
            except (ValueError, json.JSONDecodeError) as e:
                logger.warning(f"[Anomaly Injection Synthesis][WARNING] Failed to extract valid JSON from action block: {e} --> Retrying ({cnt}/{llm_opt['retry_times']})...")
                cnt += 1
                continue

            code_action_state = (json_response.keys() == {'action', 'shell'} and json_response['action'] == 'code' and "# TIMEOUT THRESHOLD: " in json_response['shell'])
            finish_action_state = (json_response.keys() == {'action', 'anomaly_inject.sh'} and json_response['action'] == 'finish' and "# TIMEOUT THRESHOLD: " in json_response['anomaly_inject.sh'])

            if code_action_state or finish_action_state :
                matched_mess = True
                break
            else : 
                logger.warning(f"[Anomaly Injection Synthesis][WARNING] Unrecognized Response Format from LLM --> Retrying ({cnt}/{llm_opt['retry_times']})...")
            
            cnt += 1
        if matched_mess is False : logger.warning(f"[Anomaly Injection Synthesis][WARNING] LLM does not infer the expected message with in the retried times.")
        messages.append(response)

        logger.info(f"[Anomaly Injection Synthesis] -- LLM Inference Finish [Turn {i}] --")

        # Action Parse and Execute
        observation = ""
        # Set when the observation hit an orchestration-level timeout: the
        # in-container anomaly-eval + injection processes may still be alive, so
        # the next-turn reset must do a FULL container rebuild (which reaps them)
        # rather than a DB-only fast reset that would race the runaway backends.
        observation_timed_out = False
        if code_action_state is True :
            with open(temp_shell_script_path, 'w') as file :
                file.write(json_response['shell'])
            shutil.copy(temp_shell_script_path, shell_script_path)

            timeout_threshold = int(json_response['shell'].split("# TIMEOUT THRESHOLD: ")[-1].strip().split()[0].strip())
            timeout_threshold += median_runs * 2.5 *  merged_workload_total_exec_time
            if timeout_threshold > 86400 : logger.warning(f"[Anomaly Injection Synthesis][WARNING] Timeout Threshold {timeout_threshold} seconds exceeds 24 hours. Capping to 86400 seconds.")
            timeout_threshold = min(timeout_threshold, 86400)

            # Execute the generated script in the docker container and get the observation
            logger.info(f"[Anomaly Injection Synthesis] - Executing Script with Timeout Threshold {timeout_threshold} at Turn {i}...")
            anomaly_eval_command_list = [
                "python",
                anomaly_eval_opt['anomaly_eval_path'],
                "--dbms", str(anomaly_eval_opt.get('dbms', 'PostgreSQL 16.11')),
                "--script-path", temp_shell_script_path,
                "--verified-workload-path", anomaly_eval_opt['verified_workload_path'],
                "--initialization-workload-path", anomaly_eval_opt['initialization_workload_path'],
                "--host", anomaly_eval_opt['host'],
                "--port", str(anomaly_eval_opt['port']),
                "--user", anomaly_eval_opt['user'],
                "--database", anomaly_eval_opt['database'],
                "--schema", anomaly_eval_opt['schema'],
                "--password", anomaly_eval_opt['password'],
                "--timeout", str(timeout_threshold),
                "--median-runs", str(median_runs)
            ]
            obser_log = os.path.join(output_sub_dir, "obser_log.txt")
            state, obser_output = run_docker_exec_observation(docker_name, anomaly_eval_command_list, obser_log, timeout_threshold, session_name=docker_name)            
            
            # Observation Parsing and Formatting
            if state is True : 
                # NOTE: A verification workload that is blocked by a SUCCESSFULLY
                # injected anomaly until it times out is already reported as a
                # success by anomaly_eval.py (it returns 0 and logs an explicit
                # "anomaly successfully injected" message), so it reaches this
                # branch. The injection-success explanation is carried in the
                # observation log appended below.
                observation = "Script executed successfully, with the output as follows:\n"
            else :
                # Case 2: a genuine script-level (orchestration) timeout means the
                # whole anomaly evaluation flow did not finish within the allotted
                # time budget, i.e. the timeout threshold is insufficient. Make
                # this explicit so the next LLM turn is guided to increase the
                # threshold rather than misreading an injection-blocked workload
                # (which is handled as success above).
                if "Command timed out" in obser_output :
                    observation = ("Script execution timed out at the orchestration level: the whole "
                                   "anomaly evaluation flow did not finish within the allotted time "
                                   "budget. This indicates the timeout threshold is insufficient and "
                                   "should be increased. Output as follows:\n")
                    # On a timeout the in-container processes are likely still
                    # running; force a full container rebuild next turn so they are
                    # reaped atomically instead of racing a DB-only fast reset.
                    observation_timed_out = True
                else :
                    observation = "Script executed failed, with the output as follows:\n"

                reset_tmux_window(session_name=docker_name, window_name="observation")

            observation += prune_obser_log(obser_log)
            if obser_output != "" : observation += f"\n\n-- COMMENTS :\n{obser_output}"
                
            messages.append(observation)

        else :
            logger.info("[Anomaly Injection Synthesis] - FINISH Reasoning Process -")
            
            messages_json = {}
            for idx, message in enumerate(messages) :
                messages_json[f"message_{idx}"] = message
            with open(output_mes_path, 'w') as file :
                json.dump(messages_json, file, indent=4)

            for key, value in json_response.items() :
                if key == "action" : continue
                with open(os.path.join(output_base_dir, f"{key}"), 'w') as file :
                    file.write(value)

            finish_synthesized = True
            break
        
        # Recreate the environment for the next turn. Prefers the fast golden-
        # baseline reset (container reuse, seconds) and transparently falls back
        # to a full container rebuild; captures the (possibly new) container name
        # so subsequent turns (and the caller) track the live container. An
        # observation timeout forces the full rebuild so runaway in-container
        # anomaly-eval/injection processes are reaped instead of racing a DB-only
        # reset (the root cause of "database does not exist" after a timeout).
        docker_name = reinitialize_docker_environment_for_next_turn(
            docker_name,
            base_docker_name,
            container_config,
            init_db_config,
            dbms,
            system_user_config,
            data_modification_path,
            output_sub_dir,
            logger,
            i,
            fast_reset_state=fast_reset_state,
            force_full_rebuild=observation_timed_out
        )

        logger.info(f"[Anomaly Injection Synthesis] -- Parsing LLM Response Finish [Turn {i}] --")

    if not finish_synthesized :
        logger.error(f"[Anomaly Injection Synthesis][ERROR][== SYNTHESIS FAILED ==] Max Turn Number ({MAX_TURN_NUM}) Reached without obtaining a valid `finish` action.")

    # Return the final live container name so the caller cleans up the right one.
    return docker_name

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description='Database Diagnosis for Anomaly Injection Synthesis')
    parser.add_argument('--config', type=str, required=False, default=os.path.join(base_dir, "Configs/syn_ano/config.json"), help='Path to the configuration JSON file')
    args = parser.parse_args()
    config_path = resolve_path_from_base(args.config, base_dir)
    with open(config_path, 'r') as file:
        config = json.load(file)

    # database names
    db_names = config.get("database_names", ['tpch_1', 'imdb', 'tpcds_1'])
    database_schema_dir = resolve_path_from_base(config.get('database_schema_directory', "Data/Schema"), base_dir) # _Database_Diagnosis/Data/Schema

    syn_int_base_directory = resolve_path_from_base(config.get('syn_int_directory', None), base_dir)
    if syn_int_base_directory is None or not os.path.exists(syn_int_base_directory):
        print("[ERROR] Synthetic Intermediate Directory Path is not provided in the config file. EXIT()")
        raise RuntimeError("[MAIN][ERROR] Synthetic Intermediate Directory Path is not provided or does not exist.")
    
    syn_data_base_directory = resolve_path_from_base(config.get('syn_data_directory', None), base_dir)
    if syn_data_base_directory is None or not os.path.exists(syn_data_base_directory):
        print("[ERROR] Synthetic Data Directory Path is not provided in the config file. EXIT()")
        raise RuntimeError("[MAIN][ERROR] Synthetic Data Directory Path is not provided or does not exist.")

    MAX_TURN_NUM = config.get('max_turn_num', 20)
    # anomaly_desp = anomaly_desp[87:]

    # Resolve this after DBMS detection. MySQL logical table copies duplicate
    # large indexed tables and saturate shared storage under parallel workers.
    FAST_RESET_REQUESTED = bool(config.get("fast_reset_enabled", True))

    # Curated domain-knowledge skills for the synthesis prompt (`{SKILLS}`). The
    # directory is configurable but defaults to the repo-local `Skills/`; loading
    # once here keeps every case's prompt consistent. Loaded lazily-safe: a
    # missing folder/file only omits guidance, it never blocks synthesis.
    skills_dir = resolve_path_from_base(config.get('skills_directory', "Skills"), base_dir)

    # LLM OPT
    llm_opt = config.get('llm_configuration', None)
    if llm_opt is None:
        llm_opt = {
            "base_url" : openai.base_url,
            "api_key" : openai.api_key,
            "model_name" : model_name,
            "temperature" : temperature,
            "retry_times" : 10,
        }

    # Docker Configuration
    BASE_DOCKER_NAME = config.get('docker_name', "pg_syn_ano") # pg_syn_ano
    # Containers use UNIQUE timestamped names derived from BASE_DOCKER_NAME (never
    # reused) to eliminate RC 125 name conflicts under heavy parallel churn.
    # BASE_DOCKER_NAME is the stable per-worker prefix used for orphan sweeps.
    DOCKER_NAME = BASE_DOCKER_NAME
    output_root_dir = resolve_path_from_base(config.get('output_base_dir', "Output"), base_dir)
    output_base_dir = obtain_output_dir(os.path.abspath(__file__), output_root_dir)
    if not os.path.exists(output_base_dir): os.makedirs(output_base_dir)
    with open(os.path.join(output_base_dir, "config.json"), 'w') as file :
        json.dump(config, file, indent=4)

    # Docker Configuration
    DBMS = config.get('dbms', "PostgreSQL 16.11") # PostgreSQL 16.11
    DBMS_KIND = normalize_dbms(DBMS)
    DB_TYPE = resolve_db_type(config, DBMS)
    FAST_RESET_ENABLED = resolve_fast_reset_enabled(DBMS_KIND, config)

    SYSTEM_HARDWARE_CONFIG = config.get('system_hardware_config', None)
    if SYSTEM_HARDWARE_CONFIG is None :
        SYSTEM_HARDWARE_CONFIG = {
            "CPU" : 16,
            "Memory" : 16,
            "Memory + Swap" : 32,
            "Storage" : 256
        }
    HOST_PORT = config.get('host_port', 5429)
    for GB_Config in GB_Configurations :
        SYSTEM_HARDWARE_CONFIG[f"{GB_Config}"] = f"{SYSTEM_HARDWARE_CONFIG[GB_Config]}GB"
    
    SYSTEM_USER_CONFIG_JSON = config.get('system_user_config', None)
    if SYSTEM_USER_CONFIG_JSON is None :
        if DBMS_KIND == "mysql":
            SYSTEM_USER_CONFIG_JSON = {
                "DB_USER" : "diagnosis",
                "DB_PASSWORD" : "diagnosis",
                "DB_HOST" : "localhost",
                "DB_PORT" : "3306",
                "DB_NAME" : "",
                "DB_DATA_DIR" : "/var/lib/mysql",
                "DB_LOG_DIR" : "/var/log/mysql",
            }
        else:
            SYSTEM_USER_CONFIG_JSON = {
                "DB_USER" : "diagnosis",
                "DB_PASSWORD" : "",
                "DB_HOST" : "localhost",
                "DB_PORT" : "5432",
                "DB_NAME" : "",
                "DB_DATA_DIR" : "/var/lib/postgresql/16/main",
                "DB_LOG_DIR" : "/var/lib/postgresql/16/logs",
            }
    
    container_config = build_container_config(DBMS, SYSTEM_HARDWARE_CONFIG, HOST_PORT, SYSTEM_USER_CONFIG_JSON)
    SYSTEM_HARDWARE_CONFIG = str(SYSTEM_HARDWARE_CONFIG)

    log_path = os.path.join(output_base_dir, "log.txt")
    logger = set_up_logger(log_path)

    logger = logging.getLogger(f"DB_DIAG.{__name__}")
    logger.info("[MAIN][Database Diagnosis for Anomaly Injection Synthesis] == START ==")
    logger.info(f"[MAIN] Synthetic Intermediate Directory: {syn_int_base_directory}, with Database Length: {len(db_names)}")
    if (
        DBMS_KIND == "mysql"
        and FAST_RESET_REQUESTED
        and not FAST_RESET_ENABLED
    ):
        logger.info(
            "[MAIN] MySQL golden-clone fast reset is disabled because logical "
            "table copies are unsafe for large parallel workloads; using full "
            "container rebuilds. Set `mysql_fast_reset_experimental_enabled` "
            "to true only for explicitly controlled small databases."
        )

    # Load curated synthesis skills ONCE (now that the logger exists). Injected
    # into every case's prompt via the template's `{SKILLS}` placeholder so the
    # LLM keeps injections DB-internal and per-turn fast reset stays correct.
    SKILLS = load_synthesis_skills(skills_dir, logger)

    # Reap any orphan containers/sessions left by a previously crashed/killed run
    # of THIS worker before starting (matched by the per-worker unique prefix).
    logger.info(f"[MAIN] Sweeping stale containers/sessions with prefix '{BASE_DOCKER_NAME}' before start ...")
    remove_containers_by_prefix(BASE_DOCKER_NAME)
    kill_tmux_sessions_by_prefix(BASE_DOCKER_NAME)

    
    # Discover synthetic intermediate cases produced by `test_syn_int.py`.
    # `test_syn_data.py` consumes the same listing and renames each retained
    # case to `{enumerate_index}_{DB_NAME}`, so replicating the identical
    # enumeration here lets us pair every `test_syn_int.py` case directory with
    # its corresponding `test_syn_data.py` output directory by index + DB_NAME.
    syn_int_list = sorted(
        [
            f for f in os.listdir(syn_int_base_directory)
            if os.path.isdir(os.path.join(syn_int_base_directory, f))
            and os.path.exists(os.path.join(syn_int_base_directory, f, "anomaly_description.json"))
        ],
        key=lambda f: int(f.split("_", 1)[0])
    )
    # syn_int_list = syn_int_list[:2]
    logger.info(f"[MAIN] Current Synthetic Intermediate Cases Length: {len(syn_int_list)}")

    inference_times = []
    for idx, syn_int_dir_name in tqdm(enumerate(syn_int_list), total=len(syn_int_list), desc="Anomaly Injection Synthesis") :
        parts = syn_int_dir_name.split("_", 1)
        DB_NAME = parts[1] if len(parts) == 2 else ""
        if DB_NAME not in db_names :
            logger.warning(f"[MAIN][WARNING] Skip invalid synthetic intermediate directory `{syn_int_dir_name}` with database `{DB_NAME}`.")
            continue

        syn_int_directory = os.path.join(syn_int_base_directory, syn_int_dir_name)
        syn_data_directory = os.path.join(syn_data_base_directory, f"{idx}_{DB_NAME}")

        anomaly_desp_case_path = os.path.join(syn_int_directory, "anomaly_description.json")
        with open(anomaly_desp_case_path, "r") as f :
            anomaly_desp = json.load(f)

        logger.info(f"[MAIN][Database Diagnosis for Anomaly Injection Synthesis] Processing Case {idx} / {len(syn_int_list)} : {anomaly_desp['Name']} for Database `{DB_NAME}`")

        DB_SCHEMA_NAME = config.get('database_schema_name', "diagnosis")
        if DB_NAME in database_schema_mapping.keys() : DB_SCHEMA_NAME = database_schema_mapping[DB_NAME]
        if DBMS_KIND == "mysql": DB_SCHEMA_NAME = DB_NAME
        PROMPT_DB_SCHEMA_NAME = MYSQL_SCHEMA_PROMPT_VALUE if DBMS_KIND == "mysql" else DB_SCHEMA_NAME

        output_dir = os.path.join(output_base_dir, f"{idx}_{DB_NAME}")
        os.makedirs(output_dir, exist_ok=True)

        shutil.copy(anomaly_desp_case_path, os.path.join(output_dir, "anomaly_description.json"))

        required_input_paths = [
            os.path.join(syn_int_directory, "initialization_workload.sql"),
            os.path.join(syn_int_directory, "verified_workload.sql"),
            os.path.join(syn_int_directory, "user_phenomenon_description.txt"),
            os.path.join(syn_data_directory, "data_feature_summarization.txt")
        ]
        missing_input_paths = [path for path in required_input_paths if not os.path.exists(path)]
        if missing_input_paths :
            logger.error(f"[MAIN][ERROR] Skip case {idx} for database `{DB_NAME}` because required input files are missing: {missing_input_paths}")
            continue

        # Docker Container Initialization
        # Fresh UNIQUE name per sample; on retry, move to a new name after reaping
        # the failed attempt so a slow teardown cannot cause a name conflict.
        initialization_times = 0
        docker_init_status = False
        DOCKER_NAME = make_unique_container_name(BASE_DOCKER_NAME)
        logger.info(f"[MAIN] Initializing Docker Container [{DOCKER_NAME}] ...")
        while initialization_times < RETRY_TIMES :
            docker_init_status = docker_container_init(DOCKER_NAME, container_config)
            if docker_init_status is True :
                logger.info(f"[MAIN] Docker Container ``{DOCKER_NAME}`` Initialized Successfully.")
                break

            logger.error(f"[MAIN][ERROR] Error in Initialize Docker Container ``{DOCKER_NAME}''. Retrying ({initialization_times}/{RETRY_TIMES})...")
            initialization_times += 1
            time.sleep(5)
            docker_init_status, DOCKER_NAME = recreate_container_with_new_name(DOCKER_NAME, BASE_DOCKER_NAME, container_config)
            if docker_init_status is True :
                logger.info(f"[MAIN] Docker Container ``{DOCKER_NAME}`` Initialized Successfully.")
                break

        if docker_init_status is False :
            logger.error(f"[MAIN][ERROR] Error in Initialize Docker Container ``{DOCKER_NAME}''. EXIT()")
            raise RuntimeError(f"[MAIN][ERROR] Docker Container `{DOCKER_NAME}` initialization failed after {RETRY_TIMES} retries.")

        # Initialize Database in Docker Container
        logger.info(f"[MAIN] Initializing Database in Docker Container...")
        tmp_dbms_name = DBMS.replace(" ", "_")
        tmp_directory = initialize_shared_tmp_directory(base_dir, tmp_dbms_name)
        init_db_config = {
            "dbms" : DBMS,
            "base_dir" : base_dir,
            "db_host" : SYSTEM_USER_CONFIG_JSON['DB_HOST'],
            "db_port" : SYSTEM_USER_CONFIG_JSON['DB_PORT'],
            "db_name" : DB_NAME,
            "db_type" : DB_TYPE,
            "db_schema" : DB_SCHEMA_NAME,
            "db_user" : SYSTEM_USER_CONFIG_JSON['DB_USER'],
            "password" : SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
            "timeout" : TIMEOUTS.get(DB_NAME, 3600),
            "output_dir" : output_dir,
            "tmp_directory" : tmp_directory
        }
        initialization_times = 0
        docker_status = False
        while initialization_times < RETRY_TIMES :
            docker_status = docker_container_init_db(DOCKER_NAME, init_db_config, session_name=DOCKER_NAME)
            if docker_status is True :
                logger.info(f"[MAIN] Database `{DB_NAME}` Initialization in Docker Container `{DOCKER_NAME}` Succeeded.")
                break
            else :
                logger.error(f"[MAIN][ERROR] Database `{DB_NAME}` Initialization in Docker Container `{DOCKER_NAME}` Failed. Retrying ({initialization_times}/{RETRY_TIMES})...")
                initialization_times += 1
                time.sleep(5)
                # Recreate under a NEW unique name before the next DB-init retry.
                _, DOCKER_NAME = recreate_container_with_new_name(DOCKER_NAME, BASE_DOCKER_NAME, container_config)

        if docker_status is False :
            logger.error(f"[MAIN][ERROR] Database `{DB_NAME}` Initialization in Docker Container `{DOCKER_NAME}` Failed.")
            raise RuntimeError(f"[MAIN][ERROR] Database `{DB_NAME}` initialization in Docker Container `{DOCKER_NAME}` failed after {RETRY_TIMES} retries.")

        # Prepare the additional information for anomaly synthesis
        logger.info(f"[MAIN] Preparing Additional Information for Anomaly Synthesis...")
        syn_data_path = required_input_paths[3]
        with open(syn_data_path, "r") as f :
            data_feature_summarization = f.readlines()
        DB_MODIFICATION = ''.join(data_feature_summarization).strip()
        rewrite_syn_data_path = os.path.join(output_dir, "data_feature_summarization.txt")
        with open(rewrite_syn_data_path, "w") as f :
            f.write(DB_MODIFICATION)

        initialization_workload_path = required_input_paths[0]
        verified_workload_path = required_input_paths[1]
        INITIALIZATION_WORKLOAD = extract_sql_from_file(initialization_workload_path)
        VERIFIED_WORKLOAD = extract_sql_from_file(verified_workload_path)
        MERGED_WORKLOAD = INITIALIZATION_WORKLOAD + "\n\n" + VERIFIED_WORKLOAD
        verified_workload_path = os.path.join(output_dir, "verified_workload.sql")
        with open(verified_workload_path, "w") as f :
            f.write(VERIFIED_WORKLOAD)
        initialization_workload_path = os.path.join(output_dir, "initialization_workload.sql")
        with open(initialization_workload_path, "w") as f :
            f.write(INITIALIZATION_WORKLOAD)
        merged_workload_path = os.path.join(output_dir, "merged_workload.sql")
        with open(merged_workload_path, "w") as f :
            f.write(MERGED_WORKLOAD)

        user_phenomenon_path = required_input_paths[2]
        with open(user_phenomenon_path, "r") as f :
            user_phenomenon_description = f.readlines()
            user_phenomenon_description = [line for line in user_phenomenon_description if line.strip() != ""]
        PHENOMENON_DESCRIPTION = ''.join(user_phenomenon_description).strip()
        rewrite_user_phenomenon_path = os.path.join(output_dir, "user_phenomenon_description.txt")
        with open(rewrite_user_phenomenon_path, "w") as f :
            f.write(PHENOMENON_DESCRIPTION)

        data_modification_flag = False
        data_modification_original_path = os.path.join(syn_data_directory, "data_modification.sql")
        rewrite_data_modification_path = os.path.join(output_dir, "data_modification.sql")
        with open(rewrite_data_modification_path, "w") as f :
            f.write("")
        if os.path.exists(data_modification_original_path) :
            logging.info(f"[MAIN] Data Modification SQL File Found for the Current Anomaly. Preparing Data Modification SQL File...")
            data_modification_sqls = extract_sql_from_file(data_modification_original_path)
            with open(rewrite_data_modification_path, "w") as f :
                f.write(data_modification_sqls)
            data_modification_flag = True

        # Data Modification If Needed
        data_modification_path = rewrite_data_modification_path
        run_data_modification_step(
            DOCKER_NAME,
            DBMS,
            SYSTEM_USER_CONFIG_JSON,
            data_modification_path,
            DB_NAME,
            output_dir,
            logger
        )

        # Obtain Database Schema Information
        db_config = {
            'host' : SYSTEM_USER_CONFIG_JSON['DB_HOST'],
            'port' : SYSTEM_USER_CONFIG_JSON['DB_PORT'],
            'host_port' : HOST_PORT,
            'user' : SYSTEM_USER_CONFIG_JSON['DB_USER'],
            'password' : SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
            'db_name' : DB_NAME,
            'schema' : DB_SCHEMA_NAME,
            'dbms' : DBMS
        }
        database_schema_path = os.path.join(database_schema_dir, f"{DB_NAME}_schema.json")
        if not data_modification_flag :
            logger.info(f"[MAIN] Data Modification Not Needed for Current Anomaly.")
            if os.path.exists(database_schema_path) :
                with open(database_schema_path, 'r') as file :
                    database_schema = json.load(file)
            else :
                database_schema = obtain_database_schema(db_config, database_schema_path, DOCKER_NAME)
        else :
            schema_time_str = current_time_str()
            logger.info(f"[MAIN] Data Modification Needed for Current Anomaly. Obtaining Database Schema Information after Data Modification with Timing...")
            database_schema_path = os.path.join(database_schema_dir, schema_time_str, f"{DB_NAME}_schema.json")
            database_schema = obtain_database_schema(db_config, database_schema_path, DOCKER_NAME)
        

        DB_SCHEMA = ""
        DB_ROWS = ""
        for table_info in database_schema['table_info'] :
            DB_ROWS += f"- Table {table_info['table']} with {table_info['rows']} rows.\n"
            DB_SCHEMA += table_info["ddl"] + "\n\n"

        # Record Merged Workload Performance [Execution Time]
        logger.info("[MAIN] Recording Merged Workload (Initialization + Verified Workload) Performance on Original Database...")
        merged_workload_results = execute_sql_file_with_timing(merged_workload_path, db_config)
        with open(os.path.join(output_dir, "merged_workload_performance.json"), 'w') as file :
            json.dump(merged_workload_results, file, indent=4)

        # 仅累加执行成功的 SQL 耗时: execute_sql_file_with_timing 对失败的 SQL
        # 会写入哨兵值 execution_time=-1, 不能计入总时间, 否则总耗时会变负。
        # 再用 max(0.0, ...) 对单条耗时兜底, 确保累加结果始终非负。
        merged_workload_total_exec_time = sum(
            max(0.0, result['execution_time'])
            for result in merged_workload_results
            if result.get('success') and isinstance(result.get('execution_time'), (int, float))
        )
        logger.info(f"[MAIN] Merged Workload Total Execution Time on Original Database : {merged_workload_total_exec_time} seconds.")

        # LLM INPUT INFORMATION
        ANOMALY_INFO = ""
        anomaly_info_dict = anomaly_desp
        if anomaly_info_dict['Type'] == "BOTH": anomaly_info_dict['Type'] = "OLAP + OLTP"
        for key, value in anomaly_info_dict.items() :
            ANOMALY_INFO += f"- {key} : {value}\n"

        SYSTEM_USER_CONFIG_JSON['DB_NAME'] = DB_NAME
        SYSTEM_USER_CONFIG_JSON["SCRIPT_PATH"] = "SCRIPT_PATH_PLACEHOLDER"

        FAKE_SYSTEM_USER_CONFIG_JSON = SYSTEM_USER_CONFIG_JSON.copy()
        FAKE_SYSTEM_USER_CONFIG_JSON['DB_HOST'] = FAKE_DATABASE_IP
        input_information = synthesize_anomaly_prompt_template.format(
            MAX_TURN_NUM=MAX_TURN_NUM,
            SYSTEM_HARDWARE_CONFIG=SYSTEM_HARDWARE_CONFIG,
            SYSTEM_USER_CONFIG=FAKE_SYSTEM_USER_CONFIG_JSON,
            DBMS=DBMS,
            DB_NAME=DB_NAME,
            DB_SCHEMA_NAME=PROMPT_DB_SCHEMA_NAME,
            DB_SCHEMA=DB_SCHEMA,
            DB_ROWS=DB_ROWS,
            DB_MODIFICATION=DB_MODIFICATION,
            ANOMALY_INFO=ANOMALY_INFO,
            VERIFIED_WORKLOAD=MERGED_WORKLOAD,
            PHENOMENON_DESCRIPTION=PHENOMENON_DESCRIPTION,
            SKILLS=SKILLS,
        )

        system_opt = {
            'docker_name' : DOCKER_NAME,
            'base_docker_name' : BASE_DOCKER_NAME,
            'container_config' : container_config,
            'system_user_config' : SYSTEM_USER_CONFIG_JSON,
            'dbms' : DBMS,
            'data_modification_path' : data_modification_path,
            'init_db_config' : init_db_config,
            'docker_main_dir' : base_dir,
            'output_mes_path' : os.path.join(output_dir, f"llm_messages.json"),
            'output_base_dir' : output_dir,
            # Per-sample fast-reset state. `enabled` from config; `snapshot_ready`
            # starts False and is flipped True once the golden baseline is built
            # inside the live container at the start of llm_infer_multi.
            'fast_reset_state' : {'enabled' : FAST_RESET_ENABLED, 'snapshot_ready' : False},
        }
        anomaly_eval_opt = {
            "anomaly_eval_path" : os.path.join(base_dir, 'Scripts', 'anomaly_eval.py'),
            "initialization_workload_path" : initialization_workload_path,
            "verified_workload_path" : verified_workload_path,
            "merged_workload_execution_time" : merged_workload_total_exec_time,
            "host" : SYSTEM_USER_CONFIG_JSON['DB_HOST'],
            "port" : SYSTEM_USER_CONFIG_JSON['DB_PORT'],
            "user" : SYSTEM_USER_CONFIG_JSON['DB_USER'],
            "password" : SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
            "database" : SYSTEM_USER_CONFIG_JSON['DB_NAME'],
            "schema" : DB_SCHEMA_NAME,
            "dbms" : DBMS,
        }
        config_opt = {
            "system_opt" : system_opt,
            "llm_opt" : llm_opt,
            "anomaly_eval_opt" : anomaly_eval_opt,
            "max_turn_num" : MAX_TURN_NUM,
        }

        # LLM Inference
        start_time = time.time() 
        # llm_infer_multi may recreate the container under a new unique name across
        # turns; capture the final live name so cleanup targets the right container.
        DOCKER_NAME = llm_infer_multi(input_information, config_opt, logger)
        endtime = time.time()
        
        initialization_times = 0
        while initialization_times < RETRY_TIMES :
            docker_del_status = docker_container_del(DOCKER_NAME)
            if docker_del_status is True : break
            else :
                logger.error(f"[MAIN][ERROR] Error in Delete Docker Container ``{DOCKER_NAME}''. Retrying ({initialization_times}/{RETRY_TIMES})...")
                initialization_times += 1
                time.sleep(5)

        logger.info(f"[MAIN] Docker Container ``{DOCKER_NAME}`` Deleted Successfully.")
        kill_tmux_session(DOCKER_NAME)

        inference_times.append(endtime - start_time)

        # if idx==1 : exit()

    # Final defensive sweep: reap any container/session left behind by this
    # worker (e.g. a mid-run recreation whose old name lingered) using the stable
    # per-worker prefix before reporting completion.
    remove_containers_by_prefix(BASE_DOCKER_NAME)
    kill_tmux_sessions_by_prefix(BASE_DOCKER_NAME)

    logger.info("[MAIN] LLM Inference Completed, with inference times : {}".format(str(inference_times)))
