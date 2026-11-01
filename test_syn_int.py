import openai
import json
import os
import time
from tqdm import tqdm
import argparse
import random

from prompt_template import *
from utils import *
from llm_api import *
from docker_func import *

# python test_syn_int.py --config ./Configs/syn_int/example.json

random.seed(42) # 42, 4, 2, 21, 36

llm_provider = "azure"
openai.api_key = os.environ.get("OPENAI_API_KEY", "")
openai.base_url = default_llm_base_url("Configs/syn_int/example.json")
openai.api_version = "2024-03-01-preview"
model_name = "o3-2025-04-16"
temperature = 1

GB_Configurations = ["Memory", "Memory + Swap", "Storage"]

LOG_NAME = os.path.basename(__file__).split('.')[0]

olap_databases = ['tpch_1', 'tpch_10', 'tpcds_1', 'imdb', 'sqlbrain_ap']
oltp_databases = ['benchbase', 'sqlbrain_tp']

TIMEOUTS = {'tpch_1' : 1560, 'imdb' : 4560, 'tpcds_1' : 1860, 'tpch_10' : 11800, 'sqlbrain_ap' : 36000, 'sqlbrain_tp' : 14400}
TIMEOUTS = {'tpch_1' : 240, 'imdb' : 480, 'tpcds_1' : 360, 'tpch_10' : 420, 'sqlbrain_ap' : 1800, 'sqlbrain_tp' : 780}  
TIMEOUTS = {'tpch_1' : 1500, 'imdb' : 4500, 'tpcds_1' : 1800, 'tpch_10' : 11800, 'sqlbrain_ap' : 36000, 'sqlbrain_tp' : 14400}
TIMEOUTS = {'tpch_1' : 3600, 'imdb' : 9000, 'tpcds_1' : 4500, 'tpch_10' : 11800, 'sqlbrain_ap' : 36000, 'sqlbrain_tp' : 14400}
RETRY_TIMES = 10

database_schema_mapping = {
    "sqlbrain_ap" : "bytebrain_data_source",
}


def llm_infer_int(input_information, llm_opt, max_iter_num, db_config, logger, retry_dir, timeout=600) : # rtn error_state, messages, final_response   
    messages = [input_information]
    
    for iter in range(max_iter_num) :
        iter_dir = os.path.join(retry_dir, f"{iter}_iteration")
        os.makedirs(iter_dir, exist_ok=True)

        logger.info(f"[USER INTENTION SYNTHESIS] LLM Inference Iteration {iter + 1} / {max_iter_num} ...")
        if iter + 1 == max_iter_num :
            logger.warning(f"[USER INTENTION SYNTHESIS][WARNING] Max Iteration Number Reached.")

        cnt = 0
        json_response = ""
        matched_mess = False

        llm_messages = ""
        while cnt < llm_opt['retry_times'] : #
            response = get_llm_response(messages, llm_opt)

            pure_response = response.strip()
            llm_messages += "======= LLM Inference {} =======\n" .format(cnt) + pure_response + "\n=======================================\n\n"
            with open(os.path.join(iter_dir, f"llm_response.txt"), 'w') as f :
                f.write(llm_messages)

            try:
                json_str = extract_json_object(pure_response)
                json_response = json.loads(json_str)
            except (ValueError, json.JSONDecodeError) as e:
                logger.warning(f"[USER INTENTION SYNTHESIS][WARNING] Failed to extract valid JSON from action block: {e} --> Retrying ({cnt}/{llm_opt['retry_times']})...")
                cnt += 1
                continue

            if json_response.keys() == {'action', 'initialization_workload.sql', 'verified_workload.sql', 'user_phenomenon_description.txt'} and json_response['action'] in ['generate', 'finish'] :
                matched_mess = True
                break
            else : 
                logger.warning(f"[USER INTENTION SYNTHESIS][WARNING] Unrecognized response JSON format from LLM --> Retrying ({cnt}/{llm_opt['retry_times']})...")
            
            cnt += 1
        messages.append(response)
        
        if matched_mess is False : 
            logger.warning("[USER INTENTION SYNTHESIS][WARNING] Max Retry Times Reached for LLM Inference in this Iteration without obtaining a valid response JSON --> Terminating with Current Response.")
            return True, messages, json_response

        # Verified Workload Time Threshold [Initialization -> Verified Workload Execution]
        logger.info(f"[USER INTENTION SYNTHESIS] LLM Inference Succeeded in Iteration {iter + 1} -> Collecting User Observation ...")

        pre_workload = json_response['initialization_workload.sql']
        error_state, initialization_results = check_workload_timeout(pre_workload, db_config, timeout_seconds=timeout)
        with open(os.path.join(iter_dir, f"initialization_results.json"), 'w') as f :
            json.dump(initialization_results, f, indent=4)
        if error_state :
            logger.warning(f"[USER INTENTION SYNTHESIS] LLM Inference in Iteration {iter + 1} Failed as Initialization Workload Exceeds Time Threshold / Occurs Execution Error --> Refining...")
            messages.append(f"Initialization workload execution results:\n Exceeds Time Threshold / Occurs Execution Error.\n\nYou should refine the intention synthesis based on the execution results and previous messages, and then output the `generate` action for observation.")
            continue

        verified_workload = json_response['verified_workload.sql']
        error_state, execution_results = check_workload_timeout(verified_workload, db_config, timeout_seconds=timeout)

        with open(os.path.join(iter_dir, f"verified_workload_results.json"), 'w') as f :
            json.dump(execution_results, f, indent=4)
        
        if json_response['action'] == 'finish' and not error_state :
            logger.info(f"[USER INTENTION SYNTHESIS] LLM Inference Finished in Iteration {iter + 1} with Action `finish` [with Total Execution Time : {execution_results['total_execution_time']:.4f}s].")
            return error_state, messages, json_response
        else :
            if not error_state :
                logger.info(f"[USER INTENTION SYNTHESIS] LLM Inference Succeeded in Iteration {iter + 1} with Verified Workload within Time Threshold [Total Execution Time : {execution_results['total_execution_time']:.4f}s].")
                messages.append(f"Verified workload execution results:\n {format_observation_for_llm(execution_results)}\n\nLLM Inference Succeeded with Verified Workload within Time Threshold.")
            else :
                logger.warning(f"[USER INTENTION SYNTHESIS] LLM Inference in Iteration {iter + 1} Failed as Verified Workload Exceeds Time Threshold / Occurs Execution Error --> Refining...")
                messages.append(f"Verified workload execution results:\n {format_observation_for_llm(execution_results)}\n\nYou should refine the intention synthesis based on the execution results and previous messages, and then output the `generate` action for observation.")

    return True, messages, json_response

      
if __name__ == "__main__" :
    base_dir = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description="Database Diagnosis for USER INTENTION SYNTHESIS")
    parser.add_argument("--config", type=str, required=False, default=os.path.join(base_dir, "Configs/syn_int/example.json"), help="Path to the Configuration JSON File")
    args = parser.parse_args()
    config_path = resolve_path_from_base(args.config, base_dir)

    with open(config_path, 'r') as file :
        config = json.load(file)

    # database names
    db_names = config.get("database_names", ['tpch_1', 'imdb', 'tpcds_1', 'benchbase']) # ['sqlbrain_ap', 'sqlbrain_tp']
    database_schema_dir = resolve_path_from_base(config.get('database_schema_directory', "Data/Schema"), base_dir) # _Database_Diagnosis/Data/Schema

    # anomaly description: RAG-expanded anomaly definitions produced by test_retri_top_n.py
    anomaly_desp_path = resolve_path_from_base(config.get('anomaly_description_path', "Anomaly_Def/anomaly_def_rag/0.json"), base_dir)
    with open(anomaly_desp_path, "r") as f:
        anomaly_desp = json.load(f)

    # LLM OPT
    LLM_OPT = config.get('llm_configuration', None)
    if LLM_OPT is None:
        LLM_OPT = {
            "provider" : llm_provider,
            "base_url" : openai.base_url,
            "api_version" : openai.api_version,
            "api_key" : openai.api_key,
            "model_name" : model_name,
            "temperature" : temperature,
            "retry_times" : 10,
        }
    MAX_TURN_NUM = config.get('max_turn_num', 20)

    # System Configuration
    output_root_dir = resolve_path_from_base(config.get('output_base_dir', "Output"), base_dir)
    output_base_dir = obtain_output_dir(os.path.abspath(__file__), output_root_dir)
    if not os.path.exists(output_base_dir): os.makedirs(output_base_dir)
    with open(os.path.join(output_base_dir, "config.json"), 'w') as file :
        json.dump(config, file, indent=4)

    DBMS = config.get('dbms', "PostgreSQL 16.11") # PostgreSQL 16.11
    DBMS_KIND = normalize_dbms(DBMS)
    DB_SCHEMA_NAME = config.get('database_schema_name', "diagnosis")
    # if DB_NAME in database_schema_mapping.keys() : DB_SCHEMA_NAME = database_schema_mapping[DB_NAME]
    HOST_PORT = config.get('host_port', 5431)
    BASE_DOCKER_NAME = config.get('docker_name', "pg_syn_int")
    # Each container is created under a UNIQUE timestamped name derived from
    # BASE_DOCKER_NAME (see make_unique_container_name). Names are never reused, so
    # a slow daemon-side teardown of a previous container can no longer collide
    # with the next `docker run` (root cause of the intermittent RC 125 name
    # conflicts under heavy parallel load). DOCKER_NAME below is (re)generated per
    # sample; BASE_DOCKER_NAME stays the stable prefix used for orphan sweeps.
    DOCKER_NAME = BASE_DOCKER_NAME
    
    SYSTEM_HARDWARE_CONFIG = config.get('system_hardware_config', None)
    if SYSTEM_HARDWARE_CONFIG is None :
        SYSTEM_HARDWARE_CONFIG = {
            "CPU" : 16,
            "Memory" : 16,
            "Memory + Swap" : 32,
            "Storage" : 256
        }
    container_config = [SYSTEM_HARDWARE_CONFIG["CPU"], SYSTEM_HARDWARE_CONFIG["Memory"], SYSTEM_HARDWARE_CONFIG["Memory + Swap"], SYSTEM_HARDWARE_CONFIG["Storage"], HOST_PORT]
    for GB_Config in GB_Configurations :
        SYSTEM_HARDWARE_CONFIG[f"{GB_Config}"] = f"{SYSTEM_HARDWARE_CONFIG[GB_Config]}GB"
    
    SYSTEM_USER_CONFIG_JSON = config.get('system_user_configuration', None)
    if SYSTEM_USER_CONFIG_JSON is None :
        if DBMS_KIND == "mysql":
            SYSTEM_USER_CONFIG_JSON = {
                "DB_USER": "diagnosis",
                "DB_PASSWORD": "diagnosis",
                "DB_HOST": "localhost",
                "DB_PORT": "3306",
                "DB_NAME": "",
                "DB_DATA_DIR" : "/var/lib/mysql",
                "DB_LOG_DIR" : "/var/log/mysql",
            }
        else:
            SYSTEM_USER_CONFIG_JSON = {
                "DB_USER": "diagnosis",
                "DB_PASSWORD": "diagnosis",
                "DB_HOST": "localhost",
                "DB_PORT": "5432",
                "DB_NAME": "",
                "DB_DATA_DIR" : "/var/lib/postgresql/16/main",
                "DB_LOG_DIR" : "/var/lib/postgresql/16/logs",
            }

    # Docker Configuration    
    container_config = build_container_config(DBMS, SYSTEM_HARDWARE_CONFIG, HOST_PORT, SYSTEM_USER_CONFIG_JSON)

    SYSTEM_HARDWARE_CONFIG = str(SYSTEM_HARDWARE_CONFIG)

    log_path = os.path.join(output_base_dir, "log.txt")
    logger = set_up_logger(log_path)

    logger = logging.getLogger(f"DB_DIAG.{__name__}")
    logger.info("[MAIN][Database Diagnosis for USER INTENTION SYNTHESIS] == START ==")
    logger.info(f"[MAIN] Current Anomaly Descriptions Length: {len(anomaly_desp)}, with Database Names: {db_names}")

    # Reap any orphan containers/sessions left by a previously crashed/killed run
    # of THIS worker before starting. The base name carries a unique numeric prefix
    # per worker, so this only ever touches our own resources.
    logger.info(f"[MAIN] Sweeping stale containers/sessions with prefix '{BASE_DOCKER_NAME}' before start ...")
    remove_containers_by_prefix(BASE_DOCKER_NAME)
    kill_tmux_sessions_by_prefix(BASE_DOCKER_NAME)

    i = 0
    for _, anomaly_dict in tqdm(enumerate(anomaly_desp), total=len(anomaly_desp), desc=f"USER INTENTION SYNTHESIS for Anomalies") :
        # Each record bundles the anomaly definition with its retrieved RAG content
        # (produced offline by test_retri.py -> test_retri_top_n.py).
        anomaly = anomaly_dict['anomaly_def']
        user_phenomenon_suggestion_item = anomaly_dict['rag_content']

        identify_id = "error_code" if "error_code" in user_phenomenon_suggestion_item['entry'] else "timeout_id"

        DB_NAME = random.choice(db_names)
        DB_TYPE = resolve_db_type(config, DBMS)
        CURRENT_DB_SCHEMA_NAME = DB_NAME if DBMS_KIND == "mysql" else DB_SCHEMA_NAME
        PROMPT_DB_SCHEMA_NAME = MYSQL_SCHEMA_PROMPT_VALUE if DBMS_KIND == "mysql" else CURRENT_DB_SCHEMA_NAME

        logger.info(f"[MAIN] Starting Inference for Anomaly {i} : {DB_NAME}, {anomaly['Name']}, {user_phenomenon_suggestion_item['entry'].get('error_code', user_phenomenon_suggestion_item['entry'].get('timeout_id', ''))} ...")

        output_base_directory = os.path.join(output_base_dir, f"{i}_{DB_NAME}")
        if not os.path.exists(output_base_directory):
            os.makedirs(output_base_directory)
        with open(os.path.join(output_base_directory, "anomaly_description.json"), 'w') as file :
            json.dump(anomaly, file, indent=4)

        # Docker Container Initialization
        # Generate a fresh UNIQUE container name for this sample and, on every
        # retry, move to a brand new name so a slow teardown of the previous
        # attempt cannot cause a name conflict. Reap the previous attempt's
        # container + tmux session before recreating so no orphan accumulates.
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
            time.sleep(30)
            # Tear down the failed attempt and recreate under a NEW unique name.
            docker_init_status, DOCKER_NAME = recreate_container_with_new_name(DOCKER_NAME, BASE_DOCKER_NAME, container_config)
            if docker_init_status is True :
                logger.info(f"[MAIN] Docker Container ``{DOCKER_NAME}`` Initialized Successfully.")
                break

        if docker_init_status is False :
            logger.error(f"[MAIN][ERROR] Error in Initialize Docker Container ``{DOCKER_NAME}''. EXIT()")
            raise RuntimeError(f"[MAIN][ERROR] Docker Container `{DOCKER_NAME}` initialization failed after {RETRY_TIMES} retries.")

        # Initialize Database in Docker Container
        logger.info(f"[MAIN] Initializing Database `{DB_NAME}` in Docker Container...")
        tmp_dbms_name = DBMS.replace(" ", "_")
        tmp_directory = initialize_shared_tmp_directory(base_dir, tmp_dbms_name)
        init_db_config = {
            "dbms" : DBMS,
            "base_dir" : base_dir,
            "db_host" : SYSTEM_USER_CONFIG_JSON['DB_HOST'],
            "db_port" : SYSTEM_USER_CONFIG_JSON['DB_PORT'],
            "db_name" : DB_NAME,
            "db_type" : DB_TYPE,
            "db_schema" : CURRENT_DB_SCHEMA_NAME,
            "db_user" : SYSTEM_USER_CONFIG_JSON['DB_USER'],
            "password" : SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
            "timeout" : TIMEOUTS.get(DB_NAME, 3600),
            "output_dir" : output_base_directory,
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
                time.sleep(420)
                # Recreate under a NEW unique name before the next DB-init retry.
                _, DOCKER_NAME = recreate_container_with_new_name(DOCKER_NAME, BASE_DOCKER_NAME, container_config)

        if docker_status is False :
            logger.error(f"[MAIN][ERROR] Database `{DB_NAME}` Initialization in Docker Container `{DOCKER_NAME}` Failed.")
            raise RuntimeError(f"[MAIN][ERROR] Database `{DB_NAME}` initialization in Docker Container `{DOCKER_NAME}` failed after {RETRY_TIMES} retries.")

        db_config = {
            'host' : SYSTEM_USER_CONFIG_JSON['DB_HOST'],
            'port' : SYSTEM_USER_CONFIG_JSON['DB_PORT'],
            'host_port' : HOST_PORT,
            'user' : SYSTEM_USER_CONFIG_JSON['DB_USER'],
            'password' : SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
            'db_name' : DB_NAME,
            'schema' : CURRENT_DB_SCHEMA_NAME,
            'dbms' : DBMS
        }

        database_schema_path = os.path.join(database_schema_dir, f"{DB_NAME}_schema.json")
        if not os.path.exists(database_schema_path) :
            logger.error(f"[MAIN][ERROR] Database Schema File does not exist for database ``{DB_NAME}``.")
            database_schema = obtain_database_schema(db_config, database_schema_path, DOCKER_NAME)
        with open(database_schema_path, "r") as f:
            database_schema = json.load(f)
        
        DB_SCHEMA = ""
        DB_ROWS = ""
        for table_info in database_schema['table_info'] :
            DB_ROWS += f"- Table {table_info['table']} with {table_info['rows']} rows.\n"
            DB_SCHEMA += table_info["ddl"] + "\n\n"

        ANOMALY_INFO = ""
        anomaly_info_dict = anomaly
        if anomaly_info_dict['Type'] == "BOTH": anomaly_info_dict['Type'] = "OLAP + OLTP"
        for key, value in anomaly_info_dict.items() :
            ANOMALY_INFO += f"- {key} : {value}\n"
        
        # Build the suggestion from the user's perspective: lead with the
        # symptoms a user actually observes, then surface the message text,
        # and keep the internal error_code / timeout_id only as a mechanism
        # reference so the LLM does not mistake it for the visible phenomenon.
        phenomenon_entry = user_phenomenon_suggestion_item['entry']
        official_id = 'official_description' if identify_id == "error_code" else 'official_definition'

        observable_symptoms = phenomenon_entry.get('observable_symptoms', [])
        if isinstance(observable_symptoms, (list, tuple, set)) :
            observable_symptoms_text = "; ".join(str(symptom).strip() for symptom in observable_symptoms if str(symptom).strip())
        else :
            observable_symptoms_text = str(observable_symptoms).strip()

        internal_reference = f"{phenomenon_entry.get(official_id, '')} (internal {identify_id}: {phenomenon_entry.get(identify_id, '')})"

        suggestion_lines = []
        if observable_symptoms_text :
            suggestion_lines.append(f"What the user observes: {observable_symptoms_text}.")
        suggestion_lines.append(f"Underlying mechanism (not directly visible to the user): {internal_reference}.")
        suggestion_lines.append(phenomenon_entry.get('cause_analysis', ''))
        USER_PHENOMENON_SUGGESTION = "\n".join(line for line in suggestion_lines if line)

        input_information = synthesize_intention_prompt_template.format(
            MAX_TURN_NUM = MAX_TURN_NUM,
            SYSTEM_HARDWARE_CONFIG = SYSTEM_HARDWARE_CONFIG,
            DBMS = DBMS,
            DB_NAME=DB_NAME,
            DB_SCHEMA_NAME=PROMPT_DB_SCHEMA_NAME,
            DB_SCHEMA=DB_SCHEMA,
            DB_ROWS=DB_ROWS,
            ANOMALY_INFO=ANOMALY_INFO,
            USER_PHENOMENON_SUGGESTION = USER_PHENOMENON_SUGGESTION
        )

        current_retry = 0
        llm_inference_messages_json = {}
        while current_retry < RETRY_TIMES :
            retry_dir = os.path.join(output_base_directory, f"{current_retry}_inference")
            os.makedirs(retry_dir, exist_ok=True)

            error_state, llm_inference_messages, final_response = llm_infer_int(input_information, LLM_OPT, MAX_TURN_NUM, db_config, logger, retry_dir)

            for idx, message in enumerate(llm_inference_messages) :
                llm_inference_messages_json[f"Message_{idx}"] = message
            with open(os.path.join(retry_dir, f"llm_inference.json"), 'w') as file :
                json.dump(llm_inference_messages_json, file, indent=4)
                    
            with open(os.path.join(retry_dir, "final_response.json"), 'w') as file :
                json.dump(final_response, file, indent=4)

            if not error_state :
                logger.info(f"[MAIN] LLM Inference Succeeded for Anomaly {i} / {len(anomaly_desp)} : {anomaly['Name']} with Verified Workload within Time Threshold.")
                break
            else :
                logger.warning(f"[MAIN][WARNING] LLM Inference Failed for Anomaly {i} / {len(anomaly_desp)} : {anomaly['Name']} --> Retrying ({current_retry}/{RETRY_TIMES})...")
                current_retry += 1
        if current_retry == RETRY_TIMES : logger.error(f"[MAIN][ERROR][== SYNTHESIS FAILED ==] LLM Inference Failed for Anomaly {i} / {len(anomaly_desp)} : {anomaly['Name']} after Retrying {RETRY_TIMES} times without obtaining a valid `finish` action.")

        for idx, message in enumerate(llm_inference_messages) :
            llm_inference_messages_json[f"Message_{idx}"] = message
        with open(os.path.join(output_base_directory, "llm_inference.json"), 'w') as file :
            json.dump(llm_inference_messages_json, file, indent=4)
                
        with open(os.path.join(output_base_directory, "final_response.json"), 'w') as file :
            json.dump(final_response, file, indent=4)

        for key, value in final_response.items() :
            if key == 'action' : continue

            with open(os.path.join(output_base_directory, f"{key}"), 'w') as file :
                file.write(value)         
        
        initialization_times = 0
        while initialization_times < RETRY_TIMES :
            docker_del_status = docker_container_del(DOCKER_NAME)
            if docker_del_status is True : break
            else :
                logger.error(f"[MAIN][ERROR] Error in Delete Docker Container ``{DOCKER_NAME}''. Retrying ({initialization_times}/{RETRY_TIMES})...")
                initialization_times += 1
                time.sleep(5)

        # Kill this sample's tmux session so it does not linger across samples.
        kill_tmux_session(DOCKER_NAME)
        logger.info(f"[MAIN] Docker Container ``{DOCKER_NAME}`` Deleted Successfully.")

        i += 1

    # Final defensive sweep: reap any container/session of THIS worker that may
    # have leaked (e.g. a mid-sample crash) so no orphan survives the run.
    remove_containers_by_prefix(BASE_DOCKER_NAME)
    kill_tmux_sessions_by_prefix(BASE_DOCKER_NAME)
    logger.info("[MAIN][Database Diagnosis for USER INTENTION SYNTHESIS] == COMPLETED ==")
