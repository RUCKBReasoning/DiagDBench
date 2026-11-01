import openai
import json
import os
import time
import shutil
from tqdm import tqdm
import argparse
import logging

from prompt_template import *
from utils import *
from llm_api import *
from docker_func import *

# ACTIONS : execute_sql_command [with execute sql file] : psql -U {DB_USER} -d {DB_NAME} -p {DB_PORT} -f {SQL_FILE_PATH}

# python test_syn_data.py --config ./Configs/syn_data/example.json

llm_provider = "azure"
openai.api_key = os.environ.get("OPENAI_API_KEY", "")
openai.base_url = default_llm_base_url("Configs/syn_data/example.json")
openai.api_version = "2024-03-01-preview"
model_name = "gpt-4o-2024-05-13"
model_name = "o3-2025-04-16"
temperature = 1

GB_Configurations = ["Memory", "Memory + Swap", "Storage"]

LOG_NAME = os.path.basename(__file__).split('.')[0]

olap_databases = ['tpch_1', 'tpch_10', 'tpcds_1', 'imdb', 'sqlbrain_ap']
oltp_databases = ['benchbase', 'sqlbrain_tp']

# TIMEOUTS = {'tpch_1' : 1500, 'imdb' : 4500, 'tpcds_1' : 1800, 'tpch_10' : 11800, 'sqlbrain_ap' : 36000, 'sqlbrain_tp' : 14400}
TIMEOUTS = {'tpch_1' : 240, 'imdb' : 480, 'tpcds_1' : 360, 'tpch_10' : 420, 'sqlbrain_ap' : 1800, 'sqlbrain_tp' : 780} 
TIMEOUTS = {'tpch_1' : 1500, 'imdb' : 4500, 'tpcds_1' : 1800, 'tpch_10' : 11800, 'sqlbrain_ap' : 36000, 'sqlbrain_tp' : 14400}
TIMEOUTS = {'tpch_1' : 3600, 'imdb' : 9000, 'tpcds_1' : 4500, 'tpch_10' : 11800, 'sqlbrain_ap' : 36000, 'sqlbrain_tp' : 14400}
# [暂未更新sqlbrain关于schema定义的问题]
RETRY_TIMES = 10
FAKE_DATABASE_IP = "8888.8888.8888.8888"

database_schema_mapping = {
    "default" : "diagnosis",
    "sqlbrain_ap" : "bytebrain_data_source",
}


def llm_infer_multi_turn(prompt, output_opt, llm_opt, logger, docker_name, base_docker_name, container_config, system_user_config, dbms, MAX_TURN_NUM=10):
    output_mes_path = output_opt['output_mes_path']
    output_base_dir = output_opt['output_base_dir']
    temp_sql_queries_path = os.path.join(output_base_dir, "temp_sql_queries.sql")

    messages = [prompt]

    pure_response = ""
    action_content = ""
    finish_synthesized = False
    for i in range(MAX_TURN_NUM) :
        if i == MAX_TURN_NUM - 1 : logger.info(f"[Target Data Synthesis] -- Multi-Turn LLM Response at Turn {i} (MAX TURN NUMBER) --")
        else : logger.info(f"[Target Data Synthesis] -- Multi-Turn LLM Response at Turn {i} --")

        output_sub_sub_dir = os.path.join(output_base_dir, f"{i}_multi_turn")
        if not os.path.exists(output_sub_sub_dir) : os.makedirs(output_sub_sub_dir)

        sql_queries_path = os.path.join(output_sub_sub_dir, f"sql_queries.sql")

        cnt = 0
        reasoning_responses = ""
        json_response = ""
        matched_mess = False
        sql_action_state = False
        finish_action_state = False
        while cnt < llm_opt['retry_times'] : #
            response = get_llm_response(messages, llm_opt)

            reasoning_responses += response + "\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
            with open(os.path.join(output_sub_sub_dir, f"reasoning_responses.txt"), 'w') as file :
                file.write(reasoning_responses)
            
            action_response = response.split("<action>")[-1].split("</action>")[0].strip()
            try:
                json_str_response = extract_json_object(action_response)
                json_response = json.loads(json_str_response)
            except (ValueError, json.JSONDecodeError) as e:
                logger.warning(f"Failed to extract valid JSON from action block: {e} --> Retrying ({cnt}/{llm_opt['retry_times']})...")
                cnt += 1
                continue

            sql_action_state = (json_response.keys() == {'action', 'query'} and json_response['action'] == 'sql')
            finish_action_state = (json_response.keys() == {'action', 'data_modification.sql', 'data_feature_summarization.txt'} and json_response['action'] == 'finish')

            if sql_action_state or finish_action_state :
                matched_mess = True
                break
            else : 
                logger.warning(f"Unrecognized response format from LLM --> Retrying ({cnt}/{llm_opt['retry_times']})...")

            cnt += 1
        
        if matched_mess is False : logger.warning("[Target Data Synthesis][WARNING] LLM does not infer the expected message with in the retried times.")
        messages.append(response)

        logger.info(f"[Target Data Synthesis] -- LLM Inference Finish [Turn {i}] --")

        # Action Parse and Execute
        observation = ""
        if sql_action_state : # Not the last turn     
            sql_script = json_response['query']
            with open(sql_queries_path, 'w') as file :
                file.write(sql_script)
            shutil.copy(sql_queries_path, temp_sql_queries_path)

            cmd = build_sql_file_command(dbms, system_user_config, temp_sql_queries_path)
            obser_log = os.path.join(output_sub_sub_dir, "obser_log.txt")
            state, obser_output = run_docker_exec_observation(docker_name, cmd, obser_log, 600, session_name=docker_name)            

            if state is True : 
                observation = "Script executed successfully, with the output as follows:\n"
            else :
                observation = "Script executed failed, with the output as follows:\n"
                # Recreate under a NEW unique name (never reuse) so the daemon's
                # in-progress teardown of the old container cannot cause a name
                # conflict. docker_name is rebound for all subsequent turns.
                _, docker_name = recreate_container_with_new_name(docker_name, base_docker_name, container_config)

            observation += prune_obser_log(obser_log)
            
            if obser_output != "" : observation += f"\n\n-- COMMENTS :\n{obser_output}"
                
            messages.append(observation)

        elif finish_action_state : # The last turn with finish action
            logger.info("[Target Data Synthesis] - FINISH Reasoning Process -")

            messages_json = {}
            for idx, message in enumerate(messages) :
                messages_json[f"message_{idx}"] = message
            with open(output_mes_path, 'w') as file :
                json.dump(messages_json, file, indent=4)

            for key, value in json_response.items() :
                if key == 'action' : continue
                with open(os.path.join(output_base_dir, f"{key}"), 'w') as file :
                    file.write(value)

            finish_synthesized = True
            break
        
        logger.info(f"[Target Data Synthesis] -- Parsing LLM Response Finish [Turn {i}] --")

    if not finish_synthesized :
        logger.error(f"[Target Data Synthesis][ERROR][== SYNTHESIS FAILED ==] Max Turn Number ({MAX_TURN_NUM}) Reached without obtaining a valid `finish` action.")

    # Return the (possibly rebound) container name so the caller cleans up the
    # container that is actually alive after any mid-run recreation.
    return docker_name

if __name__ == "__main__" :
    base_dir = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description='Database Diagnosis for Target Data Synthesis')
    parser.add_argument('--config', type=str, required=False, default=os.path.join(base_dir, "Configs/syn_data/example.json"), help='Path to the Configuration JSON File')
    args = parser.parse_args()
    config_path = resolve_path_from_base(args.config, base_dir)
    with open(config_path, "r") as file:
        config = json.load(file)

    # database names
    database_names = config.get('database_names', ['tpch_1', 'imdb', 'tpcds_1'])
    database_schema_dir = resolve_path_from_base(config.get('database_schema_directory', "Data/Schema"), base_dir) # _Database_Diagnosis/Data/Schema
    syn_int_base_directory = resolve_path_from_base(config.get('syn_int_directory', "Output/test_syn_int_default"), base_dir)
    MAX_TURN_NUM = config.get('max_turn_num', 20)

    # LLM OPT
    llm_opt = config.get('llm_configuration', None)
    if llm_opt is None:
        llm_opt = {
            "provider" : llm_provider,
            "base_url" : openai.base_url,
            "api_version" : openai.api_version,
            "api_key" : openai.api_key,
            "model_name" : model_name,
            "temperature" : temperature,
            "retry_times" : 10,
        }

    # Docker Configuration
    BASE_DOCKER_NAME = config.get('docker_name', 'pg_syn_data') # "pg_syn_data"
    # Containers are created under UNIQUE timestamped names derived from
    # BASE_DOCKER_NAME (never reused) to avoid RC 125 name conflicts caused by a
    # slow daemon-side teardown racing the next `docker run`. BASE_DOCKER_NAME is
    # the stable per-worker prefix used for orphan sweeps.
    DOCKER_NAME = BASE_DOCKER_NAME
    output_root_dir = resolve_path_from_base(config.get('output_base_dir', "Output"), base_dir)
    output_base_dir = obtain_output_dir(os.path.abspath(__file__), output_root_dir)
    if not os.path.exists(output_base_dir): os.makedirs(output_base_dir)
    with open(os.path.join(output_base_dir, "config.json"), 'w') as file :
        json.dump(config, file, indent=4)
    # output_base_dir = '/home' + obtain_output_dir(os.path.abspath(__file__)).split('/home')[-1]

    DBMS = config.get('dbms', "PostgreSQL 16.11") # PostgreSQL 16.11
    DBMS_KIND = normalize_dbms(DBMS)
    DB_TYPE = resolve_db_type(config, DBMS)
    SYSTEM_HARDWARE_CONFIG = config.get('system_hardware_config', None)
    if SYSTEM_HARDWARE_CONFIG is None :
        SYSTEM_HARDWARE_CONFIG = {
            "CPU" : 16,
            "Memory" : 16,
            "Memory + Swap" : 32,
            "Storage" : 256
        }
    DOCKER_PORT = config.get('docker_port', 5430) # 5431
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
            }
        else:
            SYSTEM_USER_CONFIG_JSON = {
                "DB_USER" : "diagnosis",
                "DB_PASSWORD" : "diagnosis",
                "DB_HOST" : "localhost",
                "DB_PORT" : "5432",
                "DB_NAME" : "",
            }

    container_config = build_container_config(DBMS, SYSTEM_HARDWARE_CONFIG, DOCKER_PORT, SYSTEM_USER_CONFIG_JSON)
    SYSTEM_HARDWARE_CONFIG = str(SYSTEM_HARDWARE_CONFIG)

    log_path = os.path.join(output_base_dir, "log.txt")
    logger = set_up_logger(log_path)

    logger = logging.getLogger(f"DB_DIAG.{__name__}")
    if not os.path.exists(syn_int_base_directory) :
        logger.error("[MAIN][ERROR] Synthetic Intermediate Directory Path is not provided in the config file. EXIT()")
        raise RuntimeError("[MAIN][ERROR] Synthetic Intermediate Directory Path is not provided or does not exist.")
    logger.info("[MAIN][Database Diagnosis for Target Data Synthesis] == START ==")

    # Reap any orphan containers/sessions left by a previously crashed/killed run
    # of THIS worker before starting (matched by the per-worker unique prefix).
    logger.info(f"[MAIN] Sweeping stale containers/sessions with prefix '{BASE_DOCKER_NAME}' before start ...")
    remove_containers_by_prefix(BASE_DOCKER_NAME)
    kill_tmux_sessions_by_prefix(BASE_DOCKER_NAME)

    syn_int_list = sorted(
        [
            f for f in os.listdir(syn_int_base_directory)
            if os.path.isdir(os.path.join(syn_int_base_directory, f))
            and os.path.exists(os.path.join(syn_int_base_directory, f, "anomaly_description.json"))
        ],
        key=lambda f: int(f.split("_", 1)[0])
    )
    # syn_int_list = syn_int_list[:2]
    logger.info(f"[MAIN] Current Anomaly Descriptions Length: {len(syn_int_list)}")
    
    inference_times = []
    for i, syn_int_dir_name in tqdm(enumerate(syn_int_list), total=len(syn_int_list), desc="Processing Anomaly Descriptions for Target Data Synthesis") :
        syn_int_directory = os.path.join(syn_int_base_directory, syn_int_dir_name)

        parts = syn_int_dir_name.split("_", 1)
        DB_NAME = parts[1] if len(parts) == 2 else ""
        if DB_NAME not in database_names :
            logger.warning(f"[MAIN][WARNING] Skip invalid synthetic intermediate directory `{syn_int_dir_name}` with database `{DB_NAME}`.")
            continue

        anomaly_desp_path = os.path.join(syn_int_directory, "anomaly_description.json")
        with open(anomaly_desp_path, "r") as f :
            anomaly = json.load(f)
        
        logger.info(f"[MAIN][Database Diagnosis for Target Data Synthesis] Processing Anomaly Description {i} / {len(syn_int_list)} : {anomaly['Name']} for Database `{DB_NAME}` ...")
        
        output_dir = os.path.join(output_base_dir, f"{i}_{DB_NAME}")
        os.makedirs(output_dir, exist_ok=True)
        
        DB_SCHEMA_NAME = config.get('database_schema_name', "diagnosis")
        if DB_NAME in database_schema_mapping.keys() : DB_SCHEMA_NAME = database_schema_mapping[DB_NAME]
        if DBMS_KIND == "mysql": DB_SCHEMA_NAME = DB_NAME
        PROMPT_DB_SCHEMA_NAME = MYSQL_SCHEMA_PROMPT_VALUE if DBMS_KIND == "mysql" else DB_SCHEMA_NAME
        
        verified_workload_path = os.path.join(syn_int_directory, "verified_workload.sql")
        initialization_workload_path = os.path.join(syn_int_directory, "initialization_workload.sql")
        user_phenomenon_path = os.path.join(syn_int_directory, "user_phenomenon_description.txt")
        missing_input_files = [
            path for path in [verified_workload_path, initialization_workload_path, user_phenomenon_path]
            if not os.path.exists(path)
        ]
        if missing_input_files :
            logger.error(f"[MAIN][ERROR] Skip `{syn_int_dir_name}` because required input files are missing: {missing_input_files}")
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
                logger.info(f"[MAIN] Docker Container ``{DOCKER_NAME}`` Reinitialized Successfully.")

        if docker_status is False :
            logger.error(f"[MAIN][ERROR] Database `{DB_NAME}` Initialization in Docker Container `{DOCKER_NAME}` Failed.")
            raise RuntimeError(f"[MAIN][ERROR] Database `{DB_NAME}` initialization in Docker Container `{DOCKER_NAME}` failed after {RETRY_TIMES} retries.")
    
        # Obtain Database Schema Information
        db_config = {
            'host' : SYSTEM_USER_CONFIG_JSON['DB_HOST'],
            'port' : SYSTEM_USER_CONFIG_JSON['DB_PORT'],
            'host_port' : DOCKER_PORT,
            'user' : SYSTEM_USER_CONFIG_JSON['DB_USER'],
            'password' : SYSTEM_USER_CONFIG_JSON['DB_PASSWORD'],
            'db_name' : DB_NAME,
            'schema' : DB_SCHEMA_NAME,
            'dbms' : DBMS
        }
        database_schema_path = os.path.join(database_schema_dir, f"{DB_NAME}_schema.json")
        if not os.path.exists(database_schema_path) :
            logger.error(f"[MAIN][ERROR] Database Schema File does not exist for database ``{DB_NAME}``.")
            database_schema = obtain_database_schema(db_config, database_schema_path, DOCKER_NAME)
            if not os.path.exists(database_schema_path) :
                logger.error(f"[MAIN][ERROR] Failed to obtain Database Schema File for database ``{DB_NAME}``.")
                raise RuntimeError(f"[MAIN][ERROR] Failed to obtain Database Schema File for database `{DB_NAME}`.")
        with open(database_schema_path, "r") as f:
            database_schema = json.load(f)
            
        DB_SCHEMA = ""
        DB_ROWS = ""
        for table_info in database_schema['table_info'] :
            DB_ROWS += f"- Table {table_info['table']} with {table_info['rows']} rows.\n"
            DB_SCHEMA += table_info["ddl"] + "\n\n"

        # LLM INPUT INFORMATION
        ANOMALY_INFO = ""
        anomaly_info_dict = anomaly
        if anomaly_info_dict['Type'] == "BOTH": anomaly_info_dict['Type'] = "OLAP + OLTP"
        for key, value in anomaly_info_dict.items() :
            ANOMALY_INFO += f"- {key} : {value}\n"

        SYSTEM_USER_CONFIG_JSON['DB_NAME'] = DB_NAME

        # Initialize Workload + Verified Workload
        VERIFIED_WORKLOAD = extract_sql_from_file(initialization_workload_path) + '\n\n' + extract_sql_from_file(verified_workload_path)

        # User Phenomenon Description
        PHENOMENON_DESCRIPTION = ""
        with open(user_phenomenon_path, "r") as f :
            PHENOMENON_DESCRIPTION = f.read() 

        FAKE_SYSTEM_USER_CONFIG_JSON = SYSTEM_USER_CONFIG_JSON.copy()
        FAKE_SYSTEM_USER_CONFIG_JSON['DB_HOST'] = FAKE_DATABASE_IP

        input_information = synthesize_data_prompt_template.format(
            MAX_TURN_NUM = MAX_TURN_NUM,
            SYSTEM_HARDWARE_CONFIG = SYSTEM_HARDWARE_CONFIG,
            SYSTEM_USER_CONFIG = str(FAKE_SYSTEM_USER_CONFIG_JSON),
            DBMS = DBMS,
            DB_NAME=DB_NAME,
            DB_SCHEMA_NAME=PROMPT_DB_SCHEMA_NAME,
            DB_SCHEMA=DB_SCHEMA,
            DB_ROWS=DB_ROWS,
            ANOMALY_INFO=ANOMALY_INFO,
            VERIFIED_WORKLOAD=VERIFIED_WORKLOAD,
            PHENOMENON_DESCRIPTION=PHENOMENON_DESCRIPTION,
        )

        output_opt = {
            "output_mes_path" : os.path.join(output_dir, f"llm_messages.json"),
            "output_base_dir" : output_dir
        }

        # LLM Inference
        start_time = time.time() 
        # llm_infer_multi_turn may recreate the container under a new unique name
        # across turns; capture the final live name so cleanup targets it.
        DOCKER_NAME = llm_infer_multi_turn(input_information, output_opt, llm_opt, logger, DOCKER_NAME, BASE_DOCKER_NAME, container_config, SYSTEM_USER_CONFIG_JSON, DBMS, MAX_TURN_NUM)
        endtime = time.time()

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

        inference_times.append(endtime - start_time)
        # print(f"Inference {i} completed in {endtime - start_time} seconds.")

        # if i==1 : exit()

    # Final defensive sweep: reap any leaked container/session of THIS worker.
    remove_containers_by_prefix(BASE_DOCKER_NAME)
    kill_tmux_sessions_by_prefix(BASE_DOCKER_NAME)
    logger.info("[MAIN][Database Diagnosis for Target Data Synthesis] LLM Inference Completed, with inference times : {}".format(str(inference_times)))
