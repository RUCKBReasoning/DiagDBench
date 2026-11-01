import os
import json
import shutil
import argparse

from utils import normalize_dbms, resolve_path_from_base

# sample_id unique
# sample directory : base_dir/{case_idx}_{database_name}/



if __name__ == "__main__" :
    base_dir = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description='Database Diagnosis for Anomaly Injection Key Information Extraction.')
    parser.add_argument('--config', type=str, required=False, default=os.path.join(base_dir, "Configs", "extract_key_info", "example.json"), help='Path to the configuration JSON file')
    args = parser.parse_args()

    config_path = resolve_path_from_base(args.config, base_dir)
    with open(config_path, "r") as f:
        config = json.load(f)
    
    database_names = config.get('database_names', ['tpch_1', 'imdb', 'tpcds_1'])
    database_schema_name = config.get('database_schema_name', 'diagnosis')
    syn_ano_directory = resolve_path_from_base(config['synthesized_anomaly_directory'], base_dir)
    output_directory = resolve_path_from_base(config['output_directory'], base_dir)
    
    key_info_output_path = os.path.join(output_directory, "key_information.json")

    if not os.path.exists(output_directory): os.makedirs(output_directory)

    with open(os.path.join(output_directory, "config.json"), 'w') as file :
        json.dump(config, file, indent=4)

    truncate_num = config.get('truncate_num')

    core_file_names = ['anomaly_description.json', 'user_phenomenon_description.txt', 'initialization_workload.sql', 'verified_workload.sql', 'merged_workload.sql', 'merged_workload_performance.json', 'data_modification.sql', 'data_feature_summarization.txt', 'anomaly_inject.sh']
    anomaly_eval_path = os.path.join(base_dir, "Scripts", "anomaly_eval.py")

    syn_ano_list = sorted(
        [
            name for name in os.listdir(syn_ano_directory)
            if os.path.isdir(os.path.join(syn_ano_directory, name))
            and len(name.split("_", 1)) == 2
            and name.split("_", 1)[0].isdigit()
        ],
        key=lambda name: int(name.split("_", 1)[0])
    )
    if truncate_num is not None and truncate_num >= 0 :
        syn_ano_list = syn_ano_list[:truncate_num]

    with open(os.path.join(syn_ano_directory, 'config.json')) as file:
        syn_ano_config = json.load(file)
    sample_config = {
        "dbms" : syn_ano_config.get("dbms", ""),
        "database_name" : "",
        "database_schema_name" : syn_ano_config.get("database_schema_name", "diagnosis"),
        "system_hardware_config" : syn_ano_config.get("system_hardware_config", {}),
        "system_user_config" : syn_ano_config.get("system_user_config", {}),
    }

    key_information = []
    sample_id = 0
    for syn_ano_dir_name in syn_ano_list :
        parts = syn_ano_dir_name.split("_", 1)
        anomaly_id = int(parts[0])
        db_name = parts[1]
        dbms = syn_ano_config.get("dbms", "")
        current_database_schema_name = db_name if normalize_dbms(dbms) == "mysql" else database_schema_name
        if db_name not in database_names :
            print(f"Database `{db_name}` not in the specified database list, skipping sample {syn_ano_dir_name}.")
            exit(0)

        current_output_directory = os.path.join(output_directory, syn_ano_dir_name)
        current_anomaly_directory = os.path.join(syn_ano_directory, syn_ano_dir_name)
        if not os.path.exists(current_output_directory) :
            os.makedirs(current_output_directory)

        for file_name in core_file_names :
            source_file_path = os.path.join(current_anomaly_directory, file_name)
            target_file_path = os.path.join(current_output_directory, file_name)
            shutil.copyfile(source_file_path, target_file_path)

        target_eval_path = os.path.join(current_output_directory, "anomaly_eval.py")
        shutil.copyfile(anomaly_eval_path, target_eval_path)

        sample_config['database_name'] = db_name
        sample_config['database_schema_name'] = current_database_schema_name
        sample_config_path = os.path.join(current_output_directory, "config.json")
        with open(sample_config_path, "w") as f :
            json.dump(sample_config, f, indent=4)

        anomaly_path = os.path.join(current_anomaly_directory, "anomaly_description.json")
        if os.path.exists(anomaly_path) :
            with open(anomaly_path, "r") as f :
                anomaly = json.load(f)
        else :
            anomaly = {}

        current_key_information = {
            'anomaly_id' : anomaly_id,
            'dbms' : dbms,
            'database_name' : db_name,
            'database_schema_name' : current_database_schema_name,
            'anomaly_name' : anomaly.get('Name', ''),
            'anomaly_description' : anomaly.get('Description', ''),
            'anomaly_type' : anomaly.get('Type', '') if anomaly.get('Type', '')!='BOTH' else 'OLAP + OLTP',
            # 'anomaly_files' : core_file_names,
        }
        key_information.append(current_key_information)

    with open(key_info_output_path, "w") as f :
        json.dump(key_information, f, indent=4)
