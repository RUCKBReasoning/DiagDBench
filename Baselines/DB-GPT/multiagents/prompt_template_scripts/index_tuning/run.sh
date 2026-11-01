#!/usr/bin/env bash
set -e
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
python run_index_tuning.py --exp_id 1 --algo_conf ../index_tuning/configs/instruction_induction.yaml --db_conf ../index_tuning/configs/config.ini --train_data ./data/raw/train/tpch_tpcds.json --eval_data ./data/raw/execute/job.json --gen_sample 2 --gen_demo 16 --gen_prompt_per_sample 3 --gen_model text-davinci-003 --gen_max_tokens 200 --eval_sample 10 --eval_model text-davinci-003 --eval_max_tokens 1000
