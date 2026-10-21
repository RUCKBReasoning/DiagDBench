import os

# 诊断结果文件夹
DIAGNOSTIC_RESULTS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "alert_results")

# 诊断所用的模型列表
DIAGNOSE_LLM_MODEL_LIST = ["GPT4-0613", "Llama2-13b", "CodeLlama2-13b", "BaiChuan-13b", "Qwen1.5-14B-Chat"]

# 诊断文件所在文件夹
DIAGNOSTIC_FILES_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "diagnostic_files")
if not os.path.exists(DIAGNOSTIC_FILES_PATH):
    os.mkdir(DIAGNOSTIC_FILES_PATH)
# 诊断用的模型，默认gpt4。要用qwen改成config_qwen.yaml
DIAGNOSTIC_CONFIG_FILE = "config.yaml"

# 诊断运行日志文件
DIAGNOSE_RUN_LOG_PATH = "./diagnose_run_log.txt"
# 诊断运行pid文件
DIAGNOSE_RUN_PID_PATH = "./diagnose_run_pid.txt"
# 诊断用户输入文件
DIAGNOSE_USER_FEEDBACK_PATH = "./diagnose_user_feedback.txt"
# 诊断输出信息
DIAGNOSE_RUN_DATA_PATH = "./diagnose_run_data.jsonl"
# 诊断输出原始信息
DIAGNOSE_RUN_DATA_COPY_PATH = "./diagnose_run_data_copy.jsonl"

# 用户反馈是使用交互文件还是终端
IS_FEEDBACK_BY_UI = True

# 数据库配置

POSTGRESQL_CONFIG = {
  "host": os.getenv("DB_GPT_POSTGRES_HOST", "127.0.0.1"),
  "port": int(os.getenv("DB_GPT_POSTGRES_PORT", "5432")),
  "user": os.getenv("DB_GPT_POSTGRES_USER", "postgres"),
  "password": os.getenv("DB_GPT_POSTGRES_PASSWORD", ""),
  "dbname": os.getenv("DB_GPT_POSTGRES_DB", "postgres")
}

MYSQL_CONFIG = {
  "host": os.getenv("DB_GPT_MYSQL_HOST", "127.0.0.1"),
  "port": 3306,
  "user": os.getenv("DB_GPT_MYSQL_USER", "root"),
  "password": os.getenv("DB_GPT_MYSQL_PASSWORD", ""),
}
