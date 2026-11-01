"""Run DBAIOps' open-source knowledge-Q&A flow in the evaluation harness."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


BASELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASELINE_DIR.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Baselines.ReAct import react  # noqa: E402
from Baselines.native_runtime import new_run_directory, resolve_conda_home, resolve_knowledge_config


def published_port(container_name: str, container_port: int) -> int:
    completed = subprocess.run(
        ["docker", "port", container_name, f"{container_port}/tcp"],
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    if completed.returncode:
        raise RuntimeError(
            f"Cannot resolve published port for `{container_name}`: "
            f"{completed.stderr.strip()}"
        )
    address = completed.stdout.splitlines()[0].strip()
    try:
        return int(address.rsplit(":", 1)[1])
    except (IndexError, ValueError) as exc:
        raise RuntimeError(f"Unexpected Docker port mapping: {address}") from exc


def run_dbaiops_agent(
    prompt: str,
    config: dict[str, Any],
    container_name: str,
    output_dir: Path,
    files: dict[str, Path],
    logger,
) -> dict[str, str]:
    del prompt
    output_dir = Path(output_dir).expanduser().resolve()
    config_file = Path(files["config"]).expanduser().resolve()
    if not config_file.is_file():
        raise FileNotFoundError(f"Sample config does not exist: {config_file}")
    sample_config = react.load_json_file(config_file)
    dbms = sample_config.get("dbms", config.get("dbms", "PostgreSQL"))
    dbms_kind = react.normalize_dbms(dbms)
    internal_port = 3306 if dbms_kind == "mysql" else 5432
    host_port = published_port(container_name, internal_port)

    user_config = dict(react.DEFAULT_DB_USER)
    user_config.update(config.get("system_user_config") or {})
    user_config.update(sample_config.get("system_user_config") or {})
    superuser_config = dict(config.get("superuser_config") or {})
    superuser_config.update(sample_config.get("superuser_config") or {})
    database_name = str(sample_config.get("database_name", user_config.get("DB_NAME", "postgres")))
    native_root = output_dir / "native_dbaiops"
    run_dir = new_run_directory(native_root)

    llm_config = config["llm_configuration"]
    knowledge_config = resolve_knowledge_config(config, PROJECT_ROOT)
    if not knowledge_config.get("api_key"):
        raise RuntimeError("DBAIOps knowledge-service credential is not configured.")
    if knowledge_config.get("local_knowledge_dir") or os.getenv("DBAIOPS_LOCAL_KNOWLEDGE_DIR"):
        logger.warning(
            "Ignoring obsolete local knowledge fallback (not upstream DBAIOps). Remove "
            "local_knowledge_dir/DBAIOPS_LOCAL_KNOWLEDGE_DIR and configure the remote service."
        )
    environment = os.environ.copy()
    environment.update(
        {
            "OPENAI_API_KEY": str(llm_config["api_key"]),
            "OPENAI_MODEL": str(llm_config["model_name"]),
            "OPENAI_API_BASE": str(llm_config["base_url"]),
            "OPENAI_PROVIDER": str(llm_config.get("provider", "")),
            "OPENAI_API_VERSION": str(llm_config.get("api_version", "")),
            "DBAIOPS_SNAPSHOT_CONTAINER": container_name,
            "DBAIOPS_SNAPSHOT_ADMIN_USER": str(superuser_config.get("DB_USER", "root" if dbms_kind == "mysql" else "postgres")),
            "DBAIOPS_SNAPSHOT_ADMIN_PASSWORD": str(superuser_config.get("DB_PASSWORD", "")),
            "TEMPERATURE": str(llm_config.get("temperature", 1)),
            "DBAIOPS_RETRY_TIMES": str(knowledge_config.get("retry_times", 3)),
            "DBAIOPS_DATASET_ID": str(knowledge_config.get("dataset_id", "")),
            "DBAIOPS_MODEL_REQUEST_ATTEMPTS": str(
                int(llm_config["request_retry_times"]) + 1
                if "request_retry_times" in llm_config else int(llm_config.get("retry_times", 3))
            ),
            "DBAIOPS_MODEL_REQUEST_DELAY": str(llm_config.get("request_retry_delay_seconds", 30)),
            "DBAIOPS_MODEL_REQUEST_TIMEOUT": str(llm_config.get("timeout", 300)),
            "DBAIOPS_KNOWLEDGE_URL": str(
                knowledge_config.get(
                    "url",
                    os.environ.get(
                        "DBAIOPS_KNOWLEDGE_URL",
                        "https://api.bic-qa.com/api/chat/message",
                    ),
                )
            ),
            "DBAIOPS_KNOWLEDGE_API_KEY": str(
                knowledge_config.get("api_key")
                or os.environ.get("DBAIOPS_KNOWLEDGE_API_KEY", "")
            ),
            "CONDA_HOME": str(resolve_conda_home(config, PROJECT_ROOT)),
            "DIAGDBENCH_NATIVE_RUN_DIR": str(run_dir),
            "CONDA_ENV": str(config.get("conda_env", "dbaiops_baseline")),
        }
    )
    command = [
        "bash",
        str(BASELINE_DIR / "script.sh"),
        "--host",
        "127.0.0.1",
        "--port",
        str(host_port),
        "--user",
        str(user_config["DB_USER"]),
        "--password",
        str(user_config["DB_PASSWORD"]),
        "--db",
        database_name,
        "--dbms",
        dbms_kind,
        "--sample-dir",
        str(config_file.parent),
        "--output-root",
        str(native_root),
        "--collect-db-snapshot",
    ]

    logger.info(
        f"[DBAIOps] Starting native knowledge-Q&A diagnosis for `{dbms}` in container "
        f"`{container_name}` on host port `{host_port}`."
    )
    completed = subprocess.run(
        command,
        cwd=BASELINE_DIR,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
        timeout=float(config.get("baseline_timeout_seconds", 3600)),
    )
    (output_dir / "dbaiops_stdout.log").write_text(completed.stdout, encoding="utf-8")
    (output_dir / "dbaiops_stderr.log").write_text(completed.stderr, encoding="utf-8")
    if completed.returncode:
        raise RuntimeError(
            f"DBAIOps exited with status {completed.returncode}; "
            f"see `{output_dir / 'dbaiops_stderr.log'}`."
        )

    action_path = run_dir / "artifact_action.json"
    if not action_path.is_file():
        raise RuntimeError("DBAIOps completed without producing this invocation's artifact_action.json.")
    action = json.loads(action_path.read_text(encoding="utf-8"))
    (output_dir / "native_artifact_action.json").write_text(
        json.dumps(action, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return action


if __name__ == "__main__":
    react.run(
        privileged=True,
        default_config=PROJECT_ROOT / "Configs" / "baselines" / "dbaiops" / "example.json",
        description="DBAIOps open-source knowledge-Q&A baseline.",
        agent_runner=run_dbaiops_agent,
        workflow_type="dbaiops",
    )
