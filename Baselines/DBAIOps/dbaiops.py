"""Run DBAIOps' open-source knowledge-Q&A flow in the evaluation harness."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


BASELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASELINE_DIR.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Baselines import react  # noqa: E402
from llm_api import resolve_api_key


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
    sample_config = react.load_json_file(files["config"])
    dbms = sample_config.get("dbms", config.get("dbms", "PostgreSQL"))
    dbms_kind = react.normalize_dbms(dbms)
    internal_port = 3306 if dbms_kind == "mysql" else 5432
    host_port = published_port(container_name, internal_port)

    user_config = dict(react.DEFAULT_DB_USER)
    user_config.update(config.get("system_user_config") or {})
    user_config.update(sample_config.get("system_user_config") or {})
    database_name = str(sample_config.get("database_name", user_config.get("DB_NAME", "postgres")))
    native_root = output_dir / "native_dbaiops"
    native_root.mkdir(parents=True, exist_ok=True)
    started_at = time.time()

    llm_config = config["llm_configuration"]
    knowledge_config = config.get("knowledge_service") or {}
    environment = os.environ.copy()
    environment.update(
        {
            "OPENAI_API_KEY": resolve_api_key(llm_config),
            "OPENAI_MODEL": str(llm_config["model_name"]),
            "OPENAI_API_BASE": str(llm_config["base_url"]),
            "OPENAI_PROVIDER": str(llm_config.get("provider", "")),
            "OPENAI_API_VERSION": str(llm_config.get("api_version", "")),
            "DBAIOPS_SNAPSHOT_CONTAINER": container_name,
            "DBAIOPS_SNAPSHOT_ADMIN_USER": str((config.get("superuser_config") or {}).get("DB_USER", "root")),
            "DBAIOPS_SNAPSHOT_ADMIN_PASSWORD": str((config.get("superuser_config") or {}).get("DB_PASSWORD", "")),
            "TEMPERATURE": str(llm_config.get("temperature", 1)),
            "DBAIOPS_RETRY_TIMES": str(llm_config.get("retry_times", 24)),
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
            "CONDA_HOME": str(
                config.get(
                    "conda_home",
                    str(Path.home() / "miniforge3"),
                )
            ),
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
        str(files["config"].parent),
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

    candidates = [
        path
        for path in native_root.glob("*/artifact_action.json")
        if path.stat().st_mtime >= started_at
    ]
    if not candidates:
        raise RuntimeError("DBAIOps completed without producing artifact_action.json.")
    action_path = max(candidates, key=lambda path: path.stat().st_mtime)
    action = json.loads(action_path.read_text(encoding="utf-8"))
    (output_dir / "native_artifact_action.json").write_text(
        json.dumps(action, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return action


if __name__ == "__main__":
    react.run(
        privileged=True,
        default_config=PROJECT_ROOT / "Configs" / "baselines" / "dbaiops.json",
        description="DBAIOps open-source knowledge-Q&A baseline.",
        agent_runner=run_dbaiops_agent,
        workflow_type="dbaiops",
    )
