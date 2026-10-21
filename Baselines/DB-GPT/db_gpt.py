"""Run DB-GPT inside the project's isolated anomaly evaluation harness."""

from __future__ import annotations

import argparse
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


DEFAULT_CONFIG = PROJECT_ROOT / "Configs" / "baselines" / "db_gpt.json"


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


def run_db_gpt_agent(
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
    if react.normalize_dbms(dbms) != "pg":
        raise RuntimeError(
            f"DB-GPT's native diagnostic tools support PostgreSQL only; got `{dbms}`."
        )

    user_config = dict(react.DEFAULT_DB_USER)
    user_config.update(config.get("system_user_config") or {})
    user_config.update(sample_config.get("system_user_config") or {})
    superuser_config = dict(config.get("superuser_config") or {})
    superuser_config.update(sample_config.get("superuser_config") or {})
    database_name = str(sample_config.get("database_name", user_config.get("DB_NAME", "postgres")))
    host_port = published_port(container_name, 5432)
    native_root = output_dir / "native_db_gpt"
    native_root.mkdir(parents=True, exist_ok=True)
    started_at = time.time()

    llm_config = config["llm_configuration"]
    environment = os.environ.copy()
    environment.update(
        {
            "OPENAI_API_KEY": resolve_api_key(llm_config),
            "OPENAI_MODEL": str(llm_config["model_name"]),
            "OPENAI_API_BASE": str(llm_config["base_url"]),
            "OPENAI_API_VERSION": str(llm_config.get("api_version", "")),
            "OPENAI_TEMPERATURE": str(llm_config.get("temperature", 1)),
            "OPENAI_RETRY_TIMES": str(llm_config.get("retry_times", 24)),
            "CONDA_HOME": str(
                config.get(
                    "conda_home",
                    str(Path.home() / "miniforge3"),
                )
            ),
            "CONDA_ENV": str(config.get("conda_env", "db_gpt_baseline")),
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
        "--metric-superuser",
        str(superuser_config.get("DB_USER", "")),
        "--metric-superpassword",
        str(superuser_config.get("DB_PASSWORD", "")),
        "--db",
        database_name,
        "--container",
        container_name,
        "--sample-dir",
        str(files["config"].parent),
        "--output-root",
        str(native_root),
    ]
    logger.info(
        "[DB-GPT] Starting native multi-agent diagnosis against container "
        f"`{container_name}` on host port `{host_port}`."
    )
    completed = subprocess.run(
        command,
        cwd=BASELINE_DIR,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
        timeout=float(config.get("baseline_timeout_seconds", 14400)),
    )
    (output_dir / "db_gpt_stdout.log").write_text(completed.stdout, encoding="utf-8")
    (output_dir / "db_gpt_stderr.log").write_text(completed.stderr, encoding="utf-8")
    if completed.returncode:
        raise RuntimeError(
            f"DB-GPT exited with status {completed.returncode}; "
            f"see `{output_dir / 'db_gpt_stderr.log'}`."
        )

    candidates = [
        path
        for path in native_root.glob("*/artifact_action.json")
        if path.stat().st_mtime >= started_at
    ]
    if not candidates:
        raise RuntimeError("DB-GPT completed without producing artifact_action.json.")
    action_path = max(candidates, key=lambda path: path.stat().st_mtime)
    action = json.loads(action_path.read_text(encoding="utf-8"))
    (output_dir / "native_artifact_action.json").write_text(
        json.dumps(action, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return action


def prepare_supported_config(
    config_path: Path,
) -> tuple[Path, dict[str, dict[str, str]], dict[str, Any], int]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    key_info_path = react.project_path(config["key_information_path"])
    cases = json.loads(key_info_path.read_text(encoding="utf-8"))
    supported = []
    skipped = {}
    output_base = react.project_path(config["output_base_dir"])
    staged_input = output_base / "_db_gpt_supported_input"
    staged_input.mkdir(parents=True, exist_ok=True)

    for index, case in enumerate(cases):
        sample_id, sample_dir = react.find_sample_directory(key_info_path.parent, case)
        sample_config = react.load_json_file(sample_dir / "config.json")
        dbms = sample_config.get("dbms", case.get("dbms", "PostgreSQL"))
        if react.normalize_dbms(dbms) != "pg":
            skipped[sample_id] = {
                "status": "skipped",
                "reason": f"DB-GPT native diagnostic tools do not support `{dbms}`.",
            }
            continue
        supported.append(case)
        staged_sample = staged_input / sample_id
        if staged_sample.is_symlink():
            if staged_sample.resolve() != sample_dir.resolve():
                staged_sample.unlink()
        elif staged_sample.exists():
            raise RuntimeError(f"Cannot replace non-symlink staged sample: {staged_sample}")
        if not staged_sample.exists():
            staged_sample.symlink_to(sample_dir.resolve(), target_is_directory=True)

    staged_key_info = staged_input / "key_information.json"
    staged_key_info.write_text(
        json.dumps(supported, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    runtime_config = dict(config)
    runtime_config["workflow_type"] = "db_gpt"
    runtime_config["key_information_path"] = str(staged_key_info)
    runtime_config_path = output_base / "_db_gpt_runtime_config.json"
    runtime_config_path.write_text(
        json.dumps(runtime_config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return runtime_config_path, skipped, config, len(supported)


def write_skipped_statuses(
    output_base: Path,
    skipped: dict[str, dict[str, str]],
    original_config: dict[str, Any],
) -> None:
    summary_path = output_base / "summary.json"
    summary = {}
    if summary_path.is_file():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(skipped)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    for sample_id, status in skipped.items():
        status_path = output_base / sample_id / "status.json"
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n")
    (output_base / "config.json").write_text(
        json.dumps(original_config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="DB-GPT database diagnosis baseline.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    runtime_config, skipped, original_config, supported_count = prepare_supported_config(
        args.config.resolve()
    )
    output_base = react.project_path(original_config["output_base_dir"])
    print(
        f"DB-GPT selected {supported_count} PostgreSQL sample(s); "
        f"skipping {len(skipped)} unsupported sample(s)."
    )

    saved_argv = sys.argv
    sys.argv = [saved_argv[0], "--config", str(runtime_config)]
    if args.dry_run:
        sys.argv.append("--dry-run")
    exit_code = 0
    try:
        react.run(
            privileged=True,
            default_config=runtime_config,
            description="DB-GPT database diagnosis baseline.",
            agent_runner=run_db_gpt_agent,
            workflow_type="db_gpt",
        )
    except SystemExit as exc:
        exit_code = int(exc.code or 0)
    finally:
        sys.argv = saved_argv
        if not args.dry_run:
            write_skipped_statuses(output_base, skipped, original_config)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
