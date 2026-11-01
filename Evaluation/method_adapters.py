"""Thin native-method adapters; no alternative scoring or diagnosis algorithm."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import threading

from Baselines.ReAct import react
from Baselines.native_runtime import resolve_conda_home, resolve_knowledge_config

ROOT = Path(__file__).resolve().parents[1]
NATIVE = {"db_gpt": ("DB-GPT/db_gpt.py", "run_db_gpt_agent"),
          "dbaiops": ("DBAIOps/dbaiops.py", "run_dbaiops_agent")}
_modules = {}
_module_lock = threading.Lock()


def knowledge_config(config):
    """Backward-compatible public alias shared with the standalone CLI."""
    return resolve_knowledge_config(config, ROOT)


def validate_config(config):
    workflow = config.get("workflow_type")
    if workflow not in {"react", *NATIVE}:
        raise ValueError("Unsupported native workflow: " + str(workflow))
    react.validate_config(config, True)
    if workflow in NATIVE:
        if not config.get("conda_home") or not config.get("conda_env"):
            raise ValueError(workflow + " requires its existing conda_home and conda_env.")


def preflight(config):
    """Check existing runtime dependencies; never call a model or start a DB."""
    validate_config(config)
    workflow = config["workflow_type"]
    result = {"workflow": workflow, "model": config["llm_configuration"]["model_name"],
              "parallelism": config["parallelism_num"], "ok": True}
    from Evaluation.namespace import preflight as namespace_preflight
    result["measurement_runtime"] = namespace_preflight()
    if workflow in NATIVE:
        home = resolve_conda_home(config, ROOT)
        python = home / "envs" / config["conda_env"] / "bin/python"
        if not python.is_file() or not (home / "etc/profile.d/conda.sh").is_file():
            raise RuntimeError("Missing configured native environment: " + str(python))
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        # DBAIOps uses its stdlib HTTP client, not the OpenAI SDK.
        imports = "openai, psycopg2" if workflow == "db_gpt" else "psycopg2"
        probe = subprocess.run(
            [str(python), "-B", "-c", f"import {imports}; print('native_dependencies_ok')"],
            env=env, capture_output=True, text=True, timeout=30)
        if probe.returncode:
            raise RuntimeError("Native dependency import failed: " + probe.stderr[-1200:])
        native_dir = ROOT / "Baselines" / ("DB-GPT" if workflow == "db_gpt" else "DBAIOps")
        native = subprocess.run(
            ["bash", str(native_dir / "script.sh"), "--preflight"],
            cwd=str(native_dir),
            env=dict(env, CONDA_HOME=str(home), CONDA_ENV=config["conda_env"]),
            capture_output=True, text=True, timeout=90)
        if native.returncode:
            raise RuntimeError("Native script preflight failed: " + native.stderr[-1600:])
        result["native_script_preflight"] = native.stdout.strip()
        if workflow == "dbaiops":
            knowledge = knowledge_config(config)
            env_name = knowledge.get("api_key_env", "DBAIOPS_KNOWLEDGE_API_KEY")
            if not (knowledge.get("api_key") or os.environ.get(env_name)):
                raise RuntimeError(
                    "DBAIOps requires its separate knowledge-service API key; set " +
                    env_name + " or provide " + str(knowledge.get("credentials_file",
                    "baseline.knowledge_service.api_key")) + ". The LLM key is not substituted.")
        result["native_python"] = str(python)
    return result


def run_agent(prompt, config, container, output, files, logger):
    """Delegate to the existing DB-GPT/DBAIOps implementation unchanged."""
    workflow = config["workflow_type"]
    if workflow == "react":
        raise RuntimeError("ReAct must use its existing _run_case_body loop with agent_runner=None.")
    relative, function = NATIVE[workflow]
    with _module_lock:
        if workflow not in _modules:
            name = "_dbdiag_native_" + workflow
            spec = importlib.util.spec_from_file_location(name, ROOT / "Baselines" / relative)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
            _modules[workflow] = module
        module = _modules[workflow]
    if workflow == "dbaiops":
        config = dict(config)
        knowledge = knowledge_config(config)
        key = knowledge.get("api_key") or os.environ.get(
            knowledge.get("api_key_env", "DBAIOPS_KNOWLEDGE_API_KEY"))
        if not key:
            raise RuntimeError("DBAIOps knowledge-service credential is not configured.")
        knowledge["api_key"] = key
        config["knowledge_service"] = knowledge
    return getattr(module, function)(prompt, config, container, output, files, logger)
