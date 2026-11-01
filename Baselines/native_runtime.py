"""Shared, dependency-light invocation boundary for external native methods."""
import json
import os
from pathlib import Path
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def project_path(value, root=ROOT):
    path = Path(value).expanduser()
    return (path if path.is_absolute() else Path(root) / path).resolve()


def resolve_conda_home(config, root=ROOT):
    value = config.get("conda_home") or os.getenv("CONDA_HOME") or Path.home() / "miniforge3"
    return project_path(value, root)


def resolve_knowledge_config(config, root=ROOT):
    """Keep independent knowledge credentials identical for CLI and evaluator."""
    knowledge = dict(config.get("knowledge_service") or {})
    credentials = knowledge.get("credentials_file")
    if credentials:
        path = project_path(credentials, root)
        if path.is_file():
            stored = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(stored, dict):
                raise ValueError("DBAIOps knowledge credential file must contain a JSON object.")
            for name in ("api_key", "url"):
                if stored.get(name):
                    knowledge[name] = stored[name]
    if not knowledge.get("api_key"):
        knowledge["api_key"] = os.getenv(knowledge.get("api_key_env", "DBAIOPS_KNOWLEDGE_API_KEY"), "")
    return knowledge


def new_run_directory(native_root):
    """Give each caller an exact result location, independent of mtimes/workers."""
    native_root = Path(native_root).resolve()
    native_root.mkdir(parents=True, exist_ok=True)
    prefix = time.strftime("%Y%m%dT%H%M%SZ_", time.gmtime())
    return Path(tempfile.mkdtemp(prefix=prefix, dir=native_root))
