"""Lightweight host/kernel compatibility probe, not a database benchmark."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import uuid

from Baselines.Codex.sandbox import DatabaseSandbox, command
from Baselines.Codex import react


def main():
    root = react.PROJECT_ROOT
    private = Path(tempfile.mkdtemp(prefix="codex_host_", dir=root / "_Back_Up"))
    canary = private / "raw_sample_canary.txt"
    canary.write_text("HIDDEN_SOURCE_" + uuid.uuid4().hex)
    name = "codex_host_" + uuid.uuid4().hex
    state = {"checks": {}, "fixture": str(private)}
    try:
        command(["docker", "run", "-d", "--name", name, "--network", "none",
                 "--memory", "128m", "--cpus", "0.25", "-w", "/",
                 "--mount", f"type=bind,source={root},target={root},readonly,bind-propagation=rprivate",
                 "--entrypoint", "python3", "pg_diag", "-c", "import time; time.sleep(240)"],
                timeout=120)
        sandbox = DatabaseSandbox(name, root, "codex_host")
        state["checks"]["mount_seal"] = sandbox.seal()["sealed"]
        result = sandbox.execute("printf 'LIVE_OK\\n'\ncat " + str(canary) + "\n",
                                 private / "probe.log", 10)
        text = (private / "probe.log").read_text()
        state["checks"]["container_execution"] = "LIVE_OK" in text
        state["checks"]["source_hidden"] = "HIDDEN_SOURCE_" not in text and result["returncode"] != 0
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)
        state["checks"]["container_reclaimed"] = not subprocess.check_output(
            ["docker", "ps", "-a", "--filter", "name=^/" + name + "$", "--format", "{{.ID}}"], text=True).strip()
        (private / "result.json").write_text(json.dumps(state, indent=2))
    print(json.dumps(state, indent=2))
    assert all(state["checks"].values())


if __name__ == "__main__":
    main()
