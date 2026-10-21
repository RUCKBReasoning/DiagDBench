"""Restricted Codex method entry point using the shared native-agent sample driver."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Baselines.Codex import driver as react
from Baselines.Codex.agent import preflight, run_agent, validate_config


def main():
    parser = argparse.ArgumentParser(description="Codex controlled database diagnosis")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    validate_config(config)
    if args.preflight:
        print(json.dumps(preflight(config), indent=2))
        return
    if not args.dry_run:
        preflight(config)
    # run_case -> original Docker init -> original DB init -> original data
    # modification -> original direct injection -> run_agent -> original cleanup.
    react.run(
        privileged=True, default_config=args.config,
        description="Codex: isolated tools with the original Codex setup",
        agent_runner=run_agent, workflow_type="codex",
    )


if __name__ == "__main__":
    main()
