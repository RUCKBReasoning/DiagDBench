"""Codex native CLI baseline with sample-scoped diagnostic capabilities."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Baselines.Codex import react
from Baselines.Codex.agent import preflight, run_agent, validate_config


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Codex controlled database diagnosis")
    parser.add_argument("--config", type=Path, required=True,
                        help="Path to a Codex workflow JSON configuration.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="Validate configuration and sample files without Docker or model calls.")
    mode.add_argument("--preflight", action="store_true",
                      help="Check credentials and local runtime prerequisites without running samples.")
    args = parser.parse_args(argv)
    config_path = args.config.resolve()
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a JSON object.")
        validate_config(config)
    except (OSError, ValueError, TypeError) as exc:
        parser.error(str(exc))
    if args.preflight:
        print(json.dumps(preflight(config), indent=2))
        return
    if not args.dry_run:
        preflight(config)
    # The driver applies shared defaults before validating the full workflow.
    workflow_args = ["--config", str(config_path)]
    if args.dry_run:
        workflow_args.append("--dry-run")
    react.run(
        privileged=True, default_config=config_path,
        description="Codex: native CLI with sample-scoped tools",
        agent_runner=run_agent, workflow_type="codex", argv=workflow_args,
    )


if __name__ == "__main__":
    main()
