"""stdio MCP entry point. All model-controlled functionality goes through Tools."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mcp.server.fastmcp import FastMCP
from Baselines.Codex.tools import SPECS, Tools


def make_server(context: dict):
    tools = Tools(context)
    server = FastMCP("codex_mcp")
    for name in sorted(tools.enabled):
        model, description, read_only = SPECS[name]

        def bind(tool_name, input_model):
            async def call(params):
                try:
                    return await asyncio.to_thread(tools.invoke, tool_name, params.model_dump())
                except (OSError, TimeoutError) as exc:
                    raise ValueError(f"Tool operation failed: {type(exc).__name__}. No wider access was granted.") from None
            call.__name__ = tool_name
            call.__annotations__ = {"params": input_model, "return": dict}
            return call

        server.tool(name=name, description=description, annotations={
            "readOnlyHint": read_only,
            "destructiveHint": name == "db_execute",
            "idempotentHint": read_only and name not in {"agent_status"},
            "openWorldHint": name in {"web_search", "web_fetch"},
        })(bind(name, model))
    return server, tools


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    server, tools = make_server(json.loads(args.context.read_text()))
    try:
        server.run(transport="stdio")
    finally:
        tools.specialists.close()


if __name__ == "__main__":
    main()
