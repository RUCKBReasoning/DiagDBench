# Codex

This is the restricted, evidence-scoped Codex method. There is only one Codex
method in this tree. The previous unrestricted method entry point is not included.

Entry point: `python Baselines/Codex/codex.py --config Configs/baselines/codex.json`

The method retains its post-injection mount sealing, bounded MCP actions,
database-process checks, offline analysis workspace, documentation search, and
specialist budgets. `cli_runtime.py`, `driver.py`, `schemas.py`, `artifacts.py`,
and `chat_transport.py` are its required shared infrastructure, not other methods.

Set CODEX_API_KEY in the environment, configure an accessible model endpoint and
model name, install the Codex CLI, and provide external input data and database
images before running. The example is a configuration template, not a runnable
benchmark bundle. The `docs` directory contains the existing documentation
snapshot and its source-URL/hash manifest; it contains no benchmark samples.

No model requests or database actions were executed during this import.
