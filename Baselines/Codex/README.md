# Codex

Codex diagnoses and recovers database incidents through a native agent loop and
sample-scoped MCP tools. The benchmark prepares each database and injects its
anomaly before diagnosis; the agent receives the observed symptoms, workload,
and database accounts, then submits a report and a reusable recovery script.

## Directory structure

```text
Codex/
├── codex.py                 # Public command-line entry point
├── config.example.json      # Standalone workflow configuration template
├── agent.py                 # Configuration checks and agent lifecycle
├── react.py                 # Shared sample preparation, scheduling, and cleanup
├── cli_runtime.py           # Native CLI command and MCP configuration
├── chat_transport.py        # Model protocol adapter
├── transport.py             # Tool filtering and shared model budgets
├── diagnosis_mcp.py         # MCP server entry point
├── tools.py                 # Diagnostic tools, dispatch, and action budgets
├── sandbox.py               # Database isolation and disposable analysis execution
├── workspace.py             # Evidence workspace and private control-directory name
├── native_agent.py          # Prompt and final-artifact helpers
├── schemas.py               # Database action and completion schemas
├── knowledge.py             # Pinned documentation and restricted web access
├── subagents.py             # Read-only specialist workers
├── docs/                    # Documentation snapshots and SHA-256 manifest
└── Test/                    # Offline regressions and opt-in runtime probes
```

## Prerequisites

- An activated Python environment with the repository's `requirements.txt`
  dependencies installed.
- A Codex CLI executable compatible with the flags in `cli_runtime.py`, available
  on `PATH` or selected with `llm_configuration.codex_executable`.
- For live runs: Linux, Docker, `tmux`, `nsenter`, and the required non-interactive
  privileges (`sudo -n` when not running as root). Prepare the benchmark's database
  images and seed data. The `pg_diag` image is also required for analysis tools,
  including when diagnosing MySQL samples.
- An authorized model endpoint and credential. Use an environment variable rather
  than placing a real key in configuration files, which are copied into run output.

## Configure and run

Run these commands from the repository root with the activated Python environment:

```bash
# Run from the repository root.
cp -n Baselines/Codex/config.example.json Baselines/Codex/config.local.json
```

Edit `config.local.json` before continuing: set the dataset index, output directory,
unique container prefix, available host ports, database credentials, and model
provider/endpoint/model. Replace all placeholder paths and endpoint values with
ones available in your environment.

```bash
# Show command-line options.
python -B Baselines/Codex/codex.py --help

# Validate configuration and required sample files; no model key or Docker needed.
python -B Baselines/Codex/codex.py \
  --config Baselines/Codex/config.local.json --dry-run

# Load the credential without placing it in shell history.
read -rsp 'Model API key: ' CODEX_MODEL_KEY; echo
export CODEX_MODEL_KEY

# Check credentials and local runtime prerequisites without diagnosing samples.
python -B Baselines/Codex/codex.py \
  --config Baselines/Codex/config.local.json --preflight

# Run diagnosis for the indexed samples.
python -B Baselines/Codex/codex.py \
  --config Baselines/Codex/config.local.json
```

`--config` is required and is resolved relative to the shell's working directory.
Relative dataset and output paths **inside** the configuration are resolved from
the repository root. `--dry-run` and `--preflight` are mutually exclusive.
Preflight checks that a credential is present, inspects the CLI and local analysis
image, and verifies the pinned documentation; it does not authenticate with the
model service or validate every database prerequisite. Dry-run checks file
presence, not whether SQL or injection scripts will execute successfully.

### Dataset layout

`key_information_path` points to a JSON list beside the sample directories. Each
entry needs `database_name` and a `sample_id` or `anomaly_id` matching its directory.
The runner also accepts an unambiguous `<id>_<database>` directory name. Each sample
must contain:

```text
<id>/
├── config.json
├── user_phenomenon_description.txt
├── initialization_workload.sql
├── verified_workload.sql
├── data_modification.sql
└── anomaly_inject.sh
```

Keep any additional files required by the injection entry point, including compiled
payloads. The agent's evidence workspace does not expose the source sample directory.

## Configuration parameters

### Workflow

| Parameter | Meaning |
|---|---|
| `workflow_type` | Use `"codex"`; the entry point selects this method automatically. |
| `key_information_path` | Required dataset index path. |
| `output_base_dir` | Run output directory; set a dedicated directory per experiment. |
| `docker_name` | Required container-name prefix, unique among concurrent runs. |
| `host_port` | First published database port. Reserve one port per worker. |
| `parallelism_num` | Number of concurrent sample workers; default `1`. |
| `superuser_config` | Required `DB_USER` and `DB_PASSWORD` for database recovery. |
| `max_turn_num` | Database actions including the finish action; default `20`, range `2–100`. |
| `action_timeout` | Maximum database action duration in seconds; default `1800`, range `1–1800`. |
| `agent_timeout` | Per-sample native agent duration in seconds; default `14400`, range `60–14400`. |
| `database_init_timeout` | Optional database initialization timeout in seconds; otherwise selected by dataset. |

### Model (`llm_configuration`)

| Parameter | Meaning |
|---|---|
| `provider`, `base_url`, `model_name` | Provider routing, authorized endpoint, and model identifier. |
| `api_key` | Required configuration field; leave `""` when using an environment variable. |
| `api_key_env` | Credential environment variable; defaults to `CODEX_MODEL_KEY`. |
| `api_version` | Provider-specific API version, where required. |
| `temperature` | Model sampling temperature supported by the chosen endpoint. |
| `timeout` | Upstream request timeout in seconds; capped at `300` by the adapter. |
| `retry_times`, `request_retry_times`, `request_retry_delay_seconds`, `sdk_max_retries` | Request/retry policy. The adapter caps retry counts at `4` and `3` respectively and disables SDK-level retries. |
| `codex_executable` | Optional CLI executable name or absolute path. |
| `codex_tool_mode` | `"function"` for function-calling endpoints or `"json"` for JSON tool-action adaptation. |
| `codex_transport_retries`, `codex_transport_timeout_seconds` | Local protocol adapter retries and timeout. |

The runner uses the chat-completions adapter for its upstream model requests.
An explicitly configured `codex_api_key` or non-empty `api_key` takes precedence
over `api_key_env`; avoid conflicting credential settings.

### Tool and model budgets (`codex`)

These optional limits apply per sample. Specialists share the auxiliary-action
and model budgets with the main agent.

| Parameter | Default | Allowed values / purpose |
|---|---:|---|
| `max_aux_actions` | `100` | `1–200`; workspace, documentation, planning, and other auxiliary actions. |
| `max_model_calls` | `100` | `1–200`; shared upstream model requests. |
| `max_model_tokens` | `500000` | `1000–2000000`; usage budget checked before subsequent requests. |
| `max_specialists` | `4` | `1–4`; maximum specialist jobs per sample. |
| `specialist_turns` | `6` | `1–10`; maximum turns per specialist. |
| `enable_specialists` | `true` | Enable bounded read-only specialists. |
| `enable_web_docs` | `true` | Enable restricted official-documentation web tools. Pinned local docs remain available. |

## Outputs and reruns

The run directory contains `config.json`, `log.txt`, and `summary.json`. Each sample
has its own ID directory with `status.json`, runtime logs, and, after successful
completion, `diagnosis_report.md`, `solution_script.sh`, and
`optimized_verified_workload.sql`. An empty optimization submission retains the
original verified workload.

`agent_workspace/inputs/` holds the approved evidence and `agent_workspace/work/`
holds agent-created files. `agent_control/` holds private runtime state and tool
audits. Native event logs and `model_usage.json` support debugging; treat the
entire output directory as private experiment data.

Samples already marked `completed`, `failed`, or `cancelled_after_api_error` are
skipped on rerun. Use a new output directory for a fresh attempt, including after
an incomplete attempt that left private agent state.

This entry point produces diagnosis artifacts, not evaluation scores. For the
combined diagnosis-and-scoring pipeline, configure `Configs/evaluation/codex/example.json`
and pass it to `Evaluation/test_baseline_eval.py --config`. That pipeline uses
separate `baseline` and `eval` sections; do not pass its configuration to `codex.py`.

## Runtime checks

```bash
python -B Baselines/Codex/codex.py --config YOUR_CONFIG.json --dry-run
python -B Baselines/Codex/codex.py --config YOUR_CONFIG.json --preflight
```

Dry-run validates configuration and sample paths; preflight checks local runtime
prerequisites. The retained `Test/host_probe.py` and `Test/integration.py` are
opt-in checks using real Docker containers. `Test/native_smoke.py` also calls the
configured model. These checks do not calculate benchmark evaluation scores.
