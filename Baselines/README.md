# Database Diagnosis Agents

This directory contains five methods with a shared output format:
`diagnosis_report.md`, `solution_script.sh`, and `optimized_verified_workload.sql`.
Diagnosis inputs include the reported symptoms, initialization and verification
workloads, database account information, and runtime observations.

| Method | Entry point | Diagnosis approach |
|---|---|---|
| ReAct | `ReAct/react.py` | Explicit reasoning, action, and observation loop |
| Codex | `Codex/codex.py` | Native Codex CLI loop with controlled MCP tools |
| Claude Code | `Claude_Code/claude_code.py` | Native Claude Agent SDK with WebSearch/WebFetch and controlled MCP tools |
| DB-GPT | `DB-GPT/db_gpt.py` | Native multi-agent diagnosis followed by recovery-artifact generation; currently PostgreSQL only |
| DBAIOps | `DBAIOps/dbaiops.py` | Knowledge-retrieval Q&A diagnosis followed by recovery-artifact generation |

## Prompt requirements

`input_contract.py` defines the input format and shared evaluation requirements
for all five methods. ReAct adds them when rendering its templates; Codex and
Claude Code include them in their native task prompts. DB-GPT adds them to role
descriptions and templates, and DBAIOps includes them in its diagnosis system
prompt. Both adapters that convert reports into recovery code apply the same
requirements.

- Optimize only `verified_workload.sql`. Keep the initialization workload
  unchanged and exclude it from the optimization output.
- Preserve statement count and order, per-statement results, duplicates, NULLs,
  transaction boundaries, side effects, and persistent state. If equivalence
  cannot be established or there is no concrete performance benefit, return an
  empty optimization field; the artifact writer retains the original workload.
- Recovery scripts must run independently from the anomalous state, without
  relying on actions performed during diagnosis.
- A successful script exit must mean the anomaly and any remaining recovery
  activity have been resolved, so workload verification can start immediately.
  Use bounded polling and postconditions as appropriate for rollback/undo, lock
  release, I/O, maintenance completion, resources, and service readiness. Exit
  with a nonzero status if recovery cannot be confirmed; fixed sleeps, background
  jobs, and ignored errors do not establish completion.
- Reports must distinguish observations, recommendations, and unverified claims.
  The artifact writer checks format and Bash syntax only. SQL semantics and
  actual recovery still require evaluation; saving a file does not establish
  successful recovery.

## Native capabilities and isolation

| Capability | Codex | Claude Code |
|---|---|---|
| Autonomous loop | Native CLI | Native Agent SDK |
| Database Bash/SQL | `db_execute` in the sample's isolated container | Same |
| Local files | `workspace_*` | Same |
| Python/Bash analysis | `analysis_execute` in a separate offline container | Same |
| Plans and observation logs | `plan_*`, `read_observation` | Same |
| Documentation and network | Pinned documents, online search of official PostgreSQL documentation, and reads of official PostgreSQL/MySQL documentation | Same, plus native `WebSearch`/`WebFetch` |
| Auxiliary specialists | Read-only MCP specialists using the same model and shared budget | Disabled |

Codex currently uses a Chat Completions gateway to adapt the Responses protocol.
The gateway cannot execute the native Responses `web_search` tool, so online
search is provided through MCP. This setup does not exercise the complete native
web-tool interface. Claude's native web capabilities depend on the model
provider; offline checks cannot verify the provider's online search service.

Each sample exposes only `<sample_output>/agent_workspace/inputs/` and `work/`.
`inputs/` contains approved symptoms, workloads, and database account information;
`work/` holds generated notes and scripts. Access to parent directories, absolute
paths, hidden paths, symbolic links, and hard links is rejected. Initialization
logs, injection code, raw samples, control context, and other samples' outputs
are not exposed.

After initialization and injection, both native agents seal host mounts in the
**same database container**, inspect file references held by running processes,
and disconnect external networking. SQL file reads follow the same boundary.
If sealing fails, the run fails without relaxing isolation or killing anomalous
processes to pass the checks. Normal database processes and diagnosis evidence
are preserved. Anomalies that require continued access to raw files or external
networks may be incompatible with this isolation scheme and must be recorded
separately.

Native host shell and file tools remain disabled; controlled tools provide the
corresponding operations. A working directory and a read-only sandbox alone do
not prevent reads from other host directories. Codex disables automatic loading
of user configuration, rules, and project instructions, and filters tools at the
gateway. Claude uses a separate configuration directory, no inherited settings
sources, strict MCP configuration, and no automatic CLAUDE.md or memory loading.
Its `PreToolUse` hook rejects unknown capabilities. Controller and SDK runtime
files are outside the model-accessible workspace.

The default budget is 20 database actions, including submission, and 100 auxiliary
tool actions. Claude's SDK turn limit is their sum; native web calls count toward
SDK turns. Auxiliary MCP actions also have their own budget. These settings do
not automatically equalize total compute across methods.

## Directory structure and public interfaces

The top-level shared modules have the following responsibilities:

| File | Responsibility |
|---|---|
| `__init__.py` | Lightweight package initialization, without legacy ReAct aliases or eager loading of diagnosis dependencies |
| `input_contract.py` | Present initialization and verification workloads with their stage boundaries, render prompts, and add shared diagnosis and recovery requirements |
| `preparation.py` | Preparation-error classification, data preparation and I/O scheduling, LLM retry policy, injection commands, and a sequential batch CLI |
| `container_boundary.py` | Sample staging, the four-file allowlist, diagnosis isolation, trusted injection namespaces, and restricted container execution |
| `script.sh` | Configuration checks, run locking, and entry-point dispatch for a single method |

Methods and evaluation entry points import the current modules directly; ReAct
uses `from Baselines.ReAct import react`. Codex and Claude Code retain their own
sample drivers. Importing `preparation.py` does not launch a batch; scheduling
starts only through its script or module entry point. Sample staging and
container boundaries share one module, preserving the function signatures,
public file allowlist, and `_sample_staging_dir` configuration field without
changing the anomaly mechanism or evaluation result format.

Paths are case-sensitive: use `ReAct`, `Codex`, and `Claude_Code` exactly.
The Codex driver, protocol adapter, and artifact schemas live in its directory.
Claude Code reuses its MCP, tool, and isolation components; preserve these
dependencies when reorganizing files.

The canonical configuration uses `workflow_type: "codex"` with a `"codex"`
options block. Its entry point is `Baselines/Codex/codex.py`, and its default
credential environment variable is `CODEX_MODEL_KEY`. See
[Codex documentation](Codex/README.md) for configuration, commands, and parameters.

Standalone diagnosis examples for all five methods are stored at
`Configs/baselines/<method>/example.json`. Copy an example to a private
configuration outside `Configs/` and fill in the LLM/Judge endpoint, model, and
any required API version and credentials. Use `api_key_env` to supply baseline
and Judge credentials through environment variables. DBAIOps also requires a
separate knowledge-service endpoint and credential environment variable.

| Configuration | Entry point and purpose |
|---|---|
| `Configs/evaluation/codex/example.json` | `Evaluation/test_baseline_eval.py`: Codex diagnosis and evaluation |
| `Configs/evaluation/<react\|db_gpt\|dbaiops>/example.json` | `Evaluation/test_baseline_eval.py`: diagnosis and evaluation with the selected method |
| `Configs/evaluation/scoring/example.json` | `Evaluation/test_eval.py`: score existing diagnosis artifacts |

The integrated evaluator defaults to the Codex example; the standalone evaluator
defaults to the scoring example. Select other integrated methods explicitly with
`--config`. Integrated configurations contain `baseline` and `eval` sections;
standalone scoring configurations use flat fields.

The examples use three measured runs, three Judge repetitions, a 60-second
recovery settling interval, and a recovery score of zero at or above eight times
the clean-baseline latency. They default to one concurrent task. They contain no
evaluation samples; fill in input paths, output directories, model settings,
container prefixes, and ports before running.

`script.sh CONFIG_JSON` launches one method. It validates the configuration,
locates the Python environment, checks container prefixes and ports, performs a
dry-run, locks the output directory, saves a configuration snapshot, and starts
the selected entry point. It accepts standalone method configurations of the
`Configs/baselines` form, rather than the evaluation configurations above.

The batch CLI in `preparation.py` accepts `--config-dir`, `--output-dir`, and an
optional `--framework`. The input directory holds method JSON configurations; the
output directory holds `agentic_smoke_status.json` and `suite_status.json`.
Defaults are `Configs/baselines` and `Output/baseline_suite`. Both
`<method>/example.json` and user-provided legacy `<method>.json` layouts are
supported. Batch execution requires a parallelism of two for each method, a
successful preflight, and matching configuration hashes. It supervises process
groups, selects ports, records failures, and cleans up the batch's containers and
tmux sessions.

Temporary debug-reporting code has been removed from batch scheduling, the Codex
protocol adapter, and Claude SDK configuration.

## Running and validating

Run these commands from the repository root in the `db_diag` Python environment
with the required dependencies installed:

```bash
python Baselines/Codex/codex.py --config YOUR_CONFIG.json --dry-run
python Baselines/Codex/codex.py --config YOUR_CONFIG.json --preflight
python Baselines/Claude_Code/claude_code.py --config YOUR_CONFIG.json --dry-run
bash Baselines/script.sh YOUR_CONFIG.json
python Evaluation/test_baseline_eval.py --config YOUR_EVAL_CONFIG.json --dry-run
python Evaluation/test_eval.py --config YOUR_SCORING_CONFIG.json
python Baselines/preparation.py --help

python -m Baselines.Codex.Test.host_probe
python -m Baselines.Codex.Test.integration --dbms postgresql
python -m Baselines.Codex.Test.integration --dbms mysql
```

`--dry-run` checks configuration and sample paths; `--preflight` checks local
runtime prerequisites. `Codex/Test/` retains opt-in runtime checks:
`host_probe.py` checks container isolation, `integration.py` uses separate small
PostgreSQL/MySQL containers, and `native_smoke.py` calls the configured model
through the native agent. These checks do not replace benchmark evaluation.
`Evaluation/test_baseline_eval.py` and `Evaluation/test_eval.py` are the actual
evaluation entry points.
