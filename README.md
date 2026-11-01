# DiagDBench: Evaluating Agentic LLMs on Interactive Database Diagnosis and Anomaly Remediation

We introduce DiagDBench, a large-scale benchmark for training and evaluating database agents on interactive diagnosis, anomaly remediation, and optional workload performance optimization across diverse, realistic anomaly scenarios.

To construct these samples at scale, we propose an LLM-based framework with two complementary pipelines: **Manual-Guided Anomaly Synthesis**, which sequentially synthesize user requests, database modifications, and anomaly-injection scripts grounded in DBA manuals and official DBMS documentation; and **Incident-Guided Anomaly Reproduction**, which reconstructs anomaly samples end to end from real-world online diagnosis incident records.

![Overview of LLM-based Anomaly Sample Construction Framework](Overview.png)

## Contents

- [Repository Layout](#repository-layout)
- [Manual-Guided Anomaly Synthesis](#manual-guided-anomaly-synthesis)
- [Incident-Guided Anomaly Reproduction](#incident-guided-anomaly-reproduction)
- [DiagDBench](#diagdbench)
- [Database Agents for Diagnosis and Remediation](#database-agents-for-diagnosis-and-remediation)
- [Evaluation](#evaluation)

## Repository Layout

The repository covers four main steps: generate anomaly samples, organize them into datasets, run database agents, and evaluate their diagnosis reports and recovery results.

```text
DiagDBench/
├── README.md
├── Overview.png                      
├── Anomaly_Def/                      # Anomaly definitions and supporting knowledge
├── Baselines/                        # Database diagnosis agents and shared tools
│   ├── ReAct/                        # Agent that repeats reasoning, actions, and observations
│   ├── Codex/                        # Codex CLI integration, database tools, and tests
│   ├── Claude_Code/                  # Claude Agent SDK integration using shared Codex tools
│   ├── DB-GPT/                       # Multi-expert diagnosis agent adapted for this benchmark
│   ├── DBAIOps/                      # Knowledge-based diagnosis service and browser extension
│   ├── input_contract.py             
│   ├── preparation.py                
│   ├── container_boundary.py         
│   ├── native_runtime.py             
│   ├── recovery_artifacts.py         
│   ├── script.sh                     # Starts a diagnosis agent from a configuration file
│   └── test_*.py                     
├── Evaluation/                       # Runs diagnosis and measures recovery quality
│   ├── test_baseline_eval.py         # Runs an agent and scores its diagnosis and recovery
│   ├── test_eval.py                  # Scores existing reports, recovery scripts, and workloads
│   ├── method_adapters.py            
│   ├── measurement.py                
│   ├── namespace.py                  
│   ├── recovery_reassessment.py      
│   ├── offline_rescore.py            
│   └── Test/                         
├── Configs/                          # Example configurations to copy and customize before running
├── Filter/                           # Filters samples using rules or an LLM agent; includes configurations
├── Revision/                         # Revises samples based on human review comments
├── Docker/                           # Builds and starts PostgreSQL and MySQL containers
├── Scripts/                          # Initializes/resets databases, checks anomalies, and reports database health
├── Skills/                           # Instructions for generating, reproducing, and revising anomalies
├── DiagDBench/                        # Dataset archives and sample files used in experiments
├── Data/                             # Directory for database schema files
├── Database_Instances/                # Seed data and loading files for PostgreSQL and MySQL
├── test_syn_*.py                      # LLM-based anomaly synthesis scripts
├── test_extract_*.py                  # Collects generated samples, sample indexes, and diagnosis outputs
├── test_retri*.py                     # Retrieve relevant knowledge for anomaly definitions
├── test_stat_key_info.py              # Summarizes sample counts and characteristics
├── docker_func.py                    # Manages containers, runs database commands, and limits heavy I/O tasks
├── llm_api.py                        # LLM API function call wrapper
├── utils.py                          # Common helpers, including splitting SQL into executable statements
├── prompt_template.py                # Prompt templates for the generation and diagnosis workflows
└── requirements.txt                  # Python packages required by the core workflows
```

See the [agent documentation](Baselines/README.md),
[evaluation documentation](Evaluation/README.md), and
[revision documentation](Revision/README.md) for setup and usage instructions.

### Environment Preparation

Use Linux, a compatible Python environment, Docker, Bash, tmux, and the
`nsenter`/`unshare`/`setpriv` tools required by the isolation paths. Prepare the
necessary host permissions; do not disable isolation checks to bypass a failed
preflight. From the repository root:

```bash
python -m pip install -r requirements.txt

docker build -t pg_diag \
  -f Docker/Ubuntu_PG/Dockerfile.postgresql Docker/Ubuntu_PG
docker build -t mysql_diag \
  -f Docker/Ubuntu_MySQL/Dockerfile.mysql Docker/Ubuntu_MySQL
```

The image definitions target PostgreSQL 16 and MySQL 8.4. Database seeds, schema
files and sufficient CPU/memory/disk capacity must be prepared separately.

Core requirements do **not** install every third-party baseline runtime:

- Codex needs its native CLI and the runtime checked by its preflight.
- Claude Code uses its Agent SDK and an endpoint compatible with that integration.
- DB-GPT and DBAIOps need their own configured Conda environments; DB-GPT also
  needs its native knowledge/embedding dependencies and resources.

### Configuration map

| Workflow | Configuration | Entry |
|---|---|---|
| Standalone diagnosis | `Configs/baselines/<method>/example.json` | `Baselines/script.sh` or the method's Python entry |
| Diagnosis + scoring | `Configs/evaluation/<method>/example.json` | `Evaluation/test_baseline_eval.py` |
| Score existing artifacts | `Configs/evaluation/scoring/example.json` | `Evaluation/test_eval.py` |
| Uer request / data modification / anomaly synthesis | `Configs/syn_int/`, `Configs/syn_data/`, `Configs/syn_ano/` | Corresponding root `test_syn_*.py` |
| Incident reproduction | `Configs/syn_ano_rep/example.json` | `test_syn_ano_rep.py` |
| Extract synthesized samples | `Configs/extract_key_info/example.json` | `test_extract_key_info.py` |
| Human-review revision | `Configs/revision/example.json`, `example_mysql.json` | `Revision/test_ref_anomaly.py` |
| Rule-based / agentic filtering | `Filter/Configs/example_rule.json`, `example_agentic.json` | Corresponding `Filter/test_*.py` |

Most `Configs/` workflow leaves contain `example.json`; revision also has a MySQL
example. `Configs/extract_solution/example.json` records extraction settings, but
`test_extract_solution.py` takes CLI arguments, **not** `--config`.

Copy examples to private `*.local.json` files and fill endpoint, model, optional
API version, input/output paths, container prefixes and ports. Public templates
are not ready-to-run configurations. In particular, replace
`REPLACE_WITH_GENERATED_RUN_DIRECTORY` with the actual preceding stage's output.

| Credential | Default environment variable in the corresponding examples |
|---|---|
| ReAct / synthesis / native baseline model | `OPENAI_API_KEY` |
| Codex model | `CODEX_MODEL_KEY` |
| Standalone Claude Code | `ANTHROPIC_API_KEY` |
| Evaluation Judge | `JUDGE_API_KEY` |
| DBAIOps knowledge service | `DBAIOPS_KNOWLEDGE_API_KEY` |
| Agentic filter model | `FILTER_LLM_API_KEY` |

Use the variable named by the relevant `api_key_env`; do not commit live keys.
Integrated configs have `baseline` and `eval` sections; standalone scoring uses
flat fields. DBAIOps needs a separate knowledge-service URL and credential.
Its obsolete `local_knowledge_dir` setting, still present in some examples, is
ignored: it does not replace the remote knowledge service.

## Manual-Guided Anomaly Synthesis

This pipeline turns anomaly knowledge from DBA manuals and official DBMS
documentation into executable samples through three sequential stages. Each stage
uses database execution feedback to refine its outputs.

| Stage | Entry point | Configuration | Main outputs |
|---|---|---|---|
| User request | `test_syn_int.py` | `Configs/syn_int/example.json` | User issue description and verification workload |
| Database modification | `test_syn_data.py` | `Configs/syn_data/example.json` | Data-modification SQL and database feature summary |
| Anomaly injection | `test_syn_ano.py` | `Configs/syn_ano/example.json` | Injection script and anomaly-validation evidence |
| Sample extraction | `test_extract_key_info.py` | `Configs/extract_key_info/example.json` | Sample directories and `key_information.json` |

### Prepare inputs

Prepare database schemas, seed data, anomaly definitions, and a private model
configuration. `Anomaly_Def/` holds single/composite definitions and knowledge
assets. `Anomaly_Def/syn_comp_anomaly.py` constructs composite definitions;
`test_retri.py` and `test_retri_top_n.py` retrieve and enrich anomaly knowledge.
These preparation utilities have their own input conventions and do not all
accept `--config`.

The synthesis examples target PostgreSQL. For MySQL, also supply matching schemas,
seed data, DBMS/account/port settings, and the appropriate initialization mode.

### Run the pipeline

```bash
cp Configs/syn_int/example.json syn-int.local.json
cp Configs/syn_data/example.json syn-data.local.json
cp Configs/syn_ano/example.json syn-ano.local.json
cp Configs/extract_key_info/example.json extract-key-info.local.json

# Fill schema, anomaly-definition, database, and model settings before starting.
python -B test_syn_int.py --config syn-int.local.json

# Set syn_int_directory to the generated user-request run directory.
python -B test_syn_data.py --config syn-data.local.json

# Set syn_int_directory and syn_data_directory to the corresponding run directories.
python -B test_syn_ano.py --config syn-ano.local.json

# Set synthesized_anomaly_directory to the generated anomaly run directory.
python -B test_extract_key_info.py --config extract-key-info.local.json
```

Replace each `REPLACE_WITH_GENERATED_RUN_DIRECTORY` placeholder with the actual
output of the preceding stage. Set the extraction `output_directory` to a location
outside the repository when the samples will be used for isolated diagnosis or
evaluation. The resulting sample contract is described in [DiagDBench](#diagdbench).

## Incident-Guided Anomaly Reproduction

This pipeline reconstructs an anomaly directly from a production incident record,
using the database schema, system/user configuration, and reproduction guidance.
`test_syn_ano_rep.py` runs an interactive construction loop to produce the anomaly
definition, user issue description, workloads, database modifications, and
injection script.

### Prepare inputs and reproduce an incident

Create a private copy of `Configs/syn_ano_rep/example.json`. Supply
`incident_record_path`, `database_schema_directory`, database/model settings, and
an output directory. The current entry uses the first database in `database_names`
as the reproduction target. Guidance is loaded from `skills_directory`, including
the incident-category and reproduction playbook files in `Skills/`.

```bash
cp Configs/syn_ano_rep/example.json syn-ano-rep.local.json
# Fill the incident record, schema, database, and model settings.
python -B test_syn_ano_rep.py --config syn-ano-rep.local.json
```

Finish validation uses `Scripts/anomaly_eval_rep.py`. The declared
`SuccessCriterion` distinguishes performance degradation, blocking/timeouts, and
execution errors so that validation can match the incident's manifestation.

### Extract a reproduced sample

```bash
# Replace the sample path with an actual <id>_<database> reproduction directory.
python -B test_extract_key_info_rep.py \
  --sample-dir /absolute/path/to/reproduction_run/1_tpch_1 \
  --output-directory /absolute/path/outside/repository/reproduced_samples
```

The extractor writes the sample directory and updates `key_information.json`.
It reads the run configuration from the sample's parent directory by default;
use `--run-config` to provide another path. Reproduced samples can then follow the
same curation and evaluation workflow as manually guided samples.

## DiagDBench

DiagDBench organizes samples from both construction pipelines into a common
interface for interactive diagnosis and remediation. A runnable sample combines
the database environment, a user-observed issue, initialization and verification
workloads, and private anomaly-injection and reference artifacts.

### Local archives versus runnable inputs

The inspected local checkout contains `DiagDBench/Eval.zip` and
`DiagDBench/Train.zip`. They are local data artifacts covered by `.gitignore`,
not a guarantee that a source checkout or release includes downloadable datasets.

**Do not point the current evaluator directly at these archives or assume that
unpacking them is sufficient.** The local `Eval.zip` has an `Eval/` directory and
`Eval/key_information.json`, but uses packaged names such as:

- `anomaly_definition.json`
- `anomaly_verified_workload.sql`
- `user_issue_description.txt`
- `.inject_entry`, invoked through `anomaly_inject.sh`

It does not contain the current driver's separate `initialization_workload.sql`,
`verified_workload.sql`, or `user_phenomenon_description.txt` files. Its ground-truth
filename also differs from the evaluation default `anomaly_description.json`.
The local training archive has a different layout and no `key_information.json`.

Prepare a compatible, validated dataset before execution. Recover the original
preparation/verification phase boundary from authoritative sample information;
merely renaming a merged SQL file or inventing an empty preparation file can
change the experiment. Configure `ground_truth_diagnosis` to the correct
per-sample reference filename and validate its content. Compiled injection
binaries also need a compatible runtime.

### Required runtime layout

Keep the **complete source samples outside the repository**. The staging code
rejects a source inside the project and uses `<repository>/DiagDBench/<id>/` as a
controlled temporary view. For example:

```text
/absolute/path/outside/repository/eval_samples/
├── key_information.json              # JSON list; one entry per sample
└── 0/
    ├── config.json                  # DBMS, database/schema, accounts and resources
    ├── user_phenomenon_description.txt
    ├── initialization_workload.sql
    ├── verified_workload.sql
    ├── data_modification.sql
    ├── anomaly_inject.sh
    ├── anomaly_description.json     # Default ground truth; configurable filename
    └── ...                          # Additional files needed by the sample/injector
```

Set `key_information_path` to the external index, replacing the examples'
`DiagDBench/eval/key_information.json` placeholder. Index entries use `sample_id`
or `anomaly_id`, include `database_name`, and must match the sample directories.
The driver also recognizes the existing `<id>_<database>` directory convention.

Only four public files remain in the agent-visible sample view: `config.json`,
the phenomenon description, and the two workload files. Injection source,
data-modification instructions, ground truth and other private artifacts are
reserved for trusted preparation/evaluation, not supplied as diagnosis evidence.
The original source must not be exposed through another mount or a symlink.

### Filtering and human-review revision

Use `Filter/` to check sample validity and recoverability:

| Filter | Entry point | Configuration | Behavior |
|---|---|---|---|
| Rule-based | `Filter/test_filter_syn_data.py` | `Filter/Configs/example_rule.json` | Measures anomaly validity without an LLM |
| Agentic | `Filter/test_agentic_llm_filter.py` | `Filter/Configs/example_agentic.json` | Filters out recoverable samples (`failed_ids`) and retains unrecovered samples (`pass_ids`) |

Both entries accept `--config` and support `--dry-run`. Real filtering performs
database operations; the agentic filter also calls a model. The root
`test_filter_syn_data.py` exposes the rule-filter workflow as well.

`Revision/test_ref_anomaly.py` refines samples using human review notes. It expects
wrapped `Raw_Data/<dataset>/<id>/raw_sample/` inputs and
`Human_Review/<dataset>/<three-digit-id>.txt`, rather than the flat evaluation
layout. See the [revision documentation](Revision/README.md).

```bash
cp Configs/revision/example.json revision.local.json
# Fill model/environment settings and prepare the review tree before running.
python -B Revision/test_ref_anomaly.py \
  --config revision.local.json \
  --input-root /absolute/path/to/review_input \
  --dataset PG_Syn \
  --output-root /absolute/path/to/revised_output \
  --workers 1
```

Use `Configs/revision/example_mysql.json` for MySQL. Only reviewed samples enter
the model revision loop. Successful revisions require anomaly evaluation and a
valid finish action; unreviewed or failed revisions keep the original sample
content. Keep revision output separate from the input tree.

`test_stat_key_info.py` provides sample statistics and aggregation. Its output
mode can clear the configured destination, so inspect `--help` before use.

### Data management

Keep dataset payloads, database seeds, credentials, private configurations, and
run outputs out of source commits. `.gitignore` covers the main local-data/output
paths and `*.local.json`, but does not untrack files already recorded by Git.
Treat model traces, database evidence, generated scripts, and configuration
snapshots as private data.

For redistribution, review bundled knowledge assets and third-party license and
provenance files, including [DB-GPT provenance](Baselines/DB-GPT/UPSTREAM.md) and
[DBAIOps provenance](Baselines/DBAIOps/UPSTREAM.md).

## Database Agents for Diagnosis and Remediation

`Baselines/` implements five database agents with a shared input and output
contract. Agents use the user-observed issue, workloads, database account
information, and runtime observations to diagnose and remediate anomalies.

### Supported entry points

| Method (`workflow_type`) | Standalone entry | Diagnosis approach | Integrated scoring |
|---|---|---|---|
| `react` | `Baselines/ReAct/react.py` | Explicit reasoning/action/observation loop | Yes |
| `codex` | `Baselines/Codex/codex.py` | Native Codex CLI with controlled MCP tools | Yes |
| `claude_code` | `Baselines/Claude_Code/claude_code.py` | Native Claude Agent SDK with controlled MCP tools | Not registered in the integrated dispatcher |
| `db_gpt` | `Baselines/DB-GPT/db_gpt.py` | Native multi-expert diagnosis and recovery-artifact adapter | Yes; PostgreSQL only |
| `dbaiops` | `Baselines/DBAIOps/dbaiops.py` | Knowledge-Q&A diagnosis and recovery-artifact adapter | Yes |

DB-GPT excludes unsupported non-PostgreSQL samples. DBAIOps runs its headless
knowledge-Q&A adapter. DB-GPT and DBAIOps reuse the ReAct sample lifecycle through
native callbacks; Claude Code reuses Codex MCP/tool/isolation components.

### Run standalone diagnosis

```bash
cp Configs/baselines/react/example.json react-baseline.local.json
# Fill the local configuration first.
python -B Baselines/ReAct/react.py --config react-baseline.local.json --dry-run
bash Baselines/script.sh react-baseline.local.json
```

`Baselines/script.sh` performs configuration/port/container checks, saves a local
configuration snapshot and locks its output directory. It runs **diagnosis only**.
Use the corresponding `Configs/baselines/<method>/example.json` for another agent.
The [Evaluation](#evaluation) section covers scoring the generated artifacts.

The optional sequential suite CLI is `python -B Baselines/preparation.py --help`.
It currently covers ReAct, Codex and Claude Code, requires two workers per method,
and checks existing smoke-test evidence plus configuration hashes. It is not a
replacement for the five-method standalone launcher or integrated scoring entry.

### Shared artifacts and constraints

Baseline completion writes:

```text
diagnosis_report.md
solution_script.sh
optimized_verified_workload.sql
```

The final action's workload field is `verified_workload.sql`; the saver writes
`optimized_verified_workload.sql`. An empty optional rewrite retains the original
verification SQL as the effective artifact. A missing artifact at scoring time is
not equivalent to that valid fallback. Initialization SQL is never part of the
optimized verification artifact.

`Baselines/input_contract.py` defines shared diagnosis and remediation constraints:
the recovery script must run independently from the anomaly state, and optional
workload changes must preserve SQL order, results, transaction boundaries, side
effects, and persistent state. A saved report or script does not by itself
establish successful recovery; evaluation executes and checks the artifacts.

To collect existing standalone artifacts without running a model:

```bash
python -B test_extract_solution.py \
  --solution-directory Output/baselines/react \
  --output-directory Output/extracted_solutions/react
```

The extractor expects immediate per-sample artifact directories, not the nested
integrated `<id>/baseline/` layout. Its optional `--clean-output` flag clears the
destination. See the [agent documentation](Baselines/README.md) for runtime,
tool-isolation, and method-specific details.

## Evaluation

`Evaluation/` provides a shared scoring policy for integrated diagnosis runs and
existing diagnosis artifacts. It measures recovery in a reconstructed anomaly
environment and uses an LLM Judge to assess diagnosis and workload equivalence.

### Integrated diagnosis and scoring

ReAct, Codex, DB-GPT, and DBAIOps use `Evaluation/test_baseline_eval.py`. Select
the method with `baseline.workflow_type`; optional `--method` checks that this
selection matches the configuration. Integrated configs have `baseline` and
`eval` sections.

```bash
cp Configs/evaluation/react/example.json react.local.json
# Edit paths/model settings; supply OPENAI_API_KEY and JUDGE_API_KEY securely.

# No DB/model calls; requires a valid config and accessible sample inputs.
python -B Evaluation/test_baseline_eval.py \
  --config react.local.json --method react --dry-run

# Check local runtime/isolation/native prerequisites.
python -B Evaluation/test_baseline_eval.py \
  --config react.local.json --method react --preflight

# Real execution: creates containers and makes model calls.
python -B Evaluation/test_baseline_eval.py \
  --config react.local.json --method react --sample-ids 0 1
```

Use the corresponding example for another method. `--dry-run` and `--preflight`
are mutually exclusive; omit `--sample-ids` to process the configured index.
Use distinct worker port ranges, container prefixes, and output locations for
concurrent runs. Start with one worker and increase concurrency only after
checking resources and isolation prerequisites. Run DB-GPT benchmark jobs
sequentially because its native implementation uses shared intermediate files.

### Score existing artifacts

```bash
cp Configs/evaluation/scoring/example.json scoring.local.json
# Set the external sample index, solution_script_directory, output, and Judge.
python -B Evaluation/test_eval.py --config scoring.local.json --dry-run
python -B Evaluation/test_eval.py --config scoring.local.json
```

Standalone scoring uses a flat configuration and does not start a diagnosis
agent. It still performs database operations and calls the Judge. Configure
`ground_truth_diagnosis` to match the per-sample reference filename.

### Outputs and scoring semantics

Integrated outputs use `<output_root>/<id>/baseline/` and `<output_root>/<id>/eval/`;
standalone scoring uses `<output_base>/<id>/`. Evaluation produces per-sample
`score_detail.json`, Judge/measurement evidence and logs, plus the aggregate
`evaluation_result.json`.

The current engine has one score channel, **`Reinitialized.optimized_llm_judge`**:

1. Measure the original verified workload on a clean database.
2. Reconstruct the database, apply the anomaly and verify injection.
3. Execute the submitted recovery script and observe the configured settling time.
4. Measure the submitted effective verification workload.
5. Judge the diagnosis and ordered workload semantics using the shared evaluator.

The sample score is `0.3 * diagnosis_score + 0.7 * recovery_score`. Recovery credit
uses the implemented latency curve and is zero at or above 8x clean-baseline
latency. SQL/initialization errors, timeouts, invalid injection and semantic
rejection remain failures. The recovery script's exit status is recorded, but a
nonzero exit alone is not the recovery-score gate. Fingerprints are supporting
evidence, not an alternative to the semantic Judge.

There is no separately scored original-workload or live post-remediation channel.
Current examples request three measured repetitions, Judge repetitions and a
settling interval. Resume decisions check scoring version, source/artifact hashes
and the exact measurement protocol; outdated results are not silently reused.
The supported mode is `both`. See the [evaluation documentation](Evaluation/README.md)
for the authoritative field layout, failure semantics, and compatibility details.

`Evaluation/offline_rescore.py` and `Evaluation/historical_scoring.py` support
explicit migration of historical evaluation snapshots. They do not produce fresh
benchmark measurements or replace the current evaluator.

### Offline checks

Use the core project environment for:

```bash
python -B -m unittest discover -s Baselines/Codex/Test -p 'test_*.py' -v
python -B -m unittest Baselines.test_native_interfaces Baselines.test_recovery_artifacts -v
python -B -m unittest discover -s Evaluation/Test -p 'test_*.py' -v
python -B Baselines/DBAIOps/test_native_runner.py -v
```

Run the DB-GPT contract test in its configured environment, which includes its
YAML dependency:

```bash
python -B Baselines/DB-GPT/test_upstream_contract.py -v
```

Targeted discovery matters: root files named `test_syn_*`, `test_eval*`, etc. are
also workflow entry points, not a uniform unit-test suite. Do not recursively
import all vendored DB-GPT packages just to discover its contract test.

Codex's `host_probe.py`, `integration.py` and `native_smoke.py` are separate,
opt-in runtime checks; integration uses real containers and native smoke may use
a real model. They are not included by the `test_*.py` command above. Passing
syntax checks, offline tests or dry-run does not establish dataset compatibility,
model-service availability, isolation support, or end-to-end diagnosis accuracy.
