# DiagDBench: Evaluating Agentic LLMs on Interactive Database Diagnosis and Anomaly Remediation [Experiment, Analysis & Benchmark]

Database diagnosis and anomaly remediation are essential to reliable database management system (DBMS) operation. However, constructing executable environments that reproduce diverse anomalies at scale remains challenging, limiting the samples available for database agent training and evaluation.

**DiagDBench** is a publicly available benchmark addressing this gap through diverse, realistic anomaly scenarios. It supports three tasks:

- **Interactive diagnosis:** Identify root causes based on the given user request through iterative interaction with a live DBMS.
- **Anomaly remediation:** Execute corrective actions and verify recovery.
- **Optional workload performance optimization:** Improve workload performance while preserving its original semantics.

---

## Repository Structure

```
DiagDBench/
├── DiagDBench/          # Benchmark dataset archive tracked with Git LFS
│   └── DiagDBench.tar.gz # Archive containing eval/ and train/
├── Anomaly_Def/         # Public anomaly-definition with RAG knowledge base
├── Baselines/           # Baseline methods for database diagnosis agents
├── Configs/             # Example configuration files
├── Docker/              # PostgreSQL/MySQL Dockerfiles and runtime init scripts
├── Scripts/             # Evaluation & database utility scripts
├── Skills/              # Agent skill for LLM-based anomaly sample construction
├── test_*.py            # Data-synthesis & generation pipeline
├── docker_func.py       # Docker/DBMS runtime orchestration helpers
├── llm_api.py           # Unified LLM API client
├── prompt_template.py   # Prompt templates for the synthesis pipeline
├── utils.py             # Shared utilities
└── requirements.txt     # Python dependencies
```

---

## Dataset (`DiagDBench/`)

The dataset is distributed as the single archive [`DiagDBench/DiagDBench.tar.gz`](./DiagDBench/DiagDBench.tar.gz) (1,026,029,584 bytes, approximately 979 MiB), tracked with Git LFS. Install Git LFS before cloning; if the clone contains only an LFS pointer, download the archive with `git lfs pull`.

The archive contains 7,394 samples and 74,463 regular files, with this layout:

```text
eval/
├── key_information.json
└── <sample_id>/          # 454 evaluation samples
train/
├── MySQL/               # 3,296 training samples
└── PostgreSQL/          # 3,644 training samples
```

Once the archive is available locally, run the following from the repository root. Extraction keeps the archive in place and creates the paths used by the evaluation configs:

```bash
tar -xzf DiagDBench/DiagDBench.tar.gz -C DiagDBench
```

Archive SHA-256: `01f6567725bbafc96b065a6f474611b22545512adc05fbf7367aa10f68ca4d5d`.

Each sample is a self-contained directory bundling its anomaly definition and the inputs needed to reproduce and evaluate it: `anomaly_description.json`, `config.json`, `user_phenomenon_description.txt`, `initialization_workload.sql`, `verified_workload.sql`, (`merged_workload.sql`), `data_modification.sql`, `data_feature_summarization.txt`, a per-sample evaluation script (`anomaly_eval.py`), and an anomaly injector (`anomaly_inject.sh`).

- **`eval/`** — Evaluation split with 454 samples in a flat layout (one numeric directory per sample), indexed by a top-level `key_information.json`. Each injector is shipped source-free: an `anomaly_inject.sh` launcher plus an encrypted `.inject_entry` binary.
- **`train/`** — Training split, grouped by DBMS into `MySQL/` (3296 samples) and `PostgreSQL/` (3644 samples). Each sample keeps its plaintext `anomaly_inject.sh`.

---

## Docker Runtime and Baseline Evaluation

Build the database images from the repository root. Each build context includes its corresponding initialization script:

```bash
docker build -t pg_diag -f Docker/Ubuntu_PG/Dockerfile.postgresql Docker/Ubuntu_PG
docker build -t mysql_diag -f Docker/Ubuntu_MySQL/Dockerfile.mysql Docker/Ubuntu_MySQL
```

The images provide PostgreSQL 16 and MySQL 8.4 with reusable initialized data directories. Database benchmark inputs under `Data/` or `Database_Instances/` must also be prepared for the selected database, as required by [`docker_func.py`](./docker_func.py).

[`test_baseline_eval.py`](./test_baseline_eval.py) runs diagnosis, recovery scoring, and workload semantic-consistency scoring sequentially for each sample. It supports the published ReAct, Codex, Claude Code, DB-GPT, and DBAIOps implementations, with configs under [`Configs/test_baseline_eval/`](./Configs/test_baseline_eval). Each method uses its native agent entry point; DB-GPT skips MySQL samples because its diagnostic tools support PostgreSQL only.

Set the model endpoints and model names in the selected config. Supply model credentials through `OPENAI_API_KEY` (ReAct, DB-GPT, DBAIOps), `CODEX_API_KEY` (Codex), or `ANTHROPIC_API_KEY` (Claude Code), plus `JUDGE_API_KEY` for both scoring stages. Codex requires its CLI and isolation tools; Claude Code requires the Claude Agent SDK and MCP dependencies; DB-GPT and DBAIOps require their configured Conda environments.

```bash
# Validate config, input sample files and method interfaces without model calls.
python test_baseline_eval.py --config Configs/test_baseline_eval/codex.json --dry-run
# Check method dependencies with credentials configured.
python test_baseline_eval.py --config Configs/test_baseline_eval/codex.json --preflight
# Diagnose and evaluate selected samples; omit --sample-ids to process the split.
python test_baseline_eval.py --config Configs/test_baseline_eval/codex.json --sample-ids 0
```

Replace `codex.json` with `react.json`, `claude_code.json`, `db_gpt.json`, or `dbaiops.json` to select another method. Outputs are stored under `<output_root_dir>/<sample_id>/baseline`, `eval`, and `eval_workload`, with a run-level `evaluation_result.json`. Completed stages are resumed; incomplete attempts are archived before retrying.

---

## Data Construction Frameworks

DiagDBench ships two complementary pipelines for building executable anomaly samples. Both reuse the shared runtime modules — [`docker_func.py`](./docker_func.py) (containerized DBMS orchestration), [`llm_api.py`](./llm_api.py) (LLM client), [`prompt_template.py`](./prompt_template.py), and [`utils.py`](./utils.py) — and emit the same self-contained sample layout, so downstream evaluation treats their outputs identically.

The figure below gives a high-level overview of the two data-construction frameworks and how their outputs feed the unified benchmark samples.

![DiagDBench Overview](./Overview.png)

### Manual-Guided Anomaly Synthesis

Sequentially synthesizes user requests, database modifications, and anomaly-injection scripts grounded in official DBMS documentation and DBA manuals. The pipeline runs in three ordered stages, each with its own entry point and example configs under [`Configs/`](./Configs):

1. **User request synthesis** — [`test_syn_int.py`](./test_syn_int.py) generates realistic user requests / diagnostic intentions. Configs: [`Configs/syn_int/`](./Configs/syn_int) (`example_mysql.json`, `example_pg.json`).
2. **Database modification synthesis** — [`test_syn_data.py`](./test_syn_data.py) derives the database-side changes (workloads and data modifications) that realize each request. Configs: [`Configs/syn_data/`](./Configs/syn_data).
3. **Anomaly-injection script synthesis** — [`test_syn_ano.py`](./test_syn_ano.py) authors the executable `anomaly_inject.sh`, grounded in the official-documentation / DBA-manual RAG knowledge base under [`Anomaly_Def/`](./Anomaly_Def) (`anomaly_def_mysql_rag/`, `anomaly_def_pg_rag/`, assembled via [`syn_comp_anomaly.py`](./Anomaly_Def/syn_comp_anomaly.py)) and the curated cards in [`Skills/`](./Skills). Configs: [`Configs/syn_ano/`](./Configs/syn_ano).

### Incident-Guided Anomaly Reproduction

Reconstructs anomaly samples end to end from real-world enterprise online diagnosis incidents. Instead of synthesizing from manuals, it drives a single unified ReAct reproduction loop directly from real incident records:

- **Anomaly reproduction** — [`test_syn_ano_rep.py`](./test_syn_ano_rep.py) reproduces the full sample (workloads, data modification, and `anomaly_inject.sh`) from each incident record, guided by the reproduction playbooks in [`Skills/`](./Skills) (`incident_category_to_anomaly.md`, `reproduction_action_playbook.md`, `fast_environment_reset.md`, `output_contract.md`). Configs: [`Configs/syn_ano_rep/`](./Configs/syn_ano_rep) (`example_mysql.json`, `example_pg.json`).
- **Key-information extraction** — [`test_extract_key_info_rep.py`](./test_extract_key_info_rep.py) distills the reproduced samples into the evaluation-ready `key_information.json` index.

### Sample Extraction

[`test_extract_key_info.py`](./test_extract_key_info.py) assembles evaluation-ready samples from the raw outputs of the **Manual-Guided** synthesis pipeline. It reads the `synthesized_anomaly_directory` (the per-anomaly working directories produced upstream), copies the core sample files into a flat `output_directory`, attaches the shared evaluation script ([`Scripts/anomaly_eval.py`](./Scripts/anomaly_eval.py)) as each sample's `anomaly_eval.py`, and emits the top-level `key_information.json` index (anomaly id / DBMS / database / name / description / type) that the evaluation split is keyed on. Config: [`Configs/extract_key_info/`](./Configs/extract_key_info) (`example.json`, with `synthesized_anomaly_directory` and `output_directory`). This mirrors the role of [`test_extract_key_info_rep.py`](./test_extract_key_info_rep.py) on the Incident-Guided side.

---

Stay tuned for updates!
