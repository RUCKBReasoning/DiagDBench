# Human-review sample revision

`test_ref_anomaly.py` repairs anomaly samples using per-sample human review notes.
Only samples with a matching review note enter the revision loop. A revised sample
is published only after a successful anomaly evaluation and a valid `Finish`
action. Samples without review notes, or whose revision attempts fail, are copied
to the output with their original content.

## Input layout

```text
<input-root>/
├── Raw_Data/
│   └── PG_Syn/
│       └── 1/
│           └── raw_sample/
│               ├── anomaly_description.json
│               ├── anomaly_eval.py
│               ├── anomaly_inject.sh
│               ├── config.json
│               ├── data_feature_summarization.txt
│               ├── data_modification.sql
│               ├── initialization_workload.sql
│               ├── merged_workload.sql
│               ├── merged_workload_performance.json
│               ├── user_phenomenon_description.txt
│               └── verified_workload.sql
└── Human_Review/
    └── PG_Syn/
        └── 001.txt
```

Use unpadded numeric input sample directories (`0`, `1`, ...). Review filenames
must contain exactly three digits (`000.txt`, `001.txt`, ...). Sibling evidence
folders beside `raw_sample/` are preserved. This entry point expects the wrapped
sample layout above, not a flat evaluation-sample directory.

## Configuration and usage

Activate the project's Python environment and prepare Docker, database images,
seed data, and the privileges required by the shared database helpers. Configure
the model provider, endpoint, and model name in a local copy of an example; set
the credential through the environment variable named by `api_key_env`.

```bash
# Run from the repository root.
cp -n Configs/revision/example.json Configs/revision/config.local.json
# Edit config.local.json and set OPENAI_API_KEY in the environment before running.

python -B Revision/test_ref_anomaly.py \
  --config Configs/revision/config.local.json \
  --input-root external_inputs/revision \
  --dataset PG_Syn \
  --output-root Output/revision \
  --workers 1
```

`Configs/revision/example.json` is the default PostgreSQL template (`db_type:
"dump"`). `Configs/revision/example_mysql.json` uses `db_type: "load_data"` and a
separate container prefix and port. Use the latter for MySQL datasets. A sample's
own `config.json` supplies its DBMS, database, schema, accounts, and hardware; its
optional `db_type` overrides the workflow default.

| Setting | Purpose |
|---|---|
| `docker_name`, `docker_port` | Worker container prefix and first host port; worker `i` uses the base port plus `i`. Use distinct values for concurrent runs. |
| `db_type` | Database initialization method, normally `dump` for PostgreSQL or `load_data` for MySQL. |
| `max_turn_num` | Maximum revision turns, at least `2` for `Refine` then `Finish`. |
| `max_revision_attempts` | Independent attempts per reviewed sample, `1–10`; default `2`. |
| `median_runs` | Repeated measurements passed to the anomaly evaluator. |
| `merge_workloads` | Optional combined-workload mode; keep `false` to retain separate initialization and verification fields. |
| `fast_reset_enabled` | Permit golden-baseline resets; full rebuild remains the fallback. |
| `mysql_fast_reset_experimental_enabled` | MySQL fast reset is opt-in; the MySQL example leaves it disabled. |
| `skills_directory` | Loads `fast_environment_reset.md` and `ano_revision_skill.md`. |
| `database_schema_directory` | Optional schema evidence directory, resolved from the project root. |
| `llm_configuration` | Model routing, credential environment variable, and bounded request settings. Fill the empty endpoint and model fields before use. |

Input/output roots, datasets, and worker count are **CLI arguments**, not JSON
settings. Repeat `--dataset` to process multiple datasets sequentially. Optional
flags include `--raw-subdir`, `--review-subdir`, `--docker-port-base`,
`--merge-workloads`, and `--schema-directory`; use `--help` for the full interface.
The worker default is `8`; start with `--workers 1` and choose a count that fits
available resources. Relative input/output/config paths are project-root-relative;
an explicit relative `--schema-directory` retains its working-directory-relative
behavior. An optional `llm_configuration_file` can supply a shared model block;
inline `llm_configuration` values override it. The examples are self-contained.

Use an output root separate from the input and review trees. Results are written
to `<output-root>/<dataset>/<three-digit-id>/`, with dataset summaries in
`revision_result.json`, logs in `log.txt` and `_Process_Logs/`, and the matching
review note attached to successful revisions. Treat process logs as private data.
Do not run the revision command as an offline check: reviewed samples invoke the
model and execute database operations.
