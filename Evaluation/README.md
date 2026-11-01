# Evaluation

This directory provides one scoring policy shared by standalone scoring and
integrated diagnosis. It does not implement competing score channels.

## Modules

| File | Role |
|---|---|
| `test_baseline_eval.py` | Unified diagnosis-plus-scoring entry for Codex, ReAct, DB-GPT, and DBAIOps. Select the method with `baseline.workflow_type`. |
| `test_eval.py` | Standalone scoring entry and shared evaluation functions. |
| `method_adapters.py` | Configuration, preflight, and existing native-method delegation. |
| `namespace.py` | Network-namespace manager and privilege-dropped SQL worker (`--worker` is internal). |
| `measurement.py` | Bounded result capture and exact measurement-protocol validation. |
| `recovery_reassessment.py` | Explicit frozen-report conversion, source-bound diagnosis-score reuse and bounded continuation. |
| `offline_rescore.py` | Migrate and verify published historical snapshots without database or model calls. |

`test_baseline_eval.py` and `test_eval.py` are operational evaluation entry points,
despite their `test_` prefix. They run diagnosis/scoring workflows and remain
required by the public commands below.

## Scoring policy

The only channel is `Reinitialized.optimized_llm_judge`:

1. Initialize a clean database and measure the original verified workload.
2. Rebuild the database, apply the anomaly, and verify successful injection.
3. Execute the submitted recovery script, retain its exit status, and wait for settling.
4. Measure the submitted optimized workload in that reconstructed environment.
5. Use the semantic Judge to compare the full ordered SQL programs, with measured
   results as supporting evidence. Keep the original diagnosis-report score and
   recovery latency formula for this channel:
   `score = 0.3 * diagnosis_score + 0.7 * recovery_score`.

The recovery-script success flag is **not** a score gate. A nonzero exit can still
receive recovery credit if the measured workload passes all remaining checks.
Anomaly-injection failure, SQL errors (including warm-up errors), timeouts,
initialization incompatibility, missing latency, and semantic rejection are not
waived. At or above 8x clean-baseline latency, recovery credit is zero. A missing
optimized workload is not treated as a successful rewrite. Judge infrastructure
failures remain explicit and retryable, never replaced with fingerprint scoring.

Original-workload recovery scores, fingerprint-based scores, live post-remediation
scores/captures, and historical five-to-three measurement conversion have been
removed. Result fingerprints are retained only as compact evidence.

## Usage

From the repository root with its Python dependencies installed:

```bash
# Copy an example outside Configs and fill the endpoint, model, paths, and credentials.
cp Configs/evaluation/react/example.json react.local.json
python -B Evaluation/test_baseline_eval.py --config react.local.json --dry-run
python -B Evaluation/test_baseline_eval.py --config react.local.json --preflight
python -B Evaluation/test_baseline_eval.py --config react.local.json --sample-ids 0 1

# Score artifacts that already exist, without starting a diagnosis agent.
cp Configs/evaluation/scoring/example.json scoring.local.json
python -B Evaluation/test_eval.py --config scoring.local.json --dry-run
python -B Evaluation/test_eval.py --config scoring.local.json
```

`--dry-run` and `--preflight` are mutually exclusive. Dry-run makes no database or
model calls, but requires complete configuration and accessible sample files.
Preflight checks local prerequisites. Live execution creates containers and makes
model calls. Both entries also support `python -m Evaluation.<entry>`.

Integrated configs contain `baseline` and `eval` sections; standalone configs are
flat. Judge keys may be supplied by `judge_llm_configuration.api_key_env` (default
`JUDGE_API_KEY`). Keep private configs and outputs out of version control.
`mode` is retained only as `both`; report-only/solution-only score selection and
disabling the semantic Judge are no longer supported. `median_runs` is an explicit
positive integer, defaulting to three; only identical protocols can resume.

Method-specific environments and algorithms remain unchanged. ReAct uses its own
loop, Codex its native agent, and DB-GPT/DBAIOps their existing adapters. DB-GPT
requires `parallelism_num: 1` and excludes unsupported non-PostgreSQL samples before diagnosis. There is no
import-time mutation of another method's driver or the artifact-path resolver.

## Outputs and compatibility

- Integrated: `<output_root>/<id>/baseline/` and `<output_root>/<id>/eval/`.
- Standalone: `<output_base>/<id>/`.
- Sample score: `score_detail.json`, containing `score_channel`, `score`,
  `diagnosis_score`, `recovery_score`, `semantic_judge`, `recovery_detail`,
  measurement/protocol identifiers, and observed recovery-script status.
- Evidence: `diagnosis_judge_detail.json`, `workload_semantic_judge_detail.json`,
  `workload_execution_summary.json`, and preparation/recovery logs.
- Aggregate: `evaluation_result.json`, with one overall score and its diagnosis,
  recovery, and semantic components. No alternate scenario-score fields are emitted.

The current scoring version is `2026-11-02-reinitialized-semantic-v6`. Old scores
are never silently treated as current. Source/artifact/protocol changes invalidate
reuse; outdated attempts are archived, not rewritten in place. Existing finished
diagnosis artifacts can still be used for fresh scoring. Update custom consumers
to read the new single-channel fields rather than removed scenario paths.

The public `evaluate_single_sample`, artifact-path resolution, SQL execution, and
method `run_agent` argument contracts are preserved. The retired duplicate entry
and helper module files are no longer present; all in-repository callers use the
consolidated modules. No live runs are started by importing them.

## Native-report reassessment and historical migration

For a DB-GPT or DBAIOps integrated config, set `native_output_root` to an existing
native diagnosis output directory, and use a disjoint `output_root_dir`.
The normal `test_baseline_eval.py` entry converts terminal, nonempty reports into
recovery artifacts, preserves hash-bound diagnosis scores, and scores recovery
in a newly reconstructed database. `continuation_passes` accepts 0–3 (default 0);
only unfinished samples retry, while completed zero scores remain final.
Relative configuration paths resolve from the repository root.

The original `Test/DB_Baselines` conversion and continuation logic is implemented
by `Baselines/recovery_artifacts.py` and `recovery_reassessment.py`.
The original `Test/re_eval` migration and verification logic is available through:

```bash
python -B Evaluation/offline_rescore.py --source /path/to/published-snapshot
python -B Evaluation/offline_rescore.py --verify /path/to/derived-snapshot
```

Historical migration preserves the original measurements and removes only the
recovery-script exit-status score gate. Its reviewed scoring fingerprint ignores
empty Python-version-specific AST fields, and rejects substantive scorer changes.
Historical inputs must use `mode: both`, matching the retained scoring formula.
