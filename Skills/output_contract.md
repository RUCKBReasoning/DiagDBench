# Skill: Output Contract (Reproduction Sample Files)

Use this card when assembling the `finish` action for `test_syn_ano_rep.py`.
Every completed reproduction sample MUST contain these 10 core artifacts,
matching the extraction contract in `test_extract_key_info.py`
(`core_file_names` = the first 9, plus the copied `anomaly_eval.py`). The LLM
emits 7 of them on `finish`; the harness produces the remaining 3
deterministically. Do not rename or omit them, or add fields to the action.
Harness-created logs, source provenance and per-turn directories can coexist
with these core files; do not delete or attempt to emit those.

## The 10 files

| # | File | Produced by | Notes |
| --- | --- | --- | --- |
| 1 | `anomaly_description.json` | LLM (finish) | JSON object string, exactly `Name`, `Description`, `Type`, `SuccessCriterion` |
| 2 | `user_phenomenon_description.txt` | LLM (finish) | User-visible symptom, non-expert wording |
| 3 | `initialization_workload.sql` | LLM (finish) | Runs before each verified-workload timing pass |
| 4 | `verified_workload.sql` | LLM (finish) | The measured workload; each statement ends with `;` |
| 5 | `merged_workload.sql` | **harness** | Deterministic concat of init + verified |
| 6 | `merged_workload_performance.json` | **harness** | Per-statement timing from a normal-state run |
| 7 | `data_modification.sql` | LLM (finish) | One-time data shaping; `""`/no-op if unused |
| 8 | `data_feature_summarization.txt` | LLM (finish) | Data characteristics behind the anomaly |
| 9 | `anomaly_inject.sh` | LLM (finish) | First line `# TIMEOUT THRESHOLD: <int>` |
| 10 | `anomaly_eval.py` | **harness** (copied) | Copied from `Scripts/anomaly_eval_rep.py` |

## anomaly_description.json structure

```json
{
  "Name": "Blocking chain on orders from long-running FOR UPDATE transaction",
  "Description": "Record evidence: the summary reports DML waiting behind an uncommitted transaction. Local reproduction: a background transaction holds a row lock on a known key and the verified UPDATE waits on that same key until timeout. Local assumption: the record does not identify the original table or key, so inspected target objects represent the same access pattern. Validation: the verified UPDATE succeeds before injection and its wait is observed behind the holder after injection. This supports the local blocking mechanism, not an independently proven production root cause.",
  "Type": "OLTP",
  "SuccessCriterion": "BLOCKING_TIMEOUT"
}
```

The example illustrates structure for a matching blocking record; substitute
actual evidence and observations. Never copy an unperformed validation claim.

- `Type` MUST be one of `OLAP`, `OLTP`, `BOTH`.
- Downstream, `test_extract_key_info.py` renders `BOTH` as the human-readable
  string `"OLAP + OLTP"` in `key_information.json` — keep the stored value as the
  bare token `BOTH`.
- `SuccessCriterion` MUST be exactly `PERFORMANCE_DEGRADATION`,
  `BLOCKING_TIMEOUT`, or `EXECUTION_ERROR`. The harness extracts it and passes
  it to the evaluator. Missing/unknown values fall back to `AUTO` for legacy
  compatibility; do not rely on that fallback in new samples.
- Use `Description` for a concise trace from **record evidence → supported or
  hypothesized mechanism → local mapping/assumptions → observed validation**.
  Identify missing platform layers or DBMS substitutions and unrepresented
  symptoms. Distinguish a reported diagnosis from a locally tested hypothesis.
  Do not add `Evidence`, `Confidence`, `RootCause`, `ResetScope` or other keys:
  the reproduction prompt requires exactly the four keys above.
- `user_phenomenon_description.txt` describes the targeted user-visible
  operation and symptom. Do not invent impact counts, durations, errors or
  outages; identify a local approximation when the real symptom is not fully
  representable. Put DBA assumptions in `Description`.
- `data_feature_summarization.txt` describes inspected/shaped local data, not
  unobserved production statistics. Keep object names, sizes and skew consistent
  with the executable SQL.

## Finish serialization

Emit exactly one action JSON object with `action: "finish"` and the seven
LLM artifact names in the table as **string** values. Encode the nested
`anomaly_description.json` object as a JSON string with escaped quotes.
Escape line breaks as `\n` in every multiline string. Keep the existing
`<think>...</think>` then `<action>`/JSON format; no additional narrative,
diagnosis file, classification JSON, `skip` action or harness-produced fields.
The first iteration cannot finish, and an unvalidated candidate is not a
successful reproduction.

## TIMEOUT THRESHOLD convention

- The FIRST line of every executable shell action and of `anomaly_inject.sh` MUST
  be `# TIMEOUT THRESHOLD: <integer seconds>`.
- Set it large enough to cover injection plus several baseline + injected
  verified-workload runs. The harness additionally inflates the evaluation
  timeout using the merged-workload runtime
  (`timeout += median_runs * 2.5 * merged_workload_total_exec_time`, capped at
  86400s), so pick a sane base (hundreds to low thousands of seconds) rather than
  something tiny.

## Self-consistency requirements

- All artifacts reference the supplied schema/database, the SAME
  tables/columns, and the SAME anomaly mechanism.
- `initialization_workload.sql` + `verified_workload.sql` MUST run cleanly on the
  **normal** database state (before injection) — validate them with a SQL action
  first, including repeated cycles. Initialization restores deterministic
  prerequisites before each verified execution; every normal verified run must
  succeed and return identical query results. Expected harmless initialization
  diagnostics are permissible only when execution continues and verification
  remains unaffected. The harness builds `merged_workload.sql` and times it on the normal
  state to produce `merged_workload_performance.json`; if the merged workload
  errors on the normal state, the sample is invalid.
- `data_modification.sql` runs before the normal baseline and must preserve that
  guarantee. It prepares data, not an already-active anomaly. Keep the causal
  perturbation in `anomaly_inject.sh`; do not depend on exploratory leftovers.
- Initialization also runs after injection. It must not erase the mechanism
  being tested or become the sole source of a blocking/error result. Measure
  the intended symptom in `verified_workload.sql`.
- After `anomaly_inject.sh` runs, the verified workload MUST visibly degrade,
  block, or error for the declared reason. The injection script returns
  successfully while required holders/workers remain effective for the measured
  window. It contains no verified-workload execution or observation queries.

## De-identification / secrets

- Do NOT print or embed real credentials or hostnames. Use the runtime
  environment variables (`PGPASSWORD`/`MYSQL_PWD`, `PGHOST`, etc.).
- The harness substitutes a fake host placeholder into prompts and restores the
  real host in your emitted actions, so always reference the host via the
  provided environment/config rather than hard-coding it.

## What the current evaluator establishes

`Scripts/anomaly_eval_rep.py` performs a normal warm-up pass, measured baseline
passes, injection, and measured injected passes. Every workload pass first runs
initialization; verified-workload time drives the latency comparison. It applies
the runtime median-run count and thresholds rather than values inferred from
record severity or impact labels.

| Declared criterion | Current mechanical acceptance |
| --- | --- |
| `PERFORMANCE_DEGRADATION` | Relative median increase reaches the configured threshold OR the enabled absolute-delta floor is reached; injected errors/timeouts are also mechanically accepted. |
| `BLOCKING_TIMEOUT` | Injected timeout or verified-workload error is mechanically accepted; pure slowdown is insufficient. |
| `EXECUTION_ERROR` | Injected verified-workload error or timeout is mechanically accepted; pure slowdown is insufficient. |

This is a coarse performance/failure check, not a semantic checker of the
record's root cause, error identity, lock graph, replica lag or historical
impact. The injected timeout handler also spans initialization, so locate the
actual waiting operation during exploration. Do not exploit a setup hang,
unrelated syntax error, missing object, or deadlock between injectors to obtain
a pass. An unrelated error accepted under a degradation criterion does not
establish faithful reproduction.

Declare the category that matches the record's primary symptom, inspect the
exact post-injection behavior, and preserve semantic limits in `Description`.
“客户影响: 性能退化” or “规则筛选: 工作负载执行异常” alone never determines
the criterion. Successful authoritative evaluation validates the local sample;
it does not by itself prove the proposed historical root cause.
