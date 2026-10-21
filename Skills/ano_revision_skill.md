---
name: ano-revision
description: Revise synthetic database anomaly samples using review evidence. Use for repairing workload, injection, timeout, and phenomenon-description defects before final validation.
---

# Anomaly Sample Revision

## Objective

Produce the smallest defensible revision that preserves the intended anomaly,
makes the injection executable, and allows the evaluator to distinguish a real
database anomaly from a harness or timeout artifact.

Treat Chinese human-review comments as authoritative revision guidance, but
write all final sample text, comments, and generated content in concise English.
Preserve exact SQL, shell commands, identifiers, paths, and error codes.

## Evidence Discipline

Use evidence in this order:

1. Logs from the same evaluation run.
2. The complete executed injection, initialization, verification, and evaluator
   source.
3. Checkpoints and command exit status.
4. Health reports or per-statement reports from other collection times only as
   supporting context.
5. Names, comments, labels, and intended behavior only as hypotheses.

Never claim that an intended branch ran merely because it exists in the script.
Confirm its process started, its command succeeded, and its database effect
occurred. A successful healthy baseline does not validate code used only during
injection.

Do not infer a specific error, wait type, saturation, corruption, replication
lag, authentication failure, or network failure from generic slowdown,
timeout, or command failure. If implementation and evidence do not support a
specific user-facing claim, remove only that claim and retain the supported
high-level symptoms.

## Revision Order

1. Fix deterministic script or SQL defects.
2. Correct timeout and evaluation-budget defects.
3. Remove unsupported claims from the phenomenon description.
4. Make workloads repeatable and results deterministic.
5. Add difficulty only after the original mechanism passes validation.

Prefer a minimal repair over replacing the anomaly, rewriting the business
workload, changing labels, or inventing a new failure mode.

## Timeout And Validation

Distinguish three independent limits:

- The anomaly script's `# TIMEOUT THRESHOLD`.
- Database-side limits such as `statement_timeout` or `lock_wait_timeout`.
- The evaluator's total remaining budget.

An injected timeout is not anomaly evidence when the remaining validation
budget is shorter than a normal baseline run. Use:

`validation_budget >= max(valid_baseline_runtime) + expected_injected_wait + startup_and_cleanup_margin`

Use a proportional margin when runtime is noisy. If the evaluator budget is
the limiting factor, increase that budget or the controlling timeout threshold;
do not intensify the anomaly or lengthen sleeps. Set the injection threshold
high enough for setup, synchronization, intended overlap, observation, and
cleanup, but no higher than needed.

Require healthy initialization and verification to pass consistently across
the configured repeated rounds and again in a fresh container. Compare
functional outputs, not only exit codes or runtimes.

A fast injected failure duration is time-to-failure, not improved performance.
A completed injected run proves slowdown only when the measured change reaches
the configured criterion. A timeout proves blocking only when the budget was
otherwise sufficient and the relevant wait or failure path is supported.

## Script Correctness

Before accepting an injection:

- Syntax-check shell and SQL using the actual database dialect and execution
  path.
- Preserve real newlines and quoting through nested `bash -c`, heredocs, SQL
  strings, and dollar quoting. Prevent shell expansion of PostgreSQL `$$`.
- Export or explicitly pass every variable, function, array, credential, and
  client command required by child shells.
- Verify background PIDs, exit codes, logs, connection establishment, and
  readiness. Do not hide a core failure with `|| true`.
- Confirm statements truly change data when version growth is required.
  Self-assignment may scan or lock rows without creating the claimed versions.
- Check parameter scope, legal values, privileges, reload/restart requirements,
  and the effective runtime value.
- Account for transactional behavior. MySQL DDL can implicitly commit;
  PostgreSQL `SET LOCAL` requires a transaction; an error may abort or roll back
  all prior work in the transaction.
- Use valid, non-conflicting keys unless a key conflict is the intended anomaly.
- Keep initialization idempotent and cleanup bounded.

## Database Semantics

Describe the mechanism that the engine actually implements:

- Ordinary MVCC reads are not generally blocked by row locks. Claim read
  blocking only with evidence for locking reads, table or metadata locks,
  queued DDL, connection exhaustion, or another valid dependency.
- A running query, lock wait, or `sleep` statement is not an idle session.
- A long snapshot can delay old-version cleanup; it does not by itself stop
  binlog/WAL cleanup or prove replica lag.
- Replica lag, stale replica reads, channel resets, and failover require an
  actual replica path and replication evidence. A single local endpoint cannot
  establish them.
- Memory settings are limits, not proof of allocation or exhaustion. Disk,
  CPU, connection, and I/O pressure likewise require observed utilization or a
  directly verified effect.
- Planner or statistics degradation can change runtime and plans, but does not
  imply wrong query results, syntax rejection, corruption, or data loss.
- Validate exact SQLSTATE/error-code semantics before retaining an error code
  in the user description.

## Workload Integrity

Keep initialization, verification, and injection as separate artifacts.
Preserve business intent, target tables, anomaly-sensitive operations, and
functional result sets.

Eliminate uncontrolled randomness, current-time dependence, unordered
observable output, leaked session state, and prior-run residue. Add explicit
ordering where output order is compared. Do not simplify the workload until it
no longer exercises the target anomaly.

## Adding Diagnostic Difficulty

Add at most one realistic secondary mechanism. It must be deterministic,
bounded, reversible, observable, and subordinate to the original anomaly.
Reuse an existing verified timing or lock window instead of extending a long
transaction.

A common valid pattern is a metadata or schema-change request queued behind an
already-held table lock, causing later readers to wait. Use it only after
confirming lock acquisition, object names, permissions, timeout scope, and
enough overlap with verification. Never present an expected lock chain as an
observed fact.

State how operators can distinguish the original cause from the added one,
using concrete evidence such as lock graphs, transaction age, active sessions,
query plans, database statistics, or resource metrics.

## Final Acceptance

Finish only when:

- Every changed command is executable in the target engine.
- Healthy repeated runs are deterministic.
- Fresh-container validation passes.
- The injected result meets the configured success criterion with sufficient
  budget.
- The user phenomenon contains only supported statements.
- The final response states assumptions and remaining limitations in English.
