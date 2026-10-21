# Skill: Reproduction Action Playbook (SQL / Bash / Python)

Use this card after interpreting the record with `incident_category_to_anomaly.md`.
Turn an evidence-supported hypothesis into an executable, measured scenario
using SQL, Bash or Python actions. Target the supplied DBMS/version and actual
schema; PostgreSQL and MySQL differ in locks, configuration scope, catalogs and
error behavior, not just client syntax.

## Evidence-to-experiment workflow

1. Summarize the reported symptom, supported mechanism, remaining assumption
   and intended `SuccessCriterion` briefly in the existing `<think>` section.
   Keep the full action JSON; do not output a separate report.
2. Inspect only what distinguishes plausible mechanisms: actual indexes/keys,
   relevant row counts/distribution, engine/version, settings, privileges, and
   transaction semantics. An EXPLAIN recommendation in a record is not a plan
   observation. Use local observations to test a candidate, not retroactively
   invent production findings.
3. Construct a clean normal state with `data_modification.sql` if needed.
   Validate repeated `(initialization_workload.sql -> verified_workload.sql)`
   cycles: the verified SQL must succeed and return identical normal results.
   Seed/setup conditions belong before measurement; the causal perturbation
   belongs in `anomaly_inject.sh`.
4. Apply the candidate injection during exploration; separately run the exact
   verified workload and inspect the relevant lock, plan, error or pressure.
   Compare against the same normal workload, with the same role and settings.
   Check that initialization neither undoes the injection nor blocks first.
5. Revise unsupported assumptions from observations while preserving the
   record's causal target and the selected manifestation.
6. Emit `finish` only after an observed matching effect, never in the first
   round. If authoritative validation fails, preserve the record's causal
   target and repair the measured failure; do not replace it with a convenient
   unrelated error or weaken the success criterion.

## Connection conventions

Use runtime connection environment variables; do not copy real production
identifiers or passwords from the record. Do not echo secrets.

```bash
# PostgreSQL exploration: PGPASSWORD and PGOPTIONS come from the harness.
psql -X -q -P pager=off -v ON_ERROR_STOP=1 \
  -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "SELECT 1;"
# MySQL exploration: MYSQL_PWD comes from the harness. The reproduction
# environment also exposes the PG-prefixed host/port/user/database values.
mysql --batch --raw --skip-column-names \
  -h "$PGHOST" -P "$PGPORT" -u "$PGUSER" "$PGDATABASE" -e "SELECT 1;"
```

These are connectivity probes for exploration, not injection content. Use the
configured schema/database and inspected identifiers; do not assume `diagnosis`,
`orders`, `lineitem`, or `t` exists. SQL actions contain SQL only; client options
belong in shell actions. `psql -X` skips startup files; `-P pager=off` actually
disables the pager.

## Action selection

- **SQL action** — for inspection and single-session prototyping: look at the
  schema, row counts, existing indexes, sizes, and to validate the
  initialization/verified workloads run cleanly on the normal state.
- **Bash action** — coordinate multiple client sessions, lock holders and bounded
  concurrent workloads. First line MUST carry `# TIMEOUT THRESHOLD: <seconds>`.
- **Python action** — when precise coordination or many connections are easier in
  code (e.g. open N connections and hold them, or fan out concurrent writers with
  barriers). Prefer stdlib + the target DBMS client; use a driver only after
  confirming it is available.

## Mechanism recipes and discriminating observations

### 1. Row-lock or DDL/metadata-lock contention

- Choose this when transaction/DDL evidence supports a blocker, not merely
  because the broad category says DML or Lock.
- Hold the relevant lock in an open transaction. Make the verified operation
  conflict with that lock on the same object/key. In PG and ordinary MySQL
  InnoDB consistent reads, a plain SELECT does not normally wait on a
  `SELECT ... FOR UPDATE` row lock. Use a supported conflicting writer/locking
  read, or the appropriate relation/metadata lock when reads must be blocked.
- For DDL queues, establish the long transaction, queued DDL and affected
  operation in that order; verify actual lock compatibility in the target
  engine. Do not infer that any three sessions form a blocking chain.
- During exploration inspect a small relevant subset of PG `pg_stat_activity`,
  `pg_locks` / `pg_blocking_pids`, or MySQL `performance_schema.data_lock_waits`
  / `metadata_locks` when available. Demonstrate the verified client waits
  behind the intended holder.
- A DB-side sleep can hold an acquired lock for a finite evaluation window;
  sleep alone is not evidence of contention. Never add sleep to the verified
  workload to manufacture slowdown.
- Keep initialization independent of blocked objects where possible; a timeout
  in setup is not proof that the intended user operation was blocked.

### 2. Deadlock / transaction abort

- Require a cycle of incompatible lock dependencies, not just one waiter.
  Coordinate transactions acquiring two known keys in opposite order; inspect
  the actual abort/error code (PG `40P01`, MySQL error 1213 where applicable).
- The verified transaction must participate and expose the relevant error.
  A deadlock entirely between two injectors that finishes before verification
  does not reproduce failure of the verified workload.
- Victim choice and auto-rollback break a deadlock; a one-shot cycle is not
  persistent. Establish a bounded repeatable interaction for measured runs and
  validate its victim/timing behavior. Do not claim deterministic victim
  selection without observing it.
- Use `EXECUTION_ERROR` for a verified deadlock abort. A lock timeout is a
  different observation; label any supported blocking analogue explicitly.
  High connections after an abort do not prove that deadlocks exhaust capacity.

### 3. Connection/admission pressure

- Distinguish idle connections, active queries, per-user limits, DB server
  limits and proxy admission limits. Observe capacity and the evaluator's role;
  privileged/reserved connections may bypass the limit you intend to hit.
- Scale a bounded number of DB clients to the relevant available capacity.
  Count successfully established connections rather than spawned processes.
  Avoid fixed floods such as “open 200” without inspecting configuration.
- Confirm that the same kind of connection used by the verified workload
  encounters the relevant rejection/wait. If initialization cannot connect,
  do not present its failure as evidence about a query it never ran.
- A single server cannot reproduce a proxy algorithm, client pool bug or
  network failure. Use a DB-side admission analogue only when justified by
  the record and state the missing layer.

### 4. Plan regression / stale statistics / data skew

- Inspect actual query plans, indexes, selectivity and data distribution before
  choosing a bad-plan mechanism. Increased rows scanned suggests a candidate;
  an unexecuted EXPLAIN suggestion does not confirm a missing index.
- Use bounded, key-safe data shaping for comparable baseline and injected
  runs. Blindly duplicating a table can violate unique keys or change results.
  Do not put the whole causal regression into `data_modification.sql` and
  then call that already-abnormal state the baseline.
- Demonstrate an access-path/estimate change and a matching workload latency
  change. Dropping an index is appropriate only if it is a justified local
  model; do not assert the production index was dropped without evidence.
- Distinguish parameter scope. An injection client's `SET` generally dies
  with that connection; PG role defaults affect later connections only if
  privileges and effective settings permit it. MySQL session optimizer
  settings do not carry into a separate evaluator client. Probe effective
  settings using a new workload-equivalent connection.

### 5. Spill, bloat, maintenance and resource competition

- Match the specific resource evidence. Use bounded SQL readers/writers or
  sort/hash work sized to the supplied instance. Validate actual spill,
  dead-tuple/space growth, plan changes or contention with compact observations.
  Small memory settings do not guarantee spill; a long snapshot alone does
  not create bloat without relevant concurrent updates and cleanup effects.
- Keep normal query results stable. If an injected workload changes results
  as a side effect, explain the relationship to the selected symptom rather
  than mistaking result drift for a measured latency failure.
- Model a backup/cleanup task only through its evidenced DB interactions.
  Do not claim that a heavy scan implements backup, restoration or replication.
- Bound data growth, concurrency and runtime. Do not intentionally fill storage,
  create an unbounded WAL/temp workload or run an endless cross join.

### 6. Permission/configuration failures

- Reproduce the reported error under the actual evaluator identity. PG
  superusers/owners or MySQL administrative users may bypass a revoked grant.
  A failure tested only under a separate role is insufficient.
- Preserve normal-state success and make only the evidence-supported privilege
  or configuration difference. Do not invent a syntax/missing-object failure
  to stand in for an authentication or parameter incident.

## Injection-script rules (anomaly_inject.sh)

- First line: `# TIMEOUT THRESHOLD: <integer seconds>` — big enough for injection
  plus several baseline+injected verified-workload runs (the harness further
  inflates this using the merged-workload runtime, but set a sane base).
- Contains ONLY injection operations — no verified-workload execution, no
  verification/observation queries.
- Background holders must outlive script exit and cover initialization plus
  all measured injected runs, then end within a bounded lifetime. Detach with
  available `nohup`/`setsid` or equivalent and redirect stdin/stdout/stderr so
  inherited pipes do not keep the injection action running. Do not `wait` for
  holders that must still be active during verification.
- Validate startup and timing in exploration; a fixed sleep or spawned PID
  alone does not prove lock acquisition or a successful connection. Keep final
  injection limited to setup/orchestration, without verification queries.
- Guard repeatable setup, but do not suppress unexpected SQL errors or assume
  `IF NOT EXISTS` proves an existing object's schema is compatible.
## Keep observations small

Use LIMIT, aggregation, selected plan lines and targeted session/lock rows.
Preserve the decisive error, wait dependency or before/after measurement.
Separate observations of the local candidate from facts asserted by the record.
