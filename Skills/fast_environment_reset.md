# Skill: Fast Environment Reset (Container-Local Golden Baseline)

Use this card whenever a multi-turn anomaly workflow resets its database
environment. It is the sole authority for reset lifecycle and reset-safe
injection boundaries. The harness performs every reset; the model never emits
reset commands or reset metadata.

## Harness lifecycle

The harness imports the configured database into a per-sample container and
captures a golden database in that same container. It then restores the working
database from the golden copy instead of repeatedly importing a multi-GB dump.
Parallel workers remain isolated because each golden copy belongs to one
container and is deleted with that container.

The snapshot point differs by workflow:

| Workflow | Golden baseline | Restore point |
| --- | --- | --- |
| Synthetic injection (`test_syn_ano.py`) | Imported database after the fixed data-modification step | Before each later injection turn |
| Sample revision (`Revision/test_ref_anomaly.py`) | Imported database after the candidate's data modification | Before each candidate observation |
| Online Diagnosis Incident reproduction (`test_syn_ano_rep.py`) | Raw imported database before any LLM-authored `data_modification.sql` | Before each `finish` validation and after a failed validation, followed by the current candidate's data modification |

The reproduction baseline intentionally excludes `data_modification.sql`.
That file may change during Refine, so every restore replays the current version
instead of reviving a stale candidate.

## Engine behavior

- PostgreSQL clones with `CREATE DATABASE ... TEMPLATE ...`. Reset terminates
  working-database backends, resets role settings, recreates the database from
  the golden template, and restores `search_path`.
- MySQL clones base tables inside InnoDB with `CREATE TABLE LIKE` and
  `INSERT ... SELECT`. Schemas containing views fall back to a full rebuild.
  MySQL fast reset remains opt-in through
  `mysql_fast_reset_experimental_enabled`; PostgreSQL is enabled by default.

Configuration:

```json
{
  "fast_reset_enabled": true,
  "mysql_fast_reset_experimental_enabled": false
}
```

## Failure and timeout policy

Fast reset is an optimization, never a correctness dependency. A missing
snapshot, non-zero reset result, unsupported schema, or reset exception triggers
the original full path: recreate the container, import the database, and capture
a new golden baseline. Candidate data modification is applied only after that
raw baseline exists.

An orchestration-level evaluation timeout forces a full container rebuild before
Refine. This terminates non-database child processes that a database-only reset
cannot reliably reap. A normal completed validation failure may use the fast
path; its database sessions and schema changes are discarded by the restore.

## Reset-safe anomaly boundary

Keep every anomaly side effect inside the working database whenever the target
mechanism permits it. Row/data/index changes, ordinary database objects,
transactions, lock holders, and bounded database client sessions are resettable.
PostgreSQL role defaults are scrubbed by the reset.

Do not use `ALTER SYSTEM`, DBMS restart or shutdown, container commands,
`kill -9`, host filesystem writes, daemon installation, or host-level resource
stress. These effects escape the working-database snapshot and can contaminate
later turns or other workers.

Exploration actions within one reasoning phase may share state. Never rely on
that residue in final artifacts: authoritative `finish` validation starts from
the restored golden baseline and replays only the emitted files.
