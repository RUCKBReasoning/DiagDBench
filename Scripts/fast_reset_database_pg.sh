#!/bin/sh
set -euo pipefail

# =============================================================================
# Fast baseline snapshot / reset for PostgreSQL (container-local golden clone).
#
# Instead of DROP + CREATE + re-importing the multi-GB dump on every LLM turn
# (what initialize_database_pg.sh does), this script:
#
#   --mode snapshot : clone the CURRENT ready-state database (already imported +
#                     data-modification applied) into a golden template database
#                     using `CREATE DATABASE <golden> TEMPLATE <db>` -- a fast,
#                     file-level copy performed once per case.
#   --mode reset    : restore the working database to that golden baseline via
#                     `DROP DATABASE <db>; CREATE DATABASE <db> TEMPLATE <golden>`
#                     (seconds, no text re-parse), then scrub session/role-level
#                     residue (ALTER ROLE ... RESET ALL, terminate leftover
#                     background holder sessions). This yields a bit-for-bit
#                     identical pre-injection baseline every turn.
#
# The golden database lives INSIDE the per-worker container, so parallel workers
# never share or contend on it. On any failure this script exits non-zero and the
# Python caller falls back to the original full re-initialization path.
# =============================================================================

DB_HOST="127.0.0.1"
DB_PORT="5432"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"
DB_SCHEMA="diagnosis"

REMOTE_HIGH_PRV_USER="root"
REMOTE_HIGH_PRV_PASSWORD="root"

DB_NAME=""
GOLDEN_NAME=""
MODE=""

usage() {
    echo "Usage: $0 --mode <snapshot|reset> --database_name <dbname> --golden_name <golden> [--host <host>] [--port <port>] [--database_schema <schema>] [--user <user>] [--password <pwd>]"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --mode)            MODE="$2"; shift 2 ;;
        --host)            DB_HOST="$2"; shift 2 ;;
        --port)            DB_PORT="$2"; shift 2 ;;
        --database_name)   DB_NAME="$2"; shift 2 ;;
        --golden_name)     GOLDEN_NAME="$2"; shift 2 ;;
        --database_schema) DB_SCHEMA="$2"; shift 2 ;;
        --user)            DB_USER="$2"; shift 2 ;;
        --password)        DB_PASSWORD="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

[ -z "$MODE" ] && { echo "Error: --mode is required (snapshot|reset)."; usage; }
[ "$MODE" != "snapshot" ] && [ "$MODE" != "reset" ] && { echo "Error: invalid --mode '$MODE'."; usage; }
[ -z "$DB_NAME" ] && { echo "Error: --database_name is required."; usage; }
[ -z "$GOLDEN_NAME" ] && { echo "Error: --golden_name is required."; usage; }

# Admin connection (superuser) targets the maintenance database `postgres` so it
# is never attached to $DB_NAME / $GOLDEN_NAME while we drop or clone them.
ADMIN_OPTS="-h $DB_HOST -p $DB_PORT -U $REMOTE_HIGH_PRV_USER -d postgres"

admin_psql() {
    PGPASSWORD="$REMOTE_HIGH_PRV_PASSWORD" psql $ADMIN_OPTS -X -q "$@"
}

# Terminate every backend attached to the given database (except ourselves) so a
# DROP/CREATE ... TEMPLATE never fails with "database is being accessed by other
# users". This also reaps any leftover background holder sessions started by the
# previous turn's anomaly injection (FOR UPDATE holders, connection floods, ...).
terminate_backends() {
    target_db="$1"
    admin_psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$target_db' AND pid <> pg_backend_pid();" > /dev/null 2>&1 || true
}

# Robustly drop a database even when a previous turn's injection left runaway
# backends that keep RE-CONNECTING (the race that intermittently produced
# "database is being accessed by other users" / "database does not exist"):
#   1. ALTER DATABASE ... ALLOW_CONNECTIONS false blocks ALL new connections
#      (owner included), so terminated backends cannot immediately reconnect.
#   2. Retry terminate + `DROP DATABASE ... WITH (FORCE)` (PostgreSQL 13+ kills
#      any stragglers atomically as part of the drop).
#   3. Fall back to a plain terminate + DROP for older servers lacking FORCE.
# Every step tolerates a non-existent target (guarded by `|| true` / IF EXISTS).
drop_database_forcefully() {
    target_db="$1"
    admin_psql -c "ALTER DATABASE $target_db WITH ALLOW_CONNECTIONS false;" > /dev/null 2>&1 || true

    attempt=0
    while [ $attempt -lt 5 ]; do
        terminate_backends "$target_db"
        if admin_psql -c "DROP DATABASE IF EXISTS $target_db WITH (FORCE);" > /dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    # Last resort for servers without WITH (FORCE): terminate then plain DROP.
    terminate_backends "$target_db"
    admin_psql -c "DROP DATABASE IF EXISTS $target_db;" > /dev/null
}

if ! admin_psql -c "SELECT 1;" > /dev/null 2>&1; then
    echo "Error: cannot connect to PostgreSQL at $DB_HOST:$DB_PORT as $REMOTE_HIGH_PRV_USER."
    exit 1
fi

if [ "$MODE" = "snapshot" ]; then
    echo "-- [fast-reset] Creating golden baseline '$GOLDEN_NAME' from '$DB_NAME' ..."
    # Drop any stale golden robustly, then clone from the source. CREATE ...
    # TEMPLATE requires NO active sessions on the source db, so briefly block new
    # connections + terminate stragglers on the source during the copy, then
    # re-open it. (Snapshot runs before any injection, so the source is normally
    # already idle; this only hardens against leftover holders.)
    drop_database_forcefully "$GOLDEN_NAME"
    echo "-- [fast-reset] Blocking new source connections and terminating existing backends ..."
    admin_psql -c "ALTER DATABASE $DB_NAME WITH ALLOW_CONNECTIONS false;" > /dev/null 2>&1 || true
    terminate_backends "$DB_NAME"
    create_status=0
    echo "-- [fast-reset] Cloning PostgreSQL template '$DB_NAME' into '$GOLDEN_NAME' ..."
    admin_psql -c "CREATE DATABASE $GOLDEN_NAME TEMPLATE $DB_NAME OWNER $DB_USER;" > /dev/null || create_status=$?
    # Always re-open the source db regardless of the clone outcome.
    admin_psql -c "ALTER DATABASE $DB_NAME WITH ALLOW_CONNECTIONS true;" > /dev/null 2>&1 || true
    if [ "$create_status" -ne 0 ]; then
        echo "Error: failed to create golden baseline '$GOLDEN_NAME' from '$DB_NAME'."
        exit 1
    fi
    echo "✅ [fast-reset] Golden baseline '$GOLDEN_NAME' created."
    exit 0
fi

# MODE = reset
echo "-- [fast-reset] Restoring '$DB_NAME' from golden baseline '$GOLDEN_NAME' ..."

# Verify the golden template exists; if it does not, fail so the caller falls
# back to the full re-initialization path rather than silently doing nothing.
GOLDEN_EXISTS=$(admin_psql -tAc "SELECT 1 FROM pg_database WHERE datname='$GOLDEN_NAME';")
if [ "$GOLDEN_EXISTS" != "1" ]; then
    echo "Error: golden baseline '$GOLDEN_NAME' does not exist; cannot fast-reset."
    exit 1
fi

# 1) Scrub role-level GUC residue (e.g. ALTER ROLE ... SET enable_indexscan/
#    work_mem from a previous injection) BEFORE recreating the db. Role settings
#    live in pg_db_role_setting keyed by role and survive a database drop.
admin_psql -c "ALTER ROLE $DB_USER RESET ALL;" > /dev/null 2>&1 || true

# 2) Drop the working db robustly (blocks reconnects + WITH (FORCE) + retries so
#    runaway injection backends that keep re-connecting cannot wedge the DROP),
#    then re-clone it from the golden template -- a fast file-level copy.
drop_database_forcefully "$DB_NAME"
admin_psql -c "CREATE DATABASE $DB_NAME TEMPLATE $GOLDEN_NAME OWNER $DB_USER;" > /dev/null
# The freshly created db inherits datallowconn=true from the template, but be
# explicit in case the golden itself was ever marked no-connect.
admin_psql -c "ALTER DATABASE $DB_NAME WITH ALLOW_CONNECTIONS true;" > /dev/null 2>&1 || true

# 3) Re-apply database-level settings that TEMPLATE cloning does not carry over
#    (pg_db_role_setting entries are keyed by datid, so they are not copied).
admin_psql -c "ALTER DATABASE $DB_NAME SET search_path TO $DB_SCHEMA, public;" > /dev/null 2>&1 || true

echo "✅ [fast-reset] Database '$DB_NAME' restored to golden baseline."
exit 0
