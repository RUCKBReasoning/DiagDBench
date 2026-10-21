#!/bin/bash
set -euo pipefail

# =============================================================================
# Fast baseline snapshot / reset for MySQL (container-local golden clone).
#
# MySQL has no `CREATE DATABASE ... TEMPLATE`, so the fast analog of PostgreSQL's
# template clone is an ENGINE-INTERNAL table copy: `CREATE TABLE LIKE` +
# `INSERT ... SELECT`. Data stays inside InnoDB and never round-trips through the
# client text parser, so this is dramatically cheaper than re-running LOAD DATA /
# re-sourcing the dump (what initialize_database_mysql.sh does) on every turn.
#
#   --mode snapshot : clone the CURRENT ready-state schema (imported +
#                     data-modification applied) into a golden schema, once.
#   --mode reset    : DROP the working schema, recreate it, and clone every table
#                     back from the golden schema; then scrub GLOBAL/session
#                     residue and kill leftover background holder sessions so the
#                     next turn starts from an identical pre-injection baseline.
#
# The golden schema lives INSIDE the per-worker container -> no cross-worker
# contention. Views are unsupported by this fast path; if any exist the script
# exits non-zero so the Python caller falls back to full re-initialization.
# =============================================================================

DB_HOST="127.0.0.1"
DB_PORT="3306"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"

REMOTE_HIGH_PRV_USER="root"
REMOTE_HIGH_PRV_PASSWORD="root"

DB_NAME=""
GOLDEN_NAME=""
MODE=""

usage() {
    echo "Usage: $0 --mode <snapshot|reset> --database_name <dbname> --golden_name <golden> [--host <host>] [--port <port>] [--user <user>] [--password <pwd>]"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --mode)          MODE="$2"; shift 2 ;;
        --host)          DB_HOST="$2"; shift 2 ;;
        --port)          DB_PORT="$2"; shift 2 ;;
        --database_name) DB_NAME="$2"; shift 2 ;;
        --golden_name)   GOLDEN_NAME="$2"; shift 2 ;;
        --database_schema) shift 2 ;;   # accepted for arg parity; ignored for MySQL
        --user)          DB_USER="$2"; shift 2 ;;
        --password)      DB_PASSWORD="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

[ -z "$MODE" ] && { echo "Error: --mode is required (snapshot|reset)."; usage; }
[ "$MODE" != "snapshot" ] && [ "$MODE" != "reset" ] && { echo "Error: invalid --mode '$MODE'."; usage; }
[ -z "$DB_NAME" ] && { echo "Error: --database_name is required."; usage; }
[ -z "$GOLDEN_NAME" ] && { echo "Error: --golden_name is required."; usage; }

MYSQL_CLIENT_HOST="$DB_HOST"
if [ "$MYSQL_CLIENT_HOST" = "localhost" ]; then
    MYSQL_CLIENT_HOST="127.0.0.1"
fi
ADMIN_OPTS="-h $MYSQL_CLIENT_HOST -P $DB_PORT -u $REMOTE_HIGH_PRV_USER"

admin_mysql() {
    MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysql $ADMIN_OPTS "$@"
}

# Backtick-quote an identifier (defensive against schema/table names with quirks).
qident() {
    printf '`%s`' "${1//\`/\`\`}"
}

if ! admin_mysql -e "SELECT 1;" > /dev/null 2>&1; then
    echo "Error: cannot connect to MySQL at $MYSQL_CLIENT_HOST:$DB_PORT as $REMOTE_HIGH_PRV_USER."
    exit 1
fi

# Reap leftover, non-system sessions on a schema (background holders / floods).
# Looped a few times because an injection flood may keep RE-CONNECTING: a single
# pass can miss sessions that reconnect right after being killed. Also kills
# sessions opened by the working user that are not currently bound to the schema
# (e.g. connected with no default DB), which a plain `DB = '$target_db'` filter
# would otherwise leave holding connection slots.
kill_schema_sessions() {
    target_db="$1"
    pass=0
    while [ $pass -lt 5 ]; do
        kill_stmts=$(admin_mysql -N -B -e "
            SELECT GROUP_CONCAT(CONCAT('KILL ', id, ';') SEPARATOR ' ')
            FROM information_schema.PROCESSLIST
            WHERE (DB = '$target_db' OR (USER = '$DB_USER' AND COMMAND <> 'Sleep'))
              AND ID <> CONNECTION_ID();" 2>/dev/null)
        if [ -z "$kill_stmts" ] || [ "$kill_stmts" = "NULL" ]; then
            return 0
        fi
        admin_mysql -e "$kill_stmts" > /dev/null 2>&1 || true
        pass=$((pass + 1))
        sleep 1
    done
}

# DROP a schema robustly, reaping reconnecting holder sessions between attempts
# so a leftover injection flood cannot keep the schema busy indefinitely.
drop_schema_forcefully() {
    target_db="$1"
    attempt=0
    while [ $attempt -lt 5 ]; do
        kill_schema_sessions "$target_db"
        if admin_mysql -e "DROP DATABASE IF EXISTS $(qident "$target_db");" > /dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    # Final attempt surfaces the real error (and aborts under set -e) so the
    # Python caller falls back to a full re-initialization.
    admin_mysql -e "DROP DATABASE IF EXISTS $(qident "$target_db");"
}

# Abort the fast path if the source schema contains views (CREATE TABLE LIKE
# cannot reproduce them); the caller then does a full re-init instead.
assert_no_views() {
    src="$1"
    view_count=$(admin_mysql -N -B -e "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '$src';")
    if [ "${view_count:-0}" -ne 0 ]; then
        echo "Error: schema '$src' contains $view_count view(s); fast path unsupported."
        exit 1
    fi
}

# Clone every base table from $1 (source schema) into $2 (destination schema),
# using engine-internal copies. Destination schema must already exist and be
# empty. Each table is committed independently and emits progress so the outer
# synthesis watchdog can distinguish a large, active copy from a stalled step.
# FOREIGN_KEY_CHECKS is disabled so parent/child insertion order is free.
clone_tables() {
    src="$1"
    dst="$2"

    mapfile -t tables < <(admin_mysql -N -B -e "
        SELECT TABLE_NAME FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = '$src' AND TABLE_TYPE = 'BASE TABLE';")

    total="${#tables[@]}"
    if [ "$total" -eq 0 ]; then
        echo "Error: source schema '$src' has no base tables to clone." >&2
        return 1
    fi

    index=0
    for tbl in "${tables[@]}"; do
        [ -z "$tbl" ] && continue
        index=$((index + 1))
        sq=$(qident "$src")
        dq=$(qident "$dst")
        tq=$(qident "$tbl")
        echo "-- [fast-reset] Cloning table ${index}/${total} '${tbl}' from '${src}' to '${dst}' ..."
        admin_mysql <<SQL
SET SESSION FOREIGN_KEY_CHECKS=0;
SET SESSION UNIQUE_CHECKS=0;
CREATE TABLE ${dq}.${tq} LIKE ${sq}.${tq};
INSERT INTO ${dq}.${tq} SELECT * FROM ${sq}.${tq};
SQL
        echo "-- [fast-reset] Completed table ${index}/${total} '${tbl}'."
    done
}

if [ "$MODE" = "snapshot" ]; then
    echo "-- [fast-reset] Creating golden baseline '$GOLDEN_NAME' from '$DB_NAME' ..."
    assert_no_views "$DB_NAME"
    drop_schema_forcefully "$GOLDEN_NAME"
    admin_mysql -e "CREATE DATABASE $(qident "$GOLDEN_NAME");"
    clone_tables "$DB_NAME" "$GOLDEN_NAME"
    echo "✅ [fast-reset] Golden baseline '$GOLDEN_NAME' created."
    exit 0
fi

# MODE = reset
echo "-- [fast-reset] Restoring '$DB_NAME' from golden baseline '$GOLDEN_NAME' ..."

GOLDEN_EXISTS=$(admin_mysql -N -B -e "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$GOLDEN_NAME';")
if [ "${GOLDEN_EXISTS:-0}" -eq 0 ]; then
    echo "Error: golden baseline '$GOLDEN_NAME' does not exist; cannot fast-reset."
    exit 1
fi

# 1) Reap leftover holder sessions on the working schema (looped + retried DROP so
#    a reconnecting injection flood cannot wedge it), then recreate it.
drop_schema_forcefully "$DB_NAME"
admin_mysql -e "CREATE DATABASE $(qident "$DB_NAME");"
# Restore the working user's privileges on the freshly recreated schema.
admin_mysql -e "GRANT ALL PRIVILEGES ON $(qident "$DB_NAME").* TO '$DB_USER'@'%';" 2>/dev/null || true
admin_mysql -e "FLUSH PRIVILEGES;" 2>/dev/null || true

# 2) Clone all tables back from the golden baseline (engine-internal copy).
clone_tables "$GOLDEN_NAME" "$DB_NAME"

echo "✅ [fast-reset] Database '$DB_NAME' restored to golden baseline."
exit 0
