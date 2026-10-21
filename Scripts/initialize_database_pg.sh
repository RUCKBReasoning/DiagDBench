#!/bin/sh
# set -eu
set -euo pipefail

# =============================================
# Default values (can be overridden by command-line arguments)
# =============================================
DB_HOST="127.0.0.1"
DB_PORT="5432"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"
DB_SCHEMA="diagnosis"

REMOTE_HIGH_PRV_USER="root"
REMOTE_HIGH_PRV_PASSWORD="root"

DB_NAME=""
INSTANCE_DIR=""
TMP_DIR=""
DB_TYPE="dump"

# =============================================
# Input Parameters and Usage
# =============================================
usage() {
    echo "Usage: $0 [--host <host>] [--port <port>] --database_name <dbname> --database_type <type> [--database_schema <schema>] --instance_dir <instance_dir> --tmp_dir <tmp_dir> [--user <user>] [--password <pwd>]"
    echo "  --host           Remote PostgreSQL host (default: 127.0.0.1)"
    echo "  --port           Remote PostgreSQL port (default: 5432)"
    echo "  --database_name  Target database name (required)"
    echo "  --database_type  only 'dump' file is allowed for now (required)"
    echo "  --database_schema  Target database schema (default: diagnosis)"
    echo "  --instance_dir   Local directory containing database instance (required)"
    echo "  --tmp_dir        Temporary directory for configuration files (required)"
    echo "  --user           Database user (default: diagnosis)"
    echo "  --password       Database password (default: diagnosis)"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --host)
            DB_HOST="$2"
            shift 2
            ;;
        --port)
            DB_PORT="$2"
            shift 2
            ;;
        --database_name)
            DB_NAME="$2"
            shift 2
            ;;
        --database_type)
            DB_TYPE="$2"
            shift 2
            ;;
        --database_schema)
            DB_SCHEMA="$2"
            shift 2
            ;;
        --instance_dir)
            INSTANCE_DIR="$2"
            shift 2
            ;;
        --tmp_dir)
            TMP_DIR="$2"
            shift 2
            ;;
        --user)
            DB_USER="$2"
            shift 2
            ;;
        --password)
            DB_PASSWORD="$2"
            shift 2
            ;;
        
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

if [ -z "$DB_NAME" ] ; then
    echo "Error: Missing required argument '--database_name'."
    usage
fi

if [ -z "$DB_TYPE" ] || { [ "$DB_TYPE" != "dump" ]; }; then
    echo "Error: Missing or invalid required argument '--database_type'. Must be 'dump' for now."
    usage
fi

if [ -z "$INSTANCE_DIR" ] ; then
    echo "Error: Missing required argument '--instance_dir'."
    usage
fi

if [ -z "$TMP_DIR" ] ; then
    echo "Error: Missing required argument '--tmp_dir'."
    usage
fi

# =============================================
# Connection options
# =============================================
export PGPASSWORD="$DB_PASSWORD"
CONN_OPTS="-h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_USER"
HIGH_CONN_OPTS="-h $DB_HOST -p $DB_PORT -U $REMOTE_HIGH_PRV_USER -d $REMOTE_HIGH_PRV_USER"

# =============================================
# 0. TERMINATE ACTIVE CONNECTIONS (Robust)
# =============================================
echo "-- Checking active connections to database '$DB_NAME' ..."

export PGPASSWORD="$REMOTE_HIGH_PRV_PASSWORD"

ADMIN_OPTS=$(echo "$HIGH_CONN_OPTS" | sed -E 's/(\s+-d\s+\S+)|(\s+--dbname=\S+)//g')" -d postgres"

echo "Executing: psql $ADMIN_OPTS -t -c \"SELECT count(*) FROM pg_stat_activity WHERE datname = '$DB_NAME';\"" >&2

set +e
ACTIVE_CONN_OUTPUT=$(psql $ADMIN_OPTS -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname = '$DB_NAME';" 2>&1)
STATUS=$?
set -e

if [ $STATUS -ne 0 ]; then
    echo "❌ Failed to execute query for active connection count." >&2
    echo "Error message:" >&2
    echo "$ACTIVE_CONN_OUTPUT" >&2
    exit 1
fi

ACTIVE_CONN_COUNT=$(echo "$ACTIVE_CONN_OUTPUT" | xargs)

if [ -z "$ACTIVE_CONN_COUNT" ]; then
    echo "❌ Failed to get active connection count (empty result). Please check if postgres database is accessible." >&2
    exit 1
fi

echo "🔢 Current active connections: $ACTIVE_CONN_COUNT"

echo "-- Terminating all active connections to database '$DB_NAME' ..."
psql $ADMIN_OPTS -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();" > /dev/null 2>&1 || true

echo "✅ Active connections terminated."

export PGPASSWORD="$DB_PASSWORD"

if ! psql $CONN_OPTS -c "SELECT 1" > /dev/null 2>&1; then
    echo "Error: Cannot connect to remote PostgreSQL at $DB_HOST:$DB_PORT as $DB_USER"
    exit 1
fi
echo "✅ Connected to remote PostgreSQL."

# =============================================
# 1. CREATE DATABASE IF NOT EXISTS
# =============================================
DB_EXISTS=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'")

if [ "$DB_EXISTS" == "1" ]; then
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
        -c "DROP DATABASE $DB_NAME;"
    echo "✅ Existing database $DB_NAME dropped."
fi

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
    -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;"
echo "✅ Database $DB_NAME created."


# =============================================
# 2. Apply configuration parameters from JSON
# =============================================
echo "-- Applying configuration parameters from Default Backup ..."

LOCAL_CONFIG_DIR="$TMP_DIR/Local_Instance"
DATA_DIR="/var/lib/postgresql/16/main"
REMOTE_CONFIG_DIR="$TMP_DIR/RDS"

PG_CONF="$DATA_DIR/postgresql.conf"
PG_HBA="$DATA_DIR/pg_hba.conf"


if [ "$DB_HOST" = "127.0.0.1" ] || [ "$DB_HOST" = "localhost" ]; then
    echo "Local instance detected. Copying configuration files from $LOCAL_CONFIG_DIR ..."

    mkdir -p "$LOCAL_CONFIG_DIR"
    mkdir -p "$REMOTE_CONFIG_DIR"
    chmod 755 "$LOCAL_CONFIG_DIR" "$REMOTE_CONFIG_DIR"
    

    cp "$PG_CONF" "$LOCAL_CONFIG_DIR/postgresql.conf.bak"
    cp "$PG_HBA" "$LOCAL_CONFIG_DIR/pg_hba.conf.bak"
    chmod 644 "$LOCAL_CONFIG_DIR/postgresql.conf.bak" "$LOCAL_CONFIG_DIR/pg_hba.conf.bak"
    
    echo "✅ Local configuration files restored. Restart PostgreSQL if needed."
    su - postgres -c "/usr/lib/postgresql/16/bin/pg_ctl -D $DATA_DIR restart"
    sleep 3
else
    echo "REMOTE CONNECTION needs to support initialization via remote superuser"

    CONFIG_JSON="$REMOTE_CONFIG_DIR/configurations.json"   

    if ! command -v jq > /dev/null 2>&1; then
        echo "Error: 'jq' is required to parse JSON config."
        exit 1
    fi

    jq -r '.[] | "\(.name) \(.setting)"' "$CONFIG_JSON" | while read -r param value; do
        if [ -z "$value" ] || [ "$value" = "null" ]; then
            echo "   Skipping $param (no valid setting)"
            continue
        fi
        echo "   Setting $param = $value"

        set +e
        err_output=$(psql $CONN_OPTS -c "ALTER SYSTEM SET $param TO '$value';" 2>&1)
        cmd_exit=$?
        set -e

        if [ $cmd_exit -ne 0 ]; then
            error_msg=$(echo "$err_output" | grep -i "error:" | head -1 | sed 's/^ERROR: //')
            if [ -z "$error_msg" ]; then
                error_msg="$err_output"
            fi
            echo "   Warning: Failed to set $param - $error_msg"
        fi
    done

    echo "✅ Configuration parameters applied. Reloading PostgreSQL configuration..."

    PGPASSWORD=$REMOTE_HIGH_PRV_PASSWORD psql $HIGH_CONN_OPTS -c "SELECT pg_reload_conf();"
    echo "✅ Configuration applied and reloaded."
fi

# =============================================
# 3. Recreate database (DROP + CREATE)
# =============================================
echo "-- Recreating database '$DB_NAME' ..."
psql $CONN_OPTS -c "DROP DATABASE IF EXISTS $DB_NAME;" > /dev/null 2>&1
psql $CONN_OPTS -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;" > /dev/null 2>&1
echo "✅ Database '$DB_NAME' created."

echo "-- Creating schema '$DB_SCHEMA' and setting default search_path ..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA AUTHORIZATION $DB_USER;" > /dev/null 2>&1

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -c "ALTER DATABASE $DB_NAME SET search_path TO $DB_SCHEMA, public;" > /dev/null 2>&1

echo "✅ Schema '$DB_SCHEMA' created (Owner: $DB_USER) and search_path configured."

# =============================================
# 4. DATA IMPORT
# =============================================
cd "$INSTANCE_DIR" || { echo "Error: Cannot cd into $INSTANCE_DIR"; exit 1; }

if [ "$DB_TYPE" = "dump" ]; then
    DUMP_FILE="${DB_NAME}.dump"
    if [ -f "$DUMP_FILE" ]; then
        echo "-- Importing dump file $DUMP_FILE using psql ..."

        echo "   Ensuring '$DB_SCHEMA' schema ownership and privileges for user $DB_USER..."
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
            -c "ALTER SCHEMA $DB_SCHEMA OWNER TO \"$DB_USER\";" || true
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
            -c "GRANT ALL ON SCHEMA $DB_SCHEMA TO \"$DB_USER\";" || true

        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" < "$DUMP_FILE"
        echo "✅ Dump imported."
    else
        SQL_FILE="${DB_NAME}.sql"
        if [ -f "$SQL_FILE" ]; then
            echo "-- Importing SQL file $SQL_FILE using psql ..."
            psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$SQL_FILE"
            echo "✅ SQL file imported."
        else
            echo "Error: No dump or SQL file found for database $DB_NAME (expected ${DB_NAME}.dump or ${DB_NAME}.sql)"
            exit 1
        fi
    fi
    
else
    echo "Error: Invalid database type '$DB_TYPE'. Use 'dump' for now."
    exit 1
fi

echo "✅ PostgreSQL initialization completed successfully."
