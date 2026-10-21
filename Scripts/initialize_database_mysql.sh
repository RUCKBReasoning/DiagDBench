#!/bin/bash
# set -eu
set -euo pipefail

# =============================================
# Default values (can be overridden by command-line arguments)
# =============================================
DB_HOST="127.0.0.1"
DB_PORT="3306"
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
    echo "  --host           Remote MySQL host (default: 127.0.0.1)"
    echo "  --port           Remote MySQL port (default: 3306)"
    echo "  --database_name  Target database name (required)"
    echo "  --database_type  'dump' or 'load_data' (required)"
    echo "  --database_schema  Ignored for MySQL; tables are imported into --database_name"
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

if [ -z "$DB_TYPE" ] || { [ "$DB_TYPE" != "dump" ] && [ "$DB_TYPE" != "load_data" ]; }; then
    echo "Error: Missing or invalid required argument '--database_type'. Must be 'dump' or 'load_data'."
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
export MYSQL_PWD="$DB_PASSWORD"
MYSQL_CLIENT_HOST="$DB_HOST"
if [ "$MYSQL_CLIENT_HOST" = "localhost" ]; then
    if MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysqladmin -h 127.0.0.1 -P "$DB_PORT" -u "$REMOTE_HIGH_PRV_USER" ping > /dev/null 2>&1; then
        MYSQL_CLIENT_HOST="127.0.0.1"
    fi
fi
CONN_OPTS="-h $MYSQL_CLIENT_HOST -P $DB_PORT -u $DB_USER"
HIGH_CONN_OPTS="-h $MYSQL_CLIENT_HOST -P $DB_PORT -u $REMOTE_HIGH_PRV_USER -D mysql"

wait_for_mysql_ready() {
    local opts="$1"
    local timeout="${2:-60}"
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        if mysqladmin $opts ping > /dev/null 2>&1 && mysql $opts -e "SELECT 1" > /dev/null 2>&1; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    echo "Error: MySQL is not ready after ${timeout}s." >&2
    mysqladmin $opts ping >&2 || true
    return 1
}

mysql_admin_scalar() {
    local expr="$1"
    MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysql $ADMIN_OPTS -N -B -e "SELECT @@GLOBAL.${expr};" | xargs
}

set_global_if_possible() {
    local param="$1"
    local value="$2"
    local err_output
    local cmd_exit

    set +e
    err_output=$(MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysql $ADMIN_OPTS -e "SET GLOBAL ${param} = ${value};" 2>&1)
    cmd_exit=$?
    set -e

    if [ "$cmd_exit" -ne 0 ]; then
        echo "   Warning: Failed to set ${param}=${value} - $(echo "$err_output" | tr '\n' ' ')" >&2
        return 1
    fi
    echo "   ${param}=${value}"
}

available_memory_bytes() {
    local mem_bytes
    if [ -f /sys/fs/cgroup/memory.max ]; then
        mem_bytes=$(cat /sys/fs/cgroup/memory.max)
        if [ "$mem_bytes" != "max" ]; then
            echo "$mem_bytes"
            return 0
        fi
    fi
    if [ -f /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
        mem_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        if [ "$mem_bytes" -lt 9223372036854771712 ]; then
            echo "$mem_bytes"
            return 0
        fi
    fi
    awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo
}

target_buffer_pool_size_bytes() {
    local mem_bytes
    local half_mem
    local one_gib=$((1024 * 1024 * 1024))
    local four_gib=$((4 * 1024 * 1024 * 1024))

    mem_bytes=$(available_memory_bytes)
    half_mem=$((mem_bytes / 2))
    if [ "$half_mem" -lt "$one_gib" ]; then
        echo "$half_mem"
    elif [ "$half_mem" -gt "$four_gib" ]; then
        echo "$four_gib"
    else
        echo "$half_mem"
    fi
}

restore_import_tuning() {
    if [ "${IMPORT_TUNING_APPLIED:-0}" != "1" ]; then
        return 0
    fi

    echo "-- Restoring MySQL import tuning parameters ..."
    export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"
    set_global_if_possible "innodb_flush_log_at_trx_commit" "$ORIGINAL_INNODB_FLUSH_LOG_AT_TRX_COMMIT" || true
    set_global_if_possible "sync_binlog" "$ORIGINAL_SYNC_BINLOG" || true
    set_global_if_possible "innodb_redo_log_capacity" "$ORIGINAL_INNODB_REDO_LOG_CAPACITY" || true
    set_global_if_possible "innodb_buffer_pool_size" "$ORIGINAL_INNODB_BUFFER_POOL_SIZE" || true
    set_global_if_possible "local_infile" "$ORIGINAL_LOCAL_INFILE" || true
    IMPORT_TUNING_APPLIED=0
    trap - EXIT
    export MYSQL_PWD="$DB_PASSWORD"
    return 0
}

apply_import_tuning() {
    echo "-- Applying temporary MySQL import tuning parameters ..."
    export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"

    ORIGINAL_INNODB_FLUSH_LOG_AT_TRX_COMMIT=$(mysql_admin_scalar "innodb_flush_log_at_trx_commit")
    ORIGINAL_SYNC_BINLOG=$(mysql_admin_scalar "sync_binlog")
    ORIGINAL_INNODB_REDO_LOG_CAPACITY=$(mysql_admin_scalar "innodb_redo_log_capacity")
    ORIGINAL_INNODB_BUFFER_POOL_SIZE=$(mysql_admin_scalar "innodb_buffer_pool_size")
    ORIGINAL_LOCAL_INFILE=$(mysql_admin_scalar "local_infile")
    TARGET_INNODB_BUFFER_POOL_SIZE=$(target_buffer_pool_size_bytes)
    IMPORT_TUNING_APPLIED=1
    trap restore_import_tuning EXIT

    set_global_if_possible "innodb_flush_log_at_trx_commit" "2" || true
    set_global_if_possible "sync_binlog" "0" || true
    set_global_if_possible "innodb_redo_log_capacity" "$((1024 * 1024 * 1024))" || true
    if [ "$TARGET_INNODB_BUFFER_POOL_SIZE" -gt "$ORIGINAL_INNODB_BUFFER_POOL_SIZE" ]; then
        set_global_if_possible "innodb_buffer_pool_size" "$TARGET_INNODB_BUFFER_POOL_SIZE" || true
    fi
    set_global_if_possible "local_infile" "ON" || true

    export MYSQL_PWD="$DB_PASSWORD"
    return 0
}

log_mysql_processlist() {
    echo "-- MySQL processlist snapshot for troubleshooting:" >&2
    MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysql $ADMIN_OPTS -e \
        "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 500) AS INFO_PREFIX FROM information_schema.PROCESSLIST ORDER BY TIME DESC;" >&2 || true
}

rewrite_mysql_source_to_database() {
    local source_file="$1"
    local db_name="$2"

    awk -v db_name="$db_name" '
        BEGIN { IGNORECASE = 1 }
        /^[[:space:]]*CREATE DATABASE/ {
            sub(/`[^`]+`/, "`" db_name "`")
            print
            next
        }
        /^[[:space:]]*USE[[:space:]]+`[^`]+`;[[:space:]]*$/ {
            print "USE `" db_name "`;"
            next
        }
        { print }
    ' "$source_file"
}

emit_mysql_import_stream() {
    local source_file="$1"
    local db_name="$2"

    printf "%s\n" "SET SESSION AUTOCOMMIT=0;"
    printf "%s\n" "SET SESSION UNIQUE_CHECKS=0;"
    printf "%s\n" "SET SESSION FOREIGN_KEY_CHECKS=0;"
    rewrite_mysql_source_to_database "$source_file" "$db_name"
    printf "%s\n" "COMMIT;"
    printf "%s\n" "SET SESSION FOREIGN_KEY_CHECKS=1;"
    printf "%s\n" "SET SESSION UNIQUE_CHECKS=1;"
    printf "%s\n" "SET SESSION AUTOCOMMIT=1;"
}

run_mysql_import() {
    local source_file="$1"
    local source_label="$2"
    local timeout_seconds="${MYSQL_IMPORT_TIMEOUT_SECONDS:-21600}"
    local cmd_exit

    echo "   Import timeout: ${timeout_seconds}s"
    set +e
    if command -v timeout > /dev/null 2>&1; then
        emit_mysql_import_stream "$source_file" "$DB_NAME" | timeout "$timeout_seconds" mysql $CONN_OPTS "$DB_NAME"
        pipeline_status=("${PIPESTATUS[@]}")
        if [ "${pipeline_status[0]}" -ne 0 ]; then
            cmd_exit="${pipeline_status[0]}"
        else
            cmd_exit="${pipeline_status[1]}"
        fi
    else
        emit_mysql_import_stream "$source_file" "$DB_NAME" | mysql $CONN_OPTS "$DB_NAME"
        pipeline_status=("${PIPESTATUS[@]}")
        if [ "${pipeline_status[0]}" -ne 0 ]; then
            cmd_exit="${pipeline_status[0]}"
        else
            cmd_exit="${pipeline_status[1]}"
        fi
    fi
    set -e

    if [ "$cmd_exit" -eq 124 ]; then
        echo "Error: ${source_label} import timed out after ${timeout_seconds}s." >&2
        log_mysql_processlist
        exit 1
    fi
    if [ "$cmd_exit" -ne 0 ]; then
        echo "Error: ${source_label} import failed with exit code ${cmd_exit}." >&2
        log_mysql_processlist
        exit 1
    fi
}

mysql_string_literal() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\'/\\\'}"
    printf "'%s'" "$value"
}

mysql_identifier() {
    local value="$1"
    value="${value//\`/\`\`}"
    printf "\`%s\`" "$value"
}

emit_mysql_load_data_stream() {
    local load_dir="$1"
    local manifest_file="$load_dir/manifest.tsv"
    local table_name
    local relative_file
    local row_count
    local data_file
    local table_identifier
    local data_literal
    local is_header=1

    printf "%s\n" "SET SESSION AUTOCOMMIT=0;"
    printf "%s\n" "SET SESSION UNIQUE_CHECKS=0;"
    printf "%s\n" "SET SESSION FOREIGN_KEY_CHECKS=0;"

    while IFS=$'\t' read -r table_name relative_file row_count; do
        if [ "$is_header" -eq 1 ]; then
            is_header=0
            continue
        fi
        if [ -z "$table_name" ] || [ -z "$relative_file" ]; then
            continue
        fi
        data_file="$load_dir/$relative_file"
        if [ ! -f "$data_file" ]; then
            echo "Error: LOAD DATA file not found: $data_file" >&2
            return 1
        fi
        table_identifier=$(mysql_identifier "$table_name")
        data_literal=$(mysql_string_literal "$(readlink -f "$data_file")")
        printf "LOAD DATA LOCAL INFILE %s INTO TABLE %s CHARACTER SET utf8mb4 FIELDS TERMINATED BY %s ESCAPED BY %s LINES TERMINATED BY %s;\n" \
            "$data_literal" "$table_identifier" "'\\t'" "'\\\\'" "'\\n'"
    done < "$manifest_file"

    printf "%s\n" "COMMIT;"
    printf "%s\n" "SET SESSION FOREIGN_KEY_CHECKS=1;"
    printf "%s\n" "SET SESSION UNIQUE_CHECKS=1;"
    printf "%s\n" "SET SESSION AUTOCOMMIT=1;"
}

run_mysql_load_data_files() {
    local load_dir="$1"
    local timeout_seconds="${MYSQL_IMPORT_TIMEOUT_SECONDS:-21600}"
    local cmd_exit

    echo "   LOAD DATA timeout: ${timeout_seconds}s"
    set +e
    if command -v timeout > /dev/null 2>&1; then
        emit_mysql_load_data_stream "$load_dir" | timeout "$timeout_seconds" mysql --local-infile=1 $CONN_OPTS "$DB_NAME"
        pipeline_status=("${PIPESTATUS[@]}")
        if [ "${pipeline_status[0]}" -ne 0 ]; then
            cmd_exit="${pipeline_status[0]}"
        else
            cmd_exit="${pipeline_status[1]}"
        fi
    else
        emit_mysql_load_data_stream "$load_dir" | mysql --local-infile=1 $CONN_OPTS "$DB_NAME"
        pipeline_status=("${PIPESTATUS[@]}")
        if [ "${pipeline_status[0]}" -ne 0 ]; then
            cmd_exit="${pipeline_status[0]}"
        else
            cmd_exit="${pipeline_status[1]}"
        fi
    fi
    set -e

    if [ "$cmd_exit" -eq 124 ]; then
        echo "Error: LOAD DATA import timed out after ${timeout_seconds}s." >&2
        log_mysql_processlist
        exit 1
    fi
    if [ "$cmd_exit" -ne 0 ]; then
        echo "Error: LOAD DATA import failed with exit code ${cmd_exit}." >&2
        log_mysql_processlist
        exit 1
    fi
}

run_load_data_instance() {
    local load_dir="$1"

    if [ ! -f "$load_dir/manifest.tsv" ] || [ ! -f "$load_dir/schema.sql" ]; then
        echo "Error: Invalid LOAD DATA instance directory: $load_dir" >&2
        exit 1
    fi

    echo "-- Importing LOAD DATA instance from $load_dir ..."
    echo "   Ensuring '$DB_NAME' privileges for user $DB_USER..."
    export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"
    mysql $ADMIN_OPTS \
        -e "GRANT ALL PRIVILEGES ON \`$DB_NAME\`.* TO '$DB_USER'@'%';" || true
    export MYSQL_PWD="$DB_PASSWORD"

    apply_import_tuning
    echo "   Loading schema ..."
    run_mysql_import "$load_dir/schema.sql" "LOAD DATA schema $load_dir/schema.sql"
    echo "   Loading table data via LOAD DATA LOCAL INFILE ..."
    run_mysql_load_data_files "$load_dir"
    if [ -s "$load_dir/post_load.sql" ]; then
        echo "   Building secondary indexes and foreign keys ..."
        run_mysql_import "$load_dir/post_load.sql" "LOAD DATA post-load DDL $load_dir/post_load.sql"
    fi
    restore_import_tuning
    echo "✅ LOAD DATA instance imported."
}

# =============================================
# 0. TERMINATE ACTIVE CONNECTIONS (Robust)
# =============================================
echo "-- Checking active connections to database '$DB_NAME' ..."

export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"

ADMIN_OPTS=$(echo "$HIGH_CONN_OPTS" | sed -E 's/(\s+-D\s+\S+)|(\s+--database=\S+)//g')

echo "Executing: mysql $ADMIN_OPTS -N -B -e \"SELECT count(*) FROM information_schema.PROCESSLIST WHERE DB = '$DB_NAME';\"" >&2

set +e
ACTIVE_CONN_OUTPUT=$(mysql $ADMIN_OPTS -N -B -e "SELECT count(*) FROM information_schema.PROCESSLIST WHERE DB = '$DB_NAME';" 2>&1)
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
    echo "❌ Failed to get active connection count (empty result). Please check if mysql database is accessible." >&2
    exit 1
fi

echo "🔢 Current active connections: $ACTIVE_CONN_COUNT"

echo "-- Terminating all active connections to database '$DB_NAME' ..."
mysql $ADMIN_OPTS -N -B -e "SELECT CONCAT('KILL ', ID, ';') FROM information_schema.PROCESSLIST WHERE DB = '$DB_NAME' AND ID <> CONNECTION_ID();" | mysql $ADMIN_OPTS > /dev/null 2>&1 || true

echo "✅ Active connections terminated."

export MYSQL_PWD="$DB_PASSWORD"

if ! mysql $CONN_OPTS -e "SELECT 1" > /dev/null 2>&1; then
    echo "Error: Cannot connect to remote MySQL at $DB_HOST:$DB_PORT as $DB_USER"
    exit 1
fi
echo "✅ Connected to remote MySQL."

# =============================================
# 1. CREATE DATABASE IF NOT EXISTS
# =============================================
DB_EXISTS=$(mysql -h "$MYSQL_CLIENT_HOST" -P "$DB_PORT" -u "$DB_USER" -N -B -e \
    "SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='$DB_NAME'")

if [ "$DB_EXISTS" == "1" ]; then
    mysql -h "$MYSQL_CLIENT_HOST" -P "$DB_PORT" -u "$DB_USER" \
        -e "DROP DATABASE \`$DB_NAME\`;"
    echo "✅ Existing database $DB_NAME dropped."
fi

mysql -h "$MYSQL_CLIENT_HOST" -P "$DB_PORT" -u "$DB_USER" \
    -e "CREATE DATABASE \`$DB_NAME\`;"
echo "✅ Database $DB_NAME created."


# =============================================
# 2. Apply configuration parameters from JSON
# =============================================
echo "-- Applying configuration parameters from Default Backup ..."

LOCAL_CONFIG_DIR="$TMP_DIR/Local_Instance"
DATA_DIR="/var/lib/mysql"
REMOTE_CONFIG_DIR="$TMP_DIR/RDS"
MYSQL_TMPDIR="$TMP_DIR/mysql_tmp"

MYSQL_CONF="/etc/mysql/mysql.conf.d/dbdiag.cnf"
MYSQL_CNF="/etc/mysql/my.cnf"


if [ "$DB_HOST" = "127.0.0.1" ] || [ "$DB_HOST" = "localhost" ]; then
    echo "Local instance detected. Copying configuration files from $LOCAL_CONFIG_DIR ..."

    mkdir -p "$LOCAL_CONFIG_DIR"
    mkdir -p "$REMOTE_CONFIG_DIR"
    mkdir -p "$MYSQL_TMPDIR"
    chmod 755 "$LOCAL_CONFIG_DIR" "$REMOTE_CONFIG_DIR"
    chmod 1777 "$MYSQL_TMPDIR"

    cp "$MYSQL_CONF" "$LOCAL_CONFIG_DIR/dbdiag.cnf.bak"
    cp "$MYSQL_CNF" "$LOCAL_CONFIG_DIR/my.cnf.bak"
    chmod 644 "$LOCAL_CONFIG_DIR/dbdiag.cnf.bak" "$LOCAL_CONFIG_DIR/my.cnf.bak"

    echo "✅ Local configuration files restored. Restart MySQL if needed."
    MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD" mysqladmin $ADMIN_OPTS shutdown > /dev/null 2>&1 || true
    sleep 3
    rm -f /var/run/mysqld/mysqld.sock /var/run/mysqld/mysqld.sock.lock \
          /var/run/mysqld/mysqlx.sock /var/run/mysqld/mysqlx.sock.lock \
          /var/run/mysqld/mysqld.pid 2>/dev/null || true
    mysqld --defaults-file="$MYSQL_CONF" --user=mysql --datadir="$DATA_DIR" --tmpdir="$MYSQL_TMPDIR" </dev/null >/dev/null 2>&1 &
    export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"
    wait_for_mysql_ready "$ADMIN_OPTS" 60
    mysql $ADMIN_OPTS -N -B -e "SHOW VARIABLES WHERE Variable_name IN ('tmpdir', 'innodb_tmpdir');" >&2 || true
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
        err_output=$(mysql $CONN_OPTS -e "SET PERSIST $param = '$value';" 2>&1)
        cmd_exit=$?
        set -e

        if [ $cmd_exit -ne 0 ]; then
            error_msg=$(echo "$err_output" | grep -i "error" | head -1 | sed -E 's/^ERROR[[:space:]]+[0-9]+[[:space:]]+\([^)]*\):[[:space:]]*//')
            if [ -z "$error_msg" ]; then
                error_msg="$err_output"
            fi
            echo "   Warning: Failed to set $param - $error_msg"
        fi
    done

    echo "✅ Configuration parameters applied. Reloading MySQL configuration..."

    MYSQL_PWD=$REMOTE_HIGH_PRV_PASSWORD mysqladmin $ADMIN_OPTS reload
    echo "✅ Configuration applied and reloaded."
fi

# =============================================
# 3. Recreate database (DROP + CREATE)
# =============================================
echo "-- Recreating database '$DB_NAME' ..."
export MYSQL_PWD="$DB_PASSWORD"
if ! mysql $CONN_OPTS -e "DROP DATABASE IF EXISTS \`$DB_NAME\`;" > /dev/null; then
    echo "Error: Failed to drop database '$DB_NAME'." >&2
    exit 1
fi
if ! mysql $CONN_OPTS -e "CREATE DATABASE \`$DB_NAME\`;" > /dev/null; then
    echo "Error: Failed to create database '$DB_NAME'." >&2
    exit 1
fi
echo "✅ Database '$DB_NAME' created."

echo "-- Preparing target database '$DB_NAME' (MySQL schema parameter is ignored) ..."
mysql $CONN_OPTS \
    -e "USE \`$DB_NAME\`;" > /dev/null 2>&1

echo "✅ Target database '$DB_NAME' is ready for import."

# =============================================
# 4. DATA IMPORT
# =============================================
cd "$INSTANCE_DIR" || { echo "Error: Cannot cd into $INSTANCE_DIR"; exit 1; }

if [ "$DB_TYPE" = "load_data" ] || { [ "$DB_TYPE" = "dump" ] && [ -f "${DB_NAME}.load/manifest.tsv" ]; }; then
    LOAD_DIR="${DB_NAME}.load"
    run_load_data_instance "$LOAD_DIR"
elif [ "$DB_TYPE" = "dump" ]; then
    DUMP_FILE="${DB_NAME}.dump"
    if [ -f "$DUMP_FILE" ]; then
        echo "-- Importing dump file $DUMP_FILE using mysql ..."

        echo "   Ensuring '$DB_NAME' privileges for user $DB_USER..."
        export MYSQL_PWD="$REMOTE_HIGH_PRV_PASSWORD"
        mysql $ADMIN_OPTS \
            -e "GRANT ALL PRIVILEGES ON \`$DB_NAME\`.* TO '$DB_USER'@'%';" || true
        export MYSQL_PWD="$DB_PASSWORD"
        apply_import_tuning

        echo "   Rewriting any dump database declaration to target database '$DB_NAME' during import..."
        run_mysql_import "$DUMP_FILE" "Dump file $DUMP_FILE"
        restore_import_tuning
        echo "✅ Dump imported."
    else
        SQL_FILE="${DB_NAME}.sql"
        if [ -f "$SQL_FILE" ]; then
            echo "-- Importing SQL file $SQL_FILE using mysql ..."
            apply_import_tuning
            echo "   Rewriting any SQL database declaration to target database '$DB_NAME' during import..."
            run_mysql_import "$SQL_FILE" "SQL file $SQL_FILE"
            restore_import_tuning
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

echo "✅ MySQL initialization completed successfully."
