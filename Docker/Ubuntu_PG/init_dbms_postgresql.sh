#!/bin/bash
set -Eeuo pipefail

REMOTE_IP="127.0.0.1"
REMOTE_PORT="5432"
DB_USER="diagnosis"
DB_PASSWORD="diagnosis"
PATH_VALUE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --remote_ip|-h) REMOTE_IP="$2"; shift 2 ;;
        --remote_port|-port) REMOTE_PORT="$2"; shift 2 ;;
        --user|-u) DB_USER="$2"; shift 2 ;;
        --password|-pwd) DB_PASSWORD="$2"; shift 2 ;;
        --path_value|-path) PATH_VALUE="$2"; shift 2 ;;
        --tmp_value|-tmp) shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$PATH_VALUE" ]]; then
    echo "Usage: $0 -path <PATH_VALUE>" >&2
    exit 1
fi

# docker_func.py invokes this entry point after docker run. The initialized
# cluster and fixed accounts are part of the image; only the daemon starts here.
if ! pg_isready -h "$REMOTE_IP" -p "$REMOTE_PORT" > /dev/null 2>&1; then
    rm -f /var/run/postgresql/.s.PGSQL.* /tmp/.s.PGSQL.* 2>/dev/null || true
    su -s /bin/bash postgres -c \
        "/usr/lib/postgresql/16/bin/pg_ctl -D /var/lib/postgresql/16/main -l /var/lib/postgresql/16/postgres.log start"
fi

deadline=$((SECONDS + 120))
until pg_isready -h "$REMOTE_IP" -p "$REMOTE_PORT" > /dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
        echo "PostgreSQL startup timed out after 120s" >&2
        exit 1
    fi
    sleep 1
done

PGPASSWORD=root psql -v ON_ERROR_STOP=1 -h "$REMOTE_IP" -p "$REMOTE_PORT" -U root -d postgres -c "SELECT 1" > /dev/null
PGPASSWORD="$DB_PASSWORD" psql -v ON_ERROR_STOP=1 -h "$REMOTE_IP" -p "$REMOTE_PORT" -U "$DB_USER" -d postgres -c "SELECT 1" > /dev/null
PGPASSWORD=monitor psql -v ON_ERROR_STOP=1 -h "$REMOTE_IP" -p "$REMOTE_PORT" -U monitor -d postgres -c "SELECT 1" > /dev/null

echo "PostgreSQL base image is ready."
