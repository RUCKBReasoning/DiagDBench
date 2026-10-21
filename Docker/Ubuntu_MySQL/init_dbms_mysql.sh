#!/bin/bash
set -Eeuo pipefail

REMOTE_IP="127.0.0.1"
REMOTE_PORT="3306"
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
# datadir, static configuration, and fixed service accounts are in the image.
if ! MYSQL_PWD=root mysqladmin -h "$REMOTE_IP" -P "$REMOTE_PORT" -uroot ping --silent > /dev/null 2>&1; then
    rm -f /var/run/mysqld/mysqld.sock /var/run/mysqld/mysqld.sock.lock \
        /var/run/mysqld/mysqlx.sock /var/run/mysqld/mysqlx.sock.lock \
        /var/run/mysqld/mysqld.pid 2>/dev/null || true
    mysqld --defaults-file=/etc/mysql/mysql.conf.d/dbdiag.cnf \
        --user=mysql --datadir=/var/lib/mysql --daemonize
fi

deadline=$((SECONDS + 120))
until MYSQL_PWD=root mysqladmin -h "$REMOTE_IP" -P "$REMOTE_PORT" -uroot ping --silent > /dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
        echo "MySQL startup timed out after 120s" >&2
        exit 1
    fi
    sleep 1
done

MYSQL_PWD=root mysql -h "$REMOTE_IP" -P "$REMOTE_PORT" -uroot -e "SELECT 1" > /dev/null
MYSQL_PWD="$DB_PASSWORD" mysql -h "$REMOTE_IP" -P "$REMOTE_PORT" -u"$DB_USER" -e "SELECT 1" > /dev/null
MYSQL_PWD=monitor mysql -h "$REMOTE_IP" -P "$REMOTE_PORT" -umonitor -e "SELECT 1" > /dev/null

echo "MySQL base image is ready."
