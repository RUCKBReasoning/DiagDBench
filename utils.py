import json
import os
import time
import logging
import re
import subprocess
from datetime import datetime

try:
    import sqlparse
except ImportError:
    sqlparse = None

try:
    import psycopg2
    from psycopg2 import sql as pg_sql
except ImportError:
    psycopg2 = None
    pg_sql = None

try:
    import mysql.connector as mysql_connector
except ImportError:
    mysql_connector = None

logger = logging.getLogger(f"DB_DIAG.{__name__}")

# Connection-establishment timeout (seconds) for all psycopg2 connects below.
# Bounds the "container half-dead / DB unreachable" case so schema extraction
# and related sub-flows fail fast instead of blocking forever.
DB_CONNECT_TIMEOUT = 30
MYSQL_SCHEMA_PROMPT_VALUE = "[MySQL does not involve this definition]"


def normalize_dbms(dbms=None):
    dbms_name = str(dbms or "").strip().lower()
    if "mysql" in dbms_name:
        return "mysql"
    return "pg"


def resolve_db_type(config, dbms=None):
    dbms_kind = normalize_dbms(dbms or config.get("dbms"))
    default_db_type = "load_data" if dbms_kind == "mysql" else "dump"
    db_type = config.get("db_type", config.get("database_type", default_db_type))
    db_type = str(db_type or default_db_type).strip().lower()
    if db_type not in {"dump", "load_data"}:
        raise ValueError(f"Unsupported db_type `{db_type}`. Expected `dump` or `load_data`.")
    return db_type


def effective_schema_name(db_config, dbms=None):
    if normalize_dbms(dbms or db_config.get("dbms")) == "mysql":
        return db_config.get("schema") or db_config.get("db_name")
    return db_config.get("schema", "diagnosis")


def current_time_str() :
    current_time = datetime.now()
    formatted_time = current_time.strftime('%Y%m%d%H%M%S')

    return formatted_time[2:]

def obtain_output_dir(abs_file_path, base_dir="Output"):
    current_file_dir = os.path.dirname(os.path.abspath(abs_file_path))
    current_file_name = os.path.basename(os.path.abspath(abs_file_path)).split('.')[0]
    output_dir = os.path.join(current_file_dir, base_dir, f"{current_file_name}_{current_time_str()}")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    return output_dir

def set_up_logger(log_path, log_file_level=logging.DEBUG) :
    log_dir = os.path.dirname(log_path)
    if not os.path.exists(log_dir) : os.makedirs(log_dir)
    
    logger = logging.getLogger(f"DB_DIAG")
    logger.setLevel(log_file_level)

    if logger.hasHandlers():
        logger.handlers.clear()
    logger.propagate = False
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_path, mode='w')
    file_handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


    logger = logging.getLogger(f"DB_DIAG.{__name__}")
    logger.info("[UTILS] SET UP LOGGER SUCCESSFULLY.")

    return logger

def extract_sql_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    result = []
    i = 0
    n = len(sql)

    in_single_quote = False
    in_double_quote = False
    in_block_comment = False

    while i < n:
        if not in_single_quote and not in_double_quote:
            if not in_block_comment and sql[i:i+2] == '/*':
                in_block_comment = True
                i += 2
                continue
            if in_block_comment and sql[i:i+2] == '*/':
                in_block_comment = False
                i += 2
                continue

        if not in_single_quote and not in_double_quote and not in_block_comment:
            if sql[i:i+2] == '--':
                while i < n and sql[i] != '\n':
                    i += 1
                continue

        if in_block_comment:
            i += 1
            continue

        if sql[i] == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif sql[i] == '"' and not in_single_quote:
            in_double_quote = not in_double_quote

        result.append(sql[i])
        i += 1

    lines = [line.rstrip() for line in ''.join(result).splitlines() if line.strip()]

    return '\n'.join(lines)

def get_table_names_from_db(db_config, dbms=None):
    dbms_kind = normalize_dbms(dbms or db_config.get("dbms"))
    schema = effective_schema_name(db_config, dbms_kind)
    query = (
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %s AND table_type = 'BASE TABLE';"
    )
    if dbms_kind == "mysql":
        results = mysql_execute_sql(query, db_config, (schema,))
    else:
        results = pg_execute_sql(query, db_config, (schema,))

    if results is not None:
        table_names = [res[0] for res in results]
    else:
        table_names = []
    
    return table_names


def consume_cursor_results(cursor, conn=None):
    if cursor is None:
        return

    while True:
        try:
            if getattr(cursor, "with_rows", False) or cursor.description is not None:
                cursor.fetchall()
        except Exception:
            if conn is not None and hasattr(conn, "consume_results"):
                try:
                    conn.consume_results()
                except Exception:
                    pass

        nextset = getattr(cursor, "nextset", None)
        if not callable(nextset):
            break
        try:
            if not nextset():
                break
        except Exception:
            break


def close_cursor_safely(cursor, conn=None, logger_obj=None):
    if cursor is None:
        return

    try:
        consume_cursor_results(cursor, conn)
    except Exception as exc:
        if logger_obj is not None:
            logger_obj.debug("[UTILS] Ignored error while consuming cursor results before close: %s", exc)

    try:
        cursor.close()
    except Exception as exc:
        if conn is not None and hasattr(conn, "consume_results"):
            try:
                conn.consume_results()
                cursor.close()
                return
            except Exception:
                pass
        if logger_obj is not None:
            logger_obj.warning("[UTILS] Ignored error while closing cursor: %s", exc)


def pg_execute_sql(sql_text, db_config, params=None):
    if psycopg2 is None:
        raise ImportError("psycopg2 is required for PostgreSQL SQL execution.")
    # print(db_config['host'], db_config['port'], db_config['user'], db_config['password'], db_config['db_name'])
    conn = psycopg2.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port'],
        dbname=db_config['db_name'],
        connect_timeout=DB_CONNECT_TIMEOUT
    )

    results = None
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout TO 360000;")
        cur.execute(sql_text, params)

        if cur.description is not None:
            results = cur.fetchall()
        else:
            results = None  
            conn.commit() 

    except psycopg2.Error as e:
        print(f"[ERROR OCCURRED] {e}")
        conn.rollback() 
        # exit()

    finally:
        close_cursor_safely(cur, conn, logger)
        conn.close()

    return results


def mysql_execute_sql(sql_text, db_config, params=None):
    if mysql_connector is None:
        raise ImportError("mysql-connector-python is required for MySQL SQL execution.")

    conn = mysql_connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        port=int(db_config['port']),
        database=db_config.get('db_name'),
        connection_timeout=DB_CONNECT_TIMEOUT,
        use_pure=True
    )

    results = None
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql_text, params)

        if cur.description is not None:
            results = cur.fetchall()
        else:
            results = None
            conn.commit()

    except mysql_connector.Error as e:
        print(f"[ERROR OCCURRED] {e}")
        conn.rollback()

    finally:
        if cur:
            close_cursor_safely(cur, conn, logger)
        conn.close()

    return results


def execute_sql(sql_text, db_config, params=None):
    if normalize_dbms(db_config.get("dbms")) == "mysql":
        return mysql_execute_sql(sql_text, db_config, params)
    return pg_execute_sql(sql_text, db_config, params)


def check_database_non_empty(db_config, dbms=None):
    dbms_kind = normalize_dbms(dbms or db_config.get("dbms"))
    if dbms_kind == "mysql":
        return mysql_check_database_non_empty(db_config)
    return pg_check_database_non_empty(db_config)


def pg_check_database_non_empty(db_config):
    if psycopg2 is None:
        raise ImportError("psycopg2 is required for PostgreSQL database checks.")
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            dbname='postgres',
            connect_timeout=DB_CONNECT_TIMEOUT
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s;",
            (db_config['db_name'],)
        )
        exists = cur.fetchone() is not None

        close_cursor_safely(cur, conn, logger)
        conn.close()

        if not exists:
            return False, False, f"Database '{db_config['db_name']}' does not exist."

    except Exception as e:
        return False, False, f"Failed to check database existence: {e}"

    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            dbname=db_config['db_name'],
            connect_timeout=DB_CONNECT_TIMEOUT
        )
        conn.autocommit = True
        cur = conn.cursor()

        schema = db_config['schema']
        
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE';
            """,
            (schema,)
        )
        tables = cur.fetchall()

        if not tables:
            close_cursor_safely(cur, conn, logger)
            conn.close()
            return True, False, f"Database exists but schema '{schema}' has no tables."

        non_empty = False

        for (table_name,) in tables:
            query = pg_sql.SQL("SELECT EXISTS (SELECT 1 FROM {}.{} LIMIT 1);").format(
                pg_sql.Identifier(schema),
                pg_sql.Identifier(table_name)
            )
            cur.execute(query)
            if cur.fetchone()[0]:
                non_empty = True
                break

        close_cursor_safely(cur, conn, logger)
        conn.close()

        if non_empty:
            return True, True, "Database exists and is non-empty."
        else:
            return True, False, "Database exists but all tables are empty."

    except Exception as e:
        return True, False, f"Failed to check table data: {e}"


def mysql_check_database_non_empty(db_config):
    if mysql_connector is None:
        raise ImportError("mysql-connector-python is required for MySQL database checks.")

    try:
        conn = mysql_connector.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            connection_timeout=DB_CONNECT_TIMEOUT,
            use_pure=True
        )
        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s;",
            (db_config['db_name'],)
        )
        exists = cur.fetchone() is not None

        close_cursor_safely(cur, conn, logger)
        conn.close()

        if not exists:
            return False, False, f"Database '{db_config['db_name']}' does not exist."

    except Exception as e:
        return False, False, f"Failed to check database existence: {e}"

    try:
        conn = mysql_connector.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['db_name'],
            connection_timeout=DB_CONNECT_TIMEOUT,
            use_pure=True
        )
        cur = conn.cursor()

        schema = effective_schema_name(db_config, "mysql")
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE';
            """,
            (schema,)
        )
        tables = cur.fetchall()

        if not tables:
            close_cursor_safely(cur, conn, logger)
            conn.close()
            return True, False, f"Database exists but schema '{schema}' has no tables."

        non_empty = False
        for (table_name,) in tables:
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM `{table_name}` LIMIT 1);")
            if cur.fetchone()[0]:
                non_empty = True
                break

        close_cursor_safely(cur, conn, logger)
        conn.close()

        if non_empty:
            return True, True, "Database exists and is non-empty."
        else:
            return True, False, "Database exists but all tables are empty."

    except Exception as e:
        return True, False, f"Failed to check table data: {e}"

def get_latest_timestamp_strict(timestamp_list):
    if not timestamp_list:
        return None

    def parse(ts):
        return datetime.strptime(ts, "%Y%m%d_%H%M%S")

    return max(timestamp_list, key=parse)

def format_observation_for_llm(data, indent=0):
    lines = []
    prefix = "  " * indent

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                lines.append(
                    f"{prefix}- {key}:"
                )
                lines.append(
                    format_observation_for_llm(
                        value,
                        indent + 1
                    )
                )
            else:
                lines.append(
                    f"{prefix}- {key}: {value}"
                )
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            if isinstance(item, (dict, list)):
                lines.append(
                    f"{prefix}- Item {idx + 1}:"
                )
                lines.append(
                    format_observation_for_llm(
                        item,
                        indent + 1
                    )
                )
            else:
                lines.append(
                    f"{prefix}- {item}"
                )
    else:
        lines.append(
            f"{prefix}{data}"
        )

    return "\n".join(lines)

def prune_obser_log(obser_log_path, retain_length=12000):
    if not isinstance(retain_length, int) or retain_length <= 0:
        logger.error("[UTILS] INVALID retain_length %s, FALL BACK TO 12000.", retain_length); retain_length = 12000
    if not os.path.exists(obser_log_path):
        logger.error("[UTILS] OBSERVATION LOG FILE DOES NOT EXIST: %s", obser_log_path); return ""

    try:
        with open(obser_log_path, 'r', encoding='utf-8', errors='ignore') as file: obser_log_content = file.read()
    except OSError as exc:
        logger.error("[UTILS] FAILED TO READ OBSERVATION LOG %s: %s", obser_log_path, exc); return ""

    if len(obser_log_content) <= retain_length: return obser_log_content

    truncation_notice = f"[TRUNCATED OBSERVATION LOG] The original observation log exceeded {retain_length} characters. Only the last {retain_length} characters are retained below.\n"
    pruned_content = truncation_notice + obser_log_content[-retain_length:]

    try:
        with open(obser_log_path, 'w', encoding='utf-8') as file: file.write(pruned_content)
    except OSError as exc:
        logger.error("[UTILS] FAILED TO WRITE PRUNED OBSERVATION LOG %s: %s", obser_log_path, exc); return pruned_content

    logger.info("[UTILS] PRUNED OBSERVATION LOG %s FROM %d TO %d CHARACTERS.", obser_log_path, len(obser_log_content), len(pruned_content))
    return pruned_content

def extract_json_object(text: str) -> str:
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object start found.")

    depth = 0
    in_string = False
    escape = False

    for idx in range(start, len(text)):
        ch = text[idx]

        if escape:
            escape = False
            continue

        if ch == "\\":
            escape = True
            continue

        if ch == '"':
            in_string = not in_string
            continue

        if in_string:
            continue

        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:idx + 1]

    raise ValueError("No complete JSON object found.")

def resolve_path_from_base(path_value, base_dir):
    if path_value is None:
        return None
    path_value = os.path.expanduser(str(path_value))
    
    if os.path.isabs(path_value):
        return os.path.realpath(path_value)
    return os.path.realpath(os.path.join(base_dir, path_value))


# =============================================================================
# Fast environment-reset helpers (pure, dependency-free).
#
# These support the container-reuse + golden-clone optimization used by
# test_syn_ano.py to avoid tearing down and re-importing a multi-GB database on
# every LLM turn. They are intentionally side-effect free (no docker / no db) so
# they can be unit-tested and reused by the orchestration functions in
# docker_func.py.
# =============================================================================

# Valid reset scopes, ordered from cheapest/least invasive to full rebuild. An
# anomaly may declare its footprint via `ResetScope` in anomaly_description.json;
# the harness picks the cheapest scope that still guarantees an identical
# pre-injection baseline.
RESET_SCOPES = ("SESSION_ONLY", "SCHEMA", "DATA", "FULL")


def derive_golden_db_name(db_name):
    """Return the container-local golden/baseline database name for ``db_name``.

    The golden database is a fast clone of the ready-state (imported +
    data-modification applied) database, kept inside the SAME per-worker
    container. A stable, collision-free suffix keeps it distinct from the working
    database while remaining valid for both PostgreSQL and MySQL identifiers.
    """
    return f"{db_name}__golden_baseline"


def parse_reset_scope(value, default="SCHEMA"):
    """Normalize an anomaly-declared reset scope to one of ``RESET_SCOPES``.

    Unknown / missing values fall back to ``default`` (``SCHEMA``), which fully
    rebuilds the working database from the golden clone -- always correct, and
    still far cheaper than a full container rebuild + dump re-import. ``FULL``
    forces the legacy full re-initialization path.
    """
    normalized = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    if normalized in RESET_SCOPES:
        return normalized
    return default


def build_fast_reset_command(base_dir, dbms, mode, db_name, golden_name,
                             db_host, db_port, db_user, db_password, db_schema="diagnosis"):
    """Build the in-container command list that runs the fast snapshot/reset
    shell script for the given DBMS.

    ``mode`` is ``snapshot`` (create the golden baseline once) or ``reset``
    (restore the working database from the golden baseline). The returned list is
    suitable for ``run_docker_exec_status`` / ``docker exec``.
    """
    if mode not in ("snapshot", "reset"):
        raise ValueError(f"Unsupported fast-reset mode `{mode}`. Expected `snapshot` or `reset`.")

    dbms_kind = normalize_dbms(dbms)
    script_name = "fast_reset_database_mysql.sh" if dbms_kind == "mysql" else "fast_reset_database_pg.sh"
    script_path = os.path.join(base_dir, "Scripts", script_name)

    return [
        "bash", script_path,
        "--mode", str(mode),
        "--host", str(db_host),
        "--port", str(db_port),
        "--database_name", str(db_name),
        "--golden_name", str(golden_name),
        "--database_schema", str(db_schema),
        "--user", str(db_user),
        "--password", str(db_password),
    ]


if __name__ == "__main__":
    print(current_time_str())
    exit(0)
