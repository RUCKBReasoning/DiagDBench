import re
from ..automatic_prompt_engineer import utils
from multiagents.utils.database import Database, DBArgs
from multiagents.utils.db_utils import preprocess_execute_sql
import psycopg2

def get_sql_latc_cost(prediction, answers, args, timeout=-1):
    db = Database(args, timeout=timeout)
    flag, prediction = preprocess_execute_sql(prediction)
    if flag == 0:
        return [0, -1, -1]
    db = Database(args, timeout=timeout)
    latency = db.pgsql_actual_time(prediction)
    if latency == -1:
        return [0, -1, -1]
    flag, res = db.execute_sql(prediction)
    if flag == 0 or len(res) != int(answers['result_len']):
        return [0, -2, -2]
    if len(res) == 1 and str(res[0]) != answers['result']:
        return [0, -3, -3]
    cost = db.cost_estimation(prediction)
    assert cost != -1
    return [1, cost, latency]


def get_sql_score_index_tuning(prediction, vald_data, budget, args):
    """Return baseline cost, reduction and indexes within the MB budget."""
    dbargs = DBArgs("pgsql", utils.get_conf(args.db_conf, 'pgsql_server'), args.eval_dataset)
    db = Database(dbargs, timeout=-1)
    indexes = re.findall(r'CREATE\s+(?:UNIQUE\s+)?INDEX\b.*?;', prediction,
                         re.IGNORECASE | re.DOTALL)
    selected = []
    used_bytes = 0
    try:
        baseline = 0
        with db.conn.cursor() as cursor:
            for sql in vald_data:
                cursor.execute("EXPLAIN (FORMAT JSON) " + sql)
                baseline += cursor.fetchone()[0][0]["Plan"]["Total Cost"]
        for index in indexes:
            with db.conn.cursor() as cursor:
                cursor.execute("SELECT * FROM hypopg_create_index(%s)", (index,))
                oid = cursor.fetchone()[0]
                cursor.execute("SELECT hypopg_relation_size(%s)", (oid,))
                size = int(cursor.fetchone()[0])
                if used_bytes + size > budget * 1024 * 1024:
                    cursor.execute("SELECT hypopg_drop_index(%s)", (oid,))
                    continue
            used_bytes += size
            selected.append(index)
        optimized = 0
        with db.conn.cursor() as cursor:
            for sql in vald_data:
                cursor.execute("EXPLAIN (FORMAT JSON) " + sql)
                optimized += cursor.fetchone()[0][0]["Plan"]["Total Cost"]
        return bool(selected), baseline, baseline - optimized, selected
    finally:
        # Hypothetical indexes are session-local; closing also reclaims errors.
        db.conn.close()

def check_index_query(query, args):
    conn = None
    try:

        # 
        conn = psycopg2.connect(dbname=args.dbname, user=args.user, host=args.host,
                                port=args.port, password=args.password, connect_timeout=30)
        cur = conn.cursor()
        cur.execute(query)
        cur.close()
        conn.commit()
        return True
    except psycopg2.Error:
        return False
    finally:
        if conn is not None:
            conn.close()

def create_indexes(indexes, args):
    for index in indexes:
        if not check_index_query(index, args):
            raise RuntimeError("Failed to create a selected evaluation index")

def drop_indexes(indexes, args):
    for index in indexes:
        match = re.match(r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(\S+)', index, re.I)
        if not match or not check_index_query("DROP INDEX IF EXISTS " + match.group(1), args):
            raise RuntimeError("Failed to drop an evaluation index")
