"""
Run Gold Pipeline

Runs the Gold layer pipeline. Detects
and raises errors for upstream handling
"""

from db_utils import get_cursor
from paths import SILVER_LOAD_CHECK
from paths import GOLD_LOAD_CHECK

class GoldPipelineFailed(Exception):
    def __init__(self, failed_steps):
        self.failed_steps = failed_steps

def run_check_query(sql, cursor):
    with open(sql, "r") as f:
        check = f.read()
    cursor.execute(check)

def get_run_id(cursor):
    run_check_query(SILVER_LOAD_CHECK, cursor)

    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("Expected run_id but query didn't capture any rows")
    return row[1]

def check_gold_load(cursor):
    run_check_query(GOLD_LOAD_CHECK, cursor)

    rows = cursor.fetchall()
    gold_dicts = [dict(zip([col[0] for col in cursor.description], row)) for row in rows]
    if not gold_dicts:
        raise RuntimeError("Expected run log rows but query didn't capture any rows")
    return gold_dicts


def run_gold_pipeline():
    conn = None
    cursor = None

    try:
        conn, cursor = get_cursor()

        run_id = get_run_id(cursor)

        cursor.execute("EXEC gold.load_gold_all @run_id = ?", (run_id,))
        conn.commit()

        gold_dicts = check_gold_load(cursor)
        failed_steps = [d for d in gold_dicts if d['status'] != 'SUCCESS']
        if failed_steps:
            raise GoldPipelineFailed(failed_steps)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


#To be run in a master orchestrator file
if __name__ == "__main__":
    run_gold_pipeline()


