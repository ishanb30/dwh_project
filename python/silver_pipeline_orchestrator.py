"""
Run Silver Pipeline

Runs the Silver layer pipeline. Detects
and raises errors for upstream handling
"""

from db_utils import get_cursor
from paths import BRONZE_LOAD_CHECK
from paths import SILVER_LOAD_CHECK

class SilverPipelineFailed(Exception):
    def __init__(self, failed_steps):
        self.failed_steps = failed_steps

def check_rows(sql_check_path, cursor):
    with open(sql_check_path, "r") as f:
        check = f.read()
    cursor.execute(check)
    rows = cursor.fetchall()
    dicts = [dict(zip([col[0] for col in cursor.description], row)) for row in rows]
    return dicts

def run_silver_pipeline():
    conn = None
    cursor = None

    try:
        conn, cursor = get_cursor()

        #Fetching the most recent run id so that it matches the bronze pipeline's run id
        bronze_dicts = check_rows(BRONZE_LOAD_CHECK, cursor)
        run_id = bronze_dicts[0]['run_id']

        cursor.execute("EXEC silver.load_silver_all @run_id = ?", (run_id,))
        conn.commit()

        #Check etl_run_log table for any failed steps
        silver_dicts = check_rows(SILVER_LOAD_CHECK, cursor)
        failed_steps = [d for d in silver_dicts if d['status'] != 'SUCCESS']
        if failed_steps:
            raise SilverPipelineFailed(failed_steps)

    except SilverPipelineFailed as e:
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    run_silver_pipeline()