"""
Run Bronze Pipeline

Runs the Bronze layer pipeline. Detects
and raises errors for upstream handling
"""

from config import get_cursor
from paths import BRONZE_LOAD_CHECK

class BronzePipelineFailed(Exception):
    def __init__(self, failed_steps):
        self.failed_steps = failed_steps

def run_bronze_pipeline():
    conn = None
    cursor = None

    try:
        conn, cursor = get_cursor()
        cursor.execute("EXEC bronze.load_bronze_all")
        conn.commit()

        with open(BRONZE_LOAD_CHECK, "r") as f:
            bronze_check = f.read()
        cursor.execute(bronze_check)
        rows = cursor.fetchall()
        dicts = [dict(zip([col[0] for col in cursor.description], row)) for row in rows]

        failed_steps = [d for d in dicts if d['status'] != "SUCCESS"]
        if failed_steps:
            raise BronzePipelineFailed(failed_steps)

    except BronzePipelineFailed as e:
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


#To be run in a master orchestrator file
if __name__ == "__main__":
    run_bronze_pipeline()
