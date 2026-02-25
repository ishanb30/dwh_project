"""
Run Bronze Pipeline

Runs the Bronze layer pipeline. Detects
and raises errors for upstream handling
"""

from cursor import get_cursor
from config import BRONZE_LOAD_CHECK

class BronzePipelineFailed(Exception):
    def __init__(self, proc_name, error_class, error_message):
        self.proc_name = proc_name
        self.error_class = error_class
        self.error_message = error_message

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

        failed_steps = [row for row in rows if row[3]=="FAILED"]
        if failed_steps:
            step, = failed_steps
            raise BronzePipelineFailed(
                proc_name = step[3],
                error_class = step[9],
                error_message = step[5]
            )

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
