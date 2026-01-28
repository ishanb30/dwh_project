"""
Orchestrates Data Flow

Calls functions to run SQL load files between
medallion layers
"""

from cursor import get_cursor
from config import BRONZE_LOAD_CHECK

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
            step = failed_steps[0]
            print("Bronze pipeline failed due to:")
            print(f"proc_name: {step[3]}")
            print(f"error_class: {step[9]}")
            print(f"error_message: {step[5]}")
            return False
        else:
            print("Bronze pipeline ran successfully. Proceed to Silver.")
            return True
    except Exception as e:
        print(f"ERROR: Pipeline failed before completion: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    run_bronze_pipeline()
#add silver and gold pipeline sequencing logic