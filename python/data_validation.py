"""
Bronze Data Validation

Analyses the row counts between the source files and the bronze layer tables.

Outputs: updates the admin.etl_run_log table with rows read, rows written
and validation status.
"""

import pandas as pd
from config import SOURCE_CSV_DIR
from config import BRONZE_LOAD_CHECK
from cursor import get_cursor

class RowMismatch(Exception):
    def __init__(self, row_comparison):
        self.row_comparison = row_comparison

class KeyMismatch(Exception):
    def __init__(self, key_comparison):
        self.key_comparison = key_comparison

def get_source_row_counts(files):
    counts = {}
    for file in files:
        df = pd.read_csv(file)
        index = file.parent.name.find("_")
        name = f"{file.parent.name[index+1:]}_{file.stem.lower()}"
        counts[name] = df.shape[0]
    return counts

def get_bronze_row_counts(table_names, cursor):
    counts = {}
    for name in table_names:
        cursor.execute(f"SELECT COUNT(*) FROM bronze.{name}")
        row_counts = cursor.fetchone()[0]
        counts[name] = row_counts
    return counts

def get_run_id(BRONZE_LOAD_CHECK, cursor):
    with open(BRONZE_LOAD_CHECK, "r") as f:
        bronze_check = f.read()
    cursor.execute(bronze_check)
    run_id = cursor.fetchone()[1]
    return run_id

def update_with_row_count(source_counts, bronze_counts, status, run_id, key, cursor):
    cursor.execute(f"""
        UPDATE admin.etl_run_log
        SET
            rows_read = {source_counts[key]},
            rows_written = {bronze_counts[key]},
            validation_status = '{status}'
        WHERE run_id = '{run_id}' AND layer = 'bronze' AND proc_name = 'bronze.load_{key}'
    """)

def update_without_row_count(status, run_id, key, cursor):
    cursor.execute(f"""
        UPDATE admin.etl_run_log
        SET validation_status = '{status}'
        WHERE run_id = '{run_id}' AND layer = 'bronze' AND proc_name = 'bronze.load_{key}'
    """)

try:
    conn = None
    cursor = None
    conn, cursor = get_cursor()

    cursor.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='bronze';
    """)
    rows = cursor.fetchall()
    table_names = [row[0] for row in rows]

    files = list(SOURCE_CSV_DIR.rglob("*.csv"))
    source_counts = get_source_row_counts(files)
    bronze_counts = get_bronze_row_counts(table_names, cursor)
    run_id = get_run_id(BRONZE_LOAD_CHECK, cursor)

    source_keys = set(source_counts.keys())
    bronze_keys = set(bronze_counts.keys())
    if source_keys == bronze_keys:
        if all(source_counts[key] == bronze_counts[key] for key in source_keys):
            for key in source_keys:
                update_with_row_count(source_counts, bronze_counts, 'SUCCESS', run_id, key, cursor)
            conn.commit()
        else:
            row_comparison = {}
            for key in source_keys:
                if source_counts[key] != bronze_counts[key]:
                    row_comparison[key] = (source_counts[key], bronze_counts[key])
            raise RowMismatch(row_comparison)
    else:
        key_comparison = source_keys.symmetric_difference(bronze_keys)
        raise KeyMismatch(key_comparison)
except RowMismatch as e:
    for key in e.row_comparison:
        update_with_row_count(source_counts, bronze_counts, 'FAILED', run_id, key, cursor)
    conn.commit()
    raise
except KeyMismatch as e:
    for key in source_keys:
        update_without_row_count('FAILED', run_id, key, cursor)
    conn.commit()
    raise
except Exception as e:
    for key in source_keys:
        update_without_row_count('FAILED', run_id, key, cursor)
    conn.commit()
    raise
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()











