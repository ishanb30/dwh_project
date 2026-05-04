"""
Silver Data Validation

Carries out three checks on silver layer tables:
    - Referential Integrity
    - Nulls in key columns
    - Duplicates in key columns
Outputs: updates a column for each check in the admin.etl_run_log
table with either 'PASS' or 'FAIL'. It also updates the
validation_status column with 'SUCCESS' if all three checks are
passed and 'FAILED' if at least one check fails
"""

import yaml
from db_utils import get_cursor
from paths import CONFIG_PATH
from paths import SILVER_LOAD_CHECK


with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

silver = config["silver"]
refs = config["refs"]

class SilverValidationFailed(Exception):
    def __init__(self, failed_checks):
        self.failed_checks = failed_checks

def check_ref_integrity(cursor):
    failed = []
    for ref in refs:
        cursor.execute(f"""
        SELECT
            *
        FROM
            silver.{ref["src_table"]} s
        LEFT JOIN
            silver.{ref["tgt_table"]} t ON
            s.{ref["src_col"]} = t.{ref["tgt_col"]}
        WHERE
            t.{ref["tgt_col"]} IS NULL
        """)

        success = cursor.fetchone() is None
        if not success:
            failed.append(ref)

    return failed

def check_nulls(cursor):
    failed = []
    for table, detail in silver.items():
        for key in detail["primary_keys"]:
            cursor.execute(f"""
                SELECT
                    *
                FROM
                    silver.{table}
                WHERE
                    silver.{table}.{key} IS NULL
            """)

            success = cursor.fetchone() is None
            if not success:
                failed.append({table: key})

    return failed

def check_duplicates(cursor):
    failed = []
    for table, detail in silver.items():
        cursor.execute(f"""
            SELECT
                TOP 1 *
            FROM (                
                SELECT
                    COUNT(*) as cnt
                FROM
                    silver.{table}
                GROUP BY
                    {",".join(f"silver.{table}.{key}" for key in detail["primary_keys"])}
                HAVING
                    COUNT(*) > 1
            ) sq
        """)

        success = cursor.fetchone() is None
        if not success:
            failed.append({table: detail["primary_keys"]})

    return failed

def get_run_id(cursor):
    with open(SILVER_LOAD_CHECK, "r") as f:
        silver_check = f.read()
    cursor.execute(silver_check)
    run_id = cursor.fetchone()[1]
    return run_id

def update_run_log(validation_status,ref_integrity_status,run_id,null_status,duplicate_status,table,cursor):
    cursor.execute(f"""
        UPDATE admin.etl_run_log
        SET
            validation_status = '{validation_status}',
            referential_integrity = '{ref_integrity_status}',
            null_key_check = '{null_status}',
            duplicate_key_check = '{duplicate_status}'
        WHERE run_id = '{run_id}' AND layer = 'silver' AND proc_name = 'silver.load_{table}' 
    """)

def run_silver_validation():
    conn = None
    cursor = None

    try:
        conn, cursor = get_cursor()

        ref_integrity = check_ref_integrity(cursor)
        nulls = check_nulls(cursor)
        duplicates = check_duplicates(cursor)
        run_id = get_run_id(cursor)

        for table, detail in silver.items():
            ref_integrity_status = "FAIL" if any(table == r["src_table"] or table == r["tgt_table"] for r in ref_integrity) else "PASS"
            null_status = "FAIL" if any(table in n for n in nulls) else "PASS"
            duplicate_status = "FAIL" if any(table in d for d in duplicates) else "PASS"
            validation_status = "FAIL" if any(s == "FAIL" for s in [ref_integrity_status,null_status,duplicate_status]) else "SUCCESS"

            update_run_log(validation_status,ref_integrity_status,run_id,null_status,duplicate_status,table,cursor)
        conn.commit()

        if any([ref_integrity, nulls, duplicates]):
            raise SilverValidationFailed([
                {"Referential Integrity": ref_integrity},
                {"Nulls": nulls},
                {"Duplicates": duplicates}
            ])

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    run_silver_validation()