"""
Gold Data Validation

Carries out four checks on gold layer tables:
    - Referential Integrity
    - Nulls in key columns
    - Duplicates in key columns
    - % of rows that have a 'Y' value in each of
    "is_incomplete_financial_data", "err_date_lifecycle" and
    "err_date_sequence"
Outputs: updates the validation_status column with 'SUCCESS'
if all three checks (the first three in the list above) are
passed and 'FAILED' if at least one check fails. Any failures
of all four checks are logged.
"""

import yaml
import logging
from db_utils import get_cursor
from paths import CONFIG_PATH
from paths import PYTHON_DIR
from paths import CHECK_FLAG_PCT
from paths import GOLD_LOAD_CHECK
from logging_config import setup_logging

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger('gold_data_validation')
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(PYTHON_DIR/'gold_validation.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

gold = config["gold"]
refs = config["g_refs"]

class GoldValidationFailed(Exception):
    def __init__(self, failed_message):
        self.failed_message = failed_message
        super().__init__(failed_message)

def check_ref_integrity(cursor):
    failed = []
    for ref in refs:
        null_check = f"AND s.{ref['src_col']} IS NOT NULL" if ref.get('allow_null_src') else ""
        cursor.execute(f"""
            SELECT
                TOP 1 1
            FROM
                gold.{ref["src_table"]} s
            LEFT JOIN
                gold.{ref["tgt_table"]} t
                    ON s.{ref["src_col"]} = t.{ref["tgt_col"]}
            WHERE
                t.{ref["tgt_col"]} IS NULL {null_check}
        """)

        if cursor.fetchone():
            failed.append(ref)

    if not failed:
        logger.info('Referential Integrity check passed')

    return failed

def check_nulls(cursor):
    failed = []
    for table, detail in gold.items():
        for key in detail["primary_keys"]:
            cursor.execute(f"SELECT TOP 1 1 FROM gold.{table} WHERE {key} IS NULL")

            if cursor.fetchone():
                failed.append({"table": table, "column": key})

    if not failed:
        logger.info('Null check passed')

    return failed

def check_duplicates(cursor):
    failed = []
    for table, detail in gold.items():
        pk = ",".join(f"gold.{table}.{key}" for key in detail["primary_keys"])
        cursor.execute(f"""
            WITH dups AS(        
                SELECT *, COUNT(*) OVER (PARTITION BY {pk}) AS cnt
                FROM gold.{table}
            )
            
            SELECT TOP 1 1 FROM dups WHERE cnt > 1
        """)

        if cursor.fetchone():
            failed.append({"table": table, "column": detail["primary_keys"]})

    if not failed:
        logger.info('Duplicate check passed')

    return failed

def check_flag_pct(cursor):
    with open(CHECK_FLAG_PCT, "r") as f:
        flag_pct = f.read()

    cursor.execute(flag_pct)
    row = cursor.fetchone()

    flag_pct_dicts = dict(zip([key[0] for key in cursor.description], row))

    logger.info(flag_pct_dicts)

def get_run_id(cursor):
    with open(GOLD_LOAD_CHECK, "r") as f:
        gold_check = f.read()
    cursor.execute(gold_check)
    run_id = cursor.fetchone()[1]
    return run_id

def update_run_log(check_list, run_id, cursor, conn):
    status = 'FAILED' if any(check_list) else 'SUCCESS'

    for table, detail in gold.items():
        cursor.execute(f"""
            UPDATE admin.etl_run_log
            SET validation_status = ?
            WHERE run_id = ? AND layer = 'gold' AND proc_name = 'gold.load_{table}'
        """, (status,run_id))
    conn.commit()

def run_gold_validation():
    conn = None
    cursor = None

    try:
        conn, cursor = get_cursor()
        run_id = get_run_id(cursor)

        results = {
            "Referential Integrity": check_ref_integrity(cursor),
            "Nulls": check_nulls(cursor),
            "Duplicates": check_duplicates(cursor)
        }

        check_flag_pct(cursor)

        update_run_log(list(results.values()), run_id, cursor, conn)

        if not any(results.values()):
            logger.info(f"Run {run_id}: All Gold validation checks passed")
        else:
            failed_checks = {k:v for k, v in results.items() if v}
            logger.error(f"Run {run_id}: Validation failed. Details: {failed_checks}")
            raise GoldValidationFailed("Gold Validation failed. Check gold_validation.log for details")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_logging()
    run_gold_validation()




