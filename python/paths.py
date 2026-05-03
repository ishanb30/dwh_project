"""
File Path Storage:
This scripts stores file paths that are relevant to the
project and can be imported into any script, regardless
of medallion layer
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SQL_BRONZE_DIR = BASE_DIR / "sql" / "bronze"
BRONZE_LOAD_CHECK = SQL_BRONZE_DIR / "load_orchestration_check.sql"

SQL_SILVER_DIR = BASE_DIR / "sql" / "silver"
SILVER_LOAD_CHECK = SQL_SILVER_DIR / "load_orchestration_check.sql"

SQL_GOLD_DIR = BASE_DIR / "sql" / "gold"
GOLD_LOAD_CHECK = SQL_GOLD_DIR / "load_orchestration_check.sql"
CHECK_FLAG_PCT = SQL_GOLD_DIR / "check_flag_pct.sql"

SOURCE_CSV_DIR = BASE_DIR / "source"

CONFIG_PATH = BASE_DIR / "python" / "config.yaml"

PYTHON_DIR = BASE_DIR / "python"