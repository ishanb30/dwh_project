"""
Establish connection to SQL Server

Provides a function to return a pyodbc connection
object using environment variables for credentials
"""

import os
import pyodbc
from pathlib import Path

def get_connection():
    server = os.environ.get('DB_SERVER')
    database = os.environ.get('DB_DATABASE')
    username = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')

    return pyodbc.connect(
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'Encrypt=yes;TrustServerCertificate=yes;'
    )

#File path names
BASE_DIR = Path(__file__).resolve().parent.parent

SQL_BRONZE_DIR = BASE_DIR / "sql" / "bronze"
BRONZE_LOAD_CHECK = SQL_BRONZE_DIR / "load_orchestration_check.sql"


SOURCE_CSV_DIR = BASE_DIR / "source"
