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
base_dir = Path(__file__).resolve().parent.parent
sql_dir = base_dir / "sql" / "bronze"
bronze_load_check = sql_dir / "load_orchestration_check.sql"


