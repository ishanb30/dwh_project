"""
Establish connection to SQL Server:
Provides a function to return a pyodbc connection
object using environment variables for credentials

Create a cursor:
Provides a function to return both a connection
and a cursor for executing SQL queries
"""

import os
import pyodbc

def get_connection():
    server = os.environ['DB_SERVER']
    database = os.environ['DB_DATABASE']
    username = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']

    return pyodbc.connect(
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'Encrypt=yes;TrustServerCertificate=yes;'
    )


def get_cursor():
    conn = get_connection()
    cursor = conn.cursor()
    return conn, cursor


