"""
Create a cursor

Provides a function to return both a connection
and a cursor for executing SQL queries
"""

from config import get_connection

def get_cursor():
    conn = get_connection()
    cursor = conn.cursor()
    return conn, cursor

