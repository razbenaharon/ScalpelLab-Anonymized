import sqlite3
import pandas as pd
from contextlib import contextmanager

@contextmanager
def connect(db_path: str):
    """Context manager to connect to SQLite DB safely."""
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

def list_tables(db_path: str):
    """Return all non-system table names in the database."""
    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name;
        """)
        return [r[0] for r in cur.fetchall()]

def get_table_schema(db_path: str, table: str):
    """Return PRAGMA schema info for a table (columns, types, etc)."""
    with connect(db_path) as conn:
        return pd.read_sql_query(f"PRAGMA table_info({table});", conn)

def load_table(db_path: str, table: str):
    """Load a whole table into a pandas DataFrame (safe)."""
    with connect(db_path) as conn:
        try:
            return pd.read_sql_query(f"SELECT * FROM {table}", conn)
        except Exception:
            return pd.DataFrame()
