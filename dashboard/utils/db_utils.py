import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "vitroscience.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

def load_table(table_name: str):
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df
# End of file dashboard/utils/db_utils.py