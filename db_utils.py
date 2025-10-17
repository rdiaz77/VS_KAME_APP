# === db_utils.py ===
from sqlalchemy import create_engine
import pandas as pd
import os

# -------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------
DEFAULT_DB_PATH = "data/vitroscience.db"
DEFAULT_TABLE = "ventas"


def get_engine(db_path: str = DEFAULT_DB_PATH):
    """Create and return a SQLAlchemy engine for SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    # SQLite must use check_same_thread=False for Streamlit concurrency
    return create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})


def initialize_db(db_path=DEFAULT_DB_PATH):
    """Ensure database file and folder exist (does NOT drop tables)."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    print(f"✅ Database ready at {db_path}")


def save_to_db(df: pd.DataFrame, db_path=DEFAULT_DB_PATH, table_name=DEFAULT_TABLE):
    """
    Append cleaned sales data to the SQLite database.
    If the table doesn't exist, it's created automatically.
    """
    if df.empty:
        print("⚠️ No data to save — skipping DB write.")
        return

    initialize_db(db_path)
    engine = get_engine(db_path)

    # Append if table exists, else create
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"💾 Appended {len(df)} rows ({len(df.columns)} columns) into {db_path} → table '{table_name}'")


def load_from_db(query=f"SELECT * FROM {DEFAULT_TABLE}", db_path=DEFAULT_DB_PATH) -> pd.DataFrame:
    """
    Load data from SQLite DB into pandas DataFrame.
    Works safely within Streamlit (multi-thread safe).
    """
    engine = get_engine(db_path)
    try:
        df = pd.read_sql(query, engine)
        print(f"📤 Loaded {len(df)} rows from '{db_path}'")
        return df
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return pd.DataFrame()
# === END db_utils.py ===
