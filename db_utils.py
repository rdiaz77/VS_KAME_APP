# === db_utils.py ===
import os

import pandas as pd
from sqlalchemy import create_engine

# -------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------
DEFAULT_DB_PATH = "data/vitroscience.db"
DEFAULT_TABLE = "ventas"


def get_engine(db_path: str = DEFAULT_DB_PATH):
    """Create and return a SQLAlchemy engine for SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )


def initialize_db(db_path=DEFAULT_DB_PATH):
    """Ensure database folder exists."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    print(f"✅ Database ready at {db_path}")


def save_to_db(df: pd.DataFrame, db_path=DEFAULT_DB_PATH, table_name=DEFAULT_TABLE):
    """
    Replace or create the ventas table to always match the current DataFrame schema.
    """
    if df.empty:
        print("⚠️ No data to save — skipping DB write.")
        return

    initialize_db(db_path)
    engine = get_engine(db_path)

    # Always replace to keep schema consistent
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(
        f"💾 Recreated table '{table_name}' with {len(df)} rows and {len(df.columns)} columns in {db_path}"
    )


def load_from_db(query="SELECT * FROM ventas", db_path=DEFAULT_DB_PATH) -> pd.DataFrame:
    """
    Load data from the SQLite database.
    Automatically retries if table or column names are mismatched.
    """
    engine = get_engine(db_path)
    try:
        df = pd.read_sql(query, engine)
        print(f"📤 Loaded {len(df)} rows from '{db_path}'")
        return df
    except Exception as e:
        print(f"⚠️ Query failed: {e}")
        # Fallback: load full table
        try:
            df = pd.read_sql("SELECT * FROM ventas", engine)
            print(f"📤 Loaded {len(df)} rows (full table) from '{db_path}'")
            return df
        except Exception as e2:
            print(f"❌ Error loading data: {e2}")
            return pd.DataFrame()


# === END db_utils.py ===
