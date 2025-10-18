# === db_utils.py ===
import os
import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------
DEFAULT_DB_PATH = "data/vitroscience.db"
DEFAULT_TABLE = "ventas"


# -------------------------------------------------------
# DATABASE CONNECTION
# -------------------------------------------------------
def get_engine(db_path: str = DEFAULT_DB_PATH):
    """Create and return a SQLAlchemy engine for SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})


def initialize_db(db_path=DEFAULT_DB_PATH):
    """Ensure database folder exists."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    print(f"✅ Database ready at {db_path}")


# -------------------------------------------------------
# SAVE & LOAD
# -------------------------------------------------------
def save_to_db(df: pd.DataFrame, db_path=DEFAULT_DB_PATH, table_name=DEFAULT_TABLE):
    """
    Save a DataFrame to SQLite safely, replacing the existing table.
    """
    if df.empty:
        print("⚠️ No data to save — skipping DB write.")
        return

    initialize_db(db_path)
    engine = get_engine(db_path)

    # Normalize column names for consistency
    df.columns = df.columns.str.strip().str.title()

    # Replace table with new clean data
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"💾 Saved {len(df)} rows → table '{table_name}' ({len(df.columns)} columns)")


def load_from_db(query=None, db_path=DEFAULT_DB_PATH, table_name=DEFAULT_TABLE):
    """
    Safely load data from the SQLite database.
    If no query is provided, loads the full table.
    """
    engine = get_engine(db_path)
    with engine.connect() as conn:
        if query:
            # Use parameterized text to prevent injection
            result_df = pd.read_sql(text(query), conn)
        else:
            result_df = pd.read_sql_table(table_name, conn)
    print(f"📤 Loaded {len(result_df)} rows from '{table_name}'")
    return result_df


# -------------------------------------------------------
# UTILITIES
# -------------------------------------------------------
def list_tables(db_path=DEFAULT_DB_PATH):
    """List all tables in the database."""
    engine = get_engine(db_path)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
        tables = [r[0] for r in result]
    print("📋 Tables in DB:", tables)
    return tables
