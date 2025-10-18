# === db_utils.py (deduplicate column names before writing) ===
import os

import pandas as pd
from sqlalchemy import create_engine

DEFAULT_DB_PATH = "data/vitroscience.db"
DEFAULT_TABLE = "ventas"


def get_engine(db_path: str = DEFAULT_DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )


def initialize_db(db_path=DEFAULT_DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    print(f"✅ Database ready at {db_path}")


def _swap_region_before_salud(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure 'Region' appears immediately before 'Serviciosalud'."""
    df.columns = df.columns.str.strip().str.title()
    if "ServicioSalud" in df.columns and "Serviciosalud" not in df.columns:
        df.rename(columns={"ServicioSalud": "Serviciosalud"}, inplace=True)
    if "Region" not in df.columns:
        df["Region"] = None
    if "Serviciosalud" not in df.columns:
        df["Serviciosalud"] = None

    cols = list(df.columns)
    if "Region" in cols and "Serviciosalud" in cols:
        i, j = cols.index("Region"), cols.index("Serviciosalud")
        if i > j:
            cols.pop(i)
            j = cols.index("Serviciosalud")
            cols.insert(j, "Region")
            df = df[cols]
            print(
                "📐 DB order enforced: Region placed immediately before Serviciosalud."
            )
    return df


def _deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure no duplicate column names (keep first occurrence)."""
    if df.columns.duplicated().any():
        duplicates = df.columns[df.columns.duplicated()].tolist()
        print(f"⚠️ Duplicate column names detected and dropped: {duplicates}")
        df = df.loc[:, ~df.columns.duplicated()]
    return df


def save_to_db(df: pd.DataFrame, db_path=DEFAULT_DB_PATH, table_name=DEFAULT_TABLE):
    if df.empty:
        print("⚠️ No data to save — skipping DB write.")
        return

    initialize_db(db_path)
    engine = get_engine(db_path)

    # Normalize + deduplicate + enforce order
    df = _swap_region_before_salud(df)
    df = _deduplicate_columns(df)

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(
        f"💾 Recreated table '{table_name}' with {len(df)} rows and {len(df.columns)} columns in {db_path}"
    )


def load_from_db(
    query=f"SELECT * FROM {DEFAULT_TABLE}", db_path=DEFAULT_DB_PATH
) -> pd.DataFrame:
    engine = get_engine(db_path)
    try:
        df = pd.read_sql(query, engine)
        print(f"📤 Loaded {len(df)} rows from '{db_path}'")
        return df
    except Exception as e:
        print(f"⚠️ Query failed: {e}")
        try:
            df = pd.read_sql(f"SELECT * FROM {DEFAULT_TABLE}", engine)
            print(f"📤 Loaded {len(df)} rows (full table) from '{db_path}'")
            return df
        except Exception as e2:
            print(f"❌ Error loading data: {e2}")
            return pd.DataFrame()


def list_tables(db_path=DEFAULT_DB_PATH):
    engine = get_engine(db_path)
    with engine.connect() as conn:
        result = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in result]
    print("📋 Tables in DB:", tables)
    return tables


# === END db_utils.py ===
