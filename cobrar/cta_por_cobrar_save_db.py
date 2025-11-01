# === cobrar/cta_por_cobrar_save_db.py (final version with INTEGER fields for amounts) ===
import os
import sqlite3
from datetime import datetime

import pandas as pd

DB_PATH = "data/vitroscience.db"
CLEAN_PATH = "test/cobranza/clean/cuentas_por_cobrar_clean.csv"


def save_cta_por_cobrar_to_db(input_path=CLEAN_PATH, db_path=DB_PATH):
    """
    Save cleaned 'Cuentas por Cobrar' data into SQLite database.

    - Rebuilds schema for cuentas_por_cobrar and cuentas_por_cobrar_history
    - Numeric fields (Total, TotalCP, Saldo) stored as INTEGER
    - Dates stored as TEXT (ISO 'YYYY-MM-DD')
    """

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"❌ Cleaned file not found: {input_path}")

    print(f"💾 Loading cleaned file: {input_path}")
    df = pd.read_csv(input_path, dtype=str)
    if df.empty:
        print("⚠️ Clean file is empty, nothing to save.")
        return

    # Convert numeric fields to int explicitly
    for col in ["Total", "TotalCP", "Saldo"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Add tracking fields
    now = datetime.now()
    snapshot_date = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    df["status"] = "pending"
    df["last_updated"] = timestamp
    df["snapshot_date"] = snapshot_date
    df["inserted_at"] = timestamp

    # Ensure DB directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    print(f"🗃️ Connected to database: {db_path}")

    # === Drop existing tables (to avoid legacy schema conflicts) ===
    cur.executescript(
        """
        DROP TABLE IF EXISTS cuentas_por_cobrar;
        DROP TABLE IF EXISTS cuentas_por_cobrar_history;
        DROP INDEX IF EXISTS idx_cxc_rut;
        DROP INDEX IF EXISTS idx_cxc_month;
        DROP INDEX IF EXISTS idx_cxc_hist_snapshot_date;
        DROP INDEX IF EXISTS idx_cxc_hist_rut_date;
    """
    )

    # === Create tables with correct numeric and date types ===
    cur.executescript(
        """
        CREATE TABLE cuentas_por_cobrar (
            Rut TEXT,
            RznSocial TEXT,
            NombreVendedor TEXT,
            Documento TEXT,
            FolioDocumento TEXT PRIMARY KEY,
            Fecha TEXT,
            FechaVencimiento TEXT,
            CondicionVenta TEXT,
            Total INTEGER,
            TotalCP INTEGER,
            Saldo INTEGER,
            MonthFetched TEXT,
            SnapshotDate TEXT,
            status TEXT,
            last_updated TEXT
        );

        CREATE TABLE cuentas_por_cobrar_history (
            Rut TEXT,
            RznSocial TEXT,
            NombreVendedor TEXT,
            Documento TEXT,
            FolioDocumento TEXT,
            Fecha TEXT,
            FechaVencimiento TEXT,
            CondicionVenta TEXT,
            Total INTEGER,
            TotalCP INTEGER,
            Saldo INTEGER,
            MonthFetched TEXT,
            SnapshotDate TEXT,
            status TEXT,
            last_updated TEXT,
            snapshot_date TEXT,
            inserted_at TEXT
        );
    """
    )

    # Align DataFrame columns to schema
    expected_cols = [
        "Rut",
        "RznSocial",
        "NombreVendedor",
        "Documento",
        "FolioDocumento",
        "Fecha",
        "FechaVencimiento",
        "CondicionVenta",
        "Total",
        "TotalCP",
        "Saldo",
        "MonthFetched",
        "SnapshotDate",
        "status",
        "last_updated",
        "snapshot_date",
        "inserted_at",
    ]
    df = df[[c for c in df.columns if c in expected_cols]]

    # === Save snapshot ===
    df_latest = df.drop(columns=["snapshot_date", "inserted_at"], errors="ignore")
    df_latest.to_sql("cuentas_por_cobrar", conn, if_exists="replace", index=False)
    print(f"✅ Latest snapshot saved → cuentas_por_cobrar ({len(df_latest)} rows)")

    # === Append to history ===
    df.to_sql("cuentas_por_cobrar_history", conn, if_exists="append", index=False)
    print(
        f"📜 Historical log updated → cuentas_por_cobrar_history ({len(df)} rows appended)"
    )

    # === Create indexes ===
    cur.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_cxc_rut ON cuentas_por_cobrar(Rut);
        CREATE INDEX IF NOT EXISTS idx_cxc_month ON cuentas_por_cobrar(MonthFetched);
        CREATE INDEX IF NOT EXISTS idx_cxc_hist_snapshot_date ON cuentas_por_cobrar_history(snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_cxc_hist_rut_date ON cuentas_por_cobrar_history(Rut, snapshot_date);
    """
    )

    conn.commit()
    conn.close()

    print(f"✅ Database rebuilt successfully on {snapshot_date}")
    print("📦 Tables: cuentas_por_cobrar, cuentas_por_cobrar_history"ry")
    print(f"🧾 Total invoices processed: {len(df)}")


# 🧪 Standalone run
if __name__ == "__main__":
    save_cta_por_cobrar_to_db()
# === END cobrar/cta_por_cobrar_save_db.py ===
