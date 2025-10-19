# === cta_cobrar_main.py (final fixed version with matching history schema) ===
import os
import pandas as pd
import sqlite3
from datetime import datetime
from cta_cobrar import get_cuentas_por_cobrar, save_if_changed
from collection import clean_collection

DB_PATH = "data/vitroscience.db"


def update_cxc_tables(df: pd.DataFrame, db_path=DB_PATH):
    """
    Updates vitroscience.db with:
      1. cuentas_por_cobrar (latest snapshot)
      2. cuentas_por_cobrar_history (daily archive)

    - Schema matches your cleaned CSV headers
    - Adds timestamps and version tracking
    - Creates indexes for efficient queries
    """
    print("🧾 Updating database tables for Cuentas por Cobrar...")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # === Drop and recreate tables ===
    cur.execute("DROP TABLE IF EXISTS cuentas_por_cobrar;")
    cur.execute("DROP TABLE IF EXISTS cuentas_por_cobrar_history;")

    # === Create tables explicitly ===
    cur.execute("""
        CREATE TABLE cuentas_por_cobrar (
            NombreCuenta TEXT,
            Rut TEXT NOT NULL,
            RznSocial TEXT,
            NombreVendedor TEXT,
            Documento TEXT,
            FolioDocumento TEXT NOT NULL,
            Fecha TEXT,
            FechaVencimiento TEXT,
            CondicionVenta TEXT,
            Total INTEGER,
            TotalCP INTEGER,
            Saldo INTEGER,
            last_updated TEXT,
            PRIMARY KEY (Rut, FolioDocumento)
        );
    """)

    cur.execute("""
        CREATE TABLE cuentas_por_cobrar_history (
            NombreCuenta TEXT,
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
            last_updated TEXT,
            snapshot_date TEXT,
            inserted_at TEXT
        );
    """)

    # === Create indexes for faster queries ===
    cur.execute("CREATE INDEX idx_cxc_history_snapshot_date ON cuentas_por_cobrar_history(snapshot_date);")
    cur.execute("CREATE INDEX idx_cxc_history_rut_date ON cuentas_por_cobrar_history(Rut, snapshot_date);")

    # === Clean numeric columns ===
    for col in ["Total", "TotalCP", "Saldo"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .replace("", "0")
                .astype(float)
                .round(0)
                .astype(int)
            )

    # === Add tracking fields ===
    now = datetime.now()
    df["last_updated"] = now.strftime("%Y-%m-%d %H:%M:%S")
    df["snapshot_date"] = now.strftime("%Y-%m-%d")
    df["inserted_at"] = now.strftime("%Y-%m-%d %H:%M:%S")

    # === Save latest snapshot ===
    df_latest = df.drop(columns=["snapshot_date", "inserted_at"])
    df_latest.to_sql("cuentas_por_cobrar", conn, if_exists="replace", index=False)

    # === Append to historical table ===
    df.to_sql("cuentas_por_cobrar_history", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()

    print(f"✅ Database updated successfully ({len(df)} rows).")
    print("   - cuentas_por_cobrar (latest snapshot) recreated")
    print("   - cuentas_por_cobrar_history (daily record) appended")
    print("   - Indexes on snapshot_date and (Rut, snapshot_date) created")


def main():
    """Main pipeline for fetching, cleaning, and storing CxC data."""
    print("🚀 Starting Cuentas por Cobrar pipeline...")

    today_str = datetime.today().strftime("%Y-%m-%d")

    raw_dir = "test/cobranza/raw"
    clean_dir = "test/cobranza/clean"
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(clean_dir, exist_ok=True)

    raw_path = os.path.join(raw_dir, "cuentas_por_cobrar_full.csv")
    raw_dated_path = os.path.join(raw_dir, f"cuentas_por_cobrar_full_{today_str}.csv")
    clean_path = os.path.join(clean_dir, "cuentas_por_cobrar_clean.csv")
    clean_dated_path = os.path.join(clean_dir, f"cuentas_por_cobrar_clean_{today_str}.csv")

    # Step 1: Fetch data
    df_raw = get_cuentas_por_cobrar()
    if df_raw.empty:
        print("⚠️ No records fetched. Exiting.")
        return

    # Step 2: Save raw
    print("\n💾 Saving raw files...")
    save_if_changed(df_raw, raw_path)
    df_raw.to_csv(raw_dated_path, index=False, encoding="utf-8-sig")
    print(f"📦 Raw files saved:\n - Latest: {raw_path}\n - Archive: {raw_dated_path}")

    # Step 3: Clean data
    print("\n🧹 Cleaning downloaded data...")
    df_clean = clean_collection(input_path=raw_path, output_path=clean_path)
    df_clean.to_csv(clean_dated_path, index=False, encoding="utf-8-sig")
    print(f"💾 Cleaned files saved:\n - Latest: {clean_path}\n - Archive: {clean_dated_path}")

    # Step 4: Update DB
    update_cxc_tables(df_clean, db_path=DB_PATH)

    print(f"\n✅ Pipeline completed successfully on {today_str}")
    print(f"   Total rows processed: {len(df_clean)}")
    print(f"   Database updated: {DB_PATH}")


# 🧪 Standalone run
if __name__ == "__main__":
    main()
# === END cta_cobrar_main.py ===
