# === get_ventas_incremental.py ===
"""
Incremental updater for VS_KAME_APP.
Fetches new ventas from the day after the latest record in SQLite → today,
then cleans, enriches, and appends to the database.
"""

import datetime
import os
import sqlite3
import pandas as pd

from get_ventas import get_informe_ventas_json
from pipeline import (
    run_clean_sales_pipeline,
    add_location_info,
    add_product_info,
    save_to_sqlite,
)


def get_last_date_from_db(db_path="data/vitroscience.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT MAX(Fecha) FROM ventas_enriched_product;")
    last = cur.fetchone()[0]
    conn.close()
    if not last:
        raise ValueError("No data found in DB. Run full backfill first.")
    # Strip time portion
    return pd.to_datetime(str(last)).date()


def run_incremental_update():
    print("🚀 Starting incremental ventas update...\n")

    last_date = get_last_date_from_db()
    start_date = last_date + datetime.timedelta(days=1)
    end_date = datetime.date.today()

    if start_date > end_date:
        print("ℹ️ Database already up to date.")
        return

    print(f"📅 Fetching ventas from {start_date} → {end_date}")

    # === STEP 1: Fetch new data ===
    df_new = get_informe_ventas_json(str(start_date), str(end_date))
    if df_new is None or df_new.empty:
        print("⚠️ No new ventas found.")
        return

    raw_path = f"test/ventas/raw/ventas_raw_{start_date}_to_{end_date}.csv"
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    df_new.to_csv(raw_path, index=False)
    print(f"💾 Saved raw incremental file → {raw_path}")

    # === STEP 2: Clean + enrich ===
    print("🧹 Cleaning new data...")
    df_clean = run_clean_sales_pipeline(source_path=raw_path)
    df_loc = add_location_info(df_clean)
    df_prod = add_product_info(df_loc)

    # === STEP 3: Save to DB ===
    enriched_path = f"test/ventas/clean/ventas_enriched_product_incremental_{start_date}_to_{end_date}.csv"
    os.makedirs(os.path.dirname(enriched_path), exist_ok=True)
    df_prod.to_csv(enriched_path, index=False)
    print(f"💾 Cleaned + enriched incremental file saved → {enriched_path}")

    print("🗄️ Appending new ventas to SQLite...")
    save_to_sqlite(csv_path=enriched_path)

    print("\n✅ Incremental update complete.\n")
    # === Log completion timestamp ===
    from datetime import datetime
    os.makedirs("data", exist_ok=True)
    with open("data/update_log.txt", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} — Incremental update finished.\n")


if __name__ == "__main__":
    run_incremental_update()
# === End of get_ventas_incremental.py ===
