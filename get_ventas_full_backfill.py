# === get_ventas_full_backfill.py ===
import datetime
import os
import sqlite3

from get_ventas import get_ventas_full_year
from pipeline import (
    run_clean_sales_pipeline,
    add_location_info,
    add_product_info,
    save_to_sqlite,
)


def run_backfill():
    print("🧱 Starting full backfill (2023 → today)\n")

    raw_dir = "test/ventas/raw"
    clean_dir = "test/ventas/clean"
    current_year = datetime.date.today().year

    for year in range(2023, current_year + 1):
        print(f"\n📅 Processing year {year}...")

        # === STEP 1: Fetch full-year raw data ===
        df_raw = get_ventas_full_year(year)
        if df_raw is None or df_raw.empty:
            print(f"⚠️ No data for {year}. Skipping.")
            continue

        raw_path = os.path.join(raw_dir, f"ventas_raw_{year}_full.csv")
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        df_raw.to_csv(raw_path, index=False)
        print(f"💾 Saved raw file → {raw_path}")

        # === STEP 2: Clean ===
        print(f"🧹 Cleaning {year} data...")
        df_clean = run_clean_sales_pipeline(source_path=raw_path)

        # === STEP 3: Enrich (location + product) ===
        df_loc = add_location_info(df_clean)
        df_prod = add_product_info(df_loc)

        # === STEP 4: Save enriched file ===
        enriched_path = os.path.join(clean_dir, f"ventas_enriched_product_{year}.csv")
        os.makedirs(os.path.dirname(enriched_path), exist_ok=True)
        df_prod.to_csv(enriched_path, index=False)
        print(f"💾 Cleaned + enriched file saved → {enriched_path}")

        # === STEP 5: Append to SQLite ===
        print(f"🗄️ Appending {year} data to SQLite...")
        save_to_sqlite(csv_path=enriched_path)

    # === STEP 6: Verify DB ===
    print("\n✅ Full backfill completed — verifying database contents...")
    conn = sqlite3.connect("data/vitroscience.db")
    cur = conn.cursor()
    cur.execute("SELECT MIN(Fecha), MAX(Fecha), COUNT(*) FROM ventas_enriched_product;")
    result = cur.fetchone()
    conn.close()

    if result and all(result):
        print(
            f"📅 DB contains data from {result[0]} → {result[1]} "
            f"({result[2]} total rows)"
        )
    else:
        print("⚠️ Verification failed or DB is empty.")


if __name__ == "__main__":
    run_backfill()
# === End of get_ventas_full_backfill.py ===