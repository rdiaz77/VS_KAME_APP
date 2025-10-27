# === get_ventas_main.py ===
"""
Main orchestrator for the VS_KAME_APP sales data pipeline.

Execution order:
1. get_ventas.py                — Fetch raw sales data from KAME API
2. pipeline/clean_sales_main.py — Clean and standardize data
3. pipeline/enrich_location.py  — Add Region and ServicioSalud
4. pipeline/enrich_product.py   — Add Unegocio from SKU mapping
5. pipeline/save_to_sqlite.py   — Save final data into SQLite DB

Usage:
    python get_ventas_main.py [fecha_desde] [fecha_hasta]
Example:
    python get_ventas_main.py 2024-02-01 2024-02-29

New:
    python get_ventas_main.py 2023   # fetches full year in monthly chunks and runs full pipeline
"""

import os
import shutil
import sys

import pandas as pd

# === Import fetchers ===
from get_ventas import get_informe_ventas_json, get_ventas_full_year

# === Import pipeline components (original working flow) ===
from get_ventas import get_informe_ventas_json, get_ventas_full_year

from pipeline import (
    add_location_info,
    add_product_info,
    run_clean_sales_pipeline,
)
from pipeline.save_to_sqlite import save_to_sqlite



def run_full_pipeline(
    fecha_desde: str, fecha_hasta: str, raw_override: str | None = None
):
    """
    Run the entire VS_KAME_APP sales data pipeline.

    If raw_override is provided and exists, we skip the API fetch
    and use that CSV as the raw input (e.g., full-year combined file).
    """
    print("\n🧪 Starting VS_KAME_APP pipeline...\n")

    # Where the step-1 fetch would normally save its CSV
    raw_path_default = f"test/ventas/raw/ventas_raw_{fecha_desde}_to_{fecha_hasta}.csv"
    enriched_path = "test/ventas/clean/ventas_enriched.csv"
    product_enriched_path = "test/ventas/clean/ventas_enriched_product.csv"

    # === STEP 1: Raw data source resolution ===
    if raw_override and os.path.exists(raw_override):
        print(f"🚀 STEP 1: Using pre-fetched raw file → {raw_override}")
        raw_path = raw_override
        df_raw = pd.read_csv(raw_path)
    else:
        print("🚀 STEP 1: Fetching ventas from Kame API")
        df_raw = get_informe_ventas_json(fecha_desde, fecha_hasta)
        if df_raw is None or df_raw.empty:
            print("❌ No data fetched. Exiting pipeline.")
            return
        print(f"✅ Raw data fetched ({len(df_raw)} rows)")
        raw_path = raw_path_default

    # Ensure cleaner sees the expected input path (non-breaking)
    os.makedirs("test/ventas/raw", exist_ok=True)
    cleaner_input = "test/ventas/raw/ventas_raw.csv"
    try:
        if os.path.abspath(raw_path) != os.path.abspath(cleaner_input):
            shutil.copy(raw_path, cleaner_input)
            print(f"📦 Copied raw file to cleaner input → {cleaner_input}")
    except Exception as e:
        print(f"⚠️ Could not copy raw file to {cleaner_input}: {e}")

    # === STEP 2: Clean sales data ===
    print("\n🧹 STEP 2: Cleaning sales data")
    df_clean = run_clean_sales_pipeline(source_path=cleaner_input)
    if df_clean is None or df_clean.empty:
        print("❌ Cleaning produced no rows. Exiting pipeline.")
        return
    print(f"✅ Cleaned data ready ({len(df_clean)} rows)")

    # === STEP 3: Enrich with location ===
    print("\n🌎 STEP 3: Adding location info")
    df_loc = add_location_info(df_clean)
    os.makedirs(os.path.dirname(enriched_path), exist_ok=True)
    df_loc.to_csv(enriched_path, index=False)
    print(f"💾 Saved → {enriched_path}")

    # === STEP 4: Enrich with product info ===
    print("\n🧩 STEP 4: Adding product info")
    df_prod = add_product_info(df_loc)
    os.makedirs(os.path.dirname(product_enriched_path), exist_ok=True)
    df_prod.to_csv(product_enriched_path, index=False)
    print(f"💾 Saved → {product_enriched_path}")

    # === STEP 5: Save to SQLite ===
    print("\n🗄️ STEP 5: Saving to SQLite database")
    save_to_sqlite()

    print("\n✅ Pipeline completed successfully!\n")


def run_full_year_pipeline(year: int):
    """
    Year mode: fetch all ventas for a given year (month-by-month),
    combine to a single CSV, and run the full pipeline using that CSV.
    """
    print(f"\n🗓️ Starting full-year pipeline for {year}...")

    df_all = get_ventas_full_year(year)
    if df_all is None or df_all.empty:
        print(f"❌ No data fetched for {year}. Aborting pipeline.")
        return

    combined_path = f"test/ventas/raw/ventas_raw_{year}_full.csv"
    if not os.path.exists(combined_path):
        os.makedirs("test/ventas/raw", exist_ok=True)
        df_all.to_csv(combined_path, index=False)

    print(f"✅ Combined file ready: {combined_path}")

    # Run the standard pipeline but point it to the combined CSV
    run_full_pipeline(f"{year}-01-01", f"{year}-12-31", raw_override=combined_path)


if __name__ == "__main__":
    # === CLI arguments ===
    if len(sys.argv) == 2 and sys.argv[1].isdigit():
        YEAR = int(sys.argv[1])
        run_full_year_pipeline(YEAR)
    elif len(sys.argv) == 3:
        DATE_FROM, DATE_TO = sys.argv[1], sys.argv[2]
        run_full_pipeline(DATE_FROM, DATE_TO)
    else:
        DATE_FROM, DATE_TO = "2024-01-01", "2024-01-31"
        print(f"⚙️ No dates provided — defaulting to {DATE_FROM} → {DATE_TO}")
        run_full_pipeline(DATE_FROM, DATE_TO)

# === END get_ventas_main.py ===
