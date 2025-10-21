# === get_ventas_main.py ===
"""
Main orchestrator for the VS_KAME_APP sales data pipeline.

Order of execution:
1. get_ventas.py       — Fetch raw sales data from KAME API
2. pipeline/clean_sales_main.py  — Clean and standardize data
3. pipeline/enrich_location.py   — Add Region and ServicioSalud
4. pipeline/enrich_product.py    — Add Unegocio from SKU mapping
5. pipeline/save_to_sqlite.py    — Save final data into SQLite DB
"""

import os

# === Import core functions ===
from get_ventas import get_informe_ventas_json
from pipeline import (
    add_location_info,
    add_product_info,
    run_clean_sales_pipeline,
    save_to_sqlite,
)

# === Configuration ===
DATE_FROM = "2024-01-01"
DATE_TO = "2024-01-31"

RAW_PATH = f"test/ventas/raw/ventas_raw_{DATE_FROM}_to_{DATE_TO}.csv"
CLEAN_PATH = "test/ventas/clean/ventas_clean_preview.csv"
ENRICHED_PATH = "test/ventas/clean/ventas_enriched.csv"
PRODUCT_ENRICHED_PATH = "test/ventas/clean/ventas_enriched_product.csv"


def run_full_pipeline():
    """Run the entire VS_KAME_APP pipeline end-to-end."""
    print("\n🧪 Starting VS_KAME_APP pipeline...\n")

    # === STEP 1: Fetch ventas ===
    print("🚀 STEP 1: Fetching ventas from Kame API")
    df_raw = get_informe_ventas_json(DATE_FROM, DATE_TO)
    if df_raw is None or df_raw.empty:
        print("❌ No data fetched. Exiting pipeline.")
        return
    print(f"✅ Raw data fetched ({len(df_raw)} rows)")

    # === STEP 2: Clean sales data ===
    print("\n🧹 STEP 2: Cleaning sales data")
    df_clean = run_clean_sales_pipeline(source_path=RAW_PATH)
    print(f"✅ Cleaned data ready ({len(df_clean)} rows)")

    # === STEP 3: Enrich with location ===
    print("\n🌎 STEP 3: Adding location info")
    df_loc = add_location_info(df_clean)
    os.makedirs(os.path.dirname(ENRICHED_PATH), exist_ok=True)
    df_loc.to_csv(ENRICHED_PATH, index=False)
    print(f"💾 Saved → {ENRICHED_PATH}")

    # === STEP 4: Enrich with product info ===
    print("\n🧩 STEP 4: Adding product info")
    df_prod = add_product_info(df_loc)
    os.makedirs(os.path.dirname(PRODUCT_ENRICHED_PATH), exist_ok=True)
    df_prod.to_csv(PRODUCT_ENRICHED_PATH, index=False)
    print(f"💾 Saved → {PRODUCT_ENRICHED_PATH}")

    # === STEP 5: Save to SQLite ===
    print("\n🗄️ STEP 5: Saving to SQLite database")
    save_to_sqlite()

    print("\n✅ Pipeline completed successfully!\n")


if __name__ == "__main__":
    run_full_pipeline()
# === END get_ventas_main.py ===
