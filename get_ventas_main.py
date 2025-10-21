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
"""

import os
import sys

# === Import pipeline components ===
from get_ventas import get_informe_ventas_json
from pipeline import (
    add_location_info,
    add_product_info,
    run_clean_sales_pipeline,
    save_to_sqlite,
)


def run_full_pipeline(fecha_desde: str, fecha_hasta: str):
    """Run the entire VS_KAME_APP sales data pipeline."""
    print("\n🧪 Starting VS_KAME_APP pipeline...\n")

    raw_path = f"test/ventas/raw/ventas_raw_{fecha_desde}_to_{fecha_hasta}.csv"
    enriched_path = "test/ventas/clean/ventas_enriched.csv"
    product_enriched_path = "test/ventas/clean/ventas_enriched_product.csv"

    # === STEP 1: Fetch ventas ===
    print("🚀 STEP 1: Fetching ventas from Kame API")
    df_raw = get_informe_ventas_json(fecha_desde, fecha_hasta)
    if df_raw is None or df_raw.empty:
        print("❌ No data fetched. Exiting pipeline.")
        return
    print(f"✅ Raw data fetched ({len(df_raw)} rows)")

    # === STEP 2: Clean sales data ===
    print("\n🧹 STEP 2: Cleaning sales data")
    df_clean = run_clean_sales_pipeline(source_path=raw_path)
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


if __name__ == "__main__":
    # === Optional command-line date arguments ===
    if len(sys.argv) == 3:
        DATE_FROM, DATE_TO = sys.argv[1], sys.argv[2]
    else:
        DATE_FROM, DATE_TO = "2024-01-01", "2024-01-31"
        print(f"⚙️ No dates provided — defaulting to {DATE_FROM} → {DATE_TO}")

    run_full_pipeline(DATE_FROM, DATE_TO)

# === END get_ventas_main.py ===
