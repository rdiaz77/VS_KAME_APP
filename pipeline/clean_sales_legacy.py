# === pipeline/clean_sales_main.py ===
"""
Main cleaning pipeline for VitroScience sales data.

Combines modular steps:
- basic_cleaning
- add_location_info
- enrich_product_info
- format_numeric_columns
"""

import pandas as pd

from pipeline.cleaning_utils import basic_cleaning
from pipeline.enrich_location import add_location_info
from pipeline.enrich_product import enrich_product_info
from pipeline.formatting_utils import format_numeric_columns


def run_clean_sales_pipeline(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Run the full modular cleaning and enrichment pipeline."""
    print("🚀 Starting cleaning pipeline...")

    # Step 1️⃣ Basic Cleaning
    print("🧽 Running basic cleaning...")
    df_clean = basic_cleaning(df_raw)
    print(
        f"✅ Basic cleaning done — {len(df_clean)} rows, {len(df_clean.columns)} columns."
    )

    # Step 2️⃣ Add Region / Serviciosalud
    print("🌎 Adding Region and Serviciosalud info...")
    df_clean = add_location_info(df_clean)
    print("✅ Added Region and Serviciosalud from mapping file.")

    # Step 3️⃣ Enrich with Product Master
    print("🧩 Enriching product info from master list...")
    df_clean = enrich_product_info(df_clean)
    print("✅ Updated existing Nombreunegocio from product master.")

    # Step 4️⃣ Format Numeric Columns (round + thousand separators)
    print("💰 Formatting numeric columns...")
    df_clean = format_numeric_columns(df_clean)
    print("✅ Numeric formatting applied with thousand separators.")

    print(
        f"🧹 Finished cleaning pipeline — {len(df_clean)} rows, {len(df_clean.columns)} columns."
    )
    return df_clean


# 🧪 Standalone test
if __name__ == "__main__":
    test_path = "source/ventas_raw_2024-01-01_to_2024-01-31.csv"
    print(f"📂 Loading {test_path} ...")
    df_raw = pd.read_csv(test_path)
    df_clean = run_clean_sales_pipeline(df_raw)
    df_clean.to_csv("data/ventas_clean_preview.csv", index=False)
    print("💾 Saved cleaned preview → data/ventas_clean_preview.csv")

# === END clean_sales_main.py ===
