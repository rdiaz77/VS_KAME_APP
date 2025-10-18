# === pipeline/clean_sales_main.py ===
"""
Main entry point for the VitroScience sales data cleaning pipeline.
This module orchestrates the cleaning, enrichment, and formatting steps.
"""

import pandas as pd

from pipeline.cleaning_utils import basic_cleaning
from pipeline.enrich_location import add_location_info
from pipeline.enrich_product import sync_product_info
from pipeline.formatting_utils import format_numbers


def clean_sales(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Run full cleaning pipeline on raw sales dataframe."""
    print("🚀 Starting cleaning pipeline...")

    df = basic_cleaning(df_raw)
    df = add_location_info(df)
    df = sync_product_info(df)
    df = format_numbers(df)

    print(f"🧹 Finished cleaning pipeline — {len(df)} rows, {len(df.columns)} columns.")
    return df


if __name__ == "__main__":
    # Optional: demo run for testing
    from pathlib import Path

    input_path = Path("source/ventas_raw_2024-01-01_to_2024-01-31.csv")
    if input_path.exists():
        print(f"📂 Loading {input_path.name} ...")
        df_raw = pd.read_csv(input_path)
        df_clean = clean_sales(df_raw)
        output_path = Path("data/ventas_clean_preview.csv")
        df_clean.to_csv(output_path, index=False)
        print(f"💾 Saved cleaned preview → {output_path}")
    else:
        print("⚠️ No input file found. Please run get_ventas first.")
# === END clean_sales_main.py ===
