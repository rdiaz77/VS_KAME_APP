# === pipeline/clean_sales_main.py ===
import os

import pandas as pd

from .cleaning_utils import normalize_columns, rename_and_clean_numeric
from .enrich_location import add_region_and_servicio
from .enrich_product import update_nombreunegocio
from .formatting_utils import apply_thousand_separator


def clean_sales(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Main sales cleaning pipeline."""

    # 1️⃣ Normalize and rename columns
    df = normalize_columns(df_raw)
    df = rename_and_clean_numeric(df)

    # 2️⃣ Enrich with location info
    df = add_region_and_servicio(df)

    # 3️⃣ Enrich with product family data
    df = update_nombreunegocio(df)

    # 4️⃣ Format financial columns for readability
    df = apply_thousand_separator(df)

    print(f"🧹 Finished cleaning pipeline — {len(df)} rows, {len(df.columns)} columns.")
    return df


if __name__ == "__main__":
    path = "source/ventas_raw_2024-01-01_to_2024-01-31.csv"
    df_raw = pd.read_csv(path)
    df_clean = clean_sales(df_raw)
    os.makedirs("data", exist_ok=True)
    df_clean.to_csv("data/ventas_clean_preview.csv", index=False)
    print("💾 Saved cleaned preview → data/ventas_clean_preview.csv")
# === END clean_sales_main.py ===
