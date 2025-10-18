# === pipeline/enrich_product.py ===
"""
Syncs product information (Nombreunegocio) using the Familia column
from the product master list (lista_articulos_full.csv).
"""

from pathlib import Path

import pandas as pd

PRODUCT_PATH = Path("data/lista_articulos_full.csv")


def sync_product_info(df: pd.DataFrame) -> pd.DataFrame:
    """Update or create 'Nombreunegocio' using Familia from the product master list."""
    print("🧩 Enriching product info from master list...")

    if not PRODUCT_PATH.exists():
        print(f"⚠️ Product master file not found at {PRODUCT_PATH} — skipping.")
        return df

    try:
        df_prod = pd.read_csv(PRODUCT_PATH, dtype=str)
        df_prod.columns = df_prod.columns.str.strip().str.title()

        if "Sku" not in df_prod.columns or "Familia" not in df_prod.columns:
            print("⚠️ Missing 'Sku' or 'Familia' columns in product master — skipped.")
            return df

        # Normalize and deduplicate
        df_prod["Sku"] = df_prod["Sku"].astype(str).str.strip()
        df_prod["Familia"] = df_prod["Familia"].astype(str).str.strip().str.title()
        df_prod = df_prod.drop_duplicates(subset=["Sku"], keep="first")

        # Map SKU → Familia
        sku_to_familia = df_prod.set_index("Sku")["Familia"].to_dict()
        df["Sku"] = df["Sku"].astype(str).str.strip()

        if "Nombreunegocio" in df.columns:
            df["Nombreunegocio"] = df.apply(
                lambda row: sku_to_familia.get(row["Sku"], row["Nombreunegocio"]),
                axis=1,
            )
            print("✅ Updated existing Nombreunegocio from product master.")
        else:
            df["Nombreunegocio"] = df["Sku"].map(sku_to_familia)
            print("✅ Created Nombreunegocio from Familia.")

    except Exception as e:
        print(f"⚠️ Error syncing product info: {e}")

    return df


# === END enrich_product.py ===
