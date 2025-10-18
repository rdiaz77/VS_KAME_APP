# === pipeline/cleaning_utils.py ===
"""
Basic cleaning utilities for VitroScience sales data.
Handles column normalization, dropping unused columns, and conversions.
"""

import pandas as pd


def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Perform initial cleaning of the raw sales DataFrame."""
    print("🧽 Running basic cleaning...")

    # Normalize column names
    df.columns = df.columns.str.strip().str.title()

    # Drop unused columns if present
    drop_cols = [
        "Descripciondetallada",
        "Usaminimorentabilidad",
        "Minimorentabilidad",
        "Porcdescuento",  # as per your last instructions
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # Convert numeric fields (use '.' as decimal and ',' as thousand separator)
    num_cols = [
        "Cantidad",
        "Preciounitario",
        "Descuento",
        "Totalneto",
        "Costoventaunitario",
        "Costoventatotal",
        "Margencontrib",
    ]

    for col in num_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)  # remove thousand separators
                .str.replace(",", ".", regex=False)  # convert comma decimal → dot
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").round(0)

    print(f"✅ Basic cleaning done — {len(df)} rows, {len(df.columns)} columns.")
    return df


# === END cleaning_utils.py ===
