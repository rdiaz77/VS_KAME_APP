# === pipeline/formatting_utils.py ===
"""
Handles numeric formatting (thousand separators, rounding, etc.)
for cleaned sales data.
"""

import pandas as pd


def format_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """Add thousand separators and ensure numeric consistency."""
    print("💰 Formatting numeric columns...")

    cols_to_format = [
        "Preciounitario",
        "Descuento",
        "Totalneto",
        "Costoventaunitario",
        "Costoventatotal",
        "Margencontrib",
    ]

    for col in cols_to_format:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(0)
            df[col] = df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")

    print("✅ Numeric formatting applied with thousand separators.")
    return df


# === END formatting_utils.py ===
