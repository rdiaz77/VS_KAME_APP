# === pipeline/enrich_location.py ===
"""
Adds Region and Serviciosalud information to sales data based on 'Comuna'.
"""

from pathlib import Path

import pandas as pd

# Path to your mapping CSV (adjust if needed)
MAPPING_PATH = Path("data/comunas_provincia_servicio_region(003).csv")


def add_location_info(df: pd.DataFrame) -> pd.DataFrame:
    """Add Region and Serviciosalud columns by merging with the mapping file."""
    print("🌎 Adding Region and Serviciosalud info...")

    if not MAPPING_PATH.exists():
        print(f"⚠️ Mapping file not found at {MAPPING_PATH} — skipping enrichment.")
        return df

    mapping = pd.read_csv(MAPPING_PATH)

    # Normalize columns in mapping file
    mapping.columns = mapping.columns.str.strip().str.title()

    # Standardize comuna column casing
    if "Comuna" not in mapping.columns:
        raise KeyError("Mapping file must contain a 'Comuna' column.")

    # Merge and preserve original Comuna
    df = df.merge(
        mapping[["Comuna", "Region", "Serviciosalud"]],
        on="Comuna",
        how="left",
    )

    print("✅ Added Region and Serviciosalud from mapping file.")
    return df


# === END enrich_location.py ===
