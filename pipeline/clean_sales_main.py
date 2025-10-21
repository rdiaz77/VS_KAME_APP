# === Drop unnecessary columns / round numbers / add thousand separators / standardize text / fix Folio ===
import os
import unicodedata

import pandas as pd

# === Path handling (robust to invisible Unicode or path issues) ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def resolve_source_path(relative_path: str) -> str:
    """Return an absolute path to the source CSV, handling Unicode or path mismatches safely."""
    base = os.path.join(BASE_DIR, "..", relative_path)
    if os.path.exists(base):
        return base

    # Try to find any .csv file in /test/ventas/clean that looks like the ventas_raw file
    source_dir = os.path.join(BASE_DIR, "..", "test", "ventas", "raw")
    for f in os.listdir(source_dir):
        nf = unicodedata.normalize("NFC", f)
        if f.endswith(".csv") and "ventas_raw" in nf and "2024" in nf:
            print(f"⚙️ Found CSV candidate: {f}")
            return os.path.join(source_dir, f)

    print(f"❌ File not found: {base}")
    return base


def run_clean_sales_pipeline(
    source_path: str = "test/ventas/raw/ventas_raw_2024-01-01_to-2024-01-31.csv",
):
    """Drop unnecessary columns (keeping RUT), round numeric values, add thousand separators (with commas),
    standardize text capitalization, and convert 'Folio' to clean text."""
    full_path = resolve_source_path(source_path)
    print(f"📂 Loading {full_path} ...")

    df = pd.read_csv(full_path)

    # === Drop unnecessary columns ===
    drop_cols = [
        "MultiDirNombre",
        "MultiDirDireccion",
        "MultiDirCiudad",
        "MultiDirComuna",
        "MultiDirContacto",
        "NombreUNegocio",
        "FamiliaNombre",
        "UnidadEquivalente",
        "FactorUnidadEquivalente",
        "FechaVencimientoLote",
        "PorcDescuento",
        "PorcMargenContrib",
        "MargenVentasSobreCosto",
    ]

    print(f"🧹 Dropping {len(drop_cols)} unnecessary columns (keeping 'Rut')...")
    df_clean = df.drop(
        columns=[c for c in drop_cols if c in df.columns], errors="ignore"
    )

    # === Round and format numeric columns ===
    num_cols = [
        "PrecioUnitario",
        "Descuento",
        "Total",
        "CostoVentaUnitario",
        "CostoVentaTotal",
        "MargenContrib",
    ]

    for col in num_cols:
        if col in df_clean.columns:
            df_clean[col] = (
                pd.to_numeric(df_clean[col], errors="coerce").round(0).astype("Int64")
            )
            df_clean[col] = df_clean[col].apply(
                lambda x: f"{x:,}" if pd.notnull(x) else ""
            )

    # === Standardize text capitalization ===
    text_cols = ["RznSocial", "Direccion", "Comuna", "Ciudad"]
    for col in text_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip().str.title()

    # === Convert Folio to text and remove '.0' ===
    if "Folio" in df_clean.columns:
        df_clean["Folio"] = (
            df_clean["Folio"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )

    # === Save output ===
    output_path = os.path.join(
        BASE_DIR, "..", "test", "ventas", "clean", "ventas_clean_preview.csv"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_clean.to_csv(output_path, index=False)
    print(f"💾 Saved cleaned CSV → {output_path}")
    print(f"✅ Final shape: {df_clean.shape[0]} rows, {df_clean.shape[1]} columns")

    return df_clean


if __name__ == "__main__":
    run_clean_sales_pipeline()

# === END clean_sales_main.py ===
