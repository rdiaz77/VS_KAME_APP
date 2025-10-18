# === Drop unnecessary columns / round numbers / add thousand separators / standardize text / fix Folio ===
import pandas as pd

def run_clean_sales_pipeline(source_path: str = "source/ventas_raw_2024-01-01_to-2024-01-31.csv"):
    """Drop unnecessary columns (keeping RUT), round numeric values, add thousand separators (with commas),
    standardize text capitalization, and convert 'Folio' to clean text."""
    print(f"📂 Loading {source_path} ...")
    df = pd.read_csv(source_path)

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
    df_clean = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

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
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").round(0).astype("Int64")
            df_clean[col] = df_clean[col].apply(lambda x: f"{x:,}" if pd.notnull(x) else "")

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
    output_path = "test/ventas_clean_preview.csv"
    df_clean.to_csv(output_path, index=False)
    print(f"💾 Saved cleaned CSV → {output_path}")
    print(f"✅ Final shape: {df_clean.shape[0]} rows, {df_clean.shape[1]} columns")

    return df_clean


if __name__ == "__main__":
    run_clean_sales_pipeline()
# === END clean_sales_main.py ===
