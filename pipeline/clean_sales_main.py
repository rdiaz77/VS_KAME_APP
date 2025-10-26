# === pipeline/clean_sales_main.py ===
import os
import pandas as pd
from unidecode import unidecode


def run_clean_sales_pipeline(source_path: str = None):
    """
    Clean and standardize KAME sales data.
    - Drops only the specified unnecessary columns (keeping Rut)
    - Rounds numeric values (keeps them as numeric types)
    - Normalizes text columns: RznSocial, Direccion, Comuna, Ciudad, Region, ServicioSalud
    - Removes accents from several key text fields
    - Converts Folio to text and removes trailing '.0'
    - Saves output to /test/ventas/clean/ventas_clean_preview.csv
    """

    # === Resolve file path safely ===
    base_dir = os.path.dirname(os.path.abspath(__file__))

    if source_path is None:
        candidate_full = os.path.join(base_dir, "../test/ventas/raw/ventas_raw.csv")
        candidate_fallback = os.path.join(
            base_dir, "../test/ventas/raw/ventas_raw_2024-01-01_to_2024-01-31.csv"
        )

        if os.path.exists(candidate_full):
            source_path = candidate_full
        else:
            source_path = candidate_fallback

    print(f"📂 Loading {source_path} ...")

    if not os.path.exists(source_path):
        print(f"❌ File not found: {source_path}")
        return None

    df = pd.read_csv(source_path)

    # === Normalize column names ===
    df.columns = df.columns.str.strip().str.replace("\ufeff", "", regex=True)

    # === Drop only the specified unnecessary columns (keep Rut) ===
    drop_only = [
        "MultiDirNombre",
        "MultiDirDireccion",
        "MultiDirCiudad",
        "MultiDirComuna",
        "MultiDirContacto",
        "FamiliaNombre",
        "UnidadEquivalente",
        "FactorUnidadEquivalente",
        "Lote",
        "FechaVencimientoLote",
        "PorcMargenContrib",
        "MargenVentasSobreCosto",
    ]

    existing_to_drop = [c for c in drop_only if c in df.columns]
    print(f"🗑️ Dropping columns: {existing_to_drop}")

    df = df.drop(columns=existing_to_drop, errors="ignore")

    # === Round numeric columns (KEEP AS NUMERIC) ===
    numeric_cols = [
        "PrecioUnitario",
        "Descuento",
        "Total",
        "CostoVentaUnitario",
        "CostoVentaTotal",
        "MargenContrib",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(0)

    # === Normalize text columns (capitalize each word) ===
    text_cols = [
        "RznSocial",
        "Direccion",
        "Comuna",
        "Ciudad",
        "Region",
        "ServicioSalud",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower().str.title()

    # === Remove accents from specified columns ===
    accent_cols = [
        "RznSocial",
        "NombreDocumento",
        "Direccion",
        "Comuna",
        "Ciudad",
        "DescripcionDetallada",
        "Comentario",
        "NombreRef1",
        "RazonRef1",
        "NombreRef2",
        "FechaRef2",
        "FolioRef2",
        "RazonRef2",
        "NombreRef3",
        "FechaRef3",
        "FolioRef3",
        "RazonRef3",
    ]
    for col in accent_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(lambda x: unidecode(x))

    # === Convert Folio to text and remove trailing ".0" ===
    if "Folio" in df.columns:
        df["Folio"] = (
            df["Folio"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        )

    # === Save output ===
    output_path = os.path.join(
        base_dir, "../test/ventas/clean/ventas_clean_preview.csv"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"💾 Saved cleaned file to {output_path}")
    print("✅ Accents removed and numeric columns remain numeric.")
    print("🧾 Columns after cleaning:")
    print(df.columns.tolist())

    return df


if __name__ == "__main__":
    run_clean_sales_pipeline()

## === End of pipeline/clean_sales_main.py ===
