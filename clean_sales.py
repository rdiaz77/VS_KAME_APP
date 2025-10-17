import pandas as pd
import numpy as np

def clean_sales(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize KAME sales data.
    - Drops only the requested columns
    - Rounds numeric fields safely (sign preserved)
    - Converts Folio and FolioRef columns to text (no decimals)
    """
    df = df_raw.copy()

    # 1️⃣ Normalize column names
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()

    # 2️⃣ Drop only the requested columns
    drop_only = [
        "multidirnombre",
        "multidirdireccion",
        "multidirciudad",
        "multidircomuna",
        "multidircontacto",
        "familianombre",
        "unidadequivalente",
        "factorunidadequivalente",
        "porcmargencontrib",
        "margenventassobrecosto",
    ]
    df.drop(columns=[c for c in drop_only if c in df.columns], inplace=True, errors="ignore")

    # 3️⃣ Rename key columns
    rename_map = {
        "rut": "ClienteRut",
        "rznsocial": "Cliente",
        "fecha": "Fecha",
        "folio": "Folio",
        "comuna": "Comuna",
        "ciudad": "Ciudad",
        "descripcion": "Producto",
        "sku": "SKU",
        "cantidad": "Cantidad",
        "preciounitario": "PrecioUnitario",
        "descuento": "Descuento",
        "porcdescuento": "PorcDescuento",
        "total": "TotalNeto",
        "costoventaunitario": "CostoVentaUnitario",
        "costoventatotal": "CostoVentaTotal",
        "margencontrib": "MargenContrib",
        "folioref1": "FolioRef1",
        "folioref2": "FolioRef2",
        "folioref3": "FolioRef3",
        "nombrevendedor": "Vendedor",
    }
    df.rename(columns=rename_map, inplace=True)

    # 4️⃣ Clean text fields (title case)
    for col in ["Cliente", "Comuna", "Ciudad", "Producto", "Vendedor"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    # 5️⃣ Round numeric fields (preserve sign, remove decimals)
    round_cols = [
        "Cantidad",
        "PrecioUnitario",
        "Descuento",
        "PorcDescuento",
        "TotalNeto",
        "CostoVentaUnitario",
        "CostoVentaTotal",
        "MargenContrib",
    ]
    for col in round_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = np.trunc(df[col].fillna(0)).astype(int)

    # 6️⃣ Convert Folio and reference fields to text (no decimals, no loss)
    folio_cols = ["Folio", "FolioRef1", "FolioRef2", "FolioRef3"]
    for col in folio_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(int(x)) if pd.notnull(x) and str(x).replace('.', '').isdigit() else str(x).strip())

    # 7️⃣ Convert date
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")

    # 8️⃣ Drop empties/duplicates
    subset = [c for c in ["Folio", "Fecha", "Cliente"] if c in df.columns]
    if subset:
        df.dropna(subset=subset, inplace=True)
    if {"Folio", "Producto", "Fecha"}.issubset(df.columns):
        df.drop_duplicates(subset=["Folio", "Producto", "Fecha"], inplace=True)

    print(f"🧹 Cleaned {len(df)} rows, {len(df.columns)} columns — Folio and refs stored as text.")
    return df
# === END clean_sales.py ===