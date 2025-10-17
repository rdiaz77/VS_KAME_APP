import os

import numpy as np
import pandas as pd


def clean_sales(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize KAME sales data.
    - Drops only the requested columns
    - Rounds numeric fields safely (sign preserved)
    - Converts Folio & refs to text (no decimals)
    - Adds Region and ServicioSalud by matching Comuna (preserving Comuna)
    - Reorders columns so Region and ServicioSalud appear after Ciudad
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
    df.drop(
        columns=[c for c in drop_only if c in df.columns], inplace=True, errors="ignore"
    )

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

    # 4️⃣ Clean text fields (title-case)
    for col in ["Cliente", "Comuna", "Ciudad", "Producto", "Vendedor"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    # 5️⃣ Round numeric fields (preserve sign)
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

    # 6️⃣ Convert Folio & refs to TEXT (no decimals)
    for col in ["Folio", "FolioRef1", "FolioRef2", "FolioRef3"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: (
                    str(int(x))
                    if pd.notnull(x) and str(x).replace(".", "").isdigit()
                    else str(x).strip()
                )
            )

    # 7️⃣ Convert date
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")

    # 8️⃣ Drop empties / duplicates
    subset = [c for c in ["Folio", "Fecha", "Cliente"] if c in df.columns]
    if subset:
        df.dropna(subset=subset, inplace=True)
    if {"Folio", "Producto", "Fecha"}.issubset(df.columns):
        df.drop_duplicates(subset=["Folio", "Producto", "Fecha"], inplace=True)

    # 9️⃣ Add Region and ServicioSalud (preserving Comuna)
    mapping_path = os.path.expanduser(
        "~/Desktop/Python_KAME_ERP/VS_KAME_APP/data/comunas_provincia_servicio_region(002).csv"
    )

    if os.path.exists(mapping_path):
        comunas_map = pd.read_csv(mapping_path)
        comunas_map.columns = comunas_map.columns.str.strip().str.title()
        comunas_map = comunas_map.loc[:, ~comunas_map.columns.duplicated(keep="first")]

        comuna_col = next((c for c in comunas_map.columns if "Comuna" in c), None)
        region_col = next((c for c in comunas_map.columns if "Region" in c), None)
        servicio_col = next((c for c in comunas_map.columns if "Servicio" in c), None)

        if comuna_col and region_col and servicio_col:
            comunas_ref = comunas_map[[comuna_col, region_col, servicio_col]].copy()
            comunas_ref.columns = ["Comuna", "Region", "ServicioSalud"]
            comunas_ref["Comuna"] = (
                comunas_ref["Comuna"].astype(str).str.strip().str.title()
            )
            df["Comuna"] = df["Comuna"].astype(str).str.strip().str.title()
            df = pd.merge(df, comunas_ref, on="Comuna", how="left", validate="m:1")
            print(
                "🌎 Added Region and ServicioSalud columns from mapping file (Comuna preserved)."
            )
        else:
            print(
                "⚠️ Mapping file missing expected columns: Comuna, Region, ServicioSalud."
            )
            df["Region"] = None
            df["ServicioSalud"] = None
    else:
        print(f"⚠️ Mapping file not found at: {mapping_path}")
        df["Region"] = None
        df["ServicioSalud"] = None

    # 🔟 Reorder columns so Region and ServicioSalud appear after Ciudad
    desired_order = []
    if "Ciudad" in df.columns:
        for col in df.columns:
            desired_order.append(col)
            if col == "Ciudad":
                if "Region" in df.columns:
                    desired_order.append("Region")
                if "ServicioSalud" in df.columns:
                    desired_order.append("ServicioSalud")
        # Drop duplicates in case Region/ServicioSalud already exist later
        df = df[[c for c in desired_order if c in df.columns]]

    # Remove any hidden duplicate column names (causes SQLAlchemy DuplicateColumnError)
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    print(
        f"🧹 Cleaned {len(df)} rows, {len(df.columns)} unique columns — enriched with Region & ServicioSalud."
    )
    return df


# Optional standalone test
if __name__ == "__main__":
    df_test = pd.read_csv("source/ventas_raw_2024-01-01_to_2024-01-31.csv")
    df_clean = clean_sales(df_test)
    print("✅ Final columns (ordered):", df_clean.columns.tolist())
    print(df_clean.head())
# === END clean_sales.py ===
