# === clean_sales.py (adds thousand separator for key financial columns) ===
import os

import pandas as pd


def clean_sales(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize KAME sales data.

    CHANGES IN THIS VERSION:
    - Drop 'PorcDescuento'
    - Convert to numeric (treat '.' as decimal separator)
    - Round Cantidad, PrecioUnitario, Descuento, TotalNeto,
      CostoVentaUnitario, CostoVentaTotal, MargenContrib to 0 decimals (integers)
    - Format financial columns with thousand separators
    """
    df = df_raw.copy()

    # 1️⃣ Normalize column names
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()

    # 2️⃣ Drop irrelevant columns
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

    # 3.1️⃣ Drop PorcDescuento
    if "PorcDescuento" in df.columns:
        df.drop(columns=["PorcDescuento"], inplace=True)

    # 4️⃣ Clean text fields
    for col in ["Cliente", "Comuna", "Ciudad", "Producto", "Vendedor"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    # 5️⃣ Convert numeric fields treating '.' as decimal and round to 0 decimals
    numeric_cols = [
        "Cantidad",
        "PrecioUnitario",
        "Descuento",
        "TotalNeto",
        "CostoVentaUnitario",
        "CostoVentaTotal",
        "MargenContrib",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(
                    df[col].astype(str).str.replace(",", "").str.strip(),
                    errors="coerce",
                )
                .round(0)
                .astype("Int64")
            )

    # 6️⃣ Convert Folio & refs to TEXT
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

    # 9️⃣ Add Region and ServicioSalud
    mapping_path = os.path.expanduser(
        "~/Desktop/Python_KAME_ERP/VS_KAME_APP/data/comunas_provincia_servicio_region(003).csv"
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

    # 🔟 Sync Nombreunegocio using Familia (unchanged)
    product_path = os.path.expanduser(
        "~/Desktop/Python_KAME_ERP/VS_KAME_APP/data/lista_articulos_full.csv"
    )
    if os.path.exists(product_path):
        print("🔄 Updating Nombreunegocio using Familia from product master list...")
        try:
            df_products = pd.read_csv(product_path, dtype=str)
            df_products.columns = df_products.columns.str.strip().str.title()

            if "Sku" in df_products.columns and "Familia" in df_products.columns:
                df_products["Sku"] = df_products["Sku"].astype(str).str.strip()
                df_products["Familia"] = (
                    df_products["Familia"].astype(str).str.strip().str.title()
                )
                df["SKU"] = df["SKU"].astype(str).str.strip()

                before = len(df_products)
                df_products = df_products.drop_duplicates(subset=["Sku"], keep="first")
                duplicates_removed = before - len(df_products)
                if duplicates_removed > 0:
                    print(
                        f"ℹ️ Removed {duplicates_removed} duplicate SKU(s) before merge."
                    )

                sku_to_familia = df_products.set_index("Sku")["Familia"].to_dict()

                if "Nombreunegocio" in df.columns:
                    df["Nombreunegocio"] = df.apply(
                        lambda row: sku_to_familia.get(
                            row["SKU"], row["Nombreunegocio"]
                        ),
                        axis=1,
                    )
                    print(
                        f"✅ Nombreunegocio updated using Familia ({len(sku_to_familia):,} SKUs)."
                    )
                else:
                    df["Nombreunegocio"] = df["SKU"].map(sku_to_familia)
                    print(
                        f"✅ Nombreunegocio created from Familia ({len(sku_to_familia):,} SKUs)."
                    )

            else:
                print(
                    "⚠️ Columns 'Sku' or 'Familia' missing in product master — skipping update."
                )
        except Exception as e:
            print(f"⚠️ Error updating Nombreunegocio from master: {e}")
    else:
        print(f"⚠️ Product reference file not found: {product_path}")

    # 💰 Add thousand separator formatting for display columns
    money_cols = [
        "PrecioUnitario",
        "Descuento",
        "TotalNeto",
        "CostoVentaUnitario",
        "CostoVentaTotal",
        "MargenContrib",
    ]
    for col in money_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else None)

    # 🧱 Final cleanup
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    print("🧹 Cleaned data — financial columns formatted with thousand separators.")
    return df


if __name__ == "__main__":
    df_test = pd.read_csv("source/ventas_raw_2024-01-01_to_2024-01-31.csv")
    df_clean = clean_sales(df_test)
    print("✅ Final columns:", df_clean.columns.tolist())
    os.makedirs("data", exist_ok=True)
    df_clean.to_csv("data/ventas_clean_preview.csv", index=False)
    print("💾 Preview saved to data/ventas_clean_preview.csv")
# === END clean_sales.py ===
