import os

import pandas as pd

# Get the folder where this script lives
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def add_product_info(
    df: pd.DataFrame, product_path: str = None, unmatched_output: str = None
) -> pd.DataFrame:
    if product_path is None:
        product_path = os.path.join(BASE_DIR, "../data/lista_articulos_clean.csv")

    if unmatched_output is None:
        unmatched_output = os.path.join(
            BASE_DIR, "../test/ventas/unmatched/unmatched_skus.csv"
        )

    print(f"🧩 Enriching with product info from {product_path} ...")

    if not os.path.exists(product_path):
        print(f"❌ Product file not found: {product_path}")
        return df

    # === Load product mapping ===
    product_map = pd.read_csv(product_path)
    product_map.columns = product_map.columns.str.strip()

    # Normalize SKU columns for merge
    product_map["SKU_norm"] = product_map["SKU"].astype(str).str.strip().str.upper()
    df["SKU_norm"] = df["SKU"].astype(str).str.strip().str.upper()

    # Merge on normalized SKU
    df_merged = df.merge(
        product_map[["SKU_norm", "Familia"]], on="SKU_norm", how="left"
    ).drop(columns=["SKU_norm"])

    # Rename Familia -> Unegocio
    df_merged = df_merged.rename(columns={"Familia": "Unegocio"})

    # Move Unegocio right after SKU if SKU exists
    if "SKU" in df_merged.columns:
        cols = df_merged.columns.tolist()
        insert_idx = cols.index("SKU") + 1
        if "Unegocio" in cols:
            cols.remove("Unegocio")
            cols.insert(insert_idx, "Unegocio")
        df_merged = df_merged[cols]

    # === Assign default Unegocio for missing SKU ===
    df_merged.loc[df_merged["SKU"].astype(str).str.strip().eq(""), "Unegocio"] = (
        "Casa Matriz"
    )

    # === Report summary ===
    matched = df_merged["Unegocio"].notna().sum()
    total = len(df_merged)
    print(f"✅ Product enrichment complete — matched {matched} of {total} SKUs.")

    # === Show and save unmatched SKUs for debugging ===
    unmatched = df_merged.loc[df_merged["Unegocio"].isna(), "SKU"].dropna().unique()
    if len(unmatched) > 0:
        print(f"⚠️ {len(unmatched)} SKUs not found in product mapping. Example(s):")
        for sku in unmatched[:10]:
            print(f"   - {sku}")

        # Save unmatched SKUs for audit
        unmatched_df = pd.DataFrame({"Unmatched_SKU": unmatched})
        unmatched_df.to_csv(unmatched_output, index=False)
        print(f"💾 Unmatched SKUs saved to {unmatched_output}")
    else:
        print("🎉 All SKUs matched successfully!")

    return df_merged


if __name__ == "__main__":
    # Build absolute paths safely
    input_path = os.path.join(BASE_DIR, "../test/ventas/clean/ventas_enriched.csv")
    output_path = os.path.join(
        BASE_DIR, "../test/ventas/clean/ventas_enriched_product.csv"
    )
    product_path = os.path.join(BASE_DIR, "../data/lista_articulos_clean.csv")
    unmatched_output = os.path.join(
        BASE_DIR, "../test/ventas/unmatched/unmatched_skus.csv"
    )

    if not os.path.exists(input_path):
        print(f"❌ Input file not found: {input_path}")
        print("🔎 Please check that the path and filename are correct.")
    else:
        print(f"📂 Loading {input_path} ...")
        df = pd.read_csv(input_path)
        df_enriched = add_product_info(
            df, product_path=product_path, unmatched_output=unmatched_output
        )
        df_enriched.to_csv(output_path, index=False)
        print(f"💾 Enriched file saved to {output_path}")
# End of file: pipeline/enrich_product.py
