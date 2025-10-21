import os

import pandas as pd

# Get the folder where this script lives
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def add_location_info(df: pd.DataFrame, mapping_path: str = None) -> pd.DataFrame:
    if mapping_path is None:
        mapping_path = os.path.join(
            BASE_DIR, "../data/comunas_provincia_servicio_region(003).csv"
        )

    print(f"🌎 Enriching with Region and ServicioSalud from {mapping_path} ...")

    if not os.path.exists(mapping_path):
        print(f"❌ Mapping file not found: {mapping_path}")
        return df

    mapping = pd.read_csv(mapping_path)
    mapping.columns = mapping.columns.str.strip()

    mapping["Comuna_norm"] = mapping["Comuna"].astype(str).str.strip().str.title()
    df["Comuna_norm"] = df["Comuna"].astype(str).str.strip().str.title()

    df_merged = df.merge(
        mapping[["Comuna_norm", "Region", "ServicioSalud"]],
        on="Comuna_norm",
        how="left",
    ).drop(columns=["Comuna_norm"])

    if "Ciudad" in df_merged.columns:
        cols = df_merged.columns.tolist()
        insert_idx = cols.index("Ciudad") + 1
        for new_col in ["Region", "ServicioSalud"]:
            if new_col in cols:
                cols.remove(new_col)
                cols.insert(insert_idx, new_col)
                insert_idx += 1
        df_merged = df_merged[cols]

    matched = df_merged["Region"].notna().sum()
    total = len(df_merged)
    print(f"✅ Location enrichment complete — matched {matched} of {total} comunas.")
    return df_merged


if __name__ == "__main__":
    # Build absolute paths safely
    input_path = os.path.join(BASE_DIR, "../test/ventas/clean/ventas_clean_preview.csv")
    output_path = os.path.join(BASE_DIR, "../test/ventas/clean/ventas_enriched.csv")
    mapping_path = os.path.join(
        BASE_DIR, "../data/comunas_provincia_servicio_region(003).csv"
    )

    if not os.path.exists(input_path):
        print(f"❌ Input file not found: {input_path}")
        print("🔎 Please check that the path and filename are correct.")
    else:
        print(f"📂 Loading {input_path} ...")
        df = pd.read_csv(input_path)
        df_enriched = add_location_info(df, mapping_path=mapping_path)
        df_enriched.to_csv(output_path, index=False)
        print(f"💾 Enriched file saved to {output_path}")
