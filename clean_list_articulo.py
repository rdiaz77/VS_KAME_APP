# === clean_list_articulo.py (final version) ===
import pandas as pd
import os


def clean_list_articulos(
    input_path="data/lista_articulos_full.csv",
    output_path="data/lista_articulos_clean.csv"
):
    """
    Clean the full list of artículos retrieved from KAME.

    Steps:
    - Drops unnecessary columns
    - Keeps SKU as text (no modification)
    - Saves the cleaned dataset to data/lista_articulos_clean.csv
    """

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"❌ Input file not found: {input_path}")

    print(f"🧹 Cleaning file: {input_path}")
    df = pd.read_csv(input_path, dtype=str)

    # Drop unnecessary columns
    drop_cols = ["DescripcionDetallada", "UsaMinimoRentabilidad", "MinimoRentabilidad"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # Ensure SKU remains text (no trimming or conversion)
    if "Sku" in df.columns:
        df["Sku"] = df["Sku"].astype(str)

    # Save cleaned data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ Cleaned file saved successfully: {output_path}")
    return df


# 🧪 Standalone run
if __name__ == "__main__":
    df_clean = clean_list_articulos()
    print(f"✅ Done. Cleaned {len(df_clean)} rows.")
# === END clean_list_articulo.py ===
