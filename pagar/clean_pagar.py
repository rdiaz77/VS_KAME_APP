# === clean_pagar.py (final version with formatted numbers) ===
import os

import pandas as pd


def clean_pagar(
    input_path="test/pagar/raw/cuentas_por_pagar_full.csv",
    output_path="test/pagar/clean/cuentas_por_pagar_clean.csv",
):
    """
    Clean the 'Cuentas por Pagr' (collection) dataset.

    Steps:
    - Drops unnecessary columns: MultiDirNombre
    - Converts 'FolioDocumento' to text and removes trailing '.0'
    - Converts numeric columns (Total, TotalCP, Saldo) to integers
    - Formats numeric columns with thousand separators (no decimals)
    - Saves the cleaned dataset as UTF-8 with BOM for Excel compatibility
    """

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"❌ Input file not found: {input_path}")

    print(f"🧹 Cleaning file: {input_path}")
    df = pd.read_csv(input_path, dtype=str)

    # Drop unnecessary columns
    drop_cols = ["MultiDirNombre"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # Convert 'FolioDocumento' to text and remove '.0'
    if "FolioDocumento" in df.columns:
        df["FolioDocumento"] = (
            df["FolioDocumento"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )

    # Convert numeric columns and format them
    num_cols = ["Total", "TotalCP", "Saldo"]
    for col in num_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[^0-9\-]", "", regex=True)  # remove non-numeric chars
                .replace("", "0")
                .astype(float)
                .round(0)
                .astype(int)
                .map("{:,}".format)  # add thousand separators
            )

    # Save cleaned data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ Cleaned file saved successfully: {output_path}")
    return df


# 🧪 Standalone run
if __name__ == "__main__":
    df_clean = clean_pagar()
    print(f"✅ Done. Cleaned {len(df_clean)} rows.")
# === END clean_pagar.py ===
