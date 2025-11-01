# === cobrar/clean_cta_por_cobrar.py (final version with typed numeric & date fields) ===
import os

import pandas as pd
from unidecode import unidecode


def clean_cta_por_cobrar(
    input_path="test/cobranza/raw/cuentas_por_cobrar_monthly_from_Jan_023.csv",
    output_path="test/cobranza/clean/cuentas_por_cobrar_clean.csv",
):
    """
    Clean and standardize 'Cuentas por Cobrar' dataset fetched from KAME API.

    Steps:
    - Drops unneeded column: NombreCuenta
    - Normalizes text fields (no accents)
    - Converts numeric columns to integers
    - Parses and standardizes date columns
    - Removes trailing ".0" from FolioDocumento
    - Saves UTF-8 with BOM for Excel & DB compatibility
    """

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"❌ Input file not found: {input_path}")

    print(f"🧹 Cleaning file: {input_path}")
    df = pd.read_csv(input_path, dtype=str)

    # === Drop unnecessary column ===
    drop_cols = ["NombreCuenta"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # === Remove trailing ".0" from FolioDocumento ===
    if "FolioDocumento" in df.columns:
        df["FolioDocumento"] = (
            df["FolioDocumento"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )

    # === Normalize text fields ===
    text_cols = ["RznSocial", "NombreVendedor", "Documento", "CondicionVenta"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("").apply(unidecode).str.strip()

    # === Convert numeric columns to integers ===
    num_cols = ["Total", "TotalCP", "Saldo"]
    for col in num_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[^0-9\-]", "", regex=True)  # keep digits and negatives
                .replace("", "0")
                .astype(float)
                .round(0)
                .astype(int)
            )

    # === Parse and standardize date columns ===
    date_cols = ["Fecha", "FechaVencimiento", "MonthFetched", "SnapshotDate"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # === Save cleaned file ===
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ Cleaned data saved successfully: {output_path}")
    print(f"🧾 Rows: {len(df):,}")
    print(f"📊 Columns: {', '.join(df.columns)}")
    return df


# 🧪 Standalone run
if __name__ == "__main__":
    clean_cta_por_cobrar()
# === END clean_cta_por_cobrar.py ===
