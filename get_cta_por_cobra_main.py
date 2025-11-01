# === get_cta_cobrar_main.py ===
"""
VitroScience – Automated 'Cuentas por Cobrar' Pipeline

Workflow:
1️⃣ Fetch all outstanding invoices from KAME API (2023-01-01 → today)
2️⃣ Save raw data to /test/cobranza/raw
3️⃣ Clean and format → /test/cobranza/clean
4️⃣ Save both snapshot + history into SQLite → data/vitroscience.db
"""

import os
from datetime import datetime

from cobrar.clean_cta_por_cobrar import clean_cta_por_cobrar
from cobrar.cta_por_cobrar_save_db import save_cta_por_cobrar_to_db

# === Local imports ===
from get_cta_por_cobrar import get_cuentas_por_cobrar, save_if_changed

# === PATHS ===
RAW_PATH = "test/cobranza/raw/cuentas_por_cobrar_monthly_from_Jan_023.csv"
CLEAN_PATH = "test/cobranza/clean/cuentas_por_cobrar_clean.csv"
DB_PATH = "data/vitroscience.db"


def run_cta_por_cobrar_pipeline():
    """Run the full CxC baseline + update pipeline."""
    print("🚀 Starting 'Cuentas por Cobrar' pipeline...")

    # --- Step 1: Fetch from API ---
    df_raw = get_cuentas_por_cobrar(fecha_desde="2023-01-01")
    if df_raw.empty:
        print("⚠️ No data retrieved from API. Exiting.")
        return

    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)
    save_if_changed(df_raw, RAW_PATH)
    print(f"💾 Raw data saved to: {RAW_PATH}")

    # --- Step 2: Clean ---
    df_clean = clean_cta_por_cobrar(input_path=RAW_PATH, output_path=CLEAN_PATH)
    print(f"🧹 Cleaned file saved to: {CLEAN_PATH}")

    # --- Step 3: Save to SQLite ---
    save_cta_por_cobrar_to_db(input_path=CLEAN_PATH, db_path=DB_PATH)

    print("\n✅ Pipeline completed successfully.")
    print(f"📦 Total invoices processed: {len(df_clean)}")
    print(f"🗓️ Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# === Run directly ===
if __name__ == "__main__":
    run_cta_por_cobrar_pipeline()
# === END get_cta_cobrar_main.py ===
