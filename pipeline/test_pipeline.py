# === test_pipeline.py ===
"""
Automatic test runner for the full VS_KAME_APP sales data pipeline:
1. get_ventas.py
2. clean_sales_main.py
3. enrich_location.py
4. enrich_product.py
5. save_to_sqlite.py
"""

import importlib
import os
import sqlite3
import sys

import pandas as pd

# === Path setup ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.join(BASE_DIR, "..")

# 👇 Add parent folder to sys.path so imports work (important!)
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)


# === Step 1: Get ventas from Kame ===
def test_get_ventas():
    print("\n🚀 STEP 1: get_ventas.py")
    mod = importlib.import_module("get_ventas")
    df = mod.get_informe_ventas_json("2024-01-01", "2024-01-31")
    assert df is not None and not df.empty, "❌ get_ventas returned empty DataFrame"
    print(f"✅ get_ventas: {len(df)} rows")
    return df


# === Step 2: Clean sales ===
def test_clean_sales():
    print("\n🧹 STEP 2: clean_sales_main.py")
    mod = importlib.import_module("pipeline.clean_sales_main")
    df = mod.run_clean_sales_pipeline()
    out_path = os.path.join(ROOT_DIR, "test", "ventas_clean_preview.csv")
    assert os.path.exists(out_path), "❌ ventas_clean_preview.csv not found"
    assert not df.empty, "❌ cleaned DataFrame is empty"
    print(f"✅ Clean sales: {len(df)} rows")
    return df


# === Step 3: Enrich location ===
def test_enrich_location():
    print("\n🌎 STEP 3: enrich_location.py")
    mod = importlib.import_module("pipeline.enrich_location")

    input_path = os.path.join(ROOT_DIR, "test", "ventas_clean_preview.csv")

    # 🔧 Explicitly resolve absolute path to mapping file
    mapping_path = "/Users/rafaeldiaz/Desktop/Python_Kame_ERP/VS_KAME_APP/data/comunas_provincia_servicio_region(003).csv"

    if not os.path.exists(mapping_path):
        raise FileNotFoundError(f"❌ Mapping file missing at: {mapping_path}")

    df = pd.read_csv(input_path)
    df_enriched = mod.add_location_info(df, mapping_path=mapping_path)

    # 💾 Save enriched output
    out_path = os.path.join(ROOT_DIR, "test", "ventas_enriched.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df_enriched.to_csv(out_path, index=False)

    assert os.path.exists(out_path), "❌ ventas_enriched.csv not found"
    assert "Region" in df_enriched.columns, "❌ 'Region' column missing"
    print(f"✅ Enrich location: {len(df_enriched)} rows")
    return df_enriched


# === Step 4: Enrich product ===
def test_enrich_product():
    print("\n🧩 STEP 4: enrich_product.py")
    mod = importlib.import_module("pipeline.enrich_product")
    input_path = os.path.join(ROOT_DIR, "test", "ventas_enriched.csv")
    df = pd.read_csv(input_path)
    df_final = mod.add_product_info(df)

    # 💾 Save final enriched output here
    out_path = os.path.join(ROOT_DIR, "test", "ventas_enriched_product.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df_final.to_csv(out_path, index=False)

    assert os.path.exists(out_path), "❌ ventas_enriched_product.csv not found"
    assert "Unegocio" in df_final.columns, "❌ 'Unegocio' column missing"
    print(f"✅ Enrich product: {len(df_final)} rows")
    return df_final


# === Step 5: Save to SQLite ===
def test_save_to_sqlite():
    print("\n🗄️  STEP 5: save_to_sqlite.py")
    importlib.import_module("pipeline.save_to_sqlite")

    # Check that DB file and table were created
    db_path = os.path.join(ROOT_DIR, "data", "vitroscience.db")
    table_name = "ventas_enriched_product"

    assert os.path.exists(db_path), f"❌ Database not found at {db_path}"

    conn = sqlite3.connect(db_path)
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    conn.close()

    assert count > 0, f"❌ Table '{table_name}' is empty or missing"
    print(f"✅ Saved {count} rows into SQLite table '{table_name}'")


# === Run all steps ===
if __name__ == "__main__":
    print("🧪 Starting VS_KAME_APP pipeline test...\n")
    test_get_ventas()
    test_clean_sales()
    test_enrich_location()
    test_enrich_product()
    test_save_to_sqlite()
    print("\n🎉 All pipeline steps completed successfully!")
# ==== end of test_pipeline.py ===
