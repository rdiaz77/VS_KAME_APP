# === test_enrich_location.py ===
import os
import sys
import pandas as pd

print("🚀 Running test for location enrichment...")

# === Ensure project root is on Python path ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# === Import your enrichment function ===
from pipeline.enrich_location import add_location_info

# === Define input and output paths ===
input_path = "test/ventas_clean_preview.csv"
mapping_path = "data/comunas_provincia_servicio_region(003).csv"
output_path = "test/ventas_enriched_test.csv"

# === Run enrichment only if input exists ===
if not os.path.exists(input_path):
    print(f"❌ Input file not found: {input_path}")
    print("🔎 Please check that the cleaned ventas file exists first.")
else:
    print(f"📂 Loading cleaned data from {input_path} ...")
    df = pd.read_csv(input_path)

    df_enriched = add_location_info(df, mapping_path=mapping_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_enriched.to_csv(output_path, index=False)

    print(f"✅ Test enriched CSV saved → {output_path}")
    print(f"📊 Final columns ({len(df_enriched.columns)}):")
    print(df_enriched.columns.tolist())

    # Quick validation
    print("\n🔍 Validation check:")
    print(f"Region column present: {'Region' in df_enriched.columns}")
    print(f"ServicioSalud column present: {'ServicioSalud' in df_enriched.columns}")
