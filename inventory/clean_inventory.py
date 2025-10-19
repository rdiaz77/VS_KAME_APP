import pandas as pd
import os
from datetime import datetime

# Paths
INPUT_PATH = "test/stock/raw/inventario_stock_sample.csv"
OUTPUT_DIR = "test/stock/clean"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "inventario_stock_clean.csv")
LOG_PATH = os.path.join(OUTPUT_DIR, "inventario_stock_clean.log")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_inventory():
    logs = []
    start_time = datetime.now()
    logs.append(f"=== INVENTORY CLEANING LOG === {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        print("🧹 Loading inventory data...")
        df = pd.read_csv(INPUT_PATH, dtype=str)
        logs.append(f"Loaded file: {INPUT_PATH}")
        logs.append(f"Initial rows: {len(df)}\n")
    except Exception as e:
        logs.append(f"❌ ERROR loading file: {e}")
        write_log(logs)
        return

    # Drop unnecessary column
    if "descripcionDetallada" in df.columns:
        df = df.drop(columns=["descripcionDetallada"])
        logs.append("🗑️ Dropped column: descripcionDetallada")
    else:
        logs.append("⚠️ Column 'descripcionDetallada' not found — skipping drop.")

    # Convert SKU to text
    if "SKU" in df.columns:
        df["SKU"] = df["SKU"].astype(str)
    else:
        logs.append("⚠️ Column 'SKU' not found in data.")

    # Clean numeric columns
    numeric_cols = ["saldo", "costoPromedio", "precioVentaNeto"]
    for col in numeric_cols:
        if col in df.columns:
            logs.append(f"🔧 Cleaning column: {col}")
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].round(0).astype("Int64")
            df[col] = df[col].apply(lambda x: f"{x:,}" if pd.notnull(x) else "")
        else:
            logs.append(f"⚠️ Column '{col}' not found in data.")

    # Save cleaned file
    try:
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        logs.append(f"✅ Cleaned file saved: {OUTPUT_PATH}")
        logs.append(f"Final rows: {len(df)}")
    except Exception as e:
        logs.append(f"❌ ERROR saving cleaned file: {e}")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logs.append(f"\nFinished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')} (Duration: {duration:.2f}s)")
    write_log(logs)

    print(f"✅ Cleaned file saved to {OUTPUT_PATH}")
    print(df.head())

def write_log(log_entries):
    """Write log entries to file."""
    with open(LOG_PATH, "w", encoding="utf-8") as log_file:
        log_file.write("\n".join(log_entries))
    print(f"📝 Log written to {LOG_PATH}")

if __name__ == "__main__":
    clean_inventory()
