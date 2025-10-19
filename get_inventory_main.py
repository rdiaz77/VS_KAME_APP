import os
import sys
from datetime import datetime

# Ensure root path is visible for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Imports
from get_inventory import get_stock_sample
from inventory import clean_inventory, create_inventory_table
import pandas as pd

def main():
    print("🚀 Starting full inventory ETL process...\n")
    start_time = datetime.now()

    try:
        # STEP 1: Fetch data from KAME API
        print("🌐 Step 1: Fetching raw inventory data from KAME...")
        records = get_stock_sample(1000)  # Fetch 1000 records (adjust as needed)
        if not records:
            print("⚠️ No inventory data fetched. Aborting.")
            return
        df = pd.DataFrame(records)
        raw_dir = "test/stock/raw"
        os.makedirs(raw_dir, exist_ok=True)
        raw_path = os.path.join(raw_dir, "inventario_stock_sample.csv")
        df.to_csv(raw_path, index=False, encoding="utf-8-sig")
        print(f"✅ Raw data saved: {raw_path}\n")

        # STEP 2: Clean the fetched data
        print("🧹 Step 2: Cleaning inventory data...")
        clean_inventory()
        print("✅ Inventory data cleaned successfully.\n")

        # STEP 3: Update SQLite database
        print("💾 Step 3: Updating SQLite database...")
        create_inventory_table()
        print("✅ Database updated successfully.\n")

    except Exception as e:
        print(f"❌ ERROR during inventory ETL process: {e}")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"🎯 Process completed in {duration:.2f} seconds.")

if __name__ == "__main__":
    # Ensure we run from project root
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
# === END get_inventory_main.py ===