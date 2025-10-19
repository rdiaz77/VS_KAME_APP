import pandas as pd
import sqlite3
import os
from datetime import datetime

# Paths
DB_PATH = "data/vitroscience.db"
CSV_PATH = "test/stock/clean/inventario_stock_clean.csv"
TABLE_NAME = "inventory_stock"
LOG_PATH = "data/inventory_db_load.log"

def create_inventory_table():
    logs = []
    start_time = datetime.now()
    logs.append(f"=== INVENTORY DB LOAD LOG === {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

        # Load cleaned data
        print("📦 Loading cleaned inventory CSV...")
        df = pd.read_csv(CSV_PATH, dtype=str)
        logs.append(f"Loaded file: {CSV_PATH}")
        logs.append(f"Rows loaded: {len(df)}")
        logs.append(f"Columns: {', '.join(df.columns)}\n")
    except Exception as e:
        err = f"❌ ERROR loading CSV: {e}"
        logs.append(err)
        write_log(logs)
        print(err)
        return

    try:
        # Connect to SQLite
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Create table dynamically
        print("🧱 Creating table (if not exists)...")
        columns_def = ", ".join([f'"{col}" TEXT' for col in df.columns])
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            {columns_def},
            PRIMARY KEY ("SKU")
        );
        """
        cur.execute(create_table_sql)
        conn.commit()
        logs.append(f"Table created/verified: {TABLE_NAME}")

        # Insert or replace (upsert)
        print("🧩 Inserting or replacing data...")
        placeholders = ", ".join(["?"] * len(df.columns))
        columns_str = ", ".join([f'"{col}"' for col in df.columns])
        insert_sql = f"""
        INSERT OR REPLACE INTO {TABLE_NAME} ({columns_str})
        VALUES ({placeholders});
        """
        cur.executemany(insert_sql, df.values.tolist())
        conn.commit()
        logs.append(f"✅ {len(df)} records inserted/replaced in table '{TABLE_NAME}'.")

    except Exception as e:
        err = f"❌ ERROR writing to DB: {e}"
        logs.append(err)
        print(err)
    finally:
        conn.close()
        logs.append(f"Database connection closed: {DB_PATH}")

    # Runtime summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logs.append(f"\nFinished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')} (Duration: {duration:.2f}s)")
    write_log(logs)

    print(f"✅ Database updated: {DB_PATH}")
    print(f"📝 Log written to {LOG_PATH}")

def write_log(log_entries):
    """Write log entries to file."""
    with open(LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write("\n".join(log_entries) + "\n\n")

if __name__ == "__main__":
    create_inventory_table()
# End of file inventory/create_inventory_db.py