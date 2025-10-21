import sqlite3
from pathlib import Path

import pandas as pd


def save_to_sqlite():
    # === CONFIGURATION ===
    csv_path = Path("test/ventas/clean/ventas_enriched_product.csv")  # Input CSV file
    db_path = Path("data/vitroscience.db")  # SQLite database file
    table_name = "ventas_enriched_product"  # Table name

    # === STEP 1: Load CSV ===
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)

    # === STEP 2: Connect to SQLite ===
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    # === STEP 3: Save DataFrame to SQLite ===
    print(f"Saving data to table '{table_name}' in {db_path}...")
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    # === STEP 4: Verify ===
    rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"✅ Saved {rows} rows into table '{table_name}'")

    # === STEP 5: Close connection ===
    conn.close()
    print("Done.")


# Allow standalone execution
if __name__ == "__main__":
    save_to_sqlite()
