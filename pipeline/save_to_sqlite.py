# === pipeline/save_to_sqlite.py ===
import sqlite3
from pathlib import Path
import pandas as pd


def save_to_sqlite(csv_path):
    """
    Save cleaned & enriched sales data into SQLite.
    - Creates the table if it doesn't exist.
    - Appends only new rows (based on unique combo: NombreDocumento + Folio).
    - Keeps existing data intact.
    """

    csv_path = Path(csv_path)
    db_path = Path("data/vitroscience.db")
    table_name = "ventas_enriched_product"

    if not csv_path.exists():
        print(f"❌ CSV not found: {csv_path}")
        return

    # === Load the cleaned file ===
    print(f"📂 Loading data from {csv_path}...")
    df_new = pd.read_csv(csv_path)

    if df_new.empty:
        print("⚠️ No data to save.")
        return

    # === Normalize Folio and create UniqueKey ===
    df_new["Folio"] = (
        df_new["Folio"].astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )
    df_new["UniqueKey"] = (
        df_new["NombreDocumento"].astype(str).str.strip()
        + "_"
        + df_new["Folio"]
    )

    # === Connect to SQLite ===
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # === Check if table exists ===
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    )
    exists = cursor.fetchone() is not None

    if not exists:
        # Table does not exist — create it fresh
        print(f"🆕 Creating new table '{table_name}'...")
        df_new.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.commit()

        # Add unique index
        try:
            cursor.execute(
                f"CREATE UNIQUE INDEX idx_unique_doc_folio ON {table_name}(NombreDocumento, Folio);"
            )
            conn.commit()
            print("🧱 Created unique index on (NombreDocumento, Folio).")
        except Exception as e:
            print(f"⚠️ Could not create unique index: {e}")

        print(f"✅ Saved {len(df_new)} new rows into '{table_name}' (initial load).")

    else:
        # Table exists — append only new rows
        print(f"🔎 Checking for existing records in '{table_name}'...")
        try:
            df_existing = pd.read_sql(
                f"SELECT NombreDocumento, Folio FROM {table_name};", conn
            )
            df_existing["Folio"] = (
                df_existing["Folio"].astype(str)
                .str.replace(r"\.0$", "", regex=True)
                .str.strip()
            )
            df_existing["UniqueKey"] = (
                df_existing["NombreDocumento"].astype(str).str.strip()
                + "_"
                + df_existing["Folio"]
            )
        except Exception as e:
            print(f"⚠️ Could not read existing data: {e}")
            df_existing = pd.DataFrame(columns=["UniqueKey"])

        # === Identify new rows using merge-based anti-join ===
        merged = df_new.merge(
            df_existing[["UniqueKey"]], how="left", on="UniqueKey", indicator=True
        )
        new_df = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

        if new_df.empty:
            print("ℹ️ No new rows to add — database already up to date.")
        else:
            print(f"✅ Detected {len(new_df)} truly new rows to append.")
            new_df.drop(columns=["UniqueKey"], inplace=True)
            new_df.to_sql(table_name, conn, if_exists="append", index=False)
            conn.commit()
            print(f"✅ Appended {len(new_df)} new rows into '{table_name}'.")


    conn.close()
    print("🗄️ Database update complete.\n")


if __name__ == "__main__":
    save_to_sqlite()
# === End of pipeline/save_to_sqlite.py ===
