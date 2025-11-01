# === cta_por_cobrar_baseline.py (full baseline + paid/unpaid tracking) ===
import os
import sqlite3
from datetime import datetime

import pandas as pd

from cobrar.clean_collection import clean_collection

# === Imports from existing working modules ===
from get_cta_por_cobrar import get_cuentas_por_cobrar, save_if_changed

# === Paths ===
DB_PATH = "data/vitroscience.db"
RAW_DIR = "test/cobranza/raw"
CLEAN_DIR = "test/cobranza/clean"
RAW_PATH = os.path.join(RAW_DIR, "cuentas_por_cobrar_full.csv")
CLEAN_PATH = os.path.join(CLEAN_DIR, "cuentas_por_cobrar_clean.csv")
COMBINED_STATUS_PATH = os.path.join(CLEAN_DIR, "cuentas_por_cobrar_with_status.csv")


# === Helpers ===
def ensure_dirs():
    """Ensure required directories exist."""
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(CLEAN_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def _table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table_name,),
    )
    return cur.fetchone() is not None


def _create_status_table(conn: sqlite3.Connection):
    """Create the cuentas_por_cobrar_status table if it doesn't exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cuentas_por_cobrar_status (
            Rut TEXT NOT NULL,
            RznSocial TEXT,
            NombreVendedor TEXT,
            Documento TEXT,
            FolioDocumento TEXT NOT NULL,
            Fecha TEXT,
            FechaVencimiento TEXT,
            CondicionVenta TEXT,
            Total TEXT,
            TotalCP TEXT,
            Saldo TEXT,
            status TEXT CHECK(status IN ('pending','paid')) NOT NULL,
            first_seen TEXT,
            last_seen TEXT,
            paid_date TEXT,
            last_updated TEXT,
            PRIMARY KEY (Rut, FolioDocumento)
        );
        """
    )
    conn.commit()


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_key(df: pd.DataFrame):
    """Return list of composite keys (Rut, FolioDocumento)."""
    for c in ["Rut", "FolioDocumento"]:
        if c not in df.columns:
            df[c] = ""
    return list(
        zip(
            df["Rut"].astype(str).fillna(""),
            df["FolioDocumento"].astype(str).fillna(""),
            strict=False,
        )
    )


# === Core reconciliation logic ===
def update_status_db(df_clean: pd.DataFrame):
    """
    Core reconciliation:
    - First run: create baseline (all pending)
    - Later runs: mark missing ones as paid, update timestamps
    - Keeps full combined dataset in DB
    """
    conn = sqlite3.connect(DB_PATH)
    _create_status_table(conn)
    now = _now()

    # Load previous data
    if _table_exists(conn, "cuentas_por_cobrar_status"):
        df_db = pd.read_sql("SELECT * FROM cuentas_por_cobrar_status;", conn)
    else:
        df_db = pd.DataFrame()

    # Ensure all base columns exist
    base_cols = [
        "Rut",
        "RznSocial",
        "NombreVendedor",
        "Documento",
        "FolioDocumento",
        "Fecha",
        "FechaVencimiento",
        "CondicionVenta",
        "Total",
        "TotalCP",
        "Saldo",
    ]
    for c in base_cols:
        if c not in df_clean.columns:
            df_clean[c] = None

    current_keys = set(build_key(df_clean))

    # === Initial baseline ===
    if df_db.empty:
        df_clean = df_clean.copy()
        df_clean["status"] = "pending"
        df_clean["first_seen"] = now
        df_clean["last_seen"] = now
        df_clean["paid_date"] = None
        df_clean["last_updated"] = now

        conn.execute("DELETE FROM cuentas_por_cobrar_status;")
        df_clean[
            base_cols
            + ["status", "first_seen", "last_seen", "paid_date", "last_updated"]
        ].to_sql("cuentas_por_cobrar_status", conn, if_exists="append", index=False)
        conn.commit()
        conn.close()
        return df_clean  # all pending baseline

    # === Update logic ===
    df_db = df_db.copy()
    db_keys = set(build_key(df_db))
    db_idx = {(r["Rut"], r["FolioDocumento"]): r for _, r in df_db.iterrows()}
    updated_records = {}

    # 1) Handle invoices still present (pending)
    for _, row in df_clean.iterrows():
        key = (str(row.get("Rut", "") or ""), str(row.get("FolioDocumento", "") or ""))
        prev = db_idx.get(key)
        rec = {c: row.get(c, None) for c in base_cols}
        rec["last_updated"] = now
        rec["last_seen"] = now

        if prev is None:
            rec["status"] = "pending"
            rec["first_seen"] = now
            rec["paid_date"] = None
        else:
            rec["first_seen"] = prev.get("first_seen")
            if (prev.get("status") or "").lower() == "paid":
                rec["status"] = "pending"
                rec["paid_date"] = None
            else:
                rec["status"] = "pending"
                rec["paid_date"] = prev.get("paid_date")
        updated_records[key] = rec

    # 2) Mark disappeared invoices as paid
    disappeared = db_keys - current_keys
    if disappeared:
        for key in disappeared:
            prev = db_idx[key]
            rec = {c: prev.get(c, None) for c in base_cols}
            rec["status"] = "paid"
            rec["first_seen"] = prev.get("first_seen")
            rec["last_seen"] = prev.get("last_seen")
            rec["paid_date"] = (
                now if not prev.get("paid_date") else prev.get("paid_date")
            )
            rec["last_updated"] = now
            updated_records[key] = rec

    # 3) Fallback: keep untouched records as-is
    leftovers = db_keys - (disappeared | current_keys)
    for key in leftovers:
        prev = db_idx[key]
        rec = {c: prev.get(c, None) for c in base_cols}
        rec["status"] = prev.get("status")
        rec["first_seen"] = prev.get("first_seen")
        rec["last_seen"] = prev.get("last_seen")
        rec["paid_date"] = prev.get("paid_date")
        rec["last_updated"] = now
        updated_records[key] = rec

    df_final = pd.DataFrame(updated_records.values())

    # Replace data atomically
    conn.execute("DELETE FROM cuentas_por_cobrar_status;")
    df_final[
        base_cols + ["status", "first_seen", "last_seen", "paid_date", "last_updated"]
    ].to_sql("cuentas_por_cobrar_status", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()

    return df_final


# === Main workflow ===
def main():
    print("🚀 Running Cuentas por Cobrar Baseline Builder...")

    ensure_dirs()
    today = datetime.today().strftime("%Y-%m-%d")

    # Step 1: Fetch all data since 2023-01-01
    df_raw = get_cuentas_por_cobrar(fecha_desde="2023-01-01", fecha_hasta=today)
    if df_raw.empty:
        print("⚠️ No data fetched from API. Exiting.")
        return

    # Step 2: Save raw snapshot
    save_if_changed(df_raw, RAW_PATH)

    # Step 3: Clean data
    df_clean = clean_collection(input_path=RAW_PATH, output_path=CLEAN_PATH)

    # Step 4: Reconcile & update database
    df_combined = update_status_db(df_clean)

    # Step 5: Export combined status CSV
    df_combined.to_csv(COMBINED_STATUS_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ Combined CxC status exported: {COMBINED_STATUS_PATH}")

    print(
        f"🎯 Summary: Total {len(df_combined)} | Pending {(df_combined['status']=='pending').sum()} | Paid {(df_combined['status']=='paid').sum()}"
    )


if __name__ == "__main__":
    main()
# === END cta_por_cobrar_baseline.py ===
