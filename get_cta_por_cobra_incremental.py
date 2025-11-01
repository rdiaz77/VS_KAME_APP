# === get_cta_cobrar_incremental.py ===
"""
Incremental updater for Cuentas por Cobrar (CxC).

Every run:
1) Fetch current pending invoices from KAME (since 2023-01-01)
2) Save raw snapshot CSV (timestamped)
3) Clean → normalized CSV
4) Compare with current DB snapshot (cuentas_por_cobrar):
   - Any invoice that disappears = mark as PAID in history (status='paid', paid_date=now)
   - Replace cuentas_por_cobrar with current pending set (status='pending')
   - Append both 'pending' and 'paid' rows to cuentas_por_cobrar_history
"""

import os
import sqlite3
from datetime import datetime

import pandas as pd

from cobrar.clean_cta_por_cobrar import clean_cta_por_cobrar

# Local imports (existing in your repo)
from get_cta_por_cobrar import get_cuentas_por_cobrar, save_if_changed

DB_PATH = "data/vitroscience.db"

RAW_DIR = "test/cobranza/raw"
CLEAN_DIR = "test/cobranza/clean"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)


def _ensure_paid_date_column(conn: sqlite3.Connection):
    """Add paid_date column to cuentas_por_cobrar_history if missing."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(cuentas_por_cobrar_history);")
    cols = [r[1] for r in cur.fetchall()]
    if "paid_date" not in cols:
        cur.execute("ALTER TABLE cuentas_por_cobrar_history ADD COLUMN paid_date TEXT;")
        conn.commit()


def _load_current_snapshot(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        return pd.read_sql("SELECT * FROM cuentas_por_cobrar;", conn)
    except Exception:
        return pd.DataFrame()


def _replace_live_snapshot(df_pending: pd.DataFrame, conn: sqlite3.Connection):
    # Keep only live-snapshot columns
    live_cols = [
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
        "MonthFetched",
        "SnapshotDate",
        "status",
        "last_updated",
    ]
    for c in live_cols:
        if c not in df_pending.columns:
            df_pending[c] = None

    df_pending[live_cols].to_sql(
        "cuentas_por_cobrar", conn, if_exists="replace", index=False
    )


def _append_history(df_history: pd.DataFrame, conn: sqlite3.Connection):
    # Align to history schema
    hist_cols = [
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
        "MonthFetched",
        "SnapshotDate",
        "status",
        "last_updated",
        "snapshot_date",
        "inserted_at",
        "paid_date",
    ]
    for c in hist_cols:
        if c not in df_history.columns:
            df_history[c] = None

    df_history[hist_cols].to_sql(
        "cuentas_por_cobrar_history", conn, if_exists="append", index=False
    )


def run_incremental_cxc():
    now = datetime.now()
    snapshot_date = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    print("🚀 Starting incremental CxC update...\n")

    # 1) Fetch current pending invoices
    df_raw = get_cuentas_por_cobrar(fecha_desde="2023-01-01")
    if df_raw is None or df_raw.empty:
        print("⚠️ No pending invoices returned by API. Nothing to do.")
        return

    # 2) Save raw snapshot
    raw_path = os.path.join(
        RAW_DIR,
        f"cuentas_por_cobrar_snapshot_{snapshot_date}_{now.strftime('%H%M%S')}.csv",
    )
    save_if_changed(df_raw, raw_path)
    print(f"💾 Saved raw snapshot → {raw_path}")

    # 3) Clean → normalized CSV
    clean_path = os.path.join(
        CLEAN_DIR,
        f"cuentas_por_cobrar_clean_{snapshot_date}_{now.strftime('%H%M%S')}.csv",
    )
    df_clean = clean_cta_por_cobrar(input_path=raw_path, output_path=clean_path)
    print(f"🧹 Cleaned snapshot saved → {clean_path}")

    # Ensure types for numeric fields (in case cleaner output is str depending on earlier versions)
    for col in ["Total", "TotalCP", "Saldo"]:
        if col in df_clean.columns:
            df_clean[col] = (
                pd.to_numeric(df_clean[col], errors="coerce").fillna(0).astype(int)
            )

    # Attach live-run tracking fields for pending set
    df_clean["status"] = "pending"
    df_clean["last_updated"] = timestamp
    df_clean["snapshot_date"] = snapshot_date
    df_clean["inserted_at"] = timestamp
    df_clean["paid_date"] = None  # pending rows have no paid_date

    # 4) DB compare & update
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # Make sure history has paid_date column
    _ensure_paid_date_column(conn)

    # Load previous live snapshot
    df_old = _load_current_snapshot(conn)
    old_keys = set(df_old["FolioDocumento"].astype(str)) if not df_old.empty else set()
    new_keys = set(df_clean["FolioDocumento"].astype(str))

    # Determine paid (were pending before, gone now)
    paid_keys = old_keys - new_keys
    if paid_keys:
        df_paid = df_old[df_old["FolioDocumento"].astype(str).isin(paid_keys)].copy()
        # Normalize numeric types in case older snapshot stored as TEXT
        for col in ["Total", "TotalCP", "Saldo"]:
            if col in df_paid.columns:
                df_paid[col] = (
                    pd.to_numeric(df_paid[col], errors="coerce").fillna(0).astype(int)
                )
        df_paid["status"] = "paid"
        df_paid["last_updated"] = timestamp
        df_paid["snapshot_date"] = snapshot_date  # marked paid on this run's date
        df_paid["inserted_at"] = timestamp
        df_paid["paid_date"] = timestamp
        paid_count = len(df_paid)
    else:
        df_paid = pd.DataFrame(columns=df_clean.columns)
        paid_count = 0

    # Replace live pending snapshot
    _replace_live_snapshot(df_clean, conn)

    # Append both pending & paid to history
    _append_history(pd.concat([df_clean, df_paid], ignore_index=True), conn)

    # Helpful output
    print(f"🧾 Pending in this run: {len(df_clean)}")
    print(f"💰 Newly marked as PAID: {paid_count}")

    conn.commit()
    conn.close()

    # Log
    os.makedirs("data", exist_ok=True)
    with open("data/update_log.txt", "a", encoding="utf-8") as f:
        f.write(
            f"{timestamp} — CxC incremental update finished. Pending: {len(df_clean)} | Paid: {paid_count}\n"
        )

    print("\n✅ CxC incremental update complete.\n")


if __name__ == "__main__":
    run_incremental_cxc()
# === End of get_cta_cobrar_incremental.py ===
