# === cobrar/cta_por_cobrar_daily_check.py ===
import os
import sqlite3
from datetime import datetime, date
import streamlit as st
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "vitroscience.db"
LOG_PATH = Path(__file__).parent.parent / "data" / "update_log.txt"


def check_cta_cobrar_update():
    """Check if cuentas_por_cobrar was updated today and show alert in Streamlit."""
    if not DB_PATH.exists():
        st.error(f"❌ Database not found at {DB_PATH}")
        return False

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT MAX(snapshot_date) FROM cuentas_por_cobrar_history;")
    last_update = cur.fetchone()[0]
    conn.close()

    if not last_update:
        st.warning("⚠️ No cuentas_por_cobrar data found in DB.")
        return False

    last_date = datetime.strptime(last_update, "%Y-%m-%d").date()
    today = date.today()

    # Compare
    if last_date == today:
        st.success(f"✅ Cuentas por Cobrar data is up-to-date (last snapshot: {last_date})")
        return False
    else:
        # Log the event
        os.makedirs(LOG_PATH.parent, exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} — ⚠️ New CxC data available. Last snapshot: {last_date}\n")

        # Show popup alert
        st.warning(f"🆕 New Cuentas por Cobrar data detected! (last update: {last_date})")
        return True


# 🧪 Standalone run
if __name__ == "__main__":
    print("🔍 Checking for CxC data updates...")
    flag = check_cta_cobrar_update()
    print(f"New data detected: {flag}")
# === END cobrar/cta_por_cobrar_daily_check.py ===
