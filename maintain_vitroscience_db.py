# === maintain_vitroscience_db.py ===
"""
VitroScience Database Maintenance Script

✅ Automatically backs up the database before any changes
✅ Removes obsolete tables (like cuentas_por_cobrar_status)
✅ VACUUMs the database to reclaim space
✅ Displays current tables and row counts
"""

import os
import shutil
import sqlite3
from datetime import datetime

from tabulate import tabulate

DB_PATH = "data/vitroscience.db"
BACKUP_DIR = "data/backups"


def backup_database(db_path=DB_PATH, backup_dir=BACKUP_DIR):
    """Create a timestamped backup before modifying the database."""
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return None

    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"vitroscience_backup_{timestamp}.db")

    shutil.copy2(db_path, backup_path)
    print(f"🧾 Backup created: {backup_path}")
    return backup_path


def maintain_database(db_path=DB_PATH):
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return

    # --- Step 1: Backup ---
    backup_database(db_path)

    # --- Step 2: Connect to DB ---
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    print(f"🗃️ Connected to database: {db_path}")

    # --- Step 3: Drop obsolete tables ---
    obsolete_tables = ["cuentas_por_cobrar_status", "temp_old_data"]
    for tbl in obsolete_tables:
        cur.execute(f"DROP TABLE IF EXISTS {tbl};")
        print(f"🧹 Dropped table if existed → {tbl}")

    # --- Step 4: List tables ---
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [r[0] for r in cur.fetchall()]

    print("\n📋 Current tables:")
    for t in tables:
        print(f" - {t}")

    # --- Step 5: Count rows ---
    counts = []
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t};")
            n = cur.fetchone()[0]
            counts.append([t, n])
        except Exception as e:
            counts.append([t, f"⚠️ {e}"])

    print("\n📊 Table Row Counts:")
    print(tabulate(counts, headers=["Table", "Rows"], tablefmt="github"))

    # --- Step 6: Optimize space ---
    print("\n🧼 Running VACUUM to optimize DB...")
    cur.execute("VACUUM;")
    conn.commit()
    conn.close()

    print("\n✅ Maintenance completed successfully.")
    print("📦 Database optimized and backup safely stored.")


if __name__ == "__main__":
    maintain_database()
# === END maintain_vitroscience_db.py ===
