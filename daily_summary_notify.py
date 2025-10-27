# === daily_summary_notify.py ===
"""
Daily summary notifier for VitroScience.
- Counts ventas inserted in the last 24 hours
- Shows a macOS notification
- Appends a line to data/daily_summary_log.csv

Schedule via LaunchAgent (com.vitroscience.dailynotify).
"""

import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import csv

DB_PATH = Path("data/vitroscience.db")
LOG_DIR = Path("data")
LOG_PATH = LOG_DIR / "daily_summary_log.csv"


def get_new_ventas_count():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # last 24h window (use ISO for SQLite string compare)
    since = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
    cur.execute(
        "SELECT COUNT(*) FROM ventas_enriched_product WHERE Fecha >= ?;", (since,)
    )
    count = cur.fetchone()[0]
    conn.close()
    return count


def send_notification(title: str, message: str):
    # macOS notification via AppleScript
    subprocess.run(
        [
            "osascript",
            "-e",
            f'display notification "{message}" with title "{title}" sound name "Submarine"',
        ],
        check=False,
    )


def append_log_row(status: str, count: int | None):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    new_file = not LOG_PATH.exists()
    with LOG_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["timestamp", "status", "new_ventas_last_24h"])
        writer.writerow(
            [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), status, "" if count is None else count]
        )


if __name__ == "__main__":
    count = get_new_ventas_count()

    if count is None:
        msg = "No database found — run the pipeline first."
        send_notification("VitroScience", msg)
        append_log_row("no_db", None)
    elif count == 0:
        msg = "No new ventas in the last 24 hours."
        send_notification("VitroScience", msg)
        append_log_row("ok", 0)
    else:
        msg = f"✅ {count} new ventas in the last 24 hours!"
        send_notification("VitroScience", msg)
        append_log_row("ok", count)
# === End of daily_summary_notify.py ===