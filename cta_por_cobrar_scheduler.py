# === cta_cobrar_scheduler.py ===
import subprocess
import time
from datetime import datetime

print("⏱️ Starting hourly scheduler for CxC updates...")

while True:
    print(f"\n🕐 Running update at {datetime.now():%Y-%m-%d %H:%M:%S}")
    subprocess.run(["python", "cta_cobrar_main.py"])
    print("✅ Update complete. Sleeping for 1 hour...")
    time.sleep(3600)
# === END cta_cobrar_scheduler.py ===
