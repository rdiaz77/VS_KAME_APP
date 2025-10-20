# === cta por pagar.py (date-windowed final version) ===
import hashlib
import os
import time
import warnings
from datetime import datetime, timedelta

import pandas as pd
import requests

from kame_api import get_token as get_access_token

warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

BASE_URL = "https://api.kameone.cl/api/Contabilidad/getCuentaxPagar"


def daterange_chunks(start_date, end_date, days=31):
    """Generate date ranges split by N days (default monthly)."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    while start < end:
        chunk_end = min(start + timedelta(days=days), end)
        yield start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        start = chunk_end + timedelta(days=1)


def get_cuentas_por_pagar(
    fecha_desde="2020-01-01", fecha_hasta="2025-10-18", per_page=100, chunk_days=31
):
    """
    ✅ Robust method to fetch *all* CxC using rolling date windows.

    Features:
      - Fetches data month by month (or custom days)
      - Deduplicates globally by Id/NumeroDocumento
      - Handles API rate limits and retries
    """

    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    all_rows, seen_ids = [], set()

    for start, end in daterange_chunks(fecha_desde, fecha_hasta, chunk_days):
        print(f"\n🗓️ Fetching CxP for window {start} → {end}")
        page = 1
        while True:
            params = {
                "page": page,
                "per_page": per_page,
                "fechaVencimientoDesde": start,
                "fechaVencimientoHasta": end,
            }

            resp = requests.get(BASE_URL, headers=headers, params=params)
            print(f"  🔍 Page {page} | HTTP {resp.status_code}")

            if resp.status_code == 429:
                print("  ⚠️ Rate limit hit, waiting 15s...")
                time.sleep(15)
                continue
            if resp.status_code != 200:
                print(f"  ❌ Error {resp.status_code}: {resp.text}")
                break

            data = resp.json()
            items = data.get("items") or data.get("data") or []
            if not items:
                break

            new_count = 0
            for rec in items:
                rec_id = (
                    rec.get("Id")
                    or rec.get("NumeroDocumento")
                    or hash(frozenset(rec.items()))
                )
                if rec_id not in seen_ids:
                    seen_ids.add(rec_id)
                    all_rows.append(rec)
                    new_count += 1

            print(f"  ✅ Added {new_count} new unique records (Total: {len(all_rows)})")

            # stop if no new data in this window
            if new_count == 0:
                break

            page += 1
            time.sleep(0.3)

    df = pd.DataFrame(all_rows)
    print(f"\n📦 Finished: total unique records = {len(df)}")
    return df


def save_if_changed(df, output_path):
    """Save CSV only if changed."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    tmp = output_path + ".tmp"
    df.to_csv(tmp, index=False, encoding="utf-8-sig")

    def file_hash(p):
        with open(p, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    if not os.path.exists(output_path):
        os.replace(tmp, output_path)
        print(f"💾 Saved new file: {output_path}")
    elif file_hash(tmp) != file_hash(output_path):
        os.replace(tmp, output_path)
        print(f"💾 Updated file with new data: {output_path}")
    else:
        os.remove(tmp)
        print("⚙️ No changes detected, file not updated.")


# 🧪 Standalone run
if __name__ == "__main__":
    df = get_cuentas_por_pagar()
    if not df.empty:
        save_if_changed(df, "test/pagar/raw/cuentas_por_pagar_full.csv")
        print(df.head())
    else:
        print("⚠️ No data retrieved.")
# === END cts_pagar.py ===
