# === get_cta_por_cobrar.py (month-by-month outstanding invoices) ===
import hashlib
import os
import time
import warnings
from datetime import datetime, timedelta

import pandas as pd
import requests

from kame_api import get_token as get_access_token

warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

BASE_URL = "https://api.kameone.cl/api/Contabilidad/getCuentaxCobrar"


def month_range(start_date, end_date):
    """Generate monthly date windows from start_date to end_date."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    while start <= end:
        next_month = (start + timedelta(days=32)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end)
        yield start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        start = next_month


def get_cuentas_por_cobrar(
    fecha_desde="2023-01-01",
    fecha_hasta=datetime.today().strftime("%Y-%m-%d"),
    per_page=200,
):
    """
    Fetch all 'Cuentas por Cobrar' (outstanding invoices) month by month.
    ✅ Uses fechaVencimientoDesde/Hasta
    ✅ Deduplicates globally
    ✅ Adds MonthFetched + SnapshotDate
    """
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    all_rows, seen_ids = [], set()
    today_str = datetime.today().strftime("%Y-%m-%d")

    print(
        f"📅 Fetching outstanding invoices month by month from {fecha_desde} → {fecha_hasta}"
    )

    for start, end in month_range(fecha_desde, fecha_hasta):
        print(f"\n🗓️ Fetching window {start} → {end}")
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
                    rec["MonthFetched"] = start[:7]
                    rec["SnapshotDate"] = today_str
                    all_rows.append(rec)
                    new_count += 1

            print(
                f"  ✅ Added {new_count} new unique records (Total so far: {len(all_rows)})"
            )

            if len(items) < per_page:
                break

            page += 1
            time.sleep(0.3)

    df = pd.DataFrame(all_rows)
    print(f"\n📦 Finished fetching {len(df)} total outstanding invoices (as of today).")
    return df


def save_if_changed(df, output_path):
    """Save CSV only if changed (by comparing SHA-256 hash)."""
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


# === Run directly ===
if __name__ == "__main__":
    output_path = "test/cobranza/raw/cuentas_por_cobrar_monthly_from_Jan_023.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df = get_cuentas_por_cobrar(fecha_desde="2023-01-01")
    if not df.empty:
        save_if_changed(df, output_path)
        print(f"✅ Saved {len(df)} outstanding invoices → {output_path}")
        print(df.head())
    else:
        print("⚠️ No outstanding invoices retrieved.")
# === END get_cta_por_cobrar.py ===
