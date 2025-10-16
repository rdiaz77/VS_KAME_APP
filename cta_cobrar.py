import requests
import json
import pandas as pd
from kame_api import get_token as get_access_token  # ✅ using your existing token system

# Optional: silence urllib3 SSL warning
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

# API endpoint
BASE_URL = "https://api.kameone.cl/api/Contabilidad/getCuentaxCobrar"


def get_cuentas_por_cobrar(
    fecha_desde="2024-01-01",
    fecha_hasta="2024-12-31",
    page=1,
    per_page=100
):
    """Fetches Cuentas por Cobrar (Accounts Receivable) from Kame API."""
    
    token = get_access_token()
    
    params = {
        "page": page,
        "per_page": per_page,
        "fechaVencimientoDesde": fecha_desde,
        "fechaVencimientoHasta": fecha_hasta
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print(f"🔍 Fetching CxC page {page} ({fecha_desde} → {fecha_hasta}) ...")
    response = requests.get(BASE_URL, headers=headers, params=params)
    print(f"HTTP Status: {response.status_code}")

    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: {response.text}")
        return None

    data = response.json()

    # ✅ Extract data correctly from "items"
    if isinstance(data, dict) and "items" in data:
        records = data["items"]
    else:
        print(f"⚠️ Unexpected JSON structure: keys = {list(data.keys())}")
        records = []

    print(f"✅ Retrieved {len(records)} records.")
    return records


if __name__ == "__main__":
    all_records = []
    page = 1

    while True:
        records = get_cuentas_por_cobrar(page=page)
        if not records:
            break
        all_records.extend(records)
        print(f"📄 Retrieved {len(records)} records (Total: {len(all_records)})")
        page += 1

    if all_records:
        df = pd.DataFrame(all_records)
        df.to_csv("cuentas_por_cobrar_2024.csv", index=False)
        print(f"💾 Saved {len(df)} rows to cuentas_por_cobrar_2024.csv")
        print(df.head())
    else:
        print("⚠️ No records found or empty response.")
