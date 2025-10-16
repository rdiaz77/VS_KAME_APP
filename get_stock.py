import requests
import pandas as pd
from kame_api import get_token as get_access_token
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

BASE_URL = "https://api.kameone.cl/api/Inventario/getStock"


def get_stock_sample(per_page=100):
    """Fetch 100 stock records from Kame API."""
    token = get_access_token()
    params = {"page": 1, "per_page": per_page}
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json"}

    print(f"🔍 Fetching {per_page} stock records...")
    response = requests.get(BASE_URL, headers=headers, params=params)
    print(f"HTTP Status: {response.status_code}")

    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: {response.text}")
        return []

    data = response.json()

    if isinstance(data, dict) and "items" in data:
        records = data["items"]
    elif isinstance(data, list):
        records = data
    else:
        print(f"⚠️ Unexpected JSON structure: {type(data)}")
        records = []

    print(f"✅ Retrieved {len(records)} records.")
    return records


if __name__ == "__main__":
    records = get_stock_sample(100)
    if records:
        df = pd.DataFrame(records)
        df.to_csv("inventario_stock_sample.csv",
                  index=False, encoding="utf-8-sig")
        print(f"💾 Saved {len(df)} rows to inventario_stock_sample.csv")
        print(df.head())
    else:
        print("⚠️ No records found or empty response.")
