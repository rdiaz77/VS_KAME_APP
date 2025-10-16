import requests
import pandas as pd
from kame_api import get_token as get_access_token
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

BASE_URL = "https://api.kameone.cl/api/Maestro/getListVendedor"


def get_vendedores():
    """Fetch list of vendedores from Kame API."""
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print("🔍 Fetching vendedores list...")
    response = requests.get(BASE_URL, headers=headers)
    print(f"HTTP Status: {response.status_code}")

    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: {response.text}")
        return []

    try:
        data = response.json()
    except Exception as e:
        print(f"⚠️ Failed to parse JSON: {e}")
        return []

    # Handle both list and dict response formats
    if isinstance(data, dict) and "items" in data:
        vendedores = data["items"]
    elif isinstance(data, list):
        vendedores = data
    else:
        print(f"⚠️ Unexpected JSON structure: {type(data)}")
        vendedores = []

    print(f"✅ Retrieved {len(vendedores)} vendedores.")
    return vendedores


if __name__ == "__main__":
    vendedores = get_vendedores()
    if vendedores:
        df = pd.DataFrame(vendedores)
        df.to_csv("vendedores_list.csv", index=False, encoding="utf-8-sig")
        print(f"💾 Saved {len(df)} rows to vendedores_list.csv")
        print(df.head())
    else:
        print("⚠️ No vendedores found or empty response.")
# End of file: get_vendedor.py
