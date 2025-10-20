import warnings

import pandas as pd
import requests

from kame_api import get_token as get_access_token

warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

BASE_URL = "https://api.kameone.cl/api/Maestro/getListUnidadNegocio"


def get_unidades_negocio():
    """Fetch list of unidades de negocio from Kame API."""
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    print("🔍 Fetching unidades de negocio...")
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
        unidades = data["items"]
    elif isinstance(data, list):
        unidades = data
    else:
        print(f"⚠️ Unexpected JSON structure: {type(data)}")
        unidades = []

    print(f"✅ Retrieved {len(unidades)} unidades de negocio.")
    return unidades


if __name__ == "__main__":
    unidades = get_unidades_negocio()
    if unidades:
        df = pd.DataFrame(unidades)
        df.to_csv("unidades_negocio_list.csv", index=False, encoding="utf-8-sig")
        print(f"💾 Saved {len(df)} rows to unidades_negocio_list.csv")
        print(df.head())
    else:
        print("⚠️ No unidades de negocio found or empty response.")
# End of file: get_unidad_negocio.py
