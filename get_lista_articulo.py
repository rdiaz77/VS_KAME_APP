import time

import pandas as pd
import requests

from kame_api import get_token

BASE_URL = "https://api.kameone.cl/api/Maestro/getListArticulo"


def get_lista_articulos(per_page=500, pause_s=0.1, csv_file="lista_articulos_full.csv"):
    """
    Fetch the FULL list of artículos from Kame API with pagination.
    - per_page: items per page (use a high value if API allows, e.g., 500)
    - pause_s: small delay between requests to be gentle on the API
    - csv_file: output CSV filename
    """
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    all_rows = []
    page = 1

    while True:
        params = {"page": page, "per_page": per_page}
        print(f"🔍 Fetching artículos page {page} (per_page={per_page}) ...")

        try:
            resp = requests.get(BASE_URL, headers=headers, params=params, timeout=15)
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout on page {page} — retrying...")
            time.sleep(1)
            continue
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error on page {page}: {e}")
            break

        if resp.status_code != 200:
            print(f"❌ Error {resp.status_code}: {resp.text}")
            break

        try:
            data = resp.json()
        except Exception as e:
            print(f"⚠️ Failed to parse JSON on page {page}: {e}")
            break

        # Normalize items
        if isinstance(data, dict) and "items" in data:
            items = data["items"]
        elif isinstance(data, list):
            items = data
        else:
            print(f"⚠️ Unexpected response structure on page {page}: {type(data)}")
            items = []

        count = len(items)
        print(f"✅ Page {page}: retrieved {count} artículos.")
        if count == 0:
            break

        all_rows.extend(items)

        # Stop if last page (fewer than per_page)
        if count < per_page:
            break

        page += 1
        time.sleep(pause_s)

    # Build DataFrame + de-dup
    df = pd.DataFrame(all_rows)
    if df.empty:
        print("⚠️ No artículos retrieved.")
        return df

    # sensible keys for dedup (use those that exist)
    key_cols = [
        c for c in ["CodigoArticulo", "Sku", "NombreArticulo"] if c in df.columns
    ]
    if key_cols:
        before = len(df)
        df = df.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)
        print(
            f"🧹 Deduplicated: {before} → {len(df)} rows (keys: {', '.join(key_cols)})"
        )

    # Save CSV (append-safe pattern not needed since we fetched all)
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")
    print(f"💾 Saved {len(df)} artículos to {csv_file}")
    return df


if __name__ == "__main__":
    df = get_lista_articulos()
    if not df.empty:
        print(df.head())
# End of file: get_lista_articulo.py
