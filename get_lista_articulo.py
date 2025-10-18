# === get_list_articulo.py (final integrated + optimized version) ===
import os
import time
import hashlib
import pandas as pd
import requests
from kame_api import get_token
import clean_list_articulo  # ✅ import the cleaner module


BASE_URL = "https://api.kameone.cl/api/Maestro/getListArticulo"


# -------------------------------------------------------------------
# 🧮 Utility: generate SHA-256 hash to detect changes before saving
# -------------------------------------------------------------------
def hash_dataframe(df: pd.DataFrame) -> str:
    """Generate a SHA-256 hash of a DataFrame's contents."""
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


# -------------------------------------------------------------------
# 💾 Save only if file content actually changed
# -------------------------------------------------------------------
def save_if_changed(df: pd.DataFrame, csv_file="data/lista_articulos_full.csv"):
    """Save DataFrame only if content has changed compared to existing file."""
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    abs_path = os.path.abspath(csv_file)
    print(f"🗂️ Target path: {abs_path}")

    if not os.path.exists(csv_file):
        df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        print(f"💾 First-time save: {csv_file}")
        return True

    try:
        df_old = pd.read_csv(csv_file, dtype=str)
        old_hash = hash_dataframe(df_old)
    except Exception as e:
        print(f"⚠️ Could not read existing file for hash comparison: {e}")
        old_hash = None

    new_hash = hash_dataframe(df)

    if old_hash != new_hash:
        df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        print("✅ File updated — content changed in KAME.")
        return True
    else:
        print("🟢 No changes detected — file not overwritten.")
        return False


# -------------------------------------------------------------------
# 🔁 Fetch, deduplicate, save, and clean full list from KAME API
# -------------------------------------------------------------------
def get_lista_articulos(per_page=100, pause_s=0.1, csv_file="data/lista_articulos_full.csv"):
    """
    Fetch and clean all artículos from KAME API in one go.
    - per_page: KAME API max = 100
    - Fetches all pages based on total count
    - Deduplicates by CodigoArticulo
    - Saves only if data changed
    - Automatically runs cleaner from clean_list_articulo.py
    """
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    all_rows = []
    page = 1
    total = None

    while True:
        params = {"page": page, "per_page": per_page}
        print(f"🔍 Fetching artículos page {page} (per_page={per_page}) ...")

        try:
            resp = requests.get(BASE_URL, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed on page {page}: {e}")
            break

        data = resp.json()
        items = data.get("items", [])
        total = data.get("total", 0)
        count = len(items)

        print(f"✅ Page {page}: {count} artículos (total={total})")

        if count == 0:
            print("⚠️ No items returned — stopping pagination.")
            break

        all_rows.extend(items)
        print(f"📊 Progress: {len(all_rows)} / {total} artículos collected")

        # Stop when all items fetched
        if total and len(all_rows) >= total:
            print(f"🏁 All pages fetched ({len(all_rows)} / {total}).")
            break

        page += 1
        time.sleep(pause_s)

        if page > 1000:
            print("⚠️ Pagination limit reached — aborting.")
            break

    print(f"🧾 Collected {len(all_rows)} total artículos before cleaning.")

    df = pd.DataFrame(all_rows)
    if df.empty:
        print("⚠️ No artículos retrieved — DataFrame is empty.")
        return df

    # 🧹 Deduplicate by CodigoArticulo
    if "CodigoArticulo" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["CodigoArticulo"], keep="first").reset_index(drop=True)
        print(f"🧹 Deduplicated: {before} → {len(df)} rows (by CodigoArticulo)")

    # 💾 Save full data (only if changed)
    save_if_changed(df, csv_file)

    # 🚿 Automatically clean data via imported module
    print("🚿 Running cleaning process via clean_list_articulo.py ...")
    df_clean = clean_list_articulo.clean_list_articulos(
        input_path=csv_file,
        output_path="data/lista_articulos_clean.csv"
    )

    print("✅ Pipeline complete. Clean file saved as data/lista_articulos_clean.csv")
    return df_clean


# -------------------------------------------------------------------
# 🧪 Standalone test
# -------------------------------------------------------------------
if __name__ == "__main__":
    df_final = get_lista_articulos()
    if not df_final.empty:
        print(df_final.head())
    print("✅ All done. Clean DataFrame ready.")
# === END get_list_articulo.py ===
