from kame_api import get_token
import requests
import pandas as pd
import os


def get_stock_articulo(nombre_articulo, csv_file="stock_articulo.csv"):
    """
    Fetch stock information for a specific article from Kame API 
    and save (or append) to a CSV file.
    """

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    nombre_articulo_encoded = requests.utils.quote(nombre_articulo, safe='')
    url = f"https://api.kameone.cl/api/Inventario/getStockArticulo/{nombre_articulo_encoded}"

    print(f"🔍 Fetching stock for artículo: '{nombre_articulo}' ...")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: {response.text}")
        return pd.DataFrame()

    data = response.json()

    # Convert to DataFrame safely
    if isinstance(data, dict):
        data = [data]
    elif not isinstance(data, list):
        print("⚠️ Unexpected response format")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    print(f"✅ Retrieved {len(df)} stock record(s).")

    # Decide on key columns for deduplication
    key_cols = [c for c in ["Articulo", "CodigoArticulo",
                            "NombreArticulo"] if c in df.columns]
    if not key_cols:
        key_cols = df.columns.tolist()  # fallback to all columns

    # Append or create CSV
    if os.path.exists(csv_file):
        existing_df = pd.read_csv(csv_file)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.drop_duplicates(
            subset=key_cols, inplace=True, ignore_index=True)
        combined_df.to_csv(csv_file, index=False)
        print(f"💾 Appended to {csv_file} (now {len(combined_df)} rows)")
    else:
        df.to_csv(csv_file, index=False)
        print(f"💾 Created new file: {csv_file}")

    return df


if __name__ == "__main__":
    articulo = "DZ117A-CON"
    df_stock = get_stock_articulo(articulo)
    print(df_stock.head())
