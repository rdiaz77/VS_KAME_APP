import json
import os

import pandas as pd
import requests

from kame_api import get_token


def get_informe_ventas_json(fecha_desde, fecha_hasta, page=1, per_page=100):
    """
    Fetch Informe de Ventas from Kame API and return JSON + save raw files for inspection.
    """
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    url = (
        "https://api.kameone.cl/api/Documento/getInformeVentas"
        f"?page={page}&per_page={per_page}"
        f"&fechaDesde={fecha_desde}&fechaHasta={fecha_hasta}"
    )

    print(f"🔍 Fetching ventas from {fecha_desde} to {fecha_hasta} ...")
    response = requests.get(url, headers=headers, timeout=15)

    if response.status_code != 200:
        print("❌ Error:", response.status_code, response.text)
        return None

    data = response.json()

    # Save raw JSON
    os.makedirs("test", exist_ok=True)
    json_path = f"test/ventas/raw/ventas_raw_{fecha_desde}_to_{fecha_hasta}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Saved raw JSON to {json_path}")

    # Convert to DataFrame and save as CSV
    ventas = data.get("items", [])
    if not ventas:
        print(f"⚠️ No sales data found for {fecha_desde} → {fecha_hasta}")
        return None

    df = pd.json_normalize(ventas)
    csv_path = f"test/ventas/raw/ventas_raw_{fecha_desde}_to_{fecha_hasta}.csv"
    df.to_csv(csv_path, index=False)
    print(f"💾 Saved normalized CSV to {csv_path} ({len(df)} rows)")

    print("\n🧩 Columns returned by API:")
    for col in df.columns:
        print("  -", col)

    return df


if __name__ == "__main__":
    # Example: fetch January 2024 for testing
    get_informe_ventas_json("2024-01-01", "2024-01-31")
# === END OF FILE ===
