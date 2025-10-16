from kame_api import get_token
import requests
import pandas as pd
import os
from datetime import datetime, timedelta


def get_informe_ventas_df(
    fecha_desde,
    fecha_hasta,
    page=1,
    per_page=100,
    csv_file="ventas_2024.csv"
):
    """Fetch Informe de Ventas from Kame API and append to a single CSV file."""
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    url = (
        "https://api.kameone.cl/api/Documento/getInformeVentas"
        f"?page={page}&per_page={per_page}"
        f"&fechaDesde={fecha_desde}&fechaHasta={fecha_hasta}"
    )

    print(f"🔍 Fetching ventas from {fecha_desde} to {fecha_hasta} ...")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print("❌ Error:", response.status_code, response.text)
        return pd.DataFrame()

    data = response.json()
    ventas = data.get("items", [])

    if not ventas:
        print(f"⚠️ No sales data found for {fecha_desde} → {fecha_hasta}")
        return pd.DataFrame()

    df = pd.DataFrame(ventas)
    print(f"✅ Retrieved {len(df)} records.")

    # Append or create CSV
    if os.path.exists(csv_file):
        existing_df = pd.read_csv(csv_file)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.drop_duplicates(subset=["Folio", "Fecha"], inplace=True)
        combined_df.to_csv(csv_file, index=False)
        print(f"💾 Appended to {csv_file} (now {len(combined_df)} rows)")
    else:
        df.to_csv(csv_file, index=False)
        print(f"💾 Created new file: {csv_file}")

    return df


def get_month_ranges(year):
    """Generate a list of (start_date, end_date) for each month of a given year."""
    ranges = []
    for month in range(1, 13):
        start_date = datetime(year, month, 1)
        # The trick: next month minus one day = end of this month
        next_month = start_date.replace(day=28) + timedelta(days=4)
        end_date = next_month - timedelta(days=next_month.day)
        ranges.append((start_date.strftime("%Y-%m-%d"),
                      end_date.strftime("%Y-%m-%d")))
    return ranges


def fetch_all_months(year=2024):
    """Fetch ventas for all months of the year and consolidate them into one CSV."""
    csv_file = f"ventas_{year}.csv"
    month_ranges = get_month_ranges(year)

    print(f"📆 Fetching sales for all months of {year} ...")
    for fecha_desde, fecha_hasta in month_ranges:
        get_informe_ventas_df(fecha_desde, fecha_hasta, csv_file=csv_file)

    print(f"✅ All months fetched. Combined file saved as {csv_file}")


if __name__ == "__main__":
    fetch_all_months(2024)
