import requests

from kame_api import get_token

BASE_URL = "https://api.kameone.cl/api/Maestro/getListArticulo"

token = get_token()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

for p in range(1, 5):
    resp = requests.get(BASE_URL, headers=headers, params={"page": p, "per_page": 500})
    data = resp.json()
    print(
        f"Page {p}: {len(data.get('items', []))} artículos, total={data.get('total')}"
    )
