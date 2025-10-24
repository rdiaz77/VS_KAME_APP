import json
import os

p = "test/ventas/raw/ventas_raw_2023-01-01_to_2023-12-31.json"
assert os.path.exists(p), f"File not found: {p}"
with open(p, "r", encoding="utf-8") as f:
    data = json.load(f)

print("Top-level keys:", list(data.keys()))
items_key = "items" if "items" in data else ("data" if "data" in data else None)
print("Detected items_key:", items_key)

if items_key:
    items = data.get(items_key, [])
    print("Items count:", len(items))
    if items:
        print("First item keys:", list(items[0].keys())[:20])
else:
    # Sometimes APIs return an error object or a message field
    print("No 'items' or 'data' key found. Here is a short preview:")
    s = json.dumps(data, ensure_ascii=False)[:600]
    print(s)
