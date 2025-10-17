from get_ventas import get_informe_ventas_json
from clean_sales import clean_sales
from db_utils import save_to_db, load_from_db

# 1️⃣ Fetch from API
df_raw = get_informe_ventas_json("2024-01-01", "2024-01-31")

# 2️⃣ Clean and standardize
df_clean = clean_sales(df_raw)

# 3️⃣ Save to DB
save_to_db(df_clean)

# 4️⃣ Verify
df_check = load_from_db("SELECT Folio, Cliente, Comuna, TotalNeto FROM ventas LIMIT 5;")
print(df_check)
#== END =========================================================================