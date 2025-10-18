# Old:
# from clean_sales import clean_sales
# df_clean = clean_sales(df_raw)

# New:
from pipeline.clean_sales_main import run_clean_sales_pipeline

df_clean = run_clean_sales_pipeline(df_raw)
