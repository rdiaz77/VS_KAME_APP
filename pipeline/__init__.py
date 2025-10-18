# === pipeline/__init__.py ===
"""
VitroScience Sales Data Cleaning Pipeline
-----------------------------------------
This package contains modular components for the KAME ERP sales ETL process.

Modules:
- cleaning_utils.py       → normalization, renaming, numeric cleanup
- enrich_location.py       → adds Region & ServicioSalud via comuna mapping
- enrich_product.py        → syncs Nombreunegocio from product master
- formatting_utils.py      → human-friendly formatting (e.g., thousand separators)
- clean_sales_main.py      → orchestrator for full cleaning pipeline

Usage example:
    from pipeline.clean_sales_main import clean_sales
    df_clean = clean_sales(df_raw)
"""

__all__ = [
    "clean_sales_main",
    "cleaning_utils",
    "enrich_location",
    "enrich_product",
    "formatting_utils",
]
# === END __init__.py ===
