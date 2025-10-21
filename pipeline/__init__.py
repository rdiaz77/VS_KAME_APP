"""
get sales package for VitroScience.

Modules:
- clean_sales_main: Cleans raw stock data from KAME ERP.
- enrich_location: add Region and SS to main file.
- enrich_product: add Unegocio to main file.
"""

from .clean_sales_main import run_clean_sales_pipeline
from .enrich_location import add_location_info
from .enrich_product import add_product_info
from .save_to_sqlite import save_to_sqlite

__all__ = [
    "run_clean_sales_pipeline",
    "add_location_info",
    "add_product_info",
    "save_to_sqlite",
]
# Marks the pipeline directory as a Python package.
