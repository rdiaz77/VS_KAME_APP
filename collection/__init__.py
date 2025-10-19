# === collection/__init__.py ===
"""
Collection package for managing 'Cuentas por Cobrar' data.

Exports:
- clean_collection(): cleans and formats raw CxC data.
"""

from .clean_collection import clean_collection

__all__ = ["clean_collection"]
# === END collection/__init__.py ===
