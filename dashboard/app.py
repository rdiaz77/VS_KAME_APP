# === dashboard/app.py ===
import sys
from pathlib import Path

# --- Bootstrap imports (DO NOT MOVE THIS BLOCK) ---
ROOT_DIR = Path(__file__).resolve().parent.parent  # Points to VS_KAME_APP
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))  # ensure it’s at the front

# --- Standard libs AFTER bootstrap ---
import streamlit as st

# --- Local imports ---
from dashboard.scorecard import show_scorecard
from dashboard.clientes_view import show_clientes
from dashboard.cta_por_cobrar_view import show_cta_cobrar
from dashboard.cta_por_pagar_view import show_cta_pagar
from dashboard.inventario_view import show_inventario
from dashboard.settings_view import show_settings


st.set_page_config(page_title="VitroScience Dashboard", layout="wide")

st.sidebar.title("📊 Navigation")
page = st.sidebar.radio(
    "Go to:",
    [
        "🏠 Scorecard",
        "💰 Ventas",
        "👥 Clientes",
        "📦 Inventario",
        "💰 Ctas por Cobrar",
        "🧾 Ctas por Pagar",
        "⚙️ Settings",
    ],
)

if page == "🏠 Scorecard":
    show_scorecard()
elif page == "👥 Clientes":
    show_clientes()
elif page == "📦 Inventario":
    show_inventario()
elif page == "💰 Ctas por Cobrar":
    show_cta_cobrar()
elif page == "🧾 Ctas por Pagar":
    show_cta_pagar()
elif page == "⚙️ Settings":
    show_settings()
