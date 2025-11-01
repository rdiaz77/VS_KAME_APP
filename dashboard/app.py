# === dashboard/app.py ===
import sys
from pathlib import Path

import streamlit as st

# --- Bootstrap imports (DO NOT MOVE THIS BLOCK) ---
ROOT_DIR = Path(__file__).resolve().parent.parent  # Points to VS_KAME_APP
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))  # ensure it’s at the front

from dashboard.clientes_view import show_clientes
from dashboard.cta_por_cobrar_view import show_cta_cobrar
from dashboard.cta_por_pagar_view import show_cta_pagar
from dashboard.inventario_view import show_inventario

# === Local Imports ===
from dashboard.scorecard_view import show_scorecard
from dashboard.settings_view import show_settings
from dashboard.ventas_view import show_ventas

# === Streamlit Config ===
st.set_page_config(page_title="VitroScience Dashboard", layout="wide")

# === Sidebar Navigation ===
st.sidebar.title("📊 Navigation")
st.sidebar.markdown("👤 **Logged in as:** VitroScience Admin")
st.sidebar.markdown("---")

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

# === Page Routing ===
if page == "🏠 Scorecard":
    show_scorecard()

elif page == "💰 Ventas":
    show_ventas()

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

# === Footer ===
st.sidebar.markdown("---")
st.sidebar.caption("🧠 Developed for **VitroScience**  |  Data sourced from KAME ERP")
# === END dashboard/app.py ===
