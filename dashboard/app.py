# dashboard/app.py
import streamlit as st
from clientes_view import show_clientes
from cta_por_cobrar_view import show_cta_cobrar
from cta_por_pagar_view import show_cta_pagar
from inventario_view import show_inventario
from scorecard import show_scorecard
from settings_view import show_settings

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
