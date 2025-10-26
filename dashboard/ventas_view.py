# === dashboard/ventas.py ===
import streamlit as st

from dashboard.tabs.sales_analysis_tab import show_sales_analysis


def show_ventas():
    """Main view for Ventas section."""
    st.title("💰 Ventas — VitroScience")

    tabs = st.tabs(
        [
            "📊 Sales Analysis",
        ]
    )

    with tabs[0]:
        show_sales_analysis()


# === End of dashboard/ventas.py ===
