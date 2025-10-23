# dashboard/scorecard.py
import streamlit as st

from dashboard.tabs.sales_analysis_tab import show_sales_analysis


def show_scorecard():
    st.title("🏠 Balanced Scorecard — VitroScience")

    tabs = st.tabs(
        [
            "📊 Sales Analysis",
            "👥 Customers",
            "⚙️ Internal Processes",
            "📚 Learning & Growth",
        ]
    )

    with tabs[0]:
        show_sales_analysis()

    with tabs[1]:
        st.write("👥 Customers — coming soon")

    with tabs[2]:
        st.write("⚙️ Internal Processes — coming soon")

    with tabs[3]:
        st.write("📚 Learning & Growth — coming soon")
