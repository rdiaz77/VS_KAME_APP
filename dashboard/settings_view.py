# dashboard/settings_view.py
import streamlit as st

def show_settings():
    st.header("⚙️ Settings — VitroScience Dashboard")

    st.info("Here you can add configuration options for your app.")
    st.text("Example: Choose default date range, theme, or refresh rate.")
