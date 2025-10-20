# dashboard/cta_por_cobrar_view.py
import streamlit as st
from utils.db_utils import load_table

def show_cta_cobrar():
    st.header("💰 Cuentas por Cobrar — Dashboard")

    try:
        df = load_table("cta_por_cobrar")  # or your actual table name
        st.dataframe(df.head(20), use_container_width=True)
    except Exception as e:
        st.error(f"Error loading data: {e}")
