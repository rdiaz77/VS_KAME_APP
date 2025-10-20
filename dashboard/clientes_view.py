# dashboard/clientes_view.py
import streamlit as st
from utils.db_utils import load_table

def show_clientes():
    st.header("👥 Clientes — Dashboard")
    df = load_table("clientes")  # or whatever your table name is
    st.dataframe(df.head(20), use_container_width=True)
