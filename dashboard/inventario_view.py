import streamlit as st
from utils.db_utils import load_table

def show_inventario():
    st.header("📦 Inventario — Stock Overview")
    df = load_table("inventario")
    st.dataframe(df.head(20), use_container_width=True)
