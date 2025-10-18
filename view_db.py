import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path

# === CONFIGURATION ===
DB_PATH = Path("data/vitroscience.db")

st.title("🧪 VitroScience — Database Viewer")

# === STEP 1: Connect to SQLite ===
if not DB_PATH.exists():
    st.error(f"Database not found at {DB_PATH}. Run `save_to_sqlite.py` first.")
    st.stop()

conn = sqlite3.connect(DB_PATH)

# === STEP 2: Show available tables ===
tables = pd.read_sql(
    "SELECT name FROM sqlite_master WHERE type='table';", conn
)["name"].tolist()

if not tables:
    st.warning("No tables found in the database.")
    st.stop()

selected_table = st.selectbox("Select a table to view:", tables)

# === STEP 3: Show table info ===
st.subheader(f"📊 Preview of `{selected_table}`")

query = f"SELECT * FROM {selected_table} LIMIT 100;"
df = pd.read_sql(query, conn)
st.dataframe(df, use_container_width=True)

# === STEP 4: Optional query input ===
st.markdown("---")
st.subheader("🔍 Run a custom SQL query")

custom_query = st.text_area(
    "Enter SQL query",
    value=f"SELECT * FROM {selected_table} LIMIT 10;",
    height=120,
)

if st.button("Run Query"):
    try:
        result_df = pd.read_sql(custom_query, conn)
        st.success(f"Query executed successfully! Showing {len(result_df)} rows.")
        st.dataframe(result_df, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

conn.close()
