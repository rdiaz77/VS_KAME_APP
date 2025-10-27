# === view_db.py ===
import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
import io
from datetime import date

# === CONFIGURATION ===
DB_PATH = Path("data/vitroscience.db")

st.set_page_config(page_title="VitroScience — Database Viewer", layout="wide")
st.title("🧪 VitroScience — Database Viewer")

# === STEP 1: Connect to SQLite ===
if not DB_PATH.exists():
    st.error(f"❌ Database not found at {DB_PATH}. Run your data pipeline first.")
    st.stop()

conn = sqlite3.connect(DB_PATH)

# === STEP 2: Show available tables ===
tables = pd.read_sql(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';", conn
)["name"].tolist()

if not tables:
    st.warning("⚠️ No tables found in the database.")
    st.stop()

selected_table = st.selectbox("📋 Select a table to view:", tables)

# === STEP 3: Table summary ===
st.markdown("### 📅 Data Summary")
summary_query = f"""
SELECT 
    MIN(Fecha) AS earliest_date, 
    MAX(Fecha) AS latest_date, 
    COUNT(*) AS total_rows 
FROM {selected_table};
"""
try:
    summary = pd.read_sql(summary_query, conn)
    st.dataframe(summary, use_container_width=True)
except Exception as e:
    st.warning(f"⚠️ Could not retrieve summary: {e}")

# === STEP 4: Table Preview ===
st.markdown("### 📊 Table Preview")
order_option = st.radio(
    "Order records by:",
    ("Newest first", "Oldest first"),
    horizontal=True,
)

if order_option == "Newest first":
    query = f"SELECT * FROM {selected_table} ORDER BY Fecha DESC LIMIT 200;"
else:
    query = f"SELECT * FROM {selected_table} ORDER BY Fecha ASC LIMIT 200;"

df_preview = pd.read_sql(query, conn)
st.write(f"Showing {len(df_preview)} records from `{selected_table}`:")
st.dataframe(df_preview, use_container_width=True, height=600)

# === STEP 5: Full table download ===
st.markdown("### ⬇️ Download Full Table as CSV")
if st.button("Generate Download Link"):
    with st.spinner("Exporting full table to CSV..."):
        df_full = pd.read_sql(f"SELECT * FROM {selected_table};", conn)
        csv_buffer = io.StringIO()
        df_full.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        today_str = date.today().strftime("%Y-%m-%d")
        file_name = f"{selected_table}_{today_str}.csv"

        st.download_button(
            label=f"💾 Download `{file_name}`",
            data=csv_data,
            file_name=file_name,
            mime="text/csv",
        )
        st.success(f"✅ Exported {len(df_full)} rows successfully!")

# === STEP 6: Custom Query Section ===
st.markdown("---")
st.subheader("🔍 Run a Custom SQL Query")

default_query = f"SELECT * FROM {selected_table} ORDER BY Fecha DESC LIMIT 10;"
custom_query = st.text_area(
    "Enter SQL query below:",
    value=default_query,
    height=120,
)

if st.button("Run Query"):
    try:
        result_df = pd.read_sql(custom_query, conn)
        st.success(f"✅ Query executed successfully! Showing {len(result_df)} rows.")
        st.dataframe(result_df, use_container_width=True)

        # Offer CSV download for query results
        if not result_df.empty:
            csv_buffer = io.StringIO()
            result_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            today_str = date.today().strftime("%Y-%m-%d")
            st.download_button(
                label=f"💾 Download query_results_{today_str}.csv",
                data=csv_data,
                file_name=f"query_results_{today_str}.csv",
                mime="text/csv",
            )
    except Exception as e:
        st.error(f"❌ SQL Error: {e}")

# === STEP 7: Close connection ===
conn.close()

st.markdown("---")
st.caption("VitroScience DB Viewer — View, query, and download your enriched sales data (2023 → today).")
# === End of view_db.py ===
