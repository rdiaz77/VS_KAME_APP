# === view_db.py ===
import streamlit as st
import pandas as pd
from db_utils import load_from_db, list_tables

# -------------------------------------------------------
# APP CONFIG
# -------------------------------------------------------
st.set_page_config(page_title="VitroScience Ventas DB Viewer", layout="wide")
st.title("📊 VitroScience — Ventas Database Viewer")
st.caption("Inspect data stored in your local SQLite database (`data/vitroscience.db`).")


# -------------------------------------------------------
# LOAD DATA
# -------------------------------------------------------
@st.cache_data(show_spinner=False)
def get_data(table_name: str = "ventas"):
    """Load data safely from DB."""
    try:
        df = load_from_db(db_path="data/vitroscience.db", table_name=table_name)
        if df.empty:
            st.warning(f"⚠️ No data found in '{table_name}' — table is empty.")
            return pd.DataFrame()
        return df
    except Exception as e:
        st.error(f"❌ Failed to load data: {e}")
        return pd.DataFrame()


# -------------------------------------------------------
# SIDEBAR CONTROLS
# -------------------------------------------------------
st.sidebar.header("⚙️ Settings")

tables = list_tables("data/vitroscience.db")
if not tables:
    st.error("No tables found in database. Please run the ETL pipeline first.")
    st.stop()

table_selected = st.sidebar.selectbox("Select table", options=tables, index=tables.index("ventas") if "ventas" in tables else 0)
df = get_data(table_selected)

if df.empty:
    st.stop()

st.sidebar.markdown("### 🔍 Filters")

# Add filters dynamically for smaller datasets
if len(df) <= 5000:
    for col in df.columns[:5]:  # show top 5 columns for filtering
        unique_vals = sorted(df[col].dropna().unique().tolist())
        if len(unique_vals) < 100:
            selected = st.sidebar.multiselect(f"Filter by {col}", unique_vals)
            if selected:
                df = df[df[col].isin(selected)]


# -------------------------------------------------------
# DISPLAY
# -------------------------------------------------------
st.subheader(f"📦 Table: `{table_selected}` — {len(df)} rows × {len(df.columns)} columns")

# Replace deprecated use_container_width
st.dataframe(df, width="stretch", height=600)

# Basic stats
with st.expander("📈 Summary statistics"):
    st.dataframe(df.describe(include="all").transpose(), width="stretch")

# Preview bottom rows
with st.expander("🔽 Last 10 rows"):
    st.dataframe(df.tail(10), width="stretch")
