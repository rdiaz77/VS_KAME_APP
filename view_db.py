# === view_db.py ===
import os

import pandas as pd
import streamlit as st

from db_utils import load_from_db

# -------------------------------------------------------
# STREAMLIT PAGE CONFIG
# -------------------------------------------------------
st.set_page_config(
    page_title="VitroScience Sales Dashboard",
    layout="wide",
)

# -------------------------------------------------------
# LOAD DATA
# -------------------------------------------------------
DB_PATH = os.path.abspath("data/vitroscience.db")
TABLE_NAME = "ventas"

st.title("📊 VitroScience Sales Dashboard")

# Load data from SQLite
df = load_from_db(db_path=DB_PATH, query=f"SELECT * FROM {TABLE_NAME}")

if df.empty:
    st.error(
        "❌ No data found in the database. Check that the table 'ventas' exists and contains rows."
    )
    st.stop()

st.success(f"✅ Loaded {len(df)} rows from database.")

# -------------------------------------------------------
# DATA PREPARATION
# -------------------------------------------------------
# Normalize columns (ensure consistent case)
df.columns = df.columns.str.strip().str.title()

# Ensure expected columns exist
expected_cols = ["Fecha", "Cliente", "Comuna", "Region", "Serviciosalud", "Totalneto"]
for col in expected_cols:
    if col not in df.columns:
        st.warning(f"⚠️ Column missing: {col}")

# Convert Fecha to datetime
if "Fecha" in df.columns:
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")

# -------------------------------------------------------
# FILTERS
# -------------------------------------------------------
st.sidebar.header("🔍 Filters")

# Cliente
clientes = sorted(df["Cliente"].dropna().unique().tolist())
cliente_filter = st.sidebar.multiselect("Cliente", options=clientes, default=[])

# Comuna
comunas = sorted(df["Comuna"].dropna().unique().tolist())
comuna_filter = st.sidebar.multiselect("Comuna", options=comunas, default=[])

# Región
regiones = sorted(df["Region"].dropna().unique().tolist())
region_filter = st.sidebar.multiselect("Región", options=regiones, default=[])

# Servicio de Salud
servicios = sorted(df["Serviciosalud"].dropna().unique().tolist())
servicio_filter = st.sidebar.multiselect(
    "Servicio de Salud", options=servicios, default=[]
)

# Fecha range
if "Fecha" in df.columns:
    min_date, max_date = df["Fecha"].min(), df["Fecha"].max()
    date_range = st.sidebar.date_input("Rango de fechas", [min_date, max_date])
else:
    date_range = None

# Apply filters
df_filtered = df.copy()

if cliente_filter:
    df_filtered = df_filtered[df_filtered["Cliente"].isin(cliente_filter)]
if comuna_filter:
    df_filtered = df_filtered[df_filtered["Comuna"].isin(comuna_filter)]
if region_filter:
    df_filtered = df_filtered[df_filtered["Region"].isin(region_filter)]
if servicio_filter:
    df_filtered = df_filtered[df_filtered["Serviciosalud"].isin(servicio_filter)]
if date_range and len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range)
    df_filtered = df_filtered[
        (df_filtered["Fecha"] >= start_date) & (df_filtered["Fecha"] <= end_date)
    ]

# -------------------------------------------------------
# SUMMARY METRICS
# -------------------------------------------------------
if not df_filtered.empty:
    total_sales = (
        df_filtered["Totalneto"].sum() if "Totalneto" in df_filtered.columns else 0
    )
    total_clients = df_filtered["Cliente"].nunique()
    total_comunas = df_filtered["Comuna"].nunique()

    col1, col2, col3 = st.columns(3)
    col1.metric("💰 Total Ventas (Neto)", f"${total_sales:,.0f}")
    col2.metric("👥 Clientes únicos", total_clients)
    col3.metric("🏙️ Comunas cubiertas", total_comunas)

    # -------------------------------------------------------
    # DATA TABLE
    # -------------------------------------------------------
    st.divider()
    st.subheader("📄 Detalle de Ventas")

    st.dataframe(df_filtered, width="stretch", height=600)

    # CSV download
    csv = df_filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Descargar CSV",
        csv,
        "ventas_filtradas.csv",
        "text/csv",
        use_container_width=True,  # download_button still supports this param
    )
else:
    st.warning("⚠️ No data matches your filters.")

# === END view_db.py ===
