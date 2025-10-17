# === view_db.py ===
import streamlit as st
import pandas as pd
from db_utils import load_from_db

# -------------------------------------------------------
# STREAMLIT PAGE CONFIG
# -------------------------------------------------------
st.set_page_config(page_title="VitroScience - Ventas Viewer", layout="wide")
st.title("📊 VitroScience - Base de Datos de Ventas")

# -------------------------------------------------------
# LOAD DATA FROM DATABASE
# -------------------------------------------------------
st.sidebar.header("🔎 Controles de Búsqueda")
df = load_from_db()

if df.empty:
    st.warning("⚠️ No se encontraron registros en la base de datos.")
    st.stop()

# -------------------------------------------------------
# FILTERS
# -------------------------------------------------------
# Convert Fecha if necessary
if "Fecha" in df.columns:
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")

# Sidebar filters
clientes = sorted(df["Cliente"].dropna().unique().tolist())
comunas = sorted(df["Comuna"].dropna().unique().tolist())

cliente_sel = st.sidebar.multiselect("Cliente", options=clientes, default=[])
comuna_sel = st.sidebar.multiselect("Comuna", options=comunas, default=[])

if "Fecha" in df.columns:
    min_date, max_date = df["Fecha"].min(), df["Fecha"].max()
    date_range = st.sidebar.date_input(
        "Rango de fechas", [min_date, max_date]
    )

# Apply filters
filtered_df = df.copy()

if cliente_sel:
    filtered_df = filtered_df[filtered_df["Cliente"].isin(cliente_sel)]

if comuna_sel:
    filtered_df = filtered_df[filtered_df["Comuna"].isin(comuna_sel)]

if "Fecha" in df.columns and len(date_range) == 2:
    start, end = date_range
    filtered_df = filtered_df[
        (filtered_df["Fecha"] >= pd.to_datetime(start))
        & (filtered_df["Fecha"] <= pd.to_datetime(end))
    ]

# -------------------------------------------------------
# DISPLAY RESULTS
# -------------------------------------------------------
st.subheader(f"📄 Registros: {len(filtered_df)} filas")

# Format numeric columns (thousands separators, integer display)
num_cols = filtered_df.select_dtypes(include=["int", "float"]).columns
filtered_df[num_cols] = filtered_df[num_cols].applymap(
    lambda x: f"{int(x):,}" if pd.notnull(x) else ""
)

st.dataframe(filtered_df, use_container_width=True, hide_index=True)

# -------------------------------------------------------
# SUMMARY STATS
# -------------------------------------------------------
st.markdown("---")
st.subheader("📈 Resumen de Ventas")

col1, col2, col3 = st.columns(3)
if "TotalNeto" in df.columns:
    total_sales = pd.to_numeric(df["TotalNeto"], errors="coerce").sum()
    st.metric("Total Ventas (Neto)", f"${total_sales:,.0f}")
if "Cantidad" in df.columns:
    total_qty = pd.to_numeric(df["Cantidad"], errors="coerce").sum()
    col2.metric("Cantidad Total", f"{total_qty:,.0f}")
col3.metric("Clientes Únicos", f"{df['Cliente'].nunique():,}")

# -------------------------------------------------------
# DOWNLOAD SECTION
# -------------------------------------------------------
st.markdown("---")
csv = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="💾 Descargar datos filtrados (CSV)",
    data=csv,
    file_name="ventas_filtradas.csv",
    mime="text/csv",
)
# === END view_db.py ===