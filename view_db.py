# === view_db.py ===
import pandas as pd
import streamlit as st

from db_utils import load_from_db

st.set_page_config(page_title="VitroScience Ventas Viewer", layout="wide")
st.title("📊 VitroScience - Ventas Dashboard")

# -------------------------------------------------------
# LOAD DATA
# -------------------------------------------------------
df = load_from_db()

if df.empty:
    st.warning("⚠️ No se encontraron registros en la base de datos.")
    st.stop()

# Normalize column names (handles case mismatches)
df.columns = df.columns.str.strip().str.title()

# -------------------------------------------------------
# SIDEBAR FILTERS
# -------------------------------------------------------
st.sidebar.header("🔎 Filtros de Búsqueda")


# Helper to build a multiselect safely
def get_filter_values(col_name):
    """Return sorted unique values or an empty list if the column doesn't exist."""
    if col_name in df.columns:
        return sorted(df[col_name].dropna().unique().tolist())
    return []


clientes = get_filter_values("Cliente")
comunas = get_filter_values("Comuna")
regiones = get_filter_values("Region")
servicios = get_filter_values("Serviciosalud")
# Note: column may be 'Serviciosalud' or 'ServicioSalud' depending on case
if not servicios and "ServicioSalud" in df.columns:
    servicios = sorted(df["ServicioSalud"].dropna().unique().tolist())

cliente_sel = st.sidebar.multiselect("Cliente", options=clientes)
comuna_sel = st.sidebar.multiselect("Comuna", options=comunas)
region_sel = st.sidebar.multiselect("Región", options=regiones)
servicio_sel = st.sidebar.multiselect("Servicio de Salud", options=servicios)

# Date filter if Fecha exists
if "Fecha" in df.columns:
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    min_date, max_date = df["Fecha"].min(), df["Fecha"].max()
    date_range = st.sidebar.date_input("Rango de fechas", [min_date, max_date])
else:
    date_range = None

# -------------------------------------------------------
# APPLY FILTERS SAFELY
# -------------------------------------------------------
filtered_df = df.copy()


def apply_filter(df, col, values):
    if col in df.columns and values:
        return df[df[col].isin(values)]
    return df


filtered_df = apply_filter(filtered_df, "Cliente", cliente_sel)
filtered_df = apply_filter(filtered_df, "Comuna", comuna_sel)
filtered_df = apply_filter(filtered_df, "Region", region_sel)
filtered_df = apply_filter(filtered_df, "ServicioSalud", servicio_sel)
filtered_df = apply_filter(filtered_df, "Serviciosalud", servicio_sel)  # fallback

if date_range and "Fecha" in filtered_df.columns and len(date_range) == 2:
    start, end = date_range
    filtered_df = filtered_df[
        (filtered_df["Fecha"] >= pd.to_datetime(start))
        & (filtered_df["Fecha"] <= pd.to_datetime(end))
    ]

# -------------------------------------------------------
# DISPLAY TABLE
# -------------------------------------------------------
st.subheader(f"📄 Registros encontrados: {len(filtered_df)}")

if not filtered_df.empty:
    # Format numeric columns nicely
    num_cols = filtered_df.select_dtypes(include=["int", "float"]).columns
    filtered_df[num_cols] = filtered_df[num_cols].applymap(
        lambda x: f"{int(x):,}" if pd.notnull(x) else ""
    )

    st.dataframe(filtered_df, use_container_width=True, hide_index=True)
else:
    st.info("No hay registros que coincidan con los filtros seleccionados.")

# -------------------------------------------------------
# SUMMARY SECTION
# -------------------------------------------------------
st.markdown("---")
st.subheader("📈 Resumen de Ventas")

col1, col2, col3 = st.columns(3)
if "TotalNeto" in df.columns:
    total_sales = pd.to_numeric(df["TotalNeto"], errors="coerce").sum()
    col1.metric("Total Ventas (Neto)", f"${total_sales:,.0f}")

if "Cantidad" in df.columns:
    total_qty = pd.to_numeric(df["Cantidad"], errors="coerce").sum()
    col2.metric("Cantidad Total", f"{total_qty:,.0f}")

col3.metric(
    "Clientes Únicos",
    f"{df['Cliente'].nunique():,}" if "Cliente" in df.columns else "N/A",
)

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
