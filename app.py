# app.py — VS_KAME_APP Streamlit Dashboard
# -------------------------------------------------------------
# Features
# - Loads ventas_2024_revised.csv (cleaned sales)
# - Sidebar filters: Vendedor, Cliente, Unidad de Negocio, Fecha
# - KPIs: Total Ventas, Margen, Nº Documentos, % Margen, Ticket Promedio
# - Charts: Ventas por Vendedor, Ventas por Unidad de Negocio
# - Map: Chile choropleth by Región (sum of Total)
#
# Notes
# - Place your Chile regions GeoJSON at: data/chile_regiones.geojson
#   (A standard file with the 16 regiones. Common property keys include
#    name, NOMBRE_REG, NOM_REGION, Region, etc.)
# - The app attempts to auto-detect the correct property key and match
#   region names robustly (accents, punctuation, common variants like
#   "Metropolitana", "O'Higgins", "Biobio", etc.).
# - If your column names differ (e.g., VendedorNombre), the app will
#   detect from a list of plausible options.
# -------------------------------------------------------------

import json
import unicodedata
from datetime import date

import pandas as pd
import numpy as np
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="VS KAME – Ventas",
    layout="wide",
    page_icon="💰",
)

# ----------------------------
# Configurable paths
# ----------------------------
CSV_PATH = "ventas_2024_revised.csv"  # Adjust if needed
GEOJSON_PATH = "data/chile_regiones.geojson"  # Put the GeoJSON here

# ----------------------------
# Helpers
# ----------------------------

@st.cache_data(show_spinner=False)
def load_ventas(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # Parse Fecha to datetime if present
    fecha_col = pick_col(df, ["Fecha", "fecha", "FechaEmision", "fecha_emision"])  # best-guess list
    if fecha_col:
        df[fecha_col] = pd.to_datetime(df[fecha_col], errors="coerce")

    # Coerce numeric columns used in KPIs/charts if present
    for col in [
        "Total",
        "MargenContrib",
        "CostoVentaTotal",
        "CostoVentaUnitario",
        "PrecioUnitario",
        "Cantidad",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Strip + standardize text columns likely used as filters
    for cname in [
        "Vendedor", "VendedorNombre", "NombreVendedor",
        "Cliente", "ClienteNombre", "RazonSocial",
        "UnidadNegocio", "UnidadNegocioNombre",
        "Region", "REGION", "Comuna", "COMUNA",
        "Descripcion"
    ]:
        if cname in df.columns:
            df[cname] = df[cname].astype(str).str.strip()

    return df


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name that exists in df from candidates list."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


@st.cache_data(show_spinner=False)
def load_geojson(path: str) -> dict | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            gj = json.load(f)
        return gj
    except Exception as e:
        st.warning(f"No se pudo cargar el GeoJSON desde {path}. {e}")
        return None


def normalize_name(s: str) -> str:
    """Lowercase, strip accents/punctuation/spaces for robust matching."""
    if not isinstance(s, str):
        s = str(s)
    s = s.strip().lower()
    s = (
        unicodedata.normalize("NFD", s)
        .encode("ascii", "ignore")
        .decode("utf-8")
    )
    # Remove punctuation and extra spaces
    for ch in ["'", "`", "´", ",", ".", "-", "(", ")"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s


def detect_region_prop_key(geojson: dict) -> str | None:
    """Try common property keys to locate the region name property in features."""
    candidate_keys = [
        "name", "region", "NOM_REGION", "NOMBRE_REG", "Region", "NOMBRE",
        "nom_region", "NOM_REGION_", "REGION"
    ]
    features = geojson.get("features", [])
    if not features:
        return None
    props = features[0].get("properties", {})
    for k in candidate_keys:
        if k in props:
            return k
    # Fallback: pick first string-like property
    for k, v in props.items():
        if isinstance(v, str):
            return k
    return None


def build_region_map(df: pd.DataFrame, geojson: dict):
    region_col = pick_col(df, ["Region", "REGION"])  # required for choropleth
    total_col = pick_col(df, ["Total", "total"])
    if not region_col or not total_col:
        st.info("No se encontraron columnas 'Region' y 'Total' para el mapa.")
        return

    # Sum ventas por región
    reg = (
        df[[region_col, total_col]]
        .groupby(region_col, dropna=True, as_index=False)
        .sum(numeric_only=True)
        .rename(columns={region_col: "Region", total_col: "Total"})
    )

    # Detect geojson key
    prop_key = detect_region_prop_key(geojson)
    if not prop_key:
        st.info("No se pudo detectar la clave de región en el GeoJSON.")
        return

    # Build lookup from geojson names (normalized) -> original geojson property value
    features = geojson.get("features", [])
    gj_names = []
    for ft in features:
        nm = ft.get("properties", {}).get(prop_key, None)
        if nm is not None:
            gj_names.append(str(nm))

    gj_norm_map = {normalize_name(n): n for n in gj_names}

    # Convert our DF region names to the exact geojson labels using normalization
    reg["_norm"] = reg["Region"].map(normalize_name)

    # Handle common aliasing explicitly
    # e.g., Metropolitana, Region Metropolitana de Santiago, etc.
    aliases = {
        normalize_name("Metropolitana de Santiago"): normalize_name("Metropolitana de Santiago"),
        normalize_name("Santiago"): normalize_name("Metropolitana de Santiago"),
        normalize_name("Region Metropolitana"): normalize_name("Metropolitana de Santiago"),
        normalize_name("Ohiggins"): normalize_name("Libertador General Bernardo O'Higgins"),
        normalize_name("O'Higgins"): normalize_name("Libertador General Bernardo O'Higgins"),
        normalize_name("Libertador General Bernardo OHiggins"): normalize_name("Libertador General Bernardo O'Higgins"),
        normalize_name("Nuble"): normalize_name("Ñuble"),
        normalize_name("Biobio"): normalize_name("Biobío"),
        normalize_name("Aysen"): normalize_name("Aysén"),
        normalize_name("Magallanes"): normalize_name("Magallanes y de la Antártica Chilena"),
    }
    reg["_norm"] = reg["_norm"].replace(aliases)

    # Map to geojson exact names
    reg["region_geo"] = reg["_norm"].map(gj_norm_map)

    # Report anything not matched
    missing = reg[reg["region_geo"].isna()][["Region", "Total"]]
    if not missing.empty:
        with st.expander("Regiones no mapeadas (revisar nombres)"):
            st.dataframe(missing)

    # Keep only matched rows
    reg_mapped = reg.dropna(subset=["region_geo"]).copy()

    # Choropleth
    fig = px.choropleth(
        reg_mapped,
        geojson=geojson,
        locations="region_geo",
        featureidkey=f"properties.{prop_key}",
        color="Total",
        projection="mercator",
        title="Ventas por Región (Total)",
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)


# ----------------------------
# Load data
# ----------------------------
with st.sidebar:
    st.title("VS KAME – Ventas")
    st.caption("Dashboard de ventas y márgenes")

ventas = load_ventas(CSV_PATH)
if ventas is None or ventas.empty:
    st.error("No se pudo cargar el CSV de ventas. Verifique la ruta y el formato.")
    st.stop()

# Identify columns dynamically
fecha_col = pick_col(ventas, ["Fecha", "FechaEmision", "fecha", "fecha_emision"])  # datetime
vendedor_col = pick_col(ventas, ["Vendedor", "VendedorNombre", "NombreVendedor"]) 
cliente_col = pick_col(ventas, ["Cliente", "ClienteNombre", "RazonSocial"]) 
uneg_col = pick_col(ventas, ["UnidadNegocio", "UnidadNegocioNombre"]) 
region_col = pick_col(ventas, ["Region", "REGION"]) 
comuna_col = pick_col(ventas, ["Comuna", "COMUNA"]) 
folio_col = pick_col(ventas, ["Folio", "folio"]) 

# ----------------------------
# Sidebar filters
# ----------------------------
with st.sidebar:
    st.subheader("Filtros")

    # Date filter
    if fecha_col:
        min_d = pd.to_datetime(ventas[fecha_col].min()).date() if ventas[fecha_col].notna().any() else date(2024, 1, 1)
        max_d = pd.to_datetime(ventas[fecha_col].max()).date() if ventas[fecha_col].notna().any() else date(2024, 12, 31)
        fecha_range = st.date_input(
            "Rango de fechas",
            value=(min_d, max_d),
            min_value=min_d,
            max_value=max_d,
        )
    else:
        fecha_range = None

    vend_sel = None
    if vendedor_col:
        vend_sel = st.multiselect("Vendedor", sorted(ventas[vendedor_col].dropna().unique()))

    cli_sel = None
    if cliente_col:
        cli_sel = st.multiselect("Cliente", sorted(ventas[cliente_col].dropna().unique()))

    uneg_sel = None
    if uneg_col:
        uneg_sel = st.multiselect("Unidad de Negocio", sorted(ventas[uneg_col].dropna().unique()))

    region_sel = None
    if region_col:
        region_sel = st.multiselect("Región", sorted(ventas[region_col].dropna().unique()))

# Apply filters
filtered = ventas.copy()
if fecha_col and fecha_range:
    start_d, end_d = fecha_range
    mask = (filtered[fecha_col] >= pd.to_datetime(start_d)) & (filtered[fecha_col] <= pd.to_datetime(end_d))
    filtered = filtered[mask]

if vendedor_col and vend_sel:
    filtered = filtered[filtered[vendedor_col].isin(vend_sel)]
if cliente_col and cli_sel:
    filtered = filtered[filtered[cliente_col].isin(cli_sel)]
if uneg_col and uneg_sel:
    filtered = filtered[filtered[uneg_col].isin(uneg_sel)]
if region_col and region_sel:
    filtered = filtered[filtered[region_col].isin(region_sel)]

# ----------------------------
# KPIs
# ----------------------------
col_total = pick_col(filtered, ["Total", "total"])  # ventas totales
col_margen = pick_col(filtered, ["MargenContrib", "margen", "Margen"])  # margen contribución
col_folio = folio_col

total_ventas = float(filtered[col_total].sum()) if col_total else np.nan
margen_total = float(filtered[col_margen].sum()) if col_margen else np.nan
n_docs = int(filtered[col_folio].nunique()) if col_folio else len(filtered)
porc_margen = (margen_total / total_ventas * 100.0) if (col_margen and col_total and total_ventas) else np.nan
avg_ticket = (total_ventas / n_docs) if (n_docs and total_ventas) else np.nan

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Ventas", f"${total_ventas:,.0f}")
k2.metric("Margen", f"${margen_total:,.0f}")
k3.metric("% Margen", f"{porc_margen:,.1f}%" if not np.isnan(porc_margen) else "-")
k4.metric("# Documentos", f"{n_docs:,}")
k5.metric("Ticket Promedio", f"${avg_ticket:,.0f}" if not np.isnan(avg_ticket) else "-")

st.divider()

# ----------------------------
# Charts
# ----------------------------
left, right = st.columns(2)

with left:
    if vendedor_col and col_total:
        top_vend = (
            filtered[[vendedor_col, col_total]]
            .groupby(vendedor_col, as_index=False)
            .sum(numeric_only=True)
            .sort_values(col_total, ascending=False)
            .head(15)
        )
        fig1 = px.bar(top_vend, x=vendedor_col, y=col_total, title="Ventas por Vendedor (Top 15)")
        fig1.update_layout(xaxis_title="Vendedor", yaxis_title="Total")
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("No hay columna de Vendedor o Total para graficar.")

with right:
    if uneg_col and col_total:
        by_uneg = (
            filtered[[uneg_col, col_total]]
            .groupby(uneg_col, as_index=False)
            .sum(numeric_only=True)
            .sort_values(col_total, ascending=False)
        )
        fig2 = px.bar(by_uneg, x=uneg_col, y=col_total, title="Ventas por Unidad de Negocio")
        fig2.update_layout(xaxis_title="Unidad de Negocio", yaxis_title="Total")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No hay columna de Unidad de Negocio o Total para graficar.")

st.divider()

# ----------------------------
# Map (Choropleth by Región)
# ----------------------------
geojson = load_geojson(GEOJSON_PATH)
if geojson:
    build_region_map(filtered, geojson)
else:
    st.info(
        "Para habilitar el mapa, agregue el archivo GeoJSON de regiones de Chile en 'data/chile_regiones.geojson'."
    )

st.divider()

# ----------------------------
# Detail table & download
# ----------------------------
st.subheader("Detalle de Ventas (filtrado)")
st.dataframe(filtered, use_container_width=True, hide_index=True)

csv_bytes = filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Descargar CSV filtrado",
    data=csv_bytes,
    file_name="ventas_filtrado.csv",
    mime="text/csv",
)
# end of app.py