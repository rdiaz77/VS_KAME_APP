# === dashboard/tabs/clients_view.py ===
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# === PATH SETUP ===
ROOT_DIR = Path(__file__).resolve().parent.parent  # points to VS_KAME_APP
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "vitroscience.db"

# === Import Wheeler chart ===
from dashboard.tabs.statistics.clients_wheeler_analysis import show_clients_wheeler_analysis


# === Helpers ===
def get_client_data():
    """Fetch client sales and payment data from the database."""
    if not DB_PATH.exists():
        st.error(f"❌ Database not found at {DB_PATH}")
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    df_sales = pd.read_sql("SELECT * FROM ventas_enriched_product;", conn)
    conn.close()

    df_sales["Fecha"] = pd.to_datetime(df_sales["Fecha"], errors="coerce")

    if "FechaPago" in df_sales.columns:
        df_sales["FechaPago"] = pd.to_datetime(df_sales["FechaPago"], errors="coerce")

    df_sales = df_sales.dropna(subset=["Fecha"])
    return df_sales


def get_cta_por_cobrar(year=None, month=None):
    """
    Fetch pending balances and counts per client from cuentas_por_cobrar.
    Filters by Fecha for current year (and month if provided),
    and also computes YTD totals since 2023-01-01.
    Returns tuple: (df_current, df_ytd)
    """
    if not DB_PATH.exists():
        return pd.DataFrame(), pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    try:
        df_cxc = pd.read_sql("SELECT Rut, Fecha, Saldo FROM cuentas_por_cobrar;", conn)
    except Exception as e:
        st.warning(f"⚠️ Could not load 'cuentas_por_cobrar' table: {e}")
        df_cxc = pd.DataFrame(columns=["Rut", "Fecha", "Saldo"])
    finally:
        conn.close()

    if df_cxc.empty:
        return df_cxc, df_cxc

    # --- Normalize types ---
    df_cxc["Saldo"] = pd.to_numeric(df_cxc["Saldo"], errors="coerce").fillna(0)
    df_cxc["Fecha"] = pd.to_datetime(df_cxc["Fecha"], errors="coerce")
    df_cxc = df_cxc.dropna(subset=["Fecha"])

    # --- Filter Current Year (and Month if selected) ---
    df_current = df_cxc[df_cxc["Fecha"].dt.year == year] if year else df_cxc.copy()
    if month:
        df_current = df_current[df_current["Fecha"].dt.month == month]

    # --- YTD since 2023-01-01 ---
    df_ytd = df_cxc[df_cxc["Fecha"] >= pd.Timestamp("2023-01-01")].copy()

    # --- Aggregate ---
    df_curr_summary = (
        df_current.groupby("Rut", as_index=False)
        .agg(SaldoTotal=("Saldo", "sum"), FacturasPendientes=("Saldo", "count"))
    )
    df_ytd_summary = (
        df_ytd.groupby("Rut", as_index=False)
        .agg(SaldoTotal=("Saldo", "sum"), FacturasPendientes=("Saldo", "count"))
    )
    return df_curr_summary, df_ytd_summary


def filter_by_period(df, year, month=None):
    """Filter sales by selected year or YTD."""
    if month:
        mask = (df["Fecha"].dt.year == year) & (df["Fecha"].dt.month == month)
    else:
        mask = df["Fecha"].dt.year == year
    return df.loc[mask].copy()


def get_kpis(df_current, df_previous, df_pending_current=None, df_pending_ytd=None):
    """Compute KPIs for period."""
    total_sales = df_current["Total"].sum()
    facturas = df_current["Folio"].nunique() if "Folio" in df_current.columns else 0
    total_sales_prev = df_previous["Total"].sum()
    facturas_prev = df_previous["Folio"].nunique() if "Folio" in df_previous.columns else 0

    # Pending from cuentas_por_cobrar
    pending = pending_invoices = 0
    pending_ytd = pending_invoices_ytd = 0

    if df_pending_current is not None and not df_pending_current.empty:
        pending = df_pending_current["SaldoTotal"].sum()
        pending_invoices = df_pending_current["FacturasPendientes"].sum()
    if df_pending_ytd is not None and not df_pending_ytd.empty:
        pending_ytd = df_pending_ytd["SaldoTotal"].sum()
        pending_invoices_ytd = df_pending_ytd["FacturasPendientes"].sum()

    def pct_change(current, prev):
        if prev == 0:
            return 0
        return ((current - prev) / prev) * 100

    avg_days = None
    if "FechaPago" in df_current.columns:
        mask_paid = df_current["FechaPago"].notna()
        if mask_paid.any():
            avg_days = (df_current.loc[mask_paid, "FechaPago"] - df_current.loc[mask_paid, "Fecha"]).dt.days.mean()

    return {
        "facturas": (facturas, pct_change(facturas, facturas_prev)),
        "total_sales": (total_sales, pct_change(total_sales, total_sales_prev)),
        "pending": pending,
        "pending_invoices": pending_invoices,
        "pending_ytd": pending_ytd,
        "pending_invoices_ytd": pending_invoices_ytd,
        "avg_days": avg_days,
    }


# === Main View ===
def show_clients_view():
    """Main Streamlit view for client-level analysis."""
    st.title("👥 Client Overview")

    # === Load data ===
    df_sales = get_client_data()
    current_year = date.today().year
    df_pending, df_pending_ytd = get_cta_por_cobrar(year=current_year)

    if df_sales.empty:
        st.warning("No client sales data available.")
        return

    # --- Default year/month before showing KPIs ---
    year = st.session_state.get("selected_year", current_year)
    month = st.session_state.get("selected_month", "YTD")
    selected_month = None if month == "YTD" else month

    # === Compute KPIs first ===
    df_current = filter_by_period(df_sales, year, selected_month)
    df_ytd_sales = filter_by_period(df_sales, year)
    df_previous = filter_by_period(df_sales, year - 1, selected_month)

    kpis_selected = get_kpis(df_current, df_previous, df_pending, df_pending_ytd)
    kpis_ytd = get_kpis(df_ytd_sales, df_previous, df_pending, df_pending_ytd)

    # === KPI SECTION (top of page) ===
    st.subheader(f"📊 Key Performance Indicators — {year} {'(YTD)' if not selected_month else f'Month {month}'} vs Total since 2023")

    col1, col2, col3, col4, col5 = st.columns(5)

    def kpi_dual(col, title, current_val, ytd_val, suffix=""):
        col.markdown(
            f"""
            <div style="font-size:18px; font-weight:600;">{title}</div>
            <div style="color:#007bff; font-size:22px;">{current_val}{suffix}</div>
            <div style="color:gray; font-size:14px;">{ytd_val}{suffix} Total since 2023</div>
            """,
            unsafe_allow_html=True,
        )

    kpi_dual(col1, "📄 Facturas Emitidas",
             f"{kpis_selected['facturas'][0]:,}",
             f"{kpis_ytd['facturas'][0]:,}")
    kpi_dual(col2, "💰 Venta",
             f"${kpis_selected['total_sales'][0]:,.0f}",
             f"${kpis_ytd['total_sales'][0]:,.0f}")
    kpi_dual(col3, "⏳ Monto Pendiente",
             f"${kpis_selected['pending']:,.0f}",
             f"${kpis_selected['pending_ytd']:,.0f}")
    kpi_dual(col4, "🧾 Fact. Pendientes",
             f"{int(kpis_selected['pending_invoices']):,}",
             f"{int(kpis_selected['pending_invoices_ytd']):,}")
    avg_sel = f"{kpis_selected['avg_days']:.1f}" if kpis_selected["avg_days"] else "N/A"
    avg_ytd = f"{kpis_ytd['avg_days']:.1f}" if kpis_ytd["avg_days"] else "N/A"
    kpi_dual(col5, "🕒 Prom. Días Pago", avg_sel, avg_ytd, " días")

    st.divider()

    # === Year / Month and Client Filters (below KPIs) ===
    st.subheader("📅 Period & Client Filters")

    c1, c2, c3 = st.columns([2, 1, 1])
    all_clients = sorted(df_sales["RznSocial"].dropna().unique())

    search_client = c1.text_input("🔍 Search Client")
    filtered_clients = [c for c in all_clients if search_client.lower() in c.lower()]
    selected_client = c1.selectbox("Select Client", filtered_clients, index=0 if filtered_clients else None)

    new_year = c2.number_input("Year", min_value=2020, max_value=current_year, value=year)
    new_month = c3.selectbox("Month", ["YTD"] + list(range(1, 13)), index=0 if month == "YTD" else month)

    # Save selections so KPIs update dynamically
    st.session_state["selected_year"] = new_year
    st.session_state["selected_month"] = new_month

    # === Refresh Data with updated filters ===
    df_current = filter_by_period(df_sales, new_year, None if new_month == "YTD" else new_month)
    df_ytd_sales = filter_by_period(df_sales, new_year)
    df_previous = filter_by_period(df_sales, new_year - 1, None if new_month == "YTD" else new_month)
    df_pending, df_pending_ytd = get_cta_por_cobrar(year=new_year, month=None if new_month == "YTD" else new_month)

    st.divider()

    # === Client Details ===
    if selected_client:
        df_client = df_sales[df_sales["RznSocial"] == selected_client].copy()
        df_client.sort_values("Fecha", ascending=False, inplace=True)

        # === Last 3 purchases (UPDATED: uses Descripcion for products, strips time) ===
        st.markdown(f"### 🧾 Últimas 3 Compras — {selected_client}")

        latest_folios = (
            df_client.sort_values("Fecha", ascending=False)["Folio"]
            .drop_duplicates()
            .head(3)
            .tolist()
        )

        df_recent = df_client[df_client["Folio"].isin(latest_folios)].copy()
        df_recent["Fecha"] = pd.to_datetime(df_recent["Fecha"], errors="coerce")

        # Use 'Descripcion' explicitly (fallbacks only if it's missing)
        product_col = "Descripcion" if "Descripcion" in df_recent.columns else None
        if product_col is None:
            for c in ["DescripcionDetallada", "NombreProducto"]:
                if c in df_recent.columns:
                    product_col = c
                    break

        if product_col:
            df_recent_products = (
                df_recent.sort_values(["Fecha", "Folio"])
                .groupby("Folio", as_index=False)
                .agg(
                    Fecha=("Fecha", lambda s: s.max().date()),
                    Total=("Total", "sum"),
                    Productos=(product_col, lambda s: ", ".join(dict.fromkeys([str(x) for x in s.dropna().tolist()])))
                )
            )
        else:
            df_recent_products = (
                df_recent.groupby("Folio", as_index=False)
                .agg(
                    Fecha=("Fecha", lambda s: s.max().date()),
                    Total=("Total", "sum"),
                )
            )
            df_recent_products["Productos"] = "(sin descripción)"

        df_recent_products = df_recent_products.sort_values("Fecha", descending=True) if "descending" in dir(pd.Series.sort_values) else df_recent_products.sort_values("Fecha", ascending=False)
        df_recent_products["Total"] = df_recent_products["Total"].apply(lambda x: f"${x:,.0f}")

        st.dataframe(
            df_recent_products[["Fecha", "Folio", "Productos", "Total"]],
            use_container_width=True,
        )

        # === Client KPIs ===
        df_client_sel = df_current[df_current["RznSocial"] == selected_client]
        df_client_ytd = df_ytd_sales[df_ytd_sales["RznSocial"] == selected_client]
        df_client_prev = df_previous[df_previous["RznSocial"] == selected_client]

        rut_cliente = (
            df_client_sel["Rut"].iloc[0]
            if "Rut" in df_client_sel.columns and not df_client_sel.empty
            else None
        )
        df_client_pending = (
            df_pending[df_pending["Rut"] == rut_cliente]
            if rut_cliente
            else pd.DataFrame(columns=["SaldoTotal", "FacturasPendientes"])
        )
        df_client_pending_ytd = (
            df_pending_ytd[df_pending_ytd["Rut"] == rut_cliente]
            if rut_cliente
            else pd.DataFrame(columns=["SaldoTotal", "FacturasPendientes"])
        )

        kpi_sel = get_kpis(df_client_sel, df_client_prev, df_client_pending, df_client_pending_ytd)
        kpi_ytd = get_kpis(df_client_ytd, df_client_prev, df_client_pending, df_client_pending_ytd)

        st.markdown(f"### 📈 Desempeño del Cliente — {new_year}")
        c1, c2, c3, c4, c5 = st.columns(5)
        kpi_dual(c1, "Facturas", f"{kpi_sel['facturas'][0]:,}", f"{kpi_ytd['facturas'][0]:,}")
        kpi_dual(c2, "Monto Comprado", f"${kpi_sel['total_sales'][0]:,.0f}", f"${kpi_ytd['total_sales'][0]:,.0f}")
        kpi_dual(c3, "Monto Pendiente", f"${kpi_sel['pending']:,.0f}", f"${kpi_sel['pending_ytd']:,.0f}")
        kpi_dual(c4, "Facturas Pendientes", f"{int(kpi_sel['pending_invoices']):,}", f"{int(kpi_sel['pending_invoices_ytd']):,}")
        avg_c = f"{kpi_sel['avg_days']:.1f}" if kpi_sel["avg_days"] else "N/A"
        avg_y = f"{kpi_ytd['avg_days']:.1f}" if kpi_ytd["avg_days"] else "N/A"
        kpi_dual(c5, "Prom. Días Pago", avg_c, avg_y, " días")

        st.divider()
        st.subheader("📉 Wheeler Monthly Trend — Selected vs YTD")
        show_clients_wheeler_analysis(df_client_ytd, df_client_sel)


# === Run in isolation ===
if __name__ == "__main__":
    show_clients_view()
# === END dashboard/tabs/clients_view.py ===
