# === dashboard/cta_por_cobrar_view.py ===
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st



# === Paths ===
DB_PATH = Path(__file__).parent.parent / "data" / "vitroscience.db"


def show_cta_cobrar():
    """Main dashboard page for 'Cuentas por Cobrar' with KPIs, table, and ranking."""
    st.title("💰 Cuentas por Cobrar")

    if not DB_PATH.exists():
        st.error(
            "❌ Base de datos no encontrada. Ejecuta la actualización de datos primero."
        )
        return

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM cuentas_por_cobrar;", conn)

    if df.empty:
        st.warning("⚠️ No se encontraron registros de cuentas por cobrar.")
        conn.close()
        return

    # === Convert types ===
    for col in ["Saldo", "Total", "TotalCP"]:
        df[col] = (
            df[col].astype(str).str.replace(",", "").replace("", "0").astype(float)
        )
    for col in ["Fecha", "FechaVencimiento"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # === Filtros ===
    st.sidebar.header("🔍 Filtros")
    vendedores = ["Todos"] + sorted(df["NombreVendedor"].dropna().unique().tolist())
    selected_vendedor = st.sidebar.selectbox("👨‍💼 Vendedor", vendedores, index=0)
    cliente_filter = st.sidebar.text_input("🏢 Buscar Cliente (nombre parcial o RUT)")

    min_date, max_date = df["FechaVencimiento"].min(), df["FechaVencimiento"].max()
    date_range = st.sidebar.date_input(
        "📅 Rango de Fecha de Vencimiento",
        [min_date.date(), max_date.date()] if not pd.isna(min_date) else [],
    )

    mask = pd.Series(True, index=df.index)
    if selected_vendedor != "Todos":
        mask &= df["NombreVendedor"] == selected_vendedor
    if cliente_filter:
        mask &= df["RznSocial"].str.contains(cliente_filter, case=False, na=False) | df[
            "Rut"
        ].astype(str).str.contains(cliente_filter, case=False, na=False)
    if isinstance(date_range, list) and len(date_range) == 2:
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        mask &= df["FechaVencimiento"].between(start, end)

    df_filtered = df.loc[mask].copy()

    # === KPIs ===
    st.divider()
    st.subheader("📈 Indicadores Clave (KPI)")

    total_facturas = len(df_filtered)
    total_saldo = df_filtered["Saldo"].sum()
    vencidas = (df_filtered["FechaVencimiento"] < datetime.now()).sum()
    dias_prom = (datetime.now() - df_filtered["FechaVencimiento"]).dt.days.mean()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🧾 Facturas", f"{total_facturas:,}")
    col2.metric("💵 Total Pendiente", f"${total_saldo:,.0f}")
    col3.metric("⚠️ Vencidas", f"{vencidas:,}")
    col4.metric(
        "📅 Promedio Días Venc.",
        f"{dias_prom:.1f}" if not pd.isna(dias_prom) else "N/A",
    )

    # === Tabla principal ===
    st.divider()
    st.subheader("📋 Detalle de Facturas Pendientes")

    today = datetime.now().date()
    df_filtered["Días Restantes"] = (
        df_filtered["FechaVencimiento"].dt.date - today
    ).apply(lambda x: x.days)
    df_filtered["Estado"] = df_filtered["Días Restantes"].apply(
        lambda d: "🟥" if d < 0 else "🟡" if d <= 7 else "🟢"
    )

    df_display = df_filtered[
        [
            "Estado",
            "Rut",
            "RznSocial",
            "FolioDocumento",
            "Fecha",
            "FechaVencimiento",
            "CondicionVenta",
            "Saldo",
            "NombreVendedor",
            "Días Restantes",
        ]
    ].copy()

    df_display["Saldo"] = df_display["Saldo"].map("{:,.0f}".format)
    df_display["Fecha"] = df_display["Fecha"].dt.strftime("%Y-%m-%d")
    df_display["FechaVencimiento"] = df_display["FechaVencimiento"].dt.strftime(
        "%Y-%m-%d"
    )

    st.dataframe(df_display, use_container_width=True, hide_index=True)

    # === Ranking Histórico ===
    st.divider()
    st.subheader("🏆 Ranking de Clientes por Tiempo Promedio de Pago")

    try:
        df_hist = pd.read_sql(
            "SELECT * FROM cuentas_por_cobrar_history WHERE status='paid';", conn
        )
        if not df_hist.empty:
            df_hist["Fecha"] = pd.to_datetime(df_hist["Fecha"], errors="coerce")
            df_hist["last_updated"] = pd.to_datetime(
                df_hist["last_updated"], errors="coerce"
            )
            df_hist["dias_pago"] = (df_hist["last_updated"] - df_hist["Fecha"]).dt.days

            ranking = (
                df_hist.groupby("Rut")
                .agg(avg_dias=("dias_pago", "mean"), count=("Rut", "count"))
                .reset_index()
                .sort_values("avg_dias")
            )

            ranking["avg_dias"] = ranking["avg_dias"].round(1)
            ranking.insert(0, "Rank", range(1, len(ranking) + 1))

            st.dataframe(
                ranking.rename(
                    columns={
                        "Rut": "RUT Cliente",
                        "avg_dias": "Promedio Días de Pago",
                        "count": "Facturas Pagadas",
                    }
                ),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("ℹ️ No hay facturas pagadas aún para calcular el ranking.")
    except Exception as e:
        st.warning(f"⚠️ Error generando ranking: {e}")

    # === Wheeler Analysis Section ===
    st.divider()
    st.subheader("📊 Análisis de Estabilidad — Wheeler Charts")

    try:
        from dashboard.tabs.statistics.cta_por_cobrar_wheeler_analysis import show_cta_por_cobrar_wheeler_analysis as show_cta_por_cobrar_analysis


        st.info("🔍 Cargando análisis Wheeler de Cuentas por Cobrar...")
        show_cta_por_cobrar_analysis()  # display full Wheeler charts from the analysis tab
    except Exception as e:
        st.error(f"❌ Error cargando análisis Wheeler: {e}")

    finally:
        conn.close()

    st.markdown("---")
    st.caption(
        "📊 Datos desde cuentas_por_cobrar y cuentas_por_cobrar_history en vitroscience.db."
    )


# === END dashboard/cta_por_cobrar_view.py ===
