# === dashboard/tabs/statistics/cta_por_cobrar_wheeler_analysis.py ===
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).parent.parent.parent.parent / "data" / "vitroscience.db"


def get_monthly_cta_por_cobrar():
    """Aggregate total pending balance per month for Wheeler chart."""
    if not DB_PATH.exists():
        st.error(f"❌ Database not found at {DB_PATH}")
        return pd.DataFrame()

    try:
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT FechaVencimiento, Saldo
            FROM cuentas_por_cobrar
            WHERE Saldo IS NOT NULL AND TRIM(Saldo) != '';
        """
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        st.error(f"❌ SQL read error: {e}")
        return pd.DataFrame()

    if df.empty:
        st.warning("⚠️ No data retrieved from cuentas_por_cobrar.")
        return pd.DataFrame()

    st.caption(f"📦 Loaded {len(df)} invoice records for Wheeler aggregation.")

    # Convert and clean types
    df["Saldo"] = (
        df["Saldo"].astype(str).str.replace(",", "").replace("", "0").astype(float)
    )
    df["FechaVencimiento"] = pd.to_datetime(df["FechaVencimiento"], errors="coerce")

    # Group by month
    df_monthly = (
        df.groupby(df["FechaVencimiento"].dt.to_period("M"))
        .agg({"Saldo": "sum"})
        .reset_index()
    )
    df_monthly["Fecha"] = df_monthly["FechaVencimiento"].dt.to_timestamp()
    df_monthly = df_monthly.rename(columns={"Saldo": "TotalPendiente"})

    st.write("✅ Monthly aggregation result (first 5 rows):")
    st.dataframe(df_monthly.head(), use_container_width=True)

    return df_monthly


def wheeler_chart(df, value_col, title):
    """Draw Wheeler Process Behavior Chart (and MR chart) for given series."""
    if df.empty or value_col not in df.columns:
        st.warning(f"No data for {title}")
        return

    values = df[value_col].astype(float).tolist()
    if len(values) < 3:
        st.info(f"Not enough data to plot {title}")
        return

    mean = sum(values) / len(values)
    moving_ranges = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    mr_bar = sum(moving_ranges) / len(moving_ranges)
    lpl = mean - 2.66 * mr_bar
    upl = mean + 2.66 * mr_bar

    # === Charts ===
    col1, col2 = st.columns(2)

    with col1:
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(df["Fecha"], values, marker="o")
        ax.axhline(mean, color="blue", linestyle="--", label="Mean")
        ax.axhline(upl, color="red", linestyle="--", label="Upper Limit")
        ax.axhline(lpl, color="red", linestyle="--", label="Lower Limit")
        ax.fill_between(df["Fecha"], lpl, upl, color="red", alpha=0.1)
        ax.set_title(title)
        ax.legend()
        plt.xticks(rotation=45)
        st.pyplot(fig)

    with col2:
        fig2, ax2 = plt.subplots(figsize=(6, 4))
        ax2.plot(df["Fecha"].iloc[1:], moving_ranges, marker="o", color="purple")
        ax2.axhline(mr_bar, color="blue", linestyle="--", label="MR Mean")
        ax2.axhline(3.268 * mr_bar, color="red", linestyle="--", label="MR UCL")
        ax2.legend()
        ax2.set_title("Moving Range")
        plt.xticks(rotation=45)
        st.pyplot(fig2)


def show_cta_por_cobrar_wheeler_analysis():
    """Display Wheeler charts for CxC pending balance over time."""
    st.subheader("📊 Wheeler Process Behavior — Pending Balance Over Time")

    df_monthly = get_monthly_cta_por_cobrar()
    if df_monthly.empty:
        st.warning("⚠️ No valid monthly data to display Wheeler charts.")
        return

    wheeler_chart(df_monthly, "TotalPendiente", "Monthly Pending Balance Behavior")

    st.info(
        "This chart applies Donald J. Wheeler's methodology to assess "
        "stability and variation in the total pending balance month to month."
    )


# === END dashboard/tabs/statistics/cta_por_cobrar_wheeler_analysis.py ===
