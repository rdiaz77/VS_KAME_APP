# === dashboard/tabs/statistics/clients_wheeler_analysis.py ===
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st


# === Helper: generic Wheeler chart ===
def wheeler_chart(df, value_col, title):
    """Plot Wheeler process behavior chart and moving range chart side-by-side."""
    if df.empty or value_col not in df.columns:
        st.warning(f"No data for {title}")
        return

    values = df[value_col].astype(float).tolist()
    if len(values) < 3:
        st.info(f"Not enough data to plot {title}")
        return

    mean = np.mean(values)
    moving_ranges = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    mr_bar = np.mean(moving_ranges)
    lpl = mean - 2.66 * mr_bar
    upl = mean + 2.66 * mr_bar

    range_dates = df["Fecha"].iloc[1:]
    mr_mean = mr_bar
    mr_ucl = 3.268 * mr_mean  # Wheeler constant

    col1, col2 = st.columns(2, gap="large")

    # === Process Behavior Chart ===
    with col1:
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(df["Fecha"], values, marker="o", label=value_col, color="tab:blue")
        ax.axhline(mean, color="blue", linestyle="--", label="Mean")
        ax.axhline(upl, color="red", linestyle="--", label="Upper Limit")
        ax.axhline(lpl, color="red", linestyle="--", label="Lower Limit")
        ax.fill_between(df["Fecha"], lpl, upl, color="red", alpha=0.1)
        ax.set_title(f"{title}\n(Process Behavior Chart)")
        ax.legend()
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        st.pyplot(fig)

    # === Moving Range Chart ===
    with col2:
        fig2, ax2 = plt.subplots(figsize=(6, 4))
        ax2.plot(
            range_dates, moving_ranges, marker="o", color="purple", label="Moving Range"
        )
        ax2.axhline(mr_mean, color="blue", linestyle="--", label="MR Mean")
        ax2.axhline(mr_ucl, color="red", linestyle="--", label="MR Upper Limit")
        ax2.fill_between(range_dates, mr_mean, mr_ucl, color="red", alpha=0.1)
        ax2.set_title(f"{title}\n(Moving Range Chart)")
        ax2.legend()
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        st.pyplot(fig2)


# === 💰 Sales (Ventas) Wheeler Chart ===
def show_sales_wheeler_analysis(df_ytd, df_selected):
    """
    Wheeler analysis for Ventas:
    - Combines YTD + selected period
    - Shows Total sales per month with Wheeler limits
    """
    if df_ytd.empty:
        st.warning("No YTD sales data available.")
        return

    # Combine YTD + selected period if provided
    df = df_ytd.copy()
    if not df_selected.empty:
        df = pd.concat([df_ytd, df_selected])

    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.dropna(subset=["Fecha"])
    df = df.groupby(df["Fecha"].dt.to_period("M")).agg({"Total": "sum"}).reset_index()
    df["Fecha"] = df["Fecha"].dt.to_timestamp()

    st.subheader("💰 Ventas — Wheeler Analysis")
    wheeler_chart(df, "Total", "Monthly Total Sales")


# === 💳 Cuentas por Cobrar Wheeler Chart ===
def show_cta_por_cobrar_wheeler_analysis(df_ytd, df_selected):
    """
    Wheeler analysis for Cuentas por Cobrar:
    - Combines YTD + selected period
    - Shows Saldo per month with Wheeler limits
    """
    if df_ytd.empty:
        st.warning("No YTD Cuentas por Cobrar data available.")
        return

    df = df_ytd.copy()
    if not df_selected.empty:
        df = pd.concat([df_ytd, df_selected])

    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.dropna(subset=["Fecha"])
    df = df.groupby(df["Fecha"].dt.to_period("M")).agg({"Saldo": "sum"}).reset_index()
    df["Fecha"] = df["Fecha"].dt.to_timestamp()

    st.subheader("💳 Cuentas por Cobrar — Wheeler Analysis")
    wheeler_chart(df, "Saldo", "Monthly Outstanding Balance")


# === END dashboard/tabs/statistics/clients_wheeler_analysis.py ===
