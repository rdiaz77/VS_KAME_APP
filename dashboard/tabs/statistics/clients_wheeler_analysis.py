# === dashboard/tabs/statistics/clients_wheeler_analysis.py ===
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st


def show_clients_wheeler_analysis(df_ytd, df_selected):
    """Display Wheeler-style monthly comparison (YTD vs selected period)."""
    if df_ytd.empty:
        st.info("No data available for this client.")
        return

    # Group monthly totals
    def group(df):
        df["Month"] = df["Fecha"].dt.to_period("M")
        return df.groupby("Month").agg({"Total": "sum", "Folio": "nunique"}).reset_index()

    df_ytd_m = group(df_ytd)
    df_selected_m = group(df_selected) if not df_selected.empty else pd.DataFrame(columns=["Month", "Total", "Folio"])

    # === Plot ===
    fig, ax1 = plt.subplots(figsize=(8, 4))
    ax2 = ax1.twinx()

    # Bars for YTD and selected
    ax1.bar(df_ytd_m["Month"].astype(str), df_ytd_m["Total"], alpha=0.3, label="YTD")
    if not df_selected_m.empty:
        ax1.bar(df_selected_m["Month"].astype(str), df_selected_m["Total"], alpha=0.7, label="Selected Period")

    # Invoices line (YTD)
    ax2.plot(df_ytd_m["Month"].astype(str), df_ytd_m["Folio"], color="gray", marker="o", label="Facturas (YTD)")
    if not df_selected_m.empty:
        ax2.plot(df_selected_m["Month"].astype(str), df_selected_m["Folio"], color="tab:blue", marker="o", label="Facturas (Selected)")

    ax1.set_title("📊 Monthly Sales & Invoices — Selected vs YTD", fontsize=11)
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Sales Amount")
    ax2.set_ylabel("Number of Invoices")
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    st.pyplot(fig)
# === END dashboard/tabs/statistics/clients_wheeler_analysis.py ===
