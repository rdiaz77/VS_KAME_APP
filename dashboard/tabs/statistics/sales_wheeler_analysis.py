# === dashboard/tabs/statistics/sales_wheeler_analysis.py ===
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

# === Path ===
DB_PATH = Path(__file__).parent.parent.parent.parent / "data" / "vitroscience.db"


def get_monthly_sales():
    """Fetch monthly sales and gross revenue data from the database."""
    if not DB_PATH.exists():
        st.error(f"❌ Database not found at {DB_PATH}")
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        "SELECT Fecha, Total, MargenContrib FROM ventas_enriched_product;", conn
    )
    conn.close()

    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.dropna(subset=["Fecha"])

    df_monthly = (
        df.groupby(df["Fecha"].dt.to_period("M"))
        .agg({"Total": "sum", "MargenContrib": "sum"})
        .reset_index()
    )
    df_monthly["Fecha"] = df_monthly["Fecha"].dt.to_timestamp()
    return df_monthly


def wheeler_chart(df, value_col, title):
    """Plot Donald Wheeler process behavior chart (mean ± 2.66 × MR-bar)."""
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

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.plot(df["Fecha"], values, marker="o", label=value_col)
    ax.axhline(mean, color="blue", linestyle="--", label="Mean")
    ax.axhline(upl, color="red", linestyle="--", label="Upper Limit")
    ax.axhline(lpl, color="red", linestyle="--", label="Lower Limit")
    ax.fill_between(df["Fecha"], lpl, upl, color="red", alpha=0.1)
    ax.set_title(f"{title} — Wheeler Chart")
    ax.legend()
    st.pyplot(fig)


def show_sales_wheeler_analysis():
    """Display Wheeler-style process behavior charts for sales."""
    st.subheader("📊 Donald J. Wheeler — Sales Statistical Analysis")

    df_monthly = get_monthly_sales()
    if df_monthly.empty:
        st.warning("No sales data available.")
        return

    st.markdown("### 🧮 Process Behavior Charts")
    wheeler_chart(df_monthly, "Total", "Monthly Total Sales")
    wheeler_chart(df_monthly, "MargenContrib", "Monthly Gross Revenue")

    st.markdown("---")
    st.info(
        "These charts apply Donald J. Wheeler's Process Behavior methodology "
        "to visualize natural process variation and detect special causes."
    )


# End of file dashboard/tabs/statistics/sales_wheeler_analysis.py
