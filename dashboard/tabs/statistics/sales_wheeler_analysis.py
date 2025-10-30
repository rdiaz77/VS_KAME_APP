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
    """Plot Wheeler process behavior chart and moving range chart side-by-side."""
    if df.empty or value_col not in df.columns:
        st.warning(f"No data for {title}")
        return

    values = df[value_col].astype(float).tolist()
    if len(values) < 3:
        st.info(f"Not enough data to plot {title}")
        return

    # === Main Chart Calculations ===
    mean = sum(values) / len(values)
    moving_ranges = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    mr_bar = sum(moving_ranges) / len(moving_ranges)
    lpl = mean - 2.66 * mr_bar
    upl = mean + 2.66 * mr_bar

    # === Range Chart Calculations ===
    range_dates = df["Fecha"].iloc[1:]  # one fewer point
    mr_mean = mr_bar
    mr_ucl = 3.268 * mr_mean  # Wheeler constant for 2-point MR chart

    # === Create Streamlit columns ===
    col1, col2 = st.columns(2, gap="large")

    # === Main Wheeler Chart ===
    with col1:
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(df["Fecha"], values, marker="o", label=value_col)
        ax.axhline(mean, color="blue", linestyle="--", label="Mean")
        ax.axhline(upl, color="red", linestyle="--", label="Upper Limit")
        ax.axhline(lpl, color="red", linestyle="--", label="Lower Limit")
        ax.fill_between(df["Fecha"], lpl, upl, color="red", alpha=0.1)
        ax.set_title(f"{title}\n(Process Behavior Chart)")
        ax.legend()
        plt.xticks(rotation=45, ha="right")  # 🔹 Rotate date labels
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
        plt.xticks(rotation=45, ha="right")  # 🔹 Rotate date labels
        plt.tight_layout()
        st.pyplot(fig2)


def show_sales_wheeler_analysis():
    """Display Wheeler-style process behavior charts for sales."""
    st.subheader("📊 Wheeler Analysis (🧮 Process Behavior & Range Charts)")

    df_monthly = get_monthly_sales()
    if df_monthly.empty:
        st.warning("No sales data available.")
        return

    #st.markdown("### 🧮 Process Behavior & Range Charts")
    wheeler_chart(df_monthly, "Total", "Monthly Total Sales")
    st.markdown("---")
    wheeler_chart(df_monthly, "MargenContrib", "Monthly Gross Revenue")

    st.markdown("---")
    st.info(
        "These charts apply Donald J. Wheeler's Process Behavior methodology "
        "to visualize both process stability (*main chart*) and short-term variation (*range chart*)."
    )


# End of file dashboard/tabs/statistics/sales_wheeler_analysis.py
