# === dashboard/tabs/statistics/cta_por_cobrar_wheeler_analysis.py ===
import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

# === Path ===
DB_PATH = Path(__file__).parent.parent.parent.parent / "data" / "vitroscience.db"


def get_monthly_cta_por_cobrar():
    """
    Aggregate monthly:
    - Total pending balance (sum of Saldo)
    - Count of outstanding invoices
    using cuentas_por_cobrar_history snapshots.
    """
    if not DB_PATH.exists():
        st.error(f"❌ Database not found at {DB_PATH}")
        return pd.DataFrame()

    try:
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT snapshot_date, Saldo
            FROM cuentas_por_cobrar_history
            WHERE Saldo IS NOT NULL AND TRIM(Saldo) != '';
        """
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        st.error(f"❌ SQL read error: {e}")
        return pd.DataFrame()

    if df.empty:
        st.warning("⚠️ No data retrieved from cuentas_por_cobrar_history.")
        return pd.DataFrame()

    # Clean and convert
    df["Saldo"] = (
        df["Saldo"].astype(str).str.replace(",", "").replace("", "0").astype(float)
    )
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")

    # Aggregate by month
    df_monthly = (
        df.groupby(df["snapshot_date"].dt.to_period("M"))
        .agg(
            TotalPendiente=("Saldo", "sum"),
            FacturasPendientes=("Saldo", "count"),
        )
        .reset_index()
    )
    df_monthly["Fecha"] = df_monthly["snapshot_date"].dt.to_timestamp()

    st.caption(f"📦 Loaded {len(df):,} invoice records from history for Wheeler analysis.")
    st.dataframe(df_monthly.head(), use_container_width=True)
    return df_monthly


def wheeler_chart(df, value_col, title):
    """Draw Wheeler Process Behavior Chart (and Moving Range Chart)."""
    if df.empty or value_col not in df.columns:
        st.warning(f"No data for {title}")
        return

    values = df[value_col].astype(float).tolist()
    if len(values) < 3:
        st.info(f"Not enough data to plot {title}")
        return

    # === Wheeler Calculations ===
    mean = sum(values) / len(values)
    moving_ranges = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    mr_bar = sum(moving_ranges) / len(moving_ranges)
    lpl = mean - 2.66 * mr_bar
    upl = mean + 2.66 * mr_bar

    # === Layout ===
    col1, col2 = st.columns(2, gap="large")

    # === Process Behavior Chart ===
    with col1:
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(df["Fecha"], values, marker="o", color="blue")
        ax.axhline(mean, color="blue", linestyle="--", label="Mean")
        ax.axhline(upl, color="red", linestyle="--", label="Upper Limit")
        ax.axhline(lpl, color="red", linestyle="--", label="Lower Limit")
        ax.fill_between(df["Fecha"], lpl, upl, color="red", alpha=0.1)
        ax.set_title(title)
        ax.legend()
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        st.pyplot(fig)

    # === Moving Range Chart ===
    with col2:
        fig2, ax2 = plt.subplots(figsize=(6, 4))
        ax2.plot(df["Fecha"].iloc[1:], moving_ranges, marker="o", color="purple")
        ax2.axhline(mr_bar, color="blue", linestyle="--", label="MR Mean")
        ax2.axhline(3.268 * mr_bar, color="red", linestyle="--", label="MR UCL")
        ax2.fill_between(df["Fecha"].iloc[1:], mr_bar, 3.268 * mr_bar, color="red", alpha=0.1)
        ax2.set_title(f"{title} — Moving Range")
        ax2.legend()
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        st.pyplot(fig2)


def show_cta_por_cobrar_wheeler_analysis():
    """Display Wheeler charts for both total balance and invoice count."""
    st.subheader("📊 Wheeler Process Behavior — Cuentas por Cobrar")

    df_monthly = get_monthly_cta_por_cobrar()
    if df_monthly.empty:
        st.warning("⚠️ No valid monthly data to display Wheeler charts.")
        return

    # === Charts ===
    st.markdown("### 💵 Total Pending Balance (Monthly)")
    wheeler_chart(df_monthly, "TotalPendiente", "Monthly Total Pending Balance")

    st.markdown("---")

    st.markdown("### 🧾 Number of Outstanding Invoices (Monthly)")
    wheeler_chart(df_monthly, "FacturasPendientes", "Monthly Count of Outstanding Invoices")

    st.info(
        "These Wheeler charts track month-to-month stability of the total pending amount "
        "and the number of open invoices. Sharp or sustained shifts may indicate "
        "changes in client payment behavior or collection efficiency."
    )


# === END dashboard/tabs/statistics/cta_por_cobrar_wheeler_analysis.py ===
