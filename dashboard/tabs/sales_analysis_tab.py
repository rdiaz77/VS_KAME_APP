# === dashboard/tabs/sales_analysis_tab.py ===
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.tabs.statistics.sales_wheeler_analysis import show_sales_wheeler_analysis

# === PATH SETUP ===
ROOT_DIR = Path(__file__).resolve().parent.parent.parent  # points to VS_KAME_APP
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# === Paths ===
DB_PATH = ROOT_DIR / "data" / "vitroscience.db"
PIPELINE_SCRIPT = ROOT_DIR / "get_ventas_main.py"


# === Fetch Total Sales and Period (with optional date filter) ===
def get_total_sales_and_period(start_date=None, end_date=None):
    """Fetch total sales value and date range from SQLite database, filtered by date."""
    if not DB_PATH.exists():
        st.error(f"❌ Database not found at {DB_PATH}")
        return None, None, None

    try:
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT 
                SUM(Total) AS total_sales,
                MIN(Fecha) AS start_date,
                MAX(Fecha) AS end_date
            FROM ventas_enriched_product
        """
        params = ()
        if start_date and end_date:
            query += " WHERE Fecha BETWEEN ? AND ?"
            params = (start_date, end_date)

        result = pd.read_sql(query, conn, params=params)
        conn.close()

        if result.empty:
            return 0.0, None, None

        total = float(result["total_sales"].iloc[0] or 0)
        start_db = result["start_date"].iloc[0]
        end_db = result["end_date"].iloc[0]

        def clean_date(value):
            if pd.isna(value):
                return None
            if isinstance(value, (datetime, pd.Timestamp)):
                return value.strftime("%Y-%m-%d")
            if isinstance(value, str):
                return value.replace("T", " ").split(" ")[0]
            return str(value)

        return total, clean_date(start_db), clean_date(end_db)

    except Exception as e:
        st.error(f"Database error: {e}")
        return None, None, None


# === Fetch Other Metrics (with same filter) ===
def get_additional_metrics(start_date=None, end_date=None):
    """Fetch complementary metrics from the SQLite database (filtered by date)."""
    if not DB_PATH.exists():
        return None, None, None, None, None

    conn = sqlite3.connect(DB_PATH)
    metrics = {}

    try:
        date_filter = ""
        params = ()
        if start_date and end_date:
            date_filter = "WHERE Fecha BETWEEN ? AND ?"
            params = (start_date, end_date)

        # --- Total CxC ---
        query_cxc = "SELECT SUM(Saldo) AS total_cxc FROM cuentas_por_cobrar;"
        res_cxc = pd.read_sql(query_cxc, conn)
        metrics["total_cxc"] = float(res_cxc["total_cxc"].iloc[0] or 0)

        # --- No. Clientes ---
        query_clients = f"""
            SELECT COUNT(DISTINCT Rut) AS total_clients 
            FROM ventas_enriched_product {date_filter};
        """
        res_clients = pd.read_sql(query_clients, conn, params=params)
        metrics["no_clients"] = int(res_clients["total_clients"].iloc[0] or 0)

        # --- Gross Revenue ---
        query_gross = f"""
            SELECT SUM(MargenContrib) AS gross_rev 
            FROM ventas_enriched_product {date_filter};
        """
        res_gross = pd.read_sql(query_gross, conn, params=params)
        metrics["gross_rev"] = float(res_gross["gross_rev"].iloc[0] or 0)

        # --- Placeholder fields ---
        metrics["new_clients"] = metrics["no_clients"]
        metrics["working_capital"] = None

    except Exception as e:
        st.error(f"Error fetching metrics: {e}")
        return None, None, None, None, None
    finally:
        conn.close()

    return (
        metrics["total_cxc"],
        metrics["no_clients"],
        metrics["gross_rev"],
        metrics["working_capital"],
        metrics["new_clients"],
    )


# === Show Sales Analysis Tab ===
def show_sales_analysis():
    """Display the Sales Analysis tab with current-year default and custom date selector."""
    st.header("📊 Sales Analysis")

    # --- Default period: current year ---
    today = datetime.today()
    default_start = datetime(today.year, 1, 1)
    default_end = today

    # Persist in session state
    if "sales_start" not in st.session_state:
        st.session_state.sales_start = default_start
    if "sales_end" not in st.session_state:
        st.session_state.sales_end = default_end

    # --- Date selector ---
    with st.expander("📅 Select Date Range", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            start_date = st.date_input(
                "Start Date", value=st.session_state.sales_start
            )
        with c2:
            end_date = st.date_input(
                "End Date", value=st.session_state.sales_end
            )

        # Update globally
        st.session_state.sales_start = start_date
        st.session_state.sales_end = end_date

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # --- MAIN KPI GRID ---
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("💰 Total Sales (KAME)")
        total_sales, start_db, end_db = get_total_sales_and_period(start_str, end_str)
        total_cxc, no_clients, gross_rev, working_capital, new_clients = (
            get_additional_metrics(start_str, end_str)
        )

        if total_sales is not None:
            st.metric(
                label=f"Total Sales\n({start_str} → {end_str})",
                value=f"${total_sales:,.0f}",
            )
        else:
            st.metric(label="Total Sales", value="—")

        st.metric(label="Gross Revenue", value=f"${gross_rev:,.0f}" if gross_rev else "—")
        st.metric(label="—", value="—")

    with col2:
        st.metric(label="Total CxC", value=f"${total_cxc:,.0f}" if total_cxc else "—")
        st.metric(label="C. de trabajo", value=f"${working_capital:,.0f}" if working_capital else "—")
        st.metric(label="No. Deudores", value="—")

    with col3:
        st.metric(label="No. Clientes", value=f"{no_clients:,}" if no_clients else "—")
        st.metric(label="Clientes Nuevos", value=f"{new_clients:,}" if new_clients else "—")

        # CSS + Button
        st.markdown(
            """
            <style>
            div.stButton > button.small-button {
                padding: 0.3rem 0.8rem;
                font-size: 0.8rem;
                border-radius: 6px;
                background-color: #0d6efd;
                color: white;
                border: none;
                transition: background-color 0.2s ease;
            }
            div.stButton > button.small-button:hover {
                background-color: #0b5ed7;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        run_update = st.button("Run Sales Pipeline", key="update_btn")
        if run_update:
            if not PIPELINE_SCRIPT.exists():
                st.error(f"❌ Pipeline script not found at {PIPELINE_SCRIPT}")
                return

            with st.spinner("Running sales update pipeline..."):
                try:
                    result = subprocess.run(
                        ["python", str(PIPELINE_SCRIPT)],
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    st.success("✅ Pipeline executed successfully.")
                    st.text(result.stdout)
                    st.rerun()
                except subprocess.CalledProcessError as e:
                    st.error("❌ Error running pipeline.")
                    st.text(e.stderr)

    st.markdown("---")
    st.info(
        "💡 By default, this view shows data from the current year. You can select any date range to update all metrics and charts dynamically."
    )

    st.markdown("---")
    st.subheader("📊 Statistical Wheeler Analysis")

    # Display Wheeler charts (same global period)
    show_sales_wheeler_analysis()
# === End of dashboard/tabs/sales_analysis_tab.py ===
