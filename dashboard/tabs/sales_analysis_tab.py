import sys
from pathlib import Path



# === dashboard/tabs/sales_analysis_tab.py ===
import sqlite3
import subprocess
from datetime import datetime


import pandas as pd
import streamlit as st

from dashboard.tabs.statistics.sales_wheeler_analysis import show_sales_wheeler_analysis
ROOT_DIR = Path(__file__).resolve().parent.parent.parent  # points to VS_KAME_APP
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))




# === Paths ===
DB_PATH = Path(__file__).parent.parent.parent / "data" / "vitroscience.db"
PIPELINE_SCRIPT = Path(__file__).parent.parent.parent / "get_ventas_main.py"


# === Fetch Total Sales and Period ===
def get_total_sales_and_period():
    """Fetch total sales value and date range from SQLite database."""
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
            FROM ventas_enriched_product;
        """
        result = pd.read_sql(query, conn)
        conn.close()

        if result.empty:
            return 0.0, None, None

        total = float(result["total_sales"].iloc[0] or 0)
        start_date = result["start_date"].iloc[0]
        end_date = result["end_date"].iloc[0]

        # 🔹 Normalize to YYYY-MM-DD (remove time, handle datetime or text)
        def clean_date(value):
            if pd.isna(value):
                return None
            if isinstance(value, (datetime, pd.Timestamp)):
                return value.strftime("%Y-%m-%d")
            if isinstance(value, str):
                return value.replace("T", " ").split(" ")[0]
            return str(value)

        start_date = clean_date(start_date)
        end_date = clean_date(end_date)

        return total, start_date, end_date

    except Exception as e:
        st.error(f"Database error: {e}")
        return None, None, None


# === Fetch Other Metrics ===
def get_additional_metrics():
    """Fetch complementary metrics from the SQLite database."""
    if not DB_PATH.exists():
        return None, None, None, None, None

    conn = sqlite3.connect(DB_PATH)
    metrics = {}

    try:
        # --- Total CxC ---
        query_cxc = "SELECT SUM(Saldo) AS total_cxc FROM cuentas_por_cobrar;"
        res_cxc = pd.read_sql(query_cxc, conn)
        metrics["total_cxc"] = float(res_cxc["total_cxc"].iloc[0] or 0)

        # --- No. Clientes ---
        query_clients = (
            "SELECT COUNT(DISTINCT Rut) AS total_clients FROM ventas_enriched_product;"
        )
        res_clients = pd.read_sql(query_clients, conn)
        metrics["no_clients"] = int(res_clients["total_clients"].iloc[0] or 0)

        # --- Gross Revenue (MargenContrib) ---
        query_gross = (
            "SELECT SUM(MargenContrib) AS gross_rev FROM ventas_enriched_product;"
        )
        res_gross = pd.read_sql(query_gross, conn)
        metrics["gross_rev"] = float(res_gross["gross_rev"].iloc[0] or 0)

        # --- Clientes Nuevos (simplified placeholder logic) ---
        metrics["new_clients"] = metrics["no_clients"]

        # --- C. de trabajo (placeholder) ---
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
    """Display the Sales Analysis tab with 3-column KPI grid and manual update."""
    st.header("📊 Sales Analysis")

    # --- MAIN KPI GRID ---
    col1, col2, col3 = st.columns(3)

    # === COLUMN 1 ===
    with col1:
        st.subheader("💰 Total Sales (KAME)")
        total_sales, start_date, end_date = get_total_sales_and_period()
        total_cxc, no_clients, gross_rev, working_capital, new_clients = (
            get_additional_metrics()
        )

        if total_sales is not None:
            if start_date and end_date:
                period_text = f"{start_date} → {end_date}"
            else:
                period_text = "N/A"
            st.metric(
                label=f"Total Sales\n({period_text})", value=f"${total_sales:,.0f}"
            )
        else:
            st.metric(label="Total Sales", value="—")

        if gross_rev is not None:
            st.metric(label="Gross Revenue", value=f"${gross_rev:,.0f}")
        else:
            st.metric(label="Gross Revenue", value="—")

        # Placeholder row
        st.metric(label="—", value="—")

    # === COLUMN 2 ===
    with col2:
        if total_cxc is not None:
            st.metric(label="Total CxC", value=f"${total_cxc:,.0f}")
        else:
            st.metric(label="Total CxC", value="—")

        if working_capital is not None:
            st.metric(label="C. de trabajo", value=f"${working_capital:,.0f}")
        else:
            st.metric(label="C. de trabajo", value="—")

        # Placeholder row
        st.metric(label="No. Deudores", value="—")

    # === COLUMN 3 ===
    with col3:
        if no_clients is not None:
            st.metric(label="No. Clientes", value=f"{no_clients:,}")
        else:
            st.metric(label="No. Clientes", value="—")

        if new_clients is not None:
            st.metric(label="Clientes Nuevos", value=f"{new_clients:,}")
        else:
            st.metric(label="Clientes Nuevos", value="—")

        # Inject CSS for small modern button
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
                color: white;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Update Button
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
                    st.experimental_rerun()
                except subprocess.CalledProcessError as e:
                    st.error("❌ Error running pipeline.")
                    st.text(e.stderr)

    st.markdown("---")
    st.info(
        "💡 This section shows key performance metrics from the VitroScience database "
        "and allows you to manually trigger the sales update pipeline."
    )


# === Donald J. Wheeler Statistical Section ===
    st.markdown("---")
    st.subheader("📊 Statistical Wheeler Analysis")

# Display Wheeler charts from the external module
    show_sales_wheeler_analysis()


# End of file dashboard/tabs/sales_analysis_tab.py
