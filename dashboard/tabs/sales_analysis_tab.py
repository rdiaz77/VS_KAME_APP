import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

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


# === Show Sales Analysis Tab ===
def show_sales_analysis():
    """Display the Sales Analysis tab with total, period, and manual update."""
    st.header("📊 Sales Analysis")

    col1, col2 = st.columns([2, 1])

    # --- LEFT COLUMN: Total Sales + Period ---
    with col1:
        st.subheader("💰 Total Sales (from SQLite)")

        total_sales, start_date, end_date = get_total_sales_and_period()
        if total_sales is not None:
            if start_date and end_date:
                period_text = f"Period: {start_date} → {end_date}"
            else:
                period_text = "Period: not available"

            st.metric(
                label=f"Total Sales\n({period_text})",
                value=f"${total_sales:,.0f}"
            )

    # --- RIGHT COLUMN: Small "Run Sales Pipeline" Button ---
    with col2:
        # Inject CSS for smaller, modern button style
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

        # --- Small button only ---
        run_update = st.button("Run Sales Pipeline", key="update_btn")

        # --- Button logic ---
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
        "💡 This section shows the total sales for the available period "
        "and allows you to manually trigger the sales update pipeline."
    )
# === Fetch KAME Sales Data from API ===