# === dashboard/tabs/sales_analysis_tab.py ===
import sqlite3
import subprocess
import sys
from calendar import monthrange
from datetime import date
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

# === Constants ===
MIN_ALLOWED_DATE = pd.Timestamp("2023-01-01")

# Month helpers
MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
MONTH_NAME_TO_NUM = {name: i + 1 for i, name in enumerate(MONTHS)}


# === Helpers ===
def _safe_same_day_last_year(ts: pd.Timestamp):
    try:
        return ts - pd.DateOffset(years=1)
    except Exception:
        if ts.month == 2 and ts.day == 29:
            return pd.Timestamp(ts.year - 1, 2, 28)
        return ts - pd.Timedelta(days=365)


def _period_for_selection(year: int, month_opt: str | None):
    today = pd.Timestamp(date.today())
    is_current_year = year == today.year
    if month_opt == "All":
        start = pd.Timestamp(year, 1, 1)
        if is_current_year:
            end = today
            label = f"YTD {year}"
        else:
            end = pd.Timestamp(year, 12, 31)
            label = f"Full Year {year}"
        return start, end, label
    month_idx = MONTH_NAME_TO_NUM[month_opt]
    start = pd.Timestamp(year, month_idx, 1)
    last_day = monthrange(year, month_idx)[1]
    end_candidate = pd.Timestamp(year, month_idx, last_day)
    if is_current_year and month_idx == today.month:
        end = today
        label = f"{month_opt} {year} (MTD)"
    else:
        end = end_candidate
        label = f"{month_opt} {year}"
    return start, end, label


def _previous_period_for_comparison(start, end, label):
    if label.startswith("YTD "):
        year = int(label.split(" ")[1])
        prev_year = year - 1
        prev_start = pd.Timestamp(prev_year, 1, 1)
        prev_end = _safe_same_day_last_year(pd.Timestamp(date.today()))
        compare_label = f"vs YTD {prev_year}"
    elif label.startswith("Full Year "):
        year = int(label.split(" ")[2])
        prev_year = year - 1
        prev_start = pd.Timestamp(prev_year, 1, 1)
        prev_end = pd.Timestamp(prev_year, 12, 31)
        compare_label = f"vs Full Year {prev_year}"
    else:
        base = label.replace(" (MTD)", "")
        month_name, year_str = base.split(" ")
        year = int(year_str)
        prev_year = year - 1
        month_idx = MONTH_NAME_TO_NUM[month_name]
        prev_start = pd.Timestamp(prev_year, month_idx, 1)
        prev_end = pd.Timestamp(prev_year, month_idx, monthrange(prev_year, month_idx)[1])
        compare_label = f"vs {month_name} {prev_year}"
    if prev_end < MIN_ALLOWED_DATE:
        return None, None, "—"
    if prev_start < MIN_ALLOWED_DATE:
        prev_start = MIN_ALLOWED_DATE
    return prev_start, prev_end, compare_label


def _query_sum(conn, start, end, column):
    effective_start = max(start, MIN_ALLOWED_DATE)
    if end < effective_start:
        return 0.0
    q = f"""
        SELECT SUM({column}) AS total_val
        FROM ventas_enriched_product
        WHERE DATE(Fecha) BETWEEN DATE(?) AND DATE(?)
          AND DATE(Fecha) >= DATE(?)
    """
    df = pd.read_sql(q, conn, params=(
        effective_start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
        MIN_ALLOWED_DATE.strftime("%Y-%m-%d"),
    ))
    val = df["total_val"].iloc[0]
    return float(val) if pd.notna(val) else 0.0


def _format_currency(n): return f"${n:,.0f}"
def _format_percent(n): return f"{n:.1%}"
def _format_delta(curr, prev):
    if prev and prev > 0:
        pct = (curr - prev) / prev * 100.0
        sign = "+" if pct >= 0 else ""
        return f"{sign}{pct:.1f}%"
    return "—"


# === Main UI ===
def show_sales_analysis():
    
    if not DB_PATH.exists():
        st.error(f"❌ Database not found at {DB_PATH}")
        return

    today = pd.Timestamp(date.today())
    current_year = today.year

    # === Top Layout: Total Sales (left) + Date Selector + Button (right) ===
    col_sales, col_calendar = st.columns([1.5, 1.2])

    with col_calendar:
        with st.expander("📅 Select Period", expanded=True):
            col_year, col_month = st.columns(2)
            with col_year:
                years = list(range(2023, current_year + 1))
                year = st.selectbox("Year", years, index=len(years) - 1)
            with col_month:
                month_choices = ["All"] + MONTHS
                month_choice = st.selectbox("Month", month_choices, index=0)

        # === Run Sales Pipeline button (just below selector) ===
        st.markdown(
            """
            <style>
            div[data-testid="stButton"] > button {
                background: linear-gradient(90deg, #007bff, #0056d2);
                color: white;
                border: none;
                border-radius: 50px;
                padding: 0.55rem 1.4rem;
                font-size: 0.9rem;
                font-weight: 600;
                box-shadow: 0 2px 4px rgba(0,0,0,0.15);
                cursor: pointer;
                transition: all 0.25s ease;
            }
            div[data-testid="stButton"] > button:hover {
                background: linear-gradient(90deg, #0069d9, #004bb0);
                transform: scale(1.04);
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.write("")
        if st.button("Run Sales Pipeline", key="update_btn", help="Fetch, clean, and enrich new sales data"):
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

    # === Compute Dates ===
    sel_start, sel_end, sel_label = _period_for_selection(year, month_choice)
    prev_start, prev_end, compare_label = _previous_period_for_comparison(sel_start, sel_end, sel_label)
    ytd_start = pd.Timestamp(year, 1, 1)
    ytd_end = sel_end

    # === Query Data ===
    try:
        conn = sqlite3.connect(DB_PATH)
        curr_total = _query_sum(conn, sel_start, sel_end, "Total")
        prev_total = _query_sum(conn, prev_start, prev_end, "Total") if prev_start is not None else None
        curr_gross_rev = _query_sum(conn, sel_start, sel_end, "MargenContrib")
        ytd_sales = _query_sum(conn, ytd_start, ytd_end, "Total")
    finally:
        conn.close()

    with col_sales:
        st.subheader("💰 Total Sales")
        delta_str = _format_delta(curr_total, prev_total) if prev_total is not None else "—"
        st.metric(
            label=f"{sel_label}",
            value=_format_currency(curr_total),
            delta=f"{delta_str} {compare_label if prev_total is not None else ''}".strip(),
        )
        st.caption(
            f"Period: {sel_start.strftime('%Y-%m-%d')} → {sel_end.strftime('%Y-%m-%d')} "
            f"(no data before {MIN_ALLOWED_DATE.date()})"
        )

    # === Gross Revenue Section ===
    st.markdown("---")
    gr1, gr2, gr3 = st.columns([1, 1, 1])

    with gr1:
        st.subheader("🏗️ Gross Revenue")
        st.metric(label=f"{sel_label} Gross Revenue", value=_format_currency(curr_gross_rev))

    with gr2:
        ratio = (curr_gross_rev / ytd_sales) if ytd_sales > 0 else 0.0
        st.subheader("📈 GR / YTD Sales")
        st.metric(label=f"Relative to YTD Sales ({year})", value=_format_percent(ratio))

    with gr3:
        gm_ratio = (curr_gross_rev / curr_total) if curr_total > 0 else 0.0
        st.subheader("🧮 Gross Margin")
        st.metric(label=f"GR / Sales ({sel_label})", value=_format_percent(gm_ratio))

    # === Info + Wheeler ===
    st.markdown("---")
    st.info(
        "💡 Default view shows the current year's YTD. Select a year and optionally a month to view totals. "
        "Gross Revenue (MargenContrib) and all ratios are tied to the selected period."
    )

    st.markdown("---")
    #st.subheader("📊 Statistical Wheeler Analysis")
    show_sales_wheeler_analysis()
# === dashboard/tabs/statistics/sales_wheeler_analysis.py ===