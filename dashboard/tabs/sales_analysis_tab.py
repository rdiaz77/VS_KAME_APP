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


# === Helpers ===
def _safe_same_day_last_year(ts: pd.Timestamp) -> pd.Timestamp:
    """
    Return the same calendar day one year earlier.
    Handles Feb 29 -> Feb 28 fallback in non-leap years.
    """
    try:
        return ts - pd.DateOffset(years=1)
    except Exception:
        # As a fallback, clamp to Feb 28 if necessary
        if ts.month == 2 and ts.day == 29:
            return pd.Timestamp(ts.year - 1, 2, 28)
        # Generic fallback: subtract 365 days (approx)
        return ts - pd.Timedelta(days=365)


def _period_for_selection(
    year: int, month_opt: str | None
) -> tuple[pd.Timestamp, pd.Timestamp, str]:
    """
    Compute start/end dates and a human label for the selected year/month.
    - If month_opt == "All": full year for past years; YTD for current year.
    - If month is a number name like "March": that month's period.
    """
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

    # Specific month
    month_idx = MONTH_NAME_TO_NUM[month_opt]
    start = pd.Timestamp(year, month_idx, 1)
    last_day = monthrange(year, month_idx)[1]
    end_candidate = pd.Timestamp(year, month_idx, last_day)

    if is_current_year and month_idx == today.month:
        # Current month -> MTD
        end = today
        label = f"{month_opt} {year} (MTD)"
    else:
        end = end_candidate
        label = f"{month_opt} {year}"

    return start, end, label


def _previous_period_for_comparison(
    start: pd.Timestamp, end: pd.Timestamp, label: str
) -> tuple[pd.Timestamp | None, pd.Timestamp | None, str]:
    """
    Build the previous-year comparison period:
    - For YTD: previous YTD up to same calendar day last year.
    - For full year: previous full year.
    - For a month: same month previous year.
    Returns (prev_start, prev_end, compare_label). May return (None, None, "—") if invalid (<2023).
    """
    # Detect type by label
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
        # "<Month> <Year>" or "(MTD)"
        # Extract month and year
        base = label.replace(" (MTD)", "")
        month_name, year_str = base.split(" ")
        year = int(year_str)
        prev_year = year - 1
        month_idx = MONTH_NAME_TO_NUM[month_name]
        prev_start = pd.Timestamp(prev_year, month_idx, 1)
        prev_end = pd.Timestamp(
            prev_year, month_idx, monthrange(prev_year, month_idx)[1]
        )
        compare_label = f"vs {month_name} {prev_year}"

    # Enforce minimum allowed date
    if prev_end < MIN_ALLOWED_DATE:
        return None, None, "—"
    if prev_start < MIN_ALLOWED_DATE:
        prev_start = MIN_ALLOWED_DATE

    return prev_start, prev_end, compare_label


def _query_total(
    conn: sqlite3.Connection, start: pd.Timestamp, end: pd.Timestamp
) -> float:
    """
    Query SUM(Total) in ventas_enriched_product for [start, end], enforcing MIN_ALLOWED_DATE.
    """
    # Enforce lower bound in the query and parameters
    effective_start = max(start, MIN_ALLOWED_DATE)
    if end < effective_start:
        return 0.0

    q = """
        SELECT SUM(Total) AS total_sales
        FROM ventas_enriched_product
        WHERE DATE(Fecha) BETWEEN DATE(?) AND DATE(?)
          AND DATE(Fecha) >= DATE(?)
    """
    df = pd.read_sql(
        q,
        conn,
        params=(
            effective_start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            MIN_ALLOWED_DATE.strftime("%Y-%m-%d"),
        ),
    )
    val = df["total_sales"].iloc[0]
    return float(val) if pd.notna(val) else 0.0


def _format_currency(n: float) -> str:
    return f"${n:,.0f}"


def _format_delta(curr: float, prev: float) -> str:
    if prev and prev > 0:
        pct = (curr - prev) / prev * 100.0
        sign = "+" if pct >= 0 else ""
        return f"{sign}{pct:.1f}%"
    return "—"


# Month helpers
MONTHS = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
MONTH_NAME_TO_NUM = {name: i + 1 for i, name in enumerate(MONTHS)}


# === Main UI ===
def show_sales_analysis():
    """Sales Analysis tab with Year/Month selector, defaulting to current YTD, with YoY comparison."""
    st.header("📊 Sales Analysis")

    # --- DB existence check ---
    if not DB_PATH.exists():
        st.error(f"❌ Database not found at {DB_PATH}")
        return

    # --- Defaults ---
    today = pd.Timestamp(date.today())
    current_year = today.year

    # --- Selectors ---
    # --- Period selector area (1/4 width) ---
    c1, c2, c3, c4 = st.columns([1, 1, 0.1, 0.1])  # first column ≈ ¼ width
    with c1:
        with st.expander("📅 Select Period", expanded=True):
            col_year, col_month = st.columns([1, 1])  # same row: year + month
            with col_year:
                years = list(range(2023, current_year + 1))
                year = st.selectbox("Year", years, index=len(years) - 1)
            with col_month:
                month_choices = ["All"] + MONTHS
                month_choice = st.selectbox("Month", month_choices, index=0)


    # --- Compute periods ---
    sel_start, sel_end, sel_label = _period_for_selection(year, month_choice)

    prev_start, prev_end, compare_label = _previous_period_for_comparison(
        sel_start, sel_end, sel_label
    )

    # --- Query totals ---
    try:
        conn = sqlite3.connect(DB_PATH)

        curr_total = _query_total(conn, sel_start, sel_end)

        if prev_start is not None and prev_end is not None:
            prev_total = _query_total(conn, prev_start, prev_end)
        else:
            prev_total = None

    except Exception as e:
        st.error(f"Database error: {e}")
        return
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # --- KPI ---
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("💰 Total Sales")
        delta_str = (
            _format_delta(curr_total, prev_total) if prev_total is not None else "—"
        )
        # Include compare label in help text below the metric
        st.metric(
            label=f"{sel_label}",
            value=_format_currency(curr_total),
            delta=f"{delta_str} {compare_label if prev_total is not None else ''}".strip(),
        )

        # Period text
        st.caption(
            f"Period: {sel_start.strftime('%Y-%m-%d')} → {sel_end.strftime('%Y-%m-%d')} (no data before {MIN_ALLOWED_DATE.date()})"
        )
        if prev_total is not None:
            st.caption(
                f"Comparison: {prev_start.strftime('%Y-%m-%d')} → {prev_end.strftime('%Y-%m-%d')}"
            )

    with col2:
        # Styled button
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
        "💡 Default view shows the current year's YTD. "
        "Select a year and optionally a month to view totals for that period. "
        "Past years: 'All' = full year. Current year: 'All' = YTD. "
        "Comparisons are vs the same period in the previous year."
    )

    st.markdown("---")
    st.subheader("📊 Statistical Wheeler Analysis")
    # Unchanged: relies on its own internal logic / data scope
    show_sales_wheeler_analysis()


# === End of dashboard/tabs/sales_analysis_tab.py ===
