import streamlit as st
from utils.db_utils import load_table
from utils.charts import line_chart

def show_scorecard():
    st.title("🏠 Balanced Scorecard — VitroScience")

    # Example metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Revenue YTD", "$4.2M", "+5.3%")
    col2.metric("Gross Margin", "48%", "+2.1%")
    col3.metric("Customer Retention", "94%", "+1.2%")
    col4.metric("Training Hours", "12.5", "+15%")

    st.divider()
    tabs = st.tabs(["💰 Finanzas", "👥 Clientes", "⚙️ Internal", "📚 Learning & Growth"])

    with tabs[0]:
        df = load_table("ventas")  # Example table
        st.subheader("Revenue Trend")
        st.plotly_chart(line_chart(df, "Fecha", "Total", "Revenue Over Time"), use_container_width=True)
