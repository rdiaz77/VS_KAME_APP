import pandas as pd
import plotly.express as px
import streamlit as st

# Set page configuration
st.set_page_config(
    layout="wide", page_title="Balanced Scorecard Dashboard", page_icon="📊"
)


# --- Sample Data Generation ---
# In a real-world app, you would load data from a database, CSV, or API.
@st.cache_data
def load_data():
    """Generates a sample DataFrame for the Balanced Scorecard."""
    data = {
        "Date": pd.to_datetime(pd.date_range(start="2024-01-01", periods=12, freq="M")),
        "Financial_Revenue": [
            150000,
            155000,
            160000,
            158000,
            170000,
            175000,
            180000,
            185000,
            190000,
            195000,
            200000,
            210000,
        ],
        "Financial_Profit": [
            30000,
            31000,
            32000,
            31500,
            35000,
            36000,
            38000,
            39000,
            40000,
            41000,
            42000,
            45000,
        ],
        "Customer_Satisfaction": [
            4.2,
            4.3,
            4.5,
            4.4,
            4.6,
            4.5,
            4.7,
            4.8,
            4.7,
            4.9,
            4.9,
            5.0,
        ],  # Scale of 1-5
        "Customer_Retention": [
            90,
            91,
            92,
            91,
            93,
            93,
            94,
            95,
            94,
            96,
            96,
            97,
        ],  # Percentage
        "Process_Efficiency": [
            85,
            86,
            88,
            87,
            89,
            90,
            91,
            92,
            91,
            93,
            94,
            95,
        ],  # Percentage
        "Process_Defect_Rate": [
            2.5,
            2.4,
            2.2,
            2.3,
            2.0,
            1.9,
            1.8,
            1.7,
            1.6,
            1.5,
            1.4,
            1.3,
        ],  # Percentage
        "Growth_Employee_Training": [
            15,
            16,
            18,
            17,
            20,
            22,
            25,
            26,
            28,
            30,
            32,
            35,
        ],  # Hours per employee
        "Growth_Innovation_Score": [
            75,
            76,
            78,
            77,
            80,
            81,
            83,
            84,
            85,
            86,
            87,
            88,
        ],  # Scale of 1-100
    }
    df = pd.DataFrame(data)
    df.set_index("Date", inplace=True)
    return df


df = load_data()

# --- Dashboard Header ---
st.title("📊 Company Performance Balanced Scorecard")
st.markdown(
    "This dashboard provides a holistic view of the company's health across four key perspectives."
)

# --- Sidebar for Filters ---
st.sidebar.header("Filter Options")
selected_year = st.sidebar.selectbox("Select Year", options=df.index.year.unique())
filtered_df = df[df.index.year == selected_year]

# --- Scorecard Layout with Tabs ---
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Financial", "Customer", "Internal Process", "Learning & Growth", "Raw Data"]
)

with tab1:
    st.subheader("Financial Perspective")
    col1, col2 = st.columns(2)

    with col1:
        # Display current metric for Revenue
        current_revenue = filtered_df["Financial_Revenue"].iloc[-1]
        previous_revenue = filtered_df["Financial_Revenue"].iloc[-2]
        st.metric(
            label="Revenue",
            value=f"${current_revenue:,.0f}",
            delta=f"{current_revenue - previous_revenue:,.0f}",
        )

        # Plot Revenue trend
        fig_rev = px.line(
            filtered_df, y="Financial_Revenue", title="Revenue Trend Over Time"
        )
        st.plotly_chart(fig_rev, use_container_width=True)

    with col2:
        # Display current metric for Profit
        current_profit = filtered_df["Financial_Profit"].iloc[-1]
        previous_profit = filtered_df["Financial_Profit"].iloc[-2]
        st.metric(
            label="Profit",
            value=f"${current_profit:,.0f}",
            delta=f"{current_profit - previous_profit:,.0f}",
        )

        # Plot Profit trend
        fig_prof = px.bar(filtered_df, y="Financial_Profit", title="Profit Over Time")
        st.plotly_chart(fig_prof, use_container_width=True)

with tab2:
    st.subheader("Customer Perspective")
    col1, col2 = st.columns(2)

    with col1:
        current_satisfaction = filtered_df["Customer_Satisfaction"].iloc[-1]
        previous_satisfaction = filtered_df["Customer_Satisfaction"].iloc[-2]
        st.metric(
            label="Average Customer Satisfaction",
            value=f"{current_satisfaction:.1f}",
            delta=f"{current_satisfaction - previous_satisfaction:.1f}",
        )

        fig_sat = px.line(
            filtered_df, y="Customer_Satisfaction", title="Customer Satisfaction Trend"
        )
        st.plotly_chart(fig_sat, use_container_width=True)

    with col2:
        current_retention = filtered_df["Customer_Retention"].iloc[-1]
        previous_retention = filtered_df["Customer_Retention"].iloc[-2]
        st.metric(
            label="Customer Retention Rate",
            value=f"{current_retention}%",
            delta=f"{current_retention - previous_retention}%",
        )

        fig_ret = px.area(
            filtered_df, y="Customer_Retention", title="Customer Retention Trend"
        )
        st.plotly_chart(fig_ret, use_container_width=True)

with tab3:
    st.subheader("Internal Process Perspective")
    col1, col2 = st.columns(2)

    with col1:
        current_efficiency = filtered_df["Process_Efficiency"].iloc[-1]
        previous_efficiency = filtered_df["Process_Efficiency"].iloc[-2]
        st.metric(
            label="Process Efficiency",
            value=f"{current_efficiency}%",
            delta=f"{current_efficiency - previous_efficiency}%",
        )

        fig_eff = px.line(
            filtered_df, y="Process_Efficiency", title="Process Efficiency Trend"
        )
        st.plotly_chart(fig_eff, use_container_width=True)

    with col2:
        current_defects = filtered_df["Process_Defect_Rate"].iloc[-1]
        previous_defects = filtered_df["Process_Defect_Rate"].iloc[-2]
        st.metric(
            label="Defect Rate",
            value=f"{current_defects:.1f}%",
            delta=f"{current_defects - previous_defects:.1f}%",
        )

        fig_def = px.line(
            filtered_df,
            y="Process_Defect_Rate",
            title="Defect Rate Trend",
            color_discrete_sequence=["red"],
        )
        st.plotly_chart(fig_def, use_container_width=True)

with tab4:
    st.subheader("Learning and Growth Perspective")
    col1, col2 = st.columns(2)

    with col1:
        current_training = filtered_df["Growth_Employee_Training"].iloc[-1]
        previous_training = filtered_df["Growth_Employee_Training"].iloc[-2]
        st.metric(
            label="Avg Employee Training Hours",
            value=f"{current_training}",
            delta=f"{current_training - previous_training}",
        )

        fig_train = px.bar(
            filtered_df,
            y="Growth_Employee_Training",
            title="Average Employee Training Hours",
        )
        st.plotly_chart(fig_train, use_container_width=True)

    with col2:
        current_innovation = filtered_df["Growth_Innovation_Score"].iloc[-1]
        previous_innovation = filtered_df["Growth_Innovation_Score"].iloc[-2]
        st.metric(
            label="Innovation Score",
            value=f"{current_innovation}",
            delta=f"{current_innovation - previous_innovation}",
        )

        fig_inn = px.line(
            filtered_df, y="Growth_Innovation_Score", title="Innovation Score Trend"
        )
        st.plotly_chart(fig_inn, use_container_width=True)

with tab5:
    st.subheader("Raw Data")
    st.dataframe(filtered_df)
# End of file test_bsc.py
