import streamlit as st
import plotly.express as px
import sys, os
# This line adds the project root to the path to fix the import error
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils import load_table

st.header("📈 Visualizations")

db_path = st.session_state.get("db_path")

dc = load_table(db_path, "date_case")
if not dc.empty and {"date", "case"}.issubset(dc.columns):
    cases_daily = dc.groupby("date").size().reset_index(name="cases")
    fig = px.bar(cases_daily, x="date", y="cases", title="Cases per Day")
    st.plotly_chart(fig, width="stretch")
else:
    st.info("Need table 'date_case' with columns date, case.")

st.divider()

rs = load_table(db_path, "record_status")
if not rs.empty:
    camera_cols = [c for c in rs.columns if c in [
        "Cart_Center_2","Cart_LT_4","Cart_RT_1","General_3",
        "Monitor","Patient_Monitor","Ventilator_Monitor","Injection_Port"
    ]]
    if camera_cols:
        melt = rs.melt(value_vars=camera_cols, var_name="camera", value_name="status")
        dist = melt.groupby(["camera","status"]).size().reset_index(name="count")
        fig2 = px.bar(dist, x="camera", y="count", color="status", barmode="stack",
                     title="Camera Status Distribution")
        st.plotly_chart(fig2, width="stretch")
    else:
        st.info("No camera columns found in record_status.")
else:
    st.info("Table 'record_status' is empty.")