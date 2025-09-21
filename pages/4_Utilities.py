import streamlit as st
import pandas as pd
import sys, os
# This line adds the project root to the path to fix the import error
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils import connect

st.header("⚙️ Utilities")

db_path = st.session_state.get("db_path")

sql = st.text_area("SQL", value="SELECT name FROM sqlite_master WHERE type='table';")
if st.button("Execute"):
    try:
        with connect(db_path) as conn:
            df = pd.read_sql_query(sql, conn)
        st.dataframe(df, width="stretch", hide_index=True)
    except Exception as e:
        st.error(f"Query failed: {e}")

st.caption("This query runner is SELECT-only for safety.")