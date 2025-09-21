import streamlit as st
import sys, os
# This line adds the project root to the path to fix the import error
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils import list_tables, load_table

st.header("🔎 Browse Tables")

db_path = st.session_state.get("db_path")
if not db_path:
    st.warning("No database path set.")
else:
    tables = list_tables(db_path)
    if tables:
        t = st.selectbox("Choose a table", options=tables)
        df = load_table(db_path, t)
        st.caption(f"Rows: {len(df)}")
        if not df.empty:
            q = st.text_input("Search (contains, any column)")
            if q:
                mask = df.apply(lambda row: row.astype(str).str.contains(q, case=False, na=False).any(), axis=1)
                df = df[mask]
            st.dataframe(df, width="stretch", hide_index=True)
            st.download_button(
                "Download CSV",
                df.to_csv(index=False).encode("utf-8"),
                file_name=f"{t}.csv",
                mime="text/csv",
            )
        else:
            st.info("Table is empty or unreadable.")
    else:
        st.info("No user tables found.")