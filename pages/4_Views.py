import streamlit as st
import sys, os
# This line adds the project root to the path to fix the import error
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils import list_views, load_table

st.header("👁️ Database Views")

db_path = st.session_state.get("db_path")

if not db_path:
    st.warning("No database path set.")
else:
    views = list_views(db_path)
    if not views:
        st.info("No views found in the database.")
    else:
        st.success(f"Found {len(views)} view(s) in the database:")

        view_choice = st.selectbox("Select a view to display", options=views)

        if view_choice:
            st.subheader(f"View: {view_choice}")
            try:
                df = load_table(db_path, view_choice)
                if not df.empty:
                    st.dataframe(df, width="stretch", hide_index=True)
                    st.caption(f"Showing {len(df)} rows")
                else:
                    st.info("View is empty or could not be loaded.")
            except Exception as e:
                st.error(f"Error loading view: {e}")