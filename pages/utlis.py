import os
import streamlit as st

st.set_page_config(page_title="ScalpelLab DB", layout="wide")

st.title("ScalpelLab – SQLite Admin & Dashboards")

st.sidebar.header("Database")
DEFAULT_DB = os.environ.get("SCALPEL_DB", os.path.abspath("ScalpelDatabase.sqlite"))
db_path = st.sidebar.text_input("SQLite DB Path", value=DEFAULT_DB)

# make DB path available to all pages
st.session_state["db_path"] = db_path

st.sidebar.markdown("Navigate using the left sidebar menu (pages).")
