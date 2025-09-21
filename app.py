# app.py
# Streamlit admin + dashboard for your SQLite medical video DB
# - Data entry forms for resident, date_case, and record_status
# - Browsing tables with filters
# - Visualizations: cases by day, camera status coverage
#
# How to run:
#   1) pip install streamlit pandas plotly
#   2) streamlit run app.py
#
# Notes:
# - Set the DB path in the sidebar (defaults to ./ScalpelDatabase.sqlite)
# - The app uses only standard sqlite3 (no migrations) and is read/write
# - Forms are defensive to missing optional columns

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import plotly.express as px
import streamlit as st

# -----------------------------
# Config & Helpers
# -----------------------------
DEFAULT_DB = os.environ.get("SCALPEL_DB", os.path.abspath("ScalpelDatabase.sqlite"))

@contextmanager
def connect(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

@st.cache_data(show_spinner=False)
def list_tables(db_path: str) -> List[str]:
    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
        return [r[0] for r in cur.fetchall()]

@st.cache_data(show_spinner=False)
def get_table_schema(db_path: str, table: str) -> pd.DataFrame:
    with connect(db_path) as conn:
        df = pd.read_sql_query(f"PRAGMA table_info({table});", conn)
    return df

@st.cache_data(show_spinner=False)
def load_table(db_path: str, table: str) -> pd.DataFrame:
    with connect(db_path) as conn:
        try:
            return pd.read_sql_query(f"SELECT * FROM {table}", conn)
        except Exception:
            return pd.DataFrame()

# Utility: check if columns exist

def has_columns(db_path: str, table: str, cols: List[str]) -> bool:
    sch = get_table_schema(db_path, table)
    have = set(sch["name"].tolist())
    return all(c in have for c in cols)

# -----------------------------
# Sidebar: DB selection
# -----------------------------
st.set_page_config(page_title="ScalpelLab DB", layout="wide")
st.title("ScalpelLab – SQLite Admin & Dashboards")

with st.sidebar:
    st.header("Database")
    db_path = st.text_input("SQLite DB Path", value=DEFAULT_DB)
    if st.button("Refresh Cache"):
        list_tables.clear()
        get_table_schema.clear()
        load_table.clear()
        st.rerun()

    st.caption("Tip: set env var SCALPEL_DB to default your DB path.")

if not os.path.exists(db_path):
    st.warning(f"DB not found at: {db_path}. Create it or point to an existing file.")

# -----------------------------
# Tabs
# -----------------------------
Tab = st.tabs(["🔎 Browse", "➕ Data Entry", "📈 Visualizations", "⚙️ Utilities"])

# -----------------------------
# Browse
# -----------------------------
with Tab[0]:
    st.subheader("Browse Tables")
    tables = list_tables(db_path) if os.path.exists(db_path) else []
    if tables:
        t = st.selectbox("Choose a table", options=tables)
        df = load_table(db_path, t)
        st.caption(f"Rows: {len(df)}")
        if not df.empty:
            # Simple text filter
            q = st.text_input("Search (contains, any column)")
            if q:
                mask = pd.Series([False]*len(df))
                for col in df.columns:
                    mask = mask | df[col].astype(str).str.contains(q, case=False, na=False)
                df = df[mask]
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), file_name=f"{t}.csv", mime="text/csv")
        else:
            st.info("Table is empty or unreadable.")
    else:
        st.info("No user tables found.")

# -----------------------------
# Data Entry
# -----------------------------
with Tab[1]:
    st.subheader("Add / Update Data")
    subtab = st.tabs(["Resident", "Date Case", "Record Status"])

    # --- Resident form ---
    with subtab[0]:
        st.markdown("#### Add Resident")
        cols = st.columns(3)
        intern_key = cols[0].number_input("intern_key", min_value=1, step=1)
        name = cols[1].text_input("name")
        year = cols[2].number_input("year (start)", min_value=1900, max_value=2100, value=2020, step=1)
        cols2 = st.columns(4)
        month = cols2[0].number_input("month (1-12)", min_value=1, max_value=12, value=1, step=1)
        code = cols2[1].text_input("code (optional)")
        grade_a_month = cols2[2].selectbox("grade_a_month (optional)", options=["", "January","February","March","April","May","June","July","August","September","October","November","December"])
        grade_a_year = cols2[3].number_input("grade_a_year (optional)", min_value=0, max_value=2100, value=0, step=1)

        if st.button("Insert Resident"):
            with connect(db_path) as conn:
                cur = conn.cursor()
                # Discover actual columns and build insert accordingly
                sch = get_table_schema(db_path, "resident")
                cols_exist = set(sch["name"].tolist())
                payload = {
                    "intern_key": intern_key,
                    "name": name.strip(),
                    "year": int(year) if year else None,
                    "month": int(month) if month else None,
                }
                if "code" in cols_exist:
                    payload["code"] = code.strip() or None
                if "grade_a_month" in cols_exist:
                    payload["grade_a_month"] = grade_a_month or None
                if "grade_a_year" in cols_exist:
                    payload["grade_a_year"] = int(grade_a_year) if grade_a_year else None

                keys = ",".join(payload.keys())
                qmarks = ",".join(["?"]*len(payload))
                try:
                    cur.execute(f"INSERT INTO resident ({keys}) VALUES ({qmarks})", tuple(payload.values()))
                    st.success("Resident inserted.")
                except Exception as e:
                    st.error(f"Insert failed: {e}")

        st.divider()
        st.markdown("##### Residents Preview")
        st.dataframe(load_table(db_path, "resident"), use_container_width=True, hide_index=True)

    # --- Date Case form ---
    with subtab[1]:
        st.markdown("#### Add Date Case")
        c1, c2 = st.columns(2)
        date_str = c1.date_input("date").strftime("%Y-%m-%d")
        case_no = c2.number_input("case", min_value=1, step=1, value=1)
        if st.button("Insert Date Case"):
            with connect(db_path) as conn:
                cur = conn.cursor()
                try:
                    # If table has generated column 'date_case', just insert date & case
                    cur.execute("INSERT INTO date_case(\"date\", \"case\") VALUES (?, ?)", (date_str, int(case_no)))
                    st.success("date_case inserted.")
                except Exception as e:
                    st.error(f"Insert failed: {e}")
        st.divider()
        st.dataframe(load_table(db_path, "date_case"), use_container_width=True, hide_index=True)

    # --- Record Status form ---
    with subtab[2]:
        st.markdown("#### Upsert Record Status for a date_case")
        # fetch date_case list
        dc_df = load_table(db_path, "date_case")
        dc_options = dc_df["date_case"].tolist() if "date_case" in dc_df.columns else []
        date_case_sel = st.selectbox("date_case", options=dc_options)

        # camera/status fields (defensive: only show if exists)
        camera_cols = [
            "Cart_Center_2","Cart_LT_4","Cart_RT_1","General_3",
            "Monitor","Patient_Monitor","Ventilator_Monitor","Injection_Port"
        ]
        exists = has_columns(db_path, "record_status", ["date_case"]) if os.path.exists(db_path) else False
        if exists:
            sch = get_table_schema(db_path, "record_status")
            have_cols = set(sch["name"].tolist())
            use_cols = [c for c in camera_cols if c in have_cols]
            cols1 = st.columns(4)
            cols2 = st.columns(4)
            inputs: Dict[str, Any] = {}
            for i, c in enumerate(use_cols):
                col = cols1[i] if i < 4 else cols2[i-4]
                inputs[c] = col.number_input(c, min_value=0, max_value=9, step=1, value=0)
            comments = st.text_input("comments (optional)") if "comments" in have_cols else None

            if st.button("Save Record Status"):
                with connect(db_path) as conn:
                    cur = conn.cursor()
                    try:
                        # check if row exists
                        cur.execute("SELECT 1 FROM record_status WHERE date_case=?", (date_case_sel,))
                        exists_row = cur.fetchone() is not None
                        if exists_row:
                            # update
                            sets = ",".join([f"{k}=?" for k in use_cols] + (["comments=?"] if comments is not None else []))
                            vals = [inputs[k] for k in use_cols]
                            if comments is not None:
                                vals.append(comments or None)
                            vals.append(date_case_sel)
                            cur.execute(f"UPDATE record_status SET {sets} WHERE date_case=?", tuple(vals))
                        else:
                            cols_all = ["date_case"] + use_cols + (["comments"] if comments is not None else [])
                            qmarks = ",".join(["?"]*len(cols_all))
                            vals = [date_case_sel] + [inputs[k] for k in use_cols]
                            if comments is not None:
                                vals.append(comments or None)
                            cur.execute(f"INSERT INTO record_status ({','.join(cols_all)}) VALUES ({qmarks})", tuple(vals))
                        st.success("Record status saved.")
                    except Exception as e:
                        st.error(f"Save failed: {e}")
        else:
            st.info("Table 'record_status' not found or missing 'date_case' column.")

        st.divider()
        st.markdown("##### record_status Preview")
        st.dataframe(load_table(db_path, "record_status"), use_container_width=True, hide_index=True)

# -----------------------------
# Visualizations
# -----------------------------
with Tab[2]:
    st.subheader("Dashboards")

    # Cases per day
    dc = load_table(db_path, "date_case")
    if not dc.empty and {"date","case"}.issubset(dc.columns):
        cases_daily = dc.groupby("date").size().reset_index(name="cases")
        fig = px.bar(cases_daily, x="date", y="cases", title="Cases per Day")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Need table 'date_case' with columns date, case.")

    st.divider()

    # Camera status coverage (counts per status code)
    rs = load_table(db_path, "record_status")
    if not rs.empty:
        camera_cols = [c for c in rs.columns if c in [
            "Cart_Center_2","Cart_LT_4","Cart_RT_1","General_3",
            "Monitor","Patient_Monitor","Ventilator_Monitor","Injection_Port"
        ]]
        if camera_cols:
            melt = rs.melt(value_vars=camera_cols, var_name="camera", value_name="status")
            # status distribution per camera
            dist = melt.groupby(["camera","status"]).size().reset_index(name="count")
            fig2 = px.bar(dist, x="camera", y="count", color="status", barmode="stack", title="Camera Status Distribution")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No camera columns found in record_status.")
    else:
        st.info("Table 'record_status' is empty.")

# -----------------------------
# Utilities
# -----------------------------
with Tab[3]:
    st.subheader("Utilities")

    st.markdown("#### Run a quick SQL (read-only)")
    sql = st.text_area("SQL", value="SELECT name FROM sqlite_master WHERE type='table';")
    if st.button("Execute"):
        try:
            with connect(db_path) as conn:
                df = pd.read_sql_query(sql, conn)
            st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Query failed: {e}")

    st.caption("This utility query is SELECT-only to keep things safe. Use forms above to write data.")
