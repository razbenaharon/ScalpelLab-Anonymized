import streamlit as st
import pandas as pd
from collections import Counter
import plotly.express as px
import sys, os

# If utils.py is in project root (not pages/), uncomment to add parent dir to path:
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils import load_table, list_tables, get_table_schema, connect

DEFAULT_CAMERAS = [
    "Cart_Center_2","Cart_LT_4","Cart_RT_1",
    "General_3","Monitor","Patient_Monitor",
    "Ventilator_Monitor","Injection_Port"
]

LABELS_MP4 = {1: ">=200MB", 2: "<200MB", 3: "Missing"}
LABELS_SEQ  = {1: ">200MB",  2: "<200MB", 3: "Missing", 4: "FORMAT PROBLEM"}

st.header("🧮 Status Summary (mp4_status & seq_status)")

db_path = st.session_state.get("db_path")
if not db_path:
    st.warning("No database path set in session. Open the main page and set the DB path in the sidebar.")
    st.stop()

with st.sidebar:
    st.markdown("### Status Summary Options")
    mp4_table = st.text_input("MP4 status table", value="mp4_status")
    seq_table = st.text_input("SEQ status table", value="seq_status")
    cameras = st.multiselect("Cameras", DEFAULT_CAMERAS, default=DEFAULT_CAMERAS)

def fetch_camera_stats(db_path: str, table: str, cameras: list[str]) -> tuple[int, dict]:
    with connect(db_path) as conn:
        cur = conn.cursor()
        # Count distinct cases in the normalized table
        cur.execute(f"SELECT COUNT(DISTINCT recording_date || '-' || case_no) FROM {table}")
        total_cases = cur.fetchone()[0]
        camera_stats = {cam: Counter() for cam in cameras}
        # Query normalized schema: (recording_date, case_no, camera_name, value, comments, size_mb)
        placeholders = ','.join(['?'] * len(cameras))
        cur.execute(f"SELECT camera_name, value FROM {table} WHERE camera_name IN ({placeholders})", cameras)
        for camera_name, status_value in cur.fetchall():
            if status_value is None:
                continue
            try:
                camera_stats[camera_name][int(status_value)] += 1
            except (TypeError, ValueError):
                pass
        return total_cases, camera_stats

def stats_to_dataframe(camera_stats: dict, labels: dict, status_order) -> pd.DataFrame:
    rows = []
    present = set()
    for cam, ctr in camera_stats.items():
        for s, cnt in ctr.items():
            if cnt > 0:
                present.add(s)
    statuses = [s for s in status_order if (s in present) or (not present and s in labels)]
    for cam, ctr in camera_stats.items():
        for s in statuses:
            rows.append({
                "camera": cam,
                "status": s,
                "status_label": labels.get(s, str(s)),
                "count": int(ctr.get(s, 0))
            })
    return pd.DataFrame(rows)

def section(title: str, table_name: str, labels: dict, order: tuple[int, ...]):
    st.subheader(title)
    try:
        total_rows, camera_stats = fetch_camera_stats(db_path, table_name, cameras)
        st.caption(f"Total cases in `{table_name}`: **{total_rows}**")
        df = stats_to_dataframe(camera_stats, labels, order)

        if df.empty:
            st.info("No status data found for the selected cameras.")
            return

        pivot = df.pivot_table(index="camera", columns="status_label", values="count",
                               aggfunc="sum", fill_value=0)
        st.dataframe(pivot, width="stretch")

        totals = df.groupby("status_label")["count"].sum().reset_index()
        st.markdown("**Totals across all cameras:**")
        st.dataframe(totals, width="stretch", hide_index=True)

    except Exception as e:
        st.error(f"Error: {e}")

section("📁 MP4 Status Summary", mp4_table, LABELS_MP4, (1, 2, 3))
st.divider()
section("🎞️ SEQ Status Summary", seq_table, LABELS_SEQ, (1, 2, 3, 4))