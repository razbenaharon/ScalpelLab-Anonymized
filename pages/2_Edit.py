import streamlit as st
import sys, os
# This line adds the project root to the path to fix the import error
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils import list_tables, get_table_schema, load_table, connect

def get_next_anesthetic_key(db_path):
    """Get the next available anesthetic_key"""
    try:
        with connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(anesthetic_key) FROM anesthetic")
            result = cur.fetchone()[0]
            return (result + 1) if result else 1
    except Exception:
        return 1

st.header("✏️ Edit & Table Management")

db_path = st.session_state.get("db_path")

if not db_path:
    st.warning("No database path set.")
else:
    tables = list_tables(db_path)
    if not tables:
        st.info("No tables found.")
    else:
        table_choice = st.selectbox("Target table", options=tables)

        schema_df = get_table_schema(db_path, table_choice)
        cols_meta = schema_df.to_dict(orient="records")

        # Special handling for anesthetic table
        if table_choice == "anesthetic":
            # Auto-generate anesthetic_key
            next_key = get_next_anesthetic_key(db_path)
            st.info(f"Next available anesthetic key: {next_key}")

        input_values = {}
        for col in cols_meta:
            name = col["name"]
            # skip common generated column patterns (adjust if needed)
            if name.lower() in {"date_case"}:
                continue

            # For anesthetic table, skip anesthetic_key input as it's auto-generated
            if table_choice == "anesthetic" and name == "anesthetic_key":
                input_values[name] = next_key
                continue

            ctype = (col["type"] or "").upper()
            if "DATE" in ctype or name.endswith("_date") or name == "date":
                d = st.date_input(name)
                input_values[name] = d.strftime("%Y-%m-%d") if d else None
            elif "INT" in ctype:
                input_values[name] = st.number_input(name, step=1)
            elif "REAL" in ctype or "FLOA" in ctype or "DOUB" in ctype:
                input_values[name] = st.number_input(name)
            elif name.lower() in ("comments", "comment", "notes"):
                input_values[name] = st.text_area(name)
            else:
                input_values[name] = st.text_input(name)

        if st.button("Insert Row"):
            cleaned = {k: (v if v != "" else None) for k, v in input_values.items()}
            try:
                with connect(db_path) as conn:
                    cur = conn.cursor()
                    keys = ",".join(cleaned.keys())
                    qmarks = ",".join(["?"] * len(cleaned))
                    cur.execute(
                        f"INSERT INTO {table_choice} ({keys}) VALUES ({qmarks})",
                        tuple(cleaned.values()),
                    )
                st.success(f"Inserted into {table_choice}.")
                load_table.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Insert failed: {e}")

        st.divider()
        st.dataframe(load_table(db_path, table_choice), width="stretch", hide_index=True)