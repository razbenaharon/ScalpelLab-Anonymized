import streamlit as st
import subprocess
import os
import sys
from pathlib import Path

st.set_page_config(page_title="Scripts", layout="wide")
st.title("Script Runner")

# Get the scripts directory
script_dir = Path(__file__).parent.parent / "scripts"

# Available scripts with descriptions
scripts = {
    "Database Scripts": {
        "scripts/database/dedupe_exports.py": "Remove duplicate exports from database",
        "scripts/database/mp4_status_update.py": "Update MP4 status in database",
        "scripts/database/sql_to_path.py": "Convert SQL query results to file paths",
        "scripts/database/status_statistics.py": "Generate status statistics from database"
    },
    "Export Scripts": {
        "scripts/export/sqlite_to_dbdiagram.py": "Export database schema to dbdiagram format",
        "scripts/database/seq_exporter.py": "Export sequence data from database"
    }
}

st.header("Available Scripts")

for category, script_dict in scripts.items():
    st.subheader(category)

    for script_path, description in script_dict.items():
        script_name = os.path.basename(script_path)

        col1, col2, col3 = st.columns([3, 1, 1])

        with col1:
            st.write(f"**{script_name}**")
            st.write(description)

        with col2:
            if st.button("Run", key=f"run_{script_name}"):
                st.session_state[f"running_{script_name}"] = True

        with col3:
            if st.button("View Code", key=f"view_{script_name}"):
                st.session_state[f"viewing_{script_name}"] = True

        # Show script execution
        if st.session_state.get(f"running_{script_name}", False):
            st.write(f"Running {script_name}...")

            try:
                # Run the script
                result = subprocess.run(
                    [sys.executable, script_path],
                    capture_output=True,
                    text=True,
                    cwd=str(Path(__file__).parent.parent)
                )

                if result.returncode == 0:
                    st.success("Script completed successfully!")
                    if result.stdout:
                        st.subheader("Output:")
                        st.code(result.stdout)
                else:
                    st.error("Script failed!")
                    if result.stderr:
                        st.subheader("Error:")
                        st.code(result.stderr)

            except Exception as e:
                st.error(f"Error running script: {str(e)}")

            # Reset the running state
            st.session_state[f"running_{script_name}"] = False

        # Show script code
        if st.session_state.get(f"viewing_{script_name}", False):
            try:
                with open(script_path, 'r') as f:
                    code = f.read()
                st.subheader(f"Code for {script_name}:")
                st.code(code, language='python')

                if st.button("Hide Code", key=f"hide_{script_name}"):
                    st.session_state[f"viewing_{script_name}"] = False
                    st.rerun()

            except Exception as e:
                st.error(f"Error reading script: {str(e)}")

        st.divider()

# Custom script runner
st.header("Custom Script")
st.write("Run a custom Python command or script:")

custom_command = st.text_area("Python command:", placeholder="print('Hello World')")

if st.button("Run Custom Command"):
    if custom_command.strip():
        try:
            result = subprocess.run(
                [sys.executable, "-c", custom_command],
                capture_output=True,
                text=True,
                cwd=str(Path(__file__).parent.parent)
            )

            if result.returncode == 0:
                st.success("Command completed successfully!")
                if result.stdout:
                    st.subheader("Output:")
                    st.code(result.stdout)
            else:
                st.error("Command failed!")
                if result.stderr:
                    st.subheader("Error:")
                    st.code(result.stderr)

        except Exception as e:
            st.error(f"Error running command: {str(e)}")
    else:
        st.warning("Please enter a command to run.")