import os
import sys
import streamlit as st
import fitz  # PyMuPDF
from PIL import Image
import io


#streamlit run app.py

# Import path manager
sys.path.append(os.path.join(os.path.dirname(__file__)))

st.set_page_config(page_title="ScalpelLab DB", layout="wide")

st.title("ScalpelLab – Streamlit SQLite Database Manager")

# Project overview section
st.markdown("""

### 🛠 **Available Tools**
- **Browse**: Query and explore database records 
- **Edit**: Modify database records through forms
- **Status Summary**: View processing statistics and summaries
- **Scripts**: Run automated data processing and export scripts

Navigate using the sidebar to access different features and tools.
""")

st.markdown("---")

st.sidebar.header("Database")
DEFAULT_DB = os.environ.get("SCALPEL_DB", os.path.join(os.path.dirname(__file__), "ScalpelDatabase.sqlite"))
db_path = st.sidebar.text_input("SQLite DB Path", value=DEFAULT_DB)

# make DB path available to all pages
st.session_state["db_path"] = db_path

st.sidebar.markdown("Navigate using the left sidebar menu (pages).")

# Display ERD PDF on main page
st.markdown("---")
st.subheader("Database Schema Overview")

# Check if ERD.pdf exists
erd_pdf_path = os.path.join(os.path.dirname(__file__), "docs", "ERD.pdf")
if os.path.exists(erd_pdf_path):
    try:
        # Open PDF and get first page
        pdf_document = fitz.open(erd_pdf_path)
        first_page = pdf_document[0]

        # Convert page to image
        pix = first_page.get_pixmap(matrix=fitz.Matrix(2, 2))  # 2x zoom for better quality
        img_data = pix.tobytes("png")

        # Convert to PIL Image and display
        image = Image.open(io.BytesIO(img_data))
        st.image(image, caption="ScalpelLab Database Entity Relationship Diagram", width='stretch')

        pdf_document.close()

    except Exception as e:
        st.error(f"Error loading ERD.pdf: {e}")
        st.info("Please make sure ERD.pdf is in the project directory.")
else:
    st.info("ERD.pdf not found. Please add your database ERD to display it here.")
