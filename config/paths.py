"""
ScalpeLab Project Path Manager
Centralized configuration for all file paths in the project.
Modify paths here and all scripts will use the updated locations.
"""

import os
from pathlib import Path

class ProjectPaths:
    """Centralized path management for ScalpeLab project"""

    def __init__(self):
        # Project root directory
        self.PROJECT_ROOT = Path(__file__).parent.parent.absolute()

        # Core directories
        self.CONFIG_DIR = self.PROJECT_ROOT / "config"
        self.SCRIPTS_DIR = self.PROJECT_ROOT / "scripts"
        self.DATA_DIR = self.PROJECT_ROOT / "data"
        self.DOCS_DIR = self.PROJECT_ROOT / "docs"
        self.PAGES_DIR = self.PROJECT_ROOT / "pages"

        # Script subdirectories
        self.DATABASE_SCRIPTS = self.SCRIPTS_DIR / "database"
        self.EXPORT_SCRIPTS = self.SCRIPTS_DIR / "export"

        # Database files
        self.DATABASE_FILE = self.DATA_DIR / "ScalpelDatabase.sqlite"
        self.BACKUP_DATABASE = self.DATA_DIR / "ScalpelDatabase_backup.sqlite"

        # Schema files
        self.SCHEMA_FILE = self.PROJECT_ROOT / "schema.sql"
        self.SCHEMA_UPDATED = self.PROJECT_ROOT / "schema_updated.sql"

        # Output files
        self.ERD_PDF = self.DOCS_DIR / "ERD.pdf"
        self.ERD_JPG_STYLE = self.DOCS_DIR / "scalpel_database_erd_dbdiagram_style.jpg"
        self.ERD_JPG_BASIC = self.DOCS_DIR / "scalpel_database_erd.jpg"
        self.DBDIAGRAM_OUTPUT = self.DOCS_DIR / "scalpel_dbdiagram.txt"

        # Utility files
        self.UTILS_FILE = self.PROJECT_ROOT / "utils.py"

        # Data files
        self.DATA_SUMMARY_GUIDE = self.DATA_DIR / "Data_summary_guide.xlsx"

        # Script files
        self.SQLITE_TO_DBDIAGRAM_SCRIPT = self.EXPORT_SCRIPTS / "sqlite_to_dbdiagram.py"
        self.STATUS_STATISTICS_SCRIPT = self.DATABASE_SCRIPTS / "status_statistics.py"
        self.SEQ_EXPORTER_SCRIPT = self.DATABASE_SCRIPTS / "seq_exporter.py"
        self.MP4_STATUS_UPDATE_SCRIPT = self.DATABASE_SCRIPTS / "mp4_status_update.py"
        self.DEDUPE_EXPORTS_SCRIPT = self.DATABASE_SCRIPTS / "dedupe_exports.py"
        self.SQL_TO_PATH_SCRIPT = self.DATABASE_SCRIPTS / "sql_to_path.py"

        # Main application files
        self.MAIN_APP = self.PROJECT_ROOT / "app.py"
        self.MAIN_SCRIPT = self.PROJECT_ROOT / "main.py"

    def ensure_directories(self):
        """Create all necessary directories if they don't exist"""
        directories = [
            self.CONFIG_DIR,
            self.SCRIPTS_DIR,
            self.DATA_DIR,
            self.DOCS_DIR,
            self.DATABASE_SCRIPTS,
            self.EXPORT_SCRIPTS
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    def get_relative_path(self, path):
        """Get path relative to project root"""
        return os.path.relpath(path, self.PROJECT_ROOT)

    def __str__(self):
        """String representation showing all configured paths"""
        paths_info = []
        paths_info.append("=== ScalpeLab Project Paths ===")
        paths_info.append(f"Project Root: {self.PROJECT_ROOT}")
        paths_info.append(f"Database File: {self.DATABASE_FILE}")
        paths_info.append(f"ERD PDF: {self.ERD_PDF}")
        paths_info.append(f"Scripts Directory: {self.SCRIPTS_DIR}")
        paths_info.append(f"Data Directory: {self.DATA_DIR}")
        paths_info.append(f"Documentation: {self.DOCS_DIR}")
        return "\n".join(paths_info)

# Global instance for easy importing
paths = ProjectPaths()

# Convenience functions
def get_database_path():
    """Get the main database file path"""
    return str(paths.DATABASE_FILE)

def get_erd_pdf_path():
    """Get the ERD PDF file path"""
    return str(paths.ERD_PDF)

def get_dbdiagram_output_path():
    """Get the dbdiagram output file path"""
    return str(paths.DBDIAGRAM_OUTPUT)

# Removed get_visualization_script_path as visualization folder was removed

def setup_project_structure():
    """Setup the complete project directory structure"""
    paths.ensure_directories()
    print("Project directory structure created successfully!")
    print(paths)

if __name__ == "__main__":
    setup_project_structure()