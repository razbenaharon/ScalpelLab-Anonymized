# ScalpelLab Database Manager

A Streamlit-based database management system for managing and monitoring surgical recording data, including MP4 video files and SEQ sequence files from multiple camera sources.

## Features

### 📊 Streamlit Web Interface
- **Browse**: Query and explore database tables with search functionality
- **Edit**: Add, modify, and manage database records through interactive forms
- **Status Summary**: View MP4/SEQ files statistics per camera and distributions
- **Views**: Access and query database views for specialized data perspectives


### Running the Web Interface
```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

### Database Configuration
Set the database path in the sidebar to the ScalpelDatabase.sqlite file in the project directory


### 🎥 Video File Management
- **MP4 Status Tracking**: Monitor exported MP4 files per camera
- **SEQ Status Tracking**: Track original sequence files per camera
- **Automatic Status Updates**: Scripts to scan directories and update database status
- **Smart File Cleanup**: Delete small/incomplete MP4 files to free up space

### 🗄️ Database Schema
- **recording_details**: Core table for recording metadata
- **anesthetic**: Anesthetic-related information
- **mp4_status**: Normalized table tracking MP4 file status per camera
- **seq_status**: Normalized table tracking SEQ file status per camera


## Project Structure

```
ScalpeLab/
├── app.py                          # Main Streamlit application
├── main.py                         # Example usage of sql_to_path.py
├── utils.py                        # Database utility functions
├── pages/                          # Streamlit pages
│   ├── 1_Browse.py                # Browse database tables
│   ├── 2_Edit.py                  # Edit database records
│   ├── 3_Status_Summary.py        # MP4/SEQ status dashboard
│   └── 4_Views.py                 # Database views browser
├── scripts/                        # Command-line utilities
│   ├── mp4_status_update.py       # Update MP4 file status
│   ├── seq_status_update.py       # Update SEQ file status
│   ├── seq_exporter.py            # Export SEQ to MP4
│   ├── sqlite_to_dbdiagram.py     # Generate DB diagram
│   ├── status_statistics.py       # Generate status statistics and reports
│   └── sql_to_path.py             # SQL query to file path mapping utility
├── docs/                           # Documentation (if exists)
│   ├── ERD.pdf                    # Entity relationship diagram
│   └── scalpel_dbdiagram.txt      # Database schema definition
└── ScalpelDatabase.sqlite         # SQLite database file
```

## Database Tables

### recording_details
Core table storing metadata for each surgical recording session.

| Column | Type | Required | Description                                                                               |
|--------|------|----------|-------------------------------------------------------------------------------------------|
| `recording_date` | TEXT | ✓ | Date of recording (YYYY-MM-DD format)                                                     |
| `signature_time` | TEXT | | Time when recording was signed/validated                                                  |
| `case_no` | INTEGER | ✓ | Case number for the recording date (1, 2, 3, etc.)                                        |
| `code` | TEXT | | Anesthetic name code                                                                      |
| `anesthetic_key` | INTEGER | ✓| Foreign key linking to anesthetic table                                                   |
| `months_anesthetic_recording` | INTEGER | | Months of anesthetic experience at time of recording - Auto inserted                      |
| `anesthetic_attending` | TEXT | | Anesthetist level at time of recording ('A' = Attending, 'R' = Resident) - Auto inserted  |

**Primary Key**: `(recording_date, case_no)`

### anesthetic
Table storing information about anesthetists and their career progression.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `anesthetic_key` | INTEGER | | Primary key, auto-increment |
| `name` | TEXT | ✓ | Full name of the anesthetist |
| `code` | TEXT | | Short code/identifier for the anesthetist |
| `anesthetic_start_date` | TEXT | | Date when anesthetic training/career started (YYYY-MM-DD) |
| `grade_a_date` | TEXT | | Date when promoted to Grade A/Attending level |

### mp4_status
Normalized table tracking the status of exported MP4 video files for each camera per recording.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `recording_date` | TEXT | ✓ | Date of recording (YYYY-MM-DD format) |
| `case_no` | INTEGER | ✓ | Case number for the recording date |
| `camera_name` | TEXT | ✓ | Name of the camera (see Camera Configuration) |
| `value` | INTEGER | | Status code (1=Complete, 2=Incomplete, 3=Missing) |
| `comments` | TEXT | | Additional notes about the MP4 status |
| `size_mb` | INTEGER | | Total size of MP4 files in megabytes |

**Primary Key**: `(recording_date, case_no, camera_name)`

#### MP4 value
- **1**: At least one MP4 file >= 200MB (complete)
- **2**: MP4 files exist but all < 200MB (incomplete)
- **3**: No MP4 files found (missing)
- 
### seq_status
Normalized table tracking the status of original SEQ sequence files for each camera per recording.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `recording_date` | TEXT | ✓ | Date of recording (YYYY-MM-DD format) |
| `case_no` | INTEGER | ✓ | Case number for the recording date |
| `camera_name` | TEXT | ✓ | Name of the camera (see Camera Configuration) |
| `value` | INTEGER | | Status code (1=Complete, 2=Incomplete, 3=Missing) |
| `comments` | TEXT | | Additional notes about the SEQ status |
| `size_mb` | INTEGER | | Total size of SEQ files in megabytes |

**Primary Key**: `(recording_date, case_no, camera_name)`

#### SEQ value
- **1**: At least one SEQ file > 200MB (complete)
- **2**: SEQ files exist but all < 200MB (incomplete)
- **3**: No SEQ files found (missing)



### analysis_information
Table for storing analysis and labeling information.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `recording_date` | TEXT | | Date of recording being analyzed |
| `case_no` | INTEGER | | Case number being analyzed |
| `label_by` | TEXT | | Person/system who performed the labeling |

**Primary Key**: `(recording_date, case_no)`

## Database Relationships

- `recording_details.anesthetic_key` → `anesthetic.anesthetic_key` (Foreign Key)
- `mp4_status.(recording_date, case_no)` → `recording_details.(recording_date, case_no)` (Logical relationship)
- `seq_status.(recording_date, case_no)` → `recording_details.(recording_date, case_no)` (Logical relationship)
- `analysis_information.(recording_date, case_no)` → `recording_details.(recording_date, case_no)` (Logical relationship)

## Camera Configuration

The system tracks 8 camera sources:
- Cart_Center_2
- Cart_LT_4
- Cart_RT_1
- General_3
- Monitor
- Patient_Monitor
- Ventilator_Monitor
- Injection_Port


## Directory Structure Expected

### Recordings (MP4 files)
```
F:\Room_8_Data\Recordings\
└── DATA_YY-MM-DD\
    └── CaseN\
        └── <CameraName>\
            └── *.mp4
```

### Sequence Backups (SEQ files)
```
F:\Room_8_Data\Sequence_Backup\
└── DATA_YY-MM-DD\
    └── CaseN\
        └── <CameraName>\
            └── *.seq
```


## Database Views

The system provides predefined views to simplify complex queries and highlight important conditions.

### cur_mp4_missing 
Checks if any MP4 file is **missing** (`status = 3`) **while the corresponding SEQ file exists** with status `1` (complete) or `2` (incomplete).  
This view helps quickly identify recordings where the original SEQ file is present but the MP4 export is missing or failed.

### cur_seq_missing
Checks if any SEQ file is **missing** (`status = 3`) **while the corresponding MP4 file exists** with status `1` (complete) or `2` (incomplete).

### cur_seniority
**Purpose**: Calculates current seniority and attending status for each anesthetist based on their start date.

**Key Columns**:
- `seniority_month_cur`: Months of experience from `anesthetic_start_date` until now
- `anesthetic_attending_cur`: Current level ('A' = Attending if >60 months, 'R' = Resident if ≤60 months)

**Business Logic**:
- Anesthetists with >60 months (5 years) of experience are considered Attending level
- Those with ≤60 months are considered Resident level
- This view is used to dynamically determine current status without manual updates

## Scripts


#### Update MP4 Status
Scan the Recordings directory and update `mp4_status` table:


#### Update SEQ Status
Scan the Sequence_Backup directory and update `seq_status` table:


#### Export SEQ Files to MP4
Export sequence files to MP4 format:



#### Query Files by SQL (sql_to_path.py)
Execute SQL queries against the database and get corresponding file paths
