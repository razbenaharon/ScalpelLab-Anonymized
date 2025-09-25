from scripts.sql_to_path import get_paths

sql_query = """
    SELECT recording_date, case_no, Monitor, Patient_Monitor
    FROM mp4_status
    WHERE recording_date LIKE '2023-02-%'
"""

paths = get_paths(sql_query)
print(f"Found {len(paths)} matching paths:")

for recording_date, case_no, camera, mp4_path, size_mb in paths:
    print(f"{recording_date}\t{case_no}\t{camera}\t{size_mb} MB\t{mp4_path}")
