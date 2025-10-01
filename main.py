from scripts.sql_to_path import get_paths

# Example: Get Monitor and Patient_Monitor recordings from February 2023
sql_query = """
    SELECT recording_date, case_no, camera_name, value
    FROM mp4_status
    WHERE recording_date LIKE '2023-02-%'
    AND camera_name IN ('Monitor', 'Patient_Monitor')
    AND value = 1
"""

paths = get_paths(sql_query)
print(f"Found {len(paths)} matching paths:")

for recording_date, case_no, camera, mp4_path, size_mb in paths:
    print(f"{recording_date}\t{case_no}\t{camera}\t{size_mb} MB\t{mp4_path}")

# Example 2: Get all recordings with value=3 for a specific date
print("\n" + "="*60)
print("Example 2: All recordings with status=3 for 2023-01-18")
print("="*60)

sql_query2 = """
    SELECT recording_date, case_no, camera_name, value
    FROM mp4_status
    WHERE recording_date = '2023-01-18'
    AND value = 3
"""

paths2 = get_paths(sql_query2, status_value=3)
print(f"Found {len(paths2)} matching paths:")

for recording_date, case_no, camera, mp4_path, size_mb in paths2:
    print(f"{recording_date}\t{case_no}\t{camera}\t{size_mb} MB\t{mp4_path}")

# Example 3: Show only largest file per camera for Monitor cameras
print("\n" + "="*60)
print("Example 3: Largest Monitor files for February 2023")
print("="*60)

sql_query3 = """
    SELECT recording_date, case_no, camera_name, value
    FROM mp4_status
    WHERE recording_date LIKE '2023-02-%'
    AND camera_name = 'Monitor'
    AND value = 1
"""

paths3 = get_paths(sql_query3, largest_only=True)
print(f"Found {len(paths3)} matching paths:")

for recording_date, case_no, camera, mp4_path, size_mb in paths3:
    print(f"{recording_date}\t{case_no}\t{camera}\t{size_mb} MB\t{mp4_path}")
