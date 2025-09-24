from scripts.sql_to_path import get_paths

sql_query = """
    SELECT date_case, Monitor, Patient_Monitor
    FROM mp4_status
    WHERE date_case LIKE '2023-02-%'
"""

paths = get_paths(sql_query)

for row in paths:
    print(row)
