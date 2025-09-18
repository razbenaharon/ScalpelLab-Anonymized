import sqlite3

DB_PATH = r"F:\Room_8_Data\Scalpel_Raz\ScalpelDatabase.sqlite"
CAMERAS = [
    "Cart_Center_2","Cart_LT_4","Cart_RT_1",
    "General_3","Monitor","Patient_Monitor",
    "Ventilator_Monitor","Injection_Port"
]

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

rows = cur.execute("SELECT date_case, " + ", ".join(CAMERAS) + " FROM seq_status").fetchall()

all_paths = []  # flat list

for row in rows:
    date_case, *vals = row
    # split into YYYY-MM-DD and case number
    date, case_no = date_case.split("_")

    # turn YYYY-MM-DD into DATA_YY-MM-DD
    yy = date[2:4]
    mm = date[5:7]
    dd = date[8:10]
    data_folder = f"DATA_{yy}-{mm}-{dd}"
    case_folder = f"Case{case_no}"

    # collect all cameras with value 1
    channels = [cam for cam, v in zip(CAMERAS, vals) if v == 1]

    # build relative paths and extend the master list
    all_paths.extend([f"{data_folder}\\{case_folder}\\{cam}" for cam in channels])

conn.close()

# Example: print first 10
for p in all_paths[:10]:
    print(p)

# If you want the full list:
# print(all_paths)
