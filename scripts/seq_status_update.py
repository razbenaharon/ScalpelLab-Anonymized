#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Update SQLite table `seq_status` based on presence/size of SEQ files per camera.

Status per camera:
  1 = at least one .seq >= THRESHOLD_MB
  2 = some .seq exist but all < THRESHOLD_MB
  3 = no .seq found
  4 = format problems (reserved for future use)

Expected directory layout:
  <ROOT>\DATA_YY-MM-DD\CaseN\<CameraName>\*.seq
  e.g. F:\Room_8_Data\Sequence_Backup\DATA_23-02-05\Case1\Cart_Center_2\Cart_Center_2.seq

The script:
  - Walks the tree and computes status per camera for each case
  - INSERT OR IGNORE the (recording_date, case_no) row
  - UPDATE the camera columns with the computed status
  - Shows changes and requires user confirmation before updating
"""

import argparse
import os
import re
import sqlite3
from pathlib import Path

# ---------- Defaults (edit if needed) ----------
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ScalpelDatabase.sqlite")
DEFAULT_ROOT    = r"F:\Room_8_Data\Sequence_Backup"
DEFAULT_TABLE   = "seq_status"
DEFAULT_THRESHOLD_MB = 200

CAMERAS = [
    "Cart_Center_2","Cart_LT_4","Cart_RT_1",
    "General_3","Monitor","Patient_Monitor",
    "Ventilator_Monitor","Injection_Port"
]
# ------------------------------------------------

def parse_recording_date_and_case(data_dir_name: str, case_dir_name: str) -> tuple[str, int] | None:
    """Convert DATA_YY-MM-DD + CaseN -> (YYYY-MM-DD, N) (e.g., DATA_23-02-05 + Case1 -> ('2023-02-05', 1))."""
    m = re.fullmatch(r"DATA_(\d{2})-(\d{2})-(\d{2})", data_dir_name)
    n = re.fullmatch(r"Case(\d+)", case_dir_name)
    if not m or not n:
        return None
    yy, mm, dd = m.groups()
    yyyy = f"20{yy}" if int(yy) <= 69 else f"19{yy}"
    case_no = int(n.group(1))
    return f"{yyyy}-{mm}-{dd}", case_no

def compute_camera_status(camera_dir: Path, threshold_bytes: int) -> tuple[int, int | None]:
    """
    Return (status, size_mb) for a single camera directory:
      - status: 1 if any .seq >= threshold, 2 if .seq exist but all < threshold, 3 if no .seq
      - size_mb: largest .seq file size in MB (None if no files found)
    """
    if not camera_dir.is_dir():
        return 3, None
    max_size = 0
    found_any = False
    # Search recursively (handles nested seq files)
    for p in camera_dir.rglob("*.seq"):
        if p.is_file():
            found_any = True
            try:
                sz = p.stat().st_size
            except OSError:
                continue
            if sz > max_size:
                max_size = sz
    if not found_any:
        return 3, None
    status = 1 if max_size >= threshold_bytes else 2
    size_mb = int(max_size / (1024 * 1024))
    return status, size_mb

def ensure_table_exists(conn: sqlite3.Connection, table: str) -> None:
    """
    Ensure the table exists with the new normalized structure.
    """
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            recording_date TEXT NOT NULL,
            case_no INTEGER NOT NULL,
            camera_name TEXT NOT NULL,
            value INTEGER,
            comments TEXT,
            "size(mb)" INTEGER,
            PRIMARY KEY (recording_date, case_no, camera_name)
        );
    """)
    conn.commit()

def get_existing_data(conn: sqlite3.Connection, table: str, recording_date: str, case_no: int) -> dict:
    """Get existing status values for a case, return dict {camera_name: (value, size_mb)}."""
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT camera_name, value, "size(mb)" FROM "{table}" WHERE recording_date = ? AND case_no = ?', (recording_date, case_no))
        rows = cur.fetchall()
        return {row[0]: (row[1], row[2]) for row in rows}
    except sqlite3.OperationalError:
        return {}

def upsert_camera_data(conn: sqlite3.Connection, table: str, recording_date: str, case_no: int, camera_name: str, value: int, size_mb: int | None) -> None:
    """Insert or update a single camera's data."""
    cur = conn.cursor()
    cur.execute(f'''
        INSERT OR REPLACE INTO "{table}"
        (recording_date, case_no, camera_name, value, "size(mb)")
        VALUES (?, ?, ?, ?, ?)
    ''', (recording_date, case_no, camera_name, value, size_mb))

def main():
    ap = argparse.ArgumentParser(description="Update seq_status (1/2/3) based on seq sizes per camera.")
    ap.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite DB path")
    ap.add_argument("--root", default=DEFAULT_ROOT, help="Root Sequence_Backup directory")
    ap.add_argument("--table", default=DEFAULT_TABLE, help="Table name (default: seq_status)")
    ap.add_argument("--threshold-mb", type=int, default=DEFAULT_THRESHOLD_MB, help="Size threshold in MB (default: 200)")
    ap.add_argument("--dry-run", action="store_true", help="Scan and print changes without writing to DB")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"[ERROR] Root path not found: {root}")

    threshold_bytes = args.threshold_mb * 1024 * 1024

    # Collect statuses per (recording_date, case_no, camera_name)
    updates: dict[tuple[str, int, str], tuple[int, int]] = {}

    for data_dir in root.iterdir():
        if not data_dir.is_dir() or not data_dir.name.startswith("DATA_"):
            continue
        for case_dir in data_dir.iterdir():
            if not case_dir.is_dir() or not case_dir.name.startswith("Case"):
                continue
            parsed = parse_recording_date_and_case(data_dir.name, case_dir.name)
            if not parsed:
                continue
            recording_date, case_no = parsed

            for cam in CAMERAS:
                cam_path = case_dir / cam
                status, size_mb = compute_camera_status(cam_path, threshold_bytes)
                updates[(recording_date, case_no, cam)] = (status, size_mb)

    if not updates:
        print("[WARN] Found no cases to update.")
        return

    # Show summary and check for changes
    print(f"[INFO] Found {len(updates)} cases under {root}")

    if args.dry_run:
        for (recording_date, case_no, camera_name), (status, size_mb) in sorted(updates.items()):
            print(f"  {recording_date} Case {case_no} {camera_name}: status={status}, size={size_mb}MB")
        return

    # Connect to DB to check existing values
    conn = sqlite3.connect(args.db)
    try:
        ensure_table_exists(conn, args.table)

        # Group updates by case to compare with existing data
        cases = {}
        for (recording_date, case_no, camera_name), (status, size_mb) in updates.items():
            case_key = (recording_date, case_no)
            if case_key not in cases:
                cases[case_key] = {}
            cases[case_key][camera_name] = (status, size_mb)

        # Check for changes
        changes = []
        new_cameras = []

        for (recording_date, case_no), new_data in cases.items():
            existing = get_existing_data(conn, args.table, recording_date, case_no)

            for camera_name, (new_status, new_size) in new_data.items():
                if camera_name not in existing:
                    new_cameras.append((recording_date, case_no, camera_name, new_status, new_size))
                else:
                    old_status, old_size = existing[camera_name]
                    if old_status != new_status or old_size != new_size:
                        changes.append((recording_date, case_no, camera_name, old_status, old_size, new_status, new_size))

        # Show what will be changed
        if new_cameras:
            print(f"\n[NEW] {len(new_cameras)} new camera entries will be added:")
            for recording_date, case_no, camera_name, status, size_mb in new_cameras:
                print(f"  {recording_date} Case {case_no} {camera_name}: status={status}, size={size_mb}MB")

        if changes:
            print(f"\n[CHANGES] {len(changes)} existing camera entries will be updated:")
            for recording_date, case_no, camera_name, old_status, old_size, new_status, new_size in changes:
                old_size_str = str(old_size) if old_size is not None else "NULL"
                new_size_str = str(new_size) if new_size is not None else "NULL"
                print(f"  {recording_date} Case {case_no} {camera_name}: status {old_status}->{new_status}, size {old_size_str}->{new_size_str}MB")

        if not new_cameras and not changes:
            print("\n[INFO] No changes detected. Database is already up to date.")
            return

        # Ask for confirmation
        print(f"\n[CONFIRM] This will update {len(new_cameras) + len(changes)} camera entries in the database.")
        response = input("Do you want to proceed? (y/N): ").strip().lower()

        if response not in ['y', 'yes']:
            print("[CANCELLED] Database update cancelled by user.")
            return

        # Write to DB
        for (recording_date, case_no, camera_name), (status, size_mb) in updates.items():
            upsert_camera_data(conn, args.table, recording_date, case_no, camera_name, status, size_mb)
        conn.commit()
        print(f"[OK] Updated '{args.table}' with {len(new_cameras) + len(changes)} camera entries (threshold {args.threshold_mb} MB).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()