#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Execute a FULL SQL query against your status table and output matching MP4 file paths.

Your SQL must SELECT at least:
  - recording_date
  - case_no
  - camera_name
  - value (status)

Examples (PowerShell):
  # 1) Inline SQL
  python sql_to_path.py --sql "SELECT recording_date, case_no, camera_name, value FROM mp4_status WHERE camera_name='Monitor' AND value=1"

  # 2) SQL from file
  python sql_to_path.py --sql-file "F:\\Room_8_Data\\Scalpel_Raz\\queries\\monitor_good.sql"

  # Restrict to specific cameras and keep only the largest file
  python sql_to_path.py --sql "SELECT recording_date, case_no, camera_name, value FROM mp4_status WHERE camera_name IN ('Monitor', 'General_3') AND value=1" ^
                        --only-cameras Monitor,General_3 --largest-only --save-csv "F:\\good_paths.csv"

Notes:
- On PowerShell, prefer double quotes for --sql and escape inner quotes as needed.
- The script searches files under <ROOT>\\DATA_YY-MM-DD\\CaseN\\<Camera>\\*.mp4
"""

import argparse
import csv
import os
import re
import sqlite3
from pathlib import Path
from typing import Iterable, List, Tuple

# ------------ Defaults (edit if needed) ------------
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ScalpelDatabase.sqlite")
DEFAULT_ROOT    = r"F:\Room_8_Data\Recordings"

# All known camera columns (the script will only use the subset actually returned by your SQL)
CAMERAS: List[str] = [
    "Cart_Center_2","Cart_LT_4","Cart_RT_1",
    "General_3","Monitor","Patient_Monitor",
    "Ventilator_Monitor","Injection_Port"
]
# ---------------------------------------------------


def read_sql_from_args(args: argparse.Namespace) -> str:
    """Load SQL from --sql or --sql-file (mutually exclusive)."""
    if bool(args.sql) == bool(args.sql_file):
        raise SystemExit("[ERROR] Provide exactly one of --sql or --sql-file.")
    if args.sql_file:
        p = Path(args.sql_file)
        if not p.exists():
            raise SystemExit(f"[ERROR] SQL file not found: {p}")
        return p.read_text(encoding="utf-8")
    return args.sql


def data_dir_from_recording_date_and_case(recording_date: str, case_no: int) -> Tuple[str, str]:
    """'2023-02-05', 1 -> ('DATA_23-02-05', 'Case1')."""
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", recording_date)
    if not m:
        raise ValueError(f"Bad recording_date format: {recording_date}")
    yyyy, mm, dd = m.groups()
    yy = yyyy[2:]
    return f"DATA_{yy}-{mm}-{dd}", f"Case{case_no}"


def list_files_for_camera(root: Path, recording_date: str, case_no: int, camera: str, file_ext: str = "mp4") -> List[Path]:
    """Return list of files under <root>/DATA_YY-MM-DD/CaseN/<camera>/ recursively."""
    data_dir, case_dir = data_dir_from_recording_date_and_case(recording_date, case_no)
    cam_dir = root / data_dir / case_dir / camera
    if not cam_dir.exists():
        return []
    return [p for p in cam_dir.rglob(f"*.{file_ext}") if p.is_file()]


def pick_largest(paths: Iterable[Path]) -> List[Path]:
    """Return a 1-element list containing the largest path (or [] if none)."""
    paths = list(paths)
    if not paths:
        return []
    try:
        return [max(paths, key=lambda p: p.stat().st_size)]
    except OSError:
        readable = []
        for p in paths:
            try:
                _ = p.stat().st_size
                readable.append(p)
            except OSError:
                pass
        return [max(readable, key=lambda p: p.stat().st_size)] if readable else []


def run_sql(conn: sqlite3.Connection, sql_query: str) -> Tuple[List[str], List[tuple]]:
    """Execute full SQL and return (column_names, rows)."""
    cur = conn.cursor()
    rows = cur.execute(sql_query).fetchall()
    colnames = [d[0] for d in cur.description] if cur.description else []
    return colnames, rows

def get_paths(sql_query: str,
              db_path: str = DEFAULT_DB_PATH,
              root_path: str = DEFAULT_ROOT,
              status_value: int = 1,
              largest_only: bool = False,
              only_cameras: list[str] | None = None) -> list[tuple[str, int, str, str, float]]:
    """
    Run SQL query and return list of (recording_date, case_no, camera, mp4_path, size_mb).
    """
    root = Path(root_path)
    conn = sqlite3.connect(db_path)
    try:
        colnames, rows = run_sql(conn, sql_query)
        required_cols = ["recording_date", "case_no", "camera_name", "value"]
        if not rows or not all(col in colnames for col in required_cols):
            return []

        out_rows = []
        for row in rows:
            row_map = dict(zip(colnames, row))
            recording_date = row_map["recording_date"]
            case_no = row_map["case_no"]
            camera_name = row_map["camera_name"]
            value = row_map["value"]

            if int(value or 0) != status_value:
                continue

            if only_cameras and camera_name not in only_cameras:
                continue

            # Auto-detect file extension based on root path
            file_ext = "seq" if "Sequence_Backup" in str(root) else "mp4"

            # Try to find actual files
            files = list_files_for_camera(root, recording_date, case_no, camera_name, file_ext)

            if files:
                # Files exist - process them
                if largest_only:
                    files = pick_largest(files)
                for p in files:
                    try:
                        size_mb = round(p.stat().st_size / (1024 * 1024), 2)
                    except OSError:
                        size_mb = -1.0
                    out_rows.append((recording_date, case_no, camera_name, str(p), size_mb))
            else:
                # Files don't exist - return expected path
                data_dir, case_dir = data_dir_from_recording_date_and_case(recording_date, case_no)
                expected_path = root / data_dir / case_dir / camera_name
                expected_file_path = expected_path / f"*.{file_ext}"
                out_rows.append((recording_date, case_no, camera_name, str(expected_file_path), 0.0))
        return out_rows
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description="Run FULL SQL and output MP4 paths for matching cameras/status.")
    ap.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite DB.")
    ap.add_argument("--root", default=DEFAULT_ROOT, help="Root Recordings directory.")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", default="", help="Full SQL query string.")
    group.add_argument("--sql-file", default="", help="Path to a .sql file containing the full query.")
    ap.add_argument("--only-cameras", default="", help="Comma-separated list to restrict cameras.")
    ap.add_argument("--status-value", type=int, default=1, help="Which status value to match (default: 1).")
    ap.add_argument("--largest-only", action="store_true", help="Return only the largest MP4 per (recording_date,case_no,camera).")
    ap.add_argument("--save-csv", default="", help="Optional CSV output path.")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"[ERROR] Root path not found: {root}")

    sql_query = read_sql_from_args(args)

    conn = sqlite3.connect(args.db)
    try:
        colnames, rows = run_sql(conn, sql_query)
        if not rows:
            print("[INFO] Query returned no rows.")
            return

        required_cols = ["recording_date", "case_no", "camera_name", "value"]
        missing_cols = [col for col in required_cols if col not in colnames]
        if missing_cols:
            raise SystemExit(f"[ERROR] SQL must SELECT: {', '.join(missing_cols)}")

        # Camera restriction (optional)
        restrict = [c.strip() for c in args.only_cameras.split(",") if c.strip()]

        out_rows: List[Tuple[str, int, str, str, float]] = []  # (recording_date, case_no, camera, path_str, size_mb)

        # Iterate rows and emit paths for cameras whose status equals --status-value
        for row in rows:
            row_map = dict(zip(colnames, row))
            recording_date = row_map["recording_date"]
            case_no = row_map["case_no"]
            camera_name = row_map["camera_name"]
            value = row_map["value"]

            try:
                st_int = int(value) if value is not None else None
            except (TypeError, ValueError):
                st_int = None
            if st_int != args.status_value:
                continue

            if restrict and camera_name not in restrict:
                continue

            # Auto-detect file extension based on root path
            file_ext = "seq" if "Sequence_Backup" in str(root) else "mp4"

            files = list_files_for_camera(root, recording_date, case_no, camera_name, file_ext)
            if args.largest_only:
                files = pick_largest(files)

            if files:
                for p in files:
                    try:
                        size_mb = round(p.stat().st_size / (1024 * 1024), 2)
                    except OSError:
                        size_mb = -1.0
                    out_rows.append((recording_date, case_no, camera_name, str(p), size_mb))
            else:
                # Files don't exist - return expected path
                data_dir, case_dir = data_dir_from_recording_date_and_case(recording_date, case_no)
                expected_path = root / data_dir / case_dir / camera_name
                expected_file_path = expected_path / f"*.{file_ext}"
                out_rows.append((recording_date, case_no, camera_name, str(expected_file_path), 0.0))

        # Print results
        if not out_rows:
            print("[INFO] No matching MP4 files for the given SQL and options.")
        else:
            for recording_date, case_no, cam, path_str, size_mb in out_rows:
                print(f"{recording_date}\t{case_no}\t{cam}\t{size_mb} MB\t{path_str}")

        # Optional CSV
        if args.save_csv:
            out = Path(args.save_csv)
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["recording_date", "case_no", "camera", "mp4_path", "size_mb"])
                w.writerows(out_rows)
            print(f"[OK] Saved CSV with {len(out_rows)} rows -> {out}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
