#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Execute a FULL SQL query against your status table and output matching MP4 file paths.

Your SQL must SELECT at least:
  - date_case
  - One or more camera columns with status values (1/2/3[/4])

Examples (PowerShell):
  # 1) Inline SQL
  python sql_to_path.py --sql "SELECT date_case, Monitor, Patient_Monitor FROM mp4_status WHERE Monitor=1"

  # 2) SQL from file
  python sql_to_path.py --sql-file "F:\\Room_8_Data\\Scalpel_Raz\\queries\\monitor_good.sql"

  # Restrict to specific cameras and keep only the largest file
  python sql_to_path.py --sql "SELECT date_case, Monitor, General_3 FROM mp4_status WHERE Monitor=1" ^
                        --only-cameras Monitor,General_3 --largest-only --save-csv "F:\\good_paths.csv"

Notes:
- On PowerShell, prefer double quotes for --sql and escape inner quotes as needed.
- The script searches files under <ROOT>\\DATA_YY-MM-DD\\CaseN\\<Camera>\\*.mp4
"""

import argparse
import csv
import re
import sqlite3
from pathlib import Path
from typing import Iterable, List, Tuple

# ------------ Defaults (edit if needed) ------------
DEFAULT_DB_PATH = r"F:\Room_8_Data\Scalpel_Raz\ScalpelDatabase.sqlite"
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


def data_dir_from_date_case(date_case: str) -> Tuple[str, str]:
    """'2023-02-05_1' -> ('DATA_23-02-05', 'Case1')."""
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})_(\d+)", date_case)
    if not m:
        raise ValueError(f"Bad date_case format: {date_case}")
    yyyy, mm, dd, case = m.groups()
    yy = yyyy[2:]
    return f"DATA_{yy}-{mm}-{dd}", f"Case{case}"


def list_mp4s_for_camera(root: Path, date_case: str, camera: str) -> List[Path]:
    """Return list of *.mp4 under <root>/DATA_YY-MM-DD/CaseN/<camera>/ recursively."""
    data_dir, case_dir = data_dir_from_date_case(date_case)
    cam_dir = root / data_dir / case_dir / camera
    if not cam_dir.exists():
        return []
    return [p for p in cam_dir.rglob("*.mp4") if p.is_file()]


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
              only_cameras: list[str] | None = None) -> list[tuple[str, str, str, float]]:
    """
    Run SQL query and return list of (date_case, camera, mp4_path, size_mb).
    """
    root = Path(root_path)
    conn = sqlite3.connect(db_path)
    try:
        colnames, rows = run_sql(conn, sql_query)
        if not rows or "date_case" not in colnames:
            return []

        returned_cameras = [c for c in CAMERAS if c in colnames]
        if only_cameras:
            returned_cameras = [c for c in returned_cameras if c in only_cameras]

        out_rows = []
        for row in rows:
            row_map = dict(zip(colnames, row))
            dc = row_map["date_case"]
            for cam in returned_cameras:
                st = row_map.get(cam)
                if int(st or 0) != status_value:
                    continue
                mp4s = list_mp4s_for_camera(root, dc, cam)
                if largest_only:
                    mp4s = pick_largest(mp4s)
                for p in mp4s:
                    try:
                        size_mb = round(p.stat().st_size / (1024 * 1024), 2)
                    except OSError:
                        size_mb = -1.0
                    out_rows.append((dc, cam, str(p), size_mb))
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
    ap.add_argument("--largest-only", action="store_true", help="Return only the largest MP4 per (date_case,camera).")
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
        if "date_case" not in colnames:
            raise SystemExit("[ERROR] SQL must SELECT 'date_case'.")

        # Cameras actually present in the SQL result:
        returned_cameras = [c for c in CAMERAS if c in colnames]
        if not returned_cameras:
            raise SystemExit("[ERROR] SQL must SELECT at least one camera column: " + ", ".join(CAMERAS))

        # Camera restriction (optional)
        restrict = [c.strip() for c in args.only_cameras.split(",") if c.strip()]
        if restrict:
            unknown = [c for c in restrict if c not in returned_cameras]
            if unknown:
                raise SystemExit(f"[ERROR] --only-cameras contains columns not in SQL result: {unknown}\n"
                                 f"Returned cameras: {returned_cameras}")
            cameras_to_check = restrict
        else:
            cameras_to_check = returned_cameras

        out_rows: List[Tuple[str, str, str, float]] = []  # (date_case, camera, path_str, size_mb)

        # Iterate rows and emit paths for cameras whose status equals --status-value
        for row in rows:
            row_map = dict(zip(colnames, row))
            dc = row_map["date_case"]

            for cam in cameras_to_check:
                st = row_map.get(cam)
                try:
                    st_int = int(st) if st is not None else None
                except (TypeError, ValueError):
                    st_int = None
                if st_int != args.status_value:
                    continue

                mp4s = list_mp4s_for_camera(root, dc, cam)
                if args.largest_only:
                    mp4s = pick_largest(mp4s)

                for p in mp4s:
                    try:
                        size_mb = round(p.stat().st_size / (1024 * 1024), 2)
                    except OSError:
                        size_mb = -1.0
                    out_rows.append((dc, cam, str(p), size_mb))

        # Print results
        if not out_rows:
            print("[INFO] No matching MP4 files for the given SQL and options.")
        else:
            for dc, cam, path_str, size_mb in out_rows:
                print(f"{dc}\t{cam}\t{size_mb} MB\t{path_str}")

        # Optional CSV
        if args.save_csv:
            out = Path(args.save_csv)
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["date_case", "camera", "mp4_path", "size_mb"])
                w.writerows(out_rows)
            print(f"[OK] Saved CSV with {len(out_rows)} rows -> {out}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
