#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Update SQLite table `mp4_status` based on presence/size of MP4s per camera.

Status per camera:
  1 = at least one .mp4 >= THRESHOLD_MB
  2 = some .mp4 exist but all < THRESHOLD_MB
  3 = no .mp4 found

Expected directory layout:
  <ROOT>\DATA_YY-MM-DD\CaseN\<CameraName>\*.mp4
  e.g. F:\Room_8_Data\Recordings\DATA_23-02-05\Case1\Cart_Center_2\Cart_Center_2_4.mp4

The script:
  - Walks the tree and computes status per camera for each case
  - INSERT OR IGNORE the date_case row
  - UPDATE the camera columns with the computed status
"""

import argparse
import os
import re
import sqlite3
from pathlib import Path

# ---------- Defaults (edit if needed) ----------
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ScalpelDatabase.sqlite")
DEFAULT_ROOT    = r"F:\Room_8_Data\Recordings"
DEFAULT_TABLE   = "mp4_status"
DEFAULT_THRESHOLD_MB = 200

CAMERAS = [
    "Cart_Center_2","Cart_LT_4","Cart_RT_1",
    "General_3","Monitor","Patient_Monitor",
    "Ventilator_Monitor","Injection_Port"
]
# ------------------------------------------------

def parse_date_case(data_dir_name: str, case_dir_name: str) -> str | None:
    """Convert DATA_YY-MM-DD + CaseN -> YYYY-MM-DD_N (e.g., DATA_23-02-05 + Case1 -> 2023-02-05_1)."""
    m = re.fullmatch(r"DATA_(\d{2})-(\d{2})-(\d{2})", data_dir_name)
    n = re.fullmatch(r"Case(\d+)", case_dir_name)
    if not m or not n:
        return None
    yy, mm, dd = m.groups()
    yyyy = f"20{yy}" if int(yy) <= 69 else f"19{yy}"
    return f"{yyyy}-{mm}-{dd}_{n.group(1)}"

def compute_camera_status(camera_dir: Path, threshold_bytes: int) -> int:
    """
    Return 1/2/3 status for a single camera directory:
      - 1 if any .mp4 >= threshold
      - 2 if .mp4 exist but all < threshold
      - 3 if no .mp4
    """
    if not camera_dir.is_dir():
        return 3
    max_size = 0
    found_any = False
    # Search recursively (handles nested exports)
    for p in camera_dir.rglob("*.mp4"):
        if p.is_file():
            found_any = True
            try:
                sz = p.stat().st_size
            except OSError:
                continue
            if sz > max_size:
                max_size = sz
            if max_size >= threshold_bytes:
                # Early exit if we already hit 1
                return 1
    if not found_any:
        return 3
    return 1 if max_size >= threshold_bytes else 2

def ensure_table_columns(conn: sqlite3.Connection, table: str, cameras: list[str]) -> None:
    """
    Ensure the `mp4_status` table exists with date_case + camera columns.
    If your table already exists, missing camera columns will be added.
    """
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            date_case TEXT PRIMARY KEY
        );
    """)
    cur.execute(f"PRAGMA table_info('{table}')")
    existing = {row[1] for row in cur.fetchall()}
    to_add = [c for c in cameras if c not in existing]
    for col in to_add:
        cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" INTEGER')
    conn.commit()

def upsert_row(conn: sqlite3.Connection, table: str, date_case: str, status_map: dict) -> None:
    """INSERT OR IGNORE a row, then UPDATE camera columns with computed statuses."""
    cur = conn.cursor()
    cur.execute(f'INSERT OR IGNORE INTO "{table}" (date_case) VALUES (?)', (date_case,))
    sets = []
    vals = []
    for cam, val in status_map.items():
        sets.append(f'"{cam}" = ?')
        vals.append(int(val))
    vals.append(date_case)
    cur.execute(f'UPDATE "{table}" SET {", ".join(sets)} WHERE date_case = ?', vals)

def main():
    ap = argparse.ArgumentParser(description="Update mp4_status (1/2/3) based on mp4 sizes per camera.")
    ap.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite DB path")
    ap.add_argument("--root", default=DEFAULT_ROOT, help="Root Recordings directory")
    ap.add_argument("--table", default=DEFAULT_TABLE, help="Table name (default: mp4_status)")
    ap.add_argument("--threshold-mb", type=int, default=DEFAULT_THRESHOLD_MB, help="Size threshold in MB (default: 200)")
    ap.add_argument("--dry-run", action="store_true", help="Scan and print changes without writing to DB")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"[ERROR] Root path not found: {root}")

    threshold_bytes = args.threshold_mb * 1024 * 1024

    # Collect statuses per date_case
    updates: dict[str, dict[str, int]] = {}

    for data_dir in root.iterdir():
        if not data_dir.is_dir() or not data_dir.name.startswith("DATA_"):
            continue
        for case_dir in data_dir.iterdir():
            if not case_dir.is_dir() or not case_dir.name.startswith("Case"):
                continue
            date_case = parse_date_case(data_dir.name, case_dir.name)
            if not date_case:
                continue

            status_map = {}
            for cam in CAMERAS:
                cam_path = case_dir / cam
                status_map[cam] = compute_camera_status(cam_path, threshold_bytes)
            updates[date_case] = status_map

    if not updates:
        print("[WARN] Found no cases to update.")
        return

    # Show summary
    print(f"[INFO] Found {len(updates)} date_case entries under {root}")
    if args.dry_run:
        for dc in sorted(updates):
            statuses = ", ".join(f"{k}={v}" for k, v in updates[dc].items())
            print(f"  {dc}: {statuses}")
        return

    # Write to DB
    conn = sqlite3.connect(args.db)
    try:
        ensure_table_columns(conn, args.table, CAMERAS)
        for dc, sm in updates.items():
            upsert_row(conn, args.table, dc, sm)
        conn.commit()
        print(f"[OK] Updated '{args.table}' with {len(updates)} rows (threshold {args.threshold_mb} MB).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
