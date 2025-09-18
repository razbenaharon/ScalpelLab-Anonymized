#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sqlite3
from pathlib import Path
from datetime import datetime
import re
import sys
from typing import List, Tuple

CAMERA_DIRS = [
    "Cart_Center_2",
    "Cart_LT_4",
    "Cart_RT_1",
    "General_3",
    "Monitor",
    "Patient_Monitor",
    "Ventilator_Monitor",
    "Injection_Port",
]

def parse_date_case(data_dir: Path, case_dir: Path) -> str:
    """
    Convert folders like DATA_23-02-09 and Case1 into date_case like 2023-02-09_1.
    """
    m = re.search(r"DATA_(\d{2})-(\d{2})-(\d{2})$", data_dir.name, flags=re.IGNORECASE)
    if not m:
        raise ValueError(f"Cannot parse date from folder name: {data_dir.name}")
    yy, mm, dd = m.group(1), m.group(2), m.group(3)
    # Interpret as 20yy, for years 00-99 -> 2000-2099
    year = int(yy)
    year += 2000
    try:
        date_str = datetime(year, int(mm), int(dd)).strftime("%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date in {data_dir.name}: {e}")

    m2 = re.search(r"Case\s*([0-9]+)$", case_dir.name, flags=re.IGNORECASE)
    if not m2:
        raise ValueError(f"Cannot parse case number from folder name: {case_dir.name}")
    case_no = int(m2.group(1))
    return f"{date_str}_{case_no}"

def ensure_table(conn: sqlite3.Connection, table: str):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            date_case          TEXT NOT NULL,
            camera_name        TEXT NOT NULL,
            mp4_count          INTEGER NOT NULL DEFAULT 0,
            total_size_bytes   INTEGER,
            first_mp4_path     TEXT,
            scanned_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
            PRIMARY KEY (date_case, camera_name)
        )
    """)
    conn.commit()

def scan_camera_dir(cam_dir: Path) -> Tuple[int, int, str]:
    """
    Return (count, total_size_bytes, first_path) for .mp4 files under cam_dir.
    """
    mp4s: List[Path] = [p for p in cam_dir.glob("*.mp4") if p.is_file()]
    count = len(mp4s)
    if count == 0:
        return 0, 0, None
    total = sum(p.stat().st_size for p in mp4s)
    # Pick the largest file as the "first" representative, this is usually the main video.
    first = max(mp4s, key=lambda p: p.stat().st_size)
    return count, total, str(first)

def upsert_inventory(conn: sqlite3.Connection, table: str, date_case: str, camera: str,
                     count: int, total_size: int, first_path: str):
    conn.execute(f"""
        INSERT INTO "{table}" (date_case, camera_name, mp4_count, total_size_bytes, first_mp4_path, scanned_at)
        VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%d %H:%M:%S','now'))
        ON CONFLICT(date_case, camera_name) DO UPDATE SET
            mp4_count = excluded.mp4_count,
            total_size_bytes = excluded.total_size_bytes,
            first_mp4_path = excluded.first_mp4_path,
            scanned_at = excluded.scanned_at
    """, (date_case, camera, count, total_size, first_path))
    conn.commit()

def maybe_sync_video_path(conn: sqlite3.Connection, sync: bool, date_case: str, camera: str,
                          count: int, first_path: str, total_size: int):
    if not sync:
        return
    # Check if video_path table exists
    cur = conn.execute("""
        SELECT name FROM sqlite_master WHERE type='table' AND name='video_path'
    """)
    if cur.fetchone() is None:
        return
    # Ensure basic columns exist, skip if not.
    # We attempt to add missing columns safely.
    # mp4_path, size_bytes, status columns are expected.
    try:
        conn.execute("ALTER TABLE video_path ADD COLUMN mp4_path TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE video_path ADD COLUMN size_bytes INTEGER")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE video_path ADD COLUMN status TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()

    status = "converted" if count > 0 else "missing"
    # Upsert into video_path without breaking its existing PK if present.
    conn.execute("""
        INSERT INTO video_path (date_case, camera_name, mp4_path, size_bytes, status)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(date_case, camera_name) DO UPDATE SET
            mp4_path = excluded.mp4_path,
            size_bytes = excluded.size_bytes,
            status = excluded.status
    """, (date_case, camera, first_path, total_size if count > 0 else None, status))
    conn.commit()

def scan(root: Path, db_path: Path, table: str, sync_video_path: bool):
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_table(conn, table)
        data_dirs = [p for p in root.iterdir() if p.is_dir() and re.match(r"DATA_\d{2}-\d{2}-\d{2}$", p.name, flags=re.IGNORECASE)]
        data_dirs.sort()
        total_rows = 0

        for ddir in data_dirs:
            case_dirs = [p for p in ddir.iterdir() if p.is_dir() and re.match(r"Case[0-9]+$", p.name, flags=re.IGNORECASE)]
            case_dirs.sort(key=lambda p: int(re.search(r"([0-9]+)$", p.name).group(1)))
            for cdir in case_dirs:
                try:
                    date_case = parse_date_case(ddir, cdir)
                except ValueError as e:
                    print(f"[WARN] {e}", file=sys.stderr)
                    continue

                for cam in CAMERA_DIRS:
                    cam_dir = cdir / cam
                    if not cam_dir.exists() or not cam_dir.is_dir():
                        # No camera folder, record as zero files
                        upsert_inventory(conn, table, date_case, cam, 0, 0, None)
                        maybe_sync_video_path(conn, sync_video_path, date_case, cam, 0, None, 0)
                        total_rows += 1
                        continue

                    count, total, first_path = scan_camera_dir(cam_dir)
                    upsert_inventory(conn, table, date_case, cam, count, total, first_path)
                    maybe_sync_video_path(conn, sync_video_path, date_case, cam, count, first_path, total)
                    total_rows += 1

        print(f"Done. Wrote or updated {total_rows} rows into table '{table}' in {db_path}.")
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Scan MP4 files under recordings folder into SQLite.")
    parser.add_argument("root_dir", type=str, help="Path to the 'recordings' directory that contains DATA_date folders.")
    parser.add_argument("sqlite_db", type=str, help="Path to the SQLite database file. It will be created if missing.")
    parser.add_argument("--table", type=str, default="mp4_inventory", help="Destination table name. Default is mp4_inventory.")
    parser.add_argument("--sync-video-path", action="store_true",
                        help="Also upsert into existing video_path table if present.")
    args = parser.parse_args()

    root = Path(args.root_dir).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"Root directory does not exist or is not a directory: {root}", file=sys.stderr)
        sys.exit(2)

    db_path = Path(args.sqlite_db).expanduser().resolve()
    try:
        scan(root, db_path, args.table, args.sync_video_path)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
