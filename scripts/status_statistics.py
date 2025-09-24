#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Summarize `mp4_status` and `seq_status` tables with per-camera distributions.

Status meanings:
  mp4_status:
    1 = >=200MB, 2 = <200MB, 3 = Missing
  seq_status:
    1 = >200MB,  2 = <200MB, 3 = Missing, 4 = FORMAT PROBLEM (optional)
"""

import argparse
import sqlite3
from collections import Counter
import os

# Get the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Default DB path in the parent directory
DEFAULT_DB_PATH = os.path.join(os.path.dirname(BASE_DIR), "ScalpelDatabase.sqlite")

CAMERAS = [
    "Cart_Center_2","Cart_LT_4","Cart_RT_1",
    "General_3","Monitor","Patient_Monitor",
    "Ventilator_Monitor","Injection_Port"
]

LABELS_MP4 = {1: ">=200MB", 2: "<200MB", 3: "Missing"}
LABELS_SEQ = {1: ">200MB",  2: "<200MB", 3: "Missing", 4: "FORMAT PROBLEM"}

def fetch_camera_stats(conn: sqlite3.Connection, table: str, cameras: list[str]) -> tuple[int, dict]:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    total_rows = cur.fetchone()[0]

    camera_stats = {cam: Counter() for cam in cameras}
    cur.execute(f"SELECT {', '.join(cameras)} FROM {table}")
    for row in cur.fetchall():
        for cam, st in zip(cameras, row):
            if st is None:
                continue
            try:
                camera_stats[cam][int(st)] += 1
            except (TypeError, ValueError):
                # if some stray non-integer sneaks in
                pass

    return total_rows, camera_stats

def print_table(title: str, camera_stats: dict, labels: dict, status_order=(1,2,3,4)):
    # only show statuses that actually appear (but keep order)
    present = [s for s in status_order if any(camera_stats[c][s] for c in camera_stats)]
    if not present:  # fallback to declared labels if none counted
        present = [s for s in status_order if s in labels]

    widths = [20] + [14]*len(present)
    header_cells = ["Camera"] + [f"{s} ({labels.get(s,str(s))})" for s in present]
    header = " | ".join(
        [f"{header_cells[0]:<{widths[0]}}"] +
        [f"{h:>{w}}" for h, w in zip(header_cells[1:], widths[1:])]
    )
    print(f"\n[INFO] {title}\n")
    print(header)
    print("-" * len(header))

    # rows
    for cam in camera_stats:
        row = [f"{cam:<{widths[0]}}"] + [f"{camera_stats[cam][s]:>{widths[i+1]}}" for i, s in enumerate(present)]
        print(" | ".join(row))

    # totals
    totals = Counter()
    for cam in camera_stats:
        totals.update(camera_stats[cam])
    total_row = ["TOTAL"] + [str(totals[s]) for s in present]
    total_line = " | ".join(
        [f"{total_row[0]:<{widths[0]}}"] +
        [f"{val:>{widths[i+1]}}" for i, val in enumerate(total_row[1:])]
    )
    print("-" * len(header))
    print(total_line)

def main():
    ap = argparse.ArgumentParser(description="Print per-camera distributions for mp4_status and seq_status.")
    ap.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite DB")
    ap.add_argument("--mp4-table", default="mp4_status", help="mp4 table name")
    ap.add_argument("--seq-table", default="seq_status", help="seq table name")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        # mp4 table
        try:
            total_rows, mp4_stats = fetch_camera_stats(conn, args.mp4_table, CAMERAS)
            print(f"[INFO] Total date_case rows in {args.mp4_table}: {total_rows}")
            print_table(
                title=f"Per-camera status distribution ({args.mp4_table})",
                camera_stats=mp4_stats,
                labels=LABELS_MP4,
                status_order=(1,2,3),
            )
        except sqlite3.Error as e:
            print(f"[WARN] Could not read {args.mp4_table}: {e}")

        # seq table
        try:
            total_rows, seq_stats = fetch_camera_stats(conn, args.seq_table, CAMERAS)
            print(f"\n[INFO] Total date_case rows in {args.seq_table}: {total_rows}")
            print_table(
                title=f"Per-camera status distribution ({args.seq_table})",
                camera_stats=seq_stats,
                labels=LABELS_SEQ,
                status_order=(1,2,3,4),
            )
        except sqlite3.Error as e:
            print(f"[WARN] Could not read {args.seq_table}: {e}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
