#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Summarize mp4_status table contents and print per-camera status distribution as a table.
"""

import argparse
import sqlite3
from collections import Counter

DEFAULT_DB_PATH = r"F:\Room_8_Data\Scalpel_Raz\ScalpelDatabase.sqlite"
DEFAULT_TABLE   = "mp4_status"

CAMERAS = [
    "Cart_Center_2","Cart_LT_4","Cart_RT_1",
    "General_3","Monitor","Patient_Monitor",
    "Ventilator_Monitor","Injection_Port"
]

def analyze_table(conn: sqlite3.Connection, table: str, cameras: list[str]) -> None:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    total_rows = cur.fetchone()[0]
    print(f"[INFO] Total date_case rows: {total_rows}\n")

    camera_stats = {cam: Counter() for cam in cameras}
    cur.execute(f"SELECT {', '.join(cameras)} FROM {table}")

    for row in cur.fetchall():
        for cam, st in zip(cameras, row):
            if st is not None:
                camera_stats[cam][st] += 1

    # Print as table
    print("[INFO] Per-camera status distribution:\n")
    header = f"{'Camera':20} | {'1 (>=200MB)':>10} | {'2 (<200MB)':>10} | {'3 (Missing)':>10}"
    print(header)
    print("-" * len(header))
    for cam in cameras:
        c = camera_stats[cam]
        print(f"{cam:20} | {c[1]:10} | {c[2]:10} | {c[3]:10}")

def main():
    ap = argparse.ArgumentParser(description="Summarize mp4_status table contents.")
    ap.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite DB")
    ap.add_argument("--table", default=DEFAULT_TABLE, help="Table name (default: mp4_status)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        analyze_table(conn, args.table, CAMERAS)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
