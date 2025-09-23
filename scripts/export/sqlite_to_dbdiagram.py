#!/usr/bin/env python3
"""
SQLite to dbdiagram.io converter
Converts SQLite database to dbdiagram.io format for easy visualization
"""

import sqlite3
import re
import sys
import os
from datetime import date
from typing import List, Dict, Tuple

def parse_foreign_keys_from_sql(create_sql: str) -> List[Tuple[str, str, str]]:
    """Extract foreign key relationships from CREATE TABLE SQL"""
    fk_pattern = r'FOREIGN KEY\s*\(\s*([^)]+)\s*\)\s*REFERENCES\s*["\']?([^"\'(\s]+)["\']?\s*(?:\(\s*([^)]+)\s*\))?'
    matches = re.findall(fk_pattern, create_sql, re.IGNORECASE)

    foreign_keys = []
    for match in matches:
        local_col = match[0].strip().strip('"').strip("'")
        ref_table = match[1].strip().strip('"').strip("'")
        ref_col = match[2].strip().strip('"').strip("'") if match[2] else None
        foreign_keys.append((local_col, ref_table, ref_col))

    return foreign_keys

def sqlite_to_dbdiagram(db_path: str, output_path: str):
    """Convert SQLite database to dbdiagram.io format"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all tables including sqlite_sequence
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = [row[0] for row in cursor.fetchall()]

    # Regular tables (excluding sqlite_sequence)
    tables = [t for t in all_tables if t != 'sqlite_sequence']

    # Store table information
    table_schemas = {}
    all_foreign_keys = {}

    # Add sqlite_sequence if it exists
    if 'sqlite_sequence' in all_tables:
        table_schemas['sqlite_sequence'] = [
            (0, 'name', 'varchar', 0, None, 0),
            (1, 'seq', 'varchar', 0, None, 0)
        ]

    for table in tables:
        # Get table schema
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        table_schemas[table] = columns

        # Get foreign keys using PRAGMA foreign_key_list (more reliable)
        cursor.execute(f"PRAGMA foreign_key_list({table})")
        pragma_fks = cursor.fetchall()

        foreign_keys = []
        for fk in pragma_fks:
            # fk structure: (id, seq, table, from, to, on_update, on_delete, match)
            local_col = fk[3]  # from column
            ref_table = fk[2]  # referenced table
            ref_col = fk[4]    # to column
            foreign_keys.append((local_col, ref_table, ref_col))

        # If PRAGMA didn't find any, try parsing CREATE TABLE SQL as fallback
        if not foreign_keys:
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
            create_sql = cursor.fetchone()
            if create_sql and create_sql[0]:
                foreign_keys = parse_foreign_keys_from_sql(create_sql[0])

        if foreign_keys:
            all_foreign_keys[table] = foreign_keys

    conn.close()

    # Table notes mapping
    table_notes = {
        'sqlite_sequence': 'SQLite internal autoincrement tracking table',
        'analysis_information': 'Per case labeling metadata',
        'anesthetic': 'Resident roster',
        'recording_details': 'Authoritative case list, parent for case scoped tables',
        'mp4_status': 'MP4 export status per camera, 1..n semantics driven by your app',
        'seq_status': 'SEQ ingestion status per camera, values are non negative. 1 more than 200MB, 2 under 200MB, 3 missing, 4 format problem'
    }

    # Generate dbdiagram.io format
    output_lines = []
    today = date.today().strftime("%Y-%m-%d")
    output_lines.append(f"//// ScalpelLab Database, exported {today}")
    output_lines.append("//// Paste into https://dbdiagram.io")
    output_lines.append("")

    # Handle sqlite_sequence first if it exists
    if 'sqlite_sequence' in table_schemas:
        output_lines.append("//// System table from SQLite. Usually not modeled, included here for completeness.")
        output_lines.append("Table sqlite_sequence {")
        output_lines.append("  name varchar")
        output_lines.append("  seq  varchar")
        output_lines.append("  Note: 'SQLite internal autoincrement tracking table'")
        output_lines.append("}")
        output_lines.append("")

    # Generate table definitions for regular tables
    for table in ['analysis_information', 'anesthetic', 'recording_details', 'mp4_status', 'seq_status']:
        if table not in table_schemas:
            continue

        columns = table_schemas[table]
        output_lines.append(f"Table {table} {{")

        for col in columns:
            col_name = col[1]
            col_type = col[2] or "TEXT"
            is_pk = col[5]
            not_null = col[3]
            default_val = col[4]

            # Convert SQLite types to dbdiagram.io types
            db_type = col_type.upper()
            if "INT" in db_type:
                db_type = "int"
            elif "REAL" in db_type or "FLOAT" in db_type or "DOUBLE" in db_type:
                db_type = "decimal"
            elif "TEXT" in db_type or "CHAR" in db_type or "VARCHAR" in db_type:
                db_type = "varchar"
            elif "DATE" in db_type:
                db_type = "varchar"  # Keep as varchar for flexibility
            else:
                db_type = "varchar"

            # Build column definition with proper spacing
            col_def = f"  {col_name:<18} {db_type}"

            # Add constraints
            constraints = []
            if is_pk:
                constraints.append("pk")
                if table == 'recording_details' and col_name == 'date_case':
                    constraints.append("unique")
            if not_null and not is_pk:
                constraints.append("not null")
            if default_val is not None and table == 'seq_status':
                constraints.append(f"default: {default_val}")

            if constraints:
                col_def += f" [{', '.join(constraints)}]"

            # Add inline comments for specific fields
            if table == 'analysis_information' and col_name == 'labeled':
                col_def += "     // 0 or 1"
            elif table == 'anesthetic' and col_name == 'grade_a_date':
                col_def += "  // merged column"
            elif table in ['mp4_status', 'seq_status'] and col_name == 'intern_key':
                col_def += "      // FK to anesthetic.intern_key"
            elif table == 'seq_status' and col_name == 'date_case':
                col_def += " // FK to recording_details.date_case"

            output_lines.append(col_def)

        # Add table note
        if table in table_notes:
            output_lines.append(f"  Note: '{table_notes[table]}'")

        output_lines.append("}")
        output_lines.append("")

    # Generate relationships dynamically from detected foreign keys
    if all_foreign_keys:
        output_lines.append("// Relationships detected from database:")
        for table, foreign_keys in all_foreign_keys.items():
            for local_col, ref_table, ref_col in foreign_keys:
                # Default to primary key if ref_col is not specified
                if not ref_col:
                    # Find primary key of referenced table
                    if ref_table in table_schemas:
                        for col in table_schemas[ref_table]:
                            if col[5]:  # is primary key
                                ref_col = col[1]
                                break
                    if not ref_col:
                        ref_col = "id"  # fallback

                # Generate relationship
                output_lines.append(f"Ref: {ref_table}.{ref_col} > {table}.{local_col}")
        output_lines.append("")
    else:
        output_lines.append("// No foreign key relationships detected in database schema")
        output_lines.append("")

    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(output_lines))

    print(f"dbdiagram.io format saved to: {output_path}")
    print("\nTo use:")
    print("1. Go to https://dbdiagram.io/")
    print("2. Click 'Go to App'")
    print("3. Copy and paste the content of the output file")
    print("4. Your database diagram will be generated automatically!")

def main():
    # Import path manager
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from config.paths import paths

    db_path = str(paths.DATABASE_FILE)
    output_path = str(paths.DBDIAGRAM_OUTPUT)

    print(f"Converting database: {db_path}")
    print(f"Output file: {output_path}")
    print()

    try:
        sqlite_to_dbdiagram(db_path, output_path)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()