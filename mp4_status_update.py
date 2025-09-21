import os
import sys
import json
import sqlite3
import time
from subprocess import Popen, CREATE_NEW_CONSOLE, PIPE, STDOUT
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterable
import argparse

# ============================================
# Cameras (columns expected in your seq_status)
# ============================================
CAMERAS = [
    "Cart_Center_2", "Cart_LT_4", "Cart_RT_1",
    "General_3", "Monitor", "Patient_Monitor",
    "Ventilator_Monitor", "Injection_Port"
]

# ============================================
# NorPix CLExport possible locations (Windows)
# ============================================
CLEXPORT_PATHS = [
    r"C:\Program Files\NorPix\BatchProcessor\CLExport.exe",
    r"C:\Program Files (x86)\NorPix\BatchProcessor\CLExport.exe",
    r"C:\NorPix\BatchProcessor\CLExport.exe",
]

# ============================================
# Tuning knobs (adjusted for better success rate)
# ============================================
MAX_RETRIES_MP4 = 2  # attempts for MP4
MAX_RETRIES_AVI = 1  # attempts for AVI fallback
TIMEOUT_SECS = 20  # increased timeout for larger files
KILL_AFTER_ERROR_LINES = 6  # slightly more tolerant
SUPPRESS_CLEXPORT_OUTPUT = True  # keep console clean
MIN_VALID_FILE_SIZE_MB = 1.0  # Minimum size for valid MP4/AVI file

# Broader phrase so we catch both MP4/AVI variants
ERROR_LINE_SIGNATURE = "Error writing video"


# =========================
# CLExport helpers
# =========================
def find_clexport() -> Optional[str]:
    for path in CLEXPORT_PATHS:
        if os.path.exists(path):
            return path
    return None


def _build_cmd(clexport_path: str, seq_path: Path, out_dir: Path, exported_name: str, container: str) -> List[str]:
    """
    Build CLExport command. DO NOT pass '-cmp' (it causes 'No value found for parameter -cmp' on some installs).
    """
    return [
        clexport_path,
        "-i", str(seq_path),
        "-o", str(out_dir),
        "-of", exported_name,
        "-f", container,
    ]


def is_valid_video_file(file_path: Path, min_size_mb: float = MIN_VALID_FILE_SIZE_MB) -> bool:
    """Check if video file exists and has reasonable size."""
    if not file_path.exists():
        return False
    try:
        size_mb = file_path.stat().st_size / (1024 * 1024)
        return size_mb > min_size_mb
    except:
        return False


def find_existing_export(out_dir: Path, base_stem: str, extensions: List[str] = ['.mp4', '.avi']) -> Optional[Path]:
    """Find any existing export of this file (with or without counter suffix)."""
    for ext in extensions:
        # Check base name
        base_path = out_dir / f"{base_stem}{ext}"
        if is_valid_video_file(base_path):
            return base_path

        # Check numbered variants
        for i in range(1, 100):  # Check up to _99
            numbered_path = out_dir / f"{base_stem}_{i}{ext}"
            if is_valid_video_file(numbered_path):
                return numbered_path

    return None


def clean_invalid_exports(out_dir: Path, base_stem: str, debug: bool = False) -> int:
    """Remove invalid/incomplete export files. Returns count of removed files."""
    removed = 0
    patterns = [f"{base_stem}.mp4", f"{base_stem}_*.mp4", f"{base_stem}.avi", f"{base_stem}_*.avi"]

    for pattern in patterns:
        for file in out_dir.glob(pattern):
            if not is_valid_video_file(file):
                try:
                    file.unlink()
                    removed += 1
                    if debug:
                        print(f"[CLEAN] Removed invalid file: {file.name} (size: {file.stat().st_size / 1024:.1f}KB)")
                except Exception as e:
                    if debug:
                        print(f"[CLEAN] Could not remove {file}: {e}")

    return removed


def get_next_available_filename(out_dir: Path, base_stem: str, extension: str) -> Tuple[str, Path]:
    """Get next available filename that doesn't exist."""
    # First try without counter
    exported_name = f"{base_stem}{extension}"
    file_path = out_dir / exported_name

    if not file_path.exists():
        return exported_name, file_path

    # Try with counter
    counter = 1
    while counter < 1000:  # Safety limit
        exported_name = f"{base_stem}_{counter}{extension}"
        file_path = out_dir / exported_name
        if not file_path.exists():
            return exported_name, file_path
        counter += 1

    raise ValueError(f"Could not find available filename for {base_stem} after 1000 attempts")


def export_seq_once_streaming(
        seq_path: Path,
        out_dir: Path,
        exported_name: str,
        container: str,
        simulate: bool,
        spawn_console: bool = False,
        timeout_secs: Optional[int] = None,
        kill_after_error_lines: Optional[int] = None,
        suppress_console_output: bool = True
) -> Tuple[int, str]:
    """
    Run a single CLExport attempt while *stream-reading* its stdout.
    - If we see >= kill_after_error_lines of the known error line, kill the process and fail fast.
    - Also hard-timeout after timeout_secs.
    Returns (exitcode, message). 0 = success.
    """
    if simulate:
        return 0, f"Simulated export: {seq_path} -> {out_dir / exported_name} ({container})"

    clexport_path = find_clexport()
    if not clexport_path:
        searched = "\n".join([f"  - {p}" for p in CLEXPORT_PATHS])
        return 1, f"CLExport.exe not found. Searched:\n{searched}"

    cmd = _build_cmd(clexport_path, seq_path, out_dir, exported_name, container)
    creationflags = CREATE_NEW_CONSOLE if spawn_console else 0

    if spawn_console:
        # Can't capture stdout; just enforce timeout loop.
        try:
            proc = Popen(cmd, universal_newlines=True, creationflags=creationflags)
            start = time.time()
            while True:
                ret = proc.poll()
                if ret is not None:
                    return (0, f"Exported successfully ({container})") if ret == 0 else (ret,
                                                                                         f"CLExport failed with exit code {ret} ({container})")
                if timeout_secs is not None and (time.time() - start) > timeout_secs:
                    try:
                        proc.kill()
                    finally:
                        return 1, f"CLExport timed out after {timeout_secs}s ({container})"
                time.sleep(0.2)
        except Exception as e:
            return 1, f"Exception running CLExport: {str(e)} ({container})"

    # Not spawning a console: capture merged stdout/stderr to detect spam and enforce fast-kill.
    try:
        proc = Popen(
            cmd,
            text=True,
            bufsize=1,  # line-buffered
            stdout=PIPE,
            stderr=STDOUT,
            creationflags=creationflags
        )
    except Exception as e:
        return 1, f"Exception starting CLExport: {str(e)} ({container})"

    error_count = 0
    start_time = time.time()

    def mirror(line: str):
        if not SUPPRESS_CLEXPORT_OUTPUT and not suppress_console_output:
            print(line, end="")

    try:
        while True:
            if timeout_secs is not None and (time.time() - start_time) > timeout_secs:
                try:
                    proc.kill()
                finally:
                    return 1, f"CLExport timed out after {timeout_secs}s ({container})"

            ret = proc.poll()
            if ret is not None:
                if proc.stdout:
                    for _ in proc.stdout:
                        pass
                return (0, f"Exported successfully ({container})") if ret == 0 else (ret,
                                                                                     f"CLExport failed with exit code {ret} ({container})")

            if proc.stdout:
                line = proc.stdout.readline()
            else:
                line = ""

            if not line:
                time.sleep(0.05)
                continue

            mirror(line)

            if ERROR_LINE_SIGNATURE in line:
                error_count += 1
                if kill_after_error_lines is not None and error_count >= kill_after_error_lines:
                    try:
                        proc.kill()
                    finally:
                        return 1, f"Killed after {error_count} repeated errors ({container})"

    except Exception as e:
        try:
            proc.kill()
        except Exception:
            pass
        return 1, f"Exception while streaming CLExport output: {str(e)} ({container})"


# =========================
# Path utilities
# =========================
def expand_seq_paths(paths: Iterable[str], debug: bool = False) -> List[Path]:
    """
    Each input may be:
      - a directory that contains one or more .seq files (we'll pick the first),
      - a direct .seq file.
    """
    expanded: List[Path] = []
    for p in paths:
        Pobj = Path(p)
        if Pobj.is_dir():
            seqs = sorted(Pobj.glob("*.seq"))
            if not seqs:
                if debug:
                    print(f"[WARN] No .seq file found in directory: {Pobj}")
                continue
            if len(seqs) > 1 and debug:
                print(f"[WARN] Multiple .seq files in {Pobj}, taking first: {seqs[0].name}")
            expanded.append(seqs[0])
        else:
            if Pobj.suffix.lower() == ".seq":
                expanded.append(Pobj)
            else:
                if debug:
                    print(f"[WARN] Skipping non-seq path: {Pobj}")
    return expanded


def resolve_channel_label(seq_path: Path, channel_names: Dict[str, str]) -> str:
    """
    Choose the output base name using mapping (stem -> filename -> fullpath -> parent name),
    falling back to parent folder name or stem.
    """
    stem = seq_path.stem
    name = seq_path.name
    full = str(seq_path.resolve())
    parent_name = seq_path.parent.name if seq_path.parent else ""

    return (
            channel_names.get(stem)
            or channel_names.get(name)
            or channel_names.get(full)
            or channel_names.get(parent_name)
            or parent_name
            or stem
    )


def dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# =========================
# Out dir computation (robust)
# =========================
def compute_out_dir(seq_path: Path, out_root_path: Path) -> Path:
    """
    Decide where to write the output.
    - If the input path includes a DATA_* anchor, mirror from there.
    - Otherwise, fall back to DATA_Unknown/CaseUnknown/<Channel>.
    Always mkdir(parents=True, exist_ok=True).
    """
    parts = seq_path.parts
    anchor_idx = None
    for i, part in enumerate(parts):
        if part.upper().startswith("DATA_"):
            anchor_idx = i
            break

    if anchor_idx is not None:
        rel_from_data = Path(*parts[anchor_idx:])  # e.g., DATA_22-12-04/Case1/General_3/file.seq
        out_dir = out_root_path / rel_from_data.parent
    else:
        channel = seq_path.parent.name if seq_path.parent else "ChannelUnknown"
        case = seq_path.parent.parent.name if seq_path.parent and seq_path.parent.parent else "CaseUnknown"
        date = seq_path.parent.parent.parent.name if seq_path.parent and seq_path.parent.parent and seq_path.parent.parent.parent else "DATA_Unknown"
        if not str(date).upper().startswith("DATA_"):
            date = "DATA_Unknown"
        out_dir = out_root_path / str(date) / str(case) / str(channel)

    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


# =========================
# DB -> channel dir list
# =========================
def query_channel_dirs_from_db(db_path: str,
                               table: str,
                               cameras: List[str],
                               only_value: int = 1,
                               debug: bool = False) -> List[str]:
    """
    Reads rows from `table` (expects columns: date_case + camera flags),
    returns a list of relative channel dirs like 'DATA_22-12-04\\Case1\\General_3'
    for all cameras where flag == only_value.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cols = ", ".join(cameras)
    rows = cur.execute(f"SELECT date_case, {cols} FROM {table}").fetchall()
    conn.close()

    all_rel_dirs: List[str] = []
    for row in rows:
        date_case, *vals = row
        if not isinstance(date_case, str) or "_" not in date_case:
            if debug:
                print(f"[WARN] Bad date_case format: {date_case}")
            continue

        date, case_no = date_case.split("_", 1)
        # date: 'YYYY-MM-DD' -> 'DATA_YY-MM-DD'
        yy = date[2:4];
        mm = date[5:7];
        dd = date[8:10]
        data_folder = f"DATA_{yy}-{mm}-{dd}"
        case_folder = f"Case{case_no}"

        # collect cameras with given value (default == 1)
        active_channels = [cam for cam, v in zip(cameras, vals) if v == only_value]

        # build relative dirs
        for cam in active_channels:
            all_rel_dirs.append(f"{data_folder}\\{case_folder}\\{cam}")

    return dedupe_preserve_order(all_rel_dirs)


# =========================
# Full pipeline with improved handling
# =========================
def run_pipeline(db_path: str,
                 table: str,
                 seq_root: str,
                 out_root: str,
                 channel_names: Dict[str, str],
                 only_value: int,
                 simulate: bool,
                 debug: bool,
                 spawn_console: bool,
                 skip_existing: bool,
                 clean_invalid: bool,
                 fallback_avi: bool) -> None:
    seq_root_path = Path(seq_root).resolve()
    out_root_path = Path(out_root).resolve()
    out_root_path.mkdir(parents=True, exist_ok=True)

    # Statistics
    stats = {
        'total': 0,
        'skipped_existing': 0,
        'success_mp4': 0,
        'success_avi': 0,
        'failed': 0,
        'cleaned': 0
    }

    # 1) Build relative channel dirs from DB rows
    rel_dirs = query_channel_dirs_from_db(
        db_path=db_path,
        table=table,
        cameras=CAMERAS,
        only_value=only_value,
        debug=debug
    )

    if debug:
        print(f"[DEBUG] Relative channel dirs from DB (count={len(rel_dirs)}). Example:")
        for p in rel_dirs[:5]:
            print("        ", p)

    # 2) Convert to absolute channel directories in SEQ tree
    channel_dirs = [str(seq_root_path / rd) for rd in rel_dirs]

    # 3) Expand channel dirs into actual .seq files
    seq_files = expand_seq_paths(channel_dirs, debug=debug)

    if debug:
        print(f"[DEBUG] Discovered .seq files to process: {len(seq_files)}")

    # 4) Export loop with improved handling
    log_path = out_root_path / "export_log.txt"
    total = len(seq_files)
    stats['total'] = total

    with log_path.open('a', encoding='utf-8') as log_file:
        log_file.write(f"\n{'=' * 60}\n")
        log_file.write(f"Export session started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"{'=' * 60}\n")

        for idx, seq_path in enumerate(seq_files, 1):
            try:
                seq_path = seq_path.resolve()
                if debug:
                    print(f"\n[{idx}/{total}] START {seq_path}")

                # Decide destination dir robustly (creates it if needed)
                out_dir = compute_out_dir(seq_path, out_root_path)

                # Label and base filename
                ch_label = resolve_channel_label(seq_path, channel_names)
                base_stem = ch_label

                # Clean invalid files if requested
                if clean_invalid:
                    removed = clean_invalid_exports(out_dir, base_stem, debug)
                    stats['cleaned'] += removed

                # Check if valid export already exists
                if skip_existing:
                    existing = find_existing_export(out_dir, base_stem)
                    if existing:
                        status = "SKIPPED"
                        reason = f"Valid export already exists: {existing.name}"
                        stats['skipped_existing'] += 1

                        if debug:
                            print(f"[{idx}/{total}] {status}: {reason}")

                        log_file.write(f"{seq_path} -> {existing}: {status} | {reason}\n")
                        log_file.flush()
                        continue

                status, reason = "PENDING", ""
                final_path = None

                # Pre-checks
                if not seq_path.exists():
                    status, reason = "FAILED", "File does not exist"
                    stats['failed'] += 1
                elif seq_path.stat().st_size == 0:
                    status, reason = "FAILED", "File is empty"
                    stats['failed'] += 1

                # Attempt MP4 with retries
                if status == "PENDING":
                    # Get next available filename for MP4
                    exported_name, mp4_path = get_next_available_filename(out_dir, base_stem, ".mp4")

                    if debug:
                        print(f"[{idx}/{total}] TRY MP4 -> {mp4_path}")

                    for attempt in range(1, MAX_RETRIES_MP4 + 1):
                        if debug:
                            print(f"[{idx}/{total}]  MP4 attempt {attempt}/{MAX_RETRIES_MP4}")

                        exitcode, reason = export_seq_once_streaming(
                            seq_path=seq_path,
                            out_dir=out_dir,
                            exported_name=exported_name[:-4],  # Remove .mp4 extension
                            container="mp4",
                            simulate=simulate,
                            spawn_console=spawn_console,
                            timeout_secs=TIMEOUT_SECS,
                            kill_after_error_lines=KILL_AFTER_ERROR_LINES,
                            suppress_console_output=SUPPRESS_CLEXPORT_OUTPUT
                        )

                        if exitcode == 0 and is_valid_video_file(mp4_path):
                            status = "SUCCESS_MP4"
                            stats['success_mp4'] += 1
                            final_path = mp4_path
                            break
                        else:
                            # Remove potentially invalid file
                            if mp4_path.exists() and not is_valid_video_file(mp4_path):
                                try:
                                    mp4_path.unlink()
                                    if debug:
                                        print(f"[{idx}/{total}]  Removed invalid MP4 after attempt {attempt}")
                                except:
                                    pass

                            if debug and attempt == MAX_RETRIES_MP4:
                                print(f"[{idx}/{total}]  MP4 failed after {MAX_RETRIES_MP4} attempts")

                # Fallback to AVI if MP4 failed and fallback is enabled
                if status == "PENDING" and fallback_avi:
                    # Get next available filename for AVI
                    exported_name, avi_path = get_next_available_filename(out_dir, base_stem, ".avi")

                    if debug:
                        print(f"[{idx}/{total}] FALLBACK AVI -> {avi_path}")

                    for attempt in range(1, MAX_RETRIES_AVI + 1):
                        if debug:
                            print(f"[{idx}/{total}]  AVI attempt {attempt}/{MAX_RETRIES_AVI}")

                        exitcode, reason = export_seq_once_streaming(
                            seq_path=seq_path,
                            out_dir=out_dir,
                            exported_name=exported_name[:-4],  # Remove .avi extension
                            container="avi",
                            simulate=simulate,
                            spawn_console=spawn_console,
                            timeout_secs=TIMEOUT_SECS * 2,  # Give AVI more time
                            kill_after_error_lines=KILL_AFTER_ERROR_LINES * 2,  # More tolerant for AVI
                            suppress_console_output=SUPPRESS_CLEXPORT_OUTPUT
                        )

                        if exitcode == 0 and is_valid_video_file(avi_path):
                            status = "SUCCESS_AVI"
                            stats['success_avi'] += 1
                            final_path = avi_path
                            break
                        else:
                            # Remove potentially invalid file
                            if avi_path.exists() and not is_valid_video_file(avi_path):
                                try:
                                    avi_path.unlink()
                                    if debug:
                                        print(f"[{idx}/{total}]  Removed invalid AVI after attempt {attempt}")
                                except:
                                    pass

                # Final status update
                if status == "PENDING":
                    status = "FAILED"
                    stats['failed'] += 1

                # Per-folder mapping + root log
                try:
                    with (out_dir / "_seq_mapping.txt").open('a', encoding='utf-8') as mapfile:
                        mapfile.write(f"{ch_label} = {seq_path} | {status} | {reason}\n")
                except Exception as e:
                    if debug:
                        print(f"[WARN] Could not write mapping file in {out_dir}: {e}")

                try:
                    output_file = final_path if final_path else "None"
                    log_file.write(f"{seq_path} -> {output_file}: {status} | {reason}\n")
                    log_file.flush()
                except Exception as e:
                    if debug:
                        print(f"[WARN] Could not write export_log: {e}")

                if debug:
                    print(f"[{idx}/{total}] [{status}] {seq_path} -> {final_path if final_path else 'FAILED'}")

            except Exception as e:
                if debug:
                    print(f"[{idx}/{total}] [HARD-FAIL] {seq_path} | {e}. Skipping.")
                stats['failed'] += 1
                continue

    # Print summary
    print("\n" + "=" * 60)
    print("EXPORT SUMMARY")
    print("=" * 60)
    print(f"Total files:       {stats['total']}")
    print(f"Skipped existing:  {stats['skipped_existing']}")
    print(f"Success (MP4):     {stats['success_mp4']}")
    print(f"Success (AVI):     {stats['success_avi']}")
    print(f"Failed:            {stats['failed']}")
    print(f"Cleaned invalid:   {stats['cleaned']}")
    print("=" * 60)


# =========================
# Main entry point - HARDCODED VALUES
# =========================
if __name__ == "__main__":
    # HARDCODED VALUES - Just run the script directly!
    DB_PATH = r"F:\Room_8_Data\Scalpel_Raz\ScalpelDatabase.sqlite"
    SEQ_ROOT = r"F:\Room_8_Data\Sequence_Backup"
    OUT_ROOT = r"F:\Room_8_Data\Recordings"
    TABLE = "seq_status"
    ONLY_VALUE = 1
    SKIP_EXISTING = True  # Skip files that already exist (RECOMMENDED!)
    CLEAN_INVALID = True  # Clean up invalid files (RECOMMENDED!)
    FALLBACK_AVI = True  # Try AVI if MP4 fails (RECOMMENDED!)
    SIMULATE = False  # Set to True for a dry run
    DEBUG = True  # Show detailed progress
    SPAWN_CONSOLE = False  # Don't spawn separate console windows

    print("Starting seq_exporter with hardcoded values...")
    print(f"Database: {DB_PATH}")
    print(f"Seq root: {SEQ_ROOT}")
    print(f"Out root: {OUT_ROOT}")
    print(f"Skip existing: {SKIP_EXISTING}")
    print(f"Clean invalid: {CLEAN_INVALID}")
    print(f"Fallback to AVI: {FALLBACK_AVI}")
    print("=" * 60)

    run_pipeline(
        db_path=DB_PATH,
        table=TABLE,
        seq_root=SEQ_ROOT,
        out_root=OUT_ROOT,
        channel_names={},  # Leave empty to use folder names
        only_value=ONLY_VALUE,
        simulate=SIMULATE,
        debug=DEBUG,
        spawn_console=SPAWN_CONSOLE,
        skip_existing=SKIP_EXISTING,
        clean_invalid=CLEAN_INVALID,
        fallback_avi=FALLBACK_AVI
    )