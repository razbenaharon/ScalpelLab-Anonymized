import os
import sys
import shlex
import json
from subprocess import Popen, CREATE_NEW_CONSOLE
import argparse
from pathlib import Path

# Try multiple possible CLExport paths
CLEXPORT_PATHS = [
    'C:\\Program Files\\NorPix\\BatchProcessor\\CLExport.exe',
    'C:\\Program Files (x86)\\NorPix\\BatchProcessor\\CLExport.exe',
    'C:\\NorPix\\BatchProcessor\\CLExport.exe'
]

def find_clexport():
    """Find the CLExport executable."""
    for path in CLEXPORT_PATHS:
        if os.path.exists(path):
            return path
    return None


def export_seq(seq_path, out_dir, exported_name, simulate):
    if simulate:
        # Simulate export
        print(f"Simulated export: {seq_path} -> {out_dir / exported_name}")
        return 0, "Simulated export"
    
    # Find CLExport.exe
    clexport_path = find_clexport()
    if not clexport_path:
        searched_paths = "\n".join([f"  - {path}" for path in CLEXPORT_PATHS])
        return 1, f"CLExport.exe not found. Searched:\n{searched_paths}"
    
    # Build the command with proper arguments
    cmd = f'"{clexport_path}" -i "{seq_path}" -o "{out_dir}" -of "{exported_name}" -f mp4 -cmp 1'
    
    try:
        # Run CLExport with console window for debugging
        proc = Popen(shlex.split(cmd), universal_newlines=True, creationflags=CREATE_NEW_CONSOLE)
        exitcode = proc.wait()
        
        if exitcode == 0:
            return 0, "Exported successfully"
        else:
            return exitcode, f"CLExport failed with exit code {exitcode}"
    except Exception as e:
        return 1, f"Exception running CLExport: {str(e)}"

def main(seq_files, out_root, channel_names, simulate):
    log_path = Path(out_root) / "export_log.txt"
    log_file = log_path.open('a', encoding='utf-8')
    per_case = {}
    for seq_path in seq_files:
        seq_path = Path(seq_path)
        seq_path_str = str(seq_path.resolve())
        # Use the full file path as key for channel mapping
        ch_label = channel_names.get(seq_path_str, seq_path.stem)
        # Use the provided output directory directly since the GUI already creates the correct structure
        out_dir = Path(out_root)
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # Create unique filename to avoid conflicts if multiple sequences have the same stem
        base_name = f"{ch_label}.mp4"
        exported_name = base_name
        mp4_path = out_dir / exported_name
        
        # If file already exists, add a counter to make it unique
        counter = 1
        while mp4_path.exists():
            name_without_ext = base_name[:-4]  # Remove .mp4
            exported_name = f"{name_without_ext}_{counter}.mp4"
            mp4_path = out_dir / exported_name
            counter += 1
        # Status logic
        if not seq_path.exists():
            status, reason = "FAILED", "File does not exist"
        elif seq_path.stat().st_size == 0:
            status, reason = "FAILED", "File is empty"
        elif mp4_path.exists() and not simulate:
            status, reason = "SKIPPED", "Already exported"
        else:
            try:
                exitcode, reason = export_seq(seq_path, out_dir, exported_name, simulate)
                status = "EXPORTED SUCCESSFULLY" if exitcode == 0 else "FAILED"
            except Exception as e:
                status, reason = "FAILED", f"Exception during export: {str(e)}"
        # Write to mapping file
        map_path = out_dir / "_seq_mapping.txt"
        with map_path.open('a', encoding='utf-8') as mapfile:
            mapfile.write(f"{ch_label} = {seq_path} | {status} | {reason}\n")
        # Write to log file
        log_file.write(f"{seq_path} -> {mp4_path}: {status} | {reason}\n")
    log_file.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Export .seq files to the desired output folder.")
    parser.add_argument("seq_files", nargs='+', help="List of .seq files to be exported.")
    parser.add_argument("output_folder", help="Root folder to export the .mp4 files to.")
    parser.add_argument("--channel-names", type=str, default="{}", help="JSON string mapping seq file stems to channel labels.")
    parser.add_argument("--simulate", action="store_true", help="Simulate export (no actual conversion).")
    parser.add_argument("--debug", action="store_true", help="Enable debug output.")
    args = parser.parse_args()
    channel_names = json.loads(args.channel_names)
    
    if args.debug:
        print(f"Debug mode enabled")
        print(f"Sequence files: {args.seq_files}")
        print(f"Output folder: {args.output_folder}")
        print(f"Channel names: {channel_names}")
        print(f"Simulate: {args.simulate}")
        print(f"Total files to process: {len(args.seq_files)}")
    
    main(args.seq_files, args.output_folder, channel_names, args.simulate)