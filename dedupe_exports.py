import re
from pathlib import Path
from collections import defaultdict

BASE_NAMES = {"Cart_Center_2", "Cart_LT_4", "Cart_RT_1", "General_3"}
SEPARATORS = ["_", " ", "-", "("]  # suffix patterns: _1, (1), -copy

def stem_matches_base(stem: str, base: str) -> bool:
    if stem == base:
        return True
    return any(stem.startswith(base + sep) for sep in SEPARATORS)

def is_canonical(file: Path, base: str, ext: str) -> bool:
    return file.name.lower() == f"{base.lower()}{ext.lower()}"

def dedupe_camera_exports(
    root_dir: str,
    dry_run: bool = True,
    min_valid_mb: float = 1.0,
    keep_canonical: bool = True,
) -> None:
    root = Path(root_dir)
    # group by parent dir + base + extension
    groups = defaultdict(list)

    for ext in (".mp4", ".avi"):
        for f in root.rglob(f"*{ext}"):
            try:
                stem = f.stem
                base = next((b for b in BASE_NAMES if stem_matches_base(stem, b)), None)
                if not base:
                    continue
                sz_mb = f.stat().st_size / (1024 * 1024)
                key = (str(f.parent.resolve()), base, ext.lower())
                groups[key].append((f, sz_mb, f.stat().st_mtime, is_canonical(f, base, ext)))
            except Exception:
                continue

    to_delete = []
    for (parent, base, ext), files in groups.items():
        if len(files) <= 1:
            continue

        canon = [t for t in files if t[3]]
        variants = [t for t in files if not t[3]]

        keep = set()
        if canon and keep_canonical:
            keep.add(canon[0][0])  # keep the canonical one in this folder
        else:
            best = sorted(files, key=lambda t: (t[1], t[2]), reverse=True)[0]
            keep.add(best[0])

        for f, sz, _, _ in files:
            if f not in keep:
                to_delete.append((f, sz, parent, base, ext))

    if not to_delete:
        print("[DEDUPE] No duplicates found.")
        return

    print(f"[DEDUPE] Found {len(to_delete)} duplicates across {len(groups)} groups. (dry_run={dry_run})")
    for f, sz, parent, base, ext in to_delete:
        print(f"  remove: {f}  ({sz:.1f} MB)  group=({parent} | {base}{ext})")

    if dry_run:
        print("[DEDUPE] Dry-run: nothing deleted.")
        return

    removed = 0
    for f, *_ in to_delete:
        try:
            f.unlink()
            removed += 1
        except Exception as e:
            print(f"[DEDUPE] Could not delete {f}: {e}")
    print(f"[DEDUPE] Removed {removed} files.")




if __name__ == "__main__":
    # EXAMPLES:
    # dedupe_camera_exports(r"F:\Room_8_Data\Recordings", dry_run=True)   # preview
    # dedupe_camera_exports(r"F:\Room_8_Data\Recordings\DATA_22-12-19\Case1\Cart_LT_4", dry_run=False)  # apply on a single dir
    dedupe_camera_exports(r"F:\Room_8_Data\Recordings", dry_run=False)
