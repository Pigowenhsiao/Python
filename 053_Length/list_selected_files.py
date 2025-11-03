import os
import re
from pathlib import Path
from configparser import ConfigParser

def main():
    ini_path = "Config_Length_NG.ini"
    cfg = ConfigParser()
    cfg.read(ini_path, encoding="utf-8")

    # 取得 input_paths 與 pattern
    input_paths = [s.strip() for s in cfg.get("Paths", "input_paths").split(",")]
    patterns = [s.strip() for s in cfg.get("Basic_info", "file_name_patterns", fallback="*.csv").split(",")]
    prefix = cfg.get("FileSelection", "prefix", fallback=None)
    lot_filter_pos_5_6 = cfg.get("FileSelection", "lot_filter_pos_5_6", fallback=None)

    print("=== 檔案篩選條件 ===")
    print(f"input_paths: {input_paths}")
    print(f"patterns: {patterns}")
    print(f"prefix: {prefix}")
    print(f"lot_filter_pos_5_6: {lot_filter_pos_5_6}")
    print("===================")

    for input_dir in input_paths:
        base = Path(input_dir)
        if not base.exists():
            print(f"[WARN] input path not found: {base}")
            continue

        for pattern in patterns:
            files = [p for p in base.glob(pattern) if p.is_file()]
            print(f"\n[DIR] {base} ({pattern}) 找到 {len(files)} 個檔案")
            for f in files:
                name = f.name
                # prefix
                if prefix and not name.startswith(prefix):
                    continue
                # 25 + 19 alnum + _
                m = re.search(r"25([A-Za-z0-9]{19})_", name)
                if not m:
                    continue
                seg = m.group(1)
                # lot_filter_pos_5_6
                if lot_filter_pos_5_6:
                    if len(seg) < 6 or seg[4:6] != lot_filter_pos_5_6:
                        continue
                print(f"  - {name}")

if __name__ == "__main__":
    main()
