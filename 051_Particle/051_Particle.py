# -*- coding: utf-8 -*-


import os
import re
import glob
import logging
import shutil
from pathlib import Path
from datetime import datetime, date
from configparser import ConfigParser
import sys

import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom

EXPECTED_HEADERS = [
    "No","tNo","ResTime","SenID","SenName","PtNo","PtName",
    "GrpNo","GrpName",
    "Ch1AlarmNo","Ch1KeisokuData",
    "Ch2AlarmNo","Ch2KeisokuData",
    "Ch3AlarmNo","Ch3KeisokuData",
    "Ch4AlarmNo","Ch4KeisokuData",
    "Ch5AlarmNo","Ch5KeisokuData",
    "Ch6AlarmNo","Ch6KeisokuData",
]

# ---------------- Logging / Config helpers ----------------
def setup_logging(log_dir: str, operation_name: str) -> str:
    log_folder = Path(log_dir) / datetime.today().strftime("%Y-%m-%d")
    log_folder.mkdir(parents=True, exist_ok=True)
    log_file = log_folder / f"{operation_name}.log"
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)
    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return str(log_file)

def read_ini(path: str) -> ConfigParser:
    cfg = ConfigParser()
    cfg.optionxform = str  # keep key casing (for ManualAssign field names)
    cfg.read(path, encoding="utf-8")
    return cfg

def parse_fields_map(fields_text: str):
    mapping = {}
    for raw in fields_text.splitlines():
        s = raw.strip()
        if not s or s.startswith("#") or ":" not in s:
            continue
        parts = [p.strip() for p in s.split(":")]
        if len(parts) < 2:
            continue
        k, col_s = parts[:2]
        try:
            mapping[k] = {"col": int(col_s)}
        except ValueError:
            pass
    return mapping

def write_to_csv(csv_path: Path, df: pd.DataFrame):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    header = not csv_path.exists()
    df.to_csv(csv_path, mode="a", header=header, index=False, encoding="utf-8-sig")

# ---------------- XML helpers (with Windows-safe filename) ----------------
def _sanitize_filename_component(s: str, fallback: str) -> str:
    if s is None:
        return fallback
    s = str(s).strip()
    if not s:
        return fallback
    s2 = re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', s)
    return s2 if s2.strip('_ ').strip() else fallback

def _prettify(elem: ET.Element) -> bytes:
    return minidom.parseString(ET.tostring(elem)).toprettyxml(indent="  ", encoding="utf-8")

def generate_pointer_xml(
    output_path: Path,
    csv_path: Path,
    site: str,
    product_family: str,
    operation: str,
    test_station: str,
    serial_no: str,   # already generated <Prefix><YYMMDD>
    part_no: str,
    result_value: str,
    teststep_status_value: str
) -> Path:
    """
    Create XML (Results/Result/Header/TestStep/Data), where Data.Value points to the CSV path.
    Windows-safe filename: sanitize illegal characters; fallback to UNKNOWPN/NA when PN/SN are blank or illegal.
    XML content still uses provided SN/PN (ElementTree escapes content).
    """
    now = datetime.now()
    now_iso_content = now.strftime("%Y-%m-%dT%H:%M:%S")  # Format for XML content
    now_iso_filename = now.strftime("%Y-%m-%dT%H.%M.%S") # Format for Windows-safe filename
    output_path.mkdir(parents=True, exist_ok=True)

    safe_part = _sanitize_filename_component(part_no, "UNKNOWPN")
    safe_sn   = _sanitize_filename_component(serial_no, "NA")
    raw_name = (
        f"Site={site},ProductFamily={product_family},Operation={operation},"
        f"Partnumber={safe_part},Serialnumber={safe_sn},Testdate={now_iso_filename}.xml"
    )
    xml_name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', raw_name)
    xml_fp = Path(output_path) / xml_name

    results = ET.Element("Results", {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xmlns:xsd": "http://www.w3.org/2001/XMLSchema"
    })
    result = ET.SubElement(results, "Result", startDateTime=now_iso_content, endDateTime=now_iso_content, Result=result_value)
    ET.SubElement(
        result, "Header",
        SerialNumber=(serial_no or "NA"),
        PartNumber=(part_no or "UNKNOWPN"),
        Operation=operation, TestStation=test_station,
        Operator="NA", StartTime=now_iso_content, Site=str(site), LotNumber=""
    )
    tstep = ET.SubElement(
        result, "TestStep",
        Name=operation, startDateTime=now_iso_content, endDateTime=now_iso_content, Status=teststep_status_value
    )
    ET.SubElement(
        tstep, "Data",
        DataType="Table", Name=f"tbl_{operation.upper()}",
        Value=str(csv_path), CompOperation="LOG"
    )

    with open(xml_fp, "wb") as f:
        f.write(_prettify(results))
    return xml_fp

# ---------------- Directory scanning & picking ----------------
def debug_list_dir(d: str, patterns: list[str], max_show: int = 50):
    print(f"\n[SCAN] Path: Done")
    try:
        exists = Path(d).exists()
        #print(f"  - exists: {exists}")
        if not exists:
            print("  - Not accessible (not connected/no permission/wrong path)")
            return []
        names = []
        with os.scandir(d) as it:
            for i, entry in enumerate(it):
                if i >= max_show:
                    break
                names.append(entry.name)
        #print(f"  - listing (up to {max_show}): {names}")
        found = []
        for p in patterns:
            g = glob.glob(str(Path(d) / p))
            print(f"  - pattern {p} hits: {len(g)}")
            found.extend(g)
        return list(set(found))
    except PermissionError:
        print("  - PermissionError: no read permission on this path")
        return []
    except FileNotFoundError:
        print("  - FileNotFoundError: path does not exist")
        return []
    except OSError as e:
        print(f"  - OSError: {e}")
        return []

def pick_latest_two_days_files(input_dirs, patterns):
    """
    Picks all .xls files from the latest two available dates.
    Returns a list of file paths.
    """
    all_hits = []  # (path, file_date, mtime)
    for d in input_dirs:
        hits = debug_list_dir(d, patterns)
        for f in hits:
            name = os.path.basename(f)
            if not name.lower().endswith(".xls"):
                continue
            m = re.match(r"^(\d{8})", name)
            if not m:
                continue
            try:
                fdate = datetime.strptime(m.group(1), "%Y%m%d").date()
            except ValueError:
                continue
            all_hits.append((f, fdate, os.path.getmtime(f)))

    print(f"\nFiles matching YYYYMMDD.xls: {len(all_hits)}")
    if not all_hits:
        return []

    # Find all unique dates and sort them in descending order
    unique_dates = sorted(list(set(c[1] for c in all_hits)), reverse=True)
    
    # Get the latest two dates
    latest_two_dates = unique_dates[:2]
    
    # Filter files that match these two dates
    selected_files = [c[0] for c in all_hits if c[1] in latest_two_dates]
    return selected_files

def pick_by_filename_closest_date(input_dirs, patterns):
    today = datetime.today().date()
    all_hits = []  # (path, file_date, mtime)
    for d in input_dirs:
        hits = debug_list_dir(d, patterns)
        for f in hits:
            name = os.path.basename(f)
            if not name.lower().endswith(".xls"):
                continue
            m = re.match(r"^(\d{8})", name)
            if not m:
                continue
            try:
                fdate = datetime.strptime(m.group(1), "%Y%m%d").date()
            except ValueError:
                continue
            all_hits.append((f, fdate, os.path.getmtime(f)))

    print(f"\nFiles matching YYYYMMDD.xls: {len(all_hits)}")
    if all_hits:
        preview = sorted(all_hits, key=lambda x: (x[1], x[2]))[:30]
        #for i, (f, fd, mt) in enumerate(preview, 1):
        #    print(f"  [{i:02d}] {os.path.basename(f)} | date={fd} | mtime={datetime.fromtimestamp(mt)}")

    if not all_hits:
        return None, None, None

    todays = [c for c in all_hits if c[1] == today]
    if todays:
        best = max(todays, key=lambda x: x[2])
        return best[0], best[1], 0

    def dist(c): return abs((c[1] - today).days)
    min_dist = min(dist(c) for c in all_hits)
    nearest = [c for c in all_hits if dist(c) == min_dist]
    best = max(nearest, key=lambda x: x[2])
    return best[0], best[1], min_dist

# ---------------- Serial Number generator ----------------
def build_serial_from_prefix(prefix: str, date_source: str | datetime = None) -> str:
    """
    Build Serial Number as <Prefix><YYMMDD> from a given date source.
    If date_source is not provided or invalid, defaults to today's date.
    """
    yymmdd = ""
    try:
        if date_source:
            yymmdd = pd.to_datetime(date_source).strftime("%y%m%d")
    except (ValueError, TypeError):
        pass # Will fall through to using today's date
    if not yymmdd:
        yymmdd = datetime.today().strftime("%y%m%d")
    prefix = (prefix or "").strip()
    return f"{prefix}{yymmdd}" if prefix else yymmdd

# ---------------- Optional header detection (fallback) ----------------
def detect_header_row(df_like, expected=EXPECTED_HEADERS, min_hits=12):
    top = min(30, len(df_like))
    exp_set = set(h.lower() for h in expected)
    for r in range(top):
        row_vals = [str(v).strip() for v in list(df_like.iloc[r, :].values)]
        hits = sum(1 for v in row_vals if v.lower() in exp_set)
        if hits >= min_hits:
            return r, r + 1
    return 0, 1

# ---------------- Main ----------------
def main():
    ini_files = [f for f in os.listdir(".") if f.lower().endswith(".ini")]
    if not ini_files:
        print("No config (.ini) found.")
        return

    for ini in ini_files:
        cfg = read_ini(ini)

        # Basic info
        site = cfg.get("Basic_info", "Site", fallback="350")
        product_family = cfg.get("Basic_info", "ProductFamily", fallback="SAG FAB")
        operation = cfg.get("Basic_info", "Operation", fallback="PARTICLE_MONITOR_CR3F")
        test_station = cfg.get("Basic_info", "TestStation", fallback="PARTICLE_MONITOR")
        tool_name = cfg.get("Basic_info", "Tool_Name", fallback="UNKNOWN")

        # Paths & logging
        setup_logging(cfg.get("Paths", "log_path", fallback="./Log/"), operation)
        input_paths = [s.strip() for s in cfg.get("Paths", "input_paths").split(",")]
        csv_dir = Path(cfg.get("Paths", "CSV_path", fallback="./CSV/"))
        output_dir = Path(cfg.get("Paths", "output_path", fallback="./XML/"))
        intermediate = Path(cfg.get("Paths", "intermediate_data_path", fallback="./DataFile/"))
        intermediate.mkdir(parents=True, exist_ok=True)

        # Excel parameters
        desired_sheet = cfg.get("Excel", "sheet_name", fallback="KeisokuDataTable")
        cols = cfg.get("Excel", "data_columns", fallback="A:U")
        skiprows = cfg.getint("Excel", "main_skip_rows", fallback=1)

        # DataFields mapping
        fields_lines = cfg.get("DataFields", "fields", fallback="").strip()
        fmap = parse_fields_map(fields_lines)

        # ManualAssign (prefix for SN)
        manual_items = dict(cfg.items("ManualAssign")) if cfg.has_section("ManualAssign") else {}
        sn_prefix = manual_items.pop("SerialNumber", manual_items.pop("serialnumber", "")).strip() if manual_items else ""
        part_no   = manual_items.pop("PartNumber",  manual_items.pop("partnumber",  "")).strip() if manual_items else ""

        # Options
        enforce_today_restime = cfg.getboolean("Options", "enforce_today_restime", fallback=False)
        tz_name = cfg.get("Options", "timezone", fallback="Asia/Taipei")  # reserved for future TZ handling
        time_interval = cfg.getfloat("Options", "time_interval", fallback=None) # 每幾小時抓一點 (可為小數)，若無設定則不篩選

        # XML defaults
        result_value = cfg.get("XML_Defaults", "result_value", fallback="Passed")
        teststep_status_value = cfg.get("XML_Defaults", "teststep_status_value", fallback="Passed")

        # Patterns (xls only)
        patterns = [s.strip() for s in cfg.get("Basic_info", "file_name_patterns", fallback="*.xls").split(",")]
        patterns = [p for p in patterns if p.lower().endswith(".xls")] or ["*.xls"]

        # Pick files from the latest two days
        source_files = pick_latest_two_days_files(input_paths, patterns)
        if not source_files:
            print("\n❌ No .xls files with leading YYYYMMDD found in any input_paths.")
            print("   Please confirm: 1) path reachable (UNC mounted / VPN / permission), 2) correct folder level, 3) extension is .xls.")
            continue

        print(f"\n✅ Selected {len(source_files)} files for processing:")
        for f in source_files:
            print(f"   - {os.path.basename(f)}")

        all_data_frames = []
        for src_file in source_files:
            # Copy to intermediate
            copied = shutil.copy(src_file, intermediate / os.path.basename(src_file))

            # List sheets and choose best match
            xls = pd.ExcelFile(copied, engine="xlrd")  # .xls requires xlrd
            sheets = xls.sheet_names
            print(f"\nAvailable sheets: {sheets}")
            norm = lambda s: re.sub(r"\s+", "", s).lower()
            nd = norm(desired_sheet)
            use_sheet = desired_sheet if desired_sheet in sheets else \
                        next((s for s in sheets if norm(s) == nd), None) or \
                        next((s for s in sheets if nd in norm(s)), sheets[0])
            print(f"Using sheet: {use_sheet}")

            # Read with header (as configured). If empty, try header detection.
            df = pd.read_excel(
                copied,
                sheet_name=use_sheet,
                header=0,
                usecols=cols,
                skiprows=skiprows - 1,
                engine="xlrd",
                dtype=str  # Read all as string to avoid type inference issues
            )
            
            if df.empty:
                raw = pd.read_excel(copied, sheet_name=use_sheet, header=None, engine="xlrd")
                hdr_row, data_start = detect_header_row(raw, EXPECTED_HEADERS, min_hits=12)
                print(f"Header auto-detected at row: {hdr_row+1} (data start: {data_start+1})")
                df = pd.read_excel(copied, sheet_name=use_sheet, header=hdr_row, usecols=cols, engine="xlrd", dtype=str)
            
            if not df.empty:
                all_data_frames.append(df)

        # Merge all dataframes into one
        try:
            df = pd.concat(all_data_frames, ignore_index=True) if all_data_frames else pd.DataFrame()
        except Exception as e:
            print(f"Failed to read Excel: {e}")
            continue

        if df.empty:
            print("⚠️ DataFrame is empty after reading. Check sheet name/header row/column range (A:U).")

        # Apply key_* mapping to column names
        if fmap and not df.empty:
            rename_by_index = {}
            for k, v in fmap.items():
                idx = v["col"]
                if 0 <= idx < len(df.columns):
                    rename_by_index[df.columns[idx]] = k
            df = df.rename(columns=rename_by_index)

        # Optional: enforce today's ResTime
        if enforce_today_restime and "key_ResTime" in df.columns and not df.empty:
            def _is_today(x):
                try:
                    ts = pd.to_datetime(x, errors="coerce")
                    return not pd.isna(ts) and ts.date() == date.today()
                except Exception:
                    return False
            before = len(df)
            df = df[df["key_ResTime"].apply(_is_today)].copy()
            print(f"Filter 'ResTime == today' enabled: kept {len(df)}/{before} rows")

        # Inject system/ManualAssign fields
        if not df.empty:
            df["Operation"] = operation
            df["TestStation"] = test_station
            df["Site"] = site
            df["key_Tool_name"] = tool_name

            rename_map = {"key_ResTime": "Start_Date_Time", "key_Tool_name": "DeviceSerialNumber"}
            for c in list(df.columns):
                if c.startswith("key_") and c not in rename_map:
                    rename_map[c] = c.replace("key_", "", 1)
            df = df.rename(columns=rename_map)

            # --- Resample data based on time_interval ---
            if time_interval is not None and "PtName" in df.columns and "Start_Date_Time" in df.columns and not df.empty:
                # 將小時轉換為分鐘，以支援小數
                interval_minutes = int(time_interval * 60)
                print(f"Resampling data: keeping one point every {time_interval} hours ({interval_minutes} minutes) per PtName...")
                
                before_resample_count = len(df)
                # Ensure Start_Date_Time is a datetime object for time-based operations.
                df['Start_Date_Time'] = pd.to_datetime(df['Start_Date_Time'], errors='coerce')
                
                # 將所有 KeisokuData 欄位轉換為數值型別，以便比較大小
                keisoku_cols = [col for col in df.columns if 'KeisokuData' in col]
                for col in keisoku_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                df.dropna(subset=['Start_Date_Time'], inplace=True)

                # --- Efficient Resampling ---

                df = df.sort_values(['PtName', 'Start_Date_Time']).reset_index(drop=True)
                
                # 1. 建立時間區間標記
                df['time_bin'] = df['Start_Date_Time'].dt.floor(f'{interval_minutes}min')
                # 2. 找到每個 (PtName, time_bin) 群組中，'Ch1KeisokuData' 數值最大的那筆資料的索引
                #    如果 'Ch1KeisokuData' 不存在，則退回使用時間戳記取第一筆
                target_col_for_max = 'Ch1KeisokuData' if 'Ch1KeisokuData' in df.columns else 'Start_Date_Time'
                idx_to_keep = df.groupby(['PtName', 'time_bin'])[target_col_for_max].idxmax()
                # 3. 根據索引篩選 DataFrame，並移除輔助欄位
                df = df.loc[idx_to_keep].drop(columns=['time_bin']).reset_index(drop=True)
                
                print(f"Resampling complete. Kept {len(df)} of {before_resample_count} rows.")
            else:
                print("No time_interval set or required columns are missing. Skipping resampling, uploading all data.")

            # --- Filter out rows with None/NaN in critical columns ---
            if not df.empty:
                before_dropna_count = len(df)
                # Define critical columns to check for nulls, e.g., PtName and measurement data.
                critical_cols = [col for col in df.columns if 'KeisokuData' in col or col == 'PtName']
                df.dropna(subset=critical_cols, how='any', inplace=True)
                after_dropna_count = len(df)
                print(f"Filtering None/NaN values in critical columns. Kept {after_dropna_count} of {before_dropna_count} rows.")

            # --- Data Cleaning for specific columns ---
            # Convert full-width to half-width characters
            # str.translate is much faster than applying a function row-by-row.
            full_to_half_map = str.maketrans(
                "＂＃＄％＆＇（）＊＋，－．／０１２３４５６７８９：；＜＝＞？＠ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ［＼］＾＿｀ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ｛｜｝～",
                "\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
            )
            for col in ["SenName", "PtName", "GrpName"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.translate(full_to_half_map)

            if "SenName" in df.columns:
                df["SenName"] = df["SenName"].str.replace(r'[^a-zA-Z]', '', regex=True)
            if "PtName" in df.columns:
                df["PtName"] = df["PtName"].str.replace(r'[^a-zA-Z0-9]', '', regex=True)
            if "GrpName" in df.columns:
                df["GrpName"] = df["GrpName"].str.replace(r'[^a-zA-Z]', '', regex=True)
            print("Applied cleaning rules to SenName, PtName, and GrpName columns.")

            # Generate Serial Number for each row based on its own Start_Date_Time
            if 'Start_Date_Time' in df.columns:
                # Ensure Start_Date_Time is in datetime format before applying the function
                df['Start_Date_Time_dt'] = pd.to_datetime(df['Start_Date_Time'], errors='coerce')
                df['Serial_Number'] = df['Start_Date_Time_dt'].apply(lambda dt: build_serial_from_prefix(sn_prefix, dt))
                df.drop(columns=['Start_Date_Time_dt'], inplace=True)
            else:
                df['Serial_Number'] = build_serial_from_prefix(sn_prefix) # Fallback
            df["Part_Number"] = part_no if part_no else "UNKNOWPN"

            # Any extra ManualAssign fields go to CSV too
            for mk, mv in (manual_items or {}).items():
                df[mk] = mv

            # Column order
            front = ["Serial_Number", "Part_Number", "Start_Date_Time", "Operation", "TestStation", "Site"]
            df = df[front + [c for c in df.columns if c not in front]]

        # Write CSV
        ts_for_csv = datetime.now().strftime("%Y_%m_%dT%H.%M.%S")
        csv_path = Path(csv_dir) / f"{operation}_{ts_for_csv}.csv"
        write_to_csv(csv_path, df if not df.empty else pd.DataFrame())

        # For the XML filename, use the first Serial Number as a representative value
        representative_sn = ""
        if not df.empty and "Serial_Number" in df.columns:
            representative_sn = df["Serial_Number"].iloc[0]

        xml_fp = generate_pointer_xml(
            output_path=Path(output_dir),
            csv_path=csv_path,
            site=site, product_family=product_family, operation=operation, test_station=test_station,
            serial_no=representative_sn, part_no=part_no,
            result_value=result_value, teststep_status_value=teststep_status_value
        )

        print(f"\n✅ Done: {os.path.basename(csv_path)}")
        print(f"📄 XML: {os.path.basename(xml_fp)}")

if __name__ == "__main__":
    main()
