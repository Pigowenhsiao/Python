#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import re
import sys
import uuid
import glob
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from configparser import ConfigParser
import logging
from logging.handlers import RotatingFileHandler
import xml.etree.ElementTree as ET
from xml.dom import minidom


# ========= Logging =========

def setup_logger(log_root: str, operation: str, is_main_logger: bool = False) -> logging.Logger:
    """
    Create a rotating logger.
    For main logger: ../Log/YYYY-MM-DD/main.log
    For operation logger: ../Log/YYYY-MM-DD/operation.log
    """
    date_folder = Path(log_root) / datetime.now().strftime("%Y-%m-%d")
    date_folder.mkdir(parents=True, exist_ok=True)
    log_name = "main" if is_main_logger else operation
    logfile = date_folder / f"{log_name}.log"

    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    # File handler
    handler = RotatingFileHandler(str(logfile), maxBytes=5_000_000,
                                  backupCount=5, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)
    return logger


# ========= Config dataclass =========

@dataclass
class IniSettings:
    # Basic
    site: str
    product_family: str
    operation: str
    test_station: str
    retention_date: int
    file_name_patterns: List[str]

    # Paths
    input_paths: List[str]
    output_path: str
    csv_path: str
    intermediate_data_path: str
    log_path: str

    # CSV section
    data_columns: str  # e.g., "A:L"
    main_skip_rows: int  # 8 means data start at row 8 (1-based human), skiprows = 7

    # DataFields
    field_map: Dict[str, Dict[str, str]]  # key -> {"col": "6", "dtype": "str"}

    # XML defaults
    xml_result_value: str
    xml_teststep_status: str
    xml_part_number_value: str

    # FileSelection (optional)
    fs_prefix: Optional[str]
    fs_timestamp_pattern: Optional[str]
    fs_lot_filter_pos_5_6: Optional[str]
    fs_select_latest_per_day: bool


def _col_letters_to_indices(range_letters: str) -> List[int]:
    """
    Convert Excel-like range "A:L" to zero-based integer column indices [0..11].
    """
    def letter_to_index(s: str) -> int:
        s = s.strip().upper()
        n = 0
        for ch in s:
            n = n * 26 + (ord(ch) - ord('A') + 1)
        return n - 1  # zero-based

    range_letters = range_letters.replace(" ", "")
    if ":" in range_letters:
        left, right = range_letters.split(":", 1)
        li, ri = letter_to_index(left), letter_to_index(right)
        if li <= ri:
            return list(range(li, ri + 1))
        return list(range(ri, li + 1))
    # single column like "G"
    return [letter_to_index(range_letters)]


def load_ini(path: Path) -> IniSettings:
    """Parse INI file into IniSettings."""
    cfg = ConfigParser()
    cfg.read(path, encoding="utf-8")

    # Basic_info
    site = cfg.get("Basic_info", "Site")
    product_family = cfg.get("Basic_info", "ProductFamily")
    operation = cfg.get("Basic_info", "Operation")
    test_station = cfg.get("Basic_info", "TestStation")
    retention = cfg.getint("Basic_info", "Retention_date", fallback=30)
    patterns = [s.strip() for s in cfg.get("Basic_info", "file_name_patterns", fallback="*.csv").split(",")]

    # Paths
    input_paths = [s.strip() for s in cfg.get("Paths", "input_paths").split(",")]
    output_path = cfg.get("Paths", "output_path")
    csv_path = cfg.get("Paths", "CSV_path")
    intermediate = cfg.get("Paths", "intermediate_data_path", fallback="./intermediate/")
    log_path = cfg.get("Paths", "log_path", fallback="./log/")

    # CSV
    data_columns = cfg.get("CSV", "data_columns", fallback="A:L")
    main_skip_rows = cfg.getint("CSV", "main_skip_rows", fallback=8)

    # DataFields
    field_map: Dict[str, Dict[str, str]] = {}
    if cfg.has_option("DataFields", "fields"):
        for line in cfg.get("DataFields", "fields").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                key, col, dtype = map(str.strip, line.split(":", 2))
                field_map[key] = {"col": col, "dtype": dtype}

    # XML_Defaults
    xml_result_value = cfg.get("XML_Defaults", "result_value", fallback="Passed")
    xml_teststep_status = cfg.get("XML_Defaults", "teststep_status_value", fallback="Passed")
    xml_part_number_value = cfg.get("XML_Defaults", "part_number_value", fallback="UNKNOWNPN")

    # FileSelection
    fs_prefix = cfg.get("FileSelection", "prefix", fallback=None) if cfg.has_section("FileSelection") else None
    fs_timestamp_pattern = cfg.get("FileSelection", "timestamp_pattern", fallback=r"\d{8}_\d{6}") \
        if cfg.has_section("FileSelection") else r"\d{8}_\d{6}"
    fs_lot_filter_pos_5_6 = cfg.get("FileSelection", "lot_filter_pos_5_6", fallback=None) \
        if cfg.has_section("FileSelection") else None
    fs_select_latest_per_day = cfg.getboolean("FileSelection", "select_latest_per_day", fallback=True) \
        if cfg.has_section("FileSelection") else True

    return IniSettings(
        site=site,
        product_family=product_family,
        operation=operation,
        test_station=test_station,
        retention_date=retention,
        file_name_patterns=patterns,
        input_paths=input_paths,
        output_path=output_path,
        csv_path=csv_path,
        intermediate_data_path=intermediate,
        log_path=log_path,
        data_columns=data_columns,
        main_skip_rows=main_skip_rows,
        field_map=field_map,
        xml_result_value=xml_result_value,
        xml_teststep_status=xml_teststep_status,
        xml_part_number_value=xml_part_number_value,
        fs_prefix=fs_prefix,
        fs_timestamp_pattern=fs_timestamp_pattern,
        fs_lot_filter_pos_5_6=fs_lot_filter_pos_5_6,
        fs_select_latest_per_day=fs_select_latest_per_day,
    )


# ========= File selection =========

def select_files_per_rules(input_dir: Path, pattern: str, s: IniSettings, logger: logging.Logger) -> List[Path]:
    """
    Apply [FileSelection] rules:
      - prefix (e.g., "RC")
      - 19-char alnum between '25' and next '_', with pos 5-6 = '1B'
      - filename ends with 13-digit timestamp YYYYMMDD_HHMMSS
      - pick latest per day if requested
    """
    files = [p for p in input_dir.glob(pattern) if p.is_file()]
    logger.info(f"[Select] scanning {input_dir} with pattern '{pattern}', found {len(files)} files")
    print(f"[Cond1] .csv files, total: {len(files)}")

    # Condition 2: prefix
    files_prefix = [f for f in files if not s.fs_prefix or f.name.startswith(s.fs_prefix)]
    print(f"[Cond2] prefix='{s.fs_prefix}', matched: {len(files_prefix)}")

    # Condition 3: 25+19 chars
    files_25_19 = []
    seg_dict = {}
    for f in files_prefix:
        m = re.search(r"25([A-Za-z0-9]{19})_", f.name)
        if m:
            files_25_19.append(f)
            seg_dict[f] = m.group(1)
    print(f"[Cond3] 25+19 chars, matched: {len(files_25_19)}")

    # Condition 4: lot_filter_pos_5_6
    files_lot = []
    for f in files_25_19:
        seg = seg_dict[f]
        # 8th-9th char must be '1B'
        if not s.fs_lot_filter_pos_5_6 or (len(seg) >= 9 and seg[2:4] == (s.fs_lot_filter_pos_5_6)):
            files_lot.append(f)
    print(f"[Cond4] lot_filter_pos_5_6='{s.fs_lot_filter_pos_5_6}', matched: {len(files_lot)}")

    # Condition 5: timestamp
    ts_re = re.compile(s.fs_timestamp_pattern or r"\d{8}_\d{6}")
    files_ts = []
    ts_dict = {}
    date_dict = {}
    for f in files_lot:
        m_ts = ts_re.search(f.name)
        if m_ts:
            files_ts.append(f)
            ts_dict[f] = m_ts.group(0)
            date_match = re.search(r"_(\d{8})_\d{6}\.csv$", f.name)
            if date_match:
                date_part = date_match.group(1)
            else:
                date_part = ""
            date_dict[f] = date_part
    print(f"[Cond5] timestamp_pattern='{s.fs_timestamp_pattern}', matched: {len(files_ts)}")

    # Show all matched files and their date
    print("[Matched files and date]:")
    for f in files_ts:
        print(f"  {f.name}  date={date_dict[f]}")

    # Only keep the latest date file (only one)
    selected: List[Path] = []
    if files_ts:
        all_dates = [date_dict[f] for f in files_ts if date_dict[f]]
        if all_dates:
            latest_date = max(all_dates)
            latest_files = [f for f in files_ts if date_dict[f] == latest_date]
            if latest_files:
                latest_file = max(latest_files, key=lambda f: ts_dict[f])
                selected = [latest_file]
    print(f"[Cond6] Latest date ({latest_date if files_ts else 'N/A'}), matched: {len(selected)}")
    if selected:
        print(f"[Selected latest file]: {selected[0].name}")

    logger.info(f"[Select] selected {len(selected)} file(s)")
    return sorted(selected)


# ========= IO helpers =========

def read_csv_data(csv_path: Path, s: IniSettings, logger: logging.Logger) -> pd.DataFrame:
    """
    Read CSV with cp932, auto-sep, skipping headers so that data starts at row 8.
    Limit columns to A:L (0..11) as configured.
    """
    use_indices = _col_letters_to_indices(s.data_columns)  # e.g., [0..11]
    skiprows = max(s.main_skip_rows - 1, 0)  # "8" means start at 8th row -> skip 7
    logger.info(f"[CSV] reading {csv_path.name}, usecols={use_indices}, skiprows={skiprows}")

    df = pd.read_csv(
        csv_path,
        sep=None,  # auto-detect
        engine="python",
        header=None,
        encoding="cp932",
        usecols=use_indices,
        skiprows=skiprows,
        dtype=str,  # keep raw as str first; we only need category counts
    )
    df.columns = list(range(df.shape[1]))  # normalize integer columns
    logger.info(f"[CSV] loaded shape={df.shape}")
    return df


def count_category_g(df: pd.DataFrame, s: IniSettings, logger: logging.Logger) -> pd.DataFrame:
    """
    Count unique categories in G column (7th col, index=6).
    """
    # decide G column index:
    g_idx = 6  # default G (0-based)
    # if there's mapping in field_map (e.g., key_Judge:6), prefer it:
    for k, meta in s.field_map.items():
        if k.lower() == "key_judge":
            try:
                g_idx = int(meta.get("col", "6"))
            except ValueError:
                g_idx = 6
            break

    if g_idx >= df.shape[1]:
        logger.warning(f"[COUNT] G index {g_idx} out of bounds for df with {df.shape[1]} columns; using last column")
        g_idx = df.shape[1] - 1

    series = df.iloc[:, g_idx].astype(str).str.strip()
    series = series[series.notna() & (series != "")]

    counts = series.value_counts(dropna=True).rename_axis("Judge").reset_index(name="Count")
    # Exclude Judge == "nan" or "0"
    counts = counts[~counts["Judge"].isin(["nan", "0"])]
    logger.info(f"[COUNT] unique categories={counts.shape[0]}, total rows={len(series)}")
    return counts


def write_result_csv(counts: pd.DataFrame, s: IniSettings, logger: logging.Logger, src_csv_filename: str) -> Path:
    """
    Emit CSV with the SAME header style as existing system:
      Always include: Serial_Number, Part_Number, Start_Date_Time, Operation, TestStation, Site
      Then include domain columns: Judge, Count
      Unknowns are "Wait assign"
    """
    Path(s.csv_path).mkdir(parents=True, exist_ok=True)
    # Extract yyyymmdd from RAW CSV filename
    m = re.search(r"_(\d{8})_", src_csv_filename)
    if m:
        yyyymmdd = m.group(1)  # YYYYMMDD
        yymmdd = yyyymmdd[2:]  # YYMMDD
    else:
        yyyymmdd = datetime.now().strftime("%Y%m%d")
        yymmdd = datetime.now().strftime("%y%m%d")
    serial_number = f"LNG{yymmdd}"
    part_number = s.xml_part_number_value if s.xml_part_number_value else "Dummy"

    # Use RAW CSV date for output CSV filename
    now = datetime.now()
    uid = uuid.uuid4().hex[:6]
    out_csv = Path(s.csv_path) / f"{s.operation}_{yyyymmdd}_{uid}.csv"

    base = pd.DataFrame({
        "Serial_Number": [serial_number] * len(counts),
        "Part_Number": [part_number] * len(counts),
        "Start_Date_Time": [now.strftime("%Y-%m-%d %H:%M:%S")] * len(counts),
        "Operation": [s.operation] * len(counts),
        "TestStation": [s.test_station] * len(counts),
        "Site": [s.site] * len(counts),
        "Operator": ["None"] * len(counts),
    })
    dom = counts.rename(columns={"Judge": "Judge", "Count": "Count"})
    df_out = pd.concat([base.reset_index(drop=True), dom.reset_index(drop=True)], axis=1)

    ordered_cols = [
        "Serial_Number", "Part_Number", "Start_Date_Time",
        "Operation", "TestStation", "Site", "Operator",
        "Judge", "Count"
    ]
    df_out = df_out[ordered_cols]

    df_out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    logger.info(f"[CSV OUT] {out_csv}")
    return out_csv


def generate_pointer_xml(csv_path: Path, s: IniSettings, logger: logging.Logger, src_csv_filename: str) -> Path:
    """
    Generate pointer XML with the SAME element/attributes as legacy:
    <Results>
      <Result startDateTime=... endDateTime=... Result=...>
        <Header SerialNumber=..., PartNumber=..., Operation=..., TestStation=...,
                Operator=..., StartTime=..., Site=..., LotNumber=... />
        <TestStep Name=... startDateTime=... endDateTime=... Status=...>
          <Data DataType="Table" Name="tbl_<OPERATION_UPPER>" Value="<csv_path>" CompOperation="LOG" />
    """
    Path(s.output_path).mkdir(parents=True, exist_ok=True)
    # Extract yyyymmdd from RAW CSV filename
    m = re.search(r"_(\d{8})_", src_csv_filename)
    if m:
        yymmdd = m.group(1)[2:]  # YYMMDD
    else:
        yymmdd = datetime.now().strftime("%y%m%d")
    serial_number = f"LNG{yymmdd}"
    operator_value = "None"
    lot_number_value = "Dummy"
    part_number_value = s.xml_part_number_value if s.xml_part_number_value else "Dummy"

    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    xml_file_name = (
        f"Site={s.site},"
        f"ProductFamily={s.product_family},"
        f"Operation={s.operation},"
        f"Partnumber={part_number_value},"
        f"Serialnumber={serial_number},"
        f"Testdate={now_iso}.xml"
    ).replace(":", ".")
    xml_path = Path(s.output_path) / xml_file_name

    results = ET.Element(
        "Results",
        {"xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
         "xmlns:xsd": "http://www.w3.org/2001/XMLSchema"}
    )
    result = ET.SubElement(
        results, "Result",
        startDateTime=now_iso, endDateTime=now_iso, Result=s.xml_result_value or "Passed"
    )
    # Header
    ET.SubElement(
        result, "Header",
        SerialNumber=serial_number,
        PartNumber=part_number_value,
        Operation=s.operation,
        TestStation=s.test_station,
        Operator=operator_value,
        StartTime=now_iso,
        Site=s.site,
        LotNumber=lot_number_value,
    )
    # TestStep
    test_step = ET.SubElement(
        result, "TestStep",
        Name=s.operation, startDateTime=now_iso, endDateTime=now_iso,
        Status=s.xml_teststep_status or "Passed"
    )
    ET.SubElement(
        test_step, "Data",
        DataType="Table",
        Name=f"tbl_{s.operation.upper()}",
        Value=str(csv_path),
        CompOperation="LOG"
    )

    pretty = minidom.parseString(ET.tostring(results)).toprettyxml(indent="  ", encoding="utf-8")
    with open(xml_path, "wb") as f:
        f.write(pretty)
    logger.info(f"[XML OUT] {xml_path}")
    return xml_path


# ========= Main orchestration =========

def process_one_ini(ini_path: Path) -> None:
    s = load_ini(ini_path)
    logger = setup_logger(s.log_path, s.operation)
    logger.info(f"===== Start processing INI: {ini_path.name} =====")

    total_outputs = 0
    for in_dir in s.input_paths:
        base = Path(in_dir)
        if not base.exists():
            logger.warning(f"[WARN] input path not found: {base}")
            print(f"[WARN] input path not found: {base}")
            continue

        matched_files: List[Path] = []
        for pat in s.file_name_patterns:
            selected = select_files_per_rules(base, pat, s, logger)
            matched_files.extend(selected)

        # Print found file list in English
        file_list = [str(f) for f in matched_files]
        #print(f"\n[Found file list]:\n{file_list}")

        #print(f"\n[STEP] File list ({base}):")
        for f in matched_files:
            print(f"  - {f}")

        if not matched_files:
            logger.info("[INFO] no files matched selection rules")
            print("[INFO] no files matched selection rules")
            continue

        # process each file independently (your rule #4)
        for csv_file in matched_files:
            try:
                #print(f"\n[STEP] Reading file: {csv_file}")
                df = read_csv_data(csv_file, s, logger)
                #print(f"[STEP] Read complete, rows: {len(df)}, columns: {len(df.columns)}")
                #print(f"[STEP] First 5 rows:\n{df.head()}")

                counts = count_category_g(df, s, logger)
                #print(f"[STEP] Judge column category count:\n{counts}")

                if counts.empty:
                    logger.info(f"[SKIP] no categories found in G column for {csv_file.name}")
                    print(f"[SKIP] no categories found in G column for {csv_file.name}")
                    continue

                out_csv = write_result_csv(counts, s, logger, src_csv_filename=csv_file.name)
                #print(f"[STEP] Result written to CSV: {out_csv}")

                xml_fp = generate_pointer_xml(out_csv, s, logger, src_csv_filename=csv_file.name)
                #print(f"[STEP] XML pointer file generated: {xml_fp}")

                total_outputs += 1
            except Exception as e:
                logger.error(f"[ERROR] processing {csv_file.name}: {e}\n{traceback.format_exc()}")
                print(f"[ERROR] processing {csv_file.name}: {e}")
                continue  # except block must have content

    logger.info(f"===== Finished INI: {ini_path.name} ; outputs={total_outputs} =====")
    print(f"===== Finished INI: {ini_path.name} ; outputs={total_outputs} =====")

def main() -> None:
    # run in current directory: find all .ini
    cwd = Path(os.getcwd())
    ini_files = [Path(p) for p in glob.glob("*.ini")]
    if not ini_files:
        print("No .ini found in current directory.")
        return

    for ini in ini_files:
        process_one_ini(ini)

if __name__ == "__main__":
    main()

# Example comparison code for two filenames:
def compare_csv_dates(file1: str, file2: str) -> str:
    # Extract YYYYMMDD_HHMMSS from filenames
    def get_datetime(fname):
        m = re.search(r"_(\d{8}_\d{6})\.csv$", fname)
        if m:
            return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
        return None
    dt1 = get_datetime(file1)
    dt2 = get_datetime(file2)
    if dt1 and dt2:
        if dt1 > dt2:
            return f"{file1} is newer"
        elif dt2 > dt1:
            return f"{file2} is newer"
        else:
            return "Both files have the same timestamp"
    return "Cannot compare, invalid filename format"

# Usage example:
result = compare_csv_dates(
    "RC_25CS1B301Ba0000000001_20251101_230722.csv",
    "RC_25FJ1B658Ba0000000000_20251104_222429.csv"
)
print(result)
# Output: RC_25FJ1B658Ba0000000000_20251104_222429.csv is newer
