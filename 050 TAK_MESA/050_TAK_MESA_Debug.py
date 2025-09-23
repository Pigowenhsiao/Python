#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Version: 1.2.0
Last Modified: 2025-08-07

Description:
A generic script to process specific data ranges from Excel files based on configurations
provided in INI files. The script is designed to be modular, processing each INI file
found in its directory as a separate task.

Changelog:
[V1.2.0]: Added print statements for real-time console monitoring of script progress.
[V1.1.0]: Re-implemented the Running_date filter to retain only recent data.
[V1.0.0]: Initial stable release with English comments and all features.
"""

import os
import sys
import glob
import shutil
import logging
import random
import re
import traceback
import numpy as np
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timedelta
from configparser import ConfigParser, NoSectionError, NoOptionError
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd

# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

def setup_logging(log_file_path: str) -> None:
    """
    Configures the logging settings for the script execution.
    It removes existing handlers to ensure clean logging for each INI file.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    print(f"📝 日誌檔案已設定於: {log_file_path}") # <--- 新增監控點

def get_col_index_from_char(col_char: str) -> int:
    """Converts a column letter (e.g., 'F') to a zero-based integer index (e.g., 5)."""
    return ord(col_char.upper()) - ord('A')

def find_latest_sheet(all_sheets: List[str], pattern: str) -> Optional[str]:
    """
    Finds the latest version of a sheet in a list of sheet names.
    The latest version is determined by the largest number in parentheses, e.g., "Sheet (3)".
    A sheet with no number is considered version 0.
    """
    latest_sheet = None
    latest_version = -1
    
    regex = re.compile(re.escape(pattern) + r'(?:\s*\(([0-9]+)\))?$')

    for sheet in all_sheets:
        match = regex.match(sheet.strip())
        if match:
            version_str = match.group(1)
            version = int(version_str) if version_str else 0
            
            if version >= latest_version:
                latest_version = version
                latest_sheet = sheet
    
    if latest_sheet:
        logging.info(f"Found latest sheet '{latest_sheet}' for pattern '{pattern}'.")
    else:
        logging.warning(f"No sheet found matching pattern '{pattern}'.")
        
    return latest_sheet

# ---------------------------------------------------------------------------
# Main Data Processing Workflow
# ---------------------------------------------------------------------------

def process_excel_file(
    excel_file_path: str,
    source_config: Dict[str, Any],
    fields_config: Dict[str, Tuple[str, str]],
    basic_info: Dict[str, Any],
    paths: Dict[str, str]
) -> None:
    """
    Reads data from a single source within an Excel file, processes it, and generates outputs.
    """
    sheet_pattern = source_config['sheet_pattern']
    output_prefix = source_config['output_prefix']
    
    logging.info(f"--- Processing data source '{output_prefix}' from file: {excel_file_path} ---")

    try:
        print(f"  - 正在讀取 Excel 檔案...") # <--- 新增監控點
        xls = pd.ExcelFile(excel_file_path)
        all_sheets = xls.sheet_names
        target_sheet = find_latest_sheet(all_sheets, sheet_pattern)

        if not target_sheet:
            print(f"  - ❗ 警告: 在檔案中找不到符合 '{sheet_pattern}' 格式的工作表，跳過此檔案。") # <--- 新增監控點
            return

        print(f"  - 找到了目標工作表: '{target_sheet}'") # <--- 新增監控點
        df_full = pd.read_excel(xls, sheet_name=target_sheet, header=None)
    except Exception as e:
        error_msg = f"Error reading Excel file {excel_file_path}: {e}"
        print(f"\n❌ 錯誤: 讀取 Excel 檔案失敗: {error_msg}") # <--- 新增監控點
        traceback.print_exc()
        logging.error(error_msg, exc_info=True)
        return

    try:
        start_cell = source_config['start_cell']
        end_row = int(source_config['end_row'])
        should_transpose = source_config.get('transpose', 'False').lower() == 'true'

        start_row_match = re.search(r'(\d+)', start_cell)
        start_col_match = re.search(r'([A-Z]+)', start_cell, re.IGNORECASE)
        if not start_row_match or not start_col_match:
            raise ValueError("start_cell in INI must be in a valid format like 'F20'")

        start_row_idx = int(start_row_match.group(1)) - 1
        start_col_idx = get_col_index_from_char(start_col_match.group(1))
        end_row_idx = end_row - 1

        df_sliced = df_full.iloc[start_row_idx:end_row_idx + 1, start_col_idx:].copy()
        
        df_sliced.dropna(axis=1, how='all', inplace=True)

        if df_sliced.empty:
            logging.warning(f"No data found in the specified range for sheet '{target_sheet}'.")
            return

        df_processed = df_sliced.T if should_transpose else df_sliced

        if len(df_processed.columns) == len(fields_config.keys()):
            df_processed.columns = fields_config.keys()
        else:
            error_msg = f"Column count mismatch. Expected {len(fields_config.keys())} columns but got {len(df_processed.columns)} for sheet '{target_sheet}'."
            print(f"\n❌ 錯誤: 欄位數量不符. {error_msg}") # <--- 新增監控點
            logging.error(error_msg)
            return
        
        first_col_name = list(fields_config.keys())[0]
        if first_col_name in df_processed.columns:
            df_processed.dropna(subset=[first_col_name], inplace=True)
        
        df_processed.reset_index(drop=True, inplace=True)

    except Exception as e:
        error_msg = f"Error slicing or transposing data from sheet '{target_sheet}': {e}"
        print(f"\n❌ 錯誤: 擷取或轉置資料時出錯: {error_msg}") # <--- 新增監控點
        traceback.print_exc()
        logging.error(error_msg, exc_info=True)
        return

    if df_processed.empty:
        logging.info(f"No valid data rows after initial processing for '{output_prefix}'. Skipping.")
        return

    # Perform strict data type validation for numeric columns.
    numeric_columns = []
    for col_name, (_, dtype) in fields_config.items():
        if col_name in df_processed.columns and dtype in ['int', 'float']:
            numeric_columns.append(col_name)
            df_processed[col_name] = pd.to_numeric(df_processed[col_name], errors='coerce')
    
    if numeric_columns:
        original_rows = len(df_processed)
        df_processed.dropna(subset=numeric_columns, inplace=True)
        dropped_rows = original_rows - len(df_processed)
        if dropped_rows > 0:
            print(f"  - 🧹 資料清理: 因數值格式錯誤移除了 {dropped_rows} 筆資料。") # <--- 新增監控點
            logging.info(f"Dropped {dropped_rows} rows due to data type errors in columns: {', '.join(numeric_columns)}")

    if df_processed.empty:
        logging.info(f"No valid data rows left after strict type validation for '{output_prefix}'. Skipping.")
        return

    # Filter data based on Running_date from INI.
    running_date = int(basic_info.get('running_date', 0))
    date_col_key = 'key_start_date_time'
    
    if running_date > 0 and date_col_key in df_processed.columns:
        df_processed['datetime_col_temp'] = pd.to_datetime(df_processed[date_col_key], errors='coerce')

        original_rows = len(df_processed)
        df_processed.dropna(subset=['datetime_col_temp'], inplace=True)
        
        cutoff_date = datetime.now() - timedelta(days=running_date)
        
        df_processed = df_processed[df_processed['datetime_col_temp'] >= cutoff_date].copy()

        df_processed.drop(columns=['datetime_col_temp'], inplace=True)
        
        dropped_rows = original_rows - len(df_processed)
        if dropped_rows > 0:
            print(f"  - ⏳ 日期篩選: 移除了 {dropped_rows} 筆舊資料 (超過 {running_date} 天)。") # <--- 新增監控點
            logging.info(f"Dropped {dropped_rows} rows older than {running_date} days or with invalid date format.")

    if df_processed.empty:
        logging.info(f"No data left after date filtering for '{output_prefix}'. Skipping.")
        return
        
    # Custom transformation - Add 'X' prefix to specific part numbers.
    part_number_col_key = 'key_part_number'
    if part_number_col_key in df_processed.columns:
        condition = df_processed[part_number_col_key].astype(str).str.startswith('QJ-30150', na=False)
        df_processed[part_number_col_key] = np.where(condition, 'X' + df_processed[part_number_col_key], df_processed[part_number_col_key])
        logging.info("Applied 'X' prefix to relevant part_number values.")
    
    # Clean column headers for the final CSV output by removing "key_".
    clean_column_names = {col: col.replace('key_', '', 1) for col in df_processed.columns}
    df_processed.rename(columns=clean_column_names, inplace=True)

    # Add metadata columns from INI.
    df_processed['ProductFamily'] = basic_info['productfamily']
    df_processed['Operation'] = basic_info['operation']

    # Save the processed data to a CSV file.
    ts = datetime.now().strftime("%Y%m%d%H%M") + f"{random.randint(10,99)}"
    csv_name = f"TAK_CVD_{output_prefix}_{ts}.csv"
    csv_path = os.path.join(paths['csv_path'], csv_name)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df_processed.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logging.info(f"CSV for '{output_prefix}' saved to: {csv_path}")
    print(f"  - ✔️ CSV 檔案已儲存: {os.path.basename(csv_path)}") # <--- 新增監控點

    # Generate the corresponding XML metadata file.
    part_number_col_clean = 'Part_Number'
    generate_xml(
        output_path=paths['output_path'],
        csv_path=csv_path,
        serial_no=ts,
        part_number="UNKNOWPN",
        prefix=output_prefix,
        basic_info=basic_info
    )

# ---------------------------------------------------------------------------
# XML Generation
# ---------------------------------------------------------------------------
def generate_xml(
    output_path: str, csv_path: str, serial_no: str, part_number: str,
    prefix: str, basic_info: Dict[str, Any]
) -> None:
    os.makedirs(output_path, exist_ok=True)
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    xml_file_name = (
        f"Site={basic_info['site']},"
        f"ProductFamily={basic_info['productfamily']},"
        f"Operation={basic_info['operation']},"
        f"Partnumber={part_number},"
        f"Serialnumber={serial_no},"
        f"Testdate={now_iso}.xml"
    ).replace(":", ".")
    xml_file_path = os.path.join(output_path, xml_file_name)
    results = ET.Element("Results", {"xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance", "xmlns:xsd": "http://www.w3.org/2001/XMLSchema"})
    result = ET.SubElement(results, "Result", startDateTime=now_iso, endDateTime=now_iso, Result="Passed")
    ET.SubElement(
        result, "Header",
        SerialNumber=serial_no, PartNumber=part_number,
        Operation=f"{basic_info.get('operation', 'NA')}",
        TestStation=basic_info.get('teststation', 'NA'),
        Operator="NA", StartTime=now_iso, Site=basic_info.get('site', 'NA'),
        LotNumber="", Quantity="",
    )
    ET.SubElement(result, "HeaderMisc").append(ET.Element("Item", Description=""))
    test_step = ET.SubElement(result, "TestStep", Name=f"{basic_info.get('operation', 'NA')}", startDateTime=now_iso, endDateTime=now_iso, Status="Passed")
    ET.SubElement(test_step, "Data", DataType="Table", Name=f"tbl_{prefix.upper()}", Value=csv_path, CompOperation="LOG")
    xml_str = minidom.parseString(ET.tostring(results)).toprettyxml(indent="  ", encoding="utf-8")
    with open(xml_file_path, "wb") as f:
        f.write(xml_str)
    logging.info(f"XML for '{prefix}' saved to: {xml_file_path}")
    print(f"  - ✔️ XML 檔案已儲存: {os.path.basename(xml_file_path)}") # <--- 新增監控點

# ---------------------------------------------------------------------------
# INI Processing & Main Program
# ---------------------------------------------------------------------------
def run_process_from_ini(config_path: str) -> None:
    cfg = ConfigParser()
    try:
        cfg.read(config_path, encoding="utf-8")
    except Exception as e:
        print(f"❌ 嚴重錯誤: 無法讀取 INI 檔案 {config_path}: {e}") # <--- 新增監控點
        traceback.print_exc()
        return

    ini_name = os.path.splitext(os.path.basename(config_path))[0]
    log_dir_fallback = os.path.join(os.path.dirname(config_path), '..', 'Log')
    log_dir = cfg.get("Logging", "log_path", fallback=log_dir_fallback)
    today_str = datetime.today().strftime("%Y-%m-%d")
    log_file_path = os.path.join(log_dir, today_str, f"{ini_name}.log")
    setup_logging(log_file_path)

    try:
        print("⚙️  正在讀取 INI 設定檔...") # <--- 新增監控點
        basic_info = dict(cfg.items("Basic_info"))
        paths = dict(cfg.items("Paths"))
        source_config = dict(cfg.items("DataSource"))
        
        fields_raw = [l.strip() for l in cfg.get("DataFields", "fields").splitlines() if l.strip()]
        fields_config: Dict[str, Tuple[str, str]] = {}
        for line in fields_raw:
            try:
                key, col, dtype = (s.strip() for s in line.split(":"))
                fields_config[key] = (col, dtype)
            except ValueError:
                logging.warning(f"Skipping malformed line in [DataFields]: {line}")
        print("...INI 設定檔讀取成功。") # <--- 新增監控點
        
    except (NoSectionError, NoOptionError) as e:
        error_msg = f"INI 檔案 '{config_path}' 缺少必要的區塊或選項: {e}"
        print(f"\n❌ 錯誤: 初始化失敗，詳情請見日誌檔案: {log_file_path}") # <--- 新增監控點
        print(f"詳細資訊: {error_msg}") # <--- 新增監OK點
        logging.critical(error_msg)
        return

    file_pattern = basic_info.get("file_name_pattern", "*.xlsx")
    input_paths = [p.strip() for p in paths.get('input_paths', '').splitlines() if p.strip()]

    if not input_paths:
        logging.error("No input_paths defined in the INI file.")
        print("❌ 錯誤: INI 檔案中未定義 'input_paths'，處理已停止。") # <--- 新增監控點
        return
        
    for ipath in input_paths:
        print(f"\n📁 正在 '{ipath}' 中尋找檔案...") # <--- 新增監控點
        matched_files = glob.glob(os.path.join(ipath, file_pattern))
        logging.info(f"Found {len(matched_files)} files matching '{file_pattern}' in '{ipath}'.")
        print(f"   -> 找到了 {len(matched_files)} 個符合 '{file_pattern}' 的檔案。") # <--- 新增監控點
        
        for f in matched_files:
            try:
                print(f"\n▶️  開始處理檔案: {os.path.basename(f)}") # <--- 新增監控點
                dst_dir = paths.get("copy_destination_path", "./copied_files/")
                os.makedirs(dst_dir, exist_ok=True)
                copied_path = shutil.copy(f, dst_dir)
                logging.info(f"Copied {f} -> {copied_path}")
                print(f"  - 已複製檔案到處理區: {os.path.basename(copied_path)}") # <--- 新增監控點

                process_excel_file(copied_path, source_config, fields_config, basic_info, paths)
            
            except Exception as e:
                error_msg = f"A critical error occurred while processing file {f}: {e}"
                print(f"\n❌ 嚴重錯誤: 處理檔案 {f} 時發生錯誤: {error_msg}") # <--- 新增監控點
                traceback.print_exc()
                logging.error(error_msg, exc_info=True)

def main() -> None:
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
        
    ini_files = glob.glob(os.path.join(script_dir, "*.ini"))
    
    if not ini_files:
        print("在當前目錄下找不到任何 .ini 檔案。")
        return
        
    print(f"🔍 找到 {len(ini_files)} 個設定檔，準備處理: {', '.join([os.path.basename(f) for f in ini_files])}")
    for ini_file in ini_files:
        print(f"\n----- Processing: {os.path.basename(ini_file)} -----")
        run_process_from_ini(ini_file)
        print(f"----- Finished: {os.path.basename(ini_file)} -----")
    
    print("\n✅ 所有處理任務皆已完成。")

if __name__ == "__main__":
    main()