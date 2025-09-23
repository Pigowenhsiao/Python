#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Version: 1.1.0
Last Modified: 2025-07-18

Description:
A generic script to process specific data ranges from Excel files based on configurations
provided in INI files. The script is designed to be modular, processing each INI file
found in its directory as a separate task.

Changelog:
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
        xls = pd.ExcelFile(excel_file_path)
        all_sheets = xls.sheet_names
        target_sheet = find_latest_sheet(all_sheets, sheet_pattern)

        if not target_sheet:
            return

        df_full = pd.read_excel(xls, sheet_name=target_sheet, header=None)
    except Exception as e:
        error_msg = f"Error reading Excel file {excel_file_path}: {e}"
        print(f"\nERROR: {error_msg}")
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
            print(f"\nERROR: {error_msg}")
            logging.error(error_msg)
            return
        
        first_col_name = list(fields_config.keys())[0]
        if first_col_name in df_processed.columns:
            df_processed.dropna(subset=[first_col_name], inplace=True)
        
        df_processed.reset_index(drop=True, inplace=True)

    except Exception as e:
        error_msg = f"Error slicing or transposing data from sheet '{target_sheet}': {e}"
        print(f"\nERROR: {error_msg}")
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
            logging.info(f"Dropped {dropped_rows} rows due to data type errors in columns: {', '.join(numeric_columns)}")

    if df_processed.empty:
        logging.info(f"No valid data rows left after strict type validation for '{output_prefix}'. Skipping.")
        return

    # Filter data based on Running_date from INI.
    running_date = int(basic_info.get('running_date', 0))
    date_col_key = 'key_start_date_time'
    
    if running_date > 0 and date_col_key in df_processed.columns:
        # Convert the date column to datetime objects for comparison.
        # Errors will be converted to NaT (Not a Time).
        df_processed['datetime_col_temp'] = pd.to_datetime(df_processed[date_col_key], errors='coerce')

        # Drop rows where date conversion failed.
        original_rows = len(df_processed)
        df_processed.dropna(subset=['datetime_col_temp'], inplace=True)
        
        # Calculate the cutoff date.
        cutoff_date = datetime.now() - timedelta(days=running_date)
        
        # Keep only the rows with a date on or after the cutoff date.
        df_processed = df_processed[df_processed['datetime_col_temp'] >= cutoff_date].copy()

        # Clean up the temporary datetime column.
        df_processed.drop(columns=['datetime_col_temp'], inplace=True)
        
        dropped_rows = original_rows - len(df_processed)
        if dropped_rows > 0:
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

    # Generate the corresponding XML metadata file.
    part_number_col_clean = 'part_number'
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

# ---------------------------------------------------------------------------
# INI Processing & Main Program
# ---------------------------------------------------------------------------
def run_process_from_ini(config_path: str) -> None:
    cfg = ConfigParser()
    try:
        cfg.read(config_path, encoding="utf-8")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to read INI file {config_path}: {e}")
        traceback.print_exc()
        return

    ini_name = os.path.splitext(os.path.basename(config_path))[0]
    log_dir_fallback = os.path.join(os.path.dirname(config_path), '..', 'Log')
    log_dir = cfg.get("Logging", "log_path", fallback=log_dir_fallback)
    today_str = datetime.today().strftime("%Y-%m-%d")
    log_file_path = os.path.join(log_dir, today_str, f"{ini_name}.log")
    setup_logging(log_file_path)

    try:
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
        
    except (NoSectionError, NoOptionError) as e:
        error_msg = f"INI file '{config_path}' is missing a required section or option: {e}"
        print(f"\nERROR: Initialization failed. See log for details: {log_file_path}")
        print(f"DETAILS: {error_msg}")
        logging.critical(error_msg)
        return

    file_pattern = basic_info.get("file_name_pattern", "*.xlsx")
    input_paths = [p.strip() for p in paths.get('input_paths', '').splitlines() if p.strip()]

    if not input_paths:
        logging.error("No input_paths defined in the INI file.")
        print("ERROR: No input_paths defined in the INI file. Processing stopped.")
        return
        
    for ipath in input_paths:
        matched_files = glob.glob(os.path.join(ipath, file_pattern))
        logging.info(f"Found {len(matched_files)} files matching '{file_pattern}' in '{ipath}'.")
        for f in matched_files:
            try:
                dst_dir = paths.get("copy_destination_path", "./copied_files/")
                os.makedirs(dst_dir, exist_ok=True)
                copied_path = shutil.copy(f, dst_dir)
                logging.info(f"Copied {f} -> {copied_path}")

                process_excel_file(copied_path, source_config, fields_config, basic_info, paths)
            
            except Exception as e:
                error_msg = f"A critical error occurred while processing file {f}: {e}"
                print(f"\nERROR: {error_msg}")
                traceback.print_exc()
                logging.error(error_msg, exc_info=True)

def main() -> None:
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
        
    ini_files = glob.glob(os.path.join(script_dir, "*.ini"))
    
    if not ini_files:
        print("No .ini files found in the current directory.")
        return
        
    print(f"Found {len(ini_files)} configuration file(s) to process: {', '.join([os.path.basename(f) for f in ini_files])}")
    for ini_file in ini_files:
        print(f"\n----- Processing: {os.path.basename(ini_file)} -----")
        run_process_from_ini(ini_file)
        print(f"----- Finished: {os.path.basename(ini_file)} -----")
    
    print("\nAll processing runs complete.")

if __name__ == "__main__":
    main()