#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Modified on 2025‑07‑09
Based on the latest field definitions in Config_TAK_PLX.ini 
"""

import os
import sys
import glob
import shutil
import logging
import random
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timedelta
from configparser import ConfigParser, NoSectionError, NoOptionError
from typing import List, Dict, Any
import pandas as pd

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def setup_logging(log_file_path: str) -> None:
    """Configure the logging format and file location."""
    # To reconfigure logging for each INI file inside a loop, remove existing handlers first.
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

# ---------------------------------------------------------------------------
# Main data processing workflow
# ---------------------------------------------------------------------------

def process_excel_file(
    excel_file: str,
    sheet_name: str,
    data_columns: str,
    data_row: int,
    output_path: str,
    csv_base_path: str,
    pl_tool: str,
    fields_cfg: Dict[str, tuple[str, str]],
    site: str,
    product_family: str,
    operation: str,
    test_station: str,
    running_date: int,
) -> None:
    """Read the Excel file according to settings, export to CSV, and generate XML."""

    logging.info(f"Processing Excel file: {excel_file}")

    # -------------------------------------------------------------------
    # 1) Read Excel
    # -------------------------------------------------------------------
    try:
        skiprows = data_row - 1  # Data_Row starts at 1 in the INI file
        df = pd.read_excel(
            excel_file,
            header=None,
            sheet_name=sheet_name,
            usecols=data_columns,
            skiprows=skiprows,
        )
        df["key_SORTNUMBER"] = df.index + data_row  # Track the original row number
    except Exception as e:
        logging.error(f"Error reading {excel_file}: {e}")
        return

    # According to requirements, search the Part_Number column (column 2) for 'QJ'
    # Keep only rows where 'QJ' is found, and update that column to the 8 characters starting from 'QJ'.

    # Ensure column 3 is a string for processing
    df[2] = df[2].astype(str)

    # Use a regex to extract the 8 characters starting with 'QJ'; .str.extract returns NaN if not found
    df[2] = "X"+df[2].str.extract(r'(QJ.{6})', expand=False)

    # Remove rows where Part_Number is NaN (no 'QJ' or invalid format)
    original_rows_before_qj_filter = len(df)
    df.dropna(subset=[2], inplace=True)
    dropped_rows = original_rows_before_qj_filter - len(df)
    if dropped_rows > 0:
        logging.info(
            f"Dropped {dropped_rows} rows from {excel_file} because a valid "
            f"Part_Number starting with 'QJ' was not found."
        )

    if df.empty:
        logging.info(f"No valid data rows left in {excel_file} after filtering for 'QJ' Part_Number. Skipping file.")
        return

    # -------------------------------------------------------------------
    # 2) Dynamic field mapping
    # -------------------------------------------------------------------
    # Convert fields parsed from the INI file into integer positions
    fields: Dict[str, tuple[int, str]] = {
        k: (int(v[0]), v[1]) for k, v in fields_cfg.items()
    }
    if "key_SORTNUMBER" not in fields:
        # Place this helper column at the last position
        fields["key_SORTNUMBER"] = (df.shape[1] - 1, "int")

    # Dynamically select all fields defined in the INI file
    all_field_keys = list(fields.keys())
    col_idx = [fields[k][0] for k in all_field_keys]
    df1 = df.iloc[:, col_idx].copy()
    df1.columns = all_field_keys

    # -------------------------------------------------------------------
    # 3) Data type validation
    # -------------------------------------------------------------------
    # Validate data types according to the INI definitions. For numeric types, invalid values are coerced to NaN.
    numeric_cols_to_check = []
    for col_name in df1.columns:
        if col_name in fields:
            dtype_str = fields[col_name][1]
            # Treat AssignRate columns as numeric as well
            if dtype_str in ['int', 'float'] or col_name.startswith("key_assingrate_"):
                df1[col_name] = pd.to_numeric(df1[col_name], errors='coerce')
                numeric_cols_to_check.append(col_name)

    # Remove rows that failed numeric validation (contain NaN in numeric columns)
    if numeric_cols_to_check:
        original_rows = len(df1)
        df1.dropna(subset=numeric_cols_to_check, inplace=True)
        dropped_rows = original_rows - len(df1)
        if dropped_rows > 0:
            logging.info(
                f"Dropped {dropped_rows} rows from {excel_file} due to non-numeric "
                f"values in columns: {', '.join(numeric_cols_to_check)}"
            )

    if df1.empty:
        logging.info(f"No data left for {excel_file} after type validation. Skipping file.")
        return

    # Process Serial_Number columns: keep only the part before '(' if present
    serial_cols = [col for col in df1.columns if col.startswith("key_Serial_Number_")]
    for col in serial_cols:
        # .str.split is vectorized and handles NaNs; fillna restores the original value for non-string entries
        df1[col] = df1[col].str.split('(', n=1).str[0].fillna(df1[col])

    # -------------------------------------------------------------------
    # 4) Flatten Serial_Number / AssignRate (wide to long)
    # -------------------------------------------------------------------
    # Identify columns to melt and common columns
    serial_cols = sorted([k for k in df1.columns if k.startswith("key_Serial_Number_")])
    assign_cols = sorted([k for k in df1.columns if k.startswith("key_assingrate_")])

    # All other columns are considered common identifiers
    common_cols = [
        k for k in df1.columns
        if not k.startswith("key_Serial_Number_") and not k.startswith("key_assingrate_")
    ]

    melted_rows: List[Dict[str, Any]] = []
    for _, row in df1.iterrows():
        # Create a base record with all common columns, removing the 'key_' prefix
        common_data = {key.replace("key_", "", 1): row[key] for key in common_cols}

        for i, serial_key in enumerate(serial_cols):
            serial_val = row.get(serial_key)
            # Skip if the serial number is missing or empty
            if pd.isna(serial_val) or str(serial_val).strip() == "":
                continue

            record = common_data.copy()
            record["Serial_Number"] = str(serial_val).strip()

            # Add a Location column to mark the origin (a, b, c)
            location_suffix = serial_key.split('_')[-1]
            record["Location"] = location_suffix.upper()

            # If a corresponding AssignRate column exists, add it to the record
            if i < len(assign_cols):
                assign_key = assign_cols[i]
                assign_val = row.get(assign_key)
                record["AssignRate"] = assign_val
            
            melted_rows.append(record)

    if not melted_rows:
        logging.info(f"No valid serial numbers found in {excel_file} to melt. Skipping file.")
        return

    df_final = pd.DataFrame(melted_rows)

    # Convert Start_Date_Time to datetime objects for comparison
    # Providing the format boosts performance and avoids "Could not infer format" warnings.
    # Adjust the format string to match your Excel date format, e.g., "%Y-%m-%d %H:%M:%S"
    df_final["Start_Date_Time"] = pd.to_datetime(df_final["Start_Date_Time"], format="%Y/%m/%d %H:%M:%S", errors="coerce")

    # Remove rows where the date could not be parsed (NaT)
    df_final.dropna(subset=["Start_Date_Time"], inplace=True)
 
    # Filter by running_date if specified
    if running_date > 0:
        cutoff_date = datetime.now() - timedelta(days=running_date)
        df_final = df_final[df_final["Start_Date_Time"].dt.date >= cutoff_date.date()]

    # Check if any data remains after date filtering
    if df_final.empty:
        logging.info(f"No data left for {excel_file} after date filtering. Skipping file.")
        return

    # For the XML filename, use the Part_Number from the first valid row.
    # Note: If the source file contains multiple part numbers, only the first one is used for the filename.
    part_number_for_xml = df_final['Part_Number'].iloc[0]

    # Reformat the date back to string
    df_final["Start_Date_Time"] = df_final["Start_Date_Time"].dt.strftime("%Y/%m/%d %H:%M:%S")
    # Add the fixed PL_Tool column
    df_final["PL_Tool"] = pl_tool

    # -------------------------------------------------------------------
    # 5) Export CSV
    # -------------------------------------------------------------------
    ts = datetime.now().strftime("%Y%m%d%H%M") + f"{random.randint(0,60):02}"
    csv_name = f"TAK_PLX_{ts}.csv"
    csv_path = os.path.join(csv_base_path, csv_name)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df_final.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logging.info(f"CSV saved: {csv_path}")

    # -------------------------------------------------------------------
    # 6) Generate XML
    # -------------------------------------------------------------------
    generate_xml(output_path, site, product_family, operation, test_station, ts, csv_path, part_number_for_xml)


# ---------------------------------------------------------------------------
# XML generation
# ---------------------------------------------------------------------------

def generate_xml(
    output_path: str,
    site: str,
    product_family: str,
    operation: str,
    test_station: str,
    serial_no: str,
    csv_path: str,
    part_number: str,
) -> None:
    os.makedirs(output_path, exist_ok=True)
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    xml_file = os.path.join(
        output_path,
        f"Site={site},ProductFamily={product_family},Operation={operation},Partnumber=UNKNOWPN,Serialnumber={serial_no},Testdate={now_iso}.xml".replace(":", ".")
    )

    # Build the XML structure
    results = ET.Element(
        "Results",
        {
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        },
    )
    result = ET.SubElement(
        results, "Result", startDateTime=now_iso, endDateTime=now_iso, Result="Passed"
    )
    ET.SubElement(
        result,
        "Header",
        SerialNumber=serial_no,
        PartNumber="UNKNOWPN",
        Operation=operation,
        TestStation=test_station,
        Operator="NA",
        StartTime=now_iso,
        Site=site,
        LotNumber="",
        Quantity="",
    )
    header_misc = ET.SubElement(result, "HeaderMisc")
    ET.SubElement(header_misc, "Item", Description="")
    test_step = ET.SubElement(
        result, "TestStep", Name=operation, startDateTime=now_iso, endDateTime=now_iso, Status="Passed"
    )
    ET.SubElement(
        test_step, "Data", DataType="Table", Name=f"tbl_{operation.upper()}", Value=csv_path, CompOperation="LOG"
    )

    # Write the XML file with pretty formatting
    xml_str = minidom.parseString(ET.tostring(results)).toprettyxml(indent="  ", encoding="utf-8")
    with open(xml_file, "wb") as f:
        f.write(xml_str)

    logging.info(f"XML saved: {xml_file}")


# ---------------------------------------------------------------------------
# INI processing & main program
# ---------------------------------------------------------------------------

def process_ini_file(config_path: str) -> None:
    cfg = ConfigParser()
    
    # Skip comment lines that start with '#'
    with open(config_path, "r", encoding="utf-8") as fp:
        cfg.read_file(line for line in fp if not line.strip().startswith("#"))

    # Read basic settings
    try:
        input_paths = [p.strip() for p in cfg.get("Paths", "input_paths").splitlines() if p.strip()]
        output_path = cfg.get("Paths", "output_path")
        csv_base_path = cfg.get("Paths", "CSV_path")
        sheet_name = cfg.get("Excel", "sheet_name")
        data_columns = cfg.get("Excel", "data_columns")
        data_row = cfg.getint("Excel", "Data_Row")
        log_dir = cfg.get("Logging", "log_path")
        fields_raw = [l for l in cfg.get("DataFields", "fields").splitlines() if l.strip()]
        site = cfg.get("Basic_info", "Site")
        pl_tool = cfg.get("Basic_info", "PL_Tool", fallback="NA")
        product_family = cfg.get("Basic_info", "ProductFamily")
        operation = cfg.get("Basic_info", "Operation")
        test_station = cfg.get("Basic_info", "TestStation")
        running_date = cfg.getint("Basic_info", "Running_date", fallback=0)
        file_pattern = cfg.get("Basic_info", "file_name_pattern")
    except (NoSectionError, NoOptionError) as e:
        print(f"[INI ERROR] {e}")
        return

    # Prepare logging
    today_str = datetime.today().strftime("%Y-%m-%d")
    ini_name = os.path.splitext(os.path.basename(config_path))[0]
    log_file_path = os.path.join(log_dir, today_str, f"{ini_name}.log")
    setup_logging(log_file_path)

    # Parse DataFields
    fields_cfg: Dict[str, tuple[str, str]] = {}
    for line in fields_raw:
        if ":" not in line:
            continue
        key, col, dtype = (s.strip() for s in line.split(":"))
        fields_cfg[key] = (col, dtype)

    # Process all input paths
    for ipath in input_paths:
        matched_files = glob.glob(os.path.join(ipath, file_pattern))
        for f in matched_files:
            dst_dir = cfg.get("Paths", "copy_destination_path")
            os.makedirs(dst_dir, exist_ok=True)
            copied = shutil.copy(f, dst_dir)
            logging.info(f"Copied {f} -> {copied}")
            process_excel_file(
                copied,
                sheet_name,
                data_columns,
                data_row,
                output_path,
                csv_base_path,
                pl_tool,
                fields_cfg,
                site,
                product_family,
                operation,
                test_station,
                running_date,
            )


def main() -> None:
    for ini in glob.glob("*.ini"):
        process_ini_file(ini)
    logging.info("Program End")


if __name__ == "__main__":
    main()
