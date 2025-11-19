# -*- coding: utf-8 -*-
import os
import re
import sys
import shutil
import logging
import traceback
from datetime import datetime, date
from configparser import ConfigParser
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from xml.dom import minidom
from dateutil.relativedelta import relativedelta

 # Allow custom modules to be imported
sys.path.append("../MyModule")
import Log  # type: ignore
import SQL  # type: ignore
import Convert_Date  # type: ignore
 # Row_Number_Func can be kept; remove if unused as needed
 # import Row_Number_Func  # type: ignore


class IniSettings:
    """Settings loaded from INI file"""

    def __init__(self) -> None:
        # Basic Info
        self.site: str = ""
        self.product_family: str = ""
        self.operation: str = ""
        self.test_station: str = ""
        self.retention_date: int = 30
        self.file_name_patterns: List[str] = []
        self.tool_name: str = ""

        # Paths
        self.input_paths: List[str] = []
        self.output_path: str = ""
        self.csv_path: str = ""
        self.intermediate_data_path: str = ""
        self.log_path: str = ""
        self.running_rec: str = ""

        # Excel
        self.sheet_name: List[Any] = []  # Support integer index or sheet name
        self.data_columns: str = ""
        self.main_skip_rows: int = 0

        # Database (connection details remain in INI, actual connection handled by SQL module)
        self.db_server: str = ""
        self.db_database: str = ""
        self.db_username: str = ""
        self.db_password: str = ""
        self.db_driver: str = ""

        # DataFields mapping (key_* -> {"col": "index/-1", "dtype": "str/float/..."})
        self.field_map: Dict[str, Dict[str, str]] = {}


 # ---------- Logging ----------
def setup_logging(log_dir: str, operation_name: str) -> str:
    """Create daily log file"""
    log_folder = os.path.join(log_dir, str(date.today()))
    os.makedirs(log_folder, exist_ok=True)
    log_file = os.path.join(log_folder, f"{operation_name}.log")

    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    return log_file


 # ---------- INI Reading and Parsing ----------
def _read_and_parse_ini_config(config_file_path: str) -> ConfigParser:
    """Read INI file"""
    config = ConfigParser()
    config.read(config_file_path, encoding="utf-8")
    return config


def _parse_fields_map_from_lines(fields_lines: List[str]) -> Dict[str, Dict[str, str]]:
    """Parse [DataFields] field mapping"""
    fields: Dict[str, Dict[str, str]] = {}
    for line in fields_lines:
        if ":" in line and not line.strip().startswith("#"):
            try:
                key, col_str, dtype_str = map(str.strip, line.split(":", 3))
                fields[key] = {"col": col_str, "dtype": dtype_str}
            except ValueError:
                continue
    return fields


def _extract_settings_from_config(config: ConfigParser) -> IniSettings:
    """Extract INI settings into object"""

    s = IniSettings()
    # Basic Info
    s.site = config.get("Basic_info", "Site")
    s.product_family = config.get("Basic_info", "ProductFamily")
    s.operation = config.get("Basic_info", "Operation")
    s.test_station = config.get("Basic_info", "TestStation")
    s.retention_date = config.getint("Basic_info", "Retention_date", fallback=30)
    s.file_name_patterns = [
        x.strip() for x in config.get("Basic_info", "file_name_patterns").split(",")
    ]
    s.tool_name = config.get("Basic_info", "Tool_Name")

    # Paths
    s.input_paths = [x.strip() for x in config.get("Paths", "input_paths").split(",")]
    s.output_path = config.get("Paths", "output_path")
    s.csv_path = config.get("Paths", "CSV_path")
    s.intermediate_data_path = config.get("Paths", "intermediate_data_path")
    s.log_path = config.get("Paths", "log_path")
    s.running_rec = config.get("Paths", "running_rec", fallback="")

    # Excel (support sheet index or name)
    sheet_raw = [x.strip() for x in config.get("Excel", "sheet_name").split(",")]
    sheet_list: List[Any] = []
    for x in sheet_raw:
        if x.isdigit():
            sheet_list.append(int(x))
        else:
            sheet_list.append(x)
    s.sheet_name = sheet_list
    s.data_columns = config.get("Excel", "data_columns")
    s.main_skip_rows = config.getint("Excel", "main_skip_rows", fallback=0)

    # Database info (can be used by SQL module if needed)
    s.db_server = config.get("Database", "server", fallback="")
    s.db_database = config.get("Database", "database", fallback="")
    s.db_username = config.get("Database", "username", fallback="")
    s.db_password = config.get("Database", "password", fallback="")
    s.db_driver = config.get("Database", "driver", fallback="")

    # DataFields
    fields_lines = config.get("DataFields", "fields").splitlines()
    s.field_map = _parse_fields_map_from_lines(fields_lines)
    return s


 # ---------- Output ----------
def write_to_csv(csv_filepath: str, dataframe: pd.DataFrame, log_file: str) -> bool:
    """Append DataFrame to CSV (UTF-8-SIG)"""
    try:
        file_exists = os.path.isfile(csv_filepath)
        dataframe.to_csv(
            csv_filepath,
            mode="a",
            header=not file_exists,
            index=False,
            encoding="utf-8-sig",
        )
        Log.Log_Info(log_file, f"CSV written: {csv_filepath}")
        return True
    except Exception as e:
        Log.Log_Error(log_file, f"CSV write failed: {e}")
        return False


def generate_pointer_xml(output_path: str, csv_path: str, settings: IniSettings, log_file: str) -> None:
    """Generate pointer XML that points to CSV (same style as reference program)"""
    try:
        os.makedirs(output_path, exist_ok=True)
        now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        serial_no = Path(csv_path).stem

        xml_file_name = (
            f"Site={settings.site},ProductFamily={settings.product_family},"
            f"Operation={settings.operation},Partnumber=UNKNOWPN,"
            f"Serialnumber={serial_no},Testdate={now_iso}.xml"
        ).replace(":", ".")

        xml_file_path = os.path.join(output_path, xml_file_name)

        results = ET.Element(
            "Results",
            {
                "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
            },
        )
        result = ET.SubElement(
            results,
            "Result",
            startDateTime=now_iso,
            endDateTime=now_iso,
            Result="Passed",
        )

        ET.SubElement(
            result,
            "Header",
            SerialNumber=serial_no,
            PartNumber="UNKNOWPN",
            Operation=settings.operation,
            TestStation=settings.test_station,
            Operator="NA",
            StartTime=now_iso,
            Site=settings.site,
            LotNumber="",
        )

        test_step = ET.SubElement(
            result,
            "TestStep",
            Name=settings.operation,
            startDateTime=now_iso,
            endDateTime=now_iso,
            Status="Passed",
        )

        ET.SubElement(
            test_step,
            "Data",
            DataType="Table",
            Name=f"tbl_{settings.operation.upper()}",
            Value=str(csv_path),
            CompOperation="LOG",
        )

        xml_str = minidom.parseString(ET.tostring(results)).toprettyxml(
            indent="  ", encoding="utf-8"
        )
        with open(xml_file_path, "wb") as f:
            f.write(xml_str)
        Log.Log_Info(log_file, f"Pointer XML generated: {xml_file_path}")
    except Exception as e:
        Log.Log_Error(log_file, f"XML generation failed: {e}")


 # ---------- Utilities ----------
def _get_col_index(settings: IniSettings, field_key: str, fallback: Optional[int] = None) -> Optional[int]:
    """Get column index from INI mapping (valid only if not -1)"""
    m = settings.field_map.get(field_key, {})
    col_str = m.get("col", "") if m else ""
    if col_str and col_str not in {"-1", ""}:
        try:
            return int(col_str)
        except ValueError:
            return fallback
    return fallback


def _parse_datetime_series(raw: pd.Series) -> pd.Series:
    """
    Convert mixed-type time column to pandas datetime:
        1) Try regular to_datetime first
        2) For pure numbers (Excel serial), convert with origin=1899-12-30
    """
    dt = pd.to_datetime(raw, errors="coerce")
    numeric_mask = dt.isna() & pd.to_numeric(raw, errors="coerce").notna()
    if numeric_mask.any():
        numeric_vals = pd.to_numeric(raw[numeric_mask], errors="coerce")
        dt.loc[numeric_mask] = pd.to_datetime(
            numeric_vals, unit="d", origin="1899-12-30", errors="coerce"
        )
    return dt


 # ---------- Main Processing ----------
def process_excel_file(filepath_str: str, settings: IniSettings, log_file: str, csv_filepath: str) -> None:
    """
    Read Excel → extract all N-prefixed serials from each cell → query SQL for unique serials → output CSV by INI column order
    """
    filepath = Path(filepath_str)
    Log.Log_Info(log_file, f"Start processing {filepath.name}")

    # Parse required column indexes (by INI)
    idx_date = _get_col_index(settings, "key_Start_Date_Time", fallback=0)
    idx_operator = _get_col_index(settings, "key_Operator", fallback=None)
    idx_serial_cell = _get_col_index(settings, "key_Serial_Number", fallback=7)
    idx_ref_front = _get_col_index(settings, "key_Reflectance_Front", fallback=None)
    idx_ref_back = _get_col_index(settings, "key_Reflectance_Back", fallback=None)

    if idx_date is None or idx_serial_cell is None:
        Log.Log_Error(log_file, "Missing required column index (Start_Date_Time / Serial_Number) in INI.")
        return

    # Read multiple sheets
    all_data: List[pd.DataFrame] = []
    all_row_indices: List[int] = []
    for sheet in settings.sheet_name:
        try:
            df = pd.read_excel(
                filepath,
                header=None,
                sheet_name=sheet,
                usecols=settings.data_columns,
                skiprows=settings.main_skip_rows,
            )
            df = df.dropna(how="all")
            # Save original row indices (relative to the sheet, after skiprows)
            for idx in df.index:
                all_row_indices.append(idx + settings.main_skip_rows + 1)  # +1 for 1-based Excel row
            all_data.append(df)
            Log.Log_Info(log_file, f"Read sheet '{sheet}', rows={df.shape[0]}")
        except Exception as e:
            Log.Log_Error(log_file, f"Failed reading sheet '{sheet}': {e}")

    if not all_data:
        Log.Log_Info(log_file, "No valid data read from any sheet.")
        return

    df_all = pd.concat(all_data, ignore_index=True)
    df_all.columns = range(df_all.shape[1])

    # Keep only rows with non-empty time and serial columns
    valid_mask = (~df_all.replace("nan", np.nan)[[idx_date, idx_serial_cell]].isna().any(axis=1))
    df_all = df_all.replace("nan", np.nan)[valid_mask].copy()
    # Use DataFrame index to generate original Excel row number (after skiprows)
    df_all["_ROW_IDX_ORIG"] = df_all.index + settings.main_skip_rows + 1  # 1-based Excel row

    # Convert time and filter by retention days
    dt = _parse_datetime_series(df_all[idx_date])
    df_all = df_all[dt.notna()].copy()
    df_all["_datetime"] = dt[dt.notna()]
    df_all = df_all[df_all["_datetime"] >= (datetime.now() - relativedelta(days=settings.retention_date))]
    if df_all.empty:
        Log.Log_Info(log_file, "No data after date/retention filtering.")
        return

    # Extract all "N####" tokens or "N####-..." tokens from each cell and expand to multiple rows
    def extract_tokens(cell: Any) -> List[str]:
        if isinstance(cell, str):
            return re.findall(r"(N\d+(?:-[^\s]+)?)", cell)
        return []

    df_all["Serial_Tokens"] = df_all[idx_serial_cell].apply(extract_tokens)
    # Explode tokens and keep row index
    df_all = df_all.explode(["Serial_Tokens"]).reset_index(drop=True)
    df_all = df_all[df_all["Serial_Tokens"].notna() & (df_all["Serial_Tokens"] != "")]
    if df_all.empty:
        Log.Log_Info(log_file, "No N-series serial tokens found after explode.")
        return

    # Create standard columns (get source by INI index, fill empty if not set)
    df_all["Serial_Number_Addr"] = df_all["Serial_Tokens"].astype(str)
    # Serial_Number keeps only the first five characters
    df_all["Serial_Number"] = df_all["Serial_Number_Addr"].str[:5]

    if idx_operator is not None and idx_operator in df_all.columns:
        df_all["Operator"] = df_all[idx_operator]
    else:
        df_all["Operator"] = ""

    # Add Operator2 column
    idx_operator2 = _get_col_index(settings, "key_Operator2", fallback=None)
    if idx_operator2 is not None and idx_operator2 in df_all.columns:
        df_all["Operator2"] = df_all[idx_operator2]
    else:
        df_all["Operator2"] = ""

    if idx_ref_front is not None and idx_ref_front in df_all.columns:
        df_all["Reflectance_Front"] = df_all[idx_ref_front]
    else:
        df_all["Reflectance_Front"] = np.nan

    if idx_ref_back is not None and idx_ref_back in df_all.columns:
        df_all["Reflectance_Back"] = df_all[idx_ref_back]
    else:
        df_all["Reflectance_Back"] = np.nan

    # Start_Date_Time string formatting (you may use Convert_Date if desired)
    df_all["Start_Date_Time"] = df_all["_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # ========= Key: send SerialNumber to SQL to get Part_Number / LotNumber_9 (same as sample program) =========
    # Refer to sample: use serial to call SQL.selectSQL(), return part name and 9-digit lot number, and fill back to DataFrame.
    unique_serials = sorted(df_all["Serial_Number"].dropna().unique().tolist())
    part_map: Dict[str, Any] = {}
    lot9_map: Dict[str, Any] = {}

    if unique_serials:
        conn, cursor = None, None
        try:
            conn, cursor = SQL.connSQL()
            if conn is None:
                Log.Log_Error(log_file, "DB connection failed; skip Part_Number/LotNumber_9 lookup.")
            else:
                for s in unique_serials:
                    try:
                        pn, lot9 = SQL.selectSQL(cursor, str(s))
                        part_map[s] = pn
                        lot9_map[s] = lot9
                    except Exception as e:
                        Log.Log_Error(log_file, f"DB select failed for {s}: {e}")
        finally:
            if conn:
                SQL.disconnSQL(conn, cursor)

    df_all["Part_Number"] = df_all["Serial_Number"].map(part_map).astype(object)
    df_all["LotNumber_9"] = df_all["Serial_Number"].map(lot9_map).astype(object)
    # ===========================================================================================

    # Other columns (device name, sorting)
    base = datetime(1899, 12, 30)
    df_all["Dev"] = settings.tool_name
    # SORTNUMBER: original Excel row number
    df_all["SORTNUMBER"] = df_all["_ROW_IDX_ORIG"].astype(int)
    df_all["STARTTIME_SORTED"] = (
        (df_all["_datetime"] - base).dt.days.astype(float) + (df_all["SORTNUMBER"].astype(float) / 10**6)
    )

    # Determine output columns and order by INI [DataFields] (not hardcoded)
    ordered_keys = list(settings.field_map.keys())  # e.g., key_Start_Date_Time, key_Operator, ...
    csv_columns = [k.replace("key_", "") for k in ordered_keys]

    for col in csv_columns:
        if col not in df_all.columns:
            df_all[col] = ""

    df_final = df_all[csv_columns].copy()

    # Final type conversion according to INI instructions (avoid type errors)
    for k, meta in settings.field_map.items():
        out_col = k.replace("key_", "")
        dtype = meta.get("dtype", "").lower()
        if out_col in df_final.columns:
            if dtype in {"float", "double"}:
                df_final[out_col] = pd.to_numeric(df_final[out_col], errors="coerce")
            elif dtype in {"int", "int32", "int64"}:
                df_final[out_col] = pd.to_numeric(df_final[out_col], errors="coerce").astype("Int64")
            elif dtype in {"str", "string"}:
                df_final[out_col] = df_final[out_col].astype(str)

    Log.Log_Info(log_file, f"Writing CSV with {len(df_final)} rows...")
    write_to_csv(csv_filepath, df_final, log_file)


def main() -> None:
    """Scan all INI files in directory, process one by one, and output CSV / Pointer XML"""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    log_file = setup_logging("../Log/", "Coating_MG_Reflectance")
    Log.Log_Info(log_file, "===== Script Start =====")

    ini_files = [f for f in os.listdir(".") if f.endswith(".ini")]
    if not ini_files:
        Log.Log_Info(log_file, "No ini found, exit.")
        print("No ini found.")
        return

    for ini_path in ini_files:
        try:
            config = _read_and_parse_ini_config(ini_path)
            settings = _extract_settings_from_config(config)
            log_file = setup_logging(settings.log_path, settings.operation)

            Log.Log_Info(log_file, f"Processing INI: {ini_path}")
            Path(settings.csv_path).mkdir(parents=True, exist_ok=True)
            Path(settings.intermediate_data_path).mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y_%m_%dT%H.%M.%S")
            csv_file = Path(settings.csv_path) / f"{settings.operation}_{timestamp}.csv"

            # Copy source file to intermediate folder (keep filename)
            found_any = False
            for input_dir in settings.input_paths:
                for pattern in settings.file_name_patterns:
                    files = list(Path(input_dir).glob(pattern))
                    if not files:
                        continue
                    latest = max(files, key=os.path.getmtime)
                    dst_path = Path(settings.intermediate_data_path) / Path(latest).name
                    shutil.copy(latest, dst_path)
                    Log.Log_Info(log_file, f"Copied source to: {dst_path}")
                    process_excel_file(str(dst_path), settings, log_file, str(csv_file))
                    found_any = True

            if not found_any:
                Log.Log_Info(log_file, "No matching source files found for this INI.")

            # Generate corresponding Pointer XML for this INI (same style as reference program)
            if csv_file.exists():
                generate_pointer_xml(settings.output_path, str(csv_file), settings, log_file)
            else:
                Log.Log_Info(log_file, "CSV not found; skip pointer XML generation.")

            Log.Log_Info(log_file, f"Finished INI: {ini_path}")

        except Exception:
            error_message = f"Error in {ini_path}: {traceback.format_exc()}"
            Log.Log_Error(log_file, error_message)

    Log.Log_Info(log_file, "===== Script End =====")
    print("✅ All INI processed.")


if __name__ == "__main__":
    main()
