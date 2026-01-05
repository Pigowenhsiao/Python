import os
import sys
import shutil
import logging
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from configparser import ConfigParser
from pathlib import Path
import traceback

# Ensure MyModule is in the Python search path

sys.path.append('../MyModule')
import Log
import SQL
import Convert_Date
import Row_Number_Func

class IniSettings:
    """Class to hold all settings read from the INI file (Universal Version)"""
    def __init__(self):
        # Common settings
        self.site = ""
        self.product_family = ""
        self.operation = ""
        self.test_station = ""
        self.retention_date = 30
        self.file_name_patterns = []
        self.input_paths = []
        self.output_path = ""
        self.csv_path = "" 
        self.intermediate_data_path = ""
        self.log_path = ""
        self.running_rec = ""
        self.backup_running_rec_path = ""
        self.sheet_name = ""
        self.data_columns = ""
        self.skip_rows = 500
        self.field_map = {}
        # CVD-specific
        self.tool_name = ""
        # ICP/Dry-specific
        self.xy_sheet_name = ""
        self.xy_columns = ""
        self.tool_name_map = {}

def setup_logging(log_dir, operation_name):
    """Sets up the logging feature."""
    log_folder = os.path.join(log_dir, str(date.today()))
    os.makedirs(log_folder, exist_ok=True)
    log_file = os.path.join(log_folder, f'{operation_name}.log')
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    return log_file

def _read_and_parse_ini_config(config_file_path):
    """Reads and parses the INI configuration file."""
    config = ConfigParser()
    config.read(config_file_path, encoding='utf-8')
    return config

def _parse_fields_map_from_lines(fields_lines):
    """Parses the field mapping from the [DataFields] section."""
    fields = {}
    for line in fields_lines:
        if ':' in line and not line.strip().startswith('#'):
            try:
                key, col_str, dtype_str = map(str.strip, line.split(':', 2))
                fields[key] = {'col': col_str}
            except ValueError:
                continue
    return fields


def _extract_settings_from_config(config):
    """Extracts all settings from the parsed config object."""
    s = IniSettings()
    # Basic Info
    s.site = config.get('Basic_info', 'Site')
    s.product_family = config.get('Basic_info', 'ProductFamily')
    s.operation = config.get('Basic_info', 'Operation')
    s.test_station = config.get('Basic_info', 'TestStation')
    s.retention_date = config.getint('Basic_info', 'retention_date', fallback=30)
    s.file_name_patterns = [x.strip() for x in config.get('Basic_info', 'file_name_patterns').split(',')]
    s.tool_name = config.get('Basic_info', 'Tool_Name', fallback=None) # CVD
    
    # Paths
    s.input_paths = [x.strip() for x in config.get('Paths', 'input_paths').split(',')]
    s.output_path = config.get('Paths', 'output_path', fallback=None)
    s.csv_path = config.get('Paths', 'CSV_path', fallback=None)
    s.intermediate_data_path = config.get('Paths', 'intermediate_data_path')
    s.log_path = config.get('Paths', 'log_path')
    s.running_rec = config.get('Paths', 'running_rec')
    s.backup_running_rec_path = config.get('Paths', 'backup_running_rec_path', fallback=None)

    # Excel
    s.sheet_name = config.get('Excel', 'sheet_name')
    s.data_columns = config.get('Excel', 'data_columns')
    s.skip_rows = config.getint('Excel', 'main_skip_rows')
    s.xy_sheet_name = config.get('Excel', 'xy_sheet_name', fallback=None) # ICP/Dry
    s.xy_columns = config.get('Excel', 'xy_columns', fallback=None) # ICP/Dry

    # DataFields and ToolNameMapping
    fields_lines = config.get('DataFields', 'fields').splitlines()
    s.field_map = _parse_fields_map_from_lines(fields_lines)
    if config.has_section('ToolNameMapping'): # ICP/Dry
        s.tool_name_map = dict(config.items('ToolNameMapping'))
        
    return s

def detect_tool_name(filename, tool_map):
    """Detects tool name based on filename (for ICP/Dry)."""
    filename_str = str(filename)
    for keyword, tool in tool_map.items():
        if keyword != 'default' and keyword in filename_str:
            return tool
    return tool_map.get('default', 'UNKNOWN')

def write_to_csv(csv_filepath, dataframe, log_file):
    """Appends a DataFrame to the specified CSV file."""
    Log.Log_Info(log_file, "Executing function write_to_csv...")
    try:
        file_exists = os.path.isfile(csv_filepath)
        dataframe.to_csv(csv_filepath, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')
        Log.Log_Info(log_file, "Function write_to_csv executed successfully.")
        return True
    except Exception as e:
        Log.Log_Error(log_file, f"Function write_to_csv failed: {e}")
        return False

def generate_pointer_xml(output_path, csv_path, settings, log_file):
    """Generates the pointer XML file that points to the CSV."""
    Log.Log_Info(log_file, "Executing function generate_pointer_xml...")
    try:
        os.makedirs(output_path, exist_ok=True)
        now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        serial_no = Path(csv_path).stem
        
        xml_file_name = (
            f"Site={settings.site},"
            f"ProductFamily={settings.product_family},"
            f"Operation={settings.operation},"
            f"Partnumber=UNKNOWPN,"
            f"Serialnumber={serial_no},"
            f"Testdate={now_iso}.xml"
        ).replace(":", ".")
        
        xml_file_path = os.path.join(output_path, xml_file_name)

        results = ET.Element("Results", {"xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance", "xmlns:xsd": "http://www.w3.org/2001/XMLSchema"})
        result = ET.SubElement(results, "Result", startDateTime=now_iso, endDateTime=now_iso, Result="Passed")
        ET.SubElement(result, "Header",
            SerialNumber=serial_no, PartNumber="UNKNOWPN",
            Operation=settings.operation, TestStation=settings.test_station,
            Operator="NA", StartTime=now_iso, Site=settings.site, LotNumber=""
        )
        test_step = ET.SubElement(result, "TestStep", Name=settings.operation, startDateTime=now_iso, endDateTime=now_iso, Status="Passed")
        ET.SubElement(test_step, "Data", DataType="Table", Name=f"tbl_{settings.operation.upper()}", Value=str(csv_path), CompOperation="LOG")
        
        xml_str = minidom.parseString(ET.tostring(results)).toprettyxml(indent="  ", encoding="utf-8")
        with open(xml_file_path, "wb") as f:
            f.write(xml_str)

        Log.Log_Info(log_file, f"Pointer XML generated successfully at: {xml_file_path}")
    except Exception as e:
        Log.Log_Error(log_file, f"Function generate_pointer_xml failed: {e}")

def process_excel_file(filepath_str, settings, log_file, csv_filepath):
    """Processes a single Excel file in a batched, vectorized manner (Universal Version)."""
    filepath = Path(filepath_str)
    Log.Log_Info(log_file, f"--- Start processing file: {filepath.name} ---")
    start_row = max(Row_Number_Func.start_row_number(settings.running_rec) - settings.skip_rows, 4)
    start_row = int(20)
    
    try:
        # Step 1: Read the main Excel worksheet
        df = pd.read_excel(filepath, header=None, sheet_name=settings.sheet_name, usecols=settings.data_columns, skiprows=start_row)
        Log.Log_Info(log_file, f"Step 1: Successfully read main sheet '{settings.sheet_name}', {df.shape[0]} rows loaded.")
        ini_keys_by_col_index = {int(v['col']): k for k, v in settings.field_map.items() if not v['col'].startswith('xy_')}
        df.columns = [ini_keys_by_col_index.get(i, f'unused_{i}') for i in range(df.shape[1])]
        
        # Step 2: Conditionally read the XY coordinate worksheet (ICP/Dry mode)
        xy_data = {}
        if settings.xy_sheet_name:
            Log.Log_Info(log_file, f"ICP/Dry mode detected. Reading XY coordinate sheet: '{settings.xy_sheet_name}'")
            df_xy = pd.read_excel(filepath, header=None, sheet_name=settings.xy_sheet_name, usecols=settings.xy_columns)
            for key, mapping in settings.field_map.items():
                col_str = mapping['col']
                if col_str.startswith('xy_'):
                    parts = col_str.split('_')
                    row_idx, col_idx = int(parts[1]) - 1, int(parts[2]) - 1
                    xy_data[key] = df_xy.iloc[row_idx, col_idx]
            Log.Log_Info(log_file, f"XY coordinate data parsed.")
        else:
            Log.Log_Info(log_file, "No XY coordinate sheet setting detected. Processing in CVD mode.")
        # Step 3: Initial filtering
        date_series = pd.to_datetime(df['key_Start_Date_Time'], errors='coerce')
        df = df[date_series.notna() & (date_series >= (datetime.now() - relativedelta(days=settings.retention_date)))]
        df.dropna(subset=['key_Serial_Number'], inplace=True)
        Log.Log_Info(log_file, f"Step 2: Initial filtering (date, serial number) complete. {df.shape[0]} rows remaining.")
    except Exception as e:
        Log.Log_Error(log_file, f"Step 1/2/3 failed: Error during Excel read or filter. Error: {e}")
        return

    if df.empty:
        Log.Log_Info(log_file, "No data left after initial filtering. Ending process for this file.")
        return

    # Step 4: Database query
    conn, cursor = None, None
    try:
        Log.Log_Info(log_file, "Step 3: Starting database query...")
        conn, cursor = SQL.connSQL()
        if conn is None: 
            Log.Log_Error(log_file, "Database connection failed.")
            return
        def get_db_info(serial): return pd.Series(SQL.selectSQL(cursor, str(serial)))
        df[['key_Part_Number', 'key_LotNumber_9']] = df['key_Serial_Number'].apply(get_db_info)
        df.dropna(subset=['key_Part_Number'], inplace=True)
        df = df[df['key_Part_Number'] != 'LDアレイ_']
        Log.Log_Info(log_file, f"Database query and filtering complete. {df.shape[0]} valid rows remaining.")
    finally:
        if conn: 
            SQL.disconnSQL(conn, cursor)
            Log.Log_Info(log_file, "Database connection closed.")
    
    if df.empty:
        Log.Log_Info(log_file, "No data left after database lookup. Ending process for this file.")
        return

    # Step 5: Data transformation and calculation
    Log.Log_Info(log_file, "Step 4: Starting data transformation and calculation...")
    def clean_date(raw_date):
        try: return pd.to_datetime(Convert_Date.Edit_Date(raw_date).replace('T', ' ').replace('.', ':'))
        except (ValueError, TypeError): return pd.NaT
            
    df['datetime_obj'] = df['key_Start_Date_Time'].apply(clean_date)
    df.dropna(subset=['datetime_obj'], inplace=True)
    
    base_date = datetime(1899, 12, 30)
    df['date_excel_number'] = (df['datetime_obj'] - base_date).dt.days
    df['excel_row'] = start_row + df.index + 1
    df['key_STARTTIME_SORTED'] = df['date_excel_number'] + (df['excel_row'] / 10**6)
    df['key_SORTNUMBER'] = df['excel_row']
    Log.Log_Info(log_file, "Date and SORTED field calculations complete.")
    
    # Step 6: Append additional info
    df['Operation'] = settings.operation
    df['TestStation'] = settings.test_station
    df['Site'] = settings.site
    df['key_Start_Date_Time'] = df['datetime_obj'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    if settings.tool_name_map:
        tool_name = detect_tool_name(filepath.name, settings.tool_name_map)
        df['key_Tool_name'] = tool_name
        Log.Log_Info(log_file, f"Dynamically detected tool name: '{tool_name}'")
    else:
        df['key_Tool_name'] = settings.tool_name
        Log.Log_Info(log_file, f"Using fixed tool name from INI: '{settings.tool_name}'")
    
    for key, value in xy_data.items():
        df[key] = value
    Log.Log_Info(log_file, "Step 5: Appending additional info (Operation, ToolName, XY coords, etc.) complete.")

    # Step 7: Dynamically generate columns and write to CSV
    rename_map = {}
    special_renames = {'key_Serial_Number': 'Serial_Number', 'key_Part_Number': 'Part_Number', 'key_Start_Date_Time': 'Start_Date_Time', 'key_TestEquipment_Nano': 'Nanospec_DeviceSerialNumber', 'key_Tool_name': 'DryEtch_DeviceSerialNumber'}
    for key in settings.field_map.keys():
        rename_map[key] = special_renames.get(key, key.replace('key_', '', 1))
    rename_map.update({'Operation': 'Operation', 'TestStation': 'TestStation', 'Site': 'Site'})
    
    dynamic_column_order = ['Serial_Number', 'Part_Number', 'Start_Date_Time', 'Operation', 'TestStation', 'Site']
    for key in settings.field_map.keys():
        final_header = rename_map.get(key)
        if final_header and final_header not in dynamic_column_order:
            dynamic_column_order.append(final_header)
    dynamic_column_order.extend(['STARTTIME_SORTED', 'SORTNUMBER'])
    Log.Log_Info(log_file, "Step 6: Dynamic CSV columns and order generated.")
    
    df['key_Serial_Number'] = df['key_Serial_Number'].astype(str) + '_' + df['key_Banchi'].astype(str)
    
    df_renamed = df.rename(columns=rename_map)
    final_columns = [col for col in dynamic_column_order if col in df_renamed.columns]
    df_to_csv = df_renamed[final_columns]

    if csv_filepath:
        Log.Log_Info(log_file, f"Step 7: Preparing to write {len(df_to_csv)} rows to CSV...")
        write_to_csv(csv_filepath, df_to_csv, log_file)
        

    # Step 8: Update the starting row record
    original_row_count = pd.read_excel(filepath_str, header=None, sheet_name=settings.sheet_name).shape[0]
    next_start_row = start_row + original_row_count + 1
    Row_Number_Func.next_start_row_number(settings.running_rec, next_start_row)
    Log.Log_Info(log_file, f"Step 8: Updating next start row to {next_start_row}")
    if settings.backup_running_rec_path:
        try: shutil.copy(settings.running_rec, settings.backup_running_rec_path)
        except Exception as e: Log.Log_Error(log_file, f"Failed to backup running_rec file: {e}")
    Log.Log_Info(log_file, f"--- Function process_excel_file executed successfully ---")

def main():
    """Main function to find and process all INI files."""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    log_file = setup_logging('../Log/', 'UniversalScript_Init')
    Log.Log_Info(log_file, "===== Universal Script Start =====")

    ini_files = [f for f in os.listdir('.') if f.endswith('.ini')]
    if not ini_files:
        Log.Log_Info(log_file, "No .ini or .txt config files found in the current directory. Exiting.")
        print("No config files (.ini or .txt) found in the current directory.")
        return
    Log.Log_Info(log_file, f"Found {len(ini_files)} config file(s): {', '.join(ini_files)}")

    for ini_path in ini_files:
        try:
            print(f"--- Processing config: {ini_path} ---")
            config = _read_and_parse_ini_config(ini_path)
            settings = _extract_settings_from_config(config)
            
            # Set up a specific log file for this operation
            log_file = setup_logging(settings.log_path, settings.operation)
            Log.Log_Info(log_file, f"--- Start processing config file: {ini_path} ---")
            
            # Create a unique CSV file for this INI's execution
            csv_filepath_for_this_ini = None
            if settings.csv_path:
                Path(settings.csv_path).mkdir(parents=True, exist_ok=True)
                timestamp = datetime.now().strftime('%Y_%m_%dT%H.%M.%S')
                filename = f"{settings.operation}_{timestamp}.csv"
                csv_filepath_for_this_ini = Path(settings.csv_path) / filename
                Log.Log_Info(log_file, f"CSV output for this config will be: {csv_filepath_for_this_ini}")

            intermediate_path = Path(settings.intermediate_data_path)
            intermediate_path.mkdir(parents=True, exist_ok=True)
            source_files_found = False
            for input_p_str in settings.input_paths:
                input_p = Path(input_p_str)
                for pattern in settings.file_name_patterns:
                    Log.Log_Info(log_file, f"Searching in path '{input_p}' with pattern '{pattern}'")
                    files = [p for p in input_p.glob(pattern) if not p.name.startswith('~$')]
                    if not files: continue
                    source_files_found = True
                    latest_file = max(files, key=os.path.getmtime)
                    Log.Log_Info(log_file, f"Found latest source file: {latest_file.name}")
                    try:
                        dst_path = shutil.copy(latest_file, intermediate_path)
                        Log.Log_Info(log_file, f"File copied successfully -> {dst_path}")
                        process_excel_file(dst_path, settings, log_file, csv_filepath_for_this_ini)
                    except Exception:
                        Log.Log_Error(log_file, f"Error processing file {latest_file.name}: {traceback.format_exc()}")

            if not source_files_found:
                Log.Log_Info(log_file, "No matching source files found for this configuration.")

            # Generate the pointer XML for this specific INI's CSV
            if csv_filepath_for_this_ini and os.path.exists(csv_filepath_for_this_ini) and settings.output_path:
                Log.Log_Info(log_file, f"--- Generating pointer XML for {ini_path} ---")
                
                generate_pointer_xml(
                    output_path=settings.output_path,
                    csv_path=csv_filepath_for_this_ini,
                    settings=settings,
                    log_file=log_file
                )
            
            Log.Log_Info(log_file, f"--- Finished processing config file: {ini_path} ---")

        except Exception:
            error_message = f"FATAL Error with INI {ini_path}: {traceback.format_exc()}"
            print(error_message)
            if log_file: Log.Log_Error(log_file, error_message)

    Log.Log_Info(log_file, "===== Universal Script End =====")
    print("✅ All .ini configurations have been processed.")
    print("This window will close in 5 seconds...")


if __name__ == '__main__':
    main()