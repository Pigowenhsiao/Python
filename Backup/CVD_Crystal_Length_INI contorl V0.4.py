# Python程序碼 - 讀取所有.ini文件並執行數據處理和生成XML文件
import os
import sys
import glob
import shutil
import logging
import pandas as pd
from configparser import ConfigParser, NoSectionError, NoOptionError
from datetime import datetime, timedelta

# Custom modules
sys.path.append('../MyModule')
import Log
import SQL
import Check
import Convert_Date
import Row_Number_Func

# Global variable for log file
global_log_file = None

# Set up logging configuration
def setup_logging(log_file_path):
    try:
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
    except OSError as e:
        print(f"Error setting up logging with file {log_file_path}: {e}")
        raise

# Update running record file
def update_running_rec(running_rec_path, end_date):
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(global_log_file, f"Updated running_rec file {running_rec_path} with end date {end_date}")
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error updating running_rec file {running_rec_path}: {e}")

# Ensure running record file exists and update if necessary
def ensure_running_rec_exists_and_update(running_rec_path, end_date):
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(global_log_file, f"Checked and updated running_rec file {running_rec_path} with end date {end_date}")
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error handling running_rec file {running_rec_path}: {e}")

# Read the last running record
def read_running_rec(running_rec_path):
    if not os.path.exists(running_rec_path):
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write('')
        return datetime.today() - timedelta(days=30)
    
    try:
        with open(running_rec_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                last_run_date = pd.to_datetime(content, errors='coerce')
                if pd.isnull(last_run_date):
                    return datetime.today() - timedelta(days=30)
                return last_run_date
            else:
                return datetime.today() - timedelta(days=30)
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading running_rec file {running_rec_path}: {e}")
        return datetime.today() - timedelta(days=30)

# Function to process a given .ini file
def process_ini_file(config_path):
    global global_log_file
    config = ConfigParser()
    try:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config.read_file(line for line in config_file if not line.strip().startswith('#'))
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading config file {config_path}: {e}")
        return

    # Fetch settings from config
    try:
        input_paths = [path.strip() for path in config.get('Paths', 'input_paths').split(',')]
        output_path = config.get('Paths', 'output_path')
        running_rec = config.get('Paths', 'running_rec')
        sheet_name = config.get('Excel', 'sheet_name')
        data_columns = config.get('Excel', 'data_columns')
        xy_sheet_name = config.get('Excel', 'xy_sheet_name')
        xy_columns = config.get('Excel', 'xy_columns')
        log_path = config.get('Logging', 'log_path')
        fields_config = config.get('DataFields', 'fields').splitlines()
        site = config.get('Basic_info', 'Site')
        product_family = config.get('Basic_info', 'ProductFamily')
        operation = config.get('Basic_info', 'Operation')
        test_station = config.get('Basic_info', 'TestStation')
        file_name_pattern = config.get('Basic_info', 'file_name_pattern')
    except NoSectionError as e:
        Log.Log_Error(global_log_file, f"Missing section in config file {config_path}: {e}")
        return
    except NoOptionError as e:
        Log.Log_Error(global_log_file, f"Missing option in config file {config_path}: {e}")
        return

    # Create log folder and file
    log_folder_name = str(datetime.today().date())
    log_folder_path = os.path.join(log_path, log_folder_name)
    if not os.path.exists(log_folder_path):
        os.makedirs(log_folder_path)
    log_file = os.path.join(log_folder_path, '001_Crystal.log')
    global_log_file = log_file

    # Set up logging
    setup_logging(global_log_file)
    Log.Log_Info(log_file, f'Program Start for config {config_path}')

    # Parse field settings into a dictionary
    fields = {}
    for field in fields_config:
        if field.strip():
            key, col, dtype = field.split(':')
            fields[key.strip()] = (col.strip(), dtype.strip())

    def process_excel_file(file_path):
        Log.Log_Info(global_log_file, f'Processing Excel File: {file_path}')
        
        try:
            # Read Excel data
            df = pd.read_excel(file_path, header=None, sheet_name=sheet_name, usecols=data_columns, skiprows=100)
            df_xy = pd.read_excel(file_path, header=None, sheet_name=xy_sheet_name, usecols=xy_columns)
        except Exception as e:
            Log.Log_Error(global_log_file, f'Error reading Excel file {file_path}: {e}')
            return
        
        # Set column numbers
        df.columns = range(df.shape[1])
        df_xy.columns = range(df_xy.shape[1])
        df = df.dropna(subset=[0])  # Drop rows where df[0] is NaN

        # Create output XML directory if not exists
        if not os.path.exists(output_path):
            os.makedirs(output_path)
            
        # Set processing date range (within one month)
        current_date = datetime.today()
        #one_month_ago = current_date - timedelta(days=3)
        one_month_ago = read_running_rec(running_rec)

        # Filter out rows with key_Start_Date_Time older than one month or last running record date
        if 'key_Start_Date_Time' in fields:
            start_date_col = int(fields['key_Start_Date_Time'][0])
            df = df[df[start_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]
        else:
            Log.Log_Error(global_log_file, 'key_Start_Date_Time not found in fields configuration')
            
        row_end = len(df)
        row_number = 0
        # Data processing
        while row_number < row_end:
            data_dict = {}

            # Process data conversion
            # Update running record with the latest key_Start_Date_Time
            if 'key_Start_Date_Time' in fields:
                latest_date = df[start_date_col].max()
                update_running_rec(running_rec, latest_date)
                
            for key, (col, dtype) in fields.items():
                try:
                    if col.startswith('xy'):
                        # Process df_xy data
                        _, row, column = col.split('_')
                        if int(column) > 1:  # Ignore the first column
                            value = df_xy.iloc[int(row)-1, int(column)-1]
                    else:
                        # Process df data
                        value = df.iloc[row_number, int(col)]

                    # Convert value to specified data type
                    if dtype == 'float':
                        value = float(value)
                    elif dtype == 'str':
                        value = str(value)
                    elif dtype == 'int':
                        value = int(value)
                    elif dtype == 'bool':
                        value = bool(value)
                    elif dtype == 'datetime':
                        value = pd.to_datetime(value)
                    else:
                        Log.Log_Error(global_log_file, f'Unsupported data type {dtype} for key {key}')
                        continue

                    data_dict[key] = value
                except ValueError as ve:
                    Log.Log_Error(global_log_file, f'ValueError processing field {key}: {ve}')
                    data_dict[key] = None
                except Exception as e:
                    Log.Log_Error(global_log_file, f'Error processing field {key}: {e}')
                    data_dict[key] = None
                    continue

            # SQL connection and query
            conn, cursor = SQL.connSQL()
            if conn is None:
                Log.Log_Error(global_log_file, f"{data_dict.get('key_Serial_Number', 'Unknown')} : Connection with Prime Failed")
                row_number += 1
                continue  # Continue with next row
            try:
                part_number, nine_serial_number = SQL.selectSQL(cursor, df.iloc[row_number, 3])
            except Exception as e:
                Log.Log_Error(global_log_file, f"{data_dict.get('key_Serial_Number', 'Unknown')} : SQL query failed: {e}")
                SQL.disconnSQL(conn, cursor)
                row_number += 1
                continue  # Continue with next row
            SQL.disconnSQL(conn, cursor)

            if part_number is not None:
                data_dict['key_Part_Number'] = part_number
                data_dict['key_LotNumber_9'] = nine_serial_number
            else:
                Log.Log_Error(global_log_file, data_dict.get('key_Serial_Number', 'Unknown') + ' : ' + 'PartNumber Error')
                row_number += 1
                continue
            
            # Generate XML file
            if None in data_dict.values():
                Log.Log_Error(global_log_file, f"Skipping row {row_number} due to None values in data_dict")
            else:
                generate_xml(data_dict)
            row_number += 1

    def generate_xml(data_dict):
        print(data_dict.get('key_Start_Date_Time', ''))
        xml_filename = f"Site={site},ProductFamily={product_family},Operation={operation},PartNumber={data_dict.get('key_Part_Number', 'Unknown')},SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')}.xml"
        xml_filepath = os.path.join(output_path, xml_filename)
        with open(xml_filepath, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<Results>\n')
            f.write(f"    <Result startDateTime=\"{data_dict.get('key_Start_Date_Time', '')}\" Result=\"{data_dict.get('key_Result', 'Done')}\">\n")
            f.write(f"        <Header SerialNumber=\"{data_dict.get('key_Serial_Number', '')}\" PartNumber=\"{data_dict.get('key_Part_Number', '')}\" />\n")
            for key, value in data_dict.items():
                value_str = "" if value is None else str(value)
                f.write(f"        <Data key=\"{key}\" value=\"{value_str}\" />\n")
            f.write('    </Result>\n')
            f.write('</Results>\n')
        Log.Log_Info(global_log_file, f'XML File Created: {xml_filepath}')

    # Process Excel files based on input paths
    for input_path in input_paths:
        files = glob.glob(os.path.join(input_path, file_name_pattern))
        files = [file for file in files if not os.path.basename(file).startswith('~$')]
        if not files:
            Log.Log_Error(global_log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")
        for file in files:
            if not os.path.basename(file).startswith('~$'):
                destination_dir = '../DataFile/001_GRATING/'
                if not os.path.exists(destination_dir):
                    os.makedirs(destination_dir)
                shutil.copy(file, destination_dir)
                Log.Log_Info(global_log_file, f"Copy excel file {file} to ../DataFile/001_GRATING/")
                copied_file_path = os.path.join(destination_dir, os.path.basename(file))
                process_excel_file(copied_file_path)

# Main function to scan for all .ini files and process them
def main():
    ini_files = glob.glob("*.ini")
    for ini_file in ini_files:
        process_ini_file(ini_file)

if __name__ == '__main__':
    main()

Log.Log_Info(global_log_file, 'Program End')
