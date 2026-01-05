# Python程序碼 - 讀取所有.ini文件並執行數據處理和生成XML文件
import os
import sys
import glob
import shutil
import logging
import pandas as pd
from configparser import ConfigParser
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
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading config file {config_path}: {e}")
        return

    try:
        # Fetch settings from config
        if not config.has_section('Paths'):
            Log.Log_Error(global_log_file, "Missing 'Paths' section in config file")
            return

        input_paths = [path.strip() for path in config.get('Paths', 'input_paths').split(',')]
        output_path = config.get('Paths', 'output_path')
        running_rec_path = config.get('Paths', 'running_rec', fallback=None)
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

        # Read the last processed row if available
        last_processed_row = 0
        if running_rec_path and os.path.exists(running_rec_path):
            try:
                with open(running_rec_path, 'r', encoding='utf-8') as rec_file:
                    last_processed_row = int(rec_file.readline().strip())
            except Exception as e:
                Log.Log_Error(global_log_file, f'Error reading running record file {running_rec_path}: {e}')
                last_processed_row = 0

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
            
            row_end = len(df)
            row_number = last_processed_row

            # Set processing date range (within one month)
            current_date = datetime.today()
            one_month_ago = current_date - timedelta(days=30)

            # Filter out rows with key_Start_Date_Time older than one month
            if 'key_Start_Date_Time' in fields:
                start_date_col = int(fields['key_Start_Date_Time'][0])
                df = df[df[start_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]
            else:
                Log.Log_Error(global_log_file, 'key_Start_Date_Time not found in fields configuration')

            # Data processing
            while row_number < row_end:
                data_dict = {}

                # Process data conversion
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

                # SQL connection and query
                try:
                    conn, cursor = SQL.connSQL()
                    if conn is None:
                        raise Exception('Connection with Prime Failed')
                    part_number, nine_serial_number = SQL.selectSQL(cursor, df.iloc[row_number, 3])
                    SQL.disconnSQL(conn, cursor)

                    if part_number is not None:
                        data_dict['key_Part_Number'] = part_number
                        data_dict['key_LotNumber_9'] = nine_serial_number
                    else:
                        raise Exception('PartNumber Error')
                except Exception as e:
                    Log.Log_Error(global_log_file, f"Error during SQL query for row {row_number}: {e}")
                    row_number += 1
                    continue
                
                # Generate XML file
                try:
                    generate_xml(data_dict)
                except Exception as e:
                    Log.Log_Error(global_log_file, f"Error generating XML for row {row_number}: {e}")
                row_number += 1

            # Write the last processed row to the running record file
            if running_rec_path:
                try:
                    with open(running_rec_path, 'w', encoding='utf-8') as rec_file:
                        rec_file.write(str(row_number))
                except Exception as e:
                    Log.Log_Error(global_log_file, f'Error writing running record to file {running_rec_path}: {e}')

        def generate_xml(data_dict):
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
            try:
                files = glob.glob(os.path.join(input_path, file_name_pattern))
                if not files:
                    Log.Log_Error(global_log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")
                    continue
                for file in files:
                    if not os.path.basename(file).startswith('$'):
                        destination_dir = '../DataFile/001_GRATING/'
                        if not os.path.exists(destination_dir):
                            os.makedirs(destination_dir)
                            shutil.copy(file, destination_dir)
                        Log.Log_Info(global_log_file, f"Copy excel file {file} to ../DataFile/001_GRATING/")
                        copied_file_path = os.path.join(destination_dir, os.path.basename(file))
                        process_excel_file(copied_file_path)
            except Exception as e:
                Log.Log_Error(global_log_file, f"Error processing files in path {input_path}: {e}")
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error processing ini file {config_path}: {e}")
# Main function to scan for all .ini files and process them
def main():
    ini_files = glob.glob("*.ini")
    for ini_file in ini_files:
        try:
            process_ini_file(ini_file)
        except Exception as e:
            Log.Log_Error(global_log_file, f"Error processing ini file {ini_file}: {e}")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        Log.Log_Error(global_log_file, f"Unexpected error: {e}")

Log.Log_Info(global_log_file, 'Program End')
