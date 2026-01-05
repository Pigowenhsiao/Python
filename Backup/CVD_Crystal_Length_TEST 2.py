# Python程式碼 - 讀取config.ini並根據內容處理資料和生成XML文件
import os
import sys
import glob
import shutil
import logging
import pandas as pd
from configparser import ConfigParser
from datetime import datetime, timedelta

# 自定義模組
sys.path.append('../MyModule')
import Log
import SQL
import Check
import Convert_Date
import Row_Number_Func

# 讀取 config.ini 的設置
config = ConfigParser()
with open('Config_GRATING_CVD_Duty.ini', 'r', encoding='utf-8') as config_file:
    config.read_file(line for line in config_file if not line.strip().startswith('#'))

# 獲取配置
input_paths = [path.strip() for path in config.get('Paths', 'input_paths').split(',')]
output_path = config.get('Paths', 'output_path')
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

# 創建日志文件夾
log_folder_name = str(datetime.today().date())
log_folder_path = os.path.join(log_path, log_folder_name)
if not os.path.exists(log_folder_path):
    os.makedirs(log_folder_path)
log_file = os.path.join(log_folder_path, '001_Crystal.log')
Log.Log_Info(log_file, 'Program Start')

# 將欄位設定解析為字典
fields = {}
for field in fields_config:
    if field.strip():
        key, col, dtype = field.split(':')
        fields[key.strip()] = (col.strip(), dtype.strip())

def process_excel_file(file_path):
    Log.Log_Info(log_file, f'Processing Excel File: {file_path}')
    
    # 讀取Excel數據
    df = pd.read_excel(file_path, header=None, sheet_name=sheet_name, usecols=data_columns, skiprows=100)
    df_xy = pd.read_excel(file_path, header=None, sheet_name=xy_sheet_name, usecols=xy_columns)
    
    # 設置列號
    df.columns = range(df.shape[1])
    df_xy.columns = range(df_xy.shape[1])
    df = df.dropna(subset=[0])    # 刪除df[0] 為空值的資料列
 
# 設置列號
    df.columns = range(df.shape[1])
    df_xy.columns = range(df_xy.shape[1])

    # 創建輸出XML文件夾
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    row_end = len(df)
    row_number = 0

    # 設定處理的日期範圍 (一個月內)
    current_date = datetime.today()
    one_month_ago = current_date - timedelta(days=30)
    
    # 濾除 key_Start_Date_Time > 一個月以上的資料
    if 'key_Start_Date_Time' in fields:
        start_date_col = int(fields['key_Start_Date_Time'][0])
        df = df[df[start_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]
    else:
        Log.Log_Error(log_file, 'key_Start_Date_Time not found in fields configuration')

    # 數據處理
    while row_number < row_end:
        data_dict = {}
  
        # 處理數據轉換
        for key, (col, dtype) in fields.items():
            try:
                if col.startswith('xy'):
                # 處理df_xy數據
                    _, row, column = col.split('_')
                    if int(column) > 1:  # 忽略第一列
                        value = df_xy.iloc[int(row)-1, int(column)-1]
                else:
                # 處理df數據
                    value = df.iloc[row_number, int(col)]                
                # 根據指定的數據類型轉換值
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
                    Log.Log_Error(log_file, f'Unsupported data type {dtype} for key {key}')
                    continue                
                data_dict[key] = value
            except ValueError as ve:
                Log.Log_Error(log_file, f'ValueError processing field {key}: {ve}')
                data_dict[key] = None
            except Exception as e:
                Log.Log_Error(log_file, f'Error processing field {key}: {e}')
                data_dict[key] = None
      
        # SQL連接和查詢
        conn, cursor = SQL.connSQL()
        if conn is None:
            Log.Log_Error(log_file, data_dict.get('key_Serial_Number', 'Unknown') + ' : ' + 'Connection with Prime Failed')
            row_number += 1
            continue  # 繼續處理下一行
        part_number, nine_serial_number = SQL.selectSQL(cursor, df.iloc[row_number,3])
        print('serial_num:',df.iloc[row_number,3])
        SQL.disconnSQL(conn, cursor)

        if part_number is not None:
            data_dict['key_Part_Number'] = part_number
            data_dict['key_LotNumber_9'] = nine_serial_number
        else:
            Log.Log_Error(log_file, data_dict.get('key_Serial_Number', 'Unknown') + ' : ' + 'PartNumber Error')
            row_number += 1
            continue
        
        # 生成XML文件
        generate_xml(data_dict)
        row_number += 1

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
    Log.Log_Info(log_file, f'XML File Created: {xml_filepath}')

def main():
    for input_path in input_paths:
        files = glob.glob(os.path.join(input_path, file_name_pattern))
        if not files:
            Log.Log_Error(log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")
        for file in files:
            if not os.path.basename(file).startswith('$'):
                # Copy the file to the destination directory
                destination_dir = '../DataFile/001_GRATING/'
                if not os.path.exists(destination_dir):
                    os.makedirs(destination_dir)
                    shutil.copy(file, destination_dir)
                Log.Log_Info(log_file, f"Copy excel file {file} to ../DataFile/001_GRATING/")                
                # Process the copied file
                copied_file_path = os.path.join(destination_dir, os.path.basename(file))
                process_excel_file(copied_file_path)

if __name__ == '__main__':
    main()

Log.Log_Info(log_file, 'Program End')
