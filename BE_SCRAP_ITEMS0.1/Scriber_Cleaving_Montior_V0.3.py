#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
このプログラムは、すべての .ini ファイルを読み込み、設定に基づいて Excel データを処理し、XML ファイルを生成します。
実行ログとエラーログはカスタムモジュール Log を使用して出力されます。

依存モジュール:
- Log, SQL, Check, Convert_Date, Row_Number_Func (../MyModule 内)
"""

import os
import sys
import glob
import shutil
import logging
import pandas as pd
from configparser import ConfigParser, NoSectionError, NoOptionError
from datetime import datetime, timedelta, date

sys.path.append('../MyModule')
import Log
import SQL
import Check
import Convert_Date
import Row_Number_Func

global_log_file = None

def setup_logging(log_file_path: str) -> None:
    """ログの形式と出力先を設定する"""
    try:
        logging.basicConfig(
            filename=log_file_path,
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    except OSError as e:
        print(f"Error setting up log file {log_file_path}: {e}")
        raise

def update_running_rec(running_rec_path: str, end_date: datetime) -> None:
    """実行記録ファイルを更新する"""
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(global_log_file, f"Running record file {running_rec_path} updated with end date {end_date}")
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error updating running record file {running_rec_path}: {e}")

def ensure_running_rec_exists_and_update(running_rec_path: str, end_date: datetime) -> None:
    """実行記録ファイルが存在しなければ作成して更新する"""
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(global_log_file, f"Running record file {running_rec_path} confirmed and updated with end date {end_date}")
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error processing running record file {running_rec_path}: {e}")

def read_running_rec(running_rec_path: str) -> datetime:
    """
    最後の実行記録を読み込み、ファイルが存在しないまたは内容が無効な場合は DayGap 日前の日時を返す。
    """
    DayGap = 5
    if not os.path.exists(running_rec_path):
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write('')
        return datetime.today() - timedelta(days=DayGap)
    try:
        with open(running_rec_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                last_run_date = pd.to_datetime(content, errors='coerce')
                if pd.isnull(last_run_date):
                    return datetime.today() - timedelta(days=DayGap)
                return last_run_date
            else:
                return datetime.today() - timedelta(days=DayGap)
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading running record file {running_rec_path}: {e}")
        return datetime.today() - timedelta(days=DayGap)

def process_excel_file(file_path: str, sheet_name: str, data_columns, running_rec: str,
                       output_path: str, fields: dict, site: str, product_family: str,
                       operation: str, Test_Station: str) -> None:
    """Excel ファイルを読み込み、データ変換後に XML ファイルを生成する"""
    Log.Log_Info(global_log_file, f"Processing Excel File: {file_path}")
    Excel_file_list = []
    for file in glob.glob(file_path):
        if '$' not in file:
            dt = datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d %H:%M:%S")
            Excel_file_list.append([file, dt])
    if not Excel_file_list:
        Log.Log_Error(global_log_file, f"Excel file not found: {file_path}")
        return
    Excel_file_list = sorted(Excel_file_list, key=lambda x: x[1], reverse=True)
    Excel_File = Excel_file_list[0][0]

    try:
        df = pd.read_csv(Excel_File, header=0)
        df['key_SORTNUMBER'] = df.index + 2
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading Excel file {file_path}: {e}")
        return

    df.columns = range(df.shape[1])
    df['key_Start_Date_Time'] = df.apply(lambda row: f"{str(row[0])} {str(row[1])}", axis=1)
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('key_Start_Date_Time')))
    df = df[cols]
    df = df.drop(columns=[0, 1])
    df = df.reset_index(drop=True)
    df.columns = range(df.shape[1])
    df = df.dropna(subset=[0])
    for key, (col, dtype) in fields.items():
        df.rename(columns={int(col): key}, inplace=True)

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    one_month_ago = read_running_rec(running_rec)
    df = df[pd.to_datetime(df['key_Start_Date_Time']) >= one_month_ago]
    df['key_Serial_Number'] = df['Nine_Serial_Number'].apply(lambda x: str(x)[4:9])
    df['key_Start_Date_Time'] = pd.to_datetime(df['key_Start_Date_Time'], format='%Y/%m/%d %H:%M:%S').dt.strftime('%Y-%m-%dT%H.%M.%S')

    Serial_Number = df['key_Serial_Number'].tolist()
    conn, cursor = SQL.connSQL()
    if conn is None:
        Log.Log_Error(global_log_file, "Connection with Prime Failed")
        return
    try:
        for serial in Serial_Number:
            part_number, nine_serial_number = SQL.selectSQL(cursor, serial)
            if part_number and nine_serial_number:
                df.loc[df['key_Serial_Number'] == serial, 'Part_Number'] = part_number
                df.loc[df['key_Serial_Number'] == serial, 'Nine_Serial_Number'] = nine_serial_number
            else:
                Log.Log_Error(global_log_file, f"Serial number {serial} not found in database")
    except Exception as e:
        Log.Log_Error(global_log_file, f"SQL query failed: {e}")
    finally:
        SQL.disconnSQL(conn, cursor)

    df = df.dropna(subset=['Part_Number'])
    df = df.reset_index(drop=True)
    row_end = len(df)
    row_number = 0
    while row_number < row_end:
        data_dict = {}
        if row_number == row_end - 1:
            latest_date = df['key_Start_Date_Time'].max()
            update_running_rec(running_rec, latest_date)
        for key, (col, dtype) in fields.items():
            try:
                value = df.iloc[row_number, int(col)]
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
                    Log.Log_Error(global_log_file, f"Unsupported data type {dtype} for key {key}")
                    continue
                data_dict[key] = value
            except ValueError as ve:
                Log.Log_Error(global_log_file, f"ValueError processing field {key}: {ve}")
                data_dict[key] = None
            except Exception as e:
                Log.Log_Error(global_log_file, f"Error processing field {key}: {e}")
                data_dict[key] = None
                continue
        data_dict['key_SORTNUMBER'] = df.loc[row_number, 'key_SORTNUMBER']
        data_dict['Part_Number'] = df.loc[row_number, 'Part_Number']
        data_dict['key_Serial_Number'] = df.loc[row_number, 'key_Serial_Number']
        data_dict['key_Operation'] = operation
        try:
            dt = datetime.strptime(str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'), "%Y-%m-%d %H:%M:%S")
            date_excel_number = int(str(dt - datetime(1899, 12, 30)).split()[0])
        except Exception as e:
            Log.Log_Error(global_log_file, f"Date conversion error: {e}")
            date_excel_number = None
        data_dict["key_STARTTIME_SORTED"] = date_excel_number
        if None in data_dict.values():
            Log.Log_Error(global_log_file, f"Skipping row {row_number} due to None values in data_dict")
        else:
            generate_xml(data_dict, output_path, site, product_family, operation, Test_Station)
        row_number += 1
        Log.Log_Info(global_log_file, "Write the next starting line number")
        Row_Number_Func.next_start_row_number(log_file, row_number)

def generate_xml(data_dict: dict, output_path: str, site: str, product_family: str,
                 operation: str, Test_Station: str) -> None:
    """受け取ったデータから XML ファイルを生成する"""
    print(data_dict.get('key_Start_Date_Time', ''))
    xml_filename = (
        f"Site={site},ProductFamily={product_family},Operation={operation},"
        f"PartNumber={data_dict.get('Part_Number', 'Unknown')},"
        f"SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')},"
        f"Testdate={data_dict.get('key_Start_Date_Time', 'Unknown')}.xml"
    )
    xml_filepath = os.path.join(output_path, xml_filename)
    Log.Log_Info(global_log_file, f"XML File Path: {xml_filepath}")
    with open(xml_filepath, 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="utf-8"?>\n')
        f.write('<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n')
        f.write(f'    <Result startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Result="Passed">\n')
        f.write(f'        <Header SerialNumber="{data_dict["key_Serial_Number"]}" PartNumber="{data_dict["Part_Number"]}" Operation="{operation}" TestStation="{Test_Station}" Operator="{data_dict["key_Operator"]}" StartTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Site="{site}" LotNumber="{data_dict["key_Serial_Number"]}"/>\n')
        f.write('        <HeaderMisc>\n')
        f.write(f'            <Item Description="{operation}"></Item>\n')
        f.write('        </HeaderMisc>\n')
        f.write(f'        <TestStep Name="{data_dict["key_Operation"]}" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')
        f.write(f'            <Data DataType="String" Name="Scriber" Units="" Value="{data_dict["key_scriber"]}"/>\n')
        f.write(f'            <Data DataType="String" Name="Cleaving" Units="" Value="{data_dict["key_cleaving"]}"/>\n')
        f.write(f'            <Data DataType="String" Name="Neddle_vendor" Units="" Value="{data_dict["key_neddle_vendor"]}"/>\n')
        f.write(f'            <Data DataType="String" Name="Neddle_no" Units="" Value="{data_dict["key_neddle_no"]}"/>\n')
        f.write(f'            <Data DataType="Numeric" Name="scribe_length" Units="" Value="{data_dict["key_scribe_length"]}"/>\n')
        f.write(f'            <Data DataType="Numeric" Name="scribe_force" Units="" Value="{data_dict["key_scribe_force"]}"/>\n')
        f.write(f'            <Data DataType="Numeric" Name="unseparate_No" Units="" Value="{data_dict["key_unseparate"]}"/>\n')
        f.write(f'            <Data DataType="Numeric" Name="peeling_No" Units="" Value="{data_dict["key_peeling"]}"/>\n')
        f.write('        </TestStep>\n')
        f.write(f'        <TestStep Name="SORTED_DATA" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')
        f.write(f'            <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value="{data_dict["key_STARTTIME_SORTED"]}"/>\n')
        f.write(f'            <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value="{data_dict["key_SORTNUMBER"]}"/>\n')
        f.write(f'            <Data DataType="String" Name="LotNumber_5" Value="{data_dict["key_Serial_Number"]}" CompOperation="LOG"/>\n')
        f.write(f'            <Data DataType="String" Name="LotNumber_9" Value="{data_dict["Nine_Serial_Number"]}" CompOperation="LOG"/>\n')
        f.write('        </TestStep>\n')
        f.write('        <TestEquipment>\n')
        f.write(f'            <Item DeviceName="Scriber" DeviceSerialNumber="{data_dict["key_scriber"]}"></Item>\n')
        f.write(f'            <Item DeviceName="Cleaving" DeviceSerialNumber="{data_dict["key_cleaving"]}"></Item>\n')
        f.write('        </TestEquipment>\n')
        f.write('    </Result>\n')
        f.write('</Results>\n')
    Log.Log_Info(global_log_file, f"XML File Created: {xml_filepath}")

def process_ini_file(config_path: str) -> None:
    """.ini ファイルを読み込み、Excel と XML の処理を実行する"""
    global global_log_file, input_paths, output_path, xml_path, running_rec, sheet_name, data_columns, log_path, log_file, fields, site, product_family, operation, Test_Station, file_name_pattern, file_location, DayGap

    config = ConfigParser()
    try:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config.read_file(line for line in config_file if not line.strip().startswith('#'))
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading config file {config_path}: {e}")
        return

    try:
        input_paths = [path.strip() for path in config.get('Paths', 'input_paths').split(',')]
        output_path = config.get('Paths', 'output_path')
        xml_path = config.get('Paths', 'xml_path')
        running_rec = config.get('Paths', 'running_rec')
        sheet_name = config.get('Excel', 'sheet_name')
        data_columns = config.get('Excel', 'data_columns')
        log_path = config.get('Logging', 'log_path')
        fields_config = config.get('DataFields', 'fields').splitlines()
        site = config.get('Basic_info', 'Site')
        product_family = config.get('Basic_info', 'ProductFamily')
        operation = config.get('Basic_info', 'Operation')
        Test_Station = config.get('Basic_info', 'TestStation')
        DayGap = config.get('Basic_info', 'DayGap')
        file_name_pattern = config.get('Basic_info', 'file_name_pattern')
        file_location = config.get('Logging', 'file_location')
        log_file = config.get('Logging', 'log_file')
    except NoSectionError as e:
        Log.Log_Error(global_log_file, f"Missing section in config file {config_path}: {e}")
        return
    except NoOptionError as e:
        Log.Log_Error(global_log_file, f"Missing option in config file {config_path}: {e}")
        return

    log_folder_name = str(datetime.today().date())
    log_folder_path = os.path.join(log_path, log_folder_name)
    if not os.path.exists(log_folder_path):
        os.makedirs(log_folder_path)
    log_file = os.path.join(log_folder_path, log_file)
    global_log_file = log_file
    setup_logging(global_log_file)
    Log.Log_Info(log_file, f"Program Start for config {config_path}")

    fields = {}
    for field in fields_config:
        if field.strip():
            key, col, dtype = field.split(':')
            fields[key.strip()] = (col.strip(), dtype.strip())
    for input_path in input_paths:
        files = glob.glob(os.path.join(input_path, file_name_pattern))
        files = [file for file in files if not os.path.basename(file).startswith('~$')]
        if not files:
            Log.Log_Error(global_log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")
        for file in files:
            if not os.path.basename(file).startswith('~$'):
                destination_dir = file_location
                if not os.path.exists(destination_dir):
                    os.makedirs(destination_dir)
                shutil.copy(file, destination_dir)
                Log.Log_Info(global_log_file, f"Copy excel file {file} to {file_location}")
                copied_file_path = os.path.join(destination_dir, os.path.basename(file))
                process_excel_file(copied_file_path, sheet_name, data_columns, running_rec,
                                   output_path, fields, site, product_family, operation, Test_Station)

def main() -> None:
    """カレントディレクトリ内の .ini ファイルをスキャンして処理を実行する"""
    ini_files = glob.glob("*.ini")
    for ini_file in ini_files:
        process_ini_file(ini_file)

if __name__ == '__main__':
    main()
    Log.Log_Info(global_log_file, "Program End")
