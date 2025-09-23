#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
このプログラムの機能：
1. 全ての .ini ファイルをスキャンする。
2. .ini の設定に基づき Excel ファイルを読み取り、データ処理を実行し、XML ファイルを生成する。
3. 実行記録およびエラーログは、カスタムモジュール Log によって処理される。

依存モジュール：
- Log, SQL, Check, Convert_Date, Row_Number_Func (全て ../MyModule 内)
"""

import os
import sys
import glob
import shutil
import logging
import pandas as pd
from configparser import ConfigParser, NoSectionError, NoOptionError
from datetime import datetime, timedelta, date

# カスタムモジュールのパスを追加し、インポート
sys.path.append('../MyModule')
import Log
import SQL
import Check
import Convert_Date
import Row_Number_Func

# グローバル変数
global_log_file = None

def setup_logging(log_file_path: str) -> None:
    """ログのフォーマットとファイル設定を行う"""
    try:
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
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
    """実行記録ファイルが存在しない場合は作成し、更新する"""
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(global_log_file, f"Running record file {running_rec_path} confirmed and updated with end date {end_date}")
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error processing running record file {running_rec_path}: {e}")

def read_running_rec(running_rec_path: str) -> datetime:
    """
    最後の実行記録を読み取る。
    ファイルが存在しないまたは内容が無効な場合は30日前の日付を返す。
    """
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
        Log.Log_Error(global_log_file, f"Error reading running record file {running_rec_path}: {e}")
        return datetime.today() - timedelta(days=30)

def generate_xml(data_dict: dict, output_dir: str, site: str, product_family: str, Test_Station: str) -> None:
    """
    XML テンプレートを用いて XML ファイルを生成する。
    ※ 画面に出力されるテキストは英語で表示される。
    """
    xml_filename = (
        f"Site={site},ProductFamily={product_family},Operation={data_dict['Operation']},"
        f"PartNumber={data_dict.get('key_Part_Number', 'Unknown')},"
        f"SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')},"
        f"Testdate={data_dict.get('key_Start_Date_Time', 'Unknown')}.xml"
    )
    xml_filepath = os.path.join(output_dir, xml_filename)
    with open(xml_filepath, 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="utf-8"?>\n')
        f.write('<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                'xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n')
        f.write(f'       <Result startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Result="{data_dict["key_Judge"]}">\n')
        f.write(f'               <Header SerialNumber="{data_dict["key_Serial_Number"]}" '
                f'PartNumber="{data_dict["key_Part_Number"]}" Operation="{data_dict["Operation"]}" '
                f'TestStation="{Test_Station}" Operator="{data_dict.get("key_Operator", "")}" '
                f'StartTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" '
                f'Site="{site}" LotNumber="{data_dict["key_Serial_Number"]}"/>\n')
        f.write('               <HeaderMisc>\n')
        f.write('                   <Item Description="Facet Coating"></Item>\n')
        f.write('               </HeaderMisc>\n')
        f.write(f'            <TestStep Name="{data_dict["Operation"]}" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="{data_dict["key_Judge"]}">\n')
        f.write(f'              <Data DataType="Numeric" Name="Aa" Units="um" Value="{data_dict["key_Aa"]}"/>\n')
        f.write(f'              <Data DataType="Numeric" Name="Ah" Units="um" Value="{data_dict["key_Ah"]}"/>\n')
        f.write(f'              <Data DataType="Numeric" Name="Dh" Units="um" Value="{data_dict["key_Dh"]}"/>\n')
        f.write(f'              <Data DataType="Numeric" Name="V_Max" Units="um" Value="{data_dict["key_V_Max"]}"/>\n')
        f.write('               </TestStep>\n')
        f.write(f'               <TestStep Name="SORTED_DATA" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="{data_dict["key_Judge"]}">\n')
        f.write(f'                   <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value="{data_dict["key_STARTTIME_SORTED"]}"/>\n')
        f.write(f'                   <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value="{data_dict["key_SORTNUMBER"]}"/>\n')
        f.write(f'                   <Data DataType="String" Name="LotNumber_5" Value="{data_dict["key_Serial_Number"]}" CompOperation="LOG"/>\n')
        f.write(f'                   <Data DataType="String" Name="LotNumber_9" Value="{data_dict["key_LotNumber_9"]}" CompOperation="LOG"/>\n')
        f.write('               </TestStep>\n')
        f.write('    </Result>\n')
        f.write('</Results>\n')
    Log.Log_Info(global_log_file, f"XML File Created: {xml_filepath}")

def process_excel_file(file_path: str, sheet_name: str, data_columns: list,
                       running_rec: str, output_path: str, fields: dict,
                       site: str, product_family: str, operation1: str, operation2: str,
                       Test_Station: str) -> None:
    """
    Excel ファイルを処理し、データの読み込み、変換、SQL クエリの実行、XML ファイルの生成を行う。
    """
    Log.Log_Info(global_log_file, f"Processing Excel File: {file_path}")
    
    # 指定されたパターンに一致するファイルを収集し、更新日時で最新のファイルを選択する
    excel_files = [
        [f, datetime.fromtimestamp(os.path.getmtime(f)).strftime("%Y-%m-%d %H:%M:%S")]
        for f in glob.glob(file_path) if '$' not in f
    ]
    if not excel_files:
        Log.Log_Error(global_log_file, f"Excel file not found: {file_path}")
        return
    latest_file = sorted(excel_files, key=lambda x: x[1], reverse=True)[0][0]
    
    dest_dir = '../DataFile/043_LD-SPUT/'
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, os.path.basename(latest_file))
    # コピー元とコピー先が同一の場合はコピーせずに利用する
    if os.path.abspath(latest_file) == os.path.abspath(dest_path):
        Excel_File = latest_file
    else:
        Excel_File = shutil.copy(latest_file, dest_dir)

    try:
        # Excel のデータを読み込み、最終有効列を判定する
        df_temp = pd.read_excel(Excel_File, header=None, sheet_name=sheet_name, nrows=2)
        last_non_empty_col = df_temp.iloc[1].last_valid_index()
        data_columns = list(range(2, last_non_empty_col + 1))
        df = pd.read_excel(Excel_File, header=None, sheet_name=sheet_name, usecols=data_columns, skiprows=1)
        new_rows = pd.DataFrame([[None] * df.shape[1]] * 3, columns=df.columns)
        df = pd.concat([df.iloc[:1], new_rows, df.iloc[1:]]).reset_index(drop=True)
        df = df.transpose()
        df = df.dropna(axis=1, how='all')
        df_split = df[0].str.split('\n', expand=True)
        df_split.columns = ['startdatetime', 'SerialNumber', 'Type']
        df = pd.concat([df_split, df.drop(columns=[0])], axis=1)
        df = df.dropna(axis=1, how='all')
        df['key_SORTNUMBER'] = df.index + 1
        df = df.drop(columns=df.columns[3:12])
        df = df[pd.to_datetime(df.iloc[:, 0], errors='coerce') >= (datetime.today() - timedelta(days=31))]
        df.rename(columns={df.columns[3]: 'Aa_EA'}, inplace=True)
        df.rename(columns={df.columns[4]: 'Aa_LD'}, inplace=True)
        df.rename(columns={df.columns[5]: 'Ah_EA'}, inplace=True)
        df.rename(columns={df.columns[6]: 'Ah_LD'}, inplace=True)
        df.rename(columns={df.columns[7]: 'Dh_EA'}, inplace=True)
        df.rename(columns={df.columns[8]: 'Dh_LD'}, inplace=True)
        df.rename(columns={df.columns[9]: 'Judge'}, inplace=True)
        df.rename(columns={df.columns[10]: 'V_EA_Max'}, inplace=True)
        df.rename(columns={df.columns[11]: 'V_LD_Max'}, inplace=True)
        df['startdatetime'] = pd.to_datetime(df['startdatetime'], errors='coerce').dt.strftime('%Y-%m-%d %H.%M.%S')
        df['startdatetime'] = df['startdatetime'].str.replace(' ', 'T')
        df['Judge'] = df['Judge'].apply(lambda x: 'Passed' if x == '合格' else 'Fail')
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading Excel file {file_path}: {e}")
        return

    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)

    if 'key_Start_Date_Time' in fields:
        try:
            start_date_col = int(fields['key_Start_Date_Time'][0]) - 1
        except Exception as e:
            Log.Log_Error(global_log_file, f"Error processing start_date_col: {e}")
            return
    else:
        Log.Log_Error(global_log_file, "key_Start_Date_Time not found in fields configuration")
        return

    Serial_Number = df['SerialNumber'].tolist()
    conn, cursor = SQL.connSQL()
    if conn is None:
        Log.Log_Error(global_log_file, "Connection with Prime Failed for Serial Numbers: " + str(Serial_Number))
        return
    try:
        for serial in Serial_Number:
            part_number, nine_serial_number = SQL.selectSQL(cursor, serial)
            df.loc[df['SerialNumber'] == serial, 'Part_Number'] = part_number
            df.loc[df['SerialNumber'] == serial, 'Nine_Serial_Number'] = nine_serial_number
    except Exception as e:
        Log.Log_Error(global_log_file, f"SQL query failed for Serial Numbers {Serial_Number}: {e}")
    finally:
        SQL.disconnSQL(conn, cursor)
    
    df = df.dropna(subset=['Part_Number']).reset_index(drop=True)
    row_end = len(df)
    row_number = 0

    while row_number < row_end:
        data_dict = {}
        if row_number == row_end - 1:
            try:
                latest_date = df.iloc[:, start_date_col].max()
                update_running_rec(running_rec, latest_date)
            except KeyError as e:
                Log.Log_Error(global_log_file, f"KeyError processing start_date_col: {e}")
                return
            
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

        try:
            dt = datetime.strptime(str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'), "%Y-%m-%d %H:%M:%S")
            data_dict["key_STARTTIME_SORTED"] = int(str(dt - datetime(1899, 12, 30)).split()[0])
        except Exception as e:
            Log.Log_Error(global_log_file, f"Date conversion error: {e}")
            data_dict["key_STARTTIME_SORTED"] = None

        if df.loc[row_number, 'Part_Number'] is not None:
            data_dict['key_Part_Number'] = df.loc[row_number, 'Part_Number']
            data_dict['key_LotNumber_9'] = df.loc[row_number, 'Nine_Serial_Number']
        else:
            Log.Log_Error(global_log_file, f"{data_dict.get('key_Serial_Number', 'Unknown')} : PartNumber Error")
            row_number += 1
            continue

        data_dict_EA = {
            "key_Start_Date_Time": data_dict["key_Start_Date_Time"],
            "key_Serial_Number": data_dict["key_Serial_Number"],
            "Operation": operation1,
            "key_Operator": "NA",
            "key_Aa": data_dict.get("key_Aa_EA"),
            "key_Ah": data_dict.get("key_Ah_EA"),
            "key_Dh": data_dict.get("key_Dh_EA"),
            "key_Judge": data_dict.get("key_Judge"),
            "key_V_Max": data_dict.get("key_V_EA_Max"),
            "key_Part_Number": data_dict.get("key_Part_Number"),
            "key_STARTTIME_SORTED": data_dict.get("key_STARTTIME_SORTED"),
            "key_SORTNUMBER": data_dict.get("key_SORTNUMBER"),
            "key_LotNumber_9": data_dict.get("key_LotNumber_9")
        }

        data_dict_LD = {
            "key_Start_Date_Time": data_dict["key_Start_Date_Time"],
            "key_Serial_Number": data_dict["key_Serial_Number"],
            "Operation": operation2,
            "key_Operator": "NA",
            "key_Aa": data_dict.get("key_Aa_LD"),
            "key_Ah": data_dict.get("key_Ah_LD"),
            "key_Dh": data_dict.get("key_Dh_LD"),
            "key_Judge": data_dict.get("key_Judge"),
            "key_V_Max": data_dict.get("key_V_LD_Max"),
            "key_Part_Number": data_dict.get("key_Part_Number"),
            "key_STARTTIME_SORTED": data_dict.get("key_STARTTIME_SORTED"),
            "key_SORTNUMBER": data_dict.get("key_SORTNUMBER"),
            "key_LotNumber_9": data_dict.get("key_LotNumber_9")
        }

        if None in data_dict.values():
            Log.Log_Error(global_log_file, f"Skipping row {row_number} due to None values in data_dict")
        else:
            generate_xml(data_dict_EA, output_path, site, product_family, Test_Station)
            generate_xml(data_dict_LD, output_path, site, product_family, Test_Station)
        row_number += 1
        Log.Log_Info(global_log_file, "Write the next starting line number")
        Row_Number_Func.next_start_row_number("EA-WG_LD-WG_StartROW.txt", row_number)

def process_ini_file(config_path: str) -> None:
    """
    指定された .ini ファイルを処理し、設定情報を読み込んで Excel および XML の処理を実行する。
    """
    global global_log_file
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
        running_rec = config.get('Paths', 'running_rec')
        sheet_name = config.get('Excel', 'sheet_name')
        data_columns = list(map(int, config.get('Excel', 'data_columns').split(':')))
        log_path = config.get('Logging', 'log_path')
        fields_config = config.get('DataFields', 'fields').splitlines()
        site = config.get('Basic_info', 'Site')
        product_family = config.get('Basic_info', 'ProductFamily')
        operation1 = config.get('Basic_info', 'Operation1')
        operation2 = config.get('Basic_info', 'Operation2')
        Test_Station = config.get('Basic_info', 'TestStation')
        file_name_pattern = config.get('Basic_info', 'file_name_pattern')
    except NoSectionError as e:
        Log.Log_Error(global_log_file, f"Missing section in config file {config_path}: {e}")
        return
    except NoOptionError as e:
        Log.Log_Error(global_log_file, f"Missing option in config file {config_path}: {e}")
        return

    # ログフォルダとログファイルの作成
    log_folder_name = str(datetime.today().date())
    log_folder_path = os.path.join(log_path, log_folder_name)
    os.makedirs(log_folder_path, exist_ok=True)
    log_file = os.path.join(log_folder_path, '044_EA-WG_LD-WG.log')
    global_log_file = log_file

    setup_logging(global_log_file)
    Log.Log_Info(log_file, f"Program Start for config {config_path}")

    # フィールド設定を辞書に解析する
    fields = {}
    for field in fields_config:
        if field.strip():
            try:
                key, col, dtype = field.split(':')
                fields[key.strip()] = (col.strip(), dtype.strip())
            except ValueError:
                Log.Log_Error(global_log_file, f"Field configuration parse error: {field}")
                continue

    for input_path in input_paths:
        files = glob.glob(os.path.join(input_path, file_name_pattern))
        files = [file for file in files if not os.path.basename(file).startswith('~$')]
        if not files:
            Log.Log_Error(global_log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")
        for file in files:
            dest_dir = '../DataFile/044_EA-WG_LD_WG/'
            os.makedirs(dest_dir, exist_ok=True)
            shutil.copy(file, dest_dir)
            Log.Log_Info(global_log_file, f"Copy excel file {file} to ../DataFile/044_EA-WG_LD_WG/")
            copied_file_path = os.path.join(dest_dir, os.path.basename(file))
            process_excel_file(copied_file_path, sheet_name, data_columns, running_rec,
                               output_path, fields, site, product_family, operation1, operation2, Test_Station)

def main() -> None:
    """全ての .ini ファイルをスキャンして処理を実行する"""
    ini_files = glob.glob("*.ini")
    for ini_file in ini_files:
        process_ini_file(ini_file)

if __name__ == '__main__':
    main()
    Log.Log_Info(global_log_file, "Program End")
