#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
本プログラムの主な機能：
1. すべての.iniファイルをスキャンする。
2. 各.iniファイルから設定を読み取り、設定に基づいてExcelファイルを処理する。
3. Excelデータの処理を行い、XMLファイルを生成する。
4. 実行ログおよびエラーログは、Logモジュールを通じて記録される。

依存モジュール：
- Log, SQL, Check, Convert_Date, Row_Number_Func (カスタムモジュール)
"""

import os
import sys
import glob
import shutil
import logging
import pandas as pd
from configparser import ConfigParser, NoSectionError, NoOptionError
from datetime import datetime, timedelta, date

# カスタムモジュールの読み込み（パスを追加）
sys.path.append('../MyModule')
import Log
import SQL
import Check
import Convert_Date
import Row_Number_Func

# グローバル変数：ログファイルのパスを記録
global_log_file = None

####################################
# 共通ログおよび実行記録関連関数
####################################
def setup_logging(log_file_path: str) -> None:
    """ログのフォーマットとファイル設定を行う"""
    try:
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
    except OSError as e:
        print(f"Error setting up log file {log_file_path}: {e}")
        raise

def update_running_rec(running_rec_path: str, end_date: datetime) -> None:
    """実行記録ファイルを更新する（更新のみ）"""
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
    ファイルが存在しないまたは不正な場合は30日前の日付を返す。
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

####################################
# XML生成関数（独立関数）
####################################
def generate_xml(data_dict: dict, output_path: str, site: str, product_family: str, 
                 operation: str, Test_Station: str) -> None:
    """
    XMLテンプレートを用いてXMLファイルを生成する。
    画面出力メッセージは英語のまま。
    """
    xml_filename = (f"Site={site},ProductFamily={product_family},Operation={operation},"
                    f"PartNumber={data_dict.get('key_Part_Number', 'Unknown')},"
                    f"SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')},"
                    f"Testdate={data_dict.get('key_Start_Date_Time', 'Unkonow')}.xml")
    xml_filepath = os.path.join(output_path, xml_filename)
    with open(xml_filepath, 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="utf-8"?>\n')
        f.write('<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                'xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n')
        f.write(f'    <Result startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Result="Passed">\n')
        f.write(f'        <Header SerialNumber="{data_dict["key_Serial_Number"]}" '
                f'PartNumber="{data_dict["key_Part_Number"]}" Operation="{operation}" '
                f'TestStation="{Test_Station}" Operator="{data_dict["key_Operator"]}" '
                f'StartTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" '
                f'Site="{site}" LotNumber="{data_dict["key_Serial_Number"]}"/>\n')
        f.write('        <HeaderMisc>\n')
        f.write('            <Item Description="AFM_Step_Height"></Item>\n')
        f.write('        </HeaderMisc>\n')
        f.write(f'        <TestStep Name="{data_dict["key_Operation"]}" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')
        # 指定されたキー群に対してデータ出力
        for key in ["key_Ah_L1", "key_Ah_L2", "key_Ah_R1", "key_Ah_R2",
                    "key_Da_L1", "key_Da_L2", "key_Da_R1", "key_Da_R2",
                    "key_Dh_L1", "key_Dh_L2", "key_Dh_R1", "key_Dh_R2"]:
            f.write(f'            <Data DataType="Numeric" Name="{key.split("_")[1]}_{key.split("_")[2]}" Units="um" Value="{data_dict[key]}"/>\n')
        f.write('        </TestStep>\n')
        f.write(f'        <TestStep Name="SORTED_DATA" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')
        f.write(f'            <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value="{data_dict["key_STARTTIME_SORTED"]}"/>\n')
        f.write(f'            <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value="{data_dict["key_SORTNUMBER"]}"/>\n')
        f.write(f'            <Data DataType="String" Name="LotNumber_5" Value="{data_dict["key_Serial_Number"]}" CompOperation="LOG"/>\n')
        f.write(f'            <Data DataType="String" Name="LotNumber_9" Value="{data_dict["key_LotNumber_9"]}" CompOperation="LOG"/>\n')
        f.write('        </TestStep>\n')
        f.write('        <TestEquipment>\n')
        f.write(f'            <Item DeviceName="MOCVD" DeviceSerialNumber="{data_dict["key_Equipment"]}"></Item>\n')
        f.write('        </TestEquipment>\n')
        f.write('    </Result>\n')
        f.write('</Results>\n')
    Log.Log_Info(global_log_file, f"XML File Created: {xml_filepath}")

####################################
# Excelファイル処理関数（独立関数）
####################################
def process_excel_file(file_path: str, sheet_name: str, data_columns: str,
                       running_rec: str, output_path: str, fields: dict,
                       site: str, product_family: str, operation: str, Test_Station: str) -> None:
    """
    Excelファイルを処理し、データの読み取り、変換、SQLクエリ実行、XML生成を行う。
    必要なパラメータはすべて引数として渡す。
    """
    Log.Log_Info(global_log_file, f"Processing Excel File: {file_path}")
    
    # 指定パターンに一致するファイルを収集し、更新日時でソートして最新のファイルを選択
    excel_files = [[f, datetime.fromtimestamp(os.path.getmtime(f)).strftime("%Y-%m-%d %H:%M:%S")]
                   for f in glob.glob(file_path) if '$' not in f]
    if not excel_files:
        Log.Log_Error(global_log_file, f"Excel file not found: {file_path}")
        return
    latest_file = sorted(excel_files, key=lambda x: x[1], reverse=True)[0][0]
    
    dest_dir = '../DataFile/045_Ru_AFM/'
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, os.path.basename(latest_file))
    # コピー先と元が同じ場合はコピーせずそのまま利用
    if os.path.abspath(latest_file) == os.path.abspath(dest_path):
        Excel_File = latest_file
    else:
        Excel_File = shutil.copy(latest_file, dest_dir)
    
    try:
        # Excelデータを読み取る（skiprows=100）
        df = pd.read_excel(Excel_File, header=None, sheet_name=sheet_name, usecols=data_columns, skiprows=100)
        df['key_SORTNUMBER'] = df.index + 100
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading Excel file {file_path}: {e}")
        return

    # 列番号を再設定し、0列目がNaNの行を削除
    df.columns = range(df.shape[1])
    df = df.dropna(subset=[0])
    
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
    
    # 処理日付範囲（過去1ヶ月以内）を設定
    one_month_ago = read_running_rec(running_rec)
    
    # key_Start_Date_Time列でフィルタリング
    if 'key_Start_Date_Time' in fields:
        start_date_col = int(fields['key_Start_Date_Time'][0])
        df = df[df[start_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]
        df[start_date_col] = df[start_date_col].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%dT%H.%M.%S'))
    else:
        Log.Log_Error(global_log_file, "key_Start_Date_Time not found in fields configuration")
    
    # key_AFM_Start_Date_Timeでフィルタリング（存在する場合）
    if 'key_AFM_Start_Date_Time' in fields:
        start_AFM_date_col = int(fields['key_AFM_Start_Date_Time'][0])
        df = df[df[start_AFM_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]
        df[start_AFM_date_col] = df[start_AFM_date_col].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%dT%H.%M.%S'))
    else:
        Log.Log_Error(global_log_file, "key_AFM_Start_Date_Time not found in fields configuration")
    
    # シリアル番号の列を取得
    Serial_Number = df[int(fields['key_Serial_Number'][0])]
    
    # SQL接続して各シリアル番号のデータを更新
    conn, cursor = SQL.connSQL()
    if conn is None:
        Log.Log_Error(global_log_file, "Connection with Prime Failed")
        return
    try:
        for serial in Serial_Number:
            part_number, nine_serial_number = SQL.selectSQL(cursor, serial)
            if part_number and nine_serial_number:
                df.loc[df[int(fields['key_Serial_Number'][0])] == serial, 'Part_Number'] = part_number
                df.loc[df[int(fields['key_Serial_Number'][0])] == serial, 'Nine_Serial_Number'] = nine_serial_number
            else:
                Log.Log_Error(global_log_file, f"Serial number {serial} not found in database")
    except Exception as e:
        Log.Log_Error(global_log_file, f"SQL query failed: {e}")
    finally:
        SQL.disconnSQL(conn, cursor)
    
    # 'Part_Number'がNaNの行を削除し、インデックスをリセット
    df = df.dropna(subset=['Part_Number']).reset_index(drop=True)
    row_end = len(df)
    row_number = 0
    
    # 各行ごとにデータ変換およびXML生成処理
    while row_number < row_end:
        data_dict = {}
        # 最終行の場合、最新のkey_Start_Date_Timeで実行記録を更新
        if row_number == row_end - 1:
            try:
                latest_date = df[start_date_col].max()
                update_running_rec(running_rec, latest_date)
            except KeyError as e:
                Log.Log_Error(global_log_file, f"KeyError processing start_date_col: {e}")
                return
        
        # 各フィールドごとにデータ変換
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
        
        # 列番号を更新
        sort_number_col = int(fields['key_SORTNUMBER'][0])
        data_dict['key_SORTNUMBER'] = df.loc[row_number, sort_number_col]
        data_dict['key_Operation'] = 'AFM_Step_Height'
        try:
            dt = datetime.strptime(str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'), "%Y-%m-%d %H:%M:%S")
            date_excel_number = int(str(dt - datetime(1899, 12, 30)).split()[0])
            data_dict["key_STARTTIME_SORTED"] = date_excel_number
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
        
        if None in data_dict.values():
            Log.Log_Error(global_log_file, f"Skipping row {row_number} due to None values in data_dict")
        else:
            generate_xml(data_dict, output_path, site, product_family, operation, Test_Station)
        row_number += 1
        Log.Log_Info(global_log_file, "Write the next starting line number")
        Row_Number_Func.next_start_row_number("Ru_AFM_StartROW.txt", row_number)

####################################
# .iniファイル処理関数（独立関数）
####################################
def process_ini_file(config_path: str) -> None:
    """
    指定された.iniファイルを処理し、設定を読み取りExcel・XML処理を実行する。
    すべての必要なパラメータは設定ファイルから取得する。
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
        data_columns = config.get('Excel', 'data_columns')
        log_path = config.get('Logging', 'log_path')
        fields_config = config.get('DataFields', 'fields').splitlines()
        site = config.get('Basic_info', 'Site')
        product_family = config.get('Basic_info', 'ProductFamily')
        operation = config.get('Basic_info', 'Operation')
        Test_Station = config.get('Basic_info', 'TestStation')
        file_name_pattern = config.get('Basic_info', 'file_name_pattern')
    except NoSectionError as e:
        Log.Log_Error(global_log_file, f"Missing section in config file {config_path}: {e}")
        return
    except NoOptionError as e:
        Log.Log_Error(global_log_file, f"Missing option in config file {config_path}: {e}")
        return

    # ログフォルダとファイルを作成する
    log_folder_name = str(datetime.today().date())
    log_folder_path = os.path.join(log_path, log_folder_name)
    os.makedirs(log_folder_path, exist_ok=True)
    log_file = os.path.join(log_folder_path, '043_LD-SPUT.log')
    global_log_file = log_file

    setup_logging(global_log_file)
    Log.Log_Info(log_file, f"Program Start for config {config_path}")

    # フィールド設定を解析し辞書に格納
    fields = {}
    for field in fields_config:
        if field.strip():
            try:
                key, col, dtype = field.split(':')
                fields[key.strip()] = (col.strip(), dtype.strip())
            except ValueError:
                Log.Log_Error(global_log_file, f"Field configuration parse error: {field}")
                continue

    # input_pathsに基づいてExcelファイルを処理する
    for input_path in input_paths:
        files = glob.glob(os.path.join(input_path, file_name_pattern))
        files = [file for file in files if not os.path.basename(file).startswith('~$')]
        if not files:
            Log.Log_Error(global_log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")
        for file in files:
            dest_dir = '../DataFile/044_/Ru_AFM/'
            os.makedirs(dest_dir, exist_ok=True)
            shutil.copy(file, dest_dir)
            Log.Log_Info(global_log_file, f"Copy excel file {file} to ../DataFile/044_/Ru_AFM/")
            copied_file_path = os.path.join(dest_dir, os.path.basename(file))
            process_excel_file(copied_file_path, sheet_name, data_columns, running_rec,
                               output_path, fields, site, product_family, operation, Test_Station)

####################################
# メイン処理
####################################
def main() -> None:
    """すべての.iniファイルをスキャンし、順次処理を実行する"""
    ini_files = glob.glob("*.ini")
    for ini_file in ini_files:
        process_ini_file(ini_file)

if __name__ == '__main__':
    main()

Log.Log_Info(global_log_file, "Program End")
