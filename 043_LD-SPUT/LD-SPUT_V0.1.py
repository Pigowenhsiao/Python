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
        print(f"ファイル {log_file_path} のログ設定時にエラーが発生しました: {e}")
        raise

def update_or_create_running_rec(running_rec_path: str, end_date: datetime) -> None:
    """
    実行記録ファイルが存在しない場合は作成し、最新の終了日時を記録する
    （更新と作成の処理を統合）
    """
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(global_log_file, f"実行記録ファイル {running_rec_path} を終了日時 {end_date} で更新しました")
    except Exception as e:
        Log.Log_Error(global_log_file, f"実行記録ファイル {running_rec_path} の更新時にエラーが発生しました: {e}")

def read_running_rec(running_rec_path: str) -> datetime:
    """
    最後の実行記録を読み取る。ファイルが存在しないまたは不正な場合は
    30日前の日付を返す。
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
        Log.Log_Error(global_log_file, f"実行記録ファイル {running_rec_path} の読み取り中にエラーが発生しました: {e}")
        return datetime.today() - timedelta(days=30)

####################################
# Excelファイル処理関数（独立関数）
####################################
def process_excel_file(file_path: str, sheet_name: str, data_columns: str,
                       running_rec: str, output_path: str, fields: dict,
                       site: str, prod_family: str, oper: str, test_station: str) -> None:
    """
    Excelファイルを処理し、データの読み取り、変換、SQLクエリ実行、XML生成を行う。
    必要なパラメータはすべて引数として渡す。
    """
    Log.Log_Info(global_log_file, f"Excelファイルの処理開始: {file_path}")
    
    # 指定パターンに一致するファイルを収集し、更新日時でソートして最新のファイルを選択
    excel_files = [
        [f, datetime.fromtimestamp(os.path.getmtime(f)).strftime("%Y-%m-%d %H:%M:%S")]
        for f in glob.glob(file_path) if '$' not in f
    ]
    if not excel_files:
        Log.Log_Error(global_log_file, f"Excelファイルが見つかりません: {file_path}")
        return
    latest_file = sorted(excel_files, key=lambda x: x[1], reverse=True)[0][0]
    
    dest_dir = '../DataFile/043_LD-SPUT/'
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, os.path.basename(latest_file))
    # コピー先と元が同じ場合はコピーせず、そのまま利用
    if os.path.abspath(latest_file) == os.path.abspath(dest_path):
        excel_file = latest_file
    else:
        excel_file = shutil.copy(latest_file, dest_dir)

    # Excelファイルからデータを読み込む
    try:
        df = pd.read_excel(excel_file, header=None, sheet_name=sheet_name,
                           usecols=data_columns, skiprows=100)
        df['key_SORTNUMBER'] = df.index + 100
    except Exception as e:
        Log.Log_Error(global_log_file, f"Excelファイル {file_path} の読み込み中にエラー: {e}")
        return

    df.columns = range(df.shape[1])
    df = df.dropna(subset=[0])  # 0列目がNaNの行を削除

    os.makedirs(output_path, exist_ok=True)
    one_month_ago = read_running_rec(running_rec)

    # 「key_Start_Date_Time」に基づいてデータをフィルタリングする
    if 'key_Start_Date_Time' in fields:
        start_col = int(fields['key_Start_Date_Time'][0])
        df = df[df[start_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]
        df[start_col] = df[start_col].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%dT%H.%M.%S'))
    else:
        Log.Log_Error(global_log_file, "設定ファイルに key_Start_Date_Time フィールドが見つかりません")

    # SQLクエリを使用してデータを更新する
    serial_numbers = df[3]
    conn, cursor = SQL.connSQL()
    if conn is None:
        Log.Log_Error(global_log_file, f"{serial_numbers} : Primeデータベース接続失敗")
        return
    try:
        for serial in serial_numbers:
            part_num, nine_serial = SQL.selectSQL(cursor, serial)
            df.loc[df[3] == serial, 'Part_Number'] = part_num
            df.loc[df[3] == serial, 'Nine_Serial_Number'] = nine_serial
    except Exception as e:
        Log.Log_Error(global_log_file, f"{serial_numbers} : SQLクエリ失敗: {e}")
    finally:
        SQL.disconnSQL(conn, cursor)
    
    df = df.dropna(subset=['Part_Number']).reset_index(drop=True)
    total_rows = len(df)
    row = 0

    while row < total_rows:
        data_dict = {}
        # 最終行の場合、実行記録ファイルを更新
        if row == total_rows - 1:
            latest_date = df[start_col].max()
            update_or_create_running_rec(running_rec, latest_date)
        
        # 各フィールド毎にデータ変換を実施
        for key, (col, dtype) in fields.items():
            try:
                value = df.iloc[row, int(col)]
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
                    Log.Log_Error(global_log_file, f"{key} の型 {dtype} は未対応")
                    continue
                data_dict[key] = value
            except Exception as e:
                Log.Log_Error(global_log_file, f"{key} 処理エラー: {e}")
                data_dict[key] = None

        data_dict['key_SORTNUMBER'] = df.loc[row, 16]
        try:
            dt_obj = datetime.strptime(
                str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'),
                "%Y-%m-%d %H:%M:%S"
            )
            data_dict["key_STARTTIME_SORTED"] = int(str(dt_obj - datetime(1899, 12, 30)).split()[0])
        except Exception as e:
            Log.Log_Error(global_log_file, f"日付変換エラー: {e}")
            data_dict["key_STARTTIME_SORTED"] = None

        if df.loc[row, 'Part_Number'] is not None:
            data_dict['key_Part_Number'] = df.loc[row, 'Part_Number']
            data_dict['key_LotNumber_9'] = df.loc[row, 'Nine_Serial_Number']
        else:
            Log.Log_Error(global_log_file, f"{data_dict.get('key_Serial_Number', 'Unknown')} : PartNumberエラー")
            row += 1
            continue

        if None in data_dict.values():
            Log.Log_Error(global_log_file, f"data_dictにNoneが含まれているため、行 {row} をスキップ")
        else:
            generate_xml(data_dict, output_path, site, prod_family, oper, test_station)
        
        row += 1
        Log.Log_Info(global_log_file, "次の開始行番号を更新")
        Row_Number_Func.next_start_row_number("LDSOUT_ROW.txt", row)

####################################
# XML生成関数（独立関数）
####################################
def generate_xml(data_dict: dict, output_dir: str, site: str, prod_family: str,
                 oper: str, test_station: str) -> None:
    """
    XMLテンプレートを用いてXMLファイルを生成する。
    必要なパラメータはすべて引数として渡す。
    """
    xml_filename = (
        f"Site={site},ProductFamily={prod_family},Operation={oper},"
        f"PartNumber={data_dict.get('key_Part_Number', 'Unknown')},"
        f"SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')},"
        f"Testdate={data_dict.get('key_Start_Date_Time','Unknown')}.xml"
    )
    xml_filepath = os.path.join(output_dir, xml_filename)
    xml_template = f'''<?xml version="1.0" encoding="utf-8"?>
<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
   <Result startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Result="Passed">
       <Header SerialNumber="{data_dict["key_Serial_Number"]}" PartNumber="{data_dict["key_Part_Number"]}" Operation="{oper}" TestStation="{test_station}" Operator="{data_dict["key_Operator"]}" StartTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Site="{site}" LotNumber="{data_dict["key_Serial_Number"]}"/>
       <HeaderMisc>
           <Item Description="Facet Coating"></Item>
       </HeaderMisc>
       <TestStep Name="THK_DEP" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">
           <Data DataType="String" Name="Banchi_ID1" Units="" Value="{data_dict["key_Banchi1"]}"/>
           <Data DataType="String" Name="Banchi_ID2" Units="" Value="{data_dict["key_Banchi2"]}"/>
           <Data DataType="String" Name="Banchi_ID3" Units="" Value="{data_dict["key_Banchi3"]}"/>
           <Data DataType="String" Name="Banchi_ID4" Units="" Value="{data_dict["key_Banchi4"]}"/>
           <Data DataType="String" Name="Banchi_ID5" Units="" Value="{data_dict["key_Banchi5"]}"/>
           <Data DataType="Numeric" Name="TR_THK" Units="um" Value="{data_dict["key_THK1"]}"/>
           <Data DataType="Numeric" Name="TL_THK" Units="um" Value="{data_dict["key_THK2"]}"/>
           <Data DataType="Numeric" Name="BL_THK" Units="um" Value="{data_dict["key_THK3"]}"/>
           <Data DataType="Numeric" Name="CE_THK" Units="um" Value="{data_dict["key_THK4"]}"/>
           <Data DataType="Numeric" Name="BR_THK" Units="um" Value="{data_dict["key_THK5"]}"/>
           <Data DataType="Numeric" Name="Banchi_THK_AVG" Units="" Value="{data_dict["key_THK_AVG"]}"/>
       </TestStep>
       <TestStep Name="SORTED_DATA" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">
           <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value="{data_dict["key_STARTTIME_SORTED"]}"/>
           <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value="{data_dict["key_SORTNUMBER"]}"/>
           <Data DataType="String" Name="LotNumber_5" Value="{data_dict["key_Serial_Number"]}" CompOperation="LOG"/>
           <Data DataType="String" Name="LotNumber_9" Value="{data_dict["key_LotNumber_9"]}" CompOperation="LOG"/>
       </TestStep>
   </Result>
</Results>
'''
    with open(xml_filepath, 'w', encoding='utf-8') as xf:
        xf.write(xml_template)
    Log.Log_Info(global_log_file, f"XMLファイルが生成されました: {xml_filepath}")

####################################
# .iniファイル処理関数
####################################
def process_ini_file(config_path: str) -> None:
    """
    指定された.iniファイルを処理し、設定の読み取りおよびExcel・XML処理を実行する。
    各設定パラメータはファイル内で定義され、各処理関数に渡される。
    """
    global global_log_file
    config = ConfigParser()

    # コメント行（#で始まる行）を除外して設定ファイルを読み込む
    try:
        with open(config_path, 'r', encoding='utf-8') as cf:
            config.read_file(line for line in cf if not line.strip().startswith('#'))
    except Exception as e:
        Log.Log_Error(global_log_file, f"設定ファイル {config_path} の読み取り中にエラーが発生しました: {e}")
        return

    # 設定ファイルから各パラメータを取得する
    try:
        input_paths    = [p.strip() for p in config.get('Paths', 'input_paths').split(',')]
        output_path    = config.get('Paths', 'output_path')
        running_rec    = config.get('Paths', 'running_rec')
        sheet_name     = config.get('Excel', 'sheet_name')
        data_columns   = config.get('Excel', 'data_columns')
        log_path       = config.get('Logging', 'log_path')
        fields_config  = config.get('DataFields', 'fields').splitlines()
        site           = config.get('Basic_info', 'Site')
        prod_family    = config.get('Basic_info', 'ProductFamily')
        oper           = config.get('Basic_info', 'Operation')
        test_station   = config.get('Basic_info', 'TestStation')
        file_pattern   = config.get('Basic_info', 'file_name_pattern')
    except (NoSectionError, NoOptionError) as e:
        Log.Log_Error(global_log_file, f"設定ファイル {config_path} に必要な設定が不足しています: {e}")
        return

    # ログファイルのパスを設定し、ログフォルダを作成する
    log_folder = os.path.join(log_path, str(date.today()))
    os.makedirs(log_folder, exist_ok=True)
    log_file = os.path.join(log_folder, '043_LD-SPUT.log')
    global_log_file = log_file
    setup_logging(global_log_file)
    Log.Log_Info(log_file, f"設定ファイル {config_path} の処理を開始します")

    # フィールド設定を解析して辞書に格納
    fields = {}
    for field in fields_config:
        if field.strip():
            try:
                key, col, dtype = field.split(':')
                fields[key.strip()] = (col.strip(), dtype.strip())
            except ValueError:
                Log.Log_Error(global_log_file, f"フィールド設定の解析エラー: {field}")
                continue

    # input_paths と file_pattern に基づいてExcelファイルを処理する
    for ipath in input_paths:
        files = glob.glob(os.path.join(ipath, file_pattern))
        files = [f for f in files if not os.path.basename(f).startswith('~$')]
        if not files:
            Log.Log_Error(global_log_file, f"{ipath} 内に {file_pattern} に一致するExcelファイルが見つかりません")
        for file in files:
            dest_dir = '../DataFile/001_GRATING/'
            os.makedirs(dest_dir, exist_ok=True)
            shutil.copy(file, dest_dir)
            Log.Log_Info(global_log_file, f"Excelファイル {file} を {dest_dir} にコピーしました")
            copied_path = os.path.join(dest_dir, os.path.basename(file))
            process_excel_file(copied_path, sheet_name, data_columns, running_rec,
                               output_path, fields, site, prod_family, oper, test_station)

####################################
# メイン処理
####################################
def main() -> None:
    """すべての.iniファイルをスキャンし、順次処理を実行する"""
    for ini_file in glob.glob("*.ini"):
        process_ini_file(ini_file)

if __name__ == '__main__':
    main()

Log.Log_Info(global_log_file, "プログラム終了")
