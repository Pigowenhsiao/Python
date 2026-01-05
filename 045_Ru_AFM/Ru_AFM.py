# Pythonプログラム - すべての.iniファイルを読み取り、データ処理を実行してXMLファイルを生成
import os
import sys
import glob
import shutil
import logging
import pandas as pd
from configparser import ConfigParser, NoSectionError, NoOptionError
from datetime import datetime, timedelta, date

# カスタムモジュール
sys.path.append('../MyModule')
import Log
import SQL
import Check
import Convert_Date
import Row_Number_Func

# ログファイルのグローバル変数
global_log_file = None

########## Logの設定 ##########
Log_Folder_Name = str(date.today())
if not os.path.exists("../Log/" + Log_Folder_Name):
    os.makedirs("../Log/" + Log_Folder_Name)
Log_File = '../Log/' + Log_Folder_Name + '/045_Ru_AFM.log'
Log.Log_Info(Log_File, 'Program Start')

# ログ設定の構成
def setup_logging(log_file_path):
    try:
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
    except OSError as e:
        print(f"ファイル {log_file_path} でのログ設定エラー: {e}")
        raise

# 実行記録ファイルを更新
def update_running_rec(running_rec_path, end_date):
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(global_log_file, f"実行記録ファイル {running_rec_path} を終了日 {end_date} で更新しました")
    except Exception as e:
        Log.Log_Error(global_log_file, f"実行記録ファイル {running_rec_path} の更新エラー: {e}")

# 実行記録ファイルが存在することを確認し、必要に応じて更新
def ensure_running_rec_exists_and_update(running_rec_path, end_date):
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(global_log_file, f"実行記録ファイル {running_rec_path} を終了日 {end_date} で確認および更新しました")
    except Exception as e:
        Log.Log_Error(global_log_file, f"実行記録ファイル {running_rec_path} の処理エラー: {e}")

# 最後の実行記録を読み取る
def read_running_rec(running_rec_path):
    if not os.path.exists(running_rec_path):
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write('')
        return datetime.today() - timedelta(days=10)
    
    try:
        with open(running_rec_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                last_run_date = pd.to_datetime(content, errors='coerce')
                if pd.isnull(last_run_date):
                    return datetime.today() - timedelta(days=10)
                return last_run_date
            else:
                return datetime.today() - timedelta(days=10)
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading running_rec file {running_rec_path}: {e}")
        return datetime.today() - timedelta(days=10)

# 指定された.iniファイルを処理する関数
def process_ini_file(config_path):
    global global_log_file
    config = ConfigParser()
    try:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config.read_file(line for line in config_file if not line.strip().startswith('#'))
    except Exception as e:
        Log.Log_Error(global_log_file, f"Error reading config file {config_path}: {e}")
        return

    # 設定ファイルから設定を取得
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

    # ログフォルダとファイルを作成
    log_folder_name = str(datetime.today().date())
    log_folder_path = os.path.join(log_path, log_folder_name)
    if not os.path.exists(log_folder_path):
        os.makedirs(log_folder_path)
    log_file = os.path.join(log_folder_path, '043_LD-SPUT.log')
    global_log_file = log_file

    # ログ設定を行う
    setup_logging(global_log_file)
    Log.Log_Info(log_file, f'Program Start for config {config_path}')

    # フィールド設定を辞書に解析
    fields = {}
    for field in fields_config:
        if field.strip():
            key, col, dtype = field.split(':')
            fields[key.strip()] = (col.strip(), dtype.strip())

    def process_excel_file(file_path):
        Log.Log_Info(global_log_file, f'Processing Excel File: {file_path}')
        Excel_file_list = []
        for file in glob.glob(file_path):
            if '$' not in file:
                dt = datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d %H:%M:%S")
                Excel_file_list.append([file, dt])
                
        Excel_file_list = sorted(Excel_file_list, key=lambda x: x[1], reverse=True)
        Excel_File = shutil.copy(Excel_file_list[0][0], '../DataFile/045_Ru_AFM/')
        
        try:
            # Excelデータを読み取る
            df = pd.read_excel(Excel_File, header=None, sheet_name=sheet_name, usecols=data_columns, skiprows=100)
            df['key_SORTNUMBER'] = df.index + 100

        except Exception as e:
            Log.Log_Error(global_log_file, f'Error reading Excel file {file_path}: {e}')
            return
        
        # 列番号を設定
        df.columns = range(df.shape[1])
        df = df.dropna(subset=[0])  # df[0]がNaNの行を削除

        # 出力XMLディレクトリが存在しない場合は作成
        if not os.path.exists(output_path):
            os.makedirs(output_path)
            
        # 処理日付範囲を設定（一ヶ月以内）
        one_month_ago = read_running_rec(running_rec)

        # key_Start_Date_Timeが一ヶ月前または最後の実行記録日より古い行をフィルタリング
        if 'key_Start_Date_Time' in fields:
            start_date_col = int(fields['key_Start_Date_Time'][0])
            df = df[df[start_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]
            df[start_date_col] = df[start_date_col].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%dT%H.%M.%S'))
        else:
            Log.Log_Error(global_log_file, 'key_Start_Date_Time not found in fields configuration')
        if 'key_AFM_Start_Date_Time' in fields:
            start_AFM_date_col = int(fields['key_AFM_Start_Date_Time'][0])
            df = df[df[start_AFM_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]
            df[start_AFM_date_col] = df[start_AFM_date_col].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%dT%H.%M.%S'))
        else:
            Log.Log_Error(global_log_file, 'key_Start_Date_Time not found in fields configuration') 

        Serial_Number=df[int(fields['key_Serial_Number'][0])]           
        #for serial in Serial_Number:                                                                                 #----REmove
        #    df.loc[df[int(fields['key_Serial_Number'][0])] == serial, 'Part_Number'] = 'HL13B5-BT20'                 #----REmove
        #    df.loc[df[int(fields['key_Serial_Number'][0])] == serial, 'Nine_Serial_Number'] = '24LFD1AUL'            #----REmove

        conn, cursor = SQL.connSQL()
        if conn is None:
            Log.Log_Error(global_log_file, 'Connection with Prime Failed')
            return
        try:
            for serial in Serial_Number:
                part_number, nine_serial_number = SQL.selectSQL(cursor, serial)
                if part_number and nine_serial_number:
                    df.loc[df[int(fields['key_Serial_Number'][0])] == serial, 'Part_Number'] = part_number
                    df.loc[df[int(fields['key_Serial_Number'][0])] == serial, 'Nine_Serial_Number'] = nine_serial_number
            else:
                Log.Log_Error(global_log_file, f'Serial number {serial} not found in database')
        except Exception as e:
            Log.Log_Error(global_log_file, f'SQL query failed: {e}')
        finally:
            SQL.disconnSQL(conn, cursor)

        # Drop rows where 'Part_Number' is NaN
        df = df.dropna(subset=['Part_Number'])
        # 列數重新整理，將空列的列數都排除歸零
        df = df.reset_index(drop=True)
        row_end = len(df)
        row_number = 0        

        # データ処理
        while row_number < row_end:
            data_dict = {}
            # データ変換処理
            # 最新のkey_Start_Date_Timeで実行記録を更新
            if row_number == row_end - 1:
                latest_date = df[start_date_col].max()
                update_running_rec(running_rec, latest_date)
                
            for key, (col, dtype) in fields.items():
                try:
                    # dfデータを処理
                    value = df.iloc[row_number, int(col)]
                    # 指定されたデータ型に値を変換
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
            sort_number_col = int(fields['key_SORTNUMBER'][0])
            data_dict['key_SORTNUMBER'] = df.loc[row_number, sort_number_col] # 將列數寫進去
            data_dict['key_Operation'] = 'AFM_Step_Height'
            date = datetime.strptime(str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'), "%Y-%m-%d %H:%M:%S")
            date_excel_number = int(str(date - datetime(1899, 12, 30)).split()[0])
            
            data_dict["key_STARTTIME_SORTED"] = date_excel_number
            if df.loc[row_number, 'Part_Number'] is not None:
                data_dict['key_Part_Number'] = df.loc[row_number, 'Part_Number']
                data_dict['key_LotNumber_9'] = df.loc[row_number, 'Nine_Serial_Number']
            else:
                Log.Log_Error(global_log_file, data_dict.get('key_Serial_Number', 'Unknown') + ' : ' + 'PartNumber Error')
                row_number += 1
                continue
            # XMLファイルを生成
            if None in data_dict.values():
                Log.Log_Error(global_log_file, f"Skipping row {row_number} due to None values in data_dict")
            else:
                generate_xml(data_dict)
            row_number += 1
            Log.Log_Info(Log_File, 'Write the next starting line number')
            Row_Number_Func.next_start_row_number("Ru_AFM_StartROW.txt", row_number)

    def generate_xml(data_dict):   
        print(data_dict.get('key_Start_Date_Time', ''))
        xml_filename = f"Site={site},ProductFamily={product_family},Operation={operation},PartNumber={data_dict.get('key_Part_Number', 'Unknown')},SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')},Testdate ={data_dict.get('key_Start_Date_Time','Unkonow')}.xml"
        xml_filepath = os.path.join(output_path, xml_filename)
        with open(xml_filepath, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n')
            f.write(f'    <Result startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Result="Passed">\n')
            f.write(f'        <Header SerialNumber="{data_dict["key_Serial_Number"]}" PartNumber="{data_dict["key_Part_Number"]}" Operation="{operation}" TestStation="{Test_Station}" Operator="{data_dict["key_Operator"]}" StartTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Site="{site}" LotNumber="{data_dict["key_Serial_Number"]}"/>\n')
            f.write('        <HeaderMisc>\n')
            f.write('            <Item Description="AFM_Step_Height"></Item>\n')
            f.write('        </HeaderMisc>\n')
            f.write(f'        <TestStep Name="{data_dict["key_Operation"]}" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')
            
            for key in ["key_Ah_L1", "key_Ah_L2", "key_Ah_R1", "key_Ah_R2", "key_Da_L1", "key_Da_L2", "key_Da_R1", "key_Da_R2", "key_Dh_L1", "key_Dh_L2", "key_Dh_R1", "key_Dh_R2"]:
                f.write(f'            <Data DataType="Numeric" Name="{key.split("_")[1]}_{key.split("_")[2]}" Units="um" Value="{data_dict[key]}"/>\n')
            f.write('        </TestStep>\n')
            f.write(f'        <TestStep Name="SORTED_DATA" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')
            f.write(f'            <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value="{data_dict["key_STARTTIME_SORTED"]}"/>\n')
            f.write(f'            <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value="{data_dict["key_SORTNUMBER"]}"/>\n')
            f.write(f'            <Data DataType="String" Name="LotNumber_5" Value="{data_dict["key_Serial_Number"]}" CompOperation="LOG"/>\n')
            f.write(f'            <Data DataType="String" Name="LotNumber_9" Value="{data_dict["key_LotNumber_9"]}" CompOperation="LOG"/>\n')
            f.write('        </TestStep>\n')
            f.write('        <TestEquipment>\n')
            f.write(f'            <Item DeviceName="MOCVD" DeviceSerialNumber="{data_dict["key_Equipment"]}"></Item> \n ')
            f.write('        </TestEquipment>\n') 
            f.write('    </Result>\n')
            f.write('</Results>\n')
        Log.Log_Info(global_log_file, f'XML File Created: {xml_filepath}')

    # 入力パスに基づいてExcelファイルを処理
    for input_path in input_paths:
        files = glob.glob(os.path.join(input_path, file_name_pattern))
        files = [file for file in files if not os.path.basename(file).startswith('~$')]
        if not files:
            Log.Log_Error(global_log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")
        for file in files:
            if not os.path.basename(file).startswith('~$'):
                destination_dir = '../DataFile/044_/Ru_AFM//'
                if not os.path.exists(destination_dir):
                    os.makedirs(destination_dir)
                shutil.copy(file, destination_dir)
                Log.Log_Info(global_log_file, f"Copy excel file {file} to ../DataFile/044_/Ru_AFM/")
                copied_file_path = os.path.join(destination_dir, os.path.basename(file))
                process_excel_file(copied_file_path)

# すべての.iniファイルをスキャンして処理するメイン関数
def main():
    ini_files = glob.glob("*.ini")
    for ini_file in ini_files:
        process_ini_file(ini_file)

if __name__ == '__main__':
    main()

Log.Log_Info(global_log_file, 'Program End')
