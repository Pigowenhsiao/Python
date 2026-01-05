#!/usr/bin/env python  # このプログラムのインタープリタを指定する
# -*- coding: utf-8 -*-  # このファイルのエンコーディングをUTF-8に設定する

"""
このプログラムの機能:
1. すべての.iniファイルを読み取り、設定に従ってExcelファイルのデータを処理し、XMLファイルを生成する。
2. 実行ログとエラーログはカスタムモジュールLogによって出力される。

依存モジュール:
- Log, SQL, Check, Convert_Date, Row_Number_Func (すべて../MyModule内)
"""  # 複数行コメント: プログラムの説明

import os  # OS関連の操作のためにosモジュールをインポートする
import sys  # Pythonインタープリタと対話するためにsysモジュールをインポートする
import glob  # ファイルパスのマッチングのためにglobモジュールをインポートする
import shutil  # ファイルのコピーと移動のためにshutilモジュールをインポートする
import logging  # ログ記録のためにloggingモジュールをインポートする
import pandas as pd  # データ処理のためにpandasモジュールをpdとしてインポートする
from configparser import ConfigParser, NoSectionError, NoOptionError  # configparserモジュールから設定解析関連のクラスをインポートする
from datetime import datetime, timedelta, date  # datetimeモジュールから日付と時刻関連のクラスをインポートする

sys.path.append('../MyModule')  # ../MyModuleをシステムモジュール検索パスに追加する
import Log  # ログ記録のためのカスタムLogモジュールをインポートする
import SQL  # データベース操作のためのカスタムSQLモジュールをインポートする
import Check  # カスタムCheckモジュールをインポートする
import Convert_Date  # カスタムConvert_Dateモジュールをインポートする
import Row_Number_Func  # 行番号を扱うためのカスタムRow_Number_Funcモジュールをインポートする

global_log_file = None  # グローバル変数global_log_fileを定義し、初期値をNoneに設定する

def setup_logging(log_file_path: str) -> None:  # setup_logging関数を定義し、ログのフォーマットとファイルを設定する
    """ログのフォーマットとファイルを設定する"""  # 関数の説明: ログ出力フォーマットを設定し、ファイルに書き込む
    try:  # 以下のコードを実行しようとする
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,  # ログファイルとレベルを設定する
                            format='%(asctime)s - %(levelname)s - %(message)s')  # ログ出力フォーマットを設定する
    except OSError as e:  # OSError例外をキャッチする
        print(f"Error setting up log file {log_file_path}: {e}")  # エラーメッセージを画面に出力する
        raise  # 例外を再度発生させる

def update_running_rec(running_rec_path: str, end_date: datetime) -> None:  # update_running_rec関数を定義し、実行記録ファイルを更新する
    """実行記録ファイルを更新する"""  # 関数の説明: 最新の終了日を実行記録ファイルに書き込む
    try:  # 以下のコードを実行しようとする
        with open(running_rec_path, 'w', encoding='utf-8') as f:  # 実行記録ファイルを書き込みモードで開く
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))  # 日付をフォーマットしてファイルに書き込む
        Log.Log_Info(global_log_file, f"Running record file {running_rec_path} updated with end date {end_date}")  # 更新成功メッセージをログに記録する
    except Exception as e:  # すべての例外をキャッチする
        Log.Log_Error(global_log_file, f"Error updating running record file {running_rec_path}: {e}")  # エラーメッセージをログに記録する

def ensure_running_rec_exists_and_update(running_rec_path: str, end_date: datetime) -> None:  # ensure_running_rec_exists_and_update関数を定義し、実行記録ファイルが存在することを確認し、更新する
    """実行記録ファイルが存在しない場合は作成して更新する"""  # 関数の説明: 実行記録ファイルが存在するか確認し、存在しない場合は作成して更新する
    try:  # 以下のコードを実行しようとする
        with open(running_rec_path, 'w', encoding='utf-8') as f:  # 実行記録ファイルを開く（または作成）
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))  # 終了日をファイルに書き込む
        Log.Log_Info(global_log_file, f"Running record file {running_rec_path} confirmed and updated with end date {end_date}")  # 更新成功メッセージをログに記録する
    except Exception as e:  # すべての例外をキャッチする
        Log.Log_Error(global_log_file, f"Error processing running record file {running_rec_path}: {e}")  # エラーメッセージをログに記録する

def read_running_rec(running_rec_path: str) -> datetime:  # read_running_rec関数を定義し、実行記録ファイルを読み取る
    """
    最後の実行記録を読み取る。
    ファイルが存在しない場合や内容が無効な場合は、30日前の日付を返す。
    """  # 関数の説明: 実行記録ファイルを読み取ろうとし、失敗した場合はデフォルトの日付を返す
    if not os.path.exists(running_rec_path):  # ファイルが存在しない場合
        with open(running_rec_path, 'w', encoding='utf-8') as f:  # 空のファイルを作成する
            f.write('')  # 空の文字列を書き込む
        return datetime.today() - timedelta(days= 30 )  # 30日前の日付を返す
    try:  # ファイル内容を読み取ろうとする
        with open(running_rec_path, 'r', encoding='utf-8') as f:  # ファイルを読み取りモードで開く
            content = f.read().strip()  # 内容を読み取り、前後の空白を削除する
            if content:  # 内容が空でない場合
                last_run_date = pd.to_datetime(content, errors='coerce')  # 日付形式に変換する
                if pd.isnull(last_run_date):  # 変換結果が無効な場合
                    return datetime.today() - timedelta(days=30)  # 30日前の日付を返す
                return last_run_date  # 変換された日付を返す
            else:  # 内容が空の場合
                return datetime.today() - timedelta(days=30)  # 30日前の日付を返す
    except Exception as e:  # すべての例外をキャッチする
        Log.Log_Error(global_log_file, f"Error reading running record file {running_rec_path}: {e}")  # エラーメッセージをログに記録する
        return datetime.today() - timedelta(days=30)  # 30日前の日付を返す

def process_excel_file(file_path: str, sheet_name: str, data_columns, running_rec: str,
                       output_path: str, fields: dict, site: str, product_family: str,
                       operation: str, Test_Station: str, config: ConfigParser) -> None:  # process_excel_file関数を定義し、Excelファイルを処理する
    """Excelファイルを処理し、データを読み取り、変換し、XMLファイルを生成する"""  # 関数の説明: 設定に従ってExcelデータを読み取り処理し、最終的にXMLファイルを生成する
    Log.Log_Info(global_log_file, f"Processing Excel File: {file_path}")  # Excelファイルの処理開始をログに記録する
    Excel_file_list = []  # ファイルとその変更時刻を格納する空のリストを初期化する
    for file in glob.glob(file_path):  # file_pathに一致するすべてのファイルを反復処理する
        if '$' not in file:  # 名前に'$'が含まれる一時ファイルを除外する
            dt = datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d %H:%M:%S")  # ファイルの変更時刻を取得し、フォーマットする
            Excel_file_list.append([file, dt])  # ファイルパスと変更時刻をリストに追加する
    if not Excel_file_list:  # リストが空の場合
        Log.Log_Error(global_log_file, f"Excel file not found: {file_path}")  # エラーをログに記録する
        return  # 関数の実行を終了する
    Excel_file_list = sorted(Excel_file_list, key=lambda x: x[1], reverse=True)  # 変更時刻でファイルをソートする（最新のものが最初）
    Excel_File = Excel_file_list[0][0]  # 最新のファイルパスと名前を取得する
    
    try:  # Excelデータを読み取ろうとする
        # Excelデータを読み取り、最初の100行をスキップし、指定された列のみを読み取る
        df = pd.read_excel(Excel_File, header=None, sheet_name=sheet_name, usecols=data_columns, skiprows=1000)
        df['key_SORTNUMBER'] = df.index + 1000  # 新しい列'key_SORTNUMBER'を追加し、値をインデックスに1000を加えたものにする

    except Exception as e:  # 読み取りに失敗した場合
        Log.Log_Error(global_log_file, f"Error reading Excel file {file_path}: {e}")  # エラーをログに記録する
        return  # 関数の実行を終了する
    df.columns = range(df.shape[1])  # DataFrameの列名を0, 1, 2,...に変更する
    df = df.dropna(subset=[2])  # 最初の列がNaNである行を削除する

    if not os.path.exists(output_path):  # 出力ディレクトリが存在しない場合
        os.makedirs(output_path)  # 出力ディレクトリを作成する
        
    one_month_ago = read_running_rec(running_rec)  # 実行記録に基づいて30日前の日付を取得する
    if 'key_Start_Date_Time' in fields:  # 設定にkey_Start_Date_Timeフィールドが含まれている場合
        start_date_col = int(fields['key_Start_Date_Time'][0])  # そのフィールドの列番号を取得する
        #print(start_date_col,df[start_date_col])  # そのフィールドの列番号を出力する
        running_date = config.get('Basic_info', 'Running_date')  # iniファイルからRunning_dateの値を取得する
        one_month_ago = datetime.today() - timedelta(days=int(running_date))  # 今日から進んだ日付を計算する
        df = df[df[start_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]  # 日付がRunning_date以上の行をフィルタリングする
        df[start_date_col] = df[start_date_col].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%dT%H.%M.%S'))  # その列の日付をフォーマットする
    else:  # 設定にそのフィールドが含まれていない場合
        Log.Log_Error(global_log_file, "key_Start_Date_Time not found in fields configuration")  # エラーをログに記録する
        # DataFrameからfields設定に基づいて値を抽出する

    if 'key_Start_Date_Time' in fields and 'key_END_Date_Time' in fields and 'key_Operator1' in fields and \
       'key_Operator2' in fields and 'key_Serial_Number' in fields and 'key_Material_Type' in fields and \
       'key_Coating_Type' in fields and 'key_Reflectivity' in fields:
        extracted_values = {
            "key_Start_Date_Time": df[int(fields['key_Start_Date_Time'][0])].tolist(),
            "key_END_Date_Time": df[int(fields['key_END_Date_Time'][0])].tolist(),
            "key_Operator1": df[int(fields['key_Operator1'][0])].tolist(),
            "key_Operator2": df[int(fields['key_Operator2'][0])].tolist(),
            "key_Serial_Number": df[int(fields['key_Serial_Number'][0])].tolist(),
            "key_Material_Type": df[int(fields['key_Material_Type'][0])].tolist(),
            "key_Coating_Type": df[int(fields['key_Coating_Type'][0])].tolist(),
            "key_Reflectivity": df[int(fields['key_Reflectivity'][0])].tolist(),
            "key_SORTNUMBER": df[int(fields['key_SORTNUMBER'][0])].tolist()
        }
        # extracted_valuesのフィールド名を対応する変数名に割り当てる
        # extracted_valuesの値が有効な列インデックスであることを確認する

        valid_columns = [int(fields[key][0]) for key in extracted_values.keys() if key in fields]
        df1 = df.iloc[:, valid_columns].copy()  # 有効な列インデックスに基づいてDataFrameをコピーする
        df1.columns = list(extracted_values.keys())  # 列名をextracted_valuesのキーに設定する
    else:
        Log.Log_Error(global_log_file, "Required fields are missing in the fields configuration")  # エラーをログに記録する
        return
    df1 = df1.reset_index(drop=True)

    # 'key_Serial_Number'列を'/'で分割し、新しい行を生成する
    new_rows = []
    for index, row in df1.iterrows():
        serial_numbers = str(row['key_Serial_Number']).split('/')  # '/'で分割する
        for serial in serial_numbers:
            serial = serial.strip()  # 前後の空白を削除する
            serial = serial.split()[0]  # 空白の前の部分のみを保持する
            if not serial:  # 空のシリアル番号をスキップする
                continue
            new_row = row.copy()  # 元の行をコピーする
            new_row['key_Serial_Number'] = serial  # 分割されたシリアル番号に置き換える
            new_rows.append(new_row)  # 新しい行をリストに追加する

    # 展開された行を持つ新しいDataFrameを作成する
    df1 = pd.DataFrame(new_rows).reset_index(drop=True)
    df1['Part_Number'] = None  # Part_NumberフィールドをNoneに初期化する
    df1['Chip_Part_Number'] = None  # Chip_Part_NumberフィールドをNoneに初期化する
    df1['COB_Part_Number'] = None  # COB_Part_NumberフィールドをNoneに初期化する
    for index, row in df1.iterrows():  # df1の各行を反復処理する
        key_Material_Type = str(row['key_Material_Type'])  # key_Material_Typeの値を取得する
        if "QJ-30150" in key_Material_Type:
            part_number = "XQJ-30150"
            chip_part_number = "1000047352"
            cob_part_number = "1000047353"
        elif "QJ-30115" in key_Material_Type:
            part_number = "XQJ-30115-P"
            chip_part_number = "1000034198"
            cob_part_number = "1000034812"
        else:
            part_number = None
            chip_part_number = None
            cob_part_number = None

        # フィールド値を更新する
        df1.loc[index, 'Part_Number'] = part_number
        df1.loc[index, 'Chip_Part_Number'] = chip_part_number
        df1.loc[index, 'COB_Part_Number'] = cob_part_number
        # 対応するフィールドを追加し、値をDataFrameに入れる

    row_end = len(df1)  # DataFrameの総行数を取得する
    row_number = 0  # 行番号を0に初期化する
    while row_number < row_end:  # 行番号が総行数未満の場合、各行をループ処理する
        data_dict = {}  # その行のデータを格納する空の辞書を初期化する
        if row_number == row_end - 1:  # 最後の行の場合
            latest_date = pd.to_datetime(df1['key_END_Date_Time']).max()  # 'key_Start_Date_Time'列から最大日付を取得する
            update_running_rec(running_rec, latest_date)  # 実行記録ファイルを更新する
        Count_A=0
        for key, (col, dtype) in fields.items():  # 設定されたすべてのフィールドを反復処理する
            try:  # フィールドデータを変換しようとする
                value = df1.iloc[row_number, int(Count_A)]  # 指定された行と列のデータを取得する
                if dtype == 'float':  # データ型がfloatの場合
                    value = float(value)  # floatに変換する
                elif dtype == 'str':  # データ型がstrの場合
                    value = str(value)  # 文字列に変換する
                elif dtype == 'int':  # データ型がintの場合
                    value = int(value)  # 整数に変換する
                elif dtype == 'bool':  # データ型がboolの場合
                    value = bool(value)  # ブール値に変換する
                elif dtype == 'datetime':  # データ型がdatetimeの場合
                    value = pd.to_datetime(value)  # datetimeに変換する
                else:  # データ型がサポートされていない場合
                    Log.Log_Error(global_log_file, f"Unsupported data type {dtype} for key {key}")  # エラーをログに記録する
                    continue  # このフィールドをスキップする
                data_dict[key] = value  # 辞書に変換された値を格納する
            except ValueError as ve:  # 値変換エラーをキャッチする
                Log.Log_Error(global_log_file, f"ValueError processing field {key}: {ve}")  # エラーをログに記録する
                data_dict[key] = None  # フィールドをNoneに設定する
            except Exception as e:  # その他の例外をキャッチする
                Log.Log_Error(global_log_file, f"Error processing field {key}: {e}")  # エラーをログに記録する
                data_dict[key] = None  # フィールドをNoneに設定する
                continue  # 次のフィールドの処理を続行する
            Count_A = Count_A + 1 
        # "key_Serial_Number"が"150"または"115"であるかどうかを確認する
        if not (str(data_dict.get("key_Serial_Number", "")).startswith("150") or 
            str(data_dict.get("key_Serial_Number", "")).startswith("115")):
            Log.Log_Info(global_log_file, f"Skipping row {row_number} due to key_Serial_Number not matching '150' or '115'")
            row_number += 1
            continue

    
        try:  # 日付変換を試みる
            dt = datetime.strptime(str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'), "%Y-%m-%d %H:%M:%S")  # 日付文字列をdatetimeオブジェクトに変換する
            date_excel_number = int(str(dt - datetime(1899, 12, 30)).split()[0])  # Excelの日付値を計算する
        except Exception as e:  # 変換に失敗した場合
            Log.Log_Error(global_log_file, f"Date conversion error: {e}")  # エラーをログに記録する
            date_excel_number = None  # 日付値をNoneに設定する
        data_dict["key_STARTTIME_SORTED"] = date_excel_number  # 辞書に変換された日付値を格納する
        data_dict['key_Part_Number'] = df1.loc[row_number, 'Part_Number']  # Part_Numberを格納する
        data_dict['Part_Number'] = df1.loc[row_number, 'Part_Number']
        data_dict['Chip_Part_Number'] = df1.loc[row_number, 'Chip_Part_Number']
        data_dict['COB_Part_Number'] = df1.loc[row_number, 'COB_Part_Number']
        data_dict['CVD_Tool'] = config.get('Basic_info', 'CVD_Tool')
                
        if None in data_dict.values():  # 辞書にNone値が含まれている場合
            Log.Log_Error(global_log_file, f"Skipping row {row_number} due to None values in data_dict")  # エラーをログに記録する
        else:  # すべてのデータが有効な場合
            generate_xml(data_dict, output_path, site, product_family, operation, Test_Station)  # generate_xmlを呼び出してXMLファイルを生成する
        row_number += 1  # 行番号を1増やす
        Log.Log_Info(global_log_file, "Write the next starting line number")  # 次の開始行番号のメッセージをログに記録する
        running_rec_file = config.get('Paths', 'running_rec')  # 設定から実行記録ファイルパスを取得する
        with open(running_rec_file, 'w', encoding='utf-8') as f:  # 実行記録ファイルを書き込みモードで開く
            f.write(str(row_number))  # 現在の行番号をファイルに書き込む

def generate_xml(data_dict: dict, output_path: str, site: str, product_family: str,
                 operation: str, Test_Station: str) -> None:  # generate_xml関数を定義し、XMLファイルを生成する
    """XMLファイルを生成する"""  # 関数の説明: 渡されたデータに基づいてXMLファイルを生成する
    print(data_dict.get('key_Start_Date_Time', ''))  # key_Start_Date_Timeを画面に出力する（英語出力を使用）
    xml_filename = (  # XMLファイル名を構築する
        f"Site={site},ProductFamily={product_family},Operation={operation},"
        f"PartNumber={data_dict.get('key_Part_Number', 'Unknown')},"
        f"SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')},"
        f"Testdate={data_dict.get('key_Start_Date_Time', 'Unknown')}.xml"
    )
    xml_filepath = os.path.join(output_path, xml_filename)  # XMLファイルへのフルパスを構築する
    with open(xml_filepath, 'w', encoding='utf-8') as f:  # XMLファイルを書き込みモードで開く
        f.write('<?xml version="1.0" encoding="utf-8"?>\n')  # XML宣言を書き込む
        f.write('<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n')  # ルート要素の開始タグを書き込む
        f.write(f'    <Result startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Result="Passed">\n')  # Result要素と属性を書き込む
        f.write(f'        <Header SerialNumber="{data_dict["key_Serial_Number"]}" PartNumber="{data_dict["key_Part_Number"]}" Operation="{operation}" TestStation="{Test_Station}" Operator="{data_dict["key_Operator1"]}" StartTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Site="{site}" LotNumber="{data_dict["key_Serial_Number"]}"/>\n')  # Header要素を含む、さまざまな属性を書き込む
        f.write('        <HeaderMisc>\n')  # HeaderMisc要素の開始タグを書き込む
        f.write(f'            <Item Description="{operation}"></Item>\n')  # Item要素を書き込み、操作名を記述する
        f.write('        </HeaderMisc>\n')  # HeaderMisc要素の終了タグを書き込む
        f.write(f'        <TestStep Name="{operation}" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')  # 最初のTestStep要素とその属性を書き込む
        f.write(f'            <Data DataType="Numeric" Name="Reflectivity" Value="{data_dict["key_Reflectivity"]}" CompOperation="LOG"/>\n')  # Data要素を書き込み、ロット番号（LotNumber_5）を表示する
        f.write(f'            <Data DataType="String" Name="Coating_Type" Value="{data_dict["key_Coating_Type"]}" CompOperation="LOG"/>\n')  # Data要素を書き込み、ロット番号（LotNumber_5）を表示する
        f.write('        </TestStep>\n')  # 最初のTestStep要素の終了タグを書き込む
        f.write(f'        <TestStep Name="SORTED_DATA" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')  # 2番目のTestStep要素とその属性を書き込む
        f.write(f'            <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value="{data_dict["key_STARTTIME_SORTED"]}"/>\n')  # Data要素を書き込み、ソートされた時間を表示する
        f.write(f'            <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value="{data_dict["key_SORTNUMBER"]}"/>\n')  # Data要素を書き込み、ソートされた番号を表示する
        f.write(f'            <Data DataType="String" Name="Chip_Part_Number" Value="{data_dict["Chip_Part_Number"]}" CompOperation="LOG"/>\n')  # Data要素を書き込み、ロット番号（LotNumber_5）を表示する
        f.write(f'            <Data DataType="String" Name="COB_Part_Number" Value="{data_dict["Chip_Part_Number"]}" CompOperation="LOG"/>\n')  # Data要素を書き込み、ロット番号（LotNumber_9）を表示する
        f.write('        </TestStep>\n')  # 2番目のTestStep要素の終了タグを書き込む
        f.write('        <TestEquipment>\n')  # TestEquipment要素の開始タグを書き込む
        f.write(f'            <Item DeviceName="CVD" DeviceSerialNumber="{data_dict["CVD_Tool"]}"></Item>\n')  # Item要素を書き込み、デバイス情報を表示する
        f.write('        </TestEquipment>\n')  # TestEquipment要素の終了タグを書き込む
        f.write('    </Result>\n')  # Result要素の終了タグを書き込む
        f.write('</Results>\n')  # ルート要素の終了タグを書き込む
    Log.Log_Info(global_log_file, f"XML File Created: {xml_filepath}")  # XMLファイル作成成功メッセージをログに記録する

def process_ini_file(config_path: str) -> None:  # process_ini_file関数を定義し、.ini設定ファイルを処理する
    """指定された.iniファイルを読み取り、ExcelおよびXML処理を実行する"""  # 関数の説明: 設定ファイルに基づいて関連する処理を実行する
    global global_log_file  # グローバル変数global_log_fileを使用する
    config = ConfigParser()  # 設定ファイルを解析するためのConfigParserオブジェクトを作成する
    try:  # 設定ファイルを読み取ろうとする
        with open(config_path, 'r', encoding='utf-8') as config_file:  # .iniファイルを読み取りモードで開く
            config.read_file(line for line in config_file if not line.strip().startswith('#'))  # コメント行をスキップしてファイルを読み取る
    except Exception as e:  # すべての例外をキャッチする
        Log.Log_Error(global_log_file, f"Error reading config file {config_path}: {e}")  # 設定ファイル読み取り時のエラーをログに記録する
        return  # 関数の実行を終了する

    try:  # 設定ファイルから各設定を取得しようとする
        input_paths = [path.strip() for path in config.get('Paths', 'input_paths').splitlines() if path.strip() and not path.strip().startswith('#')]  # 入力パスのリストを取得し、空行とコメント行を除外する
        output_path = config.get('Paths', 'output_path')  # 出力パスを取得する
        running_rec = config.get('Paths', 'running_rec')  # 実行記録ファイルパスを取得する
        sheet_name = config.get('Excel', 'sheet_name')  # Excelシート名を取得する
        data_columns = config.get('Excel', 'data_columns')  # 読み取るデータ列を取得する
        log_path = config.get('Logging', 'log_path')  # ログ保存パスを取得する
        fields_config = [field.strip() for field in config.get('DataFields', 'fields').splitlines() if field.strip()]  # データフィールド設定を取得し、空行を除外する
        site = config.get('Basic_info', 'Site')  # サイト情報を取得する
        product_family = config.get('Basic_info', 'ProductFamily')  # 製品ファミリー情報を取得する
        operation = config.get('Basic_info', 'Operation')  # 操作名を取得する
        Test_Station = config.get('Basic_info', 'TestStation')  # テストステーション情報を取得する
        file_name_pattern = config.get('Basic_info', 'file_name_pattern')  # ファイル名の一致パターンを取得する

    except NoSectionError as e:  # 設定にセクションが欠けている場合
        Log.Log_Error(global_log_file, f"Missing section in config file {config_path}: {e}")  # エラーをログに記録する
        return  # 関数の実行を終了する
    except NoOptionError as e:  # 設定にオプションが欠けている場合
        Log.Log_Error(global_log_file, f"Missing option in config file {config_path}: {e}")  # エラーをログに記録する
        return  # 関数の実行を終了する

    log_folder_name = str(datetime.today().date())  # 今日の日付をログフォルダ名として使用する
    log_folder_path = os.path.join(log_path, log_folder_name)  # ログフォルダパスを構築する
    if not os.path.exists(log_folder_path):  # フォルダが存在しない場合
        os.makedirs(log_folder_path)  # ログフォルダを作成する
    log_file = os.path.join(log_folder_path, '043_LD-SPUT.log')  # ログファイルへのフルパスを構築する
    global_log_file = log_file  # グローバル変数global_log_fileを更新する
    setup_logging(global_log_file)  # setup_loggingを呼び出してログを設定する
    Log.Log_Info(log_file, f"Program Start for config {config_path}")  # プログラム開始メッセージをログに記録する

    fields = {}  # フィールド設定辞書を初期化する
    for field in fields_config:  # フィールド設定の各行を反復処理する
        if field.strip():  # 行が空でない場合
            key, col, dtype = field.split(':')  # 行を分割してキー、列番号、データ型を取得する
            fields[key.strip()] = (col.strip(), dtype.strip())  # 設定を辞書に格納する

    for input_path in input_paths:  # すべての入力パスを反復処理する
        print(input_path)  # 処理中の現在の入力パスを出力する
        files = glob.glob(os.path.join(input_path, file_name_pattern))  # 一致パターンに基づいてファイルのリストを取得する
        files = [file for file in files if not os.path.basename(file).startswith('~$')]  # 一時ファイルをフィルタリングする
        if not files:  # ファイルが見つからない場合
            Log.Log_Error(global_log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")  # エラーをログに記録する
        for file in files:  # 一致する各ファイルを反復処理する
            if not os.path.basename(file).startswith('~$'):  # ファイル名が'~$'で始まらない場合
                destination_dir = '../DataFile/047/TAK_SPC/'  # 宛先ディレクトリを設定する
                if not os.path.exists(destination_dir):  # 宛先ディレクトリが存在しない場合
                    os.makedirs(destination_dir)  # 宛先ディレクトリを作成する
                shutil.copy(file, destination_dir)  # ファイルを宛先ディレクトリにコピーする
                Log.Log_Info(global_log_file, f"Copy excel file {file} to ../DataFile/047_TAK_SPC/")  # ファイルコピーのメッセージを記録する
                copied_file_path = os.path.join(destination_dir, os.path.basename(file))  # コピーされたファイルのフルパスを構築する
                process_excel_file(copied_file_path, sheet_name, data_columns, running_rec,
                                   output_path, fields, site, product_family, operation, Test_Station, config)  # Excelファイルを処理する

def main() -> None:  # main関数を定義する
    """すべての.iniファイルをスキャンして処理を実行する"""  # 関数の説明: 現在のディレクトリ内のすべての.iniファイルを反復処理し、設定に従って処理する
    ini_files = glob.glob("*.ini")  # 現在のディレクトリ内のすべての.iniファイルのリストを取得する
    for ini_file in ini_files:  # 各.iniファイルを反復処理する
        process_ini_file(ini_file)  # .iniファイルを処理する

if __name__ == '__main__':  # このモジュールがメインプログラムとして実行された場合
    main()  # main関数を呼び出す
    Log.Log_Info(global_log_file, "Program End")  # プログラム終了のメッセージを記録する
