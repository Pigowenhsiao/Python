import os
import sys
import xlrd
import glob
import pyodbc
import shutil
import logging
import numpy as np
import pandas as pd
import openpyxl as px

from time import strftime, localtime
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta


########## 自作関数の定義 ##########
sys.path.append('../MyModule')
import Log
import SQL
import Check
import Convert_Date
import Row_Number_Func


########## 全体パラメータ定義 ##########
Site = '350'
ProductFamily = 'SAG FAB'
Operation = 'Coating_MG_Reflectance'
TestStation = 'Coating'


########## Logの設定 ##########

# ----- ログファイルの作成 -----
Log_Folder_Name = str(date.today())
if not os.path.exists("../Log/" + Log_Folder_Name):
    os.makedirs("../Log/" + Log_Folder_Name)

# 実行中のスクリプトの絶対パス
script_path = os.path.abspath(__file__)
# 親ディレクトリの名前
parent_dir_name = os.path.basename(os.path.dirname(script_path))

Log_File = '../Log/' + Log_Folder_Name + '/' + parent_dir_name + '.log'
Log.Log_Info(Log_File, 'Program Start')

########## XML出力先ファイルパス ##########
Output_filepath = '//li.lumentuminc.net/data/SAG/TDS/Data/Files to Insert/XML/'
#Output_filepath = '../XML/'

########## 取得するデータの列番号を定義 ##########
Col_Start_Date_Time = 0
Col_Operator = 2
Col_Serial_Number = 7
Col_Reflectance_Front = 9
Col_Reflectance_Back = 10

########## 取得した項目と型の対応表を定義 ##########
key_type = {
    'key_Start_Date_Time': str,
    'key_Operator': str,
    'key_Serial_Number': str,
    'key_Serial_Number_Addr': str,
    'key_Reflectance_Front': float,
    'key_Reflectance_Back': float,
    'key_Dev' : str,
    'key_STARTTIME_SORTED' : float,
    'key_SORTNUMBER' : float,
    'key_LotNumber_9': str
}

########## 対象ロット番号のイニシャルを記載したファイルを取得する ##########
Log.Log_Info(Log_File, 'Get SerialNumber Initial List ')
with open('./個別対応SerialNumber_Initial.txt', 'r', encoding='utf-8') as textfile:     #個別対応なのでイニシャルファイルは共有しない
    SerialNumber_list = {s.strip() for s in textfile.readlines()}

def Main():


    ########## Excelファイルをローカルにコピー ##########

    # ----- 正規表現で取出し、直近で変更があったファイルを取得する -----
    Log.Log_Info(Log_File, 'Excel File Copy')

    ########## ファイル名の定義 ##########
    FilePath1 = 'Z:/スパッタ/MG/MG#2/'
    FileName1 = 'MG#2_着工記録*.xlsx'
    FilePath2 = 'Z:/スパッタ/MG/MG#3/'
    FileName2 = 'MG#3_着工記録*.xlsx'
    ########## シート名の定義 ##########
#    Data_Sheet_Name = 1  # 二番目のシート

    Excel_File_List = []

    for file in glob.glob(FilePath1 + FileName1):
        if '$' not in file:
            dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getmtime(file)))
            Excel_File_List.append([file, dt])

    for file in glob.glob(FilePath2 + FileName2):
        if '$' not in file:
            dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getmtime(file)))
            Excel_File_List.append([file, dt])

    # ----- dt(更新日時)の降順で並び替える -----
#    Excel_File_List = sorted(Excel_File_List, key=lambda x: x[1], reverse=True)

    for wk_Excel_File in Excel_File_List:
        if not os.path.exists("../DataFile/" + parent_dir_name):
            os.makedirs("../DataFile/" + parent_dir_name)
        Excel_File = shutil.copy(wk_Excel_File[0], '../DataFile/' + parent_dir_name + '/')

        if '#2' in wk_Excel_File[0]:
            wk_dev='#2'
            wk_loop = range(1,3)    #2シート目と3シート目からデータを読む
        elif '#3' in wk_Excel_File[0]:
            wk_dev='#3'
            wk_loop = range(0,2)    #1シート目と2シート目からデータを読む
        else:
            wk_dev=''

        for Data_Sheet_Name in wk_loop:

            ########## DaraFrameの作成 ##########

            # ----- 取得開始行の取り出し -----
        #    Log.Log_Info(Log_File, 'Get The Starting Row Count')
        #    Start_Number = Row_Number_Func.start_row_number("Grating_Ething_Depth_StartRow.txt") -500
            Start_Number = 11

            # ----- ExcelデータをDataFrameとして取得 -----
            Log.Log_Info(Log_File, 'Read Excel')
            df = pd.read_excel(Excel_File, header=None, sheet_name=Data_Sheet_Name, usecols="B:L", skiprows=Start_Number)

        #    df = df.sort_values(1,axis=0) #ソート難しいので保留
            # ----- 使わない列を落とす -----
        #    df = df.drop(range(5,13), axis=1)
            # ----- 列番号の振り直し -----
        #    Log.Log_Info(Log_File, 'Setting Columns Number')
        #    df.columns = range(df.shape[1])

        #    df = df.drop(1, axis=1)
            # ----- 列番号の振り直し -----
        #    df.columns = range(df.shape[1])

            df = df.drop(len(df) - 1)
            # ----- 末尾から欠損のデータを落としていく -----
            Getting_Row = len(df) - 1

            while Getting_Row >= 0 and df.isnull().any(axis=1)[Getting_Row]:
                Getting_Row -= 1


            df = df[:Getting_Row + 1]

            # ----- 次の開始行数をメモ -----
            Next_Start_Row = Start_Number + df.shape[0] + 1

            # ----- 日付欄に文字列が入っていたらNoneに置き換える -----
    #        for i in range(df.shape[0]):
    #            if not isinstance(df.iloc[i, 2], (pd.Timestamp, datetime)):
    #                df.iloc[i, 2] = np.nan

            # ----- 今日から1か月前のデータまでを取得する -----
#            df[1] = pd.to_datetime(df[1])
#            one_month_ago = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=2)
#            df = df[(df[1] >= one_month_ago)]

            ########## ループ前の設定 ##########

            # ----- 最終行数の取得 -----
            Log.Log_Info(Log_File, 'Get DataFrame End Index Number\n')
            row_end = len(df)

            # ----- 現在処理を行っている行数の定義 -----
            Row_Number = 0

            # ----- dfのindexリスト -----
            df_idx = df.index.values


            ########## データの取得 ##########

            while Row_Number < row_end:

                ########## 空欄判定 ##########

                # ----- 空欄が1つでも存在すればTrueを返す -----
                Log.Log_Info(Log_File, "Blank Check")
                if df.isnull().any(axis=1)[df_idx[Row_Number]]:
                    Log.Log_Error(Log_File, "Blank Error\n")
                    Row_Number += 1
                    continue

                if 'E' in df.iloc[Row_Number, Col_Start_Date_Time]:
                    Log.Log_Error(Log_File, "not datetime\n")
                    Row_Number += 1
                    continue

                ########## 現在処理を行っている行のデータの取得 ##########

                # ----- 取得したデータを格納するデータ構造(辞書)を作成 -----
                Log.Log_Info(Log_File, 'Data Acquisition')
                data_dict = dict()

                wk_edit_lots =df.iloc[Row_Number, Col_Serial_Number]
                wk_edit_lots = wk_edit_lots.replace('<', '(')
                wk_edit_lots = wk_edit_lots.replace('>', ')')
                wk_edit_lots = wk_edit_lots.replace('＜', '(')
                wk_edit_lots = wk_edit_lots.replace('＞', ')')
                wk_edit_lots = wk_edit_lots.replace('（', '(')
                wk_edit_lots = wk_edit_lots.replace('）', ')')
                wk_edit_lots = wk_edit_lots.replace('　', ' ')
                wk_edit_lots = wk_edit_lots.replace(' (', '(')
                wk_edit_lots = wk_edit_lots.replace(' (', '(')
                wk_edit_lots = wk_edit_lots.replace(' 先', '先')
                wk_edit_lots = wk_edit_lots.replace('--', '-')

                wk_lots_list = wk_edit_lots.split()
                wk_beforline_lots = ''
                for wk_lots in wk_lots_list:
                    wk_lots_Addr = wk_lots.split('-')
                    Serial_Number = wk_lots_Addr[0]
                    Serial_Number_Addr = wk_lots

                    if Serial_Number == "":
                        Serial_Number = wk_beforline_lots
                        Serial_Number_Addr = wk_beforline_lots + wk_lots

                    if len(Serial_Number) != 5 or '.' in Serial_Number:
                        Serial_Number=wk_beforline_lots
                        Serial_Number_Addr = wk_beforline_lots + '-' + wk_lots
                        wk_Addr=wk_lots_Addr[0]
                    else:
                        wk_Addr=wk_lots_Addr[1]
                    wk_Addr = wk_Addr.replace('.','')
                    wk_Addr = wk_Addr.replace('-','')
                    wk_beforline_lots = Serial_Number

                    # ----- ロット番号を取得 -----
                    #Serial_Number = str(df.iloc[Row_Number, Col_Serial_Number])
                    if Serial_Number == "nan" or Serial_Number== "":
                        Log.Log_Error(Log_File, "Lot Error\n")
                        #Row_Number += 1
                        continue

                    # ----- シートが処理対象シートかどうか確認 -----
                    if Serial_Number[0] not in SerialNumber_list:
                        Log.Log_Error(Log_File, Serial_Number + ' : ' + 'Not Covered\n')
                        #Row_Number += 1
                        continue

                    # ----- Primeに接続し、ロット番号に対応する品名を取り出す -----
                    conn, cursor = SQL.connSQL()
                    if conn is None:
                        Log.Log_Error(Log_File, Serial_Number + ' : ' + 'Connection with Prime Failed')
                        break
                    Part_Number, Nine_Serial_Number = SQL.selectSQL(cursor, Serial_Number)
                    SQL.disconnSQL(conn, cursor)

                    # ----- 品名が None であれば処理を行わない -----
                    if Part_Number is None:
                        Log.Log_Error(Log_File, Serial_Number + ' : ' + "PartNumber Error\n")
                        #Row_Number += 1
                        continue

                    # ----- 各データの取得 -----
                    data_dict = {
                        'key_Start_Date_Time' : df.iloc[Row_Number, Col_Start_Date_Time],
                        'key_Operator' : df.iloc[Row_Number, Col_Operator],
                        'key_Part_Number' : Part_Number,
                        'key_Serial_Number' : Serial_Number + '-' + wk_Addr[:2],
                        'key_Serial_Number_Addr' : Serial_Number_Addr,
                        'key_Dev': wk_dev,
                        "key_LotNumber_9": Nine_Serial_Number,
                        'key_Reflectance_Front': df.iloc[Row_Number, Col_Reflectance_Front],
                        'key_Reflectance_Back': df.iloc[Row_Number, Col_Reflectance_Back],
                    }

                    ########## 日付フォーマットの変換 ##########

                    # ----- 日付を指定されたフォーマットに変換する -----
                    Log.Log_Info(Log_File, 'Date Format Conversion')
                    data_dict["key_Start_Date_Time"] = Convert_Date.Edit_Date(data_dict["key_Start_Date_Time"])

                    # ----- 指定したフォーマットに変換出来たか確認 -----
                    if len(data_dict["key_Start_Date_Time"]) != 19:
                        Log.Log_Error(Log_File, data_dict["key_Serial_Number"] + ' : ' + "Date Error\n")
                        Row_Number += 1
                        continue


                    ########## STARTTIME_SORTEDの追加 ##########

                    # ----- 日付をExcel時間に変換する -----
                    date = datetime.strptime(str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'), "%Y-%m-%d %H:%M:%S")
                    date_excel_number = int(str(date - datetime(1899, 12, 30)).split()[0])

                    # 行数を取得し、[行数/10^6]を行う
                    excel_row = Start_Number + df_idx[Row_Number] + 1
                    excel_row_div = excel_row / 10 ** 6

                    # unix_timeに上記の値を加算する
                    date_excel_number += excel_row_div

                    # data_dictに登録する
                    data_dict["key_STARTTIME_SORTED"] = date_excel_number
                    data_dict["key_SORTNUMBER"] = excel_row


                    ########## データ型の確認 ##########

                    # ----- 数値データに入る箇所に文字列が入っていないか確認する -----
                    Log.Log_Info(Log_File, "Check Data Type")
                    Result = Check.Data_Type(key_type, data_dict)
                    if Result == False:
                        Log.Log_Error(Log_File, data_dict["key_Serial_Number"] + ' : ' + "Data Error\n")
                        Row_Number += 1
                        continue


                    ########## XMLファイルの作成 ##########

                    # ----- 保存するファイル名を定義 -----
                    XML_File_Name = 'Site=' + Site + ',ProductFamily=' + ProductFamily + ',Operation=' + Operation + \
                                    ',Partnumber=' + data_dict["key_Part_Number"] + ',Serialnumber=' + data_dict["key_Serial_Number"] + \
                                    ',Testdate=' + data_dict["key_Start_Date_Time"] + '.xml'

                    # ----- XMLファイルの作成 -----
                    Log.Log_Info(Log_File, 'Excel File To XML File Conversion')

                    f = open(Output_filepath + XML_File_Name, 'w', encoding="utf-8")

                    f.write('<?xml version="1.0" encoding="utf-8"?>' + '\n' +
                            '<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">' + '\n' +
                            '       <Result startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Result="Passed">' + '\n' +
                            '               <Header SerialNumber=' + '"' + data_dict["key_Serial_Number"] + '"' + ' PartNumber=' + '"' + data_dict["key_Part_Number"] + '"' + ' Operation=' + '"' + Operation + '"' + ' TestStation=' + '"' + TestStation + '"' + ' Operator=' + '"' + data_dict["key_Operator"] + '"' + ' StartTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Site=' + '"' + Site + '"' + ' LotNumber=' + '"' + Serial_Number + '"/>' + '\n' +
                            '\n'
                            '               <TestStep Name="Coating_MG_Reflectance" startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Status="Passed">' + '\n' +
                            '                   <Data DataType="String" Name="Dev" Units="AU" Value=' + '"' + str(data_dict["key_Dev"]) + '"/>' + '\n' +
                            '                   <Data DataType="String" Name="Serial_Number_Addr" Units="AU" Value=' + '"' + str(data_dict["key_Serial_Number_Addr"]) + '"/>' + '\n' +
                            '                   <Data DataType="Numeric" Name="Reflectance_Front" Units="AU" Value=' + '"' + str(data_dict["key_Reflectance_Front"]) + '"/>' + '\n' +
                            '                   <Data DataType="Numeric" Name="Reflectance_Back" Units="AU" Value=' + '"' + str(data_dict["key_Reflectance_Back"]) + '"/>' + '\n' +
                            '               </TestStep>' + '\n' +
                            '\n'
                            '               <TestStep Name="SORTED_DATA" startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Status="Passed">' + '\n' +
                            '                   <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value=' + '"' + str(data_dict["key_STARTTIME_SORTED"]) + '"/>' + '\n' +
                            '                   <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value=' + '"' + str(data_dict["key_SORTNUMBER"]) + '"/>' + '\n' +
                            '                   <Data DataType="String" Name="LotNumber_5" Value=' + '"' + Serial_Number + '"' + ' CompOperation="LOG"/>' + '\n' +
                            '                   <Data DataType="String" Name="LotNumber_9" Value=' + '"' + str(data_dict["key_LotNumber_9"]) + '"' + ' CompOperation="LOG"/>' + '\n' +
                            '               </TestStep>' + '\n' +
                            '\n'
                            '               <ErrorData/>' + '\n' +
                            '               <FailureData/>' + '\n' +
                            '               <Configuration/>' + '\n' +
                            '       </Result>' + '\n' +
                            '</Results>'
                            )
                    f.close()


                    ########## XML変換完了時の処理 ##########

                    Log.Log_Info(Log_File, data_dict["key_Serial_Number"] + ' : ' + "OK\n")
                Row_Number += 1


    ########## 次の開始行数の書き込み ##########

#    Log.Log_Info(Log_File, 'Write the next starting line number')
#    Row_Number_Func.next_start_row_number("Grating_Ething_Depth_StartRow.txt", Next_Start_Row)

if __name__ == '__main__':

    Main()

# ----- ログ書込：Main処理の終了 -----
Log.Log_Info(Log_File, 'Program End')

# 這個檔案用途說明：
# 本程式用於自動處理「Coating MG Reflectance」相關的 Excel 檔案，將反射率測試資料轉換為 XML 格式，並記錄處理過程的日誌。
# 主要流程：
# 1. 讀取指定資料夾（MG#2、MG#3）下所有符合命名規則的 Excel 檔案。
# 2. 依據每個檔案的不同 sheet，逐行解析反射率測試資料。
# 3. 依據 SerialNumber 初始字元過濾資料，並查詢資料庫取得 PartNumber、LotNumber_9。
# 4. 進行資料型態檢查、日期格式轉換、計算排序欄位。
# 5. 依據每筆資料產生對應的 XML 檔案，並輸出到指定路徑。
# 6. 全程記錄日誌，方便追蹤處理狀態與錯誤。
# 適用於 QC/量測自動化流程，將原始 Excel 量測資料轉換為 XML 供後續系統或資料庫使用。
