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
Operation = 'GRATING_CVD_Duty'
TestStation = 'GRATING'

########## Logの設定 ##########

# ----- ログファイルの作成 -----
Log_Folder_Name = str(date.today())
if not os.path.exists("../Log/" + Log_Folder_Name):
    os.makedirs("../Log/" + Log_Folder_Name)

Log_File = '../Log/' + Log_Folder_Name + '/001_Mask.log'
Log.Log_Info(Log_File, 'Program Start')

########## シート名の定義 ##########
Data_Sheet_Name = '004'
XY_Sheet_Name = 'ウェハ座標'

########## XML出力先ファイルパス ##########
Output_filepath = '//li.lumentuminc.net/data/SAG/TDS/Data/Files to Insert/XML/'
# Output_filepath = '../XML/001_GRATING/CVD_Mask/'


########## 取得するデータのdfの列番号を定義 ##########
Col_Start_Date_Time = 0
Col_Part_Number = 1
Col_Sem_Number = 2
Col_Serial_Number = 3
Col_Operator = 4
Col_Pitch1 = 5
Col_Pitch2 = 6
Col_Pitch3 = 7
Col_Pitch4 = 8
Col_Pitch5 = 9
Col_Space1 = 10
Col_Space2 = 11
Col_Space3 = 12
Col_Space4 = 13
Col_Space5 = 14
Col_Duty1 = 15
Col_Duty2 = 16
Col_Duty3 = 17
Col_Duty4 = 18
Col_Duty5 = 19
Col_Duty_Average = 20
Col_Duty_3sigma = 21
Col_Result = 22

########## 取得した項目と型の対応表を定義 ##########
key_type = {
    "key_Start_Date_Time": str,
    "key_Part_Number": str,
    "key_Sem_Number": str,
    "key_Serial_Number": str,
    "key_Operator": str,
    "key_Pitch1": float,
    "key_Pitch2": float,
    "key_Pitch3": float,
    "key_Pitch4": float,
    "key_Pitch5": float,
    "key_Space1": float,
    "key_Space2": float,
    "key_Space3": float,
    "key_Space4": float,
    "key_Space5": float,
    "key_Duty1": float,
    "key_Duty2": float,
    "key_Duty3": float,
    "key_Duty4": float,
    "key_Duty5": float,
    "key_Duty_Average": float,
    "key_Duty_3sigma": float,
    "key_Result": str,
    "key_X1": float,
    "key_X2": float,
    "key_X3": float,
    "key_X4": float,
    "key_X5": float,
    "key_Y1": float,
    "key_Y2": float,
    "key_Y3": float,
    "key_Y4": float,
    "key_Y5": float,
    "key_STARTTIME_SORTED": float,
    "key_SORTNUMBER" : float,
    "key_LotNumber_9" : str
}

def Main():

    ########## Excelファイルをローカルにコピー ##########

    # ----- 正規表現で取出し、直近で変更があったファイルを取得 -----
    Log.Log_Info(Log_File, 'Excel File Copy')

    FilePath = 'Z:/■QCデータシート/ドライエッチ/'
    #FilePath = r'C:\Users\hsi67063\Downloads\Data'
    FileName = '*SiO2回折格子ﾏｽｸ品質*.xlsx'
    Excel_File_List = []
    for file in glob.glob(os.path.join(FilePath, FileName)):
        if '$' not in file:
            dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getmtime(file)))
            Excel_File_List.append([file, dt])

    if not Excel_File_List:
        Log.Log_Error(Log_File, '沒有符合條件的 Excel 檔案。')
        return

    # ----- dt(更新日時)の降順で並び替える -----
    Excel_File_List = sorted(Excel_File_List, key=lambda x: x[1], reverse=True)
    latest_excel_file = Excel_File_List[0][0]
    Excel_File = shutil.copy(latest_excel_file, '../DataFile/001_GRATING/')
    Log.Log_Info(Log_File, f'複製 Excel 檔案: {latest_excel_file} 到 ../DataFile/001_GRATING/')

    ########## DataFrameの作成 ##########

    # ----- 取得開始行の取り出し -----
    Log.Log_Info(Log_File, 'Get The Starting Row Count')
    Start_Number = Row_Number_Func.start_row_number("CVD_Mask_StartRow.txt") - 500

    # ----- ExcelデータをDataFrameとして取得 -----
    Log.Log_Info(Log_File, 'Read Excel')
    try:
        df = pd.read_excel(Excel_File, header=None, sheet_name=Data_Sheet_Name, usecols="B:AR", skiprows=Start_Number)
        df_xy = pd.read_excel(Excel_File, header=None, sheet_name=XY_Sheet_Name, usecols="A:C")
        Log.Log_Info(Log_File, '成功讀取 Excel 檔案並生成 DataFrame')
        
    except Exception as e:
        Log.Log_Error(Log_File, f'讀取 Excel 檔案失敗: {e}')
        return

    # ----- 使わない列を落とす -----
    df = df.drop(range(6,26), axis=1)
  
    # ----- 列番号の振り直し -----
    Log.Log_Info(Log_File, 'Setting Columns Number')
    df.columns = range(df.shape[1])
    df_xy.columns = range(df_xy.shape[1])

    # ----- 末尾から欠損のデータを落としていく -----
    Getting_Row = len(df) - 1
    while Getting_Row >= 0 and df.isnull().any(axis=1)[Getting_Row]:
        Getting_Row -= 1
    df = df[:Getting_Row + 1]
    Start_Number = 0
    # ----- 次の開始行数をメモ -----
    Next_Start_Row = Start_Number #+ df.shape[0] + 1


    # ----- 日付欄に文字列が入っていたらNoneに置き換える -----
    df[0] = pd.to_datetime(df[0], errors='coerce')
    for i in range(df.shape[0]):
        if pd.isna(df.iloc[i, 0]):
            df.iloc[i, 0] = np.nan

    # ----- 今日から1か月前のデータまでを取得する -----
    df[0] = pd.to_datetime(df[0])
    one_month_ago = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=2)
    df = df[(df[0] >= one_month_ago)]

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
            Log.Log_Error(Log_File, f"行 {df_idx[Row_Number]} 有空欄，跳過該行。")
            Row_Number += 1
            continue

        ########## 現在処理を行っている行のデータの取得 ##########

        # ----- 取得したデータを格納するデータ構造(辞書)を作成 -----
        Log.Log_Info(Log_File, 'Data Acquisition')
        data_dict = dict()

        # ----- ロット番号を取得 -----
        Serial_Number = str(df.iloc[Row_Number, Col_Serial_Number])

        
        if Serial_Number == "nan":
            Log.Log_Error(Log_File, f"行 {df_idx[Row_Number]} 的 LotNumber 為 NaN，跳過該行。")
            Row_Number += 1
            continue

        # ----- Primeに接続し、ロット番号に対応する品名を取り出す -----
        conn, cursor = SQL.connSQL()
        if conn is None:
            Log.Log_Error(Log_File, f'{Serial_Number} : Connection with Prime Failed')
            break
        PartNumber, Nine_Serial_Number = SQL.selectSQL(cursor, Serial_Number)
        SQL.disconnSQL(conn, cursor)

        # ----- 品名が None であれば処理を行わない -----
        if PartNumber is None:
            Log.Log_Error(Log_File, f'{Serial_Number} : PartNumber Error\n')
            Row_Number += 1
            continue

        # ----- 品名が LDアレイ_ であれば処理を行わない -----
        if PartNumber == 'LDアレイ_':
            Row_Number += 1
            continue

        # ----- データの取得 -----
        data_dict = {
            "key_Start_Date_Time": df.iloc[Row_Number, Col_Start_Date_Time],
            "key_Part_Number": PartNumber,
            "key_Sem_Number": df.iloc[Row_Number, Col_Sem_Number],
            "key_Serial_Number": Serial_Number,
            "key_LotNumber_9" : Nine_Serial_Number,
            "key_Operator": df.iloc[Row_Number, Col_Operator],
            "key_Operation": Operation,
            "key_Pitch1": df.iloc[Row_Number, Col_Pitch1],
            "key_Pitch2": df.iloc[Row_Number, Col_Pitch2],
            "key_Pitch3": df.iloc[Row_Number, Col_Pitch3],
            "key_Pitch4": df.iloc[Row_Number, Col_Pitch4],
            "key_Pitch5": df.iloc[Row_Number, Col_Pitch5],
            "key_Space1": df.iloc[Row_Number, Col_Space1],
            "key_Space2": df.iloc[Row_Number, Col_Space2],
            "key_Space3": df.iloc[Row_Number, Col_Space3],
            "key_Space4": df.iloc[Row_Number, Col_Space4],
            "key_Space5": df.iloc[Row_Number, Col_Space5],
            "key_Duty1": df.iloc[Row_Number, Col_Duty1],
            "key_Duty2": df.iloc[Row_Number, Col_Duty2],
            "key_Duty3": df.iloc[Row_Number, Col_Duty3],
            "key_Duty4": df.iloc[Row_Number, Col_Duty4],
            "key_Duty5": df.iloc[Row_Number, Col_Duty5],
            "key_Duty_Average": df.iloc[Row_Number, Col_Duty_Average],
            "key_Duty_3sigma": df.iloc[Row_Number, Col_Duty_3sigma],
            "key_Result": df.iloc[Row_Number, Col_Result],
            "key_X1": df_xy.iloc[1, 1],
            "key_X2": df_xy.iloc[2, 1],
            "key_X3": df_xy.iloc[3, 1],
            "key_X4": df_xy.iloc[4, 1],
            "key_X5": df_xy.iloc[5, 1],
            "key_Y1": df_xy.iloc[1, 2],
            "key_Y2": df_xy.iloc[2, 2],
            "key_Y3": df_xy.iloc[3, 2],
            "key_Y4": df_xy.iloc[4, 2],
            "key_Y5": df_xy.iloc[5, 2]
        }
        ########## 日付フォーマットの変換 ##########

        # ----- 日付を指定されたフォーマットに変換する -----
        Log.Log_Info(Log_File, 'Date Format Conversion')
        data_dict["key_Start_Date_Time"] = Convert_Date.Edit_Date(data_dict["key_Start_Date_Time"])

        # ----- 指定したフォーマットに変換出来たか確認 -----
        if len(data_dict["key_Start_Date_Time"]) != 19:
            Log.Log_Error(Log_File, f'{data_dict["key_Serial_Number"]} : Date Error\n')
            Row_Number += 1
            continue

        ########## STARTTIME_SORTEDの追加 ##########

        # ----- 日付をExcel時間に変換する -----
        try:
            date_obj = datetime.strptime(str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'), "%Y-%m-%d %H:%M:%S")
            date_excel_number = int((date_obj - datetime(1899, 12, 30)).total_seconds() / 86400)  # Excelの日付は日単位
        except Exception as e:
            Log.Log_Error(Log_File, f'{data_dict["key_Serial_Number"]} : Date Conversion Error: {e}\n')
            Row_Number += 1
            continue

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
            Log.Log_Error(Log_File, f'{data_dict["key_Serial_Number"]} : Data Type Mismatch Error\n')
            Row_Number += 1
            continue

        ########## XMLファイルの作成 ##########

        # ----- 保存するファイル名を定義 -----
        XML_File_Name = f'Site={Site},ProductFamily={ProductFamily},Operation={Operation},Partnumber={data_dict["key_Part_Number"]},Serialnumber={data_dict["key_Serial_Number"]},Testdate={data_dict["key_Start_Date_Time"]}.xml'

        # ----- OK/NG の書き換え -----
        if data_dict["key_Result"] == "OK":
            tmp_Result = "Passed"
        elif data_dict["key_Result"] == "NG":
            tmp_Result = "Failed"
        else:
            tmp_Result = "Done"

        # ----- XMLファイルの作成 -----
        Log.Log_Info(Log_File, 'Excel File To XML File Conversion')

        # 定義 XML 文件名
        full_xml_path = os.path.join(Output_filepath, XML_File_Name)
        Log.Log_Info(Log_File, f'生成 XML 檔案: {full_xml_path}')

        # 生成 XML 內容
        xml_content = f'''<?xml version="1.0" encoding="utf-8"?>
<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <Result startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Result="{tmp_Result}">
        <Header SerialNumber="{data_dict["key_Serial_Number"]}" PartNumber="{data_dict["key_Part_Number"]}" Operation="{Operation}" TestStation="{TestStation}" Operator="{data_dict["key_Operator"]}" StartTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Site="{Site}" LotNumber="{data_dict["key_Serial_Number"]}"/>

        <TestStep Name="Length1" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="{tmp_Result}">
            <Data DataType="Numeric" Name="X" Units="um" Value="{data_dict["key_X1"]}"/>
            <Data DataType="Numeric" Name="Y" Units="um" Value="{data_dict["key_Y1"]}"/>
            <Data DataType="Numeric" Name="Pitch" Units="nm" Value="{data_dict["key_Pitch1"]}"/>
            <Data DataType="Numeric" Name="Space" Units="nm" Value="{data_dict["key_Space1"]}"/>
            <Data DataType="Numeric" Name="Duty" Units="AU" Value="{data_dict["key_Duty1"]}"/>
        </TestStep>

        <TestStep Name="Length2" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="{tmp_Result}">
            <Data DataType="Numeric" Name="X" Units="um" Value="{data_dict["key_X2"]}"/>
            <Data DataType="Numeric" Name="Y" Units="um" Value="{data_dict["key_Y2"]}"/>
            <Data DataType="Numeric" Name="Pitch" Units="nm" Value="{data_dict["key_Pitch2"]}"/>
            <Data DataType="Numeric" Name="Space" Units="nm" Value="{data_dict["key_Space2"]}"/>
            <Data DataType="Numeric" Name="Duty" Units="AU" Value="{data_dict["key_Duty2"]}"/>
        </TestStep>

        <TestStep Name="Length3" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="{tmp_Result}">
            <Data DataType="Numeric" Name="X" Units="um" Value="{data_dict["key_X3"]}"/>
            <Data DataType="Numeric" Name="Y" Units="um" Value="{data_dict["key_Y3"]}"/>
            <Data DataType="Numeric" Name="Pitch" Units="nm" Value="{data_dict["key_Pitch3"]}"/>
            <Data DataType="Numeric" Name="Space" Units="nm" Value="{data_dict["key_Space3"]}"/>
            <Data DataType="Numeric" Name="Duty" Units="AU" Value="{data_dict["key_Duty3"]}"/>
        </TestStep>

        <TestStep Name="Length4" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="{tmp_Result}">
            <Data DataType="Numeric" Name="X" Units="um" Value="{data_dict["key_X4"]}"/>
            <Data DataType="Numeric" Name="Y" Units="um" Value="{data_dict["key_Y4"]}"/>
            <Data DataType="Numeric" Name="Pitch" Units="nm" Value="{data_dict["key_Pitch4"]}"/>
            <Data DataType="Numeric" Name="Space" Units="nm" Value="{data_dict["key_Space4"]}"/>
            <Data DataType="Numeric" Name="Duty" Units="AU" Value="{data_dict["key_Duty4"]}"/>
        </TestStep>

        <TestStep Name="Length5" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="{tmp_Result}">
            <Data DataType="Numeric" Name="X" Units="um" Value="{data_dict["key_X5"]}"/>
            <Data DataType="Numeric" Name="Y" Units="um" Value="{data_dict["key_Y5"]}"/>
            <Data DataType="Numeric" Name="Pitch" Units="nm" Value="{data_dict["key_Pitch5"]}"/>
            <Data DataType="Numeric" Name="Space" Units="nm" Value="{data_dict["key_Space5"]}"/>
            <Data DataType="Numeric" Name="Duty" Units="AU" Value="{data_dict["key_Duty5"]}"/>
        </TestStep>

        <TestStep Name="Average" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="{tmp_Result}">
            <Data DataType="Numeric" Name="Duty" Units="AU" Value="{data_dict["key_Duty_Average"]}"/>
            <Data DataType="Numeric" Name="3sigma" Units="AU" Value="{data_dict["key_Duty_3sigma"]}"/>
        </TestStep>

        <TestStep Name="SORTED_DATA" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="{tmp_Result}">
            <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value="{data_dict["key_STARTTIME_SORTED"]}"/>
            <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value="{data_dict["key_SORTNUMBER"]}"/>
            <Data DataType="String" Name="LotNumber_5" Value="{data_dict["key_Serial_Number"]}" CompOperation="LOG"/>
            <Data DataType="String" Name="LotNumber_9" Value="{data_dict["key_LotNumber_9"]}" CompOperation="LOG"/>
        </TestStep>

        <TestEquipment>
            <Item DeviceName="SEM" DeviceSerialNumber="{data_dict["key_Sem_Number"]}"/>
        </TestEquipment>

        <ErrorData/>
        <FailureData/>
        <Configuration/>
    </Result>
</Results>
'''



        # ----- XMLファイルの書き込み -----
        try:
            with open(full_xml_path, 'w', encoding="utf-8") as f:
                f.write(xml_content)
            Log.Log_Info(Log_File, f'成功生成 XML 檔案: {full_xml_path}')
        except Exception as e:
            Log.Log_Error(Log_File, f'生成 XML 檔案失敗: {full_xml_path}，錯誤訊息: {e}')
            Row_Number += 1
            continue

        # ----- 確認 XML 檔案是否存在且不為空 -----
        if os.path.exists(full_xml_path) and os.path.getsize(full_xml_path) > 0:
            Log.Log_Info(Log_File, f'XML 檔案已成功生成並存在: {full_xml_path}')
        else:
            Log.Log_Error(Log_File, f'XML 檔案生成失敗或檔案為空: {full_xml_path}')

        ########## XML変換完了時の処理 ##########

        Log.Log_Info(Log_File, f'{data_dict["key_Serial_Number"]} : OK\n')
        Row_Number += 1

    ########## 次の開始行数の書き込み ##########

    Log.Log_Info(Log_File, 'Write the next starting line number')
    Row_Number_Func.next_start_row_number("CVD_Mask_StartRow.txt", Next_Start_Row)

    # ----- 最終行を書き込んだファイルをGドライブに転送 -----
    try:
        #shutil.copy("CVD_Mask_StartRow.txt", 'T:/04_プロセス関係/10_共通/91_KAIZEN-TDS/01_開発/001_GRATING/13_ProgramUsedFile/')
        Log.Log_Info(Log_File, '成功複製 CVD_Mask_StartRow.txt 到 Gドライブ')
    except Exception as e:
        Log.Log_Error(Log_File, f'複製 CVD_Mask_StartRow.txt 到 Gドライブ失敗: {e}')

if __name__ == '__main__':

    Main()

    # ----- ログ書込：Main処理の終了 -----
    Log.Log_Info(Log_File, 'Program End')
