# Python程式碼 - 讀取config.ini並根據內容處理資料和生成XML文件
import os
import sys
import glob
import shutil
import logging
import pandas as pd
from configparser import ConfigParser
from datetime import datetime

# 自定義模組
sys.path.append('../MyModule')
import Log
import SQL
import Check
import Convert_Date
import Row_Number_Func

# 讀取 config.ini 的設置
config = ConfigParser()
config.read('config.ini')

# 獲取配置
input_paths = [path.strip() for path in config.get('Paths', 'input_paths').split(',')]
output_path = config.get('Paths', 'output_path')
sheet_name = config.get('Excel', 'sheet_name')
data_columns = config.get('Excel', 'data_columns')
xy_sheet_name = config.get('Excel', 'xy_sheet_name')
xy_columns = config.get('Excel', 'xy_columns')
log_path = config.get('Logging', 'log_path')
fields_config = config.get('DataFields', 'fields').splitlines()

# 創建日誌文件夾
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
    df = pd.read_excel(file_path, header=None, sheet_name=sheet_name, usecols=data_columns)
    df_xy = pd.read_excel(file_path, header=None, sheet_name=xy_sheet_name, usecols=xy_columns)
    
    # 設置列號
    df.columns = range(df.shape[1])
    df_xy.columns = range(df_xy.shape[1])
    
    # 創建輸出XML文件夾
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    row_end = len(df)
    row_number = 0
    df_idx = df.index.values

    # 數據處理
    while row_number < row_end:
        data_dict = {}
        for key, (col, dtype) in fields.items():
            try:
                if col.startswith('xy'):
                    # 處理df_xy數據
                    _, row, column = col.split('_')
                    value = df_xy.iloc[int(row)-1, int(column)-1]
                else:
                    # 處理df數據
                    value = df.iloc[row_number, int(col)]
                
                # 根據指定的數據類型轉換值
                if dtype == 'float':
                    value = float(value)
                elif dtype == 'str':
                    value = str(value)
                else:
                    Log.Log_Error(log_file, f'Unsupported data type {dtype} for key {key}')
                    continue
                
                data_dict[key] = value
            except Exception as e:
                Log.Log_Error(log_file, f'Error processing field {key}: {e}')
                data_dict[key] = None
        
        # 生成XML文件
        generate_xml(data_dict)
        row_number += 1

def generate_xml(data_dict):
    xml_filename = f"Site={data_dict.get('key_Part_Number', 'Unknown')},SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')}.xml"
    xml_filepath = os.path.join(output_path, xml_filename)
    with open(xml_filepath, 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="utf-8"?>\n')
        f.write('<Results>\n')
        f.write(f"    <Result startDateTime=\"{data_dict.get('key_Start_Date_Time', '')}\" Result=\"{data_dict.get('key_Result', 'Done')}\">\n")
        f.write(f"        <Header SerialNumber=\"{data_dict.get('key_Serial_Number', '')}\" PartNumber=\"{data_dict.get('key_Part_Number', '')}\" />\n")
        # 添加更多欄位數據...
        f.write('    </Result>\n')
        f.write('</Results>\n')
    Log.Log_Info(log_file, f'XML File Created: {xml_filepath}')

def main():
    for input_path in input_paths:
        for file in glob.glob(os.path.join(input_path, '*.xlsx')):
            if '$' not in file:
                process_excel_file(file)

if __name__ == '__main__':
    main()

Log.Log_Info(log_file, 'Program End')




'''# 這個程式僅用於CVD晶體長度數據的結束位置設置。
import os  # 導入操作系統相關模組，用於處理文件和目錄操作
import sys  # 導入系統相關模組，用於訪問系統參數和函數
import xlrd  # 導入xlrd模組，用於讀取Excel文件（注意：xlrd僅支持.xls格式）
import glob  # 導入glob模組，用於文件模式匹配
import pyodbc  # 導入pyodbc模組，用於連接和操作ODBC數據庫
import shutil  # 導入shutil模組，用於高級文件操作，如複製和移動文件
import logging  # 導入logging模組，用於記錄日誌
import numpy as np  # 導入numpy模組，提供數值計算功能
import pandas as pd  # 導入pandas模組，用於數據處理和分析
import openpyxl as px  # 導入openpyxl模組，用於讀寫Excel .xlsx文件

from time import strftime, localtime  # 從time模組導入strftime和localtime函數，用於格式化時間
from datetime import date, timedelta, datetime  # 從datetime模組導入date, timedelta, datetime類，用於處理日期和時間
from dateutil.relativedelta import relativedelta  # 從dateutil模組導入relativedelta，用於處理相對日期

########## 自定義函數的定義 ##########
sys.path.append('../MyModule')  # 將自定義模組路徑添加到系統路徑中，方便後續導入自定義模組
import Log  # 導入自定義的Log模組，用於記錄日誌信息
import SQL  # 導入自定義的SQL模組，用於數據庫操作
import Check  # 導入自定義的Check模組，用於數據檢查
import Convert_Date  # 導入自定義的Convert_Date模組，用於日期轉換
import Row_Number_Func  # 導入自定義的Row_Number_Func模組，用於處理行號

########## 全局參數定義 ##########
Site = '350'  # 定義站點編號
ProductFamily = 'SAG FAB'  # 定義產品系列
Operation = 'GRATING_Crystal_Duty'  # 定義操作類型
TestStation = 'GRATING'  # 定義測試站點

########## 日誌設置 ##########

# ----- 創建日誌文件夾 -----
Log_Folder_Name = str(date.today())  # 使用當前日期作為日誌文件夾名稱
if not os.path.exists("../Log/" + Log_Folder_Name):  # 檢查日誌文件夾是否存在
    os.makedirs("../Log/" + Log_Folder_Name)  # 如果不存在，創建日誌文件夾

Log_File = '../Log/' + Log_Folder_Name + '/001_Crystal.log'  # 定義日誌文件的路徑
Log.Log_Info(Log_File, 'Program Start')  # 記錄程序開始的信息到日誌文件

########## 工作表名稱定義 ##########
Data_Sheet_Name = '004'  # 定義數據工作表名稱
XY_Sheet_Name = 'ウェハ座標'  # 定義晶圓座標工作表名稱（日文，意為“晶圓座標”）

########## XML輸出文件路徑定義 ##########
Output_filepath = '//li.lumentuminc.net/data/SAG/TDS/Data/Files to Insert/XML/'  # 定義XML文件的輸出路徑
Output_filepath = r'C:\Users\hsi67063\Box\00-home-pigo.hsiao\Meeting information\2024 SAG SPC discussion\xml'
# Output_filepath = '../XML/001_GRATING/CVD_Crystal/'  # 另一個可能的輸出路徑（被註釋掉）

########## 定義要獲取的數據列號 ##########
Col_Start_Date_Time = 0  # 開始日期時間列號
Col_Part_Number = 1  # 部件編號列號
Col_Sem_Number = 2  # SEM編號列號
Col_Serial_Number = 3  # 序列號列號
Col_Operator = 4  # 操作員列號
Col_Pitch1 = 5  # Pitch1列號
Col_Pitch2 = 6  # Pitch2列號
Col_Pitch3 = 7  # Pitch3列號
Col_Pitch4 = 8  # Pitch4列號
Col_Pitch5 = 9  # Pitch5列號
Col_Space1 = 10  # Space1列號
Col_Space2 = 11  # Space2列號
Col_Space3 = 12  # Space3列號
Col_Space4 = 13  # Space4列號
Col_Space5 = 14  # Space5列號
Col_Duty1 = 15  # Duty1列號
Col_Duty2 = 16  # Duty2列號
Col_Duty3 = 17  # Duty3列號
Col_Duty4 = 18  # Duty4列號
Col_Duty5 = 19  # Duty5列號
Col_Duty_Average = 20  # Duty平均值列號
Col_Duty_3sigma = 21  # Duty 3sigma列號
Col_Result = 22  # 結果列號

########## 定義獲取項目與數據類型對應表 ##########
key_type = {
    "key_Start_Date_Time": str,  # 開始日期時間為字符串類型
    "key_Part_Number": str,  # 部件編號為字符串類型
    "key_Sem_Number": str,  # SEM編號為字符串類型
    "key_Serial_Number": str,  # 序列號為字符串類型
    "key_Operator": str,  # 操作員為字符串類型
    "key_Operation": str,  # 操作類型為字符串類型
    "key_Pitch1": float,  # Pitch1為浮點數類型
    "key_Pitch2": float,  # Pitch2為浮點數類型
    "key_Pitch3": float,  # Pitch3為浮點數類型
    "key_Pitch4": float,  # Pitch4為浮點數類型
    "key_Pitch5": float,  # Pitch5為浮點數類型
    "key_Space1": float,  # Space1為浮點數類型
    "key_Space2": float,  # Space2為浮點數類型
    "key_Space3": float,  # Space3為浮點數類型
    "key_Space4": float,  # Space4為浮點數類型
    "key_Space5": float,  # Space5為浮點數類型
    "key_Duty1": float,  # Duty1為浮點數類型
    "key_Duty2": float,  # Duty2為浮點數類型
    "key_Duty3": float,  # Duty3為浮點數類型
    "key_Duty4": float,  # Duty4為浮點數類型
    "key_Duty5": float,  # Duty5為浮點數類型
    "key_Duty_Average": float,  # Duty平均值為浮點數類型
    "key_Duty_3sigma": float,  # Duty 3sigma為浮點數類型
    "key_Result": str,  # 結果為字符串類型
    "key_X1": float,  # X1為浮點數類型
    "key_X2": float,  # X2為浮點數類型
    "key_X3": float,  # X3為浮點數類型
    "key_X4": float,  # X4為浮點數類型
    "key_X5": float,  # X5為浮點數類型
    "key_Y1": float,  # Y1為浮點數類型
    "key_Y2": float,  # Y2為浮點數類型
    "key_Y3": float,  # Y3為浮點數類型
    "key_Y4": float,  # Y4為浮點數類型
    "key_Y5": float,  # Y5為浮點數類型
    "key_STARTTIME_SORTED": float,  # STARTTIME_SORTED為浮點數類型
    "key_SORTNUMBER": float,  # SORTNUMBER為浮點數類型
    "key_LotNumber_9": str  # LotNumber_9為字符串類型
}

def Main():
    ########## Excel文件複製 ##########
    
    # ----- 使用日誌記錄複製Excel文件的操作 -----
    Log.Log_Info(Log_File, 'Excel File Copy')
    
    FilePath = 'Z:/■QCデータシート/ドライエッチ/'  # 定義Excel文件所在的目錄路徑
    FileName = '*SiO2回折格子ﾏｽｸ品質*.xlsx'  # 定義要查找的Excel文件名模式，支持通配符
    Excel_File_List = []  # 初始化一個列表，用於存儲找到的Excel文件
    
    for file in glob.glob(os.path.join(FilePath, FileName)):  # 使用glob模組查找符合模式的文件
        if '$' not in file:  # 排除臨時文件（通常包含'$'符號）
            dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getmtime(file)))  # 獲取文件的修改時間，並格式化為字符串
            Excel_File_List.append([file, dt])  # 將文件路徑和修改時間添加到列表中
    print('Excel_File_List:', Excel_File_List)
    # ----- 按修改時間降序排序文件列表 -----
    Excel_File_List = sorted(Excel_File_List, key=lambda x: x[1], reverse=True)  # 根據修改時間對文件列表進行排序，最新的文件在前
    Excel_File = shutil.copy(Excel_File_List[0][0], '../DataFile/001_GRATING/')  # 複製最新的Excel文件到本地數據目錄
    
    ########## 創建DataFrame ##########
    
    # ----- 使用日誌記錄獲取開始行號的操作 -----
    Log.Log_Info(Log_File, 'Get The Starting Row Count')
    Start_Number = Row_Number_Func.start_row_number("CVD_Crystal_StartRow.txt") - 500  # 從文本文件中讀取開始行號，並減去500作為緩衝
    
    # ----- 使用日誌記錄讀取Excel文件的操作 -----
    Log.Log_Info(Log_File, 'Read Excel')
    df = pd.read_excel(Excel_File, header=None, sheet_name=Data_Sheet_Name, usecols="AT:BP", skiprows=Start_Number)  # 讀取指定工作表和列範圍的Excel數據，跳過指定行數
    df_xy = pd.read_excel(Excel_File, header=None, sheet_name=XY_Sheet_Name, usecols="A:C")  # 讀取晶圓座標工作表的數據
    print('df:', df.to_string())  # 印出所有的df內容
    print('df_xy:', df_xy.to_string())  # 印出所有的df_xy內容
    # ----- 使用日誌記錄設置列號的操作 -----
    Log.Log_Info(Log_File, 'Setting Columns Number')
    df.columns = range(df.shape[1])  # 重設df的列索引為連續的整數
    df_xy.columns = range(df_xy.shape[1])  # 重設df_xy的列索引為連續的整數
    
    # ----- 從末尾開始刪除含有缺失值的行 -----
    Getting_Row = len(df) - 1  # 獲取DataFrame的最後一行索引
    while Getting_Row >= 0 and df.isnull().any(axis=1)[Getting_Row]:  # 檢查該行是否有任何缺失值
        Getting_Row -= 1  # 如果有缺失值，向上移動一行
    df = df[:Getting_Row + 1]  # 截取不包含缺失值的部分
    
    # ----- 記錄下一次應該開始處理的行號 -----
    Next_Start_Row = Start_Number + df.shape[0] + 1  # 計算下一次處理應該開始的行號
    
    # ----- 將日期欄中非時間戳記類型的值替換為NaN -----
    for i in range(df.shape[0]):  # 遍歷DataFrame的每一行
        if type(df.iloc[i, 0]) is not pd.Timestamp:  # 如果第一列的數據類型不是Timestamp
            df.iloc[i, 0] = np.nan  # 將其替換為NaN
    
    # ----- 篩選出從今天起一個月前的數據 -----
    df[0] = pd.to_datetime(df[0])  # 將第一列轉換為日期時間類型
    one_month_ago = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=1)  # 計算一個月前的日期
    df = df[(df[0] >= one_month_ago)]  # 篩選出日期在一個月前及之後的數據
    
    ########## 循環前的設置 ##########
    
    # ----- 使用日誌記錄獲取DataFrame結束索引號的操作 -----
    Log.Log_Info(Log_File, 'Get DataFrame End Index Number\n')
    row_end = len(df)  # 獲取DataFrame的總行數
    
    # ----- 定義當前處理的行號 -----
    Row_Number = 0  # 初始化行號為0，開始處理第一行數據
    
    # ----- 獲取DataFrame的索引列表 -----
    df_idx = df.index.values  # 獲取DataFrame的索引值列表
    
    ########## 數據的獲取 ##########
    
    while Row_Number < row_end:  # 當前行號小於總行數時，持續處理數據
    
        ########## 空值判斷 ##########
    
        # ----- 使用日誌記錄進行空值檢查的操作 -----
        Log.Log_Info(Log_File, "Blank Check")
        if df.isnull().any(axis=1)[df_idx[Row_Number]]:  # 如果當前行有任何空值
            Log.Log_Error(Log_File, "Blank Error\n")  # 記錄空值錯誤到日誌
            Row_Number += 1  # 增加行號，跳過當前行
            continue  # 繼續下一輪循環
    
        ########## 獲取當前處理行的數據 ##########
    
        # ----- 使用日誌記錄數據獲取的操作 -----
        Log.Log_Info(Log_File, 'Data Acquisition')
        data_dict = dict()  # 初始化一個空字典，用於存儲當前行的數據
    
        # ----- 獲取序列號 -----
        Serial_Number = str(df.iloc[Row_Number, Col_Serial_Number])  # 獲取當前行的序列號並轉換為字符串
        if Serial_Number == "nan":  # 如果序列號為"nan"
            Log.Log_Error(Log_File, "Lot Error\n")  # 記錄批次錯誤到日誌
            Row_Number += 1  # 增加行號，跳過當前行
            continue  # 繼續下一輪循環
    
        # ----- 連接Prime數據庫，獲取對應的部件編號和九位序列號 -----
        conn, cursor = SQL.connSQL()  # 調用自定義SQL模組的連接函數，獲取連接和游標
        if conn is None:  # 如果連接失敗
            Log.Log_Error(Log_File, Serial_Number + ' : ' + 'Connection with Prime Failed')  # 記錄連接失敗的錯誤到日誌
            break  # 結束循環
    
        PartNumber, Nine_Serial_Number = SQL.selectSQL(cursor, Serial_Number)  # 使用游標執行查詢，獲取部件編號和九位序列號
        SQL.disconnSQL(conn, cursor)  # 關閉數據庫連接
    
        # ----- 如果部件編號為None，則不處理該行數據 -----
        if PartNumber is None:  # 如果部件編號為None
            Log.Log_Error(Log_File, Serial_Number + ' : ' + "PartNumber Error\n")  # 記錄部件編號錯誤到日誌
            Row_Number += 1  # 增加行號，跳過當前行
            continue  # 繼續下一輪循環
    
        # ----- 如果部件編號為'LDアレイ_'，則不處理該行數據 -----
        if PartNumber == 'LDアレイ_':  # 如果部件編號為特定值
            Row_Number += 1  # 增加行號，跳過當前行
            continue  # 繼續下一輪循環
    
        # ----- 獲取當前行的數據並存儲到字典中 -----
        data_dict = {
            "key_Start_Date_Time": df.iloc[Row_Number, Col_Start_Date_Time],  # 開始日期時間
            "key_Part_Number": PartNumber,  # 部件編號
            "key_Sem_Number": df.iloc[Row_Number, Col_Sem_Number],  # SEM編號
            "key_Serial_Number": Serial_Number,  # 序列號
            "key_LotNumber_9": Nine_Serial_Number,  # 九位批次號
            "key_Operator": df.iloc[Row_Number, Col_Operator],  # 操作員
            "key_Operation": Operation,  # 操作類型
            "key_Pitch1": df.iloc[Row_Number, Col_Pitch1],  # Pitch1數據
            "key_Pitch2": df.iloc[Row_Number, Col_Pitch2],  # Pitch2數據
            "key_Pitch3": df.iloc[Row_Number, Col_Pitch3],  # Pitch3數據
            "key_Pitch4": df.iloc[Row_Number, Col_Pitch4],  # Pitch4數據
            "key_Pitch5": df.iloc[Row_Number, Col_Pitch5],  # Pitch5數據
            "key_Space1": df.iloc[Row_Number, Col_Space1],  # Space1數據
            "key_Space2": df.iloc[Row_Number, Col_Space2],  # Space2數據
            "key_Space3": df.iloc[Row_Number, Col_Space3],  # Space3數據
            "key_Space4": df.iloc[Row_Number, Col_Space4],  # Space4數據
            "key_Space5": df.iloc[Row_Number, Col_Space5],  # Space5數據
            "key_Duty1": df.iloc[Row_Number, Col_Duty1],  # Duty1數據
            "key_Duty2": df.iloc[Row_Number, Col_Duty2],  # Duty2數據
            "key_Duty3": df.iloc[Row_Number, Col_Duty3],  # Duty3數據
            "key_Duty4": df.iloc[Row_Number, Col_Duty4],  # Duty4數據
            "key_Duty5": df.iloc[Row_Number, Col_Duty5],  # Duty5數據
            "key_Duty_Average": df.iloc[Row_Number, Col_Duty_Average],  # Duty平均值
            "key_Duty_3sigma": df.iloc[Row_Number, Col_Duty_3sigma],  # Duty 3sigma值
            "key_Result": df.iloc[Row_Number, Col_Result],  # 結果
            "key_X1": df_xy.iloc[1, 1],  # X1座標（從第二行開始）
            "key_X2": df_xy.iloc[2, 1],  # X2座標
            "key_X3": df_xy.iloc[3, 1],  # X3座標
            "key_X4": df_xy.iloc[4, 1],  # X4座標
            "key_X5": df_xy.iloc[5, 1],  # X5座標
            "key_Y1": df_xy.iloc[1, 2],  # Y1座標
            "key_Y2": df_xy.iloc[2, 2],  # Y2座標
            "key_Y3": df_xy.iloc[3, 2],  # Y3座標
            "key_Y4": df_xy.iloc[4, 2],  # Y4座標
            "key_Y5": df_xy.iloc[5, 2]   # Y5座標
        }
    
        ########## 日期格式的轉換 ##########
    
        # ----- 使用日誌記錄日期格式轉換的操作 -----
        Log.Log_Info(Log_File, 'Date Format Conversion')
        data_dict["key_Start_Date_Time"] = Convert_Date.Edit_Date(data_dict["key_Start_Date_Time"])  # 調用自定義的Convert_Date模組轉換日期格式
    
        # ----- 檢查日期格式是否轉換成功 -----
        if len(data_dict["key_Start_Date_Time"]) != 19:  # 如果轉換後的日期字符串長度不為19
            Log.Log_Error(Log_File, data_dict["key_Serial_Number"] + ' : ' + "Date Error\n")  # 記錄日期錯誤到日誌
            Row_Number += 1  # 增加行號，跳過當前行
            continue  # 繼續下一輪循環
    
        ########## 添加STARTTIME_SORTED ##########
    
        # ----- 將日期轉換為Excel時間格式 -----
        date = datetime.strptime(str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'), "%Y-%m-%d %H:%M:%S")  # 將日期字符串轉換為datetime對象
        date_excel_number = int(str(date - datetime(1899, 12, 30)).split()[0])  # 計算自Excel起始日期的天數
    
        # ----- 獲取行數並進行處理 -----
        excel_row = Start_Number + df_idx[Row_Number] + 1  # 計算當前行在Excel中的行號
        excel_row_div = excel_row / 10**6  # 將行號除以100萬，作為小數部分
    
        # ----- 將上面計算的值加到date_excel_number -----
        date_excel_number += excel_row_div  # 將行號的計算結果加到日期數字中
        print('date_excel_number:', date_excel_number)
    
        # ----- 將計算結果存入字典 -----
        data_dict["key_STARTTIME_SORTED"] = date_excel_number  # 添加STARTTIME_SORTED鍵
        data_dict["key_SORTNUMBER"] = excel_row  # 添加SORTNUMBER鍵
    
        ########## 數據類型的檢查 ##########
    
        # ----- 使用日誌記錄數據類型檢查的操作 -----
        Log.Log_Info(Log_File, "Check Data Type")
        Result = Check.Data_Type(key_type, data_dict)  # 調用自定義的Check模組檢查數據類型
        if Result == False:  # 如果數據類型檢查失敗
            Log.Log_Error(Log_File, data_dict["key_Serial_Number"] + ' : ' + "Data Error\n")  # 記錄數據錯誤到日誌
            Row_Number += 1  # 增加行號，跳過當前行
            continue  # 繼續下一輪循環
    
        ########## 創建XML文件 ##########
    
        # ----- 定義要保存的XML文件名 -----
        XML_File_Name = 'Site=' + Site + ',ProductFamily=' + ProductFamily + ',Operation=' + Operation + \
                        ',Partnumber=' + data_dict["key_Part_Number"] + ',Serialnumber=' + data_dict["key_Serial_Number"] + \
                        ',Testdate=' + data_dict["key_Start_Date_Time"] + '.xml'  # 根據數據定義XML文件名
    
        # ----- 根據結果設定XML中的Result屬性值 -----
        if data_dict["key_Result"] == "OK":  # 如果結果為"OK"
            tmp_Result = "Passed"  # 設定為"Passed"
        elif data_dict["key_Result"] == "NG":  # 如果結果為"NG"
            tmp_Result = "Failed"  # 設定為"Failed"
        else:
            tmp_Result = "Done"  # 其他情況設定為"Done"
    
        # ----- 使用日誌記錄Excel轉XML的操作 -----
        Log.Log_Info(Log_File, 'Excel File To XML File Conversion')
        
        f = open(Output_filepath + '\\' + XML_File_Name, 'w', encoding="utf-8")  # 打開或創建XML文件，準備寫入
        print('Output_filepath + XML_File_Name:', Output_filepath + XML_File_Name)
        # ----- 將數據寫入XML文件 -----
        f.write('<?xml version="1.0" encoding="utf-8"?>' + '\n' +
                '<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">' + '\n' +
                '       <Result startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Result=' + '"' + tmp_Result + '">' + '\n' +
                '               <Header SerialNumber=' + '"' + data_dict["key_Serial_Number"] + '"' + ' PartNumber=' + '"' + data_dict["key_Part_Number"] + '"' + ' Operation=' + '"' + Operation + '"' + ' TestStation=' + '"' + TestStation + '"' + ' Operator=' + '"' + data_dict["key_Operator"] + '"' + ' StartTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Site=' + '"' + Site + '"' + ' LotNumber=' + '"' + data_dict["key_Serial_Number"] + '"/>' + '\n' +
                '\n'
                '               <TestStep Name="Length1" startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Status=' + '"' + tmp_Result + '">' + '\n' +
                '                   <Data DataType="Numeric" Name="X" Units="um" Value=' + '"' + str(data_dict["key_X1"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Y" Units="um" Value=' + '"' + str(data_dict["key_Y1"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Pitch" Units="nm" Value=' + '"' + str(data_dict["key_Pitch1"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Space" Units="nm" Value=' + '"' + str(data_dict["key_Space1"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Duty" Units="AU" Value=' + '"' + str(data_dict["key_Duty1"]) + '"/>' + '\n' +
                '               </TestStep>' + '\n' +
                '               <TestStep Name="Length2" startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Status=' + '"' + tmp_Result + '">' + '\n' +
                '                   <Data DataType="Numeric" Name="X" Units="um" Value=' + '"' + str(data_dict["key_X2"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Y" Units="um" Value=' + '"' + str(data_dict["key_Y2"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Pitch" Units="nm" Value=' + '"' + str(data_dict["key_Pitch2"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Space" Units="nm" Value=' + '"' + str(data_dict["key_Space2"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Duty" Units="AU" Value=' + '"' + str(data_dict["key_Duty2"]) + '"/>' + '\n' +
                '               </TestStep>' + '\n' +
                '               <TestStep Name="Length3" startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Status=' + '"' + tmp_Result + '">' + '\n' +
                '                   <Data DataType="Numeric" Name="X" Units="um" Value=' + '"' + str(data_dict["key_X3"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Y" Units="um" Value=' + '"' + str(data_dict["key_Y3"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Pitch" Units="nm" Value=' + '"' + str(data_dict["key_Pitch3"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Space" Units="nm" Value=' + '"' + str(data_dict["key_Space3"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Duty" Units="AU" Value=' + '"' + str(data_dict["key_Duty3"]) + '"/>' + '\n' +
                '               </TestStep>' + '\n' +
                '               <TestStep Name="Length4" startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Status=' + '"' + tmp_Result + '">' + '\n' +
                '                   <Data DataType="Numeric" Name="X" Units="um" Value=' + '"' + str(data_dict["key_X4"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Y" Units="um" Value=' + '"' + str(data_dict["key_Y4"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Pitch" Units="nm" Value=' + '"' + str(data_dict["key_Pitch4"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Space" Units="nm" Value=' + '"' + str(data_dict["key_Space4"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Duty" Units="AU" Value=' + '"' + str(data_dict["key_Duty4"]) + '"/>' + '\n' +
                '               </TestStep>' + '\n' +
                '               <TestStep Name="Length5" startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Status=' + '"' + tmp_Result + '">' + '\n' +
                '                   <Data DataType="Numeric" Name="X" Units="um" Value=' + '"' + str(data_dict["key_X5"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Y" Units="um" Value=' + '"' + str(data_dict["key_Y5"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Pitch" Units="nm" Value=' + '"' + str(data_dict["key_Pitch5"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Space" Units="nm" Value=' + '"' + str(data_dict["key_Space5"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="Duty" Units="AU" Value=' + '"' + str(data_dict["key_Duty5"]) + '"/>' + '\n' +
                '               </TestStep>' + '\n' +
                '               <TestStep Name="Average" startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Status=' + '"' + tmp_Result + '">' + '\n' +
                '                   <Data DataType="Numeric" Name="Duty" Units="AU" Value=' + '"' + str(data_dict["key_Duty_Average"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="3sigma" Units="AU" Value=' + '"' + str(data_dict["key_Duty_3sigma"]) + '"/>' + '\n' +
                '               </TestStep>' + '\n' +
                '               <TestStep Name="SORTED_DATA" startDateTime=' + '"' + data_dict["key_Start_Date_Time"].replace(".", ":") + '"' + ' Status=' + '"' + tmp_Result + '">' + '\n' +
                '                   <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value=' + '"' + str(data_dict["key_STARTTIME_SORTED"]) + '"/>' + '\n' +
                '                   <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value=' + '"' + str(data_dict["key_SORTNUMBER"]) + '"/>' + '\n' +
                '                   <Data DataType="String" Name="LotNumber_5" Value=' + '"' + str(data_dict["key_Serial_Number"]) + '"' + ' CompOperation="LOG"/>' + '\n' +
                '                   <Data DataType="String" Name="LotNumber_9" Value=' + '"' + str(data_dict["key_LotNumber_9"]) + '"' + ' CompOperation="LOG"/>' + '\n' +
                '               </TestStep>' + '\n' +
                '\n'
                '               <TestEquipment>' + '\n' +
                '                   <Item DeviceName="SEM" DeviceSerialNumber=' + '"' + str(data_dict["key_Sem_Number"]) + '"/>' + '\n' +
                '               </TestEquipment>' + '\n' +
                '\n'
                '               <ErrorData/>' + '\n' +
                '               <FailureData/>' + '\n' +
                '               <Configuration/>' + '\n' +
                '       </Result>' + '\n' +
                '</Results>'
                )
        f.close()  # 關閉文件
        Log.Log_Info(Output_filepath + XML_File_Name, 'XML File Created')  
        ########## XML轉換完成時的處理 ##########
    
        Log.Log_Info(Log_File, data_dict["key_Serial_Number"] + ' : ' + "OK\n")  # 記錄當前序列號處理成功的信息到日誌
        Row_Number += 1  # 增加行號，處理下一行
    
    ########## 記錄下一次開始行號 ##########
    
        Log.Log_Info(Log_File, 'Write the next starting line number')  # 記錄寫入下一次開始行號的信息到日誌
        Row_Number_Func.next_start_row_number("CVD_Crystal_StartRow.txt", Next_Start_Row)  # 調用自定義模組函數，將下一次開始行號寫入文本文件
    
        # ----- 將寫入了最後一行的文件複製到G盤 -----
        #shutil.copy("CVD_Crystal_StartRow.txt", 'T:/04_プロセス関係/10_共通/91_KAIZEN-TDS/01_開発/001_GRATING/13_ProgramUsedFile/')  # 將記錄行號的文件複製到指定的G盤路徑
    
if __name__ == '__main__':  # 如果此腳本作為主程序運行
    Main()  # 調用主函數，開始執行程序

# ----- 日誌寫入：主程序處理結束 -----
Log.Log_Info(Log_File, 'Program End')  # 記錄程序結束的信息到日誌文件
'''