#!/usr/bin/env python  # 指定執行此程式的解譯器
# -*- coding: utf-8 -*-  # 設定此檔案的編碼為 UTF-8

"""  
本程序功能：
1. 讀取所有 .ini 文件，根據配置處理 Excel 文件數據並生成 XML 文件。  # 說明程序功能
2. 運行記錄和錯誤日誌由自定義模組 Log 輸出。  # 說明日誌輸出方式

依賴模組：
- Log, SQL, Check, Convert_Date, Row_Number_Func（均在 ../MyModule 中）  # 列出依賴的自定義模組
"""  # 多行註解：程序說明

import os  # 匯入 os 模組，處理作業系統相關操作
import sys  # 匯入 sys 模組，提供與 Python 解譯器互動的功能
import glob  # 匯入 glob 模組，用於檔案路徑匹配
import shutil  # 匯入 shutil 模組，提供檔案複製和移動功能
import logging  # 匯入 logging 模組，用於記錄日誌
import pandas as pd  # 匯入 pandas 模組，簡稱 pd，用於資料處理
from configparser import ConfigParser, NoSectionError, NoOptionError  # 從 configparser 模組導入配置解析相關類別
from datetime import datetime, timedelta, date  # 從 datetime 模組導入日期與時間相關類別

sys.path.append('../MyModule')  # 將 ../MyModule 加入系統模組搜尋路徑
import Log  # 匯入自定義 Log 模組，用於日誌記錄
import SQL  # 匯入自定義 SQL 模組，用於資料庫操作
import Check  # 匯入自定義 Check 模組
import Convert_Date  # 匯入自定義 Convert_Date 模組
import Row_Number_Func  # 匯入自定義 Row_Number_Func 模組，用於處理行號

global_log_file = None  # 定義全域變數 global_log_file，初始值為 None

def setup_logging(log_file_path: str) -> None:  # 定義 setup_logging 函數，設定日誌格式及檔案
    """設定日誌的格式和文件"""  # 函數說明：設定日誌輸出格式和寫入檔案
    try:  # 嘗試執行以下代碼
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,  # 設定日誌檔案與等級
                            format='%(asctime)s - %(levelname)s - %(message)s')  # 設定日誌輸出格式
    except OSError as e:  # 捕捉 OSError 異常
        print(f"Error setting up log file {log_file_path}: {e}")  # 輸出錯誤訊息至螢幕
        raise  # 重新引發異常

def update_running_rec(running_rec_path: str, end_date: datetime) -> None:  # 定義 update_running_rec 函數，更新運行記錄檔案
    """更新運行記錄文件"""  # 函數說明：將最新結束日期寫入運行記錄文件
    try:  # 嘗試執行以下代碼
        with open(running_rec_path, 'w', encoding='utf-8') as f:  # 以寫入模式開啟運行記錄文件
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))  # 將日期格式化後寫入文件
        Log.Log_Info(global_log_file, f"Running record file {running_rec_path} updated with end date {end_date}")  # 記錄更新成功訊息
    except Exception as e:  # 捕捉所有例外
        Log.Log_Error(global_log_file, f"Error updating running record file {running_rec_path}: {e}")  # 記錄錯誤訊息

def ensure_running_rec_exists_and_update(running_rec_path: str, end_date: datetime) -> None:  # 定義 ensure_running_rec_exists_and_update 函數，確認運行記錄檔存在並更新
    """若運行記錄文件不存在則創建並更新"""  # 函數說明：檢查運行記錄文件是否存在，不存在則創建後更新
    try:  # 嘗試執行以下代碼
        with open(running_rec_path, 'w', encoding='utf-8') as f:  # 以寫入模式開啟（或創建）運行記錄文件
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))  # 將結束日期寫入文件
        Log.Log_Info(global_log_file, f"Running record file {running_rec_path} confirmed and updated with end date {end_date}")  # 記錄更新成功訊息
    except Exception as e:  # 捕捉所有例外
        Log.Log_Error(global_log_file, f"Error processing running record file {running_rec_path}: {e}")  # 記錄錯誤訊息

def read_running_rec(running_rec_path: str) -> datetime:  # 定義 read_running_rec 函數，讀取運行記錄文件
    """
    讀取最後一次的運行記錄。
    如果文件不存在或內容無效，則返回30天前的日期。
    """  # 函數說明：嘗試讀取運行記錄文件，若失敗返回預設日期
    if not os.path.exists(running_rec_path):  # 如果文件不存在
        with open(running_rec_path, 'w', encoding='utf-8') as f:  # 創建一個空文件
            f.write('')  # 寫入空字串
        return datetime.today() - timedelta(days= 30 )  # 返回30天前的日期
    try:  # 嘗試讀取文件內容
        with open(running_rec_path, 'r', encoding='utf-8') as f:  # 以讀取模式開啟文件
            content = f.read().strip()  # 讀取內容並移除前後空白
            if content:  # 如果內容非空
                last_run_date = pd.to_datetime(content, errors='coerce')  # 轉換為日期格式
                if pd.isnull(last_run_date):  # 如果轉換結果無效
                    return datetime.today() - timedelta(days=30)  # 返回30天前的日期
                return last_run_date  # 返回轉換後的日期
            else:  # 如果內容為空
                return datetime.today() - timedelta(days=30)  # 返回30天前的日期
    except Exception as e:  # 捕捉所有例外
        Log.Log_Error(global_log_file, f"Error reading running record file {running_rec_path}: {e}")  # 記錄錯誤訊息
        return datetime.today() - timedelta(days=30)  # 返回30天前的日期

def process_excel_file(file_path: str, sheet_name: str, data_columns, running_rec: str,
                       output_path: str, fields: dict, site: str, product_family: str,
                       operation: str, Test_Station: str, config: ConfigParser) -> None:  # 定義 process_excel_file 函數，處理 Excel 文件
    """處理 Excel 文件，讀取數據、轉換及生成 XML 文件"""  # 函數說明：根據配置讀取並處理 Excel 數據，最終生成 XML 文件
    Log.Log_Info(global_log_file, f"Processing Excel File: {file_path}")  # 記錄開始處理 Excel 文件的日誌
    Excel_file_list = []  # 初始化一個空列表，用於存儲文件及其修改時間
    for file in glob.glob(file_path):  # 遍歷匹配 file_path 的所有文件
        if '$' not in file:  # 過濾掉文件名稱中含有 '$' 的臨時文件
            dt = datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d %H:%M:%S")  # 取得文件修改時間並格式化
            Excel_file_list.append([file, dt])  # 將文件路徑及修改時間加入列表
    if not Excel_file_list:  # 如果列表為空
        Log.Log_Error(global_log_file, f"Excel file not found: {file_path}")  # 記錄錯誤日誌
        return  # 結束函數執行
    Excel_file_list = sorted(Excel_file_list, key=lambda x: x[1], reverse=True)  # 將文件按修改時間排序（最新的在前）
    Excel_File = Excel_file_list[0][0]  # 取得最新的文件路徑及名稱
    
    try:  # 嘗試讀取 Excel 數據
        # 讀取 Excel 數據，跳過前100行，僅讀取指定列
        df = pd.read_excel(Excel_File, header=None, sheet_name=sheet_name, usecols=data_columns, skiprows=1000)
        df['key_SORTNUMBER'] = df.index + 1000  # 新增一欄 'key_SORTNUMBER'，值為索引加100
# 等待用戶按下 Enter 鍵以繼續
    except Exception as e:  # 如果讀取失敗
        Log.Log_Error(global_log_file, f"Error reading Excel file {file_path}: {e}")  # 記錄錯誤日誌
        return  # 結束函數執行
    
    df.columns = range(df.shape[1])  # 將 DataFrame 欄位重新命名為 0, 1, 2, ... 
    
    df = df.dropna(subset=[2])  # 刪除第一欄為 NaN 的行
 
    if not os.path.exists(output_path):  # 如果輸出目錄不存在
        os.makedirs(output_path)  # 創建輸出目錄
        
    one_month_ago = read_running_rec(running_rec)  # 根據運行記錄獲取30天前的日期
    
    if 'key_Start_Date_Time' in fields:  # 如果配置中包含 key_Start_Date_Time 欄位
        start_date_col = int(fields['key_Start_Date_Time'][0])  # 取得該欄位的列號
        #print(start_date_col,df[start_date_col])  # 輸出該欄位的列號
        running_date = config.get('Basic_info', 'Running_date')  # 從 ini 文件中取得 Running_date 的數值
        one_month_ago = datetime.today() - timedelta(days=int(running_date))  # 計算從今天起提前的日期
        df = df[df[start_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]  # 過濾出日期大於等於 Running_date 的行
        df[start_date_col] = df[start_date_col].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%dT%H.%M.%S'))  # 格式化該欄位的日期
    else:  # 如果配置中未包含該欄位
        Log.Log_Error(global_log_file, "key_Start_Date_Time not found in fields configuration")  # 記錄錯誤日誌
        # Extract values from the DataFrame based on the fields configuration
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
        }
        # 將 extracted_values 中的欄位名稱指定為對應的變數名稱
        # 確保 extracted_values 的值是有效的列索引
        valid_columns = [int(fields[key][0]) for key in extracted_values.keys() if key in fields]
        df1 = df.iloc[:, valid_columns].copy()  # 根據有效列索引複製 DataFrame
        df1.columns = list(extracted_values.keys())  # 將欄位名稱設置為 extracted_values 的鍵
    else:
        Log.Log_Error(global_log_file, "Required fields are missing in the fields configuration")  # 記錄錯誤日誌
        return
    
    df1 = df1.reset_index(drop=True)
    
    # Split the 'key_Serial_Number' column by '/' and generate new rows
    new_rows = []
    for index, row in df1.iterrows():
        serial_numbers = str(row['key_Serial_Number']).split('/')  # Split by '/'
        for serial in serial_numbers:
            serial = serial.strip()  # Remove leading/trailing whitespace
            serial = serial.split()[0]  # Keep only the first part before any whitespace
            if not serial:  # Skip empty serial numbers
                continue
            new_row = row.copy()  # Copy the original row
            new_row['key_Serial_Number'] = serial  # Replace with the split serial number
            new_rows.append(new_row)  # Add the new row to the list

    # Create a new DataFrame with the expanded rows
    df1 = pd.DataFrame(new_rows).reset_index(drop=True)
    df1['Part_Number'] = None  # 初始化 Part_Number 欄位為 None
    df1['Chip_Part_Number'] = None  # 初始化 Chip_Part_Number 欄位為 None
    df1['COB_Part_Number'] = None  # 初始化 COB_Part_Number 欄位為 None
    for index, row in df1.iterrows():  # 遍歷 df1 的每一行
        key_Material_Type = str(row['key_Material_Type'])  # 取得 key_Material_Type 的值
        if key_Material_Type == "XQJ-30150":
            part_number = "XQJ-30150"
            chip_part_number = "1000047352"
            cob_part_number = "1000047353"
        elif key_Material_Type == "XQJ-30115-P":
            part_number = "XQJ-30115-P"
            chip_part_number = "1000034198"
            cob_part_number = "1000034812"
        else:
            part_number = None
            chip_part_number = None
            cob_part_number = None

        # 更新欄位值
        df1.loc[index, 'Part_Number'] = part_number
        df1.loc[index, 'Chip_Part_Number'] = chip_part_number
        df1.loc[index, 'COB_Part_Number'] = cob_part_number
        # 新增對應的欄位並將數值放入 DataFrame
    
    row_end = len(df1)  # 取得 DataFrame 的總行數
    row_number = 0  # 初始化行號為 0

    for index, row in df1.iterrows():
        for column in df1.columns:
            print(f'"{column}": {row[column]}')
        print()  # Add a blank line between rows for better readability
    
    while row_number < row_end:  # 當行號小於總行數時，循環處理每一行
        data_dict = {}  # 初始化空字典以存儲該行數據
        if row_number == row_end - 1:  # 如果是最後一行
            latest_date = pd.to_datetime(df1['key_Start_Date_Time']).max()  # 取 'key_Start_Date_Time' 欄的最大日期
            update_running_rec(running_rec, latest_date)  # 更新運行記錄文件
        Count_A=0
        for key, (col, dtype) in fields.items():  # 遍歷所有配置的字段
            try:  # 嘗試轉換該字段數據
                value = df1.iloc[row_number, int(Count_A)]  # 取得指定行和列的數據
                if dtype == 'float':  # 如果數據類型為 float
                    value = float(value)  # 轉換為 float
                elif dtype == 'str':  # 如果數據類型為 str
                    value = str(value)  # 轉換為字串
                elif dtype == 'int':  # 如果數據類型為 int
                    value = int(value)  # 轉換為整數
                elif dtype == 'bool':  # 如果數據類型為 bool
                    value = bool(value)  # 轉換為布林值
                elif dtype == 'datetime':  # 如果數據類型為 datetime
                    value = pd.to_datetime(value)  # 轉換為日期時間
                else:  # 如果數據類型不支援
                    Log.Log_Error(global_log_file, f"Unsupported data type {dtype} for key {key}")  # 記錄錯誤日誌
                    continue  # 跳過此字段
                data_dict[key] = value  # 將轉換後的值存入字典
            except ValueError as ve:  # 捕捉數值轉換錯誤
                Log.Log_Error(global_log_file, f"ValueError processing field {key}: {ve}")  # 記錄錯誤日誌
                data_dict[key] = None  # 設置該字段為 None
            except Exception as e:  # 捕捉其他例外
                Log.Log_Error(global_log_file, f"Error processing field {key}: {e}")  # 記錄錯誤日誌
                data_dict[key] = None  # 設置該字段為 None
                continue  # 繼續處理下一字段
            Count_A = Count_A + 1  # 行號增加1
            
        print(data_dict)  # 輸出當前行的數據字典至螢幕（使用英文輸出）  
        input("Press Enter to continue...")  # 等待用戶按下 Enter 鍵以繼續
        sort_number_col = int(fields['key_SORTNUMBER'][0])  # 取得 key_SORTNUMBER 對應的列號
        data_dict['key_SORTNUMBER'] = df.loc[row_number, sort_number_col]  # 將該列數存入字典
        data_dict['key_Operation'] = 'AFM_Step_Height'  # 設定操作名稱為 AFM_Step_Height
        try:  # 嘗試進行日期轉換
            dt = datetime.strptime(str(data_dict["key_Start_Date_Time"]).replace('T', ' ').replace('.', ':'), "%Y-%m-%d %H:%M:%S")  # 將日期字串轉換成 datetime 物件
            date_excel_number = int(str(dt - datetime(1899, 12, 30)).split()[0])  # 計算 Excel 日期數值
        except Exception as e:  # 如果轉換失敗
            Log.Log_Error(global_log_file, f"Date conversion error: {e}")  # 記錄錯誤日誌
            date_excel_number = None  # 將日期數值設為 None
        data_dict["key_STARTTIME_SORTED"] = date_excel_number  # 將轉換後的日期數值存入字典
        
        if df.loc[row_number, 'Part_Number'] is not None:  # 如果該行的 Part_Number 不為 None
            data_dict['key_Part_Number'] = df.loc[row_number, 'Part_Number']  # 存入 Part_Number
            data_dict['key_LotNumber_9'] = df.loc[row_number, 'Nine_Serial_Number']  # 存入 Nine_Serial_Number
        else:  # 如果 Part_Number 為 None
            Log.Log_Error(global_log_file, f"{data_dict.get('key_Serial_Number', 'Unknown')} : PartNumber Error")  # 記錄錯誤日誌
            row_number += 1  # 行號增加1
            continue  # 跳過此行
        
        if None in data_dict.values():  # 如果字典中存在 None 值
            Log.Log_Error(global_log_file, f"Skipping row {row_number} due to None values in data_dict")  # 記錄錯誤日誌
        else:  # 如果所有數據均有效
            generate_xml(data_dict, output_path, site, product_family, operation, Test_Station)  # 調用 generate_xml 生成 XML 文件
        
        row_number += 1  # 行號增加1
        Log.Log_Info(global_log_file, "Write the next starting line number")  # 記錄下一行起始號訊息
        Row_Number_Func.next_start_row_number("Ru_AFM_StartROW.txt", row_number)  # 更新起始行號至指定文件

def generate_xml(data_dict: dict, output_path: str, site: str, product_family: str,
                 operation: str, Test_Station: str) -> None:  # 定義 generate_xml 函數，生成 XML 文件
    """生成 XML 文件"""  # 函數說明：根據傳入數據生成 XML 文件
    print(data_dict.get('key_Start_Date_Time', ''))  # 將 key_Start_Date_Time 輸出至螢幕（使用英文輸出）
    xml_filename = (  # 構造 XML 文件名稱
        f"Site={site},ProductFamily={product_family},Operation={operation},"
        f"PartNumber={data_dict.get('key_Part_Number', 'Unknown')},"
        f"SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')},"
        f"Testdate={data_dict.get('key_Start_Date_Time', 'Unknown')}.xml"
    )
    xml_filepath = os.path.join(output_path, xml_filename)  # 構造 XML 文件的完整路徑
    with open(xml_filepath, 'w', encoding='utf-8') as f:  # 以寫入模式開啟 XML 文件
        f.write('<?xml version="1.0" encoding="utf-8"?>\n')  # 寫入 XML 聲明
        f.write('<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n')  # 寫入根元素開始標籤
        f.write(f'    <Result startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Result="Passed">\n')  # 寫入 Result 元素及屬性
        f.write(f'        <Header SerialNumber="{data_dict["key_Serial_Number"]}" PartNumber="{data_dict["key_Part_Number"]}" Operation="{operation}" TestStation="{Test_Station}" Operator="{data_dict["key_Operator"]}" StartTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Site="{site}" LotNumber="{data_dict["key_Serial_Number"]}"/>\n')  # 寫入 Header 元素，包含各項屬性
        f.write('        <HeaderMisc>\n')  # 寫入 HeaderMisc 元素開始標籤
        f.write('            <Item Description="AFM_Step_Height"></Item>\n')  # 寫入 Item 元素，描述操作名稱
        f.write('        </HeaderMisc>\n')  # 寫入 HeaderMisc 元素結束標籤
        f.write(f'        <TestStep Name="{data_dict["key_Operation"]}" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')  # 寫入第一個 TestStep 元素及其屬性
        for key in ["key_Ah_L1", "key_Ah_L2", "key_Ah_R1", "key_Ah_R2", 
                    "key_Da_L1", "key_Da_L2", "key_Da_R1", "key_Da_R2", 
                    "key_Dh_L1", "key_Dh_L2", "key_Dh_R1", "key_Dh_R2"]:  # 遍歷所有需要輸出的數據鍵
            f.write(f'            <Data DataType="Numeric" Name="{key.split("_")[1]}_{key.split("_")[2]}" Units="um" Value="{data_dict[key]}"/>\n')  # 寫入 Data 元素，設定數據類型、名稱、單位和值
        f.write('        </TestStep>\n')  # 寫入第一個 TestStep 元素結束標籤
        f.write(f'        <TestStep Name="SORTED_DATA" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Status="Passed">\n')  # 寫入第二個 TestStep 元素及屬性
        f.write(f'            <Data DataType="Numeric" Name="STARTTIME_SORTED" Units="" Value="{data_dict["key_STARTTIME_SORTED"]}"/>\n')  # 寫入 Data 元素，顯示排序時間
        f.write(f'            <Data DataType="Numeric" Name="SORTNUMBER" Units="" Value="{data_dict["key_SORTNUMBER"]}"/>\n')  # 寫入 Data 元素，顯示排序編號
        f.write(f'            <Data DataType="String" Name="LotNumber_5" Value="{data_dict["key_Serial_Number"]}" CompOperation="LOG"/>\n')  # 寫入 Data 元素，顯示批號（LotNumber_5）
        f.write(f'            <Data DataType="String" Name="LotNumber_9" Value="{data_dict["key_LotNumber_9"]}" CompOperation="LOG"/>\n')  # 寫入 Data 元素，顯示批號（LotNumber_9）
        f.write('        </TestStep>\n')  # 寫入第二個 TestStep 元素結束標籤
        f.write('        <TestEquipment>\n')  # 寫入 TestEquipment 元素開始標籤
        f.write(f'            <Item DeviceName="MOCVD" DeviceSerialNumber="{data_dict["key_Equipment"]}"></Item>\n')  # 寫入 Item 元素，顯示設備資訊
        f.write('        </TestEquipment>\n')  # 寫入 TestEquipment 元素結束標籤
        f.write('    </Result>\n')  # 寫入 Result 元素結束標籤
        f.write('</Results>\n')  # 寫入根元素結束標籤
    Log.Log_Info(global_log_file, f"XML File Created: {xml_filepath}")  # 記錄 XML 文件創建成功訊息

def process_ini_file(config_path: str) -> None:  # 定義 process_ini_file 函數，處理 .ini 配置文件
    """讀取指定的 .ini 文件，並執行 Excel 與 XML 的處理"""  # 函數說明：根據配置文件執行相關處理
    global global_log_file  # 使用全局變數 global_log_file
    config = ConfigParser()  # 創建 ConfigParser 物件以解析配置文件
    try:  # 嘗試讀取配置文件
        with open(config_path, 'r', encoding='utf-8') as config_file:  # 以讀取模式開啟 .ini 文件
            config.read_file(line for line in config_file if not line.strip().startswith('#'))  # 讀取文件，跳過以 '#' 開頭的註解行
    except Exception as e:  # 捕捉所有例外
        Log.Log_Error(global_log_file, f"Error reading config file {config_path}: {e}")  # 記錄讀取配置文件時的錯誤
        return  # 結束函數執行

    try:  # 嘗試從配置文件中獲取各項配置
        input_paths = [path.strip() for path in config.get('Paths', 'input_paths').splitlines() if path.strip() and not path.strip().startswith('#')]  # 取得輸入路徑列表，過濾掉空行和註解行
        output_path = config.get('Paths', 'output_path')  # 取得輸出路徑
        running_rec = config.get('Paths', 'running_rec')  # 取得運行記錄文件路徑
        sheet_name = config.get('Excel', 'sheet_name')  # 取得 Excel 工作表名稱
        data_columns = config.get('Excel', 'data_columns')  # 取得需要讀取的數據列
        log_path = config.get('Logging', 'log_path')  # 取得日誌存放路徑
        fields_config = [field.strip() for field in config.get('DataFields', 'fields').splitlines() if field.strip()]  # 取得數據字段配置，過濾掉空行
        site = config.get('Basic_info', 'Site')  # 取得站點資訊
        product_family = config.get('Basic_info', 'ProductFamily')  # 取得產品系列資訊
        operation = config.get('Basic_info', 'Operation')  # 取得操作名稱
        Test_Station = config.get('Basic_info', 'TestStation')  # 取得測試站資訊
        file_name_pattern = config.get('Basic_info', 'file_name_pattern')  # 取得文件名稱匹配模式

    except NoSectionError as e:  # 如果配置中缺少某個區段
        Log.Log_Error(global_log_file, f"Missing section in config file {config_path}: {e}")  # 記錄錯誤日誌
        return  # 結束函數執行
    except NoOptionError as e:  # 如果配置中缺少某個選項
        Log.Log_Error(global_log_file, f"Missing option in config file {config_path}: {e}")  # 記錄錯誤日誌
        return  # 結束函數執行

    log_folder_name = str(datetime.today().date())  # 以今日日期作為日誌資料夾名稱
    log_folder_path = os.path.join(log_path, log_folder_name)  # 構造日誌資料夾路徑
    if not os.path.exists(log_folder_path):  # 如果資料夾不存在
        os.makedirs(log_folder_path)  # 創建日誌資料夾
    log_file = os.path.join(log_folder_path, '043_LD-SPUT.log')  # 構造日誌文件完整路徑
    global_log_file = log_file  # 更新全局變數 global_log_file
    setup_logging(global_log_file)  # 調用 setup_logging 設定日誌
    Log.Log_Info(log_file, f"Program Start for config {config_path}")  # 記錄程式啟動訊息

    fields = {}  # 初始化字段配置字典
    for field in fields_config:  # 遍歷每一行字段配置
        if field.strip():  # 如果該行不為空
            key, col, dtype = field.split(':')  # 分割該行以取得 key、列號與數據類型
            fields[key.strip()] = (col.strip(), dtype.strip())  # 將配置存入字典

    for input_path in input_paths:  # 遍歷所有輸入路徑
        print(input_path)  # 輸出當前處理的輸入路徑,
        files = glob.glob(os.path.join(input_path, file_name_pattern))  # 根據匹配模式獲取文件列表
        files = [file for file in files if not os.path.basename(file).startswith('~$')]  # 過濾臨時文件
        if not files:  # 如果未找到任何文件
            Log.Log_Error(global_log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")  # 記錄錯誤日誌
        for file in files:  # 遍歷每個匹配的文件
            if not os.path.basename(file).startswith('~$'):  # 如果文件名稱不以 '~$' 開頭
                destination_dir = '../DataFile/047/TAK_SPC/'  # 設定目標目錄
                if not os.path.exists(destination_dir):  # 如果目標目錄不存在
                    os.makedirs(destination_dir)  # 創建目標目錄
                shutil.copy(file, destination_dir)  # 複製文件到目標目錄
                Log.Log_Info(global_log_file, f"Copy excel file {file} to ../DataFile/047_TAK_SPC/")  # 記錄文件複製訊息
                copied_file_path = os.path.join(destination_dir, os.path.basename(file))  # 構造複製後的文件完整路徑
                process_excel_file(copied_file_path, sheet_name, data_columns, running_rec,
                                   output_path, fields, site, product_family, operation, Test_Station, config)  # 處理該 Excel 文件

def main() -> None:  # 定義主函數 main
    """掃描所有 .ini 文件並執行處理"""  # 函數說明：遍歷當前目錄下所有 .ini 文件，並根據配置進行處理
    ini_files = glob.glob("*.ini")  # 獲取當前目錄下所有 .ini 文件列表
    for ini_file in ini_files:  # 遍歷每個 .ini 文件
        process_ini_file(ini_file)  # 處理該 .ini 文件

if __name__ == '__main__':  # 如果此模組作為主程式執行
    main()  # 調用主函數 main
    Log.Log_Info(global_log_file, "Program End")  # 記錄程式結束訊息
