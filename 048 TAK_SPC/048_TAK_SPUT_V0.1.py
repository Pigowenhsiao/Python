#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
功能:
1. 讀取所有 .ini 設定檔，根據設定處理 Excel 檔案資料，並產生對應的 XML 檔案。
2. 執行紀錄與錯誤日誌由標準 logging 模組輸出。

優化重點:
- 採用 Code 2 的現代化架構，移除全域變數。
- 使用 Pandas 向量化操作取代 iterrows() 迴圈，大幅提升資料處理效率。
- 使用標準 xml.etree.ElementTree 模組生成 XML，提高穩定性與可讀性。
- 函式職責分離，結構更清晰，易於維護。
- 全面使用 f-string 和 Type Hints。
"""

import os
import sys
import glob
import shutil
import logging
import random
from configparser import ConfigParser, NoSectionError, NoOptionError
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any

import pandas as pd
import numpy as np # 引入 numpy 以使用 np.select 進行高效的條件賦值
from xml.dom import minidom
import xml.etree.ElementTree as ET

# ---------------------------------------------------------------------------
# 1. 組態與日誌設定 (Configuration and Logging)
# ---------------------------------------------------------------------------

def setup_logging(log_file_path: str) -> None:
    """為每次 INI 處理流程獨立設定日誌。"""
    # 移除現有的 handlers，確保日誌不會重複輸出
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

def parse_fields_config(fields_raw: List[str]) -> Dict[str, Tuple[str, str]]:
    """解析來自 INI 的 DataFields 設定。"""
    fields_cfg = {}
    for line in fields_raw:
        if ':' in line:
            key, col, dtype = (s.strip() for s in line.split(':', 2))
            fields_cfg[key] = (col, dtype)
    return fields_cfg


# ---------------------------------------------------------------------------
# 2. 執行紀錄檔案處理 (Running Record File Handling)
# ---------------------------------------------------------------------------

def read_or_initialize_running_rec(rec_path: str, default_days_ago: int = 30) -> datetime:
    """
    讀取上次執行的紀錄時間。若檔案不存在、為空或格式錯誤，
    則回傳一個預設的起始時間（例如30天前）。
    """
    default_start_time = datetime.now() - timedelta(days=default_days_ago)
    if not os.path.exists(rec_path):
        logging.warning(f"Running record file not found: {rec_path}. Creating it and using default start time.")
        with open(rec_path, 'w', encoding='utf-8') as f:
            f.write('') # 建立空檔案
        return default_start_time

    try:
        with open(rec_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                logging.info("Running record file is empty. Using default start time.")
                return default_start_time
            
            last_run_date = pd.to_datetime(content, errors='coerce')
            if pd.isnull(last_run_date):
                logging.warning(f"Invalid date format in {rec_path}. Using default start time.")
                return default_start_time
            
            return last_run_date
    except Exception as e:
        logging.error(f"Error reading running record file {rec_path}: {e}. Using default start time.")
        return default_start_time

# ---------------------------------------------------------------------------
# 3. 核心資料處理流程 (Main Data Processing Workflow)
# ---------------------------------------------------------------------------

def process_data(
    excel_path: str,
    config: ConfigParser,
    fields: Dict[str, Tuple[str, str]],
) -> None:
    """
    根據設定檔，讀取並處理單一 Excel 檔案，最終生成 CSV 和 XML。
    """
    # 從 config 中提取所需參數
    sheet_name = config.get('Excel', 'sheet_name')
    data_columns = config.get('Excel', 'data_columns')
    output_path = config.get('Paths', 'output_path')
    csv_path = config.get('Paths', 'CSV_path')
    site = config.get('Basic_info', 'Site')
    product_family = config.get('Basic_info', 'ProductFamily')
    operation = config.get('Basic_info', 'Operation')
    test_station = config.get('Basic_info', 'TestStation')

    logging.info(f"Starting to process Excel file: {excel_path}")

    # --- 3.1 讀取與初步清理 Excel ---
    try:
        df = pd.read_excel(
            excel_path,
            header=None,
            sheet_name=sheet_name,
            usecols=data_columns,
            skiprows=1000,
        )
        # 原始邏輯：添加原始索引作為排序號
        df['key_SORTNUMBER'] = df.index + 1000
    except Exception as e:
        logging.error(f"Failed to read Excel file {excel_path}: {e}")
        return

    # 原始邏輯：重設欄位名並移除第3欄為空的資料行
    df.columns = range(df.shape[1])
    df.dropna(subset=[2], inplace=True)

    if df.empty:
        logging.warning(f"No valid data rows found in {excel_path} after initial cleaning.")
        return

    # --- 3.2 根據 fields 設定，選取並命名欄位 ---
    col_map = {int(v[0]): k for k, v in fields.items()}
    # 加上自動產生的 SORTNUMBER 欄位
    sort_number_col_index = df.shape[1] - 1
    col_map[sort_number_col_index] = 'key_SORTNUMBER'

    # 檢查所需欄位是否存在
    missing_cols = [idx for idx in col_map if idx >= df.shape[1]]
    if missing_cols:
        logging.error(f"Columns defined in INI are out of bounds in Excel: {missing_cols}")
        return

    df_selected = df[list(col_map.keys())].copy()
    df_selected.rename(columns=col_map, inplace=True)
    
    # --- 3.3 日期過濾 ---
    if 'key_Start_Date_Time' in df_selected.columns:
        running_date_days = config.getint('Basic_info', 'Running_date')
        cutoff_date = datetime.now() - timedelta(days=running_date_days)
        
        # 轉換日期欄位，錯誤的格式會變成 NaT (Not a Time)
        start_dates = pd.to_datetime(df_selected['key_Start_Date_Time'], errors='coerce')
        
        # 過濾掉無效日期和早於截止日期的資料
        original_rows = len(df_selected)
        df_selected = df_selected[start_dates >= cutoff_date].copy()
        logging.info(f"Filtered {original_rows - len(df_selected)} rows based on Start_Date_Time < {cutoff_date.strftime('%Y-%m-%d')}.")
        
        if df_selected.empty:
            logging.warning("No data remains after date filtering.")
            return
            
        # 格式化日期欄位
        df_selected['key_Start_Date_Time'] = pd.to_datetime(df_selected['key_Start_Date_Time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        logging.error("key_Start_Date_Time not found in fields configuration. Cannot perform date filtering.")
        return

    # --- 3.4 核心轉換：向量化處理 (取代 iterrows) ---
    
    # a. 序號分割 (Serial Number Splitting)
    # 將 'A/B' 格式的字串分割並擴展成多行
    df_exploded = df_selected.copy()
    df_exploded['key_Serial_Number'] = df_exploded['key_Serial_Number'].astype(str).str.split('/')
    df_exploded = df_exploded.explode('key_Serial_Number').reset_index(drop=True)
    
    # 清理每個序號的前後空白，並只取空白前的部分
    df_exploded['key_Serial_Number'] = df_exploded['key_Serial_Number'].str.strip().str.split().str[0]
    df_exploded.dropna(subset=['key_Serial_Number'], inplace=True)
    df_exploded = df_exploded[df_exploded['key_Serial_Number'] != '']

    # b. 根據 Material_Type 賦值 (Part Number Generation)
    conditions = [
        df_exploded['key_Material_Type'].astype(str).str.contains("QJ-30150", na=False),
        df_exploded['key_Material_Type'].astype(str).str.contains("QJ-30115", na=False)
    ]
    pn_choices = ["XQJ-30150", "XQJ-30115-P"]
    chip_pn_choices = ["1000047352A", "1000034198A"]
    cob_pn_choices = ["1000047353A", "1000034812A"]

    df_exploded['Part_Number'] = np.select(conditions, pn_choices, default=None)
    df_exploded['Chip_Part_Number'] = np.select(conditions, chip_pn_choices, default=None)
    df_exploded['COB_Part_Number'] = np.select(conditions, cob_pn_choices, default=None)
    
    # 移除無法匹配 Part_Number 的行
    df_final = df_exploded.dropna(subset=['Part_Number']).reset_index(drop=True)

    if df_final.empty:
        logging.warning("No data remains after assigning Part Numbers.")
        return

    # --- 3.5 最終清理與格式化 ---
    # 統一重新命名欄位
    df_final.rename(columns={
        'key_Start_Date_Time': 'Start_Date_Time',
        'key_END_Date_Time': 'End_Date_Time',
        'key_Operator1': 'Operator',
        'key_Serial_Number': 'Serial_Number',
        'key_Material_Type': 'Material_Type',
        'key_Coating_Type': 'Coating_Type',
        'key_Reflectivity': 'Reflectivity',
        'key_SORTNUMBER': 'SORTNUMBER'
    }, inplace=True)
    
    # 格式化日期/時間欄位
    df_final['Start_Date_Time'] = pd.to_datetime(df_final['Start_Date_Time'], errors='coerce').dt.strftime('%Y/%m/%d %H:%M:%S')
    df_final['End_Date_Time'] = pd.to_datetime(df_final['End_Date_Time'], errors='coerce').dt.strftime('%Y/%m/%d %H:%M:%S')

    # 清理 Material_Type 中的換行符
    df_final['Material_Type'] = df_final['Material_Type'].astype(str).str.replace(r'[\r\n]+', '', regex=True)

    # 添加從 INI 讀取的固定值欄位
    df_final['CVD_Tool'] = config.get('Basic_info', 'CVD_Tool')
    
    # 移除不再需要的 'key_Operator2' 欄位（如果存在）
    if 'key_Operator2' in df_final.columns:
        df_final.drop(columns=['key_Operator2'], inplace=True)

    # --- 3.6 儲存 CSV 並生成 XML ---
    timestamp = datetime.now().strftime("%Y%m%d%H%M") + f"{random.randint(0, 59):02}"
    
    # 儲存 CSV
    os.makedirs(csv_path, exist_ok=True)
    csv_filename = f"TAK_SPC_{timestamp}.csv"
    csv_output_path = os.path.join(csv_path, csv_filename)
    df_final.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
    logging.info(f"CSV file saved at: {csv_output_path}")

    # 生成 XML
    generate_xml(
        output_path=output_path,
        site=site,
        product_family=product_family,
        operation=operation,
        test_station=test_station,
        timestamp=timestamp,
        csv_output_path=csv_output_path
    )
    logging.info(f"Successfully processed and generated outputs for {excel_path}")


# ---------------------------------------------------------------------------
# 4. XML 檔案生成 (XML Generation)
# ---------------------------------------------------------------------------

def generate_xml(
    output_path: str, site: str, product_family: str, operation: str,
    test_station: str, timestamp: str, csv_output_path: str
) -> None:
    """使用 ElementTree 生成標準格式的 XML 檔案。"""
    os.makedirs(output_path, exist_ok=True)
    
    # 產生與原始碼邏輯一致的時間戳
    now = datetime.now()
    sec = f"{random.randint(0, 59):02}"
    now_iso = now.strftime(f'%Y-%m-%dT%H:%M:{sec}')
    
    # PartNumber 和 SerialNumber 在 XML 中有固定填寫邏輯
    part_number_xml = 'UNKNOWPN'
    serial_number_xml = timestamp

    xml_filename = (
        f"Site={site},ProductFamily={product_family},Operation={operation},"
        f"Partnumber={part_number_xml},Serialnumber={serial_number_xml},"
        f"Testdate={now_iso}.xml"
    ).replace(':', '.').replace('/', '-').replace('\\', '-')
    
    xml_filepath = os.path.join(output_path, xml_filename)

    # 使用 ElementTree 建立 XML 結構
    results = ET.Element("Results", {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
    })
    result = ET.SubElement(results, "Result", startDateTime=now_iso, endDateTime=now_iso, Result="Passed")
    ET.SubElement(result, "Header", 
                  SerialNumber=serial_number_xml, PartNumber=part_number_xml, Operation=operation,
                  TestStation=test_station, Operator="NA", StartTime=now_iso, Site=site,
                  LotNumber="", Quantity="")
    header_misc = ET.SubElement(result, "HeaderMisc")
    ET.SubElement(header_misc, "Item", Description="")
    test_step = ET.SubElement(result, "TestStep", Name=operation, startDateTime=now_iso, endDateTime=now_iso, Status="Passed")
    ET.SubElement(test_step, "Data", DataType="Table", Name=f"tbl_{operation.upper()}", Value=csv_output_path, CompOperation="LOG")

    # 美化格式並寫入檔案
    xml_str = minidom.parseString(ET.tostring(results)).toprettyxml(indent="    ", encoding="utf-8")
    with open(xml_filepath, "wb") as f:
        f.write(xml_str)
    
    logging.info(f"XML file created: {xml_filepath}")

# ---------------------------------------------------------------------------
# 5. 主程式入口 (Main Program Entry)
# ---------------------------------------------------------------------------

def process_ini_file(config_path: str) -> None:
    """處理單一 INI 設定檔，並觸發後續所有流程。"""
    config = ConfigParser()
    try:
        # 讀取 INI，跳過註解行
        with open(config_path, 'r', encoding='utf-8') as f:
            config.read_file(line for line in f if not line.strip().startswith('#'))
            
        # 讀取所有必要設定
        log_path = config.get('Logging', 'log_path')
        input_paths = [p.strip() for p in config.get('Paths', 'input_paths').splitlines() if p.strip()]
        copy_dest_path = config.get('Paths', 'copy_destination_path')
        file_pattern = config.get('Basic_info', 'file_name_pattern')
        fields_raw = config.get('DataFields', 'fields').splitlines()

    except (NoSectionError, NoOptionError, FileNotFoundError) as e:
        print(f"Error reading configuration from {config_path}: {e}")
        return

    # 設定日誌
    log_folder_name = datetime.today().strftime('%Y-%m-%d')
    # 檔名從 INI 名稱來，更具識別性
    ini_name = os.path.splitext(os.path.basename(config_path))[0]
    log_file = os.path.join(log_path, log_folder_name, f'{ini_name}.log')
    setup_logging(log_file)
    
    logging.info(f"--- Program Start for config: {config_path} ---")

    fields = parse_fields_config(fields_raw)

    for input_path in input_paths:
        search_pattern = os.path.join(input_path, file_pattern)
        excel_files_found = glob.glob(search_pattern)
        
        # 過濾掉 Excel 暫存檔
        excel_files = [f for f in excel_files_found if not os.path.basename(f).startswith('~$')]

        if not excel_files:
            logging.error(f"No Excel files found in {input_path} with pattern {file_pattern}")
            continue

        # 複製檔案到目的地
        os.makedirs(copy_dest_path, exist_ok=True)
        for file_path in excel_files:
            try:
                copied_file_path = shutil.copy(file_path, copy_dest_path)
                logging.info(f"Copied file {file_path} to {copied_file_path}")
                
                # 處理複製後的檔案
                process_data(copied_file_path, config, fields)

            except Exception as e:
                logging.error(f"Failed to copy or process file {file_path}: {e}")

    logging.info(f"--- Program End for config: {config_path} ---\n")


def main() -> None:
    """程式主函式，掃描並處理當前目錄下所有的 .ini 檔案。"""
    ini_files = glob.glob("*.ini")
    if not ini_files:
        print("No .ini configuration files found in the current directory.")
        return
        
    for ini_file in ini_files:
        process_ini_file(ini_file)

if __name__ == '__main__':
    main()

