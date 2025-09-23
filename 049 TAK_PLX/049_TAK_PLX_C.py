好的，完全理解。

這個要求很合理，可以將「給開發者看的註解」和「程式執行時給使用者看的訊息」兩者分開。

我將修改上一版的程式碼，**僅將 `logging` 和 `print` 函數中的訊息改回英文**，同時**完整保留為了程式碼交接所寫的繁體中文註解**。

這樣一來，程式碼本身的可讀性（中文註解）和執行日誌的標準化（英文輸出）都能兼顧。

以下是修改後的最終版本：

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Modified on 2025-07-09
依據 Config_TAK_PLX.ini 最新欄位定義（移除 Material_Type / Coating_Type / Reflectivity，改用單一 Operator，支援
Serial_Number_a/b/c 以及 AssignRate_a/b/c，並以 Excel 區段 Data_Row 動態決定 skiprows）
"""

import os
import sys
import glob
import shutil
import logging
import random
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timedelta
from configparser import ConfigParser, NoSectionError, NoOptionError
from typing import List, Dict, Any
import pandas as pd

# ---------------------------------------------------------------------------
# 公用函式
# ---------------------------------------------------------------------------

def setup_logging(log_file_path: str) -> None:
    """
    設定 log 格式與檔案位置。
    每次呼叫此函式時，會先清除既有的 logging handlers，以確保日誌設定能被刷新。
    """
    # 為了能在迴圈中為每個 INI 檔重新設定日誌，需要先移除舊的 handler
    # 取得 logging 模組的根記錄器
    root_logger = logging.getLogger()
    # 移除所有已附加的 handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 建立日誌檔所在的目錄，如果不存在的話
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    # 設定 logging 的基本組態
    logging.basicConfig(
        filename=log_file_path,                    # Log 檔案路徑
        level=logging.DEBUG,                       # 記錄等級為 DEBUG (最低)
        format='%(asctime)s - %(levelname)s - %(message)s', # Log 格式
    )

# ---------------------------------------------------------------------------
# 主要資料處理流程
# ---------------------------------------------------------------------------

def process_excel_file(
    excel_file: str,
    sheet_name: str,
    data_columns: str,
    data_row: int,
    output_path: str,
    csv_base_path: str,
    pl_tool: str,
    fields_cfg: Dict[str, tuple[str, str]],
    site: str,
    product_family: str,
    operation: str,
    test_station: str,
    running_date: int,
) -> None:
    """
    核心處理函式：依設定讀取 Excel、清理與轉換資料、轉存 CSV，並產生對應的 XML。
    """

    logging.info(f"Processing Excel file: {excel_file}")

    # -------------------------------------------------------------------
    # 1) 讀取 Excel
    # -------------------------------------------------------------------
    try:
        # data_row 是以 1 為基底的行號，pandas 的 skiprows 是以 0 為基底，故需減 1
        skiprows = data_row - 1
        # 使用 pandas 讀取 Excel 檔案
        df = pd.read_excel(
            excel_file,
            header=None,           # 檔案沒有標頭行
            sheet_name=sheet_name,   # 讀取指定的工作表
            usecols=data_columns,  # 只讀取 ini 中定義的欄位
            skiprows=skiprows,     # 跳過資料起始行之前的行
        )
        # 新增一個 'key_SORTNUMBER' 欄位，記錄原始 Excel 中的行號，方便後續追蹤
        df["key_SORTNUMBER"] = df.index + data_row
    except Exception as e:
        logging.error(f"Error reading {excel_file}: {e}")
        return # 發生錯誤，中斷此檔案的處理

    # -------------------------------------------------------------------
    # 1.1) 篩選與格式化 Part_Number
    # -------------------------------------------------------------------
    # 根據需求，搜尋 Part_Number 欄位 (column 2) 中的 'QJ'
    # 並只保留找到 'QJ' 的資料列，且將該欄位內容更新為從 'QJ' 開始的 8 個字元。

    # 確保第 3 欄（索引為 2）是字串格式，方便進行字串處理
    df[2] = df[2].astype(str)

    # 使用正規表示式尋找並擷取 'QJ' 開頭的 8 個字元
    # .str.extract 會在找不到時返回 NaN (Not a Number)
    df[2] = df[2].str.extract(r'(QJ.{6})', expand=False)

    # 移除 Part_Number 欄位為 NaN 的資料列 (代表沒找到 'QJ' 或格式不符)
    original_rows_before_qj_filter = len(df)
    df.dropna(subset=[2], inplace=True) # inplace=True 表示直接在原 DataFrame 上修改
    dropped_rows = original_rows_before_qj_filter - len(df)
    if dropped_rows > 0:
        logging.info(
            f"Dropped {dropped_rows} rows from {os.path.basename(excel_file)} because a valid "
            f"Part_Number starting with 'QJ' was not found."
        )

    # 如果篩選後沒有任何資料，則記錄日誌並跳過此檔案
    if df.empty:
        logging.info(f"No valid data rows left in {excel_file} after filtering for 'QJ' Part_Number. Skipping file.")
        return

    # -------------------------------------------------------------------
    # 2) 動態欄位對應
    # -------------------------------------------------------------------
    # 將從 ini 解析而來的欄位設定（字串）轉為整數索引位置
    fields: Dict[str, tuple[int, str]] = {
        k: (int(v[0]), v[1]) for k, v in fields_cfg.items()
    }
    # 將 SORTNUMBER 也加入到欄位對應中
    if "key_SORTNUMBER" not in fields:
        fields["key_SORTNUMBER"] = (df.shape[1] - 1, "int") # df.shape[1] - 1 代表最後一欄

    # 動態選取所有在 ini 中定義的欄位，建立新的 DataFrame
    all_field_keys = list(fields.keys())
    col_idx = [fields[k][0] for k in all_field_keys] # 取得所有欄位的索引
    df1 = df.iloc[:, col_idx].copy() # 使用 .iloc 依索引選取欄位
    df1.columns = all_field_keys    # 將新 DataFrame 的欄位名稱設定為 ini 中定義的 key

    # -------------------------------------------------------------------
    # 3) 資料型別驗證與清理
    # -------------------------------------------------------------------
    # 根據 INI 中定義的型別，驗證資料。對於數值型別，不符合的將被轉換為 NaN。
    numeric_cols_to_check = []
    for col_name in df1.columns:
        if col_name in fields:
            dtype_str = fields[col_name][1]
            # 如果欄位型別被定義為 'int' 或 'float'，或是 AssignRate 相關欄位，則進行數值轉換
            if dtype_str in ['int', 'float'] or col_name.startswith("key_assingrate_"):
                # pd.to_numeric 會嘗試將值轉為數字，errors='coerce' 會讓無法轉換的值變成 NaN
                df1[col_name] = pd.to_numeric(df1[col_name], errors='coerce')
                numeric_cols_to_check.append(col_name)

    # 移除在數值欄位中驗證失敗的資料列 (即含有 NaN 的列)
    if numeric_cols_to_check:
        original_rows = len(df1)
        df1.dropna(subset=numeric_cols_to_check, inplace=True)
        dropped_rows = original_rows - len(df1)
        if dropped_rows > 0:
            logging.info(
                f"Dropped {dropped_rows} rows from {os.path.basename(excel_file)} due to non-numeric "
                f"values in columns: {', '.join(numeric_cols_to_check)}"
            )

    # 如果清理後已無資料，則跳過此檔案
    if df1.empty:
        logging.info(f"No data left for {excel_file} after type validation. Skipping file.")
        return

    # 依需求，處理 Serial_Number 欄位，只保留 '(' 前的內容
    serial_cols = [col for col in df1.columns if col.startswith("key_Serial_Number_")]
    for col in serial_cols:
        # 使用 .str.split() 進行切割，並取第一個元素
        df1[col] = df1[col].str.split('(', n=1).str[0].fillna(df1[col])

    # -------------------------------------------------------------------
    # 4) 拆解 Serial_Number / AssignRate (寬表轉長表)
    # -------------------------------------------------------------------
    # 找出所有 Serial_Number 和 AssignRate 相關的欄位
    serial_cols = sorted([k for k in df1.columns if k.startswith("key_Serial_Number_")])
    assign_cols = sorted([k for k in df1.columns if k.startswith("key_assingrate_")])

    # 除了上述欄位外的所有欄位，視為共用欄位
    common_cols = [
        k for k in df1.columns
        if not k.startswith("key_Serial_Number_") and not k.startswith("key_assingrate_")
    ]

    # 遍歷每一行資料，將其從寬表格式轉換為長表格式
    melted_rows: List[Dict[str, Any]] = []
    for _, row in df1.iterrows():
        # 建立一個包含所有共用欄位資料的基礎紀錄，並移除欄位名稱的 'key_' 前綴
        common_data = {key.replace("key_", "", 1): row[key] for key in common_cols}

        # 遍歷 a, b, c... 等 Serial_Number 欄位
        for i, serial_key in enumerate(serial_cols):
            serial_val = row.get(serial_key)
            # 如果 Serial Number 為空或無效，則跳過
            if pd.isna(serial_val) or str(serial_val).strip() == "":
                continue

            # 建立一筆新的長表紀錄
            record = common_data.copy()
            record["Serial_Number"] = str(serial_val).strip()

            # 增加 Location 欄位，用來標示原始來源 (a, b, c)
            location_suffix = serial_key.split('_')[-1]
            record["Location"] = location_suffix.upper()

            # 如果有對應的 AssignRate 欄位，也加入到紀錄中
            if i < len(assign_cols):
                assign_key = assign_cols[i]
                assign_val = row.get(assign_key)
                record["AssignRate"] = assign_val
            
            melted_rows.append(record)

    # 如果轉換後沒有任何有效的紀錄，則跳過此檔案
    if not melted_rows:
        logging.info(f"No valid serial numbers found in {excel_file} to melt. Skipping file.")
        return

    # 將轉換後的長表資料 list of dicts 轉回 DataFrame
    df_final = pd.DataFrame(melted_rows)

    # -------------------------------------------------------------------
    # 4.1) 日期過濾
    # -------------------------------------------------------------------
    # 先將 Start_Date_Time 轉換為 datetime 物件以進行比較
    df_final["Start_Date_Time"] = pd.to_datetime(df_final["Start_Date_Time"], format="%Y/%m/%d %H:%M:%S", errors="coerce")

    # 移除無法成功轉換的日期 (結果會是 NaT - Not a Time)
    df_final.dropna(subset=["Start_Date_Time"], inplace=True)

    # 根據 ini 中的 running_date 設定進行日期過濾
    if running_date > 0:
        # 計算截止日期
        cutoff_date = datetime.now() - timedelta(days=running_date)
        # 只保留大於等於截止日期的資料
        df_final = df_final[df_final["Start_Date_Time"].dt.date >= cutoff_date.date()]

    # 檢查過濾後是否還有資料
    if df_final.empty:
        logging.info(f"No data left for {excel_file} after date filtering. Skipping file.")
        return

    # -------------------------------------------------------------------
    # 4.2) 最終格式化
    # -------------------------------------------------------------------
    # 將日期格式化回指定的字串格式
    df_final["Start_Date_Time"] = df_final["Start_Date_Time"].dt.strftime("%Y/%m/%d %H:%M:%S")
    # 增加 PL_Tool 固定欄位
    df_final["PL_Tool"] = pl_tool

    # -------------------------------------------------------------------
    # 5) 輸出 CSV
    # -------------------------------------------------------------------
    # 產生一個包含隨機數的唯一時間戳，避免檔名重複
    ts = datetime.now().strftime("%Y%m%d%H%M") + f"{random.randint(0,60):02}"
    csv_name = f"TAK_PLX_{ts}.csv"
    csv_path = os.path.join(csv_base_path, csv_name)
    # 確保 CSV 輸出目錄存在
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    # 將最終的 DataFrame 寫入 CSV 檔案
    df_final.to_csv(csv_path, index=False, encoding="utf-8-sig") # index=False 不寫入索引欄
    logging.info(f"CSV saved: {csv_path}")

    # -------------------------------------------------------------------
    # 6) 產生 XML
    # -------------------------------------------------------------------
    # 呼叫 XML 生成函式
    generate_xml(output_path, site, product_family, operation, test_station, ts, csv_path)


# ---------------------------------------------------------------------------
# XML 產製
# ---------------------------------------------------------------------------

def generate_xml(
    output_path: str,
    site: str,
    product_family: str,
    operation: str,
    test_station: str,
    serial_no: str, # 這裡的 serial_no 來自上面產生的時間戳 ts
    csv_path: str,
) -> None:
    """根據傳入的參數，產生標準格式的 XML 檔案。"""
    # 確保 XML 輸出目錄存在
    os.makedirs(output_path, exist_ok=True)
    # 獲取當前時間的 ISO 格式字串
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    # 組合 XML 檔名，並將檔名中的 ':' 替換為 '.' 以符合某些系統規範
    xml_file = os.path.join(
        output_path,
        f"Site={site},ProductFamily={product_family},Operation={operation},Serialnumber={serial_no},Testdate={now_iso}.xml".replace(":", ".")
    )

    # 使用 xml.etree.ElementTree 建立 XML 結構
    # 根節點
    results = ET.Element("Results")
    # 子節點 Result
    result = ET.SubElement(
        results, "Result", startDateTime=now_iso, endDateTime=now_iso, Result="Passed"
    )
    # Header 節點
    ET.SubElement(
        result,
        "Header",
        SerialNumber=serial_no,
        PartNumber="UNKNOWNPN",
        Operation=operation,
        TestStation=test_station,
        Operator="NA",
        Site=site,
    )
    # HeaderMisc 節點
    header_misc = ET.SubElement(result, "HeaderMisc")
    ET.SubElement(header_misc, "Item", Description="")
    # TestStep 節點
    test_step = ET.SubElement(
        result, "TestStep", Name=operation, startDateTime=now_iso, endDateTime=now_iso, Status="Passed"
    )
    # Data 節點，其 Value 指向剛才生成的 CSV 檔案路徑
    ET.SubElement(
        test_step, "Data", DataType="Table", Name=f"tbl_{operation.upper()}", Value=csv_path, CompOperation="LOG"
    )

    # 使用 xml.dom.minidom 將產生的 XML 字串進行美化 (縮排)
    xml_str = minidom.parseString(ET.tostring(results)).toprettyxml(indent="   ", encoding="utf-8")
    # 將美化後的 XML 內容以二進位模式寫入檔案
    with open(xml_file, "wb") as f:
        f.write(xml_str)

    logging.info(f"XML saved: {xml_file}")


# ---------------------------------------------------------------------------
# INI 處理 & 主程式
# ---------------------------------------------------------------------------

def process_ini_file(config_path: str) -> None:
    """讀取並解析單一 INI 設定檔，然後觸發後續的檔案處理流程。"""
    # 實例化設定檔解析器
    cfg = ConfigParser()
    
    # 讀取 ini 檔案，忽略以 '#' 開頭的註解行
    with open(config_path, "r", encoding="utf-8") as fp:
        cfg.read_file(line for line in fp if not line.strip().startswith("#"))

    # 讀取各區段的設定值，若有缺失則會拋出例外
    try:
        # [Paths] 區段
        input_paths = [p.strip() for p in cfg.get("Paths", "input_paths").splitlines() if p.strip()]
        output_path = cfg.get("Paths", "output_path")
        csv_base_path = cfg.get("Paths", "CSV_path")
        # [Excel] 區段
        sheet_name = cfg.get("Excel", "sheet_name")
        data_columns = cfg.get("Excel", "data_columns")
        data_row = cfg.getint("Excel", "Data_Row")
        # [Logging] 區段
        log_dir = cfg.get("Logging", "log_path")
        # [DataFields] 區段
        fields_raw = [l for l in cfg.get("DataFields", "fields").splitlines() if l.strip()]
        # [Basic_info] 區段
        site = cfg.get("Basic_info", "Site")
        pl_tool = cfg.get("Basic_info", "PL_Tool", fallback="NA") # fallback 提供預設值
        product_family = cfg.get("Basic_info", "ProductFamily")
        operation = cfg.get("Basic_info", "Operation")
        test_station = cfg.get("Basic_info", "TestStation")
        running_date = cfg.getint("Basic_info", "Running_date", fallback=0)
        file_pattern = cfg.get("Basic_info", "file_name_pattern")
    except (NoSectionError, NoOptionError) as e:
        print(f"[INI ERROR] {e}. Please check the file {config_path}.")
        return # 設定檔有誤，中斷執行

    # 準備 log 檔案路徑與設定
    today_str = datetime.today().strftime("%Y-%m-%d")
    ini_name = os.path.splitext(os.path.basename(config_path))[0]
    log_file_path = os.path.join(log_dir, today_str, f"{ini_name}.log")
    setup_logging(log_file_path)

    # 解析 DataFields 區段的欄位定義
    # 格式： key: 欄位索引: 資料型別
    fields_cfg: Dict[str, tuple[str, str]] = {}
    for line in fields_raw:
        if ":" not in line: # 忽略不合格式的行
            continue
        key, col, dtype = (s.strip() for s in line.split(":"))
        fields_cfg[key] = (col, dtype)

    # 處理所有設定的輸入路徑
    for ipath in input_paths:
        # 根據檔案名稱模式搜尋符合的檔案
        matched_files = glob.glob(os.path.join(ipath, file_pattern))
        for f in matched_files:
            try:
                # 複製檔案到指定的備份/處理目錄
                dst_dir = cfg.get("Paths", "copy_destination_path")
                os.makedirs(dst_dir, exist_ok=True)
                copied = shutil.copy(f, dst_dir)
                logging.info(f"Copied file {f} -> {copied}")
                # 呼叫核心函式來處理這個複製後的檔案
                process_excel_file(
                    copied,
                    sheet_name,
                    data_columns,
                    data_row,
                    output_path,
                    csv_base_path,
                    pl_tool,
                    fields_cfg,
                    site,
                    product_family,
                    operation,
                    test_station,
                    running_date,
                )
            except Exception as e:
                logging.error(f"An unexpected error occurred while processing file {f}: {e}")
                # 即使單一檔案出錯，也繼續處理下一個檔案

def main() -> None:
    """程式主進入點。"""
    # 尋找當前目錄下所有的 .ini 檔案
    for ini in glob.glob("*.ini"):
        print(f"Processing config file: {ini}")
        process_ini_file(ini)
    
    # 重新設定一次 logging，寫入最終的結束訊息
    # 這裡可以考慮寫入一個主 log 或是在最後一個 ini 的 log 中寫入
    if logging.getLogger().hasHandlers():
        logging.info("All config files processed. Program End.")
    print("All .ini files have been processed.")


# 當此腳本被直接執行時，才執行 main()
if __name__ == "__main__":
    main()
