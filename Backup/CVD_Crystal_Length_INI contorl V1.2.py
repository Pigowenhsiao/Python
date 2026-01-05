# Python 程式 - 讀取所有 .ini 檔案，執行資料處理並產生 XML 檔案
import os
import sys
import glob
import shutil
import logging
import pandas as pd
from configparser import ConfigParser, NoSectionError, NoOptionError
from datetime import datetime, timedelta

# --- 自訂模組 ---
# Ensure MyModule is in the Python path or adjust as needed
try:
    sys.path.append('../MyModule')
    import Log
    import SQL
except ImportError as e:
    print(f"Critical Error: Could not import MyModule (Log or SQL). Ensure it's in PYTHONPATH. Details: {e}")
    sys.exit(1)


# --- 常數 ---
DEFAULT_FALLBACK_DAYS = 30
# 用於 DataFrame 欄位重新命名的靜態對映表
STATIC_RENAME_MAP = {
    0: 'Start_Date_Time', 1: 'Def_Part_Number', 2: 'Sem_Number', 3: 'Serial_Number',
    4: 'Operator', 5: 'Pitch1', 6: 'Pitch2', 7: 'Pitch3', 8: 'Pitch4', 9: 'Pitch5',
    10: 'Space1', 11: 'Space2', 12: 'Space3', 13: 'Space4', 14: 'Space5',
    15: 'Duty1', 16: 'Duty2', 17: 'Duty3', 18: 'Duty4', 19: 'Duty5',
    20: 'Duty_Avg', 21: 'Duty1_3Sigma', 22: 'Result'
}
SERIAL_NUMBER_ORIGINAL_COL_IDX = 3 # 假設序列號在原始 Excel 檔案的第 D 欄 (索引為 3)

# --- 全域變數 ---
# global_log_file: This variable is assigned in process_ini_file and main.
# setup_logging configures the root logger based on the path passed to it.

# --- 日誌與記錄檔案相關工具函數 ---

def setup_logging(log_file_path):
    """配置日誌設定"""
    try:
        log_dir = os.path.dirname(log_file_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # 移除舊的 handlers，避免重複日誌
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s [%(module)s.%(funcName)s] - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
    except OSError as e:
        # 此處使用 print 是因為 logging 可能尚未成功設定
        print(f"Critical error setting up log file {log_file_path}: {e}. Some logs may be lost.")
    except Exception as e_gen:
        print(f"Unexpected error during logging setup: {e_gen}")

def update_running_rec(log_ctx, running_rec_path, end_date):
    """更新執行記錄檔案"""
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(log_ctx, f"Running record file {running_rec_path} updated, end date is {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        Log.Log_Error(log_ctx, f"Error updating running record file {running_rec_path}: {e}")

def read_running_rec(log_ctx, running_rec_path, default_days_ago=DEFAULT_FALLBACK_DAYS):
    """
    讀取最後一次執行的記錄。
    如果記錄檔不存在或無效，則回溯 default_days_ago 天。
    """
    fallback_date = datetime.today() - timedelta(days=default_days_ago)
    if not os.path.exists(running_rec_path):
        try:
            rec_dir = os.path.dirname(running_rec_path)
            if rec_dir and not os.path.exists(rec_dir):
                os.makedirs(rec_dir)
            with open(running_rec_path, 'w', encoding='utf-8') as f:
                f.write('') # Create an empty file
            Log.Log_Info(log_ctx, f"Running record file {running_rec_path} not found, created empty file. Using default fallback days: {default_days_ago} days.")
        except Exception as e:
            Log.Log_Error(log_ctx, f"Failed to create running record file {running_rec_path}: {e}. Using default fallback days: {default_days_ago} days.")
        return fallback_date

    try:
        with open(running_rec_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                last_run_date = pd.to_datetime(content, errors='coerce')
                if pd.isnull(last_run_date):
                    Log.Log_Error(log_ctx, f"Running record file {running_rec_path} content format error ('{content}'). Using default fallback days: {default_days_ago} days.")
                    return fallback_date
                Log.Log_Info(log_ctx, f"Read last run time from {running_rec_path}: {last_run_date.strftime('%Y-%m-%d %H:%M:%S')}")
                return last_run_date
            else:
                Log.Log_Info(log_ctx, f"Running record file {running_rec_path} is empty. Using default fallback days: {default_days_ago} days.")
                return fallback_date
    except Exception as e:
        Log.Log_Error(log_ctx, f"Error reading running_rec file {running_rec_path}: {e}. Using default fallback days: {default_days_ago} days.")
        return fallback_date

# --- XML 產生函數 ---
def generate_xml_file(data_dict_param, site, product_family, operation, output_path, log_ctx):
    """根據提供的資料字典產生 XML 檔案。"""
    start_dt_val = data_dict_param.get('key_Start_Date_Time')
    start_dt_str = ''
    if isinstance(start_dt_val, datetime):
        start_dt_str = start_dt_val.strftime('%Y-%m-%d %H:%M:%S')
    elif start_dt_val is not None:
        start_dt_str = str(start_dt_val)

    pn_for_filename = str(data_dict_param.get('key_Part_Number', 'Unknown_PN'))
    sn_for_filename = str(data_dict_param.get('key_Serial_Number', 'Unknown_SN'))
    
    base_xml_filename = f"Site={site},ProductFamily={product_family},Operation={operation},PartNumber={pn_for_filename},SerialNumber={sn_for_filename}.xml"
    # 替換檔名中的非法字元
    safe_xml_filename = "".join(c if c.isalnum() or c in ['=', ',', '-', '_', '.'] else '_' for c in base_xml_filename)
    xml_filepath = os.path.join(output_path, safe_xml_filename)

    try:
        with open(xml_filepath, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<Results>\n')
            result_val_xml = data_dict_param.get('key_Result', 'Done') 
            result_str_xml = str(result_val_xml) if result_val_xml is not None else 'Done'
            header_sn_xml = str(data_dict_param.get('key_Serial_Number', '')) 
            header_pn_xml = str(data_dict_param.get('key_Part_Number', ''))

            f.write(f"    <Result startDateTime=\"{start_dt_str}\" Result=\"{result_str_xml}\">\n")
            f.write(f"        <Header SerialNumber=\"{header_sn_xml}\" PartNumber=\"{header_pn_xml}\" />\n")
            for key, value in data_dict_param.items():
                value_str = ""
                if isinstance(value, datetime):
                    value_str = value.strftime('%Y-%m-%d %H:%M:%S')
                elif value is not None:
                    value_str = str(value)
                f.write(f"        <Data key=\"{str(key)}\" value=\"{value_str}\" />\n")
            f.write('    </Result>\n')
            f.write('</Results>\n')
        Log.Log_Info(log_ctx, f'XML file created: {xml_filepath}')
    except Exception as e_xml:
        Log.Log_Error(log_ctx, f'Failed to create XML file {xml_filepath}: {e_xml}')

# --- Excel 處理的輔助函數 ---
def _read_excel_sheets(excel_path, main_sheet_name, main_cols_spec, xy_sheet_name, xy_cols_spec, log_ctx):
    """讀取 Excel 中的主要資料和 XY 座標資料"""
    df_main, df_xy_coords = None, None
    try:
        Log.Log_Info(log_ctx, f"Reading main data from sheet '{main_sheet_name}', columns '{main_cols_spec}' in {excel_path}")
        df_main = pd.read_excel(excel_path, header=None, sheet_name=main_sheet_name, usecols=str(main_cols_spec), skiprows=100)
        Log.Log_Info(log_ctx, f"Reading XY data from sheet '{xy_sheet_name}', columns '{xy_cols_spec}' in {excel_path}")
        df_xy_coords = pd.read_excel(excel_path, header=None, sheet_name=xy_sheet_name, usecols=str(xy_cols_spec))
        
        if df_main is not None and not df_main.empty:
            df_main.columns = range(df_main.shape[1])
            df_main.dropna(subset=[0], inplace=True) # 假設第0欄是時間戳，空值則移除
        else:
            Log.Log_Error(log_ctx, f"Main data sheet '{main_sheet_name}' in {excel_path} is empty or read failed.")

        if df_xy_coords is not None and not df_xy_coords.empty:
            df_xy_coords.columns = range(df_xy_coords.shape[1])
        else:
             Log.Log_Error(log_ctx, f"XY coordinates sheet '{xy_sheet_name}' in {excel_path} is empty or read failed.")
             df_xy_coords = pd.DataFrame() # 確保 df_xy_coords 不是 None

        return df_main, df_xy_coords
    except FileNotFoundError:
        Log.Log_Error(log_ctx, f'Excel file not found: {excel_path}.')
    except ValueError as ve: # 通常是欄位規格錯誤或工作表不存在
        Log.Log_Error(log_ctx, f'Error reading Excel file {excel_path} (sheet/column issue or format problem): {ve}')
    except Exception as e: # 其他未知錯誤
        Log.Log_Error(log_ctx, f'Unknown error reading Excel file {excel_path}: {e}')
    return None, pd.DataFrame() # 返回空 DataFrame 以避免後續錯誤

def _apply_date_filter(df, fields_map, running_rec_path, date_filter_days, log_ctx):
    """根據日期篩選 DataFrame"""
    if df is None or df.empty:
        return df, None

    processing_start_threshold = read_running_rec(log_ctx, running_rec_path, date_filter_days)
    latest_date_in_original_file = None
    
    start_date_col_spec = fields_map.get('key_Start_Date_Time', (None, None))[0]

    if not start_date_col_spec or not start_date_col_spec.isdigit():
        Log.Log_Error(log_ctx, f"Date filter skipped: 'key_Start_Date_Time' not configured correctly in fields_map (col_spec: {start_date_col_spec}).")
        return df.copy(), None # 返回副本，因為原始 df 可能繼續被使用

    start_date_col_idx = int(start_date_col_spec)
    if not (0 <= start_date_col_idx < df.shape[1]):
        Log.Log_Error(log_ctx, f"Date filter skipped: 'key_Start_Date_Time' column index {start_date_col_idx} is out of bounds.")
        return df.copy(), None

    # 創建一個副本進行日期篩選操作
    df_filtered = df.copy()
    original_dates_series = pd.to_datetime(df_filtered.iloc[:, start_date_col_idx], errors='coerce')

    if not original_dates_series.dropna().empty:
        latest_date_in_original_file = original_dates_series.dropna().max()

    # 附加解析後的日期列以供篩選，之後移除
    df_filtered['__parsed_date__'] = original_dates_series
    initial_row_count = len(df_filtered)
    df_filtered.dropna(subset=['__parsed_date__'], inplace=True) # 移除無法解析日期的列
    df_filtered = df_filtered[df_filtered['__parsed_date__'] >= processing_start_threshold].copy() # .copy() 避免 SettingWithCopyWarning
    df_filtered.drop(columns=['__parsed_date__'], inplace=True)
    
    rows_filtered_out = initial_row_count - len(df_filtered)
    if rows_filtered_out > 0:
        Log.Log_Info(log_ctx, f"Date filter ({processing_start_threshold.strftime('%Y-%m-%d %H:%M:%S')}): {rows_filtered_out} rows removed.")
    
    if df_filtered.empty:
        Log.Log_Info(log_ctx, f"After date filtering, no new data to process.")
        if latest_date_in_original_file and latest_date_in_original_file > processing_start_threshold:
            update_running_rec(log_ctx, running_rec_path, latest_date_in_original_file)
            
    return df_filtered, latest_date_in_original_file

def _enrich_with_sql_data(df, serial_num_col_idx, log_ctx):
    """從 SQL 資料庫獲取資料並豐富 DataFrame"""
    if df is None or df.empty:
        return df
    
    df_enriched = df.copy()
    df_enriched['part_number'] = None # 從 SQL 獲取
    df_enriched['lot_number_9'] = None # 從 SQL 獲取

    if not (0 <= serial_num_col_idx < df_enriched.shape[1]):
        Log.Log_Error(log_ctx, f"SQL data enrichment skipped: Serial number column index {serial_num_col_idx} is out of bounds.")
        return df_enriched # 返回未變更的副本

    sql_conn, sql_cursor = None, None
    try:
        sql_conn, sql_cursor = SQL.connSQL()
        if not sql_conn:
            Log.Log_Error(log_ctx, "SQL data enrichment failed: Could not connect to database.")
            return df_enriched # 返回未變更的副本

        for idx, row in df_enriched.iterrows():
            serial_num = row.iloc[serial_num_col_idx]
            if pd.notna(serial_num) and str(serial_num).strip():
                try:
                    part_num, lot_num_9 = SQL.selectSQL(sql_cursor, str(serial_num))
                    df_enriched.loc[idx, 'part_number'] = part_num
                    df_enriched.loc[idx, 'lot_number_9'] = lot_num_9
                except Exception as e_sql_query:
                    Log.Log_Error(log_ctx, f"SQL query failed for S/N {serial_num} (row index {idx}): {e_sql_query}")
            else:
                Log.Log_Error(log_ctx, f"Skipping SQL query for row index {idx}: S/N at original column {serial_num_col_idx} is empty or invalid.")
    except Exception as e_sql_conn:
        Log.Log_Error(log_ctx, f"SQL connection or general error: {e_sql_conn}")
    finally:
        if sql_conn:
            SQL.disconnSQL(sql_conn, sql_cursor)

    # 移除 SQL 查詢後 part_number 仍為空的資料
    df_enriched.dropna(subset=['part_number'], inplace=True)
    return df_enriched.reset_index(drop=True)

def _merge_xy_coordinates(df, df_xy_coords, log_ctx):
    """合併 XY 座標到主 DataFrame"""
    if df is None or df.empty:
        return df
    
    df_merged = df.copy()
    if df_xy_coords is None or df_xy_coords.empty:
        Log.Log_Error(log_ctx, "XY coordinates data is empty. Skipping merge.")
        for i in range(1, 6): # 確保欄位存在，即使是空值
            df_merged[f'X{i}'] = None
            df_merged[f'Y{i}'] = None
        return df_merged

    try:
        for i in range(1, 6): # X1-Y1 to X5-Y5
            point_row_index = i - 1 # 假設 P1 在第0行, P2 在第1行, etc.
            x_val, y_val = None, None
            if point_row_index < df_xy_coords.shape[0]:
                # X 座標在第2欄 (索引1), Y 座標在第3欄 (索引2)
                x_val = df_xy_coords.iloc[point_row_index, 1] if 1 < df_xy_coords.shape[1] else None
                y_val = df_xy_coords.iloc[point_row_index, 2] if 2 < df_xy_coords.shape[1] else None
            df_merged[f'X{i}'] = x_val
            df_merged[f'Y{i}'] = y_val
    except Exception as e_xy_merge:
        Log.Log_Error(log_ctx, f"Error merging XY coordinates: {e_xy_merge}")
        for i_err in range(1, 6): # 出錯時初始化
            df_merged[f'X{i_err}'] = None
            df_merged[f'Y{i_err}'] = None
    return df_merged

def _apply_type_conversions(df, fields_map, log_ctx):
    """根據 fields_map 套用資料型態轉換"""
    if df is None or df.empty:
        return df

    df_converted = df.copy()
    rows_to_drop_indices = set() # 使用 set 以避免重複索引

    for row_idx in range(len(df_converted)):
        for xml_key, (col_spec, dtype_str) in fields_map.items():
            if not col_spec.isdigit(): # 型態轉換通常針對原始的數字索引欄位
                continue 
            
            original_col_idx = int(col_spec)
            if not (0 <= original_col_idx < df_converted.shape[1]):
                # Log.Log_Error(log_ctx, f"Type conversion: Column index {original_col_idx} for '{xml_key}' is out of bounds for row {row_idx}. Skipping.")
                continue

            original_value = df_converted.iloc[row_idx, original_col_idx]
            converted_value = original_value # 預設值

            if pd.isna(original_value):
                converted_value = None # 標準化 NaN/NaT 為 None
            else:
                try:
                    if dtype_str == 'float': converted_value = float(original_value)
                    elif dtype_str == 'int': converted_value = int(original_value)
                    elif dtype_str == 'datetime':
                        converted_value = pd.to_datetime(original_value, errors='raise') # 'raise' 以捕獲錯誤
                    elif dtype_str == 'bool':
                        if isinstance(original_value, str):
                            if original_value.lower() == 'true': converted_value = True
                            elif original_value.lower() == 'false': converted_value = False
                            else: raise ValueError("Invalid string for bool conversion")
                        else: converted_value = bool(original_value) # 處理數字 0, 1 等
                    elif dtype_str == 'str': converted_value = str(original_value)
                    # 如果有其他型態，可在此添加
                except (ValueError, TypeError) as e_conv:
                    Log.Log_Error(log_ctx, f"Type conversion error for row {row_idx}, column '{xml_key}' (original index {original_col_idx}), value '{original_value}' to {dtype_str}: {e_conv}. Marking row for removal.")
                    rows_to_drop_indices.add(row_idx)
                    break # 此列有問題，跳到下一列的處理
            
            df_converted.iat[row_idx, original_col_idx] = converted_value
    
    if rows_to_drop_indices:
        df_converted.drop(index=list(rows_to_drop_indices), inplace=True)
        Log.Log_Info(log_ctx, f"Removed {len(rows_to_drop_indices)} rows due to type conversion failures.")
        df_converted = df_converted.reset_index(drop=True)
        
    return df_converted

def _perform_column_renaming(df, rename_map_config):
    """執行欄位重新命名"""
    if df is None or df.empty:
        return df
    df_renamed = df.copy()
    applicable_rename_map = {k: v for k, v in rename_map_config.items() if k in df_renamed.columns}
    df_renamed.rename(columns=applicable_rename_map, inplace=True)
    return df_renamed

def _save_to_csv(df, csv_base_path, ini_filename_base, log_ctx):
    """將 DataFrame 儲存為 CSV 檔案"""
    if df is None or df.empty:
        Log.Log_Info(log_ctx, "CSV saving skipped: DataFrame is empty.")
        return

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    csv_filename = f"{ini_filename_base}_{timestamp}.csv"
    csv_filepath = os.path.join(csv_base_path, csv_filename)
    
    try:
        if not os.path.exists(csv_base_path):
            os.makedirs(csv_base_path)
            Log.Log_Info(log_ctx, f"Created CSV directory: {csv_base_path}")
        df.to_csv(csv_filepath, index=False, encoding='utf-8')
        Log.Log_Info(log_ctx, f"Data written to CSV file: {csv_filepath}")
    except Exception as e_csv:
        Log.Log_Error(log_ctx, f"Error writing CSV file {csv_filepath}: {e_csv}")

def _create_xml_from_data(df, fields_map, rename_map_config, xml_gen_func, 
                           site, product_family, operation, output_path, log_ctx):
    """從處理後的 DataFrame 列產生 XML 檔案"""
    if df is None or df.empty:
        Log.Log_Info(log_ctx, "XML generation skipped: DataFrame is empty.")
        return
    
    Log.Log_Info(log_ctx, f"Starting XML generation for {len(df)} processed rows.")
    for idx_row_xml, row_data in df.iterrows():
        data_dict_for_xml = {}
        for xml_key, (col_spec, _) in fields_map.items(): # dtype from fields_map not used here, already converted
            value_from_row = pd.NA 

            if col_spec.isdigit(): 
                original_col_idx = int(col_spec)
                current_col_name = rename_map_config.get(original_col_idx)
                if current_col_name and current_col_name in row_data:
                    value_from_row = row_data[current_col_name]
                else:
                    Log.Log_Error(log_ctx, f"XML Gen (row {idx_row_xml}): Field '{xml_key}' (orig.idx {original_col_idx} -> name '{current_col_name}') not found in data row.")
            else: 
                if col_spec in row_data:
                    value_from_row = row_data[col_spec]
                else:
                    Log.Log_Error(log_ctx, f"XML Gen (row {idx_row_xml}): Field '{xml_key}' (col name '{col_spec}') not found in data row.")
            
            data_dict_for_xml[xml_key] = None if pd.isna(value_from_row) else value_from_row
        
        xml_gen_func( # 呼叫全域的 generate_xml_file
            data_dict_param=data_dict_for_xml,
            site=site, product_family=product_family, operation=operation,
            output_path=output_path, log_ctx=log_ctx
        )
    Log.Log_Info(log_ctx, f"Finished XML generation for processed rows.")


# --- 主 Excel 檔案處理函數 ---
def process_single_excel_file(excel_file_path, running_rec_path_cfg, date_filter_days_cfg, 
                              log_ctx_main, sheet_name_cfg, data_columns_cfg, xy_sheet_name_cfg, xy_columns_cfg, 
                              fields_map_cfg, ini_filename_base_id, csv_path_cfg, 
                              site_cfg, product_family_cfg, operation_cfg, output_path_cfg):
    """
    處理單個 Excel 檔案的完整流程：讀取、篩選、擴充、轉換、儲存 CSV 及產生 XML。
    參數名後綴 _cfg 表示來自設定，_id 表示識別碼，_main 表示主要上下文。
    """
    Log.Log_Info(log_ctx_main, f"Processing Excel file: {excel_file_path}")

    df_main, df_xy = _read_excel_sheets(excel_file_path, sheet_name_cfg, data_columns_cfg, 
                                        xy_sheet_name_cfg, xy_columns_cfg, log_ctx_main)

    if df_main is None or df_main.empty:
        Log.Log_Error(log_ctx_main, f"Stopping processing for {excel_file_path}: Main data is empty or could not be read.")
        return

    df_filtered, latest_date = _apply_date_filter(df_main, fields_map_cfg, running_rec_path_cfg, 
                                                  date_filter_days_cfg, log_ctx_main)
    if df_filtered.empty:
        Log.Log_Info(log_ctx_main, f"No data left in {excel_file_path} after date filtering. Skipping further processing.")
        # running_rec update is handled within _apply_date_filter if latest_date is relevant
        return

    df_with_sql = _enrich_with_sql_data(df_filtered, SERIAL_NUMBER_ORIGINAL_COL_IDX, log_ctx_main)
    if df_with_sql.empty:
        Log.Log_Info(log_ctx_main, f"No data left in {excel_file_path} after SQL enrichment. Skipping further processing.")
        return
        
    df_with_xy = _merge_xy_coordinates(df_with_sql, df_xy, log_ctx_main)
    
    df_typed = _apply_type_conversions(df_with_xy, fields_map_cfg, log_ctx_main)
    if df_typed.empty:
        Log.Log_Info(log_ctx_main, f"No data left in {excel_file_path} after type conversions. Skipping further processing.")
        if latest_date and latest_date > read_running_rec(log_ctx_main, running_rec_path_cfg, date_filter_days_cfg): # Check if running_rec should be updated
             update_running_rec(log_ctx_main, running_rec_path_cfg, latest_date)
        return

    df_renamed = _perform_column_renaming(df_typed, STATIC_RENAME_MAP)

    _save_to_csv(df_renamed, csv_path_cfg, ini_filename_base_id, log_ctx_main)
    
    #_create_xml_from_data(df_renamed, fields_map_cfg, STATIC_RENAME_MAP, 
    #                      generate_xml_file, # 傳入全域 XML 產生函數
    #                      site_cfg, product_family_cfg, operation_cfg, 
    #                      output_path_cfg, log_ctx_main)
    
    Log.Log_Info(log_ctx_main, f"Successfully processed Excel file: {excel_file_path}")


# --- INI 設定檔處理的輔助函數 ---
def _read_and_parse_ini_config(config_file_path, log_ctx):
    """讀取並解析 INI 設定檔"""
    config = ConfigParser()
    try:
        config_content = []
        with open(config_file_path, 'r', encoding='utf-8') as f_obj:
            for line in f_obj:
                if not line.strip().startswith('#'): # 跳過註解行
                    config_content.append(line)
        config.read_string("".join(config_content))
        Log.Log_Info(log_ctx, f"Successfully read and parsed INI config: {config_file_path}")
        return config
    except Exception as e:
        Log.Log_Error(log_ctx, f"Critical error reading INI config file {config_file_path}: {e}")
        return None

class IniSettings: # 使用類別來組織設定值
    def __init__(self):
        self.log_path_from_ini = None
        self.input_paths = []
        self.output_path = None
        self.running_rec_path = None
        self.intermediate_data_dir = None
        self.sheet_name = None
        self.data_columns = None
        self.xy_sheet_name = None
        self.xy_columns = None
        self.fields_config_raw_lines = []
        self.site = None
        self.product_family = None
        self.operation = None
        self.file_name_pattern = None
        self.csv_path = None
        self.data_date_days = DEFAULT_FALLBACK_DAYS
        self.is_valid = False # 標記設定是否成功載入

def _extract_settings_from_config(config_obj, config_file_path_str, log_ctx_settings):
    """從 ConfigParser 物件中提取所有設定"""
    settings = IniSettings()
    try:
        settings.log_path_from_ini = config_obj.get('Logging', 'log_path')
        settings.input_paths = [p.strip() for p in config_obj.get('Paths', 'input_paths').split(',')]
        settings.output_path = config_obj.get('Paths', 'output_path')
        settings.running_rec_path = config_obj.get('Paths', 'running_rec')
        settings.intermediate_data_dir = config_obj.get('Paths', 'intermediate_data_path')
        
        settings.sheet_name = config_obj.get('Excel', 'sheet_name')
        settings.data_columns = config_obj.get('Excel', 'data_columns')
        settings.xy_sheet_name = config_obj.get('Excel', 'xy_sheet_name')
        settings.xy_columns = config_obj.get('Excel', 'xy_columns')
        
        if config_obj.has_section('DataFields') and config_obj.has_option('DataFields', 'fields'):
            settings.fields_config_raw_lines = config_obj.get('DataFields', 'fields').splitlines()
        else:
            raise NoSectionError("DataFields section or 'fields' option missing.") # 強制錯誤以便捕獲

        settings.site = config_obj.get('Basic_info', 'Site')
        settings.product_family = config_obj.get('Basic_info', 'ProductFamily')
        settings.operation = config_obj.get('Basic_info', 'Operation')
        # settings.test_station = config_obj.get('Basic_info', 'TestStation') # 未使用，暫時註解
        settings.file_name_pattern = config_obj.get('Basic_info', 'file_name_pattern')
        settings.csv_path = config_obj.get('Paths', 'CSV_path')
        
        try:
            settings.data_date_days = config_obj.getint('Basic_info', 'Data_date')
            Log.Log_Info(log_ctx_settings, f"Data_date from config: {settings.data_date_days} days.")
        except ValueError:
            Log.Log_Error(log_ctx_settings, f"Invalid integer for Data_date in [Basic_info]. Using default: {settings.data_date_days} days.")
        except NoOptionError:
            Log.Log_Error(log_ctx_settings, f"Data_date option missing in [Basic_info]. Using default: {settings.data_date_days} days.")
        
        settings.is_valid = True # 所有必要設定已讀取
        Log.Log_Info(log_ctx_settings, f"Successfully extracted settings from {config_file_path_str}.")

    except (NoSectionError, NoOptionError) as e_section_option:
        Log.Log_Error(log_ctx_settings, f"Missing required section or option in INI file {config_file_path_str}: {e_section_option}")
    except Exception as e_extract:
        Log.Log_Error(log_ctx_settings, f"Unexpected error extracting settings from {config_file_path_str}: {e_extract}")
    
    return settings

def _parse_fields_map_from_lines(fields_lines, log_ctx_fields):
    """解析 fields_map 設定"""
    fields_map_parsed = {}
    if not fields_lines:
        Log.Log_Error(log_ctx_fields, "Fields configuration lines are empty. fields_map will be empty.")
        return fields_map_parsed

    for line_num, line_content in enumerate(fields_lines):
        line_stripped = line_content.strip()
        if line_stripped and not line_stripped.startswith('#'):
            try:
                key, col_spec, dtype = map(str.strip, line_stripped.split(':', 2))
                fields_map_parsed[key] = (col_spec, dtype)
            except ValueError:
                Log.Log_Error(log_ctx_fields, f"Field setting format error in line {line_num + 1}: '{line_content}'. Expected 'key:col_spec:dtype'.")
    
    if not fields_map_parsed:
        Log.Log_Error(log_ctx_fields, "No valid field mappings were parsed from the fields configuration.")
    else:
        Log.Log_Info(log_ctx_fields, f"Parsed {len(fields_map_parsed)} field mappings.")
    return fields_map_parsed


# --- INI 設定檔處理主函數 ---
def process_ini_file(ini_config_path, overall_program_log_ctx):
    """處理單個 .ini 設定檔"""
    ini_filename_base = os.path.splitext(os.path.basename(ini_config_path))[0]
    # 初始日誌上下文使用程式主日誌
    current_processing_log_ctx = overall_program_log_ctx
    Log.Log_Info(current_processing_log_ctx, f"Starting processing for INI file: {ini_config_path}")

    config_object = _read_and_parse_ini_config(ini_config_path, current_processing_log_ctx)
    if not config_object:
        return # 讀取INI失敗，已記錄錯誤

    # 從 ConfigParser 物件提取設定
    # 注意：此時 current_processing_log_ctx 仍是 overall_program_log_ctx
    settings = _extract_settings_from_config(config_object, ini_config_path, current_processing_log_ctx)
    if not settings.is_valid:
        Log.Log_Error(current_processing_log_ctx, f"Failed to load valid settings from {ini_config_path}. Aborting processing for this INI.")
        return

    # 設定此 INI 專用的日誌
    log_folder_name_date = str(datetime.today().date()) # YYYY-MM-DD
    ini_specific_log_dir = os.path.join(settings.log_path_from_ini, log_folder_name_date)
    ini_specific_log_file_path = os.path.join(ini_specific_log_dir, f'{ini_filename_base}.log')
    setup_logging(ini_specific_log_file_path)
    current_processing_log_ctx = ini_specific_log_file_path # 更新日誌上下文為此 INI 的日誌檔案路徑
    Log.Log_Info(current_processing_log_ctx, f"Dedicated logging for {ini_filename_base} configured at: {current_processing_log_ctx}")
    Log.Log_Info(current_processing_log_ctx, f"Continuing with settings from {ini_config_path} using its dedicated log.")


    fields_map_obj = _parse_fields_map_from_lines(settings.fields_config_raw_lines, current_processing_log_ctx)
    if not fields_map_obj:
        Log.Log_Error(current_processing_log_ctx, f"No field mappings loaded from {ini_config_path}. Cannot process Excel files.")
        return

    # 建立輸出目錄 (如果不存在)
    if not os.path.exists(settings.output_path):
        try:
            os.makedirs(settings.output_path)
            Log.Log_Info(current_processing_log_ctx, f"Created output directory: {settings.output_path}")
        except OSError as e_mkdir:
            Log.Log_Error(current_processing_log_ctx, f"Failed to create output directory {settings.output_path}: {e_mkdir}. XML storage will fail.")
            return # 如果無法建立輸出目錄，則 XML 產生會失敗

    # 處理每個輸入路徑
    for input_source_path in settings.input_paths:
        Log.Log_Info(current_processing_log_ctx, f"Scanning input path: {input_source_path} with pattern: {settings.file_name_pattern}")
        try:
            # recursive=False，因為通常模式是 '*.xlsx'，只在當前目錄下查找
            excel_files_found_list = [
                f for f in glob.glob(os.path.join(input_source_path, settings.file_name_pattern), recursive=False)
                if "コピ" not in os.path.basename(f) # 過濾日文檔名中的 "コピ" (copy)
            ]
        except Exception as e_glob_err:
            Log.Log_Error(current_processing_log_ctx, f"Error scanning path {input_source_path}: {e_glob_err}")
            continue # 繼續處理下一個輸入路徑

        valid_excel_files_list = [
            f for f in excel_files_found_list 
            if not os.path.basename(f).startswith('~$') and os.path.isfile(f) # 過濾暫存檔
        ]

        if not valid_excel_files_list:
            Log.Log_Info(current_processing_log_ctx, f"No valid Excel files matching pattern '{settings.file_name_pattern}' found in {input_source_path}.")
            continue

        # 確保中間資料目錄存在
        if not settings.intermediate_data_dir:
            Log.Log_Error(current_processing_log_ctx, "Intermediate data directory (intermediate_data_path) is not set. Cannot copy files.")
            return # 對此 INI 停止處理，因為這是關鍵路徑

        if not os.path.exists(settings.intermediate_data_dir):
            try:
                os.makedirs(settings.intermediate_data_dir)
                Log.Log_Info(current_processing_log_ctx, f"Created intermediate data directory: {settings.intermediate_data_dir}")
            except OSError as e_mkdir_interm_err:
                Log.Log_Error(current_processing_log_ctx, f"Failed to create intermediate directory {settings.intermediate_data_dir}: {e_mkdir_interm_err}.")
                continue # 無法建立此目錄，跳過此 input_source_path

        for excel_src_full_path in valid_excel_files_list:
            copied_excel_target_path = os.path.join(settings.intermediate_data_dir, os.path.basename(excel_src_full_path))
            try:
                shutil.copy(excel_src_full_path, copied_excel_target_path)
                Log.Log_Info(current_processing_log_ctx, f"Copied Excel file: {excel_src_full_path} -> {copied_excel_target_path}")
                
                # 呼叫主 Excel 處理函數
                process_single_excel_file(
                    excel_file_path=copied_excel_target_path,
                    running_rec_path_cfg=settings.running_rec_path,
                    date_filter_days_cfg=settings.data_date_days,
                    log_ctx_main=current_processing_log_ctx, # 傳入此 INI 的專用日誌上下文
                    sheet_name_cfg=settings.sheet_name,
                    data_columns_cfg=settings.data_columns,
                    xy_sheet_name_cfg=settings.xy_sheet_name,
                    xy_columns_cfg=settings.xy_columns,
                    fields_map_cfg=fields_map_obj,
                    ini_filename_base_id=ini_filename_base,
                    csv_path_cfg=settings.csv_path,
                    site_cfg=settings.site,
                    product_family_cfg=settings.product_family,
                    operation_cfg=settings.operation,
                    output_path_cfg=settings.output_path
                )
            except FileNotFoundError: # shutil.copy 可能引發 (雖然 glob 應該已經確認過)
                Log.Log_Error(current_processing_log_ctx, f"Source file {excel_src_full_path} not found during copy operation.")
            except shutil.Error as e_shutil_copy:
                Log.Log_Error(current_processing_log_ctx, f"Error copying file {excel_src_full_path} to {copied_excel_target_path}: {e_shutil_copy}")
            except Exception as e_process_single: # 捕獲 process_single_excel_file 中的任何未預期錯誤
                Log.Log_Error(current_processing_log_ctx, f"Unexpected error processing file {copied_excel_target_path} (from {excel_src_full_path}): {e_process_single}")
    
    Log.Log_Info(current_processing_log_ctx, f"Finished processing all input paths for INI file: {ini_config_path}")


# --- 程式主進入點 ---
def main():
    """程式主進入點"""
    script_directory = os.path.dirname(os.path.abspath(__file__))
    
    # 設定整體程式執行的主日誌檔案
    overall_program_log_file = os.path.join(script_directory, 'program_execution.log')
    setup_logging(overall_program_log_file)

    Log.Log_Info(overall_program_log_file, f"Program started (Script path: {script_directory})")

    # 在腳本所在目錄下搜尋 .ini 設定檔
    ini_search_pattern = os.path.join(script_directory, "*.ini")
    ini_files_to_process_list = glob.glob(ini_search_pattern)

    if not ini_files_to_process_list:
        Log.Log_Error(overall_program_log_file, f"No .ini configuration files found in directory: {script_directory}.")
    else:
        Log.Log_Info(overall_program_log_file, f"Found {len(ini_files_to_process_list)} .ini configuration file(s): {', '.join(map(os.path.basename, ini_files_to_process_list))}")
        for ini_file_path_item in ini_files_to_process_list:
            # process_ini_file 內部會為其處理的 INI 設定專用日誌
            # 傳入 overall_program_log_file 作為其初始日誌上下文
            process_ini_file(ini_file_path_item, overall_program_log_file)
            
            # 處理完一個 INI 後，將日誌記錄切換回主程式日誌
            setup_logging(overall_program_log_file) 
            Log.Log_Info(overall_program_log_file, f"Completed processing cycle for INI file: {os.path.basename(ini_file_path_item)}.")

    Log.Log_Info(overall_program_log_file, "All .ini configuration files processed. Program finishing.")

if __name__ == '__main__':
    main()