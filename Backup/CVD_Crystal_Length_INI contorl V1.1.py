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
sys.path.append('../MyModule')
import Log
import SQL


# --- 常數 ---
DEFAULT_FALLBACK_DAYS = 30
# INTERMEDIATE_DATA_DIR REMOVED - Will be read from INI

# --- 全域變數 ---
global_log_file = None

# --- 日誌與記錄檔案相關工具函數 ---

def setup_logging(log_file_path):
    """配置日誌設定"""
    try:
        log_dir = os.path.dirname(log_file_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s [%(module)s.%(funcName)s] - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
    except OSError as e:
        print(f"設定日誌檔案 {log_file_path} 時發生嚴重錯誤: {e}. 部分日誌可能遺失。")
    except Exception as e_gen:
        print(f"設定日誌時發生未預期錯誤: {e_gen}")

def update_running_rec(current_log_ctx, running_rec_path, end_date):
    """更新執行記錄檔案"""
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(current_log_ctx, f"執行記錄檔案 {running_rec_path} 已更新，結束日期為 {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        Log.Log_Error(current_log_ctx, f"更新執行記錄檔案 {running_rec_path} 時發生錯誤: {e}")

def read_running_rec(current_log_ctx, running_rec_path, default_days_ago=DEFAULT_FALLBACK_DAYS):
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
                f.write('')
            Log.Log_Info(current_log_ctx, f"執行記錄檔案 {running_rec_path} 不存在，已建立空檔案。將使用預設回溯天數: {default_days_ago} 天。")
        except Exception as e:
            Log.Log_Error(current_log_ctx, f"建立執行記錄檔案 {running_rec_path} 失敗: {e}。將使用預設回溯天數: {default_days_ago} 天。")
        return fallback_date

    try:
        with open(running_rec_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                last_run_date = pd.to_datetime(content, errors='coerce')
                if pd.isnull(last_run_date):
                    Log.Log_Error(current_log_ctx, f"執行記錄檔案 {running_rec_path} 內容格式錯誤 ('{content}')。將使用預設回溯天數: {default_days_ago} 天。")
                    return fallback_date
                Log.Log_Info(current_log_ctx, f"從 {running_rec_path} 讀取到上次執行時間: {last_run_date.strftime('%Y-%m-%d %H:%M:%S')}")
                return last_run_date
            else:
                Log.Log_Info(current_log_ctx, f"執行記錄檔案 {running_rec_path} 為空。將使用預設回溯天數: {default_days_ago} 天。")
                return fallback_date
    except Exception as e:
        Log.Log_Error(current_log_ctx, f"讀取 running_rec 檔案 {running_rec_path} 時發生錯誤: {e}。將使用預設回溯天數: {default_days_ago} 天。")
        return fallback_date

# ---核心處理邏輯---

def process_ini_file(config_path, overall_log_ctx):
    """
    處理單個 .ini 設定檔，並根據其設定執行所有相關操作。
    overall_log_ctx 是主程式的日誌上下文，用於在 ini 專用日誌設定前的記錄。
    """
    global global_log_file
    
    ini_filename_base = os.path.splitext(os.path.basename(config_path))[0]
    current_ini_log_ctx = overall_log_ctx

    Log.Log_Info(current_ini_log_ctx, f"開始處理設定檔: {config_path}")

    config = ConfigParser()
    try:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config.read_file(line for line in config_file if not line.strip().startswith('#'))
    except Exception as e:
        Log.Log_Error(current_ini_log_ctx, f"讀取設定檔 {config_path} 時發生嚴重錯誤: {e}")
        return

    # --- 讀取設定 ---
    intermediate_data_dir = None # Initialize
    try:
        log_path_from_ini = config.get('Logging', 'log_path')
        
        log_folder_name = str(datetime.today().date())
        specific_log_folder_path = os.path.join(log_path_from_ini, log_folder_name)
        global_log_file = os.path.join(specific_log_folder_path, f'{ini_filename_base}.log')
        setup_logging(global_log_file)
        current_ini_log_ctx = global_log_file
        Log.Log_Info(current_ini_log_ctx, f"已為 {ini_filename_base} 設定專用日誌: {global_log_file}")

        input_paths = [path.strip() for path in config.get('Paths', 'input_paths').split(',')]
        output_path = config.get('Paths', 'output_path')
        running_rec_path = config.get('Paths', 'running_rec')
        intermediate_data_dir = config.get('Paths', 'intermediate_data_path') # *** MODIFIED: Read from INI ***
        
        sheet_name = config.get('Excel', 'sheet_name')
        data_columns = config.get('Excel', 'data_columns')
        xy_sheet_name = config.get('Excel', 'xy_sheet_name')
        xy_columns = config.get('Excel', 'xy_columns')
        
        fields_config_lines = config.get('DataFields', 'fields').splitlines()
        
        site = config.get('Basic_info', 'Site')
        product_family = config.get('Basic_info', 'ProductFamily')
        operation = config.get('Basic_info', 'Operation')
        test_station = config.get('Basic_info', 'TestStation')
        file_name_pattern = config.get('Basic_info', 'file_name_pattern')
        CSV_path = config.get('Paths', 'CSV_path')
        
        try:
            data_date_days = config.getint('Basic_info', 'Data_date')
            Log.Log_Info(current_ini_log_ctx, f"從設定檔讀取到 Data_date: {data_date_days} 天。")
        except ValueError:
            data_date_days = DEFAULT_FALLBACK_DAYS
            Log.Log_Error(current_ini_log_ctx, f"設定檔中 [Basic_info] 的 Data_date 不是有效整數。使用預設回溯: {data_date_days} 天。")
        except NoOptionError:
            data_date_days = DEFAULT_FALLBACK_DAYS
            Log.Log_Error(current_ini_log_ctx, f"設定檔中 [Basic_info] 缺少 Data_date 選項。使用預設回溯: {data_date_days} 天。")

    except (NoSectionError, NoOptionError) as e:
        Log.Log_Error(current_ini_log_ctx, f"讀取設定檔 {config_path} 時缺少必要區段或選項 (例如 'intermediate_data_path'): {e}")
        return
    except Exception as e:
        Log.Log_Error(current_ini_log_ctx, f"讀取設定檔 {config_path} 時發生未預期錯誤: {e}")
        return

    # 解析欄位設定
    fields_map = {}
    for line in fields_config_lines:
        if line.strip() and not line.strip().startswith('#'):
            try:
                key, col_spec, dtype = line.split(':', 2)
                fields_map[key.strip()] = (col_spec.strip(), dtype.strip())
            except ValueError:
                Log.Log_Error(current_ini_log_ctx, f"欄位設定格式錯誤: '{line}'。應為 'key:col_spec:dtype'。")
    
    if not os.path.exists(output_path):
        try:
            os.makedirs(output_path)
            Log.Log_Info(current_ini_log_ctx, f"已建立輸出目錄: {output_path}")
        except OSError as e:
            Log.Log_Error(current_ini_log_ctx, f"建立輸出目錄 {output_path} 失敗: {e}。無法儲存 XML。")
            return
    
    # --- 內部輔助函數 ---
    def generate_xml_file(data_dict_param):
        start_dt_val = data_dict_param.get('key_Start_Date_Time')
        start_dt_str = ''
        if isinstance(start_dt_val, datetime):
            start_dt_str = start_dt_val.strftime('%Y-%m-%d %H:%M:%S')
        elif start_dt_val is not None:
            start_dt_str = str(start_dt_val)

        pn_for_filename = data_dict_param.get('key_Part_Number', 'Unknown_PN')
        sn_for_filename = data_dict_param.get('key_Serial_Number', 'Unknown_SN')
        
        base_xml_filename = f"Site={site},ProductFamily={product_family},Operation={operation},PartNumber={pn_for_filename},SerialNumber={sn_for_filename}.xml"
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
                    f.write(f"        <Data key=\"{key}\" value=\"{value_str}\" />\n")
                f.write('    </Result>\n')
                f.write('</Results>\n')
            Log.Log_Info(current_ini_log_ctx, f'XML 檔案已建立: {xml_filepath}')
        except Exception as e_xml:
            Log.Log_Error(current_ini_log_ctx, f'建立 XML 檔案 {xml_filepath} 失敗: {e_xml}')

    def process_single_excel_file(excel_file_path_param, current_running_rec_path_param, date_filter_days_param):
        Log.Log_Info(current_ini_log_ctx, f'開始處理 Excel 檔案: {excel_file_path_param}')
        df_main, df_xy_coords = None, None
        try:
            df_main = pd.read_excel(excel_file_path_param, header=None, sheet_name=sheet_name, usecols=str(data_columns), skiprows=100)
            df_xy_coords = pd.read_excel(excel_file_path_param, header=None, sheet_name=xy_sheet_name, usecols=str(xy_columns))
        except FileNotFoundError:
            Log.Log_Error(current_ini_log_ctx, f'Excel 檔案 {excel_file_path_param} 未找到。')
            return
        except ValueError as ve:
             Log.Log_Error(current_ini_log_ctx, f'讀取 Excel 檔案 {excel_file_path_param} 時發生欄位錯誤或檔案格式問題 (pandas): {ve}')
             return
        except Exception as e:
            Log.Log_Error(current_ini_log_ctx, f'讀取 Excel 檔案 {excel_file_path_param} 時發生未知錯誤: {e}')
            return
        
        if df_main.empty:
            Log.Log_Error(current_ini_log_ctx, f'Excel 檔案 {excel_file_path_param} 的主要資料工作表 ({sheet_name}) 為空或讀取後無資料。')
            return 
        
        df_main.columns = range(df_main.shape[1])
        df_main = df_main.dropna(subset=[0])
        
        if df_xy_coords.empty:
             Log.Log_Error(current_ini_log_ctx, f'Excel 檔案 {excel_file_path_param} 的 XY 座標工作表 ({xy_sheet_name}) 為空。')
        else:
            df_xy_coords.columns = range(df_xy_coords.shape[1])

        processing_start_threshold = read_running_rec(current_ini_log_ctx, current_running_rec_path_param, date_filter_days_param)
        
        filtered_df = df_main.copy()
        latest_date_in_original_file = None

        if 'key_Start_Date_Time' in fields_map:
            start_date_col_spec = fields_map['key_Start_Date_Time'][0]
            if not start_date_col_spec.isdigit():
                Log.Log_Error(current_ini_log_ctx, f'key_Start_Date_Time 的欄位規格 "{start_date_col_spec}" 無效，應為數字索引。跳過日期篩選。')
            else:
                start_date_col_idx = int(start_date_col_spec)
                if 0 <= start_date_col_idx < filtered_df.shape[1]:
                    original_dates_series = pd.to_datetime(filtered_df.iloc[:, start_date_col_idx], errors='coerce')
                    if not original_dates_series.dropna().empty:
                        latest_date_in_original_file = original_dates_series.dropna().max()

                    filtered_df['__parsed_date__'] = original_dates_series
                    initial_row_count = len(filtered_df)
                    filtered_df = filtered_df[filtered_df['__parsed_date__'] >= processing_start_threshold].copy()
                    rows_filtered_out = initial_row_count - len(filtered_df)
                    if rows_filtered_out > 0:
                        Log.Log_Info(current_ini_log_ctx, f"日期篩選 ({processing_start_threshold.strftime('%Y-%m-%d %H:%M:%S')}): {rows_filtered_out} 列被移除。")
                    
                    if filtered_df.empty:
                        Log.Log_Info(current_ini_log_ctx, f"日期篩選後，檔案 {excel_file_path_param} 沒有新資料可處理。")
                        if latest_date_in_original_file and latest_date_in_original_file > processing_start_threshold:
                            update_running_rec(current_ini_log_ctx, current_running_rec_path_param, latest_date_in_original_file)
                        return
                else:
                    Log.Log_Error(current_ini_log_ctx, f'key_Start_Date_Time 欄位索引 {start_date_col_idx} 超出範圍。跳過日期篩選。')
        else:
            Log.Log_Info(current_ini_log_ctx, '未設定 key_Start_Date_Time，不進行日期篩選。')

        
        # Reset index to avoid out of bounds issues
        filtered_df = filtered_df.reset_index(drop=True)
        
        # Add SQL query columns to filtered_df
        filtered_df['part_number'] = None
        filtered_df['lot_number_9'] = None
        
        # Assuming ID is in column 2 (0-based index)
        sql_conn, sql_cursor = None, None
        try:
            sql_conn, sql_cursor = SQL.connSQL()
            if sql_conn:
                for idx in range(len(filtered_df)):
                    serial_num = filtered_df.iloc[idx, 3]  # Get ID from third column
                    if pd.notna(serial_num):
                        try:
                            part_num, lot_num_9 = SQL.selectSQL(sql_cursor, str(serial_num))
                            filtered_df.loc[idx, 'part_number'] = part_num
                            filtered_df.loc[idx, 'lot_number_9'] = lot_num_9
                        except Exception as e_sql:
                            Log.Log_Error(current_ini_log_ctx, f"序列號 {serial_num}: SQL 查詢失敗: {e_sql}")
                    else:
                        Log.Log_Error(current_ini_log_ctx, f"Excel 資料列索引 {idx}: 第三欄ID為空值，無法查詢 SQL。")
            else:
                Log.Log_Error(current_ini_log_ctx, "資料庫連線失敗。")
        finally:
            if sql_conn:
                SQL.disconnSQL(sql_conn, sql_cursor)
        
        filtered_df = filtered_df.dropna(subset=['part_number'])
        # Get XY coordinates first

        xy_coords = {}
        try:
            for i in range(1, 6):
                row_idx = i  # Row index matches the point number
                if row_idx < df_xy_coords.shape[0]:  # Check if row exists
                    xy_coords[f'X{i}'] = df_xy_coords.iloc[row_idx, 1] if 1 < df_xy_coords.shape[1] else None
                    xy_coords[f'Y{i}'] = df_xy_coords.iloc[row_idx, 2] if 2 < df_xy_coords.shape[1] else None
                else:
                    xy_coords[f'X{i}'] = None
                    xy_coords[f'Y{i}'] = None
        except Exception as e:
            Log.Log_Error(current_ini_log_ctx, f'讀取 XY 座標時發生錯誤: {e}')
            # Initialize coordinates with None if error occurs 
            for i in range(1, 6):
                xy_coords[f'X{i}'] = None
                xy_coords[f'Y{i}'] = None

        # Add XY coordinates as new columns, keeping existing data
        for col, value in xy_coords.items():
            filtered_df[col] = value

        # Validate data types according to fields_map specifications
        for key, (col_spec, dtype) in fields_map.items():
            try:
                if col_spec.isdigit():
                    col_idx = int(col_spec)
                    if 0 <= col_idx < filtered_df.shape[1]:
                        if dtype == 'float':
                            filtered_df.iloc[:, col_idx] = pd.to_numeric(filtered_df.iloc[:, col_idx], errors='coerce')
                        elif dtype == 'int':
                            filtered_df.iloc[:, col_idx] = pd.to_numeric(filtered_df.iloc[:, col_idx], errors='coerce').astype('Int64')
                        elif dtype == 'datetime':
                            filtered_df.iloc[:, col_idx] = pd.to_datetime(filtered_df.iloc[:, col_idx], errors='coerce')
                        elif dtype == 'bool':
                            filtered_df.iloc[:, col_idx] = filtered_df.iloc[:, col_idx].map({'True': True, 'False': False, True: True, False: False})
                        elif dtype == 'str':
                            filtered_df.iloc[:, col_idx] = filtered_df.iloc[:, col_idx].astype(str)
                        
                        # Log any conversion failures (NaN values)
                        nan_count = filtered_df.iloc[:, col_idx].isna().sum()
                        if nan_count > 0:
                            #Log.Log_Error(current_ini_log_ctx, f"欄位 {key} (索引 {col_idx}) 有 {nan_count} 個值無法轉換為 {dtype} 型態")
                            # Remove rows with NaN values in this column
                            filtered_df = filtered_df[filtered_df.iloc[:, col_idx].notna()].copy()
                            Log.Log_Error(current_ini_log_ctx, f"已刪除第 {idx} 列，因欄位 {key} 資料型態不符")
            except Exception as e:
                # Remove the row with invalid data type
                filtered_df = filtered_df.drop(filtered_df.index[idx])
                Log.Log_Error(current_ini_log_ctx, f"已刪除第 {idx} 列，因欄位 {key} 資料型態不符")
        # --- 處理每一列資料 ---           
        # Rename first column to "Start_Date_Time"
        filtered_df = filtered_df.rename(columns={0:'Start_Date_Time'})
        filtered_df = filtered_df.rename(columns={1:'Def_Part_Number'})
        filtered_df = filtered_df.rename(columns={2:'Sem_Number'})
        filtered_df = filtered_df.rename(columns={3:'Serial_Number'})
        filtered_df = filtered_df.rename(columns={4:'Operator'})
        filtered_df = filtered_df.rename(columns={5:'Pitch1'})
        filtered_df = filtered_df.rename(columns={6:'Pitch2'})
        filtered_df = filtered_df.rename(columns={7:'Pitch3'})
        filtered_df = filtered_df.rename(columns={8:'Pitch4'})
        filtered_df = filtered_df.rename(columns={9:'Pitch5'})
        filtered_df = filtered_df.rename(columns={10:'Space1'})
        filtered_df = filtered_df.rename(columns={11:'Space2'})
        filtered_df = filtered_df.rename(columns={12:'Space3'})
        filtered_df = filtered_df.rename(columns={13:'Space4'})
        filtered_df = filtered_df.rename(columns={14:'Space5'})
        filtered_df = filtered_df.rename(columns={15:'Duty1'})
        filtered_df = filtered_df.rename(columns={16:'Duty2'})
        filtered_df = filtered_df.rename(columns={17:'Duty3'})
        filtered_df = filtered_df.rename(columns={18:'Duty4'})
        filtered_df = filtered_df.rename(columns={19:'Duty5'})
        filtered_df = filtered_df.rename(columns={20:'Duty_Avg'})
        filtered_df = filtered_df.rename(columns={21:'Duty1_3Sigma'})
        filtered_df = filtered_df.rename(columns={22:'Result'})
        
        # Get current timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        # Create CSV filename using ini filename and timestamp
        csv_filename = f"{ini_filename_base}_{timestamp}.csv"
        csv_filepath = os.path.join(CSV_path, csv_filename)
        
        # Create directory if it doesn't exist
        if not os.path.exists(CSV_path):
            os.makedirs(CSV_path)
            
        # Write dataframe to CSV
        try:
            filtered_df.to_csv(csv_filepath, index=False, encoding='utf-8')
            Log.Log_Info(current_ini_log_ctx, f"資料已寫入CSV檔案: {csv_filepath}")
        except Exception as e:
            Log.Log_Error(current_ini_log_ctx, f"寫入CSV檔案時發生錯誤: {e}")
        
        #generate_xml_file(data_dict_for_xml)


    # ---遍歷輸入路徑並處理 Excel 檔案---
    for an_input_path in input_paths:
        Log.Log_Info(current_ini_log_ctx, f"掃描輸入路徑: {an_input_path}，模式: {file_name_pattern}")
        try:
            excel_files_found = [f for f in glob.glob(os.path.join(an_input_path, file_name_pattern), recursive=True) if "コピ" not in os.path.basename(f)]
        except Exception as e_glob:
            Log.Log_Error(current_ini_log_ctx, f"掃描路徑 {an_input_path} 時發生錯誤: {e_glob}")
            continue

        valid_excel_files = [f for f in excel_files_found if not os.path.basename(f).startswith('~$') and os.path.isfile(f)]

        if not valid_excel_files:
            Log.Log_Info(current_ini_log_ctx, f"路徑 {an_input_path} 中未找到符合模式 {file_name_pattern} 的 Excel 檔案。")
            continue
        
        # *** MODIFIED: Use intermediate_data_dir read from INI ***
        if not intermediate_data_dir: # Check if path was read successfully
             Log.Log_Error(current_ini_log_ctx, "中間資料目錄 (intermediate_data_path) 未設定，無法複製檔案。")
             return # Or skip this an_input_path

        if not os.path.exists(intermediate_data_dir):
            try:
                os.makedirs(intermediate_data_dir)
                Log.Log_Info(current_ini_log_ctx, f"已建立中間資料目錄: {intermediate_data_dir}")
            except OSError as e_mkdir:
                Log.Log_Error(current_ini_log_ctx, f"建立中間目錄 {intermediate_data_dir} 失敗: {e_mkdir}。")
                continue # Skip processing for this input_path if intermediate dir cannot be made
            
        for excel_file_src_path in valid_excel_files:
            copied_excel_path = os.path.join(intermediate_data_dir, os.path.basename(excel_file_src_path))
            try:
                shutil.copy(excel_file_src_path, copied_excel_path)
                Log.Log_Info(current_ini_log_ctx, f"複製 Excel 檔案 {excel_file_src_path} 至 {copied_excel_path}")
                process_single_excel_file(copied_excel_path, running_rec_path, data_date_days)
            except FileNotFoundError:
                 Log.Log_Error(current_ini_log_ctx, f"來源檔案 {excel_file_src_path} 在複製時未找到。")
            except shutil.Error as e_copy:
                Log.Log_Error(current_ini_log_ctx, f"複製檔案 {excel_file_src_path} 至 {copied_excel_path} 失敗: {e_copy}")
            except Exception as e_proc_excel:
                Log.Log_Error(current_ini_log_ctx, f"處理檔案 {copied_excel_path} (源自 {excel_file_src_path}) 時發生未預期錯誤: {e_proc_excel}")
    
    Log.Log_Info(current_ini_log_ctx, f'設定檔 {config_path} 所有路徑處理完畢。')

def main():
    """程式主進入點"""
    global global_log_file

    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    overall_log_filename = os.path.join(script_dir, 'program_execution.log')
    global_log_file = overall_log_filename
    setup_logging(global_log_file)

    Log.Log_Info(global_log_file, f"程式啟動 (腳本路徑: {script_dir})")

    ini_search_pattern = os.path.join(script_dir, "*.ini")
    ini_files_to_process = glob.glob(ini_search_pattern)

    if not ini_files_to_process:
        Log.Log_Error(global_log_file, f"在目錄 {script_dir} 下找不到任何 .ini 設定檔。")
    else:
        Log.Log_Info(global_log_file, f"找到 {len(ini_files_to_process)} 個 .ini 設定檔: {', '.join(map(os.path.basename, ini_files_to_process))}")
        for ini_path in ini_files_to_process:
            process_ini_file(ini_path, overall_log_filename) 
            global_log_file = overall_log_filename # 切回主日誌
            Log.Log_Info(global_log_file, f"完成設定檔 {os.path.basename(ini_path)} 的處理循環。")

    Log.Log_Info(global_log_file, '所有 .ini 設定檔處理完畢，程式即將結束。')

if __name__ == '__main__':
    main()