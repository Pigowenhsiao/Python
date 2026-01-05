# Python 程式 - 讀取所有 .ini 檔案，執行資料處理並產生 XML 檔案
import os
import sys
import glob
import shutil
import logging
import random 
import pandas as pd
from configparser import ConfigParser, NoSectionError, NoOptionError
from datetime import datetime, timedelta

try:
    sys.path.append('../MyModule')
    import Log
    import SQL
except ImportError as e:
    print(f"Critical Error: Could not import MyModule (Log or SQL). Ensure it's in PYTHONPATH. Details: {e}")
    sys.exit(1)
DEFAULT_FALLBACK_DAYS = 30
SERIAL_NUMBER_ORIGINAL_COL_IDX = 3

def _parse_rename_map_from_config(config_obj, log_ctx):
    rename_map = {}
    try:
        if config_obj.has_section('ColumnMapping'):
            mapping_str = config_obj.get('ColumnMapping', 'rename_map')
            for mapping in mapping_str.strip().split(','):
                if mapping.strip():
                    idx, name = mapping.strip().split(':')
                    rename_map[int(idx)] = name.strip()
            Log.Log_Info(log_ctx, f"Successfully loaded {len(rename_map)} column mappings from config")
        else:
            Log.Log_Warning(log_ctx, "ColumnMapping section not found in config, using empty mapping")
    except Exception as e:
        Log.Log_Error(log_ctx, f"Error parsing rename_map from config: {e}")
    return rename_map

# global_log_file: This variable is assigned in process_ini_file and main.
# setup_logging configures the root logger based on the path passed to it.

def setup_logging(log_file_path):
    try:
        log_dir = os.path.dirname(log_file_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s [%(module)s.%(funcName)s] - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
    except OSError as e:
        print(f"Critical error setting up log file {log_file_path}: {e}. Some logs may be lost.")
    except Exception as e_gen:
        print(f"Unexpected error during logging setup: {e_gen}")

def update_running_rec(log_ctx, running_rec_path, end_date):
    try:
        with open(running_rec_path, 'w', encoding='utf-8') as f:
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))
        Log.Log_Info(log_ctx, f"Running record file {running_rec_path} updated, end date is {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        Log.Log_Error(log_ctx, f"Error updating running record file {running_rec_path}: {e}")

def read_running_rec(log_ctx, running_rec_path, default_days_ago=DEFAULT_FALLBACK_DAYS):
    fallback_date = datetime.today() - timedelta(days=default_days_ago)
    if not os.path.exists(running_rec_path):
        try:
            rec_dir = os.path.dirname(running_rec_path)
            if rec_dir and not os.path.exists(rec_dir):
                os.makedirs(rec_dir)
            with open(running_rec_path, 'w', encoding='utf-8') as f:
                f.write('') 
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

def generate_xml(output_path_xml: str, site_xml: str, product_family_xml: str,
                 operation_xml: str, test_station_xml: str, # test_station_xml 來自 INI，但 XML 內容中 TestStation="NA"
                 current_time_for_sn: str, # 用於檔名中的 Serialnumber
                 config_parser_obj: ConfigParser, # ConfigParser 物件
                 csv_file_path_for_xml:str, # CSV 檔案的完整路徑
                 log_ctx: str ) -> None:
    current_time_content_base = datetime.now().strftime('%Y-%m-%d %H:%M:') 
    random_seconds_suffix = f"{random.randint(0, 59):02}" # 秒數用隨機數
    current_time_content_standard = current_time_content_base + random_seconds_suffix
    current_time_content_iso = current_time_content_standard.replace(' ', 'T')

    operation_for_content = operation_xml # 使用傳入的 operation 參數
    key_part_number_content = 'UNKNOWNPN' # XML 內容中使用的 PartNumber
    xml_filename = (
        f"Site={site_xml},ProductFamily={product_family_xml},Operation={operation_for_content},"
        f"Partnumber={key_part_number_content}," # 檔名中的 PartNumber
        f"Serialnumber={current_time_for_sn}," # 檔名中的 Serialnumber (來自參數)
        f"Testdate={current_time_content_iso}.xml" # 檔名中的 Testdate (內部產生)
    ).replace(':', '.').replace('/', '-').replace('\\', '-') # 清理檔名中的非法字元
    
    xml_filepath_full = os.path.join(output_path_xml, xml_filename)

    try:
        with open(xml_filepath_full, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n')
            f.write(f'    <Result startDateTime="{current_time_content_iso}" endDateTime="{current_time_content_iso}" Result="Passed">\n') # Result 硬編碼為 Passed
            f.write(f'        <Header SerialNumber="{current_time_for_sn}" PartNumber="{key_part_number_content}" Operation="{operation_for_content}" TestStation="NA" Operator="NA" StartTime="{current_time_content_iso}" Site="{site_xml}" LotNumber="" Quantity=""/>\n')
            f.write('        <HeaderMisc>\n')
            f.write('            <Item Description=""/>\n')
            f.write('        </HeaderMisc>\n')
            f.write(f'        <TestStep Name="{operation_for_content}" startDateTime="{current_time_content_iso}" endDateTime="{current_time_content_iso}" Status="Passed">\n') # Status 硬編碼為 Passed
            f.write(f'            <Data DataType="Table" Name="tbl_{operation_for_content.upper()}" Value="{csv_file_path_for_xml}" CompOperation="LOG"/>\n') # 指向 CSV 檔案
            f.write('        </TestStep>\n')
            f.write('    </Result>\n')
            f.write('</Results>\n')
        Log.Log_Info(log_ctx, f"XML File Created: {xml_filepath_full}")
    except Exception as e_xml_write:
        Log.Log_Error(log_ctx, f"Failed to write XML file {xml_filepath_full}: {e_xml_write}")

def _read_excel_sheets(excel_path, main_sheet_name, main_cols_spec, xy_sheet_name, xy_cols_spec, log_ctx):
    df_main, df_xy_coords = None, None
    try:
        Log.Log_Info(log_ctx, f"Reading main data from sheet '{main_sheet_name}', columns '{main_cols_spec}' in {excel_path}")
        df_main = pd.read_excel(excel_path, header=None, sheet_name=main_sheet_name, usecols=str(main_cols_spec), skiprows=100)
        Log.Log_Info(log_ctx, f"Reading XY data from sheet '{xy_sheet_name}', columns '{xy_cols_spec}' in {excel_path}")
        df_xy_coords = pd.read_excel(excel_path, header=None, sheet_name=xy_sheet_name, usecols=str(xy_cols_spec))
        
        if df_main is not None and not df_main.empty:
            df_main.columns = range(df_main.shape[1])
            df_main.dropna(subset=[0], inplace=True) 
        else:
            Log.Log_Warning(log_ctx, f"Main data sheet '{main_sheet_name}' in {excel_path} is empty or read failed.")

        if df_xy_coords is not None and not df_xy_coords.empty:
            df_xy_coords.columns = range(df_xy_coords.shape[1])
        else:
             Log.Log_Warning(log_ctx, f"XY coordinates sheet '{xy_sheet_name}' in {excel_path} is empty or read failed.")
             df_xy_coords = pd.DataFrame() 

        return df_main, df_xy_coords
    except FileNotFoundError:
        Log.Log_Error(log_ctx, f'Excel file not found: {excel_path}.')
    except ValueError as ve: 
        Log.Log_Error(log_ctx, f'Error reading Excel file {excel_path} (sheet/column issue or format problem): {ve}')
    except Exception as e: 
        Log.Log_Error(log_ctx, f'Unknown error reading Excel file {excel_path}: {e}')
    return None, pd.DataFrame() 

def _apply_date_filter(df, fields_map, running_rec_path, date_filter_days, log_ctx):
    if df is None or df.empty:
        return df, None

    processing_start_threshold = read_running_rec(log_ctx, running_rec_path, date_filter_days)
    latest_date_in_original_file = None
    
    start_date_col_spec = fields_map.get('key_Start_Date_Time', (None, None))[0]

    if not start_date_col_spec or not start_date_col_spec.isdigit():
        Log.Log_Warning(log_ctx, f"Date filter skipped: 'key_Start_Date_Time' not configured correctly in fields_map (col_spec: {start_date_col_spec}).")
        return df.copy(), None 

    start_date_col_idx = int(start_date_col_spec)
    if not (0 <= start_date_col_idx < df.shape[1]):
        Log.Log_Error(log_ctx, f"Date filter skipped: 'key_Start_Date_Time' column index {start_date_col_idx} is out of bounds.")
        return df.copy(), None

    df_filtered = df.copy()
    original_dates_series = pd.to_datetime(df_filtered.iloc[:, start_date_col_idx], errors='coerce')

    if not original_dates_series.dropna().empty:
        latest_date_in_original_file = original_dates_series.dropna().max()

    df_filtered['__parsed_date__'] = original_dates_series
    initial_row_count = len(df_filtered)
    df_filtered.dropna(subset=['__parsed_date__'], inplace=True) 
    df_filtered = df_filtered[df_filtered['__parsed_date__'] >= processing_start_threshold].copy() 
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
    df_enriched['part_number'] = None 
    df_enriched['lot_number_9'] = None 

    if not (0 <= serial_num_col_idx < df_enriched.shape[1]):
        Log.Log_Warning(log_ctx, f"SQL data enrichment skipped: Serial number column index {serial_num_col_idx} is out of bounds.")
        return df_enriched 

    sql_conn, sql_cursor = None, None
    try:
        sql_conn, sql_cursor = SQL.connSQL()
        if not sql_conn:
            Log.Log_Error(log_ctx, "SQL data enrichment failed: Could not connect to database.")
            return df_enriched 

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
                Log.Log_Warning(log_ctx, f"Skipping SQL query for row index {idx}: S/N at original column {serial_num_col_idx} is empty or invalid.")
    except Exception as e_sql_conn:
        Log.Log_Error(log_ctx, f"SQL connection or general error: {e_sql_conn}")
    finally:
        if sql_conn:
            SQL.disconnSQL(sql_conn, sql_cursor)

    df_enriched.dropna(subset=['part_number'], inplace=True)
    return df_enriched.reset_index(drop=True)

def _merge_xy_coordinates(df, df_xy_coords, log_ctx):
    if df is None or df.empty:
        return df
    
    df_merged = df.copy()
    if df_xy_coords is None or df_xy_coords.empty:
        Log.Log_Warning(log_ctx, "XY coordinates data is empty. Skipping merge.")
        for i in range(1, 6): 
            df_merged[f'X{i}'] = None
            df_merged[f'Y{i}'] = None
        return df_merged

    try:
        for i in range(1, 6): 
            point_row_index = i - 1 
            x_val, y_val = None, None
            if point_row_index < df_xy_coords.shape[0]:
                x_val = df_xy_coords.iloc[point_row_index, 1] if 1 < df_xy_coords.shape[1] else None
                y_val = df_xy_coords.iloc[point_row_index, 2] if 2 < df_xy_coords.shape[1] else None
            df_merged[f'X{i}'] = x_val
            df_merged[f'Y{i}'] = y_val
    except Exception as e_xy_merge:
        Log.Log_Error(log_ctx, f"Error merging XY coordinates: {e_xy_merge}")
        for i_err in range(1, 6): 
            df_merged[f'X{i_err}'] = None
            df_merged[f'Y{i_err}'] = None
    return df_merged

def _apply_type_conversions(df, fields_map, log_ctx):
    if df is None or df.empty:
        return df

    df_converted = df.copy()
    rows_to_drop_indices = set() 

    for row_idx in range(len(df_converted)):
        for xml_key, (col_spec, dtype_str) in fields_map.items():
            if not col_spec.isdigit(): 
                continue 
            
            original_col_idx = int(col_spec)
            if not (0 <= original_col_idx < df_converted.shape[1]):
                continue

            original_value = df_converted.iloc[row_idx, original_col_idx]
            converted_value = original_value 

            if pd.isna(original_value):
                converted_value = None 
            else:
                try:
                    if dtype_str == 'float': converted_value = float(original_value)
                    elif dtype_str == 'int': converted_value = int(original_value)
                    elif dtype_str == 'datetime':
                        converted_value = pd.to_datetime(original_value).strftime('%Y/%m/%d %H:%M:%S')
                    elif dtype_str == 'bool':
                        if isinstance(original_value, str):
                            if original_value.lower() == 'true': converted_value = True
                            elif original_value.lower() == 'false': converted_value = False
                            else: raise ValueError("Invalid string for bool conversion")
                        else: converted_value = bool(original_value) 
                    elif dtype_str == 'str': converted_value = str(original_value)
                except (ValueError, TypeError) as e_conv:
                    Log.Log_Warning(log_ctx, f"Type conversion error for row {row_idx}, column '{xml_key}' (original index {original_col_idx}), value '{original_value}' to {dtype_str}: {e_conv}. Marking row for removal.")
                    rows_to_drop_indices.add(row_idx)
                    break 
            
            df_converted.iat[row_idx, original_col_idx] = converted_value
    
    if rows_to_drop_indices:
        df_converted.drop(index=list(rows_to_drop_indices), inplace=True)
        Log.Log_Info(log_ctx, f"Removed {len(rows_to_drop_indices)} rows due to type conversion failures.")
        df_converted = df_converted.reset_index(drop=True)
        
    return df_converted

def _perform_column_renaming(df, rename_map):
    if df is None or df.empty:
        return df
    df_renamed = df.copy()
    applicable_rename_map = {k: v for k, v in rename_map.items() if k in df_renamed.columns}
    df_renamed.rename(columns=applicable_rename_map, inplace=True)
    return df_renamed

def _save_to_csv(df, csv_base_path, ini_filename_base, log_ctx) -> tuple[str | None, str | None]:
    if df is None or df.empty:
        Log.Log_Info(log_ctx, "CSV saving skipped: DataFrame is empty.")
        return None, None

    current_timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S') # 給 CSV 和 XML 使用的時間戳
    csv_filename = f"{ini_filename_base}_{current_timestamp_str}.csv"
    csv_filepath_full = os.path.join(csv_base_path, csv_filename)
    
    try:
        if not os.path.exists(csv_base_path):
            os.makedirs(csv_base_path)
            Log.Log_Info(log_ctx, f"Created CSV directory: {csv_base_path}")
        df.to_csv(csv_filepath_full, index=False, encoding='utf-8')
        Log.Log_Info(log_ctx, f"Data written to CSV file: {csv_filepath_full}")
        return csv_filepath_full, current_timestamp_str
    except Exception as e_csv:
        Log.Log_Error(log_ctx, f"Error writing CSV file {csv_filepath_full}: {e_csv}")
        return None, None


# --- 主 Excel 檔案處理函數 ---
def process_single_excel_file(
        excel_file_path: str, 
        running_rec_path_cfg: str, 
        date_filter_days_cfg: int, 
        log_ctx_main: str, 
        sheet_name_cfg: str, 
        data_columns_cfg: str, 
        xy_sheet_name_cfg: str, 
        xy_columns_cfg: str, 
        fields_map_cfg: dict, 
        ini_filename_base_id: str, 
        csv_path_cfg: str, 
        site_cfg: str, 
        product_family_cfg: str, 
        operation_cfg: str, 
        test_station_cfg: str, 
        output_path_cfg: str,
        config_parser_for_xml: ConfigParser 
    ):
    
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
        return

    df_with_sql = _enrich_with_sql_data(df_filtered, SERIAL_NUMBER_ORIGINAL_COL_IDX, log_ctx_main)
    if df_with_sql.empty:
        Log.Log_Info(log_ctx_main, f"No data left in {excel_file_path} after SQL enrichment. Skipping further processing.")
        return
        
    df_with_xy = _merge_xy_coordinates(df_with_sql, df_xy, log_ctx_main)
    
    df_typed = _apply_type_conversions(df_with_xy, fields_map_cfg, log_ctx_main)
    if df_typed.empty:
        Log.Log_Info(log_ctx_main, f"No data left in {excel_file_path} after type conversions. Skipping further processing.")
        if latest_date and latest_date > read_running_rec(log_ctx_main, running_rec_path_cfg, date_filter_days_cfg): 
             update_running_rec(log_ctx_main, running_rec_path_cfg, latest_date)
        return

    df_renamed = _perform_column_renaming(df_typed, settings.rename_map)

    generated_csv_path, csv_timestamp = _save_to_csv(df_renamed, csv_path_cfg, ini_filename_base_id, log_ctx_main)
    
    if generated_csv_path and csv_timestamp:
        generate_xml(
            output_path_xml=output_path_cfg, # XML 檔案的輸出目錄
            site_xml=site_cfg,
            product_family_xml=product_family_cfg,
            operation_xml=operation_cfg,
            test_station_xml=test_station_cfg, # 來自 INI 的 TestStation
            current_time_for_sn=csv_timestamp, # 使用 CSV 的時間戳作為 XML 檔名中的 SerialNumber
            config_parser_obj=config_parser_for_xml, # 傳入 ConfigParser 物件
            csv_file_path_for_xml=generated_csv_path, # 指向剛產生的 CSV 檔案
            log_ctx=log_ctx_main
        )
    else:
        Log.Log_Warning(log_ctx_main, f"Skipping XML generation for {excel_file_path} because CSV was not saved.")
    
    Log.Log_Info(log_ctx_main, f"Successfully processed Excel file: {excel_file_path}")


def _read_and_parse_ini_config(config_file_path, log_ctx):
    """讀取並解析 INI 設定檔"""
    config = ConfigParser()
    try:
        config_content = []
        with open(config_file_path, 'r', encoding='utf-8') as f_obj:
            for line in f_obj:
                if not line.strip().startswith('#'): 
                    config_content.append(line)
        config.read_string("".join(config_content))
        Log.Log_Info(log_ctx, f"Successfully read and parsed INI config: {config_file_path}")
        return config
    except Exception as e:
        Log.Log_Error(log_ctx, f"Critical error reading INI config file {config_file_path}: {e}")
        return None

class IniSettings: 
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
        self.test_station = "NA" # 新增: TestStation 的預設值
        self.file_name_pattern = None
        self.csv_path = None
        self.data_date_days = DEFAULT_FALLBACK_DAYS
        self.is_valid = False 
        self.rename_map = {}  # 新增欄位映射屬性

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
            raise NoSectionError("DataFields section or 'fields' option missing.") 

        settings.site = config_obj.get('Basic_info', 'Site')
        settings.product_family = config_obj.get('Basic_info', 'ProductFamily')
        settings.operation = config_obj.get('Basic_info', 'Operation')
        settings.test_station = config_obj.get('Basic_info', 'TestStation', fallback="NA") # 讀取 TestStation，若無則為 "NA"
        settings.file_name_pattern = config_obj.get('Basic_info', 'file_name_pattern')
        settings.csv_path = config_obj.get('Paths', 'CSV_path')
        
        try:
            settings.data_date_days = config_obj.getint('Basic_info', 'Data_date')
            Log.Log_Info(log_ctx_settings, f"Data_date from config: {settings.data_date_days} days.")
        except ValueError:
            Log.Log_Error(log_ctx_settings, f"Invalid integer for Data_date in [Basic_info]. Using default: {settings.data_date_days} days.")
        except NoOptionError:
            Log.Log_Error(log_ctx_settings, f"Data_date option missing in [Basic_info]. Using default: {settings.data_date_days} days.")
        
        # 新增讀取欄位映射的部分
        settings.rename_map = _parse_rename_map_from_config(config_obj, log_ctx_settings)
        
        settings.is_valid = True 
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
        Log.Log_Warning(log_ctx_fields, "Fields configuration lines are empty. fields_map will be empty.")
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
        Log.Log_Warning(log_ctx_fields, "No valid field mappings were parsed from the fields configuration.")
    else:
        Log.Log_Info(log_ctx_fields, f"Parsed {len(fields_map_parsed)} field mappings.")
    return fields_map_parsed


# --- INI 設定檔處理主函數 ---
def process_ini_file(ini_config_path, overall_program_log_ctx):
    """處理單個 .ini 設定檔"""
    ini_filename_base = os.path.splitext(os.path.basename(ini_config_path))[0]
    current_processing_log_ctx = overall_program_log_ctx
    Log.Log_Info(current_processing_log_ctx, f"Starting processing for INI file: {ini_config_path}")

    config_object = _read_and_parse_ini_config(ini_config_path, current_processing_log_ctx)
    if not config_object:
        return 

    settings = _extract_settings_from_config(config_object, ini_config_path, current_processing_log_ctx)
    if not settings.is_valid:
        Log.Log_Error(current_processing_log_ctx, f"Failed to load valid settings from {ini_config_path}. Aborting processing for this INI.")
        return

    log_folder_name_date = str(datetime.today().date()) 
    ini_specific_log_dir = os.path.join(settings.log_path_from_ini, log_folder_name_date)
    ini_specific_log_file_path = os.path.join(ini_specific_log_dir, f'{ini_filename_base}.log')
    setup_logging(ini_specific_log_file_path)
    current_processing_log_ctx = ini_specific_log_file_path 
    Log.Log_Info(current_processing_log_ctx, f"Dedicated logging for {ini_filename_base} configured at: {current_processing_log_ctx}")
    Log.Log_Info(current_processing_log_ctx, f"Continuing with settings from {ini_config_path} using its dedicated log.")


    fields_map_obj = _parse_fields_map_from_lines(settings.fields_config_raw_lines, current_processing_log_ctx)
    if not fields_map_obj:
        Log.Log_Error(current_processing_log_ctx, f"No field mappings loaded from {ini_config_path}. Cannot process Excel files.")
        return

    if not os.path.exists(settings.output_path):
        try:
            os.makedirs(settings.output_path)
            Log.Log_Info(current_processing_log_ctx, f"Created output directory: {settings.output_path}")
        except OSError as e_mkdir:
            Log.Log_Error(current_processing_log_ctx, f"Failed to create output directory {settings.output_path}: {e_mkdir}. XML storage will fail.")
            return 

    for input_source_path in settings.input_paths:
        Log.Log_Info(current_processing_log_ctx, f"Scanning input path: {input_source_path} with pattern: {settings.file_name_pattern}")
        try:
            excel_files_found_list = [
                f for f in glob.glob(os.path.join(input_source_path, settings.file_name_pattern), recursive=False)
                if "コピ" not in os.path.basename(f) 
            ]
        except Exception as e_glob_err:
            Log.Log_Error(current_processing_log_ctx, f"Error scanning path {input_source_path}: {e_glob_err}")
            continue 

        valid_excel_files_list = [
            f for f in excel_files_found_list 
            if not os.path.basename(f).startswith('~$') and os.path.isfile(f) 
        ]

        if not valid_excel_files_list:
            Log.Log_Info(current_processing_log_ctx, f"No valid Excel files matching pattern '{settings.file_name_pattern}' found in {input_source_path}.")
            continue

        if not settings.intermediate_data_dir:
            Log.Log_Error(current_processing_log_ctx, "Intermediate data directory (intermediate_data_path) is not set. Cannot copy files.")
            return 

        if not os.path.exists(settings.intermediate_data_dir):
            try:
                os.makedirs(settings.intermediate_data_dir)
                Log.Log_Info(current_processing_log_ctx, f"Created intermediate data directory: {settings.intermediate_data_dir}")
            except OSError as e_mkdir_interm_err:
                Log.Log_Error(current_processing_log_ctx, f"Failed to create intermediate directory {settings.intermediate_data_dir}: {e_mkdir_interm_err}.")
                continue 
            
        for excel_src_full_path in valid_excel_files_list:
            copied_excel_target_path = os.path.join(settings.intermediate_data_dir, os.path.basename(excel_src_full_path))
            try:
                shutil.copy(excel_src_full_path, copied_excel_target_path)
                Log.Log_Info(current_processing_log_ctx, f"Copied Excel file: {excel_src_full_path} -> {copied_excel_target_path}")
                
                process_single_excel_file(
                    excel_file_path=copied_excel_target_path,
                    running_rec_path_cfg=settings.running_rec_path,
                    date_filter_days_cfg=settings.data_date_days,
                    log_ctx_main=current_processing_log_ctx, 
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
                    test_station_cfg=settings.test_station, # 傳入 TestStation
                    output_path_cfg=settings.output_path,
                    config_parser_for_xml=config_object # 傳入 ConfigParser 物件
                )
            except FileNotFoundError: 
                Log.Log_Error(current_processing_log_ctx, f"Source file {excel_src_full_path} not found during copy operation.")
            except shutil.Error as e_shutil_copy:
                Log.Log_Error(current_processing_log_ctx, f"Error copying file {excel_src_full_path} to {copied_excel_target_path}: {e_shutil_copy}")
            except Exception as e_process_single: 
                Log.Log_Error(current_processing_log_ctx, f"Unexpected error processing file {copied_excel_target_path} (from {excel_src_full_path}): {e_process_single}")
    
    Log.Log_Info(current_processing_log_ctx, f"Finished processing all input paths for INI file: {ini_config_path}")


# --- 程式主進入點 ---
def main():
    """程式主進入點"""
    script_directory = os.path.dirname(os.path.abspath(__file__)) 
    
    overall_program_log_file = os.path.join(script_directory, 'program_execution.log')
    setup_logging(overall_program_log_file) 

    Log.Log_Info(overall_program_log_file, f"Program started (Script path: {script_directory})")

    ini_search_pattern = os.path.join(script_directory, "*.ini") 
    ini_files_to_process_list = glob.glob(ini_search_pattern)

    if not ini_files_to_process_list:
        Log.Log_Error(overall_program_log_file, f"No .ini configuration files found in directory: {script_directory}.")
    else:
        Log.Log_Info(overall_program_log_file, f"Found {len(ini_files_to_process_list)} .ini configuration file(s): {', '.join(map(os.path.basename, ini_files_to_process_list))}")
        for ini_file_path_item in ini_files_to_process_list:
            process_ini_file(ini_file_path_item, overall_program_log_file)
            
            setup_logging(overall_program_log_file) 
            Log.Log_Info(overall_program_log_file, f"Completed processing cycle for INI file: {os.path.basename(ini_file_path_item)}.")

    Log.Log_Info(overall_program_log_file, "All .ini configuration files processed. Program finishing.")

if __name__ == '__main__':
    main()