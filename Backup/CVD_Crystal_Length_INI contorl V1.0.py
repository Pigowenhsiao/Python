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
# 假設這些模組在 '../MyModule' 路徑下且功能穩定
sys.path.append('../MyModule')
import Log
import SQL
# import Check # 未在此版本程式碼中使用，可考慮移除若確認不需要
# import Convert_Date # 未在此版本程式碼中使用
# import Row_Number_Func # 未在此版本程式碼中使用

# --- 常數 ---
DEFAULT_FALLBACK_DAYS = 30
INTERMEDIATE_DATA_DIR = '../DataFile/001_GRATING/' # 中間 Excel 檔案複製目錄

# --- 全域變數 ---
# global_log_file 用於指示 Log 模組應將日誌寫入哪個檔案。
# 它在 main() 和 process_ini_file() 中被賦值。
global_log_file = None

# --- 日誌與記錄檔案相關工具函數 ---

def setup_logging(log_file_path):
    """配置日誌設定"""
    try:
        # 確保日誌檔案路徑的目錄存在
        log_dir = os.path.dirname(log_file_path)
        if log_dir and not os.path.exists(log_dir): # log_dir 可能為空字串若 log_file_path 只是檔名
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
            # 確保目錄存在
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
                    Log.Log_Warning(current_log_ctx, f"執行記錄檔案 {running_rec_path} 內容格式錯誤 ('{content}')。將使用預設回溯天數: {default_days_ago} 天。")
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
    global global_log_file # 此函數會設定 ini 專用的 global_log_file
    
    ini_filename_base = os.path.splitext(os.path.basename(config_path))[0]
    current_ini_log_ctx = overall_log_ctx # 預設使用 overall log 直到專用 log 設定完成

    Log.Log_Info(current_ini_log_ctx, f"開始處理設定檔: {config_path}")

    config = ConfigParser()
    try:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config.read_file(line for line in config_file if not line.strip().startswith('#'))
    except Exception as e:
        Log.Log_Error(current_ini_log_ctx, f"讀取設定檔 {config_path} 時發生嚴重錯誤: {e}")
        return

    # --- 讀取設定 ---
    try:
        log_path_from_ini = config.get('Logging', 'log_path')
        
        # 設定此 INI 檔案的專用日誌
        log_folder_name = str(datetime.today().date())
        specific_log_folder_path = os