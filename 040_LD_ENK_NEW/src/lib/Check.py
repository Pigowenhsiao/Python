# floatもしくはintの箇所に文字列が入っていればFalseを返す
import numpy as np
import datetime
import pandas as pd
import sys

def Data_Type(key_to_type, data_dict):
    """
    檢查 data_dict 中每個 key 的型態是否符合 key_to_type 中定義的型態。
    key_to_type: 一個字典，定義了每個 key 的期望型態。
    data_dict: 要檢查的資料字典。
    返回: 一個包含型態錯誤的列表，若沒有錯誤則返回空列表。
    """
    
    errors = []  # 儲存錯誤的列表

    for key, expected_type in key_to_type.items():
        value = data_dict.get(key, None)  # 確保從字典中正確獲取值

        # 檢查空值
        if value is np.nan or str(value) == "nan":
            errors.append(f"Key '{key}' 的值為空")
            continue  # 跳過這個鍵值的後續檢查

        # 如果期望型態是字符串，允許空字串
        if expected_type is str and value == "":
            continue

        try:
            # 如果期望的是 float 型態
            if expected_type is float:
                data_dict[key] = float(value)  # 轉換為 float
                if 'e' in str(data_dict[key]):
                    data_dict[key] = int(data_dict[key])  # 將科學記號表示的數值轉為整數

            # 如果期望的是 datetime 型態
            elif expected_type == 'datetime':

                try:
                    data_dict[key] = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    errors.append(f"Key '{key}' 的值無法轉換為 datetime，實際為 {type(value).__name__}: {value}")
                    continue

            # 如果型態不匹配，記錄錯誤
            elif not isinstance(value, expected_type):
                errors.append(f"Key '{key}' 的型態錯誤，期望 {expected_type.__name__}，實際為 {type(value).__name__}")

        except (ValueError, TypeError) as e:
            errors.append(f"Key '{key}' 的值無法轉換為 {expected_type.__name__}，實際為 {type(value).__name__}: {e}")

    return errors  # 返回所有的錯誤

