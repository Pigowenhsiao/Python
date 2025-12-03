# LD-EML ENK 共用資料管線

這個目錄收納所有 F1/F2/F6/F7/F10/F11、Format1/Format2 腳本的共用版本，並用 `.ini` 檔案描述各站的差異。所有產出遵循 `001_GRATING/CVD_Grating_Common.py` 的 CSV 與指標 XML 規格。

## 目錄結構

- `main.py`：統一的進入點，透過 `--config` 指定單一 ini 或 `--all` 依序執行 `config` 內的機台 ini。
- `src/`：共用模組，包含設定載入、資料擷取（format1/format2 extractor）、CSV/XML 寫入器、驗證器等。
- `config/`：  
  - `base_format1.ini`、`base_format2.ini`：Format1/Format2 共用預設值。  
  - `F*_Format*.ini`：各機台實際設定；若要新增機台，只要複製適合的 base 並修改 `[general]` / `[paths]` / `[excel]` / `[filters]` 等區段。  
  - `mappings/`：由舊腳本自動轉出的欄位對應（B5/B8、PL MAP 等）。  
  - `keytypes_*.ini`：欄位型別定義，供驗證模組讀取。

## 重要注意事項

1. `config/F*_Format*.ini` 內的 `input_root`、`filename_patterns`、`excel.sheet_name` 等欄位先以 placeholder 填寫，請依實際量測路徑／檔名關鍵字／工作表名稱更新。  
2. Format2 的 `sheet_name`、`date_sheet_name`、`date_cell` 需對應 PL-MAP 原始檔，否則 extractor 無法定位時間戳。  
3. 若有新的 PartNumber 群組或 mapping，新增 `.ini` 至 `config/mappings/`，然後在機台 ini 的 `[group_rules]` / `[mappings]` 裡掛載即可。  
4. Pipeline 會將資料寫入 `config` 目錄下的 `../CSV/<機台>` 與 `../XML/<機台>`，可依需求調整路徑。  
5. 執行方式：

```bash
cd 040_LD_ENK_NEW
python main.py --config config/F2_Format1.ini
python main.py --all
```

CSV 會被 append，如果想依日期分檔，可調整 `[writer] csv_filename` 模板（支援 `{operation}`、`{date}` 變數）。

