# SPEC Drive Coding – E1_Qrun

> 本文件彙整並更新截至目前為止，**E1_Qrun** 專案的所有需求、規格與實作決策，作為後續開發、維護與審核的唯一依據。

---

## 1. 專案目的

* 每日掃描指定資料夾內的 Excel 檔案（每檔 = 一片 Wafer）。
* 依 INI 設定讀取 Excel 指定工作表與範圍。
* 計算量測欄位的 **MAX / MIN / AVG / STD**（向量化處理）。
* 補齊必要欄位（Serial_Number / Start_Date_Time / Part_Number 等）。
* 查詢資料庫（沿用既有 SQL 模組）。
* 產出 CSV（Table）與 Pointer XML。
* 具備 Dedup 機制，避免每日重複上傳。

---

## 2. 檔案與命名規則

### 2.1 檔案來源

* 目錄（INI 設定）：
  \\SAG-SFL-01.li.lumentuminc.net\User1\品証\User\光素子入検データ\EA先行評価\HL13E1\先行A

### 2.2 檔名規則（正式）

* 合法格式：

  ```
  Nxxxxxxx_Nxxxx先行結果.xlsm
  Nxxxxxxx_Nxxxx先行結果.xlsx
  ```

* 說明：

  * 第一段 `Nxxxxxxx`：Wafer ID（僅做追蹤）
  * 第二段 `Nxxxx`：**Lot ID**（核心識別）
  * 允許後綴文字（如「先行結果」）
  * **排除** 含有 `- Copy` 或 `copy` 的檔案

* INI 的 `file_name_patterns`：

  ```ini
  N???????_N????*.xlsm
  N???????_N????*.xlsx
  ```

* Python 以 Regex 做最終驗證：

  ```
  ^(N\d{7})_(N\d{4}).*?\.(xlsx|xlsm)$
  ```

---

## 3. 必要輸出欄位（CSV 必須包含）

| 欄位名稱            | 來源 / 規則                       |
| --------------- | ----------------------------- |
| Serial_Number   | Lot ID（檔名第二段 `N????`）         |
| Start_Date_Time | Excel 指定工作表/儲存格（見 §4）         |
| Part_Number     | 固定輸出 `HL13E1`（即使 mapping 對不到） |
| TESTER_ID       | Excel 主表 `HL13E1ﾃﾞｰﾀ!AY23`    |
| Waive_Leng_Cate | Lot Rule 對照表（INI）             |

---

## 4. Start_Date_Time 取得規則

* 來源：Excel 工作表 **「ワイヤプル」**
* 儲存格：**Q1**
* INI 設定：

  ```ini
  [StartDateTime]
  sheet_name = ワイヤプル
  cell = Q1
  datetime_format =
  fallback_mode = file_mtime
  output_format = %Y-%m-%d %H:%M:%S
  ```
* 行為：

  * 若 Q1 可解析為日期 → 使用該值
  * 否則 fallback 為檔案修改時間（file_mtime）

---

## 5. Waive_Leng_Cate 規則（動態 INI）

### 5.1 Lot Rule 定義

* Lot Rule = Lot ID 前兩碼

  * 例：`N3059` → `N3`

### 5.2 對照表（INI）

```ini
[WaiveLengthCategoryMapping]
NJ = HL13E1-L0
N1 = HL13E1-L0

NK = HL13E1-L1
N2 = HL13E1-L1

NL = HL13E1-L2
N3 = HL13E1-L2

NM = HL13E1-L3
N4 = HL13E1-L3

NN = HL13E1-G0
N5 = HL13E1-G0

NP = HL13E1-G1
N6 = HL13E1-G1

NQ = HL13E1-G2
N7 = HL13E1-G2

NR = HL13E1-G3
N8 = HL13E1-G3
```

### 5.3 對不到 mapping 的行為

```ini
[WaiveLengthCategory]
missing_rule_behavior = unknown
unknown_value = UNKNOWN
```

* 對不到 → `Waive_Leng_Cate = UNKNOWN`
* 流程不中斷，記錄 Warning log

---

## 6. Excel 讀取與量測欄位統計

### 6.1 主資料表

* 工作表：`HL13E1ﾃﾞｰﾀ`
* 範圍：`D22:KT71`
* INI：

  ```ini
  [Excel]
  sheet_name = HL13E1ﾃﾞｰﾀ
  data_columns = D:KT
  main_skip_rows = 21
  main_nrows = 50
  ```

### 6.2 DataFields 定義原則

* `[DataFields]` 只定義「原始量測欄位」與 Excel col index
* `col = -1` 代表 **Python 後處理 assign**（非 Excel 讀取）

#### 必要欄位（-1）

```ini
key_Serial_Number:-1:str
key_Start_Date_Time:-1:datetime
key_Part_Number:-1:str
```

### 6.3 統計展開規則（Python）

* 每個量測欄位展開 **4 欄**：

  * `{NAME}_MAX`
  * `{NAME}_MIN`
  * `{NAME}_AVG`
  * `{NAME}_STD`

* 例：`key_Ith2` →

  * `Ith2_MAX, Ith2_MIN, Ith2_AVG, Ith2_STD`

* 技術細節：

  * `pd.to_numeric(errors="coerce")`
  * 向量化計算（不逐列迴圈）

---

## 7. Database 規格（對齊既有範例程式）

### 7.1 原則

* **不使用** INI 中的 `query_template`
* **完全沿用既有模組**：`../MyModule/SQL.py`

### 7.2 呼叫方式（Python）

```python
conn, cursor = SQL.connSQL()
result = SQL.selectSQL(cursor, lot_id)
SQL.disconnSQL(conn, cursor)
```

* `lot_id` = `N????`
* 回傳欄位依既有模組定義（例：Part_Number, LotNumber_9）

### 7.3 DB 環境資訊輸出至 CSV

* 由 INI `[Database]` 指派（Python assign，col = -1）：

  * `DB_SERVER`
  * `DB_DATABASE`
  * `DB_USERNAME`
  * `DB_DRIVER`
  * `DB_PASSWORD`（**遮罩輸出 `***`**）

---

## 8. Dedup（避免重複上傳）

### 8.1 機制

* SQLite registry（fingerprint）
* fingerprint = `path + size + mtime`（stat 模式）

### 8.2 INI 設定

```ini
[Dedup]
enable_dedup = true
skip_dedup_check = false
dedup_db_path = ../DataFile/054_E1_Qrun/dedup_registry.sqlite
fingerprint_mode = stat
```

---

## 9. 輸出

> **本章節為「格式凍結（Format Freeze）」定義**。
> 目前 E1_Qrun.py 產出的 CSV 與 Pointer XML **即為正確且經確認的正式格式**，後續開發必須以此為唯一依據。任何欄位、順序、命名或 XML 結構的變更，皆需同步更新本 SPEC。

### 9.1 CSV（Format Freeze）

#### 9.1.1 基本原則

* 一個 Excel 檔（一片 Wafer）= **CSV 一列（Summary Row）**
* CSV 為 **Table 型資料**，供 SPC / TDS 系統引用
* 欄位順序、欄位名稱、大小寫 **不得任意更動**

#### 9.1.2 欄位結構

CSV 由以下幾大區塊組成（由左至右）：

1. **必要識別欄位（Python assign）**

   * `Serial_Number`（Lot ID，N????）
   * `Start_Date_Time`（ワイヤプル!Q1）
   * `Part_Number`（固定 `HL13E1`）

2. **設備 / 分類欄位**

   * `TESTER_ID`（HL13E1ﾃﾞｰﾀ!AY23）
   * `Waive_Leng_Cate`（由 Lot Rule mapping）

3. **DB 環境資訊（追蹤用）**

   * `DB_SERVER`
   * `DB_DATABASE`
   * `DB_USERNAME`
   * `DB_DRIVER`
   * `DB_PASSWORD`（固定遮罩值 `***`）

4. **DB 查詢結果欄位（Legacy SQL Module）**

   * `DB_LOOKUP_Part_Number`
   * `DB_LOOKUP_LotNumber_9`
   * 其他回傳欄位（若有）：`DB_LOOKUP_Field_N`

5. **量測統計欄位（依 DataFields 定義展開）**

   * 命名規則：`{MEASUREMENT_NAME}_{STAT}`
   * STAT 固定為：`MAX / MIN / AVG / STD`
   * 例：`Ith2_MAX`, `Ith2_MIN`, `Ith2_AVG`, `Ith2_STD`

#### 9.1.3 數值與格式

* 數值欄位：

  * 使用 pandas 計算結果原值輸出
  * 不強制四捨五入（由下游系統處理）
* 日期時間欄位：

  * 格式固定：`YYYY-MM-DD HH:MM:SS`

---

* 一片 Wafer = 一列（summary row）
* 自動檔名：

  ```
  {Operation}_{timestamp}_{uuid8}.csv
  ```

### 9.2 Pointer XML（Format Freeze）

#### 9.2.1 角色與目的

* Pointer XML **不承載量測數據**
* 僅作為系統間橋接檔案，指向實際 CSV Table

#### 9.2.2 XML 檔名規則

```text
Site={Site},ProductFamily={ProductFamily},Operation={Operation},Partnumber=HL13E1,Serialnumber={CSV_STEM},Testdate={ISO_DATETIME}.xml
```

* `CSV_STEM`：CSV 檔名（不含副檔名）
* `ISO_DATETIME`：產生 XML 當下時間

#### 9.2.3 XML 結構（凍結）

```xml
<Results>
  <Result startDateTime="..." endDateTime="..." Result="Passed">
    <Header
      SerialNumber="..."
      PartNumber="HL13E1"
      Operation="E1_Qrun"
      TestStation="Qrun"
      Operator="NA"
      StartTime="..."
      Site="350" />

    <TestStep Name="E1_Qrun" startDateTime="..." endDateTime="..." Status="Passed">
      <Data
        DataType="Table"
        Name="tbl_E1_QRUN"
        Value="<CSV full path>"
        CompOperation="LOG" />
    </TestStep>
  </Result>
</Results>
```

#### 9.2.4 凍結規則

* XML Tag 名稱、Attribute 名稱、大小寫 **全部固定**
* `DataType = Table`
* `CompOperation = LOG`
* XML 僅允許：

  * Site / ProductFamily / Operation 由 INI 變動
  * CSV 路徑與時間戳動態變動

---

* 指向 CSV
* Header 欄位：Site / ProductFamily / Operation / SerialNumber / PartNumber

---

## 10. Logging 與錯誤處理

* `logging + RotatingFileHandler`
* 每個檔案 try/except：

  * Excel 讀取失敗 → 跳過該檔
  * DB 查詢失敗 → 記錄 Error，但 CSV 仍可輸出
* 不因單一檔案錯誤中斷整批

---

## 11. 狀態

* 本文件已反映 **截至目前所有對話確認的需求與決策**。
* 後續若有新增欄位 / 規則變更，需同步更新本 SPEC。

---

（End of SPEC）
