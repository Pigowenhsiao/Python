import pandas as pd
from datetime import datetime
from pathlib import Path

# === 請修改這裡 ===
excel_path = Path("20251005.xls")   # 你的檔案名稱
sheet_name = "KeisokuDataTable"     # INI 中設定的表名
usecols = "A:U"                     # INI 中 data_columns
skiprows = 0                        # 若 Excel 第一列是標題，則設 0；若標題在第2列，設 1
target_pt = "A12"                   # 你想檢查的 PtName
interval_hours = 2                  # time_interval (INI)

# === 讀取 Excel ===
print(f"📂 讀取 Excel: {excel_path}")
df = pd.read_excel(excel_path, sheet_name=sheet_name, usecols=usecols, skiprows=skiprows, dtype=str)
print(f"✅ 原始資料共 {len(df)} 筆")

# 嘗試找出時間欄與 PtName 欄
possible_time_cols = [c for c in df.columns if "Time" in c or "ResTime" in c]
possible_pt_cols = [c for c in df.columns if "PtName" in c or "Pt Name" in c]
print(f"⏰ 時間欄: {possible_time_cols}")
print(f"📍 PtName欄: {possible_pt_cols}")

time_col = possible_time_cols[0]
pt_col = possible_pt_cols[0]

# === 顯示 A12 原始資料 ===
df_a12 = df[df[pt_col].astype(str).str.contains(target_pt, case=False, na=False)].copy()
print(f"\n🔎 原始資料中 PtName = {target_pt} 共 {len(df_a12)} 筆")
print(df_a12[[pt_col, time_col]].head(15))

# === 嘗試解析時間 ===
print("\n🕐 嘗試解析時間格式 ...")
dt_parsed = pd.to_datetime(df_a12[time_col], errors="coerce")
print(f"✔️ 可成功解析的筆數: {(~dt_parsed.isna()).sum()} / {len(dt_parsed)}")
print(f"❌ 無法解析 (NaT) 的筆數: {dt_parsed.isna().sum()}")

# 印出前 10 筆解析結果
preview = pd.DataFrame({
    "原始值": df_a12[time_col].head(10).values,
    "轉換後": dt_parsed.head(10).values
})
print("\n📋 時間欄轉換預覽：")
print(preview)

# === 過濾掉 NaT，模擬取樣前後比較 ===
df_a12["Start_Date_Time"] = dt_parsed
df_a12 = df_a12[df_a12["Start_Date_Time"].notna()].copy()

if df_a12.empty:
    print("\n⚠️ 所有時間都無法解析，請檢查 Excel 時間欄格式。")
else:
    df_a12 = df_a12.sort_values("Start_Date_Time")
    print(f"\n🧾 解析成功後剩 {len(df_a12)} 筆 (排序後)")
    print(df_a12[["Start_Date_Time"]].head(15))

    # 模擬 2 小時分桶取樣
    interval_minutes = int(interval_hours * 60)
    df_a12["time_bin"] = df_a12["Start_Date_Time"].dt.floor(f"{interval_minutes}min")
    idx_keep = df_a12.groupby("time_bin")["Start_Date_Time"].idxmin()
    df_sampled = df_a12.loc[idx_keep].sort_values("Start_Date_Time")

    print(f"\n⏳ 模擬 time_interval={interval_hours} 小時取樣後：")
    print(f"保留 {len(df_sampled)} 筆，原本 {len(df_a12)} 筆")
    print(df_sampled[["Start_Date_Time"]])

print("\n✅ 驗證完成！")
