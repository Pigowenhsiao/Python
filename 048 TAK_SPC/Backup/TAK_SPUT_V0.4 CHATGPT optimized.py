#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本プログラムの機能
----------------
1. すべての .ini ファイルを走査し、設定に従って Excel データを読み込み XML を生成する。
2. 実行ログおよびエラーログはカスタム Log モジュールで出力する。
依存モジュール：Log, SQL, Check, Convert_Date, Row_Number_Func（../MyModule 配下）
"""

import os
import sys
import glob
import shutil
import logging
from pathlib import Path
from datetime import datetime, timedelta
from configparser import ConfigParser, NoSectionError, NoOptionError

import pandas as pd

# --- カスタムモジュール読み込み -------------------------------------------------
sys.path.append('../MyModule')
import Log        # noqa: E402
import SQL        # noqa: E402
import Check      # noqa: E402
import Convert_Date  # noqa: E402
import Row_Number_Func  # noqa: E402
# -----------------------------------------------------------------------------

# グローバルログファイルパス（動的に設定）
global_log_file: str | None = None

# =============================================================================
# 共通ユーティリティ
# =============================================================================
def setup_logging(log_file: str) -> None:
    """ログ設定を初期化する"""
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def read_running_rec(path: str, fallback_days: int = 30) -> datetime:
    """実行記録ファイルを読み取り、無効なら `fallback_days` 日前を返す"""
    if not Path(path).exists():
        Path(path).write_text("")
        return datetime.today() - timedelta(days=fallback_days)

    try:
        text = Path(path).read_text(encoding="utf-8").strip()
        dt = pd.to_datetime(text, errors="coerce")
        if pd.isnull(dt):
            raise ValueError
        return dt.to_pydatetime()
    except Exception:
        return datetime.today() - timedelta(days=fallback_days)


def update_running_rec(path: str, dt: datetime) -> None:
    """最新の実行日時を記録する"""
    try:
        Path(path).write_text(dt.strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")
        Log.Log_Info(global_log_file, f"Running record updated: {dt}")
    except Exception as e:
        Log.Log_Error(global_log_file, f"Failed to update running record: {e}")


# =============================================================================
# DataFrame 前処理系
# =============================================================================
def get_latest_excel_file(pattern: str) -> str | None:
    """パターンに合致する最新 Excel ファイルを返す（存在しなければ None）"""
    files = [
        f for f in glob.glob(pattern)
        if "$" not in Path(f).name  # 一時ファイル除外
    ]
    if not files:
        return None
    return max(files, key=lambda f: Path(f).stat().st_mtime)


def load_and_filter_dataframe(
    excel_path: str,
    sheet_name: str,
    data_columns: str,
    running_date_days: int,
    fields: dict,
) -> pd.DataFrame:
    """Excel を読み込み、日付＆必須列でフィルタリングして DataFrame を返す"""
    df = pd.read_excel(
        excel_path,
        header=None,
        sheet_name=sheet_name,
        usecols=data_columns,
        skiprows=1000,      # 元コードと同値
    )
    df.columns = range(df.shape[1])
    df = df.dropna(subset=[2])      # col=2 が NaN 行を除去
    df["key_SORTNUMBER"] = df.index + 1000

    # Running_date フィルタ
    start_col = int(fields["key_Start_Date_Time"][0])
    threshold = datetime.today() - timedelta(days=running_date_days)
    df = df[
        df[start_col].apply(pd.to_datetime, errors="coerce") >= threshold
    ]
    df[start_col] = df[start_col].apply(
        lambda x: pd.to_datetime(x).strftime("%Y-%m-%dT%H.%M.%S")
    )
    return df.reset_index(drop=True)


def expand_serial_number_rows(df: pd.DataFrame, serial_col: str) -> pd.DataFrame:
    """Serial 列に '/' 区切りがある場合、行を分割して展開する"""
    rows = []
    for _, row in df.iterrows():
        serials = str(row[serial_col]).split("/")
        for s in serials:
            s = s.strip().split()[0]
            if s:
                new_row = row.copy()
                new_row[serial_col] = s
                rows.append(new_row)
    return pd.DataFrame(rows).reset_index(drop=True)


def append_part_number_info(df: pd.DataFrame, mat_col: str) -> pd.DataFrame:
    """Material Type に応じて Part/Chip/COB 番号を付与する"""
    df["Part_Number"] = None
    df["Chip_Part_Number"] = None
    df["COB_Part_Number"] = None

    for idx, mtype in df[mat_col].items():
        if "QJ-30150" in str(mtype):
            df.loc[idx, ["Part_Number", "Chip_Part_Number", "COB_Part_Number"]] = [
                "XQJ-30150",
                "1000047352",
                "1000047353",
            ]
        elif "QJ-30115" in str(mtype):
            df.loc[idx, ["Part_Number", "Chip_Part_Number", "COB_Part_Number"]] = [
                "XQJ-30115-P",
                "1000034198",
                "1000034812",
            ]
    return df


# =============================================================================
# XML 生成
# =============================================================================
def _excel_date_number(dt_str: str) -> int | None:
    """yyyy-mm-ddTHH.MM.SS 形式文字列を Excel 日付シリアル値に変換"""
    try:
        dt = datetime.strptime(
            dt_str.replace("T", " ").replace(".", ":"), "%Y-%m-%d %H:%M:%S"
        )
        return int((dt - datetime(1899, 12, 30)).days)
    except Exception:
        return None


def generate_xml(
    data: dict,
    output_dir: str,
    site: str,
    product_family: str,
    operation: str,
    test_station: str,
) -> None:
    """行データ dict から XML ファイルを生成する"""

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    fname = (
        f"Site={site},ProductFamily={product_family},Operation={operation},"
        f"PartNumber={data.get('key_Part_Number','Unknown')},"
        f"SerialNumber={data.get('key_Serial_Number','Unknown')},"
        f"Testdate={data.get('key_Start_Date_Time','Unknown')}.xml"
    )
    xml_path = Path(output_dir) / fname

    with xml_path.open("w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="utf-8"?>\n')
        f.write(
            '<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n'
        )
        f.write(
            f'    <Result startDateTime="{data["key_Start_Date_Time"].replace(".",
            ":")}" Result="Passed">\n'
        )
        f.write(
            f'        <Header SerialNumber="{data["key_Serial_Number"]}" '
            f'PartNumber="{data["key_Part_Number"]}" Operation="{operation}" '
            f'TestStation="{test_station}" Operator="{data["key_Operator1"]}" '
            f'StartTime="{data["key_Start_Date_Time"].replace(".",
            ":")}" Site="{site}" LotNumber="{data["key_Serial_Number"]}"/>\n'
        )
        f.write("        <HeaderMisc>\n")
        f.write(f'            <Item Description="{operation}"></Item>\n')
        f.write("        </HeaderMisc>\n")
        f.write(
            f'        <TestStep Name="{operation}" '
            f'startDateTime="{data["key_Start_Date_Time"].replace(".",
            ":")}" Status="Passed">\n'
        )
        f.write(
            f'            <Data DataType="String" Name="{data["key_Coating_Type"]}" '
            f'Value="{data["key_Reflectivity"]}" CompOperation="LOG"/>\n'
        )
        f.write("        </TestStep>\n")
        f.write(
            f'        <TestStep Name="SORTED_DATA" '
            f'startDateTime="{data["key_Start_Date_Time"].replace(".",
            ":")}" Status="Passed">\n'
        )
        f.write(
            f'            <Data DataType="Numeric" Name="STARTTIME_SORTED" '
            f'Units="" Value="{data["key_STARTTIME_SORTED"]}"/>\n'
        )
        f.write(
            f'            <Data DataType="Numeric" Name="SORTNUMBER" Units="" '
            f'Value="{data["key_SORTNUMBER"]}"/>\n'
        )
        f.write(
            f'            <Data DataType="String" Name="Chip_Part_Number" '
            f'Value="{data["Chip_Part_Number"]}" CompOperation="LOG"/>\n'
        )
        f.write(
            f'            <Data DataType="String" Name="COB_Part_Number" '
            f'Value="{data["Chip_Part_Number"]}" CompOperation="LOG"/>\n'
        )
        f.write("        </TestStep>\n")
        f.write("        <TestEquipment>\n")
        f.write(
            f'            <Item DeviceName="CVD" '
            f'DeviceSerialNumber="{data["CVD_Tool"]}"></Item>\n'
        )
        f.write("        </TestEquipment>\n")
        f.write("    </Result>\n")
        f.write("</Results>\n")

    Log.Log_Info(global_log_file, f"XML created: {xml_path}")


# =============================================================================
# メイン処理
# =============================================================================
def process_excel_file(
    excel_path: str,
    cfg: ConfigParser,
    data_columns: str,
    fields: dict,
    output_path: str,
    running_rec_path: str,
    site: str,
    product_family: str,
    operation: str,
    test_station: str,
) -> None:
    """単一 Excel ファイルを読み込み XML 出力までを実行"""

    Log.Log_Info(global_log_file, f"Processing Excel file: {excel_path}")

    running_days = int(cfg.get("Basic_info", "Running_date"))
    df = load_and_filter_dataframe(
        excel_path,
        cfg.get("Excel", "sheet_name"),
        data_columns,
        running_days,
        fields,
    )

    df = expand_serial_number_rows(df, int(fields["key_Serial_Number"][0]))
    df = append_part_number_info(df, int(fields["key_Material_Type"][0]))

    latest_end_date = pd.to_datetime(
        df[int(fields["key_END_Date_Time"][0])], errors="coerce"
    ).max()
    if pd.notnull(latest_end_date):
        update_running_rec(running_rec_path, latest_end_date)

    for _, row in df.iterrows():
        serial = str(row[int(fields["key_Serial_Number"][0])])
        if not (serial.startswith("150") or serial.startswith("115")):
            continue

        data_dict = {}
        for key, (col, dtype) in fields.items():
            val = row[int(col)]
            try:
                if dtype == "float":
                    val = float(val)
                elif dtype == "int":
                    val = int(val)
                elif dtype == "str":
                    val = str(val)
                elif dtype == "bool":
                    val = bool(val)
                elif dtype == "datetime":
                    val = pd.to_datetime(val)
            except Exception:
                val = None
            data_dict[key] = val

        data_dict["key_STARTTIME_SORTED"] = _excel_date_number(
            data_dict["key_Start_Date_Time"]
        )
        # 追加情報
        data_dict["key_Part_Number"] = row["Part_Number"]
        data_dict["Part_Number"] = row["Part_Number"]
        data_dict["Chip_Part_Number"] = row["Chip_Part_Number"]
        data_dict["COB_Part_Number"] = row["COB_Part_Number"]
        data_dict["CVD_Tool"] = cfg.get("Basic_info", "CVD_Tool")

        if None in data_dict.values():
            Log.Log_Error(global_log_file, "Row skipped due to None values")
            continue

        generate_xml(
            data=data_dict,
            output_dir=output_path,
            site=site,
            product_family=product_family,
            operation=operation,
            test_station=test_station,
        )


def process_ini_file(ini_path: str) -> None:
    """INI ファイルを読み込み、設定に従って処理を行う"""

    global global_log_file

    cfg = ConfigParser()
    cfg.read(ini_path, encoding="utf-8")

    # --- パス＆ログ設定 --------------------------------------------------------
    log_dir = Path(cfg.get("Logging", "log_path")) / str(datetime.today().date())
    log_dir.mkdir(parents=True, exist_ok=True)
    global_log_file = str(log_dir / "043_LD-SPUT.log")
    setup_logging(global_log_file)
    Log.Log_Info(global_log_file, f"Program start for {ini_path}")

    # --- 基本設定取得 ----------------------------------------------------------
    input_paths = [
        p.strip()
        for p in cfg.get("Paths", "input_paths").splitlines()
        if p.strip() and not p.strip().startswith("#")
    ]
    output_path = cfg.get("Paths", "output_path")
    running_rec = cfg.get("Paths", "running_rec")
    file_pattern = cfg.get("Basic_info", "file_name_pattern")

    # DataFields
    fields = {}
    for line in cfg.get("DataFields", "fields").splitlines():
        if line.strip():
            key, col, dtype = [x.strip() for x in line.split(":")]
            fields[key] = (col, dtype)

    # --- Excel ファイルを処理 --------------------------------------------------
    for in_path in input_paths:
        latest = get_latest_excel_file(str(Path(in_path) / file_pattern))
        if not latest:
            Log.Log_Error(
                global_log_file, f"No Excel file found at {in_path} pattern {file_pattern}"
            )
            continue

        dest_dir = Path("../DataFile/047/TAK_SPC")
        dest_dir.mkdir(parents=True, exist_ok=True)
        copied = dest_dir / Path(latest).name
        shutil.copy(latest, copied)

        process_excel_file(
            excel_path=str(copied),
            cfg=cfg,
            data_columns=cfg.get("Excel", "data_columns"),
            fields=fields,
            output_path=output_path,
            running_rec_path=running_rec,
            site=cfg.get("Basic_info", "Site"),
            product_family=cfg.get("Basic_info", "ProductFamily"),
            operation=cfg.get("Basic_info", "Operation"),
            test_station=cfg.get("Basic_info", "TestStation"),
        )


def main() -> None:
    """メインエントリーポイント：カレントの ini をすべて処理"""
    for ini in glob.glob("*.ini"):
        process_ini_file(ini)
    Log.Log_Info(global_log_file, "Program End")


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
