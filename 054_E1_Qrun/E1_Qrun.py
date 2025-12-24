"""
E1_Qrun.py (v1 - DB aligned with legacy SQL module)

功能：
- 依 INI 掃描資料夾符合命名規則的 Excel 檔（.xlsx/.xlsm）
- 讀取：
  - 主表：HL13E1ﾃﾞｰﾀ D22:KT71（由 INI 定義）
  - TESTER_ID：HL13E1ﾃﾞｰﾀ!AY23
  - Start_Date_Time：ワイヤプル!Q1（由 INI 定義）
- 由檔名擷取：
  - Lot ID（第二段 N????）-> Serial_Number
  - LotRule（LotID 前兩碼，如 N3）-> Waive_Leng_Cate (INI mapping)
- Part_Number：固定輸出 HL13E1（你指定：對不到也塞 HL13E1）
- 量測欄位：依 INI DataFields(col index) 對每欄計算 MAX/MIN/AVG/STD，欄名格式：{name}_{STAT}
- DB 查詢：對齊範例程式碼，使用 ../MyModule/SQL.py
  - SQL.connSQL() -> (conn, cursor)
  - SQL.selectSQL(cursor, lot_id) -> (Part_Number?, LotNumber_9? ...) 依你們模組回傳
- Dedup：SQLite registry 防止每日重複上傳（可由 INI 開關略過）
- Logging：RotatingFileHandler

注意：
- 本程式需要 ../MyModule/SQL.py 存在並可匯入（與你範例一致）。
- ConfigParser interpolation 已關閉，避免 %Y... 格式字串報錯。
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from configparser import ConfigParser

try:
    import openpyxl  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError("openpyxl is required to read .xlsx/.xlsm") from exc


# -------------------------
# Legacy module path & import (align with sample code)
# -------------------------
# Align with your legacy structure: sys.path.append('../MyModule')
# This script assumes it's located under a project folder where ../MyModule exists.
sys.path.append(str((Path(__file__).resolve().parent / "../MyModule").resolve()))

try:
    import SQL  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Failed to import legacy SQL module. Ensure ../MyModule/SQL.py exists and is importable."
    ) from exc


# -------------------------
# Constants / Regex
# -------------------------
# Accept filenames like:
#   N3250317_N3025先行結果.xlsm
#   N1250922_N1004先行結果.xlsx
# Exclude ones containing "- Copy" by logic (not regex)
FILENAME_RE = re.compile(r"^(N\d{7})_(N\d{4}).*?\.(xlsx|xlsm)$", re.IGNORECASE)

STAT_SUFFIXES = ("MAX", "MIN", "AVG", "STD")


# =========================
# Dataclasses: Settings
# =========================

@dataclass(frozen=True)
class BasicInfo:
    output_mode: str
    site: str
    product_family: str
    operation: str
    test_station: str
    file_name_patterns: List[str]
    retention_date_days: int
    tool_name_fallback: str
    debug_discovery: bool
    debug_limit_10_files: bool


@dataclass(frozen=True)
class PathsSettings:
    input_paths: List[Path]
    running_rec: Path
    output_path: Path
    csv_path: Path
    intermediate_data_path: Path
    log_path: Path


@dataclass(frozen=True)
class ExcelSettings:
    sheet_name: str
    data_columns: str
    main_skip_rows: int
    main_nrows: int


@dataclass(frozen=True)
class StartDateTimeSettings:
    sheet_name: str
    cell: str
    datetime_format: str
    fallback_mode: str  # file_mtime / now / blank
    output_format: str


@dataclass(frozen=True)
class DedupSettings:
    enable_dedup: bool
    skip_dedup_check: bool
    db_path: Path
    fingerprint_mode: str  # stat / sha256


@dataclass(frozen=True)
class WaiveLengthSettings:
    mapping: Dict[str, str]  # LotRule -> Waive_Leng_Cate
    missing_rule_behavior: str  # unknown / skip_file / error
    unknown_value: str


@dataclass(frozen=True)
class DatabaseSettings:
    # Keep all INI fields (even if legacy SQL module doesn't use them directly)
    db_connection_string: str
    server: str
    database: str
    username: str
    password: str
    driver: str

    # Output masking policy for CSV (default masked)
    password_mask: str = "***"


@dataclass(frozen=True)
class DataField:
    key: str
    col: str
    dtype: str

    def is_assigned_by_python(self) -> bool:
        return self.col.strip() == "-1"

    def is_excel_col_index(self) -> bool:
        c = self.col.strip()
        return c.isdigit() or (c.startswith("-") and c[1:].isdigit())

    def excel_index(self) -> int:
        return int(self.col.strip())

    def is_cell_ref(self) -> bool:
        return self.col.strip().lower().startswith("cell_")


@dataclass(frozen=True)
class Settings:
    basic: BasicInfo
    paths: PathsSettings
    excel: ExcelSettings
    start_dt: StartDateTimeSettings
    dedup: DedupSettings
    waive: WaiveLengthSettings
    db: DatabaseSettings
    fields: List[DataField]


# =========================
# Logging
# =========================

def setup_logging(log_dir: Path, operation: str) -> logging.Logger:
    """
    Set up logging with rotation.

    建立 RotatingFileHandler，依檔案大小自動分檔。
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{operation}_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger(operation)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        filename=str(log_file),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=20,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


# =========================
# Config parsing
# =========================

def _read_ini(path: Path) -> ConfigParser:
    """
    Read INI with interpolation disabled.

    關閉 interpolation，避免 %Y-%m-%d... 造成 InterpolationSyntaxError。
    """
    cfg = ConfigParser(interpolation=None)
    cfg.read(path, encoding="utf-8")
    return cfg


def _get_list(cfg: ConfigParser, section: str, key: str) -> List[str]:
    raw = cfg.get(section, key, fallback="").strip()
    if not raw:
        return []
    parts = [line.strip() for line in raw.splitlines() if line.strip()]
    if len(parts) == 1 and "," in parts[0]:
        return [p.strip() for p in parts[0].split(",") if p.strip()]
    return parts


def _parse_fields(cfg: ConfigParser) -> List[DataField]:
    raw = cfg.get("DataFields", "fields", fallback="")
    fields: List[DataField] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if ":" not in line:
            continue
        parts = [p.strip() for p in line.split(":", 2)]
        if len(parts) != 3:
            continue
        fields.append(DataField(key=parts[0], col=parts[1], dtype=parts[2]))
    return fields


def _parse_waive_mapping(cfg: ConfigParser) -> Dict[str, str]:
    if not cfg.has_section("WaiveLengthCategoryMapping"):
        return {}
    items = dict(cfg.items("WaiveLengthCategoryMapping"))
    return {k.strip().upper(): v.strip() for k, v in items.items()}


def load_settings(ini_path: Path) -> Settings:
    """
    Load settings from INI.

    從 INI 讀取所有設定並轉為 dataclass。
    """
    cfg = _read_ini(ini_path)

    basic = BasicInfo(
        output_mode=cfg.get("Basic_info", "output_mode", fallback="csv").strip(),
        site=cfg.get("Basic_info", "Site", fallback="").strip(),
        product_family=cfg.get("Basic_info", "ProductFamily", fallback="").strip(),
        operation=cfg.get("Basic_info", "Operation", fallback="").strip(),
        test_station=cfg.get("Basic_info", "TestStation", fallback="").strip(),
        file_name_patterns=_get_list(cfg, "Basic_info", "file_name_patterns"),
        retention_date_days=cfg.getint("Basic_info", "Retention_date", fallback=7),
        tool_name_fallback=cfg.get("Basic_info", "Tool_Name", fallback="").strip(),
        debug_discovery=cfg.getboolean("Basic_info", "debug_discovery", fallback=False),
        debug_limit_10_files=cfg.getboolean("Basic_info", "debug_limit_10_files", fallback=False),
    )

    input_paths = _get_list(cfg, "Paths", "input_paths")
    paths = PathsSettings(
        input_paths=[Path(p) for p in input_paths],
        running_rec=Path(cfg.get("Paths", "running_rec", fallback="./running.txt")),
        output_path=Path(cfg.get("Paths", "output_path", fallback="./output_xml")),
        csv_path=Path(cfg.get("Paths", "CSV_path", fallback="./output_csv")),
        intermediate_data_path=Path(cfg.get("Paths", "intermediate_data_path", fallback="./intermediate")),
        log_path=Path(cfg.get("Paths", "log_path", fallback="./log")),
    )

    excel = ExcelSettings(
        sheet_name=cfg.get("Excel", "sheet_name", fallback="").strip(),
        data_columns=cfg.get("Excel", "data_columns", fallback="").strip(),
        main_skip_rows=cfg.getint("Excel", "main_skip_rows", fallback=0),
        main_nrows=cfg.getint("Excel", "main_nrows", fallback=0),
    )

    start_dt = StartDateTimeSettings(
        sheet_name=cfg.get("StartDateTime", "sheet_name", fallback="").strip(),
        cell=cfg.get("StartDateTime", "cell", fallback="").strip(),
        datetime_format=cfg.get("StartDateTime", "datetime_format", fallback="").strip(),
        fallback_mode=cfg.get("StartDateTime", "fallback_mode", fallback="file_mtime").strip().lower(),
        output_format=cfg.get("StartDateTime", "output_format", fallback="%Y-%m-%d %H:%M:%S").strip(),
    )

    dedup = DedupSettings(
        enable_dedup=cfg.getboolean("Dedup", "enable_dedup", fallback=True),
        skip_dedup_check=cfg.getboolean("Dedup", "skip_dedup_check", fallback=False),
        db_path=Path(cfg.get("Dedup", "dedup_db_path", fallback="./dedup_registry.sqlite")),
        fingerprint_mode=cfg.get("Dedup", "fingerprint_mode", fallback="stat").strip().lower(),
    )

    waive = WaiveLengthSettings(
        mapping=_parse_waive_mapping(cfg),
        missing_rule_behavior=cfg.get("WaiveLengthCategory", "missing_rule_behavior", fallback="unknown").strip().lower(),
        unknown_value=cfg.get("WaiveLengthCategory", "unknown_value", fallback="UNKNOWN").strip(),
    )

    db = DatabaseSettings(
        db_connection_string=cfg.get("Database", "db_connection_string", fallback="").strip(),
        server=cfg.get("Database", "server", fallback="").strip(),
        database=cfg.get("Database", "database", fallback="").strip(),
        username=cfg.get("Database", "username", fallback="").strip(),
        password=cfg.get("Database", "password", fallback="").strip(),
        driver=cfg.get("Database", "driver", fallback="").strip(),
    )

    fields = _parse_fields(cfg)

    return Settings(
        basic=basic,
        paths=paths,
        excel=excel,
        start_dt=start_dt,
        dedup=dedup,
        waive=waive,
        db=db,
        fields=fields,
    )


# =========================
# Dedup registry (SQLite)
# =========================

class DedupRegistry:
    """
    Dedup registry using SQLite.

    使用 SQLite 紀錄已處理檔案 fingerprint，避免每日重複上傳。
    """

    def __init__(self, db_path: Path, logger: logging.Logger) -> None:
        self.db_path = db_path
        self.logger = logger
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_files (
                    fingerprint TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_mtime REAL NOT NULL,
                    processed_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    output_csv TEXT,
                    output_xml TEXT,
                    message TEXT
                )
                """
            )
            conn.commit()

    def has(self, fingerprint: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT 1 FROM processed_files WHERE fingerprint = ? LIMIT 1",
                (fingerprint,),
            )
            return cur.fetchone() is not None

    def add(
        self,
        fingerprint: str,
        file_path: Path,
        file_size: int,
        file_mtime: float,
        status: str,
        output_csv: Optional[Path],
        output_xml: Optional[Path],
        message: str = "",
    ) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO processed_files
                (fingerprint, file_path, file_size, file_mtime, processed_at, status, output_csv, output_xml, message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fingerprint,
                    str(file_path),
                    int(file_size),
                    float(file_mtime),
                    datetime.now().isoformat(timespec="seconds"),
                    status,
                    str(output_csv) if output_csv else None,
                    str(output_xml) if output_xml else None,
                    message,
                ),
            )
            conn.commit()


def compute_fingerprint(path: Path, mode: str) -> str:
    """
    Compute fingerprint for a file.

    計算檔案指紋：
    - stat：用 path+size+mtime（快速）
    - sha256：對內容做 hash（最準但慢）
    """
    st = path.stat()
    if mode == "sha256":
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    raw = f"{path.resolve()}|{st.st_size}|{st.st_mtime}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()


# =========================
# Excel helpers
# =========================

def _read_cell(workbook_path: Path, sheet_name: str, cell: str) -> Any:
    """
    Read a single cell from an Excel workbook.

    讀取指定工作表與儲存格（支援 .xlsx/.xlsm）。
    """
    wb = openpyxl.load_workbook(workbook_path, data_only=True, read_only=True)
    if sheet_name not in wb.sheetnames:
        wb.close()
        raise KeyError(f"Sheet not found: {sheet_name}")
    ws = wb[sheet_name]
    value = ws[cell].value
    wb.close()
    return value


def _coerce_datetime(value: Any, fmt_hint: str) -> Optional[datetime]:
    """
    Coerce a cell value into datetime.

    將儲存格值轉為 datetime：
    - 若已是 datetime -> 直接回傳
    - 若是字串 -> 依 fmt_hint 或 pandas 自動解析
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            base = datetime(1899, 12, 30)
            return base + pd.to_timedelta(float(value), unit="D")  # type: ignore[arg-type]
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if fmt_hint:
                return datetime.strptime(text, fmt_hint)
            parsed = pd.to_datetime(text, errors="coerce")
            if pd.isna(parsed):
                return None
            return parsed.to_pydatetime()
        except Exception:
            return None
    return None


def read_start_datetime(
    file_path: Path,
    settings: StartDateTimeSettings,
    logger: logging.Logger,
) -> str:
    """
    Read Start_Date_Time from configured sheet/cell with fallback.

    從 INI 指定的 sheet/cell 取得完成日期，若失敗則 fallback。
    """
    dt_obj: Optional[datetime] = None
    try:
        if settings.sheet_name and settings.cell:
            value = _read_cell(file_path, settings.sheet_name, settings.cell)
            dt_obj = _coerce_datetime(value, settings.datetime_format)
    except Exception as exc:
        logger.warning(
            "Failed to read StartDateTime from %s!%s: %s",
            settings.sheet_name,
            settings.cell,
            exc,
        )

    if dt_obj is None:
        if settings.fallback_mode == "now":
            dt_obj = datetime.now()
        elif settings.fallback_mode == "blank":
            return ""
        else:
            dt_obj = datetime.fromtimestamp(file_path.stat().st_mtime)

    return dt_obj.strftime(settings.output_format)


def read_tester_id_ay23(
    file_path: Path,
    main_sheet_name: str,
    logger: logging.Logger,
) -> str:
    """
    Read TESTER_ID from AY23 in the main sheet.

    從主資料表 HL13E1ﾃﾞｰﾀ 的 AY23 取得 TESTER_ID（文字）。
    """
    try:
        value = _read_cell(file_path, main_sheet_name, "AY23")
        if value is None:
            return ""
        return str(value).strip()
    except Exception as exc:
        logger.warning("Failed to read TESTER_ID from %s!AY23: %s", main_sheet_name, exc)
        return ""


def read_main_table(
    file_path: Path,
    excel: ExcelSettings,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Read main table range into a DataFrame.

    讀取指定 sheet + columns + skiprows + nrows（對應 D22:KT71）。
    """
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=excel.sheet_name,
            usecols=excel.data_columns,
            skiprows=excel.main_skip_rows,
            nrows=excel.main_nrows,
            header=None,
            engine="openpyxl",
        )
        logger.info("Loaded main table: %s rows x %s cols", df.shape[0], df.shape[1])
        return df
    except Exception as exc:
        raise RuntimeError(f"Failed to read main table from {file_path.name}: {exc}") from exc


# =========================
# Waive length category
# =========================

def compute_waive_leng_cate(
    lot_id: str,
    waive: WaiveLengthSettings,
    logger: logging.Logger,
) -> str:
    """
    Determine Waive_Leng_Cate from Lot ID prefix (first 2 chars).

    依 LotID 前兩碼（例如 N3059 -> N3）查 INI 對照表。
    """
    lot_rule = lot_id[:2].upper()
    value = waive.mapping.get(lot_rule)

    if value:
        return value

    behavior = waive.missing_rule_behavior
    if behavior == "skip_file":
        raise ValueError(f"LotRule '{lot_rule}' not found (skip_file)")
    if behavior == "error":
        raise KeyError(f"LotRule '{lot_rule}' not found (error)")

    logger.warning("LotRule '%s' not found in mapping; use %s", lot_rule, waive.unknown_value)
    return waive.unknown_value


# =========================
# DB query (align with legacy SQL module)
# =========================

def query_database_via_legacy_sql(
    lot_id: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Query DB using legacy SQL module (align with sample code).

    依範例程式碼：
      conn, cursor = SQL.connSQL()
      SQL.selectSQL(cursor, lot_id)

    回傳 dict 欄位：
    - 本版先假設 selectSQL 回傳 (Part_Number, LotNumber_9) 類似兩個欄位（如範例）。
    - 若你們回傳欄位不同，只要在此函式調整 key 對應即可。
    """
    conn = None
    cursor = None
    try:
        conn, cursor = SQL.connSQL()
        if conn is None or cursor is None:
            logger.error("Legacy SQL.connSQL() returned None (connection failed).")
            return {}

        result = SQL.selectSQL(cursor, str(lot_id))

        # Normalize result to tuple/list
        if result is None:
            logger.warning("Legacy SQL.selectSQL() returned None for LotID=%s", lot_id)
            return {}

        if isinstance(result, (list, tuple)):
            values = list(result)
        else:
            # Sometimes it might return a single scalar
            values = [result]

        # === Default mapping (adjust if your legacy SQL returns different columns) ===
        out: Dict[str, Any] = {}
        if len(values) >= 1:
            out["DB_LOOKUP_Part_Number"] = values[0]
        if len(values) >= 2:
            out["DB_LOOKUP_LotNumber_9"] = values[1]

        # If there are more fields, append generic names
        for i in range(2, len(values)):
            out[f"DB_LOOKUP_Field_{i+1}"] = values[i]

        return out

    except Exception as exc:
        logger.exception("DB query failed via legacy SQL for LotID=%s: %s", lot_id, exc)
        return {}
    finally:
        try:
            if conn is not None:
                SQL.disconnSQL(conn, cursor)
        except Exception:
            # Don't crash on disconnect
            pass


# =========================
# Stats computation
# =========================

def _stat_name_from_key(key: str) -> str:
    """
    Convert DataFields key_* to output base name.

    例如：key_V_FWHM -> V_FWHM
    """
    if key.startswith("key_"):
        return key[4:]
    return key


def compute_stats_for_fields(
    df: pd.DataFrame,
    fields: Sequence[DataField],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Compute MAX/MIN/AVG/STD for each measurement field.

    對每個數值欄位（col index）計算 MAX/MIN/AVG/STD，欄名格式 A：{name}_{STAT}。
    """
    out: Dict[str, Any] = {}

    meas = [f for f in fields if f.is_excel_col_index() and f.excel_index() >= 0]
    for f in meas:
        base = _stat_name_from_key(f.key)
        idx = f.excel_index()
        if idx >= df.shape[1]:
            logger.warning("Field %s col=%s out of range (df cols=%s)", f.key, idx, df.shape[1])
            for stat in STAT_SUFFIXES:
                out[f"{base}_{stat}"] = None
            continue

        series = pd.to_numeric(df.iloc[:, idx], errors="coerce")
        out[f"{base}_MAX"] = series.max(skipna=True)
        out[f"{base}_MIN"] = series.min(skipna=True)
        out[f"{base}_AVG"] = series.mean(skipna=True)
        out[f"{base}_STD"] = series.std(skipna=True, ddof=1)

    return out


# =========================
# CSV / XML output
# =========================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def make_output_csv_path(csv_dir: Path, operation: str) -> Path:
    """
    Create unique CSV filename with timestamp + uuid.

    產出自動檔名（含 timestamp + uuid）避免衝突。
    """
    ensure_dir(csv_dir)
    ts = datetime.now().strftime("%Y_%m_%dT%H.%M.%S")
    short = uuid.uuid4().hex[:8]
    return csv_dir / f"{operation}_{ts}_{short}.csv"


def append_csv(csv_path: Path, row: Mapping[str, Any]) -> None:
    """
    Append a single row to CSV (create header if file doesn't exist).

    追加一列到 CSV（若檔案不存在則寫入 header）。
    """
    df = pd.DataFrame([dict(row)])
    exists = csv_path.exists()
    df.to_csv(csv_path, mode="a", header=not exists, index=False, encoding="utf-8-sig")


def generate_pointer_xml(
    output_dir: Path,
    settings: Settings,
    csv_path: Path,
    serial_no: str,
) -> Path:
    """
    Generate pointer XML pointing to CSV.

    產生 Pointer XML（指向 CSV），維持與原本類似的格式。
    """
    import xml.etree.ElementTree as ET
    from xml.dom import minidom

    ensure_dir(output_dir)
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    xml_file_name = (
        f"Site={settings.basic.site},"
        f"ProductFamily={settings.basic.product_family},"
        f"Operation={settings.basic.operation},"
        f"Partnumber=HL13E1,"
        f"Serialnumber={serial_no},"
        f"Testdate={now_iso}.xml"
    ).replace(":", ".")

    xml_path = output_dir / xml_file_name

    results = ET.Element(
        "Results",
        {
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        },
    )
    result = ET.SubElement(
        results,
        "Result",
        startDateTime=now_iso,
        endDateTime=now_iso,
        Result="Passed",
    )
    ET.SubElement(
        result,
        "Header",
        SerialNumber=serial_no,
        PartNumber="HL13E1",
        Operation=settings.basic.operation,
        TestStation=settings.basic.test_station,
        Operator="NA",
        StartTime=now_iso,
        Site=settings.basic.site,
        LotNumber="",
    )
    test_step = ET.SubElement(
        result,
        "TestStep",
        Name=settings.basic.operation,
        startDateTime=now_iso,
        endDateTime=now_iso,
        Status="Passed",
    )
    ET.SubElement(
        test_step,
        "Data",
        DataType="Table",
        Name=f"tbl_{settings.basic.operation.upper()}",
        Value=str(csv_path),
        CompOperation="LOG",
    )

    xml_bytes = minidom.parseString(ET.tostring(results)).toprettyxml(
        indent="  ", encoding="utf-8"
    )
    with xml_path.open("wb") as f:
        f.write(xml_bytes)

    return xml_path


# =========================
# File discovery
# =========================

def discover_source_files(
    input_paths: Sequence[Path],
    patterns: Sequence[str],
    logger: logging.Logger,
    debug: bool = False,
) -> List[Path]:
    """
    Discover all files matching patterns in the input paths.

    Debug mode:
    - Print all files found in directories
    - Print which files match / not match patterns
    """
    found: List[Path] = []

    if debug:
        print()
        print("[DEBUG] ===== Discover Source Files =====")

    for base in input_paths:
        if debug:
            print(f"[DEBUG] Scan path: {base}")

        if not base.exists():
            if debug:
                print(f"[DEBUG] ❌ Path not exists: {base}")
            logger.warning("Input path not found: %s", base)
            continue

        if debug:
            all_files = list(base.iterdir())
            print(f"[DEBUG]   Total files in dir: {len(all_files)}")
            for p in all_files:
                print(f"[DEBUG]     - {p.name}")

        for pat in patterns:
            if debug:
                print(f"[DEBUG]   Try pattern: {pat}")
            matched = list(base.glob(pat))
            if debug:
                print(f"[DEBUG]     Matched count: {len(matched)}")

            for p in matched:
                if p.name.startswith("~$"):
                    if debug:
                        print(f"[DEBUG]       Skip temp file: {p.name}")
                    continue

                if debug:
                    print(f"[DEBUG]       ✔ Matched: {p.name}")
                found.append(p)

    uniq = sorted({p.resolve() for p in found})

    if debug:
        print(f"[DEBUG] ===== Result: {len(uniq)} candidate files =====")
        print()
    logger.info("Discovered %d candidate files.", len(uniq))

    return uniq


def parse_filename_ids(file_path: Path) -> Tuple[str, str]:
    """
    Parse wafer_id and lot_id from filename.

    - 排除含 '- Copy' 的檔案
    - 允許 '先行結果' 等後綴文字
    """
    name = file_path.name

    if "- Copy" in name or "copy" in name.lower():
        raise ValueError(f"Excluded copied file: {name}")

    m = FILENAME_RE.match(name)
    if not m:
        raise ValueError(f"Invalid filename format: {name}")

    wafer_id = m.group(1).upper()
    lot_id = m.group(2).upper()
    return wafer_id, lot_id


# =========================
# Per-file processing
# =========================

def build_output_row(
    file_path: Path,
    settings: Settings,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Build the final output row for one wafer file.

    每個檔案輸出一列（summary row），包含：
    - Serial_Number (Lot ID)
    - Start_Date_Time (ワイヤプル!Q1)
    - Part_Number (固定 HL13E1)
    - TESTER_ID (AY23)
    - Waive_Leng_Cate (INI mapping)
    - DB connection info (from INI, assigned by Python)
    - DB lookup fields (from legacy SQL module)
    - Stats fields (MAX/MIN/AVG/STD)
    """
    _wafer_id, lot_id = parse_filename_ids(file_path)

    serial_number = lot_id
    start_dt_str = read_start_datetime(file_path, settings.start_dt, logger)

    waive_leng = compute_waive_leng_cate(lot_id, settings.waive, logger)

    # Your rule: always HL13E1
    part_number = "HL13E1"

    tester_id = read_tester_id_ay23(file_path, settings.excel.sheet_name, logger)
    if not tester_id:
        tester_id = settings.basic.tool_name_fallback

    # DB query using legacy module
    db_lookup_fields = query_database_via_legacy_sql(lot_id, logger)

    # Read main data and compute stats
    df = read_main_table(file_path, settings.excel, logger)
    stats = compute_stats_for_fields(df, settings.fields, logger)

    # DB connection info columns (write into CSV as requested)
    # Password masked by default to prevent leakage
    row: Dict[str, Any] = {
        "Serial_Number": serial_number,
        "Start_Date_Time": start_dt_str,
        "Part_Number": part_number,
        "TESTER_ID": tester_id,
        "Waive_Leng_Cate": waive_leng,
        "DB_SERVER": settings.db.server,
        "DB_DATABASE": settings.db.database,
        "DB_USERNAME": settings.db.username,
        "DB_DRIVER": settings.db.driver,
        "DB_PASSWORD": settings.db.password_mask,
        "Source_File": file_path.name,
    }

    # Merge DB lookup results
    row.update(db_lookup_fields)

    # Merge stats
    row.update(stats)
    return row


# =========================
# Program entry
# =========================

def run_for_ini(ini_path: Path) -> None:
    settings = load_settings(ini_path)
    logger = setup_logging(settings.paths.log_path, settings.basic.operation)

    logger.info("==== Start E1_Qrun uploader ====")
    logger.info("INI: %s", ini_path.resolve())

    ensure_dir(settings.paths.csv_path)
    ensure_dir(settings.paths.output_path)
    ensure_dir(settings.paths.intermediate_data_path)

    csv_out = make_output_csv_path(settings.paths.csv_path, settings.basic.operation)
    logger.info("CSV output: %s", csv_out)

    registry: Optional[DedupRegistry] = None
    if settings.dedup.enable_dedup and not settings.dedup.skip_dedup_check:
        registry = DedupRegistry(settings.dedup.db_path, logger)
        logger.info("Dedup enabled: %s", settings.dedup.db_path)
    else:
        logger.info(
            "Dedup skipped (enable_dedup=%s, skip_dedup_check=%s)",
            settings.dedup.enable_dedup,
            settings.dedup.skip_dedup_check,
        )

    files = discover_source_files(
        settings.paths.input_paths,
        settings.basic.file_name_patterns,
        logger,
        debug=settings.basic.debug_discovery,
    )

    if settings.basic.debug_limit_10_files:
        files = files[:10]
        logger.info("Debug limit enabled: processing only %d files.", len(files))

    processed_count = 0
    skipped_count = 0
    error_count = 0

    for f in files:
        try:
            # strict filename check (also excludes "- Copy")
            parse_filename_ids(f)

            fp = compute_fingerprint(f, settings.dedup.fingerprint_mode)
            st = f.stat()

            if registry and registry.has(fp):
                skipped_count += 1
                logger.info("Skip (dedup): %s", f.name)
                continue

            row = build_output_row(f, settings, logger)
            append_csv(csv_out, row)

            processed_count += 1
            logger.info("Processed: %s", f.name)

            if registry:
                registry.add(
                    fingerprint=fp,
                    file_path=f,
                    file_size=int(st.st_size),
                    file_mtime=float(st.st_mtime),
                    status="SUCCESS",
                    output_csv=csv_out,
                    output_xml=None,
                    message="",
                )

        except Exception as exc:
            error_count += 1
            logger.exception("Failed processing file %s: %s", f.name, exc)
            if not f.exists():
                continue
            try:
                fp = compute_fingerprint(f, settings.dedup.fingerprint_mode)
                st = f.stat()
                if registry:
                    registry.add(
                        fingerprint=fp,
                        file_path=f,
                        file_size=int(st.st_size),
                        file_mtime=float(st.st_mtime),
                        status="FAILED",
                        output_csv=csv_out if csv_out.exists() else None,
                        output_xml=None,
                        message=str(exc),
                    )
            except Exception:
                pass

    xml_out: Optional[Path] = None
    if csv_out.exists() and csv_out.stat().st_size > 0:
        serial_for_xml = csv_out.stem
        xml_out = generate_pointer_xml(settings.paths.output_path, settings, csv_out, serial_for_xml)
        logger.info("Pointer XML generated: %s", xml_out)

    logger.info("==== End ====")
    logger.info(
        "Processed=%d Skipped=%d Errors=%d CSV=%s XML=%s",
        processed_count,
        skipped_count,
        error_count,
        csv_out,
        xml_out,
    )


def main(argv: Sequence[str]) -> int:
    """
    CLI entry.

    用法：
      python E1_Qrun.py <your_config.ini>

    若未提供參數，會在同目錄掃描所有 .ini 並逐一執行。
    """
    os.chdir(Path(__file__).resolve().parent)

    if len(argv) >= 2:
        ini_path = Path(argv[1])
        if not ini_path.exists():
            print(f"INI not found: {ini_path}")
            return 2
        run_for_ini(ini_path)
        return 0

    ini_files = sorted(Path(".").glob("*.ini"))
    if not ini_files:
        print("No .ini files found in current directory.")
        return 1

    for ini in ini_files:
        run_for_ini(ini)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
