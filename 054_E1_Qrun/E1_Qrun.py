"""
E1_Qrun_Uploader.py

第一版：依 INI 掃描資料夾內所有符合命名規則的 Excel 檔（.xlsx/.xlsm），
讀取指定 Sheet/Range，計算各量測欄位的 MAX/MIN/AVG/STD，並輸出 CSV + Pointer XML。

特點：
- 設定外部化：configparser 讀取 .ini（保留你既有 INI 結構，並支援你新增的區塊）
- 高度模組化：掃檔 / dedup / excel 讀取 / 統計 / DB 查詢 / CSV/XML 輸出 分層
- 日誌：RotatingFileHandler 依大小切檔
- Dedup：SQLite registry（可用 INI 開關 skip）
- 型別註解 + 中英 Docstring
- 向量化統計：pandas.to_numeric(errors="coerce")，避免逐列迴圈

注意（DB）：
- 由於目前 INI 沒提供 SQL 查詢語句或表結構，本版提供「query_template」可選。
  你可以在 [Database] 加上：
      query_template = SELECT PartNumber, LotNumber_9 FROM YourTable WHERE LotID = ?
  程式會用 pyodbc 執行並回填回傳欄位到 CSV。
  若未提供 query_template 或環境沒有 pyodbc，程式會記錄 warning 並略過 DB 欄位。
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from configparser import ConfigParser

try:
    import openpyxl  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError("openpyxl is required to read .xlsx/.xlsm") from exc

# pyodbc is optional in v1 (depends on runtime environment)
try:
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover
    pyodbc = None  # type: ignore


FILENAME_RE = re.compile(r"^(N\d{7})_(N\d{4})\.(xlsx|xlsm)$", re.IGNORECASE)
EXCEL_CELL_RE = re.compile(r"^([A-Z]+)(\d+)$", re.IGNORECASE)

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
    connection_string: str
    query_template: str  # optional; can be empty


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
    cfg = ConfigParser()
    cfg.read(path, encoding="utf-8")
    return cfg


def _get_list(cfg: ConfigParser, section: str, key: str) -> List[str]:
    raw = cfg.get(section, key, fallback="").strip()
    if not raw:
        return []
    # keep multi-line list style
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
        # key:col:dtype
        parts = [p.strip() for p in line.split(":", 2)]
        if len(parts) != 3:
            continue
        fields.append(DataField(key=parts[0], col=parts[1], dtype=parts[2]))
    return fields


def _parse_waive_mapping(cfg: ConfigParser) -> Dict[str, str]:
    if not cfg.has_section("WaiveLengthCategoryMapping"):
        return {}
    # ConfigParser lowercases keys by default; normalize to upper
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
        connection_string=cfg.get("Database", "db_connection_string", fallback="").strip(),
        query_template=cfg.get("Database", "query_template", fallback="").strip(),
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
        # Excel serial date may appear; openpyxl usually converts to datetime if cell is date-type,
        # but keep a safe fallback.
        try:
            # Excel epoch is 1899-12-30 in many implementations
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
        logger.warning("Failed to read StartDateTime from %s!%s: %s", settings.sheet_name, settings.cell, exc)

    if dt_obj is None:
        if settings.fallback_mode == "now":
            dt_obj = datetime.now()
        elif settings.fallback_mode == "blank":
            return ""
        else:
            # default: file_mtime
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
    # default: unknown
    logger.warning("LotRule '%s' not found in mapping; use %s", lot_rule, waive.unknown_value)
    return waive.unknown_value


# =========================
# DB query (optional template)
# =========================

def query_database(
    db: DatabaseSettings,
    lot_id: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Query DB using lot_id and return a dict of fields.

    使用 INI 的 query_template（若有）進行 DB 查詢。
    - query_template 應使用 '?' 佔位符，例如：
        SELECT PartNumber, LotNumber_9 FROM YourTable WHERE LotID = ?
    - 回傳 dict：欄位名 -> 值
    """
    if not db.connection_string:
        logger.warning("DB connection_string is empty; skip DB query.")
        return {}
    if not db.query_template:
        logger.warning("DB query_template is not set; skip DB query.")
        return {}
    if pyodbc is None:
        logger.warning("pyodbc is not installed/available; skip DB query.")
        return {}

    try:
        conn = pyodbc.connect(db.connection_string)  # type: ignore[union-attr]
        cur = conn.cursor()
        cur.execute(db.query_template, (lot_id,))
        row = cur.fetchone()
        if row is None:
            logger.warning("DB query returned no result for LotID=%s", lot_id)
            return {}
        cols = [d[0] for d in cur.description] if cur.description else []
        result: Dict[str, Any] = {}
        for idx, col in enumerate(cols):
            result[str(col)] = row[idx]
        cur.close()
        conn.close()
        return result
    except Exception as exc:
        logger.exception("DB query failed for LotID=%s: %s", lot_id, exc)
        return {}


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

    對每個數值欄位（col index）計算 MAX/MIN/AVG/STD，輸出欄名格式 A：{name}_{STAT}。
    """
    out: Dict[str, Any] = {}

    # measurement fields: numeric excel columns only
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
) -> List[Path]:
    """
    Discover all files matching patterns in the input paths.

    掃描所有符合 patterns 的檔案。
    """
    found: List[Path] = []
    for base in input_paths:
        if not base.exists():
            logger.warning("Input path not found: %s", base)
            continue
        for pat in patterns:
            # Use glob in the directory only (not recursive); adjust if needed
            for p in base.glob(pat):
                if p.name.startswith("~$"):
                    continue
                found.append(p)
    # de-duplicate
    uniq = sorted({p.resolve() for p in found})
    logger.info("Discovered %d candidate files.", len(uniq))
    return uniq


def parse_filename_ids(file_path: Path) -> Tuple[str, str]:
    """
    Parse wafer_id and lot_id from filename.

    從檔名取得 wafer_id(第一段) 與 lot_id(第二段)。
    """
    m = FILENAME_RE.match(file_path.name)
    if not m:
        raise ValueError(f"Invalid filename format: {file_path.name}")
    wafer_id = m.group(1).upper()
    lot_id = m.group(2).upper()
    return wafer_id, lot_id


# =========================
# Main processing per file
# =========================

def build_output_row(
    file_path: Path,
    settings: Settings,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Build the final output row for one wafer file.

    對單一檔案產生一列 CSV 輸出：
    - Serial_Number / Start_Date_Time / Part_Number（必填）
    - TESTER_ID（AY23）
    - Waive_Leng_Cate（mapping）
    - DB 欄位（若有）
    - 各量測欄位統計（MAX/MIN/AVG/STD）
    """
    _wafer_id, lot_id = parse_filename_ids(file_path)

    serial_number = lot_id
    start_dt_str = read_start_datetime(file_path, settings.start_dt, logger)

    # Waive_Leng_Cate from INI mapping
    waive_leng = compute_waive_leng_cate(lot_id, settings.waive, logger)

    # Part_Number rule: always "HL13E1" (even if mapping unknown)
    part_number = "HL13E1"

    tester_id = read_tester_id_ay23(file_path, settings.excel.sheet_name, logger)
    if not tester_id:
        tester_id = settings.basic.tool_name_fallback

    # DB lookup using Lot ID (N????)
    db_fields = query_database(settings.db, lot_id, logger)

    # Read main data table and compute stats
    df = read_main_table(file_path, settings.excel, logger)
    stats = compute_stats_for_fields(df, settings.fields, logger)

    row: Dict[str, Any] = {
        "Serial_Number": serial_number,
        "Start_Date_Time": start_dt_str,
        "Part_Number": part_number,
        "TESTER_ID": tester_id,
        "Waive_Leng_Cate": waive_leng,
        "Source_File": file_path.name,  # traceability
    }

    # merge DB fields (if any)
    for k, v in db_fields.items():
        row[str(k)] = v

    # merge stats
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
        logger.info("Dedup skipped (enable_dedup=%s, skip_dedup_check=%s)",
                    settings.dedup.enable_dedup, settings.dedup.skip_dedup_check)

    files = discover_source_files(
        settings.paths.input_paths,
        settings.basic.file_name_patterns,
        logger,
    )

    processed_count = 0
    skipped_count = 0
    error_count = 0

    for f in files:
        try:
            # strict filename check
            parse_filename_ids(f)

            fp = compute_fingerprint(f, settings.dedup.fingerprint_mode)
            st = f.stat()

            if registry and registry.has(fp):
                skipped_count += 1
                logger.info("Skip (dedup): %s", f.name)
                continue

            # build row
            row = build_output_row(f, settings, logger)

            # append to csv
            append_csv(csv_out, row)
            processed_count += 1
            logger.info("Processed: %s", f.name)

            # dedup registry mark success
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
            # mark failure (optional)
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
                # ignore dedup logging errors
                pass

    xml_out: Optional[Path] = None
    if csv_out.exists() and csv_out.stat().st_size > 0:
        # Use a run-level serial as CSV stem; your legacy used csv stem as serial,
        # but here we follow your new rule: Serial_Number is Lot ID per-row.
        # For pointer XML, choose a unique serial for traceability:
        serial_for_xml = csv_out.stem
        xml_out = generate_pointer_xml(settings.paths.output_path, settings, csv_out, serial_for_xml)
        logger.info("Pointer XML generated: %s", xml_out)

    logger.info("==== End ====")
    logger.info("Processed=%d Skipped=%d Errors=%d CSV=%s XML=%s",
                processed_count, skipped_count, error_count, csv_out, xml_out)


def main(argv: Sequence[str]) -> int:
    """
    CLI entry.

    用法：
      python E1_Qrun_Uploader.py <your_config.ini>

    若未提供參數，會在同目錄掃描所有 .ini 並逐一執行。
    """
    # Ensure working directory is script dir
    os.chdir(Path(__file__).resolve().parent)

    if len(argv) >= 2:
        ini_path = Path(argv[1])
        if not ini_path.exists():
            print(f"INI not found: {ini_path}")
            return 2
        run_for_ini(ini_path)
        return 0

    # fallback: run all ini in current dir
    ini_files = sorted(Path(".").glob("*.ini"))
    if not ini_files:
        print("No .ini files found in current directory.")
        return 1

    for ini in ini_files:
        run_for_ini(ini)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
