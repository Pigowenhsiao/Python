from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, Any, List

from .common_log import log_info, log_error, log_warning
from .config_loader import PipelineConfig, load_config
from .workbook_collector import WorkbookCollector
from .record_validator import validate_record
from .writers import csv_writer, xml_writer
from .extractors import get_extractor


class PipelineRunner:
    def __init__(self, config_path: Path):
        self.config_path = Path(config_path)
        self.config: PipelineConfig = load_config(self.config_path)
        self.log_file = self._init_log_file()
        extractor_cls = get_extractor(self.config.extractor)
        self.extractor = extractor_cls(self.config, self.log_file)

    def _init_log_file(self) -> str:
        log_dir = self.config.paths.log_root / str(date.today())
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{self.config.general.operation}.log"
        log_info(str(log_file), "Pipeline bootstrap")
        return str(log_file)

    def run(self) -> None:
        collector = WorkbookCollector(self.config.paths, self.config.filters)
        log_info(
            self.log_file,
            f"Searching files in {self.config.paths.input_root} (legacy={self.config.paths.legacy_input_root})",
        )
        csv_path = self._resolve_csv_path()
        field_order: List[str] = []
        total_rows = 0

        for workbook in collector.iter_files():
            log_info(self.log_file, f"Processing workbook: {workbook}")
            records = self.extractor.extract(str(workbook))
            log_info(self.log_file, f"Total sheets extracted from {workbook}: {len(records)}")
            prepared: List[Dict[str, Any]] = []
            for record in records:
                serial = record.get("key_serial_number", "UNKNOWN")
                log_info(self.log_file, f"Validating serial {serial} from {workbook}")
                if not validate_record(record, self.config.key_types):
                    log_error(
                        self.log_file,
                        f"{serial} : data validation failed",
                    )
                    continue
                row = self._to_csv_row(record)
                log_info(self.log_file, f"Record accepted for serial {serial}")
                prepared.append(row)
            if not prepared:
                log_warning(self.log_file, f"No valid rows generated from {workbook}")
                continue
            if not field_order:
                field_order = self._build_field_order(prepared[0])
            total_rows += csv_writer.append_records(csv_path, field_order, prepared, self.log_file)
            log_info(self.log_file, f"{len(prepared)} rows appended for {workbook}")

        if total_rows == 0:
            log_warning(self.log_file, "Pipeline finished without any output rows")
            return

        if total_rows:
            xml_writer.write_pointer_xml(
                self.config.paths.xml_output,
                csv_path,
                self.config.general,
                self.config.writer,
                self.log_file,
            )
        log_info(self.log_file, f"Run finished, rows written: {total_rows}")

    def _resolve_csv_path(self) -> Path:
        template = self.config.writer.csv_filename
        filename = template.format(
            operation=self.config.general.operation,
            date=datetime.now().strftime("%Y%m%d"),
        )
        return (self.config.paths.csv_output / filename).resolve()

    @staticmethod
    def _to_csv_row(record: Dict[str, Any]) -> Dict[str, Any]:
        row: Dict[str, Any] = {}
        for key, value in record.items():
            if key.startswith("key_"):
                row[key[4:]] = value
            else:
                row[key] = value
        if "Start_Date_Time" not in row and "start_date_time" in row:
            row["Start_Date_Time"] = row["start_date_time"]
        return row

    @staticmethod
    def _build_field_order(sample: Dict[str, Any]) -> List[str]:
        base = ["Serial_Number", "Part_Number", "Start_Date_Time", "Operation", "TestStation", "Site"]
        others = [key for key in sample.keys() if key not in base]
        return base + sorted(others)
