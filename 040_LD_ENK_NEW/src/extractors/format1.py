from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import openpyxl as px

from ..lib import SQL, ExpandExp

from ..common_log import log_error, log_info, log_warning

from ..config_loader import (
    PipelineConfig,
    MappingEntry,
    EquipmentRule,
)


@dataclass
class ExtractionResult:
    sheet_name: str
    data: Dict[str, object]


class Format1Extractor:
    SPECIAL_TRANSFORMS = {
        "key_start_date_time": lambda value: str(value or "").replace(" ", "T"),
    }

    def __init__(self, config: PipelineConfig, log_file: str):
        self.config = config
        self.log_file = log_file
        self.serial_initials = self._load_serial_initials()

    def _load_serial_initials(self) -> Optional[List[str]]:
        path = self.config.paths.serial_whitelist
        if not path or not path.exists():
            return None
        with path.open("r", encoding="utf-8") as fh:
            return [line.strip() for line in fh if line.strip()]

    def extract(self, workbook_path: str) -> List[Dict[str, object]]:
        log_info(self.log_file, f"Opening workbook {workbook_path}")
        wb = px.load_workbook(workbook_path, data_only=True, read_only=True)
        records: List[Dict[str, object]] = []
        for sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            if not self._sheet_allowed(sheet):
                log_info(self.log_file, f"Skipping sheet {sheet_name}: not eligible")
                continue
            log_info(self.log_file, f"Extracting sheet {sheet_name}")
            record = self._extract_sheet(workbook_path, sheet, sheet_name)
            if record:
                records.append(record)
                log_info(self.log_file, f"Sheet {sheet_name} extracted successfully")
            else:
                log_warning(self.log_file, f"Sheet {sheet_name} extraction returned no data")
        wb.close()
        return records

    def _sheet_allowed(self, sheet) -> bool:
        serial_cell = sheet["M8"].value
        if serial_cell is None:
            return False
        serial_str = str(serial_cell)
        if self.serial_initials:
            if not any(serial_str.startswith(token) for token in self.serial_initials):
                return False
            return False

        recipe_value = str(sheet["U3"].value or "")
        keywords = self.config.filters.recipe_keywords
        if keywords and not any(keyword in recipe_value for keyword in keywords):
            return False

        for ref in self.config.filters.blank_check_cells:
            if not ref:
                continue
            if sheet[ref].value is None:
                return False
        return True

    def _extract_sheet(self, workbook_path: str, sheet, sheet_name: str) -> Optional[Dict[str, object]]:
        serial_number = str(sheet["M8"].value).strip()
        conn, cursor = SQL.connSQL()
        if conn is None:
            log_error(self.log_file, f"{serial_number} : Connection with Prime Failed")
            return None
        try:
            part_number, lot_number = SQL.selectSQL(cursor, serial_number)
        except Exception as exc:
            log_error(self.log_file, f"{serial_number} : SQL Error {exc}")
            SQL.disconnSQL(conn, cursor)
            return None
        SQL.disconnSQL(conn, cursor)

        group = self._resolve_group(part_number)
        if not group:
            log_error(self.log_file, f"{serial_number} : Unknown part number group")
            return None

        data: Dict[str, object] = {
            "key_serial_number": serial_number,
            "key_part_number": part_number,
            "key_LotNumber_9": lot_number,
        }

        for entry in group.entries:
            value = self._read_entry(entry, sheet)
            if entry.key in self.SPECIAL_TRANSFORMS:
                value = self.SPECIAL_TRANSFORMS[entry.key](value)
            data[entry.key] = value

        operator_value = data.get("key_operator")
        if operator_value in ("", None):
            data["key_operator"] = "-"

        data.update(self._resolve_equipment(sheet))
        self._expand_scientific_numbers(data)
        self._compute_sort_fields(data)
        data["Operation"] = self.config.general.operation
        data["TestStation"] = self.config.general.test_station
        data["Site"] = self.config.general.site
        data["SourceSheet"] = sheet_name

        return data

    def _read_entry(self, entry: MappingEntry, sheet):
        if entry.aggregate:
            values = [self._resolve_reference(ref, sheet) for ref in entry.references]
            values = [v for v in values if v not in (None, "")]
            if not values:
                return ""
            if entry.aggregate == "AVG":
                return sum(float(v) for v in values) / len(values)
            return ""
        if entry.sheet.lower() == "main":
            target_sheet = sheet
        else:
            target_sheet = sheet.parent[entry.sheet]
        return target_sheet[entry.cell].value

    def _resolve_reference(self, ref: str, sheet):
        if "!" in ref:
            sheet_name, cell = ref.split("!", 1)
        else:
            sheet_name, cell = "main", ref
        if sheet_name.lower() == "main":
            return sheet[cell].value
        return sheet.parent[sheet_name][cell].value

    def _resolve_group(self, part_number: Optional[str]):
        if not part_number:
            return None
        for group in self.config.mapping_groups.values():
            if not group.rules:
                return group
            for token in group.rules:
                if token and token in part_number:
                    return group
        return None

    def _resolve_equipment(self, sheet) -> Dict[str, str]:
        return {
            "key_TestEquipment_SEM": self._equipment_value(sheet, self.config.equipment.sem),
            "key_TestEquipment_XRD": self._equipment_value(sheet, self.config.equipment.xrd),
            "key_TestEquipment_PLmapper": self.config.equipment.plmapper.default,
            "key_TestEquipment_MOCVD": self.config.equipment.mocvd.default,
        }

    @staticmethod
    def _equipment_value(sheet, rule: EquipmentRule) -> str:
        value = rule.default
        if rule.cell and rule.trigger:
            cell_value = sheet[rule.cell].value
            if cell_value and rule.trigger in str(cell_value):
                value = rule.trigger_value or rule.trigger.strip("#")
        return str(value)

    @staticmethod
    def _expand_scientific_numbers(data: Dict[str, object]) -> None:
        for key, value in list(data.items()):
            if isinstance(value, float) and "e" in f"{value}":
                data[key] = ExpandExp.Expand(value)

    @staticmethod
    def _compute_sort_fields(data: Dict[str, object]) -> None:
        dt_value = data.get("key_start_date_time")
        batch_number = str(data.get("key_batch_number") or "")
        if not dt_value:
            return
        normalized = str(dt_value).replace("T", " ").replace(".", ":")
        try:
            dt_obj = datetime.strptime(normalized[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return
        base = datetime(1899, 12, 30)
        excel_number = (dt_obj - base).days
        epi_number = 0
        for char in batch_number:
            if char.isdigit():
                epi_number = epi_number * 10 + int(char)
        data["key_STARTTIME_SORTED"] = excel_number + (epi_number / 1_000_000)
        data["key_SORTNUMBER"] = epi_number
