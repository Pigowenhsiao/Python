from __future__ import annotations

from configparser import ConfigParser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class GeneralSettings:
    site: str
    product_family: str
    operation: str
    test_station: str
    x: str
    y: str


@dataclass
class PathSettings:
    input_root: Path
    legacy_input_root: Optional[Path]
    csv_output: Path
    xml_output: Path
    log_root: Path
    serial_whitelist: Optional[Path]


@dataclass
class FilterSettings:
    filename_patterns: List[str]
    recipe_keywords: List[str]
    blank_check_cells: List[str]


@dataclass
class EquipmentRule:
    default: str
    cell: Optional[str] = None
    trigger: Optional[str] = None
    trigger_value: Optional[str] = None


@dataclass
class EquipmentSettings:
    sem: EquipmentRule
    xrd: EquipmentRule
    plmapper: EquipmentRule
    mocvd: EquipmentRule


@dataclass
class MappingEntry:
    key: str
    sheet: str
    cell: str
    aggregate: Optional[str] = None
    references: List[str] = field(default_factory=list)


@dataclass
class MappingGroup:
    name: str
    rules: List[str]
    entries: List[MappingEntry]


@dataclass
class ExcelSettings:
    mode: str
    sheet_name: Optional[str]
    date_sheet_name: Optional[str]
    date_cell: Optional[str]


@dataclass
class WriterSettings:
    csv_filename: str
    pointer_table_prefix: str


@dataclass
class PipelineConfig:
    general: GeneralSettings
    paths: PathSettings
    filters: FilterSettings
    excel: ExcelSettings
    writer: WriterSettings
    equipment: EquipmentSettings
    extractor: str
    mapping_groups: Dict[str, MappingGroup]
    key_types: Dict[str, str]


def _load_parser(path: Path) -> ConfigParser:
    parser = ConfigParser()
    parser.optionxform = str
    with path.open("r", encoding="utf-8") as fh:
        parser.read_file(fh)
    return parser


def _resolve_path(base_dir: Path, raw: Optional[str]) -> Optional[Path]:
    if not raw:
        return None
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = (base_dir / candidate).resolve()
    return candidate


def _load_mapping_file(base_dir: Path, mapping_path: str) -> List[MappingEntry]:
    entries: List[MappingEntry] = []
    parser = _load_parser(_resolve_path(base_dir, mapping_path))
    for key, value in parser.items("fields"):
        value = value.strip()
        if value.startswith("AVG(") and value.endswith(")"):
            refs = [ref.strip() for ref in value[4:-1].split(",")]
            entries.append(
                MappingEntry(
                    key=key,
                    sheet="aggregate",
                    cell="",
                    aggregate="AVG",
                    references=refs,
                )
            )
            continue
        if "!" in value:
            sheet_name, cell = value.split("!", 1)
        else:
            sheet_name, cell = "main", value
        entries.append(
            MappingEntry(
                key=key,
                sheet=sheet_name,
                cell=cell,
            )
        )
    return entries


def _load_mapping_groups(
    parser: ConfigParser, base_dir: Path
) -> Dict[str, MappingGroup]:
    group_rules: Dict[str, List[str]] = {}
    if parser.has_section("group_rules"):
        for group, raw_rules in parser.items("group_rules"):
            rules = [token.strip() for token in raw_rules.split(",") if token.strip()]
            group_rules[group] = rules

    mappings: Dict[str, MappingGroup] = {}
    if parser.has_section("mappings"):
        for group, mapping_path in parser.items("mappings"):
            entries = _load_mapping_file(base_dir, mapping_path)
            mappings[group] = MappingGroup(
                name=group, rules=group_rules.get(group, []), entries=entries
            )
    return mappings


def _load_key_types(parser: ConfigParser, base_dir: Path) -> Dict[str, str]:
    if parser.has_section("keytypes") and parser.has_option("keytypes", "file"):
        key_file = _resolve_path(base_dir, parser.get("keytypes", "file"))
        key_parser = _load_parser(key_file)
        return {k: v for k, v in key_parser.items("keytypes")}
    if parser.has_section("keytypes"):
        return {k: v for k, v in parser.items("keytypes")}
    return {}


def _parse_equipment(parser: ConfigParser) -> EquipmentSettings:
    def build(prefix: str, fallback: str) -> EquipmentRule:
        default = parser.get("equipment", f"{prefix}_default", fallback=fallback)
        cell = parser.get("equipment", f"{prefix}_cell", fallback=None)
        trigger = parser.get("equipment", f"{prefix}_trigger", fallback=None)
        trigger_value = parser.get(
            "equipment", f"{prefix}_trigger_value", fallback=None
        )
        return EquipmentRule(
            default=str(default),
            cell=cell,
            trigger=trigger,
            trigger_value=trigger_value,
        )

    return EquipmentSettings(
        sem=build("sem", "1"),
        xrd=build("xrd", "1"),
        plmapper=build("plmapper", "1"),
        mocvd=build("mocvd", "1"),
    )


def load_config(config_path: Path) -> PipelineConfig:
    config_path = config_path.resolve()
    parser = ConfigParser()
    parser.optionxform = str
    config_dir = config_path.parent

    raw = _load_parser(config_path)
    if raw.has_section("meta") and raw.has_option("meta", "base_config"):
        base_config = _resolve_path(config_dir, raw.get("meta", "base_config"))
        parser.read(base_config, encoding="utf-8")
    parser.read(config_path, encoding="utf-8")

    general = GeneralSettings(
        site=parser.get("general", "site"),
        product_family=parser.get("general", "product_family"),
        operation=parser.get("general", "operation"),
        test_station=parser.get("general", "test_station"),
        x=parser.get("general", "x"),
        y=parser.get("general", "y"),
    )

    paths = PathSettings(
        input_root=_resolve_path(config_dir, parser.get("paths", "input_root")),
        legacy_input_root=_resolve_path(
            config_dir, parser.get("paths", "legacy_input_root", fallback=None)
        ),
        csv_output=_resolve_path(config_dir, parser.get("paths", "csv_output")),
        xml_output=_resolve_path(config_dir, parser.get("paths", "xml_output")),
        log_root=_resolve_path(config_dir, parser.get("paths", "log_root")),
        serial_whitelist=_resolve_path(
            config_dir, parser.get("paths", "serial_whitelist", fallback=None)
        ),
    )

    filters = FilterSettings(
        filename_patterns=[
            token.strip()
            for token in parser.get("filters", "filename_patterns").split(",")
            if token.strip()
        ],
        recipe_keywords=[
            token.strip()
            for token in parser.get("filters", "recipe_keywords", fallback="").split(",")
            if token.strip()
        ],
        blank_check_cells=[
            token.strip()
            for token in parser.get("filters", "blank_check_cells", fallback="").split(",")
            if token.strip()
        ],
    )

    excel = ExcelSettings(
        mode=parser.get("excel", "mode"),
        sheet_name=parser.get("excel", "sheet_name", fallback=None),
        date_sheet_name=parser.get("excel", "date_sheet_name", fallback=None),
        date_cell=parser.get("excel", "date_cell", fallback=None),
    )

    writer = WriterSettings(
        csv_filename=parser.get("writer", "csv_filename"),
        pointer_table_prefix=parser.get(
            "writer", "pointer_table_prefix", fallback="tbl"
        ),
    )

    equipment = _parse_equipment(parser)

    config = PipelineConfig(
        general=general,
        paths=paths,
        filters=filters,
        excel=excel,
        writer=writer,
        equipment=equipment,
        extractor=parser.get("pipeline", "extractor"),
        mapping_groups=_load_mapping_groups(parser, config_dir),
        key_types=_load_key_types(parser, config_dir),
    )
    return config
