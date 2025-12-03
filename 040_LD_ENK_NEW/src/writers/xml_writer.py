from __future__ import annotations

from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET
from xml.dom import minidom

from ..common_log import log_info
from ..config_loader import GeneralSettings, WriterSettings


def write_pointer_xml(
    output_dir: Path,
    csv_path: Path,
    general: GeneralSettings,
    writer_settings: WriterSettings,
    log_file: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    serial_no = csv_path.stem
    xml_name = (
        f"Site={general.site},"
        f"ProductFamily={general.product_family},"
        f"Operation={general.operation},"
        f"Partnumber=UNKNOWPN,"
        f"Serialnumber={serial_no},"
        f"Testdate={now_iso}.xml"
    ).replace(":", ".")
    xml_path = output_dir / xml_name

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
        {"startDateTime": now_iso, "endDateTime": now_iso, "Result": "Passed"},
    )
    ET.SubElement(
        result,
        "Header",
        {
            "SerialNumber": serial_no,
            "PartNumber": "UNKNOWPN",
            "Operation": general.operation,
            "TestStation": general.test_station,
            "Operator": "NA",
            "StartTime": now_iso,
            "Site": general.site,
            "LotNumber": "",
        },
    )
    test_step = ET.SubElement(
        result,
        "TestStep",
        {
            "Name": general.operation,
            "startDateTime": now_iso,
            "endDateTime": now_iso,
            "Status": "Passed",
        },
    )
    table_name = f"{writer_settings.pointer_table_prefix}_{general.operation.upper()}"
    ET.SubElement(
        test_step,
        "Data",
        {
            "DataType": "Table",
            "Name": table_name,
            "Value": str(csv_path),
            "CompOperation": "LOG",
        },
    )

    xml_str = minidom.parseString(ET.tostring(results)).toprettyxml(
        indent="  ", encoding="utf-8"
    )
    with xml_path.open("wb") as fh:
        fh.write(xml_str)
    log_info(log_file, f"Pointer XML generated -> {xml_path}")
    return xml_path
