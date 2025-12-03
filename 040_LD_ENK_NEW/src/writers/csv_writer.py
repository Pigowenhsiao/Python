from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Dict, Any, List

from ..common_log import log_info


def append_records(
    csv_path: Path,
    field_order: List[str],
    records: Iterable[Dict[str, Any]],
    log_file: str,
) -> int:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    wrote = 0
    with csv_path.open("a", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=field_order, extrasaction="ignore")
        if fh.tell() == 0:
            writer.writeheader()
        for record in records:
            writer.writerow(record)
            wrote += 1
    log_info(log_file, f"CSV append complete ({wrote} rows) -> {csv_path}")
    return wrote
