from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

from .config_loader import PathSettings, FilterSettings


@dataclass
class WorkbookCandidate:
    path: Path
    created_at: float


class WorkbookCollector:
    def __init__(self, paths: PathSettings, filters: FilterSettings):
        self.paths = paths
        self.filters = filters

    def iter_files(self) -> Iterable[Path]:
        locations: List[Path] = [self.paths.input_root]
        if self.paths.legacy_input_root:
            locations.append(self.paths.legacy_input_root)

        candidates: List[WorkbookCandidate] = []
        for location in locations:
            if not location or not location.exists():
                continue
            for root, _, files in os.walk(location):
                for name in files:
                    if not self._match_pattern(name):
                        continue
                    full_path = Path(root) / name
                    try:
                        stat = full_path.stat()
                    except OSError:
                        continue
                    candidates.append(WorkbookCandidate(full_path, stat.st_ctime))

        for candidate in sorted(candidates, key=lambda item: item.created_at):
            yield candidate.path

    def _match_pattern(self, filename: str) -> bool:
        lowered = filename.lower()
        if lowered.startswith("~$"):
            return False
        if not lowered.endswith((".xlsx", ".xlsm", ".xls")):
            return False
        if not self.filters.filename_patterns:
            return True
        return any(pattern.lower() in lowered for pattern in self.filters.filename_patterns)

