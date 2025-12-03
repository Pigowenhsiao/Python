from __future__ import annotations

from typing import Dict, Any


TYPE_CASTERS = {
    "str": str,
    "float": float,
    "int": int,
}


def validate_record(record: Dict[str, Any], key_types: Dict[str, str]) -> bool:
    for key, expected in key_types.items():
        if key not in record:
            continue
        value = record[key]
        if value in ("", None):
            continue
        caster = TYPE_CASTERS.get(expected)
        if caster is None:
            continue
        try:
            caster(value)
        except (ValueError, TypeError):
            return False
    return True
