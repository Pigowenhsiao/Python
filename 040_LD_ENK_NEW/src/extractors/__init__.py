from .format1 import Format1Extractor
from .format2 import Format2Extractor

EXTRACTOR_REGISTRY = {
    "format1": Format1Extractor,
    "format2": Format2Extractor,
}


def get_extractor(name: str):
    try:
        return EXTRACTOR_REGISTRY[name]
    except KeyError as exc:
        raise ValueError(f"Unknown extractor: {name}") from exc

