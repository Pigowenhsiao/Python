from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

_LOGGERS: Dict[str, logging.Logger] = {}


def _get_logger(log_file: str) -> logging.Logger:
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    key = str(log_path.resolve())
    logger = _LOGGERS.get(key)
    if logger is None:
        logger = logging.getLogger(key)
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        logger.propagate = False
        _LOGGERS[key] = logger
    return logger


def log_info(log_file: str, message: str) -> None:
    print(f"[INFO] {message}")
    _get_logger(log_file).info(message)


def log_error(log_file: str, message: str) -> None:
    print(f"[ERROR] {message}")
    _get_logger(log_file).error(message)


def log_warning(log_file: str, message: str) -> None:
    print(f"[WARN] {message}")
    _get_logger(log_file).warning(message)
