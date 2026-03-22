"""Shared utilities."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Optional, Union

try:
    import certifi
except ImportError:  # pragma: no cover
    certifi = None

try:
    import urllib3
except ImportError:  # pragma: no cover
    urllib3 = None

APP_LOGGER_NAME = "geo_analyzer"


def setup_logging(level_name: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger(APP_LOGGER_NAME)
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(getattr(logging, level_name, logging.INFO))

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file:
        try:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except OSError as exc:
            logger.warning("日志文件不可写 [%s]: %s。已回退到仅控制台日志。", log_file, exc)

    return logger


def resolve_tls_verify(config_verify_tls: bool, insecure_flag: bool, logger: logging.Logger) -> Union[bool, str]:
    if insecure_flag:
        if urllib3 is not None:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        logger.warning("TLS 证书校验已通过 --insecure 关闭，仅建议在调试环境使用。")
        return False

    if not config_verify_tls:
        logger.warning("security.verify_tls=false 已被忽略；仅 --insecure 会关闭 TLS 校验。")

    if certifi is not None:
        return certifi.where()
    return True


def parse_datetime_flexible(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    iso_text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_text)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except ValueError:
        pass

    normalized = text
    normalized = normalized.replace("年", "-").replace("月", "-").replace("日", " ")
    normalized = normalized.replace("/", "-").replace(".", "-")
    normalized = normalized.replace("T", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = re.sub(r"(?:UTC|GMT)\s*[+-]?\d{0,2}:?\d{0,2}$", "", normalized, flags=re.I).strip()
    normalized = re.sub(r"[+-]\d{2}:?\d{2}$", "", normalized).strip()

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%d%H%M%S",
        "%Y%m%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue

    m = re.search(
        r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?",
        normalized,
    )
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hour = int(m.group(4) or 0)
        minute = int(m.group(5) or 0)
        second = int(m.group(6) or 0)
        try:
            return datetime(year, month, day, hour, minute, second)
        except ValueError:
            return None

    return None
