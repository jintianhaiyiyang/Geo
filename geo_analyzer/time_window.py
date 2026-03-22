"""Time-window parsing and article filtering utilities."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .utils import parse_datetime_flexible

TIME_PRESET_DAYS = {
    "today": 1,
    "week": 7,
    "month": 30,
}


def resolve_time_window(
    preset: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    recent_months: int,
) -> Dict[str, Any]:
    """Build an explicit time window from preset/custom/recent-months inputs."""
    now = datetime.now()
    soft_upper = now + timedelta(minutes=5)

    parsed_from = parse_datetime_flexible(date_from)
    parsed_to = parse_datetime_flexible(date_to)

    if date_from and parsed_from is None:
        raise ValueError(f"invalid --date-from: {date_from!r}")
    if date_to and parsed_to is None:
        raise ValueError(f"invalid --date-to: {date_to!r}")

    if parsed_from or parsed_to:
        if parsed_from is None:
            parsed_from = datetime.min
        if parsed_to is None:
            parsed_to = now
        if parsed_from > parsed_to:
            raise ValueError("--date-from must be <= --date-to")
        return {
            "mode": "custom",
            "preset": "",
            "date_from": parsed_from.strftime("%Y-%m-%d %H:%M:%S"),
            "date_to": parsed_to.strftime("%Y-%m-%d %H:%M:%S"),
        }

    normalized_preset = (preset or "").strip().lower()
    if normalized_preset:
        if normalized_preset not in TIME_PRESET_DAYS:
            raise ValueError(f"invalid --time-preset: {preset!r}, choices={sorted(TIME_PRESET_DAYS)}")
        days = TIME_PRESET_DAYS[normalized_preset]
        date_from_dt = datetime(now.year, now.month, now.day) - timedelta(days=days - 1)
        return {
            "mode": "preset",
            "preset": normalized_preset,
            "date_from": date_from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "date_to": soft_upper.strftime("%Y-%m-%d %H:%M:%S"),
        }

    if recent_months > 0:
        date_from_dt = now - timedelta(days=recent_months * 30)
        return {
            "mode": "recent_months",
            "preset": "",
            "date_from": date_from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "date_to": soft_upper.strftime("%Y-%m-%d %H:%M:%S"),
        }

    return {
        "mode": "none",
        "preset": "",
        "date_from": "",
        "date_to": "",
    }


def _parse_window_bound(value: str, fallback: datetime) -> datetime:
    parsed = parse_datetime_flexible(value)
    return parsed if parsed is not None else fallback


def filter_articles_by_time_window(
    articles: List[Dict[str, Any]],
    time_window: Dict[str, Any],
    include_undated: bool,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """Filter articles using an explicit datetime window."""
    mode = str(time_window.get("mode", "none") or "none")
    if mode == "none":
        return articles

    log = logger or logging.getLogger("geo_analyzer.time_window")
    lower = _parse_window_bound(str(time_window.get("date_from", "")), datetime.min)
    upper = _parse_window_bound(str(time_window.get("date_to", "")), datetime.max)

    kept: List[Dict[str, Any]] = []
    outdated = 0
    undated = 0

    for article in articles:
        publish_dt = parse_datetime_flexible(article.get("publish_time"))
        if publish_dt is None:
            undated += 1
            if include_undated:
                kept.append(article)
            continue

        article["publish_time"] = publish_dt.strftime("%Y-%m-%d %H:%M:%S")
        if lower <= publish_dt <= upper:
            kept.append(article)
        else:
            outdated += 1

    log.info(
        "Time-window filter(mode=%s, from=%s, to=%s): kept=%d dropped=%d undated=%d include_undated=%s",
        mode,
        lower.strftime("%Y-%m-%d"),
        upper.strftime("%Y-%m-%d"),
        len(kept),
        outdated,
        undated,
        include_undated,
    )
    return kept
