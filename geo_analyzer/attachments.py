"""Attachment/link evidence detection for crawled articles."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple
from urllib.parse import urlsplit

ATTACHMENT_HOST_SCORES = {
    "pan.baidu.com": 2.6,
    "yun.baidu.com": 2.4,
    "drive.google.com": 2.4,
    "dropbox.com": 2.2,
    "aliyundrive.com": 2.2,
    "www.aliyundrive.com": 2.2,
    "lanzou.com": 2.1,
    "lanzouw.com": 2.1,
    "123pan.com": 2.1,
    "cowtransfer.com": 2.0,
    "mega.nz": 2.2,
    "kdocs.cn": 1.6,
    "github.com": 1.4,
    "huggingface.co": 1.8,
}

FILE_EXTENSION_SCORES = {
    ".zip": 2.2,
    ".rar": 2.2,
    ".7z": 2.2,
    ".tar": 2.0,
    ".gz": 2.0,
    ".bz2": 2.0,
    ".xz": 2.0,
    ".csv": 1.8,
    ".tsv": 1.8,
    ".json": 1.6,
    ".geojson": 2.1,
    ".shp": 2.1,
    ".gpkg": 2.1,
    ".tif": 2.0,
    ".tiff": 2.0,
    ".nc": 2.0,
    ".h5": 2.0,
    ".parquet": 2.0,
    ".sqlite": 1.8,
}

KEYWORD_SCORES = {
    "附件": 0.8,
    "网盘": 1.2,
    "提取码": 1.2,
    "下载": 0.8,
    "下载地址": 1.2,
    "数据下载": 1.3,
    "开放下载": 1.2,
    "dataset": 0.9,
    "data set": 0.9,
    "download": 0.8,
    "api": 0.6,
    "wms": 0.8,
    "wfs": 0.8,
    "wmts": 0.8,
    "服务地址": 1.0,
}

URL_REGEX = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)


def _extract_urls(text: str) -> List[str]:
    if not text:
        return []
    candidates = URL_REGEX.findall(text)
    deduped: List[str] = []
    seen = set()
    for item in candidates:
        url = item.strip().rstrip(").,;")
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _score_url(url: str) -> Tuple[float, List[Dict[str, Any]]]:
    score = 0.0
    evidence: List[Dict[str, Any]] = []
    lowered = url.lower()
    host = (urlsplit(url).hostname or "").lower()

    host_score = ATTACHMENT_HOST_SCORES.get(host, 0.0)
    if host_score > 0:
        score += host_score
        evidence.append(
            {
                "type": "host",
                "url": url,
                "value": host,
                "score": round(host_score, 3),
                "source": "url_host",
            }
        )

    for suffix, suffix_score in FILE_EXTENSION_SCORES.items():
        if lowered.endswith(suffix):
            score += suffix_score
            evidence.append(
                {
                    "type": "file_extension",
                    "url": url,
                    "value": suffix,
                    "score": round(suffix_score, 3),
                    "source": "url_path",
                }
            )
            break

    return score, evidence


def detect_attachments(
    article: Dict[str, Any],
    min_score: float = 1.5,
) -> Dict[str, Any]:
    title = str(article.get("title", "") or "")
    content = str(article.get("content", "") or article.get("snippet", "") or "")
    text = f"{title}\n{content}"

    total_score = 0.0
    evidence: List[Dict[str, Any]] = []

    for url in _extract_urls(text):
        url_score, url_evidence = _score_url(url)
        if url_score > 0:
            total_score += url_score
            evidence.extend(url_evidence)

    lowered_text = text.lower()
    for token, token_score in KEYWORD_SCORES.items():
        hits = lowered_text.count(token.lower())
        if hits <= 0:
            continue
        contribution = min(2.5, hits * token_score)
        total_score += contribution
        evidence.append(
            {
                "type": "keyword",
                "url": "",
                "value": token,
                "score": round(contribution, 3),
                "source": "text",
            }
        )

    evidence.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
    has_attachment = total_score >= float(min_score)
    return {
        "has_attachment": has_attachment,
        "attachment_score": round(total_score, 6),
        "attachment_evidence": evidence[:20],
    }


def annotate_articles_with_attachments(
    articles: List[Dict[str, Any]],
    enabled: bool = True,
    min_score: float = 1.5,
) -> Dict[str, Any]:
    if not enabled:
        for article in articles:
            article["has_attachment"] = False
            article["attachment_score"] = 0.0
            article["attachment_evidence"] = []
        return {"enabled": False, "total_articles": len(articles), "with_attachment": 0}

    with_attachment = 0
    for article in articles:
        result = detect_attachments(article, min_score=min_score)
        article["has_attachment"] = bool(result["has_attachment"])
        article["attachment_score"] = float(result["attachment_score"])
        article["attachment_evidence"] = list(result["attachment_evidence"])
        if result["has_attachment"]:
            with_attachment += 1

    return {
        "enabled": True,
        "total_articles": len(articles),
        "with_attachment": with_attachment,
        "ratio": round((with_attachment / len(articles)), 6) if articles else 0.0,
        "min_score": float(min_score),
    }

