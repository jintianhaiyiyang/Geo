"""URL/content dedupe helpers and in-memory per-site cache."""

from __future__ import annotations

import copy
import hashlib
import re
from collections import OrderedDict, defaultdict
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_QUERY_KEYS = {
    "spm",
    "source",
    "from",
    "fromid",
    "ref",
    "refer",
    "referer",
}

WECHAT_HOST = "mp.weixin.qq.com"
WECHAT_ARTICLE_REQUIRED_KEYS = ("__biz", "mid", "idx")
WECHAT_ARTICLE_OPTIONAL_KEYS = ("sn", "chksm")


def normalize_url(url: str) -> str:
    """Normalize URL for stable dedupe."""
    if not url:
        return ""

    text = url.strip()
    if not text:
        return ""

    parts = urlsplit(text)
    if not parts.scheme or not parts.netloc:
        return ""

    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    if netloc == WECHAT_HOST and path.startswith("/s"):
        query_map = {}
        for key, value in parse_qsl(parts.query, keep_blank_values=False):
            if key in WECHAT_ARTICLE_REQUIRED_KEYS or key in WECHAT_ARTICLE_OPTIONAL_KEYS:
                query_map[key] = value
        if all(query_map.get(key) for key in WECHAT_ARTICLE_REQUIRED_KEYS):
            ordered_items = []
            for key in (*WECHAT_ARTICLE_REQUIRED_KEYS, *WECHAT_ARTICLE_OPTIONAL_KEYS):
                if key in query_map:
                    ordered_items.append((key, query_map[key]))
            query = urlencode(ordered_items, doseq=True)
            return urlunsplit(("https", WECHAT_HOST, "/s", query, ""))

    kept_params = []
    for key, value in parse_qsl(parts.query, keep_blank_values=False):
        lowered = key.lower()
        if lowered.startswith("utm_"):
            continue
        if lowered in TRACKING_QUERY_KEYS:
            continue
        kept_params.append((key, value))
    kept_params.sort(key=lambda item: (item[0], item[1]))
    query = urlencode(kept_params, doseq=True)

    return urlunsplit((scheme, netloc, path, query, ""))


def normalize_text_for_hash(text: str) -> str:
    if not text:
        return ""
    lowered = text.lower()
    return re.sub(r"\s+", " ", lowered).strip()


def compute_content_hash(text: str) -> str:
    normalized = normalize_text_for_hash(text)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class SiteCache:
    """In-memory LRU-like cache partitioned by domain."""

    def __init__(self, max_per_domain: int = 200):
        if max_per_domain <= 0:
            raise ValueError("max_per_domain must be positive")
        self.max_per_domain = max_per_domain
        self._data: Dict[str, OrderedDict[str, Any]] = defaultdict(OrderedDict)
        self._stats = {"hits": 0, "misses": 0, "stores": 0, "evictions": 0}

    def _domain_key(self, normalized_url: str) -> str:
        parts = urlsplit(normalized_url)
        return (parts.netloc or "").lower()

    def get(self, url: str) -> Optional[Any]:
        normalized = normalize_url(url)
        if not normalized:
            self._stats["misses"] += 1
            return None

        domain = self._domain_key(normalized)
        bucket = self._data.get(domain)
        if not bucket or normalized not in bucket:
            self._stats["misses"] += 1
            return None

        value = bucket.pop(normalized)
        bucket[normalized] = value
        self._stats["hits"] += 1
        return copy.deepcopy(value)

    def set(self, url: str, value: Any) -> None:
        normalized = normalize_url(url)
        if not normalized:
            return

        domain = self._domain_key(normalized)
        bucket = self._data[domain]
        if normalized in bucket:
            bucket.pop(normalized)
        bucket[normalized] = copy.deepcopy(value)
        self._stats["stores"] += 1

        while len(bucket) > self.max_per_domain:
            bucket.popitem(last=False)
            self._stats["evictions"] += 1

    def stats(self) -> Dict[str, int]:
        return dict(self._stats)
