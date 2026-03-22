"""Content extraction engines: trafilatura + bs4 fallback."""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

try:
    import trafilatura
except ImportError:  # pragma: no cover
    trafilatura = None

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover
    BeautifulSoup = None

from .utils import parse_datetime_flexible


class ContentExtractor:
    def __init__(
        self,
        primary: str = "auto",
        fallback: str = "bs4",
        min_content_length: int = 120,
        logger: Optional[logging.Logger] = None,
    ):
        self.primary = primary
        self.fallback = fallback
        self.min_content_length = min_content_length
        self.logger = logger or logging.getLogger("geo_analyzer.extract")
        self.stats: Counter = Counter()

    def _build_engine_chain(self) -> List[str]:
        engines: List[str] = []

        if self.primary == "auto":
            engines.extend(["trafilatura", "bs4"])
        else:
            engines.append(self.primary)

        if self.fallback != "auto" and self.fallback not in engines:
            engines.append(self.fallback)

        return engines

    def extract(self, html: str, final_url: str) -> Optional[Dict[str, str]]:
        if not html:
            self.stats["empty_html"] += 1
            return None

        for engine in self._build_engine_chain():
            parsed: Optional[Dict[str, str]] = None
            try:
                if engine == "trafilatura":
                    parsed = self._extract_with_trafilatura(html, final_url)
                elif engine == "bs4":
                    parsed = self._extract_with_bs4(html, final_url)
            except Exception as exc:  # pragma: no cover
                self.logger.warning("提取器异常 engine=%s: %s", engine, exc)
                self.stats[f"{engine}_error"] += 1
                parsed = None

            if not parsed:
                self.stats[f"{engine}_miss"] += 1
                continue

            content = parsed.get("content", "") or ""
            if len(content) < self.min_content_length:
                self.stats[f"{engine}_too_short"] += 1
                continue

            parsed["extractor"] = engine
            self.stats[f"{engine}_success"] += 1
            return parsed

        self.stats["all_failed"] += 1
        return None

    def stats_dict(self) -> Dict[str, int]:
        return dict(self.stats)

    def _extract_with_trafilatura(self, html: str, final_url: str) -> Optional[Dict[str, str]]:
        if trafilatura is None:
            return None

        extracted = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            output_format="json",
            with_metadata=True,
            url=final_url,
        )
        if not extracted:
            return None

        data = json.loads(extracted)
        content = (data.get("text") or "").strip()
        title = (data.get("title") or "").strip()
        publish_raw = data.get("date") or ""
        publish_dt = parse_datetime_flexible(publish_raw)

        return {
            "title": title,
            "content": content,
            "publish_time": publish_dt.strftime("%Y-%m-%d %H:%M:%S") if publish_dt else "",
            "publish_time_source": "trafilatura:date" if publish_dt else "",
            "url": data.get("url") or final_url,
        }

    @staticmethod
    def _iter_json_nodes(data: Any):
        if isinstance(data, dict):
            yield data
            for value in data.values():
                yield from ContentExtractor._iter_json_nodes(value)
        elif isinstance(data, list):
            for item in data:
                yield from ContentExtractor._iter_json_nodes(item)

    def _extract_publish_time_bs4(self, soup, final_url: str) -> Tuple[str, str]:
        candidates: List[Tuple[str, str]] = []

        publish_meta_keys = {
            "article:published_time",
            "article:modified_time",
            "og:published_time",
            "publishdate",
            "pubdate",
            "date",
            "datepublished",
            "dc.date",
            "dc.date.issued",
            "parsely-pub-date",
            "sailthru.date",
            "weibo:article:create_at",
            "weibo:article:update_at",
        }

        for meta in soup.find_all("meta"):
            for attr in ("property", "name", "itemprop"):
                key = str(meta.get(attr, "")).strip().lower()
                if key in publish_meta_keys:
                    content = str(meta.get("content", "")).strip()
                    if content:
                        candidates.append((f"meta:{key}", content))

        for time_tag in soup.find_all("time"):
            for field in ("datetime", "content"):
                raw = str(time_tag.get(field, "")).strip()
                if raw:
                    candidates.append((f"time:{field}", raw))
            text_value = time_tag.get_text(" ", strip=True)
            if text_value:
                candidates.append(("time:text", text_value))

        for script in soup.find_all("script", type=re.compile(r"ld\+json", re.I)):
            raw_json = script.string or script.get_text()
            if not raw_json:
                continue
            try:
                parsed_json = json.loads(raw_json)
            except Exception:
                continue

            for node in self._iter_json_nodes(parsed_json):
                for key in ("datePublished", "dateCreated", "dateModified", "uploadDate"):
                    value = node.get(key)
                    if isinstance(value, str) and value.strip():
                        candidates.append((f"jsonld:{key}", value.strip()))

        url_patterns = [
            r"(20\d{2}[/-]\d{1,2}[/-]\d{1,2}(?:[ T]\d{1,2}:\d{1,2}(?::\d{1,2})?)?)",
            r"(20\d{2}\d{2}\d{2})",
        ]
        for pattern in url_patterns:
            match = re.search(pattern, final_url)
            if match:
                candidates.append(("url", match.group(1)))

        body_text = soup.get_text(" ", strip=True)
        body_match = re.search(
            r"(20\d{2}[年/-]\d{1,2}[月/-]\d{1,2}(?:[日\sT]+\d{1,2}:\d{1,2}(?::\d{1,2})?)?)",
            body_text[:3000],
        )
        if body_match:
            candidates.append(("body", body_match.group(1)))

        for source_name, raw_value in candidates:
            dt = parse_datetime_flexible(raw_value)
            if dt is not None:
                return dt.strftime("%Y-%m-%d %H:%M:%S"), source_name

        return "", ""

    def _extract_with_bs4(self, html: str, final_url: str) -> Optional[Dict[str, str]]:
        if BeautifulSoup is None:
            return None

        soup = BeautifulSoup(html, "html.parser")
        publish_time, publish_time_source = self._extract_publish_time_bs4(soup, final_url)

        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        if not title and soup.title:
            title = soup.title.get_text(strip=True)

        for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "iframe"]):
            tag.extract()

        content = ""
        wechat_content = soup.find(id="js_content")
        if wechat_content:
            content = wechat_content.get_text("\n", strip=True)

        if not content:
            common_classes = ["article-content", "post-content", "entry-content", "main-content", "content", "article"]
            for cls in common_classes:
                div = soup.find("div", class_=re.compile(cls, re.I))
                if div:
                    content = div.get_text("\n", strip=True)
                    break

        if not content:
            paragraphs = soup.find_all("p")
            valid_paragraphs = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 10]
            content = "\n".join(valid_paragraphs)

        return {
            "url": final_url,
            "title": title,
            "content": content,
            "publish_time": publish_time,
            "publish_time_source": publish_time_source,
        }
