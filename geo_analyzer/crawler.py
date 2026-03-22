"""Synchronous crawler implementation (fallback path)."""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime
from typing import Dict, List, Optional, Union

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

from .extractors import ContentExtractor
from .dedupe import SiteCache, normalize_url
from .http_clients import HttpClientFacade


class SyncCrawler:
    def __init__(
        self,
        http_client: HttpClientFacade,
        extractor: ContentExtractor,
        request_delay: float = 1.0,
        enable_url_dedupe: bool = True,
        site_cache: Optional[SiteCache] = None,
        logger: Optional[logging.Logger] = None,
        show_progress: bool = True,
    ):
        self.http_client = http_client
        self.extractor = extractor
        self.request_delay = request_delay
        self.enable_url_dedupe = enable_url_dedupe
        self.site_cache = site_cache
        self.logger = logger or logging.getLogger("geo_analyzer.crawler")
        self.show_progress = show_progress
        self._tqdm_notice_emitted = False
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }
        self.crawl_stats = {
            "attempted": 0,
            "success": 0,
            "failed": 0,
            "url_dedup_dropped": 0,
            "site_cache_hits": 0,
        }

    def _iter_with_progress(self, urls: List[Union[str, Dict]]):
        iterator = enumerate(urls, 1)
        if self.show_progress:
            if tqdm is not None:
                return tqdm(iterator, total=len(urls), desc="抓取进度")
            if not self._tqdm_notice_emitted:
                self.logger.info("tqdm 未安装，已回退到普通抓取进度。")
                self._tqdm_notice_emitted = True
        return iterator

    def _sleep_between_requests(self) -> None:
        if self.request_delay <= 0:
            return
        lower = max(0.0, self.request_delay * 0.5)
        upper = max(lower, self.request_delay * 1.2)
        time.sleep(random.uniform(lower, upper))

    def crawl(self, urls: List[Union[str, Dict]]) -> List[Dict[str, str]]:
        articles: List[Dict[str, str]] = []
        total = len(urls)
        seen_urls = set()
        self.logger.info("开始同步抓取 %d 个页面内容...", total)

        for i, url_item in self._iter_with_progress(urls):
            source = "unknown"
            query = ""
            url = url_item

            if isinstance(url_item, dict):
                url = url_item.get("url")
                source = url_item.get("source", "unknown")
                query = url_item.get("query", "")

            if not url:
                continue

            normalized_url = normalize_url(url)
            if self.enable_url_dedupe and normalized_url:
                if normalized_url in seen_urls:
                    self.crawl_stats["url_dedup_dropped"] += 1
                    continue
                seen_urls.add(normalized_url)

            if self.site_cache is not None:
                cached = self.site_cache.get(normalized_url or url)
                if cached:
                    self.crawl_stats["site_cache_hits"] += 1
                    articles.append(cached)
                    continue

            self.crawl_stats["attempted"] += 1
            self.logger.info("[%d/%d] 抓取中 (%s): %s", i, total, source, url)
            try:
                resp = self.http_client.get(url, headers=self.headers)
                if resp.status_code != 200:
                    self.crawl_stats["failed"] += 1
                    self.logger.warning("[%d/%d] HTTP状态异常=%s", i, total, resp.status_code)
                    continue

                parsed = self.extractor.extract(resp.text, resp.url)
                if not parsed:
                    self.crawl_stats["failed"] += 1
                    self.logger.warning("[%d/%d] 内容提取失败。", i, total)
                    continue

                data = {
                    "url": parsed.get("url", resp.url),
                    "normalized_url": normalized_url,
                    "title": parsed.get("title", ""),
                    "content": parsed.get("content", ""),
                    "source": source,
                    "search_query": query,
                    "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "publish_time": parsed.get("publish_time", ""),
                    "publish_time_source": parsed.get("publish_time_source", ""),
                    "extractor": parsed.get("extractor", "unknown"),
                    "http_backend": resp.backend,
                }
                articles.append(data)
                if self.site_cache is not None:
                    self.site_cache.set(normalized_url or url, data)
                self.crawl_stats["success"] += 1
                self.logger.info("[%d/%d] 抓取成功: %s", i, total, data.get("title", "")[:30])
            except Exception as exc:
                self.crawl_stats["failed"] += 1
                self.logger.warning("[%d/%d] 抓取异常: %s", i, total, exc)

            self._sleep_between_requests()

        return articles

    def metrics(self) -> Dict[str, Dict]:
        return {
            "crawl_stats": dict(self.crawl_stats),
            "extractor_stats": self.extractor.stats_dict(),
            "backend_usage": self.http_client.backend_usage(),
        }
