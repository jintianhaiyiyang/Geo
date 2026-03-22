"""Asynchronous crawler implementation (primary path)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

from .dedupe import SiteCache, normalize_url
from .extractors import ContentExtractor
from .http_clients import AsyncHttpClient, HttpClientFacade
from .rate_limiter import AsyncRateLimiter


class AsyncCrawler:
    def __init__(
        self,
        async_http_client: AsyncHttpClient,
        fallback_http_client: HttpClientFacade,
        extractor: ContentExtractor,
        max_concurrency: int = 12,
        per_domain_concurrency: int = 4,
        enable_url_dedupe: bool = True,
        site_cache: Optional[SiteCache] = None,
        rate_limiter: Optional[AsyncRateLimiter] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.async_http_client = async_http_client
        self.fallback_http_client = fallback_http_client
        self.extractor = extractor
        self.max_concurrency = max_concurrency
        self.per_domain_concurrency = per_domain_concurrency
        self.enable_url_dedupe = enable_url_dedupe
        self.site_cache = site_cache
        self.rate_limiter = rate_limiter
        self.logger = logger or logging.getLogger("geo_analyzer.crawler_async")
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        }

        self._global_semaphore = asyncio.Semaphore(max_concurrency)
        self._domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._domain_lock = asyncio.Lock()
        self._seen_urls = set()
        self._seen_lock = asyncio.Lock()

        self.crawl_stats = {
            "attempted": 0,
            "success": 0,
            "failed": 0,
            "degraded_retry": 0,
            "url_dedup_dropped": 0,
            "site_cache_hits": 0,
        }

    async def _get_domain_semaphore(self, url: str) -> asyncio.Semaphore:
        hostname = urlparse(url).hostname or ""
        async with self._domain_lock:
            sem = self._domain_semaphores.get(hostname)
            if sem is None:
                sem = asyncio.Semaphore(self.per_domain_concurrency)
                self._domain_semaphores[hostname] = sem
            return sem

    async def _degraded_sync_fetch(self, url: str, source: str, query: str, normalized_url: str) -> Optional[Dict[str, str]]:
        self.crawl_stats["degraded_retry"] += 1
        if self.rate_limiter is not None:
            await self.rate_limiter.acquire(url)
        try:
            resp = await asyncio.to_thread(self.fallback_http_client.get, url, self.headers)
            if resp.status_code != 200:
                return None

            parsed = self.extractor.extract(resp.text, resp.url)
            if not parsed:
                return None

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
            if self.site_cache is not None:
                self.site_cache.set(normalized_url or url, data)
            return data
        except Exception:
            return None

    async def _fetch_one(self, idx: int, total: int, url_item: Union[str, Dict]) -> Optional[Dict[str, str]]:
        source = "unknown"
        query = ""
        url = url_item

        if isinstance(url_item, dict):
            url = url_item.get("url")
            source = url_item.get("source", "unknown")
            query = url_item.get("query", "")

        if not url:
            return None

        normalized_url = normalize_url(url)
        if self.enable_url_dedupe and normalized_url:
            async with self._seen_lock:
                if normalized_url in self._seen_urls:
                    self.crawl_stats["url_dedup_dropped"] += 1
                    return None
                self._seen_urls.add(normalized_url)

        if self.site_cache is not None:
            cached = self.site_cache.get(normalized_url or url)
            if cached:
                self.crawl_stats["site_cache_hits"] += 1
                return cached

        domain_sem = await self._get_domain_semaphore(url)
        async with self._global_semaphore, domain_sem:
            self.crawl_stats["attempted"] += 1
            self.logger.info("[%d/%d] 异步抓取中 (%s): %s", idx, total, source, url)

            try:
                if self.rate_limiter is not None:
                    await self.rate_limiter.acquire(url)
                resp = await self.async_http_client.get(url, headers=self.headers)
                if resp.status_code == 200:
                    parsed = self.extractor.extract(resp.text, resp.url)
                    if parsed:
                        self.crawl_stats["success"] += 1
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
                        if self.site_cache is not None:
                            self.site_cache.set(normalized_url or url, data)
                        return data

                if resp.status_code == 403:
                    degraded = await self._degraded_sync_fetch(url, source, query, normalized_url)
                    if degraded:
                        self.crawl_stats["success"] += 1
                        return degraded

                self.crawl_stats["failed"] += 1
                return None
            except Exception:
                degraded = await self._degraded_sync_fetch(url, source, query, normalized_url)
                if degraded:
                    self.crawl_stats["success"] += 1
                    return degraded
                self.crawl_stats["failed"] += 1
                return None

    async def crawl_async(self, urls: List[Union[str, Dict]]) -> List[Dict[str, str]]:
        total = len(urls)
        self.logger.info(
            "开始异步抓取 %d 个页面（max_concurrency=%d, per_domain=%d）...",
            total,
            self.max_concurrency,
            self.per_domain_concurrency,
        )

        async with self.async_http_client:
            tasks = [self._fetch_one(i, total, item) for i, item in enumerate(urls, 1)]
            results = await asyncio.gather(*tasks)

        return [item for item in results if item]

    def crawl(self, urls: List[Union[str, Dict]]) -> List[Dict[str, str]]:
        return asyncio.run(self.crawl_async(urls))

    def metrics(self) -> Dict[str, Dict]:
        backend_usage = {"httpx": self.crawl_stats.get("success", 0)}
        fallback_usage = self.fallback_http_client.backend_usage()
        for key, value in fallback_usage.items():
            backend_usage[key] = backend_usage.get(key, 0) + value

        return {
            "crawl_stats": dict(self.crawl_stats),
            "extractor_stats": self.extractor.stats_dict(),
            "backend_usage": backend_usage,
            "rate_limit_stats": self.rate_limiter.stats() if self.rate_limiter is not None else {},
        }
