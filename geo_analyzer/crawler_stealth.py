"""Playwright-based stealth crawler with proxy rotation and adaptive retry."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union
from urllib.parse import unquote, urlparse

from .dedupe import SiteCache, normalize_url
from .extractors import ContentExtractor
from .rate_limiter import AsyncRateLimiter


class RetryableStatusError(RuntimeError):
    """Raised for retryable HTTP status in browser navigation."""

    def __init__(self, status_code: int):
        super().__init__(f"retryable status={status_code}")
        self.status_code = status_code


class StealthCrawler:
    def __init__(
        self,
        extractor: ContentExtractor,
        max_concurrency: int = 3,
        per_domain_concurrency: int = 2,
        max_retries: int = 4,
        backoff_base_seconds: float = 3.0,
        backoff_max_seconds: float = 90.0,
        status_forcelist: Optional[Iterable[int]] = None,
        proxies: Optional[List[str]] = None,
        proxy_ban_ttl_seconds: float = 900.0,
        browser_name: str = "chromium",
        channel: str = "",
        executable_path: str = "",
        headless: bool = True,
        launch_slow_mo_ms: int = 0,
        navigation_timeout_ms: int = 45000,
        network_idle_timeout_ms: int = 10000,
        humanize: bool = True,
        use_stealth_plugin: bool = True,
        locale: str = "zh-CN",
        timezone_id: str = "Asia/Shanghai",
        user_agent: str = "",
        viewport: Optional[Dict[str, int]] = None,
        enable_url_dedupe: bool = True,
        site_cache: Optional[SiteCache] = None,
        rate_limiter: Optional[AsyncRateLimiter] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.extractor = extractor
        self.max_concurrency = max(1, int(max_concurrency))
        self.per_domain_concurrency = max(1, int(per_domain_concurrency))
        self.max_retries = max(0, int(max_retries))
        self.backoff_base_seconds = max(0.0, float(backoff_base_seconds))
        self.backoff_max_seconds = max(self.backoff_base_seconds, float(backoff_max_seconds))
        self.status_forcelist = set(status_forcelist or [403, 407, 429, 500, 502, 503, 504])
        self.proxy_ban_ttl_seconds = max(30.0, float(proxy_ban_ttl_seconds))
        self.browser_name = browser_name
        self.channel = (channel or "").strip()
        self.executable_path = (executable_path or "").strip()
        self.headless = bool(headless)
        self.launch_slow_mo_ms = max(0, int(launch_slow_mo_ms))
        self.navigation_timeout_ms = max(1000, int(navigation_timeout_ms))
        self.network_idle_timeout_ms = max(1000, int(network_idle_timeout_ms))
        self.humanize = bool(humanize)
        self.use_stealth_plugin = bool(use_stealth_plugin)
        self.locale = locale
        self.timezone_id = timezone_id
        self.user_agent = user_agent
        self.viewport = viewport or {"width": 1920, "height": 1080}
        self.enable_url_dedupe = enable_url_dedupe
        self.site_cache = site_cache
        self.rate_limiter = rate_limiter
        self.logger = logger or logging.getLogger("geo_analyzer.crawler_stealth")

        self._global_semaphore = asyncio.Semaphore(self.max_concurrency)
        self._domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._domain_lock = asyncio.Lock()
        self._seen_urls = set()
        self._seen_lock = asyncio.Lock()

        self._proxy_cycle = deque(self._normalize_proxies(proxies or []))
        self._proxy_bad_until: Dict[str, float] = {}
        self._playwright: Any = None
        self._stealth_unavailable_reported = False

        self.crawl_stats = {
            "attempted": 0,
            "success": 0,
            "failed": 0,
            "retry_count": 0,
            "url_dedup_dropped": 0,
            "site_cache_hits": 0,
            "proxy_pool_size": len(self._proxy_cycle),
            "proxy_ban_events": 0,
        }

    @staticmethod
    def _normalize_proxies(proxies: List[str]) -> List[str]:
        normalized: List[str] = []
        seen = set()
        for raw in proxies:
            if not isinstance(raw, str):
                continue
            candidate = raw.strip()
            if not candidate:
                continue
            if "://" not in candidate:
                candidate = f"http://{candidate}"
            parsed = urlparse(candidate)
            if not parsed.scheme or not parsed.hostname or not parsed.port:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)
        return normalized

    @staticmethod
    def _proxy_to_playwright(proxy_url: Optional[str]) -> Optional[Dict[str, str]]:
        if not proxy_url:
            return None
        parsed = urlparse(proxy_url)
        if not parsed.hostname or not parsed.port:
            return None
        config: Dict[str, str] = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
        if parsed.username:
            config["username"] = unquote(parsed.username)
        if parsed.password:
            config["password"] = unquote(parsed.password)
        return config

    async def _get_domain_semaphore(self, url: str) -> asyncio.Semaphore:
        hostname = urlparse(url).hostname or ""
        async with self._domain_lock:
            sem = self._domain_semaphores.get(hostname)
            if sem is None:
                sem = asyncio.Semaphore(self.per_domain_concurrency)
                self._domain_semaphores[hostname] = sem
            return sem

    def _next_proxy(self) -> Optional[str]:
        if not self._proxy_cycle:
            return None
        now = time.time()
        for _ in range(len(self._proxy_cycle)):
            proxy = self._proxy_cycle[0]
            self._proxy_cycle.rotate(-1)
            if self._proxy_bad_until.get(proxy, 0.0) <= now:
                return proxy
        return None

    def _mark_proxy_bad(self, proxy: Optional[str]) -> None:
        if not proxy:
            return
        self._proxy_bad_until[proxy] = time.time() + self.proxy_ban_ttl_seconds
        self.crawl_stats["proxy_ban_events"] += 1

    def _backoff_seconds(self, attempt: int, status_code: Optional[int] = None) -> float:
        if self.backoff_base_seconds <= 0:
            return 0.0

        multiplier = 1.0
        if status_code == 403:
            multiplier = 1.8
        elif status_code in (407, 429):
            multiplier = 1.6
        elif status_code is not None and status_code >= 500:
            multiplier = 1.2

        base = self.backoff_base_seconds * (2 ** attempt) * multiplier
        jitter = random.uniform(0.0, base * 0.25)
        return min(self.backoff_max_seconds, base + jitter)

    @staticmethod
    def _looks_like_antibot_page(html: str) -> bool:
        marker = html.lower()
        return any(
            token in marker
            for token in (
                "cloudflare",
                "captcha",
                "just a moment",
                "attention required",
                "access denied",
                "too many requests",
            )
        )

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        message = str(exc).lower()
        keywords = (
            "timeout",
            "timed out",
            "connection reset",
            "net::err",
            "target page, context or browser has been closed",
            "proxy",
            "cloudflare",
        )
        return any(key in message for key in keywords)

    async def _simulate_human_behavior(self, page: Any) -> None:
        await asyncio.sleep(random.uniform(1.0, 2.8))
        width = int(self.viewport.get("width", 1280))
        height = int(self.viewport.get("height", 720))
        for _ in range(random.randint(3, 8)):
            x = random.randint(30, max(60, width - 30))
            y = random.randint(30, max(60, height - 30))
            steps = random.randint(8, 25)
            await page.mouse.move(x, y, steps=steps)
            await asyncio.sleep(random.uniform(0.05, 0.35))

        await page.evaluate(
            """
            () => {
                const by = window.innerHeight * (0.35 + Math.random() * 0.45);
                window.scrollBy({top: by, left: 0, behavior: 'smooth'});
            }
            """
        )
        await asyncio.sleep(random.uniform(1.2, 3.6))

        if random.random() < 0.25:
            try:
                await page.locator("body").click(
                    position={"x": random.randint(20, 240), "y": random.randint(20, 220)},
                    timeout=1200,
                )
            except Exception:
                pass

    async def _apply_stealth_if_enabled(self, page: Any, context: Any) -> None:
        if not self.use_stealth_plugin:
            return
        try:
            from playwright_stealth import Stealth  # type: ignore

            stealth = Stealth()
            for target in (page, context):
                try:
                    await stealth.apply_stealth_async(target)
                    return
                except Exception:
                    continue
        except Exception:
            pass

        try:
            from playwright_stealth import stealth_async  # type: ignore
        except Exception as exc:
            if not self._stealth_unavailable_reported:
                self.logger.warning("playwright-stealth unavailable, continuing without stealth plugin: %s", exc)
                self._stealth_unavailable_reported = True
            return

        for target in (page, context):
            try:
                await stealth_async(target)
                return
            except TypeError:
                continue
            except Exception:
                continue
        self.logger.debug("stealth_async did not apply successfully, continuing with standard Playwright flow.")

    async def _fetch_with_browser(
        self,
        url: str,
        source: str,
        query: str,
        normalized_url: str,
        proxy_url: Optional[str],
    ) -> Optional[Dict[str, str]]:
        from playwright.async_api import Error as PlaywrightError
        from playwright.async_api import TimeoutError as PlaywrightTimeout

        if self._playwright is None:
            raise RuntimeError("Playwright runtime is not initialized")

        browser_type = getattr(self._playwright, self.browser_name, None)
        if browser_type is None:
            raise RuntimeError(f"unsupported browser type: {self.browser_name}")

        launch_kwargs: Dict[str, Any] = {"headless": self.headless}
        if self.channel:
            launch_kwargs["channel"] = self.channel
        if self.executable_path:
            launch_kwargs["executable_path"] = self.executable_path
        if self.launch_slow_mo_ms > 0:
            launch_kwargs["slow_mo"] = self.launch_slow_mo_ms
        proxy_config = self._proxy_to_playwright(proxy_url)
        if proxy_config:
            launch_kwargs["proxy"] = proxy_config

        browser = await browser_type.launch(**launch_kwargs)
        context = None
        try:
            context_kwargs: Dict[str, Any] = {"viewport": dict(self.viewport)}
            if self.locale:
                context_kwargs["locale"] = self.locale
            if self.timezone_id:
                context_kwargs["timezone_id"] = self.timezone_id
            if self.user_agent:
                context_kwargs["user_agent"] = self.user_agent
            context = await browser.new_context(**context_kwargs)
            page = await context.new_page()

            await self._apply_stealth_if_enabled(page, context)
            response = await page.goto(url, wait_until="domcontentloaded", timeout=self.navigation_timeout_ms)
            status_code = response.status if response is not None else 0
            if status_code in self.status_forcelist:
                raise RetryableStatusError(status_code)

            try:
                await page.wait_for_load_state("networkidle", timeout=self.network_idle_timeout_ms)
            except PlaywrightTimeout:
                pass

            if self.humanize:
                await self._simulate_human_behavior(page)

            html = await page.content()
            resolved_url = page.url
            if self._looks_like_antibot_page(html):
                raise RetryableStatusError(429)

            parsed = self.extractor.extract(html, resolved_url)
            if not parsed:
                return None

            data = {
                "url": parsed.get("url", resolved_url),
                "normalized_url": normalized_url,
                "title": parsed.get("title", ""),
                "content": parsed.get("content", ""),
                "source": source,
                "search_query": query,
                "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "publish_time": parsed.get("publish_time", ""),
                "publish_time_source": parsed.get("publish_time_source", ""),
                "extractor": parsed.get("extractor", "unknown"),
                "http_backend": "playwright_stealth",
            }
            return data
        except RetryableStatusError:
            raise
        except (PlaywrightTimeout, PlaywrightError) as exc:
            raise RuntimeError(str(exc)) from exc
        finally:
            if context is not None:
                await context.close()
            await browser.close()

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
            self.logger.info("[%d/%d] Stealth crawling (%s): %s", idx, total, source, url)
            for attempt in range(self.max_retries + 1):
                proxy = self._next_proxy()
                try:
                    if self.rate_limiter is not None:
                        await self.rate_limiter.acquire(url)
                    data = await self._fetch_with_browser(url, source, query, normalized_url, proxy)
                    if data:
                        self.crawl_stats["success"] += 1
                        if self.site_cache is not None:
                            self.site_cache.set(normalized_url or url, data)
                        return data
                    break
                except RetryableStatusError as exc:
                    self._mark_proxy_bad(proxy)
                    if attempt >= self.max_retries:
                        break
                    self.crawl_stats["retry_count"] += 1
                    wait_seconds = self._backoff_seconds(attempt, exc.status_code)
                    self.logger.warning(
                        "[%d/%d] Retryable status=%s attempt=%d/%d wait=%.1fs",
                        idx,
                        total,
                        exc.status_code,
                        attempt + 1,
                        self.max_retries + 1,
                        wait_seconds,
                    )
                    await asyncio.sleep(wait_seconds)
                except Exception as exc:
                    retryable = self._is_retryable_error(exc)
                    if retryable:
                        self._mark_proxy_bad(proxy)
                    if retryable and attempt < self.max_retries:
                        self.crawl_stats["retry_count"] += 1
                        wait_seconds = self._backoff_seconds(attempt)
                        self.logger.warning(
                            "[%d/%d] Retrying after exception attempt=%d/%d wait=%.1fs err=%s",
                            idx,
                            total,
                            attempt + 1,
                            self.max_retries + 1,
                            wait_seconds,
                            exc,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue
                    self.logger.warning("[%d/%d] Stealth crawl failed: %s", idx, total, exc)
                    break

            self.crawl_stats["failed"] += 1
            return None

    async def crawl_async(self, urls: List[Union[str, Dict]]) -> List[Dict[str, str]]:
        try:
            from playwright.async_api import async_playwright
        except Exception as exc:
            raise RuntimeError(
                "Playwright is required for stealth mode. Install with: "
                "pip install playwright playwright-stealth. "
                "If you use bundled Chromium, run: playwright install chromium"
            ) from exc

        total = len(urls)
        self.logger.info(
            "Starting stealth crawl for %d pages (max_concurrency=%d, per_domain=%d, proxies=%d)...",
            total,
            self.max_concurrency,
            self.per_domain_concurrency,
            len(self._proxy_cycle),
        )

        async with async_playwright() as playwright_runtime:
            self._playwright = playwright_runtime
            try:
                tasks = [self._fetch_one(i, total, item) for i, item in enumerate(urls, 1)]
                results = await asyncio.gather(*tasks)
            finally:
                self._playwright = None

        return [item for item in results if item]

    def crawl(self, urls: List[Union[str, Dict]]) -> List[Dict[str, str]]:
        return asyncio.run(self.crawl_async(urls))

    def metrics(self) -> Dict[str, Dict]:
        active_bad = sum(1 for ts in self._proxy_bad_until.values() if ts > time.time())
        proxy_stats = {
            "pool_size": len(self._proxy_cycle),
            "active_bad_proxy_count": active_bad,
            "proxy_ban_ttl_seconds": self.proxy_ban_ttl_seconds,
        }
        return {
            "crawl_stats": dict(self.crawl_stats),
            "extractor_stats": self.extractor.stats_dict(),
            "backend_usage": {"playwright_stealth": self.crawl_stats.get("success", 0)},
            "rate_limit_stats": self.rate_limiter.stats() if self.rate_limiter is not None else {},
            "proxy_stats": proxy_stats,
        }
