"""HTTP client adapters and retry/fallback strategy."""

from __future__ import annotations

import logging
import random
import time
from collections import Counter
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from typing import Dict, Iterable, Optional, Union

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

try:
    from curl_cffi import requests as cffi_requests
except ImportError:  # pragma: no cover
    cffi_requests = None

try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None


@dataclass
class HttpResponse:
    status_code: int
    text: str
    url: str
    backend: str
    headers: Dict[str, str]


def _parse_retry_after_seconds(headers: Optional[Dict[str, str]]) -> float:
    if not headers:
        return 0.0
    raw_value = str(headers.get("retry-after", "")).strip()
    if not raw_value:
        return 0.0

    if raw_value.isdigit():
        return max(0.0, float(raw_value))

    try:
        retry_dt = parsedate_to_datetime(raw_value)
        return max(0.0, retry_dt.timestamp() - time.time())
    except Exception:
        return 0.0


class HttpClientFacade:
    """Synchronous HTTP client with backend fallback and retry."""

    def __init__(
        self,
        backend: str = "auto",
        timeout: int = 15,
        verify: Union[bool, str] = True,
        retry_max_retries: int = 3,
        retry_backoff_factor: float = 0.8,
        status_forcelist: Optional[Iterable[int]] = None,
        retry_respect_retry_after: bool = True,
        host_cooldown_base_seconds: float = 5.0,
        host_cooldown_max_seconds: float = 180.0,
        host_forcelist_threshold: int = 3,
        logger: Optional[logging.Logger] = None,
    ):
        self.backend = backend
        self.timeout = timeout
        self.verify = verify
        self.retry_max_retries = retry_max_retries
        self.retry_backoff_factor = retry_backoff_factor
        self.status_forcelist = set(status_forcelist or [403, 429, 500, 502, 503, 504])
        self.retry_respect_retry_after = retry_respect_retry_after
        self.host_cooldown_base_seconds = max(0.0, float(host_cooldown_base_seconds))
        self.host_cooldown_max_seconds = max(self.host_cooldown_base_seconds, float(host_cooldown_max_seconds))
        self.host_forcelist_threshold = max(1, int(host_forcelist_threshold))
        self.logger = logger or logging.getLogger("geo_analyzer.http")

        self._requests_session = requests.Session() if requests is not None else None
        self.backend_counter: Counter = Counter()
        self.url_backend: Dict[str, str] = {}
        self._host_block_until: Dict[str, float] = {}
        self._host_consecutive_forcelist: Dict[str, int] = {}

        if self.backend in ("auto", "curl_cffi") and cffi_requests is None:
            self.logger.warning("curl_cffi 未安装或不可用，HTTP 请求将回退到 requests。")

    def _candidate_backends(self) -> list[str]:
        if self.backend == "curl_cffi":
            return ["curl_cffi", "requests"]
        if self.backend == "requests":
            return ["requests"]

        candidates = []
        if cffi_requests is not None:
            candidates.append("curl_cffi")
        candidates.append("requests")
        return candidates

    def _request_with_backend(self, backend: str, url: str, headers: Dict[str, str]) -> HttpResponse:
        if backend == "curl_cffi":
            if cffi_requests is None:
                raise RuntimeError("curl_cffi 不可用")
            impersonate_candidates = ["chrome124", "chrome120", "safari17_0", "edge120"]
            resp = cffi_requests.get(
                url,
                headers=headers,
                timeout=self.timeout,
                verify=self.verify,
                impersonate=random.choice(impersonate_candidates),
                allow_redirects=True,
            )
            return HttpResponse(
                status_code=resp.status_code,
                text=resp.text,
                url=str(resp.url),
                backend="curl_cffi",
                headers={k.lower(): v for k, v in resp.headers.items()},
            )

        if backend == "requests":
            if self._requests_session is None:
                raise RuntimeError("requests 不可用")
            resp = self._requests_session.get(url, headers=headers, timeout=self.timeout, verify=self.verify, allow_redirects=True)
            return HttpResponse(
                status_code=resp.status_code,
                text=resp.text,
                url=resp.url,
                backend="requests",
                headers={k.lower(): v for k, v in resp.headers.items()},
            )

        raise ValueError(f"未知 backend: {backend}")

    def _sleep_backoff(self, attempt: int, status_code: Optional[int] = None, headers: Optional[Dict[str, str]] = None) -> None:
        if self.retry_backoff_factor <= 0:
            return
        multiplier = 1.0
        if status_code == 403:
            multiplier = 1.8
        elif status_code == 429:
            multiplier = 1.6
        elif status_code is not None and status_code >= 500:
            multiplier = 1.2

        base = self.retry_backoff_factor * (2 ** attempt) * multiplier
        if self.retry_respect_retry_after:
            base = max(base, _parse_retry_after_seconds(headers))
        jitter = random.uniform(0.0, base * 0.2)
        time.sleep(base + jitter)

    @staticmethod
    def _host_key(url: str) -> str:
        return (urlparse(url).netloc or "").lower()

    def _wait_host_cooldown(self, url: str) -> None:
        host = self._host_key(url)
        if not host:
            return
        wait_until = self._host_block_until.get(host, 0.0)
        now = time.time()
        if wait_until > now:
            time.sleep(wait_until - now)

    def _mark_host_penalty(
        self,
        url: str,
        status_code: int,
        attempt: int,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        host = self._host_key(url)
        if not host:
            return

        status_multiplier = 1.0
        if status_code == 403:
            status_multiplier = 2.0
        elif status_code == 429:
            status_multiplier = 1.6
        elif status_code >= 500:
            status_multiplier = 1.2

        penalty = max(1.0, self.host_cooldown_base_seconds) * status_multiplier * (attempt + 1)
        if self.retry_respect_retry_after:
            penalty = max(penalty, _parse_retry_after_seconds(headers))
        penalty += random.uniform(0.3, 1.0)
        penalty = min(penalty, self.host_cooldown_max_seconds)
        self._host_block_until[host] = max(self._host_block_until.get(host, 0.0), time.time() + penalty)

        consecutive = self._host_consecutive_forcelist.get(host, 0) + 1
        self._host_consecutive_forcelist[host] = consecutive
        if consecutive >= self.host_forcelist_threshold:
            extra_wait = min(
                self.host_cooldown_base_seconds * (2 ** (consecutive - self.host_forcelist_threshold + 1)),
                self.host_cooldown_max_seconds,
            )
            self._host_block_until[host] = max(self._host_block_until.get(host, 0.0), time.time() + extra_wait)
            self.logger.warning(
                "主机触发连续 forcelist 熔断: host=%s consecutive=%d cooldown=%.1fs",
                host,
                consecutive,
                extra_wait,
            )

    def _mark_host_success(self, url: str) -> None:
        host = self._host_key(url)
        if not host:
            return
        self._host_consecutive_forcelist[host] = 0

    def get(self, url: str, headers: Optional[Dict[str, str]] = None) -> HttpResponse:
        req_headers = headers or {}
        candidates = self._candidate_backends()
        last_error: Optional[Exception] = None

        for attempt in range(self.retry_max_retries + 1):
            self._wait_host_cooldown(url)
            retry_needed = False
            retry_status: Optional[int] = None
            retry_headers: Optional[Dict[str, str]] = None

            for backend in candidates:
                try:
                    resp = self._request_with_backend(backend, url, req_headers)
                except Exception as exc:  # pragma: no cover
                    last_error = exc
                    self.logger.warning("%s 请求失败 [%s] %s", backend, url, exc)
                    continue

                if resp.status_code in self.status_forcelist:
                    self._mark_host_penalty(url, resp.status_code, attempt, resp.headers)
                    self.logger.warning(
                        "HTTP %s 触发重试 [%s] backend=%s attempt=%d/%d",
                        resp.status_code,
                        url,
                        backend,
                        attempt + 1,
                        self.retry_max_retries + 1,
                    )
                    last_error = RuntimeError(f"status={resp.status_code}")
                    retry_needed = True
                    retry_status = resp.status_code
                    retry_headers = resp.headers
                    continue

                self.backend_counter[backend] += 1
                self.url_backend[url] = backend
                self._mark_host_success(url)
                return resp

            if attempt < self.retry_max_retries and (retry_needed or last_error is not None):
                self._sleep_backoff(attempt, retry_status, retry_headers)
                continue

        error_message = f"请求失败: {url}"
        if last_error:
            error_message += f" ({last_error})"
        raise RuntimeError(error_message)

    def backend_usage(self) -> Dict[str, int]:
        return dict(self.backend_counter)


class AsyncHttpClient:
    """Async HTTPX client with retry."""

    def __init__(
        self,
        timeout: int = 15,
        verify: Union[bool, str] = True,
        retry_max_retries: int = 3,
        retry_backoff_factor: float = 0.8,
        status_forcelist: Optional[Iterable[int]] = None,
        retry_respect_retry_after: bool = True,
        host_cooldown_base_seconds: float = 5.0,
        host_cooldown_max_seconds: float = 180.0,
        host_forcelist_threshold: int = 3,
        logger: Optional[logging.Logger] = None,
    ):
        self.timeout = timeout
        self.verify = verify
        self.retry_max_retries = retry_max_retries
        self.retry_backoff_factor = retry_backoff_factor
        self.status_forcelist = set(status_forcelist or [403, 429, 500, 502, 503, 504])
        self.retry_respect_retry_after = retry_respect_retry_after
        self.host_cooldown_base_seconds = max(0.0, float(host_cooldown_base_seconds))
        self.host_cooldown_max_seconds = max(self.host_cooldown_base_seconds, float(host_cooldown_max_seconds))
        self.host_forcelist_threshold = max(1, int(host_forcelist_threshold))
        self.logger = logger or logging.getLogger("geo_analyzer.http_async")
        self.client = None
        self._host_block_until: Dict[str, float] = {}
        self._host_consecutive_forcelist: Dict[str, int] = {}

    async def __aenter__(self):
        if httpx is None:
            raise RuntimeError("缺少 httpx，请运行: pip install httpx")
        self.client = httpx.AsyncClient(timeout=self.timeout, verify=self.verify, follow_redirects=True)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.client is not None:
            await self.client.aclose()
            self.client = None

    @staticmethod
    def _host_key(url: str) -> str:
        return (urlparse(url).netloc or "").lower()

    async def _wait_host_cooldown(self, url: str) -> None:
        import asyncio

        host = self._host_key(url)
        if not host:
            return
        wait_until = self._host_block_until.get(host, 0.0)
        now = time.time()
        if wait_until > now:
            await asyncio.sleep(wait_until - now)

    def _mark_host_penalty(
        self,
        url: str,
        status_code: int,
        attempt: int,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        host = self._host_key(url)
        if not host:
            return
        status_multiplier = 1.0
        if status_code == 403:
            status_multiplier = 2.0
        elif status_code == 429:
            status_multiplier = 1.6
        elif status_code >= 500:
            status_multiplier = 1.2

        penalty = max(1.0, self.host_cooldown_base_seconds) * status_multiplier * (attempt + 1)
        if self.retry_respect_retry_after:
            penalty = max(penalty, _parse_retry_after_seconds(headers))
        penalty += random.uniform(0.2, 0.8)
        penalty = min(penalty, self.host_cooldown_max_seconds)
        self._host_block_until[host] = max(self._host_block_until.get(host, 0.0), time.time() + penalty)

        consecutive = self._host_consecutive_forcelist.get(host, 0) + 1
        self._host_consecutive_forcelist[host] = consecutive
        if consecutive >= self.host_forcelist_threshold:
            extra_wait = min(
                self.host_cooldown_base_seconds * (2 ** (consecutive - self.host_forcelist_threshold + 1)),
                self.host_cooldown_max_seconds,
            )
            self._host_block_until[host] = max(self._host_block_until.get(host, 0.0), time.time() + extra_wait)
            self.logger.warning(
                "async 主机触发连续 forcelist 熔断: host=%s consecutive=%d cooldown=%.1fs",
                host,
                consecutive,
                extra_wait,
            )

    def _mark_host_success(self, url: str) -> None:
        host = self._host_key(url)
        if not host:
            return
        self._host_consecutive_forcelist[host] = 0

    async def _sleep_backoff(
        self,
        attempt: int,
        status_code: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        if self.retry_backoff_factor <= 0:
            return
        import asyncio

        multiplier = 1.0
        if status_code == 403:
            multiplier = 1.8
        elif status_code == 429:
            multiplier = 1.6
        elif status_code is not None and status_code >= 500:
            multiplier = 1.2
        base = self.retry_backoff_factor * (2 ** attempt) * multiplier
        if self.retry_respect_retry_after:
            base = max(base, _parse_retry_after_seconds(headers))
        jitter = random.uniform(0.0, base * 0.2)
        await asyncio.sleep(base + jitter)

    async def get(self, url: str, headers: Optional[Dict[str, str]] = None) -> HttpResponse:
        if self.client is None:
            raise RuntimeError("AsyncHttpClient 未初始化，请使用 async with")

        req_headers = headers or {}
        last_error: Optional[Exception] = None

        for attempt in range(self.retry_max_retries + 1):
            await self._wait_host_cooldown(url)
            try:
                resp = await self.client.get(url, headers=req_headers)
            except Exception as exc:
                last_error = exc
                self.logger.warning("httpx 请求失败 [%s] %s", url, exc)
                if attempt < self.retry_max_retries:
                    await self._sleep_backoff(attempt)
                    continue
                break

            if resp.status_code in self.status_forcelist:
                headers_lower = {k.lower(): v for k, v in resp.headers.items()}
                self._mark_host_penalty(url, resp.status_code, attempt, headers_lower)
                last_error = RuntimeError(f"status={resp.status_code}")
                self.logger.warning(
                    "httpx 状态码触发重试 [%s] status=%s attempt=%d/%d",
                    url,
                    resp.status_code,
                    attempt + 1,
                    self.retry_max_retries + 1,
                )
                if attempt < self.retry_max_retries:
                    await self._sleep_backoff(attempt, resp.status_code, headers_lower)
                    continue

            self._mark_host_success(url)
            return HttpResponse(
                status_code=resp.status_code,
                text=resp.text,
                url=str(resp.url),
                backend="httpx",
                headers={k.lower(): v for k, v in resp.headers.items()},
            )

        error_message = f"async 请求失败: {url}"
        if last_error:
            error_message += f" ({last_error})"
        raise RuntimeError(error_message)
