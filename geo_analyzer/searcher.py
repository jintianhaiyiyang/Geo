"""Web multi-source searcher."""

from __future__ import annotations

import html
import json
import logging
import random
import re
import time
from collections import Counter
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, parse_qsl, quote_plus, unquote, urlencode, urljoin, urlsplit, urlunsplit

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover
    BeautifulSoup = None

from .dedupe import normalize_url
from .http_clients import HttpClientFacade

EXCLUDED_DOMAINS = {
    "zhihu.com",
    "www.zhihu.com",
    "zhuanlan.zhihu.com",
}

WECHAT_HOST = "mp.weixin.qq.com"
SOGOU_WEIXIN_HOST = "weixin.sogou.com"
WECHAT_REQUIRED_QUERY_KEYS = ("__biz", "mid", "idx")
WECHAT_OPTIONAL_QUERY_KEYS = ("sn", "chksm")
WECHAT_URL_REGEX = re.compile(r"https?://mp\.weixin\.qq\.com/s\?[^\s\"'<>]+", re.IGNORECASE)
ESCAPED_SLASHES_REGEX = re.compile(r"\\/")


class WebSearcher:
    """Multi-source search: SerpApi + Bing + Baidu + Sogou WeChat."""

    def __init__(
        self,
        http_client: HttpClientFacade,
        timeout: int = 15,
        request_delay: float = 1.0,
        enable_url_dedupe: bool = True,
        serpapi_enabled: bool = False,
        serpapi_api_key: str = "",
        serpapi_engine: str = "google",
        serpapi_gl: str = "cn",
        serpapi_hl: str = "zh-cn",
        enabled_providers: Optional[List[str]] = None,
        provider_experimental: Optional[Dict[str, bool]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.http_client = http_client
        self.timeout = timeout
        self.request_delay = request_delay
        self.enable_url_dedupe = enable_url_dedupe
        self.serpapi_enabled = bool(serpapi_enabled)
        self.serpapi_api_key = (serpapi_api_key or "").strip()
        self.serpapi_engine = (serpapi_engine or "google").strip() or "google"
        self.serpapi_gl = (serpapi_gl or "").strip()
        self.serpapi_hl = (serpapi_hl or "").strip()
        self.enabled_providers = self._normalize_provider_list(
            enabled_providers or ["bing", "baidu", "wechat", "serpapi"]
        )
        self.provider_experimental = {
            "google": False,
            "xiaohongshu": False,
            "bilibili": False,
            "douyin": False,
        }
        if provider_experimental:
            for key, value in provider_experimental.items():
                self.provider_experimental[str(key).strip().lower()] = bool(value)
        self.logger = logger or logging.getLogger("geo_analyzer.search")
        self._warmed_hosts = set()
        self._last_provider_stats: Dict[str, Dict[str, Any]] = {}
        self.user_agents = [
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
                "Gecko/20100101 Firefox/124.0"
            ),
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/17.4 Safari/605.1.15"
            ),
        ]
        self.base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Connection": "keep-alive",
        }

    @staticmethod
    def _normalize_provider_list(providers: List[str]) -> List[str]:
        deduped: List[str] = []
        seen = set()
        for item in providers:
            provider = str(item or "").strip().lower()
            if not provider or provider in seen:
                continue
            seen.add(provider)
            deduped.append(provider)
        return deduped

    def provider_stats(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._last_provider_stats)

    @staticmethod
    def _keyword_with_time_hint(keyword: str, time_window: Optional[Dict[str, Any]]) -> str:
        if not time_window:
            return keyword
        mode = str(time_window.get("mode", "") or "")
        preset = str(time_window.get("preset", "") or "")
        if mode == "preset":
            if preset == "today":
                return f"{keyword} 今天 最新"
            if preset == "week":
                return f"{keyword} 最近一周"
            if preset == "month":
                return f"{keyword} 最近一个月"
        if mode in {"custom", "recent_months"}:
            date_from = str(time_window.get("date_from", "") or "")
            date_to = str(time_window.get("date_to", "") or "")
            if date_from and date_to:
                return f"{keyword} {date_from[:10]} 到 {date_to[:10]}"
        return keyword

    def search(
        self,
        keyword: str,
        limit: int = 15,
        include_weixin: bool = True,
        include_overseas: bool = True,
        time_window: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, str]]:
        if BeautifulSoup is None:
            self.logger.error("Missing bs4, please run: pip install beautifulsoup4")
            return []

        self.logger.info("Searching keyword [%s] from providers=%s ...", keyword, self.enabled_providers)
        results: List[Dict[str, str]] = []
        seen_urls = set()
        provider_stats: Dict[str, Dict[str, Any]] = {}
        effective_keyword = self._keyword_with_time_hint(keyword, time_window)

        def run_provider(name: str, callback) -> None:
            if name not in self.enabled_providers:
                provider_stats[name] = {"enabled": False, "count": 0, "status": "disabled"}
                return
            try:
                incoming = callback()
            except Exception as exc:
                provider_stats[name] = {
                    "enabled": True,
                    "count": 0,
                    "status": "error",
                    "error": str(exc),
                }
                self.logger.warning("Provider [%s] failed: %s", name, exc)
                return
            before = len(results)
            self._merge_results(results, seen_urls, incoming)
            provider_stats[name] = {
                "enabled": True,
                "count": max(0, len(results) - before),
                "status": "ok",
            }

        run_provider(
            "serpapi",
            lambda: []
            if (not self.serpapi_enabled or not self.serpapi_api_key)
            else self._search_serpapi(effective_keyword, limit),
        )
        run_provider(
            "bing",
            lambda: self._search_bing(effective_keyword, limit, domain="cn.bing.com", source="cn_bing")
            + self._search_bing(effective_keyword, limit, domain="www.bing.com", source="global_bing")
            + (
                []
                if not include_overseas
                else self._search_bing(
                    self._keyword_with_time_hint(f"{keyword} global", time_window),
                    max(6, limit // 2),
                    domain="www.bing.com",
                    source="global_bing_overseas",
                )
            ),
        )
        run_provider("baidu", lambda: self._search_baidu(effective_keyword, limit))
        run_provider(
            "wechat",
            lambda: []
            if not include_weixin
            else self._search_sogou_weixin(effective_keyword, max(8, limit // 2))
            + self._search_bing_wechat(effective_keyword, max(8, limit // 2)),
        )

        for placeholder in ("google", "xiaohongshu", "bilibili", "douyin"):
            if placeholder in self.enabled_providers:
                enabled = bool(self.provider_experimental.get(placeholder, False))
                if enabled:
                    provider_stats[placeholder] = {
                        "enabled": True,
                        "count": 0,
                        "status": "placeholder",
                        "note": "Provider placeholder in v8.0",
                    }
                    self.logger.warning("Provider [%s] is a v8.0 placeholder and currently returns no data.", placeholder)
                else:
                    provider_stats[placeholder] = {
                        "enabled": False,
                        "count": 0,
                        "status": "experimental_disabled",
                    }

        final_results = self._rebalance_by_source(results, limit)
        source_counter = Counter(item.get("source", "unknown") for item in final_results)
        provider_stats["_summary"] = {"enabled_providers": list(self.enabled_providers), "total_results": len(final_results)}
        self._last_provider_stats = provider_stats
        self.logger.info(
            "Search done, total=%d by_source=%s provider_stats=%s",
            len(final_results),
            dict(source_counter),
            provider_stats,
        )
        return final_results

    def _search_serpapi(self, keyword: str, limit: int) -> List[Dict[str, str]]:
        found_results: List[Dict[str, str]] = []
        try:
            params = {
                "engine": self.serpapi_engine,
                "q": keyword,
                "api_key": self.serpapi_api_key,
                "num": max(1, min(100, int(limit))),
            }
            if self.serpapi_gl:
                params["gl"] = self.serpapi_gl
            if self.serpapi_hl:
                params["hl"] = self.serpapi_hl

            url = f"https://serpapi.com/search.json?{urlencode(params)}"
            headers = self._build_headers(host="serpapi.com", referer="https://serpapi.com/")
            resp = self._safe_get(url, headers=headers)
            if resp.status_code != 200:
                self.logger.warning("SerpApi returned unexpected status: %s", resp.status_code)
                return found_results

            payload = json.loads(resp.text or "{}")
            if isinstance(payload, dict) and payload.get("error"):
                self.logger.warning("SerpApi returned error: %s", payload.get("error"))
                return found_results

            organic_results = payload.get("organic_results", []) if isinstance(payload, dict) else []
            if not isinstance(organic_results, list):
                organic_results = []

            for item in organic_results:
                if not isinstance(item, dict):
                    continue
                link = item.get("link") or item.get("url")
                if isinstance(link, str) and link.startswith("http"):
                    found_results.append({"url": link, "source": "serpapi", "query": keyword})
                    if len(found_results) >= limit:
                        break
        except Exception as exc:
            self.logger.warning("SerpApi query failed: %s", exc)
        finally:
            self._sleep()
        return found_results

    def _build_headers(self, host: Optional[str] = None, referer: Optional[str] = None) -> Dict[str, str]:
        headers = dict(self.base_headers)
        headers["User-Agent"] = random.choice(self.user_agents)
        if host:
            headers["Host"] = host
        if referer:
            headers["Referer"] = referer
        return headers

    def _safe_get(self, url: str, headers: Optional[Dict[str, str]] = None):
        req_headers = headers if headers is not None else self._build_headers()
        return self.http_client.get(url, headers=req_headers)

    def _sleep(self, multiplier: float = 1.0) -> None:
        if self.request_delay > 0:
            base = max(0.0, self.request_delay * multiplier)
            time.sleep(random.uniform(base * 0.8, base * 1.6))

    def _warmup_host(self, host: str, referer: str) -> None:
        if host in self._warmed_hosts:
            return
        try:
            warm_url = f"https://{host}/"
            self._safe_get(warm_url, headers=self._build_headers(host=host, referer=referer))
            self._warmed_hosts.add(host)
        except Exception:
            # Warm-up is best-effort only.
            pass

    def _merge_results(self, target: List[Dict[str, str]], seen: set, incoming: List[Dict[str, str]]) -> None:
        for item in incoming:
            url = item.get("url", "")
            normalized_url = self._normalize_candidate_url(url)
            if not normalized_url:
                continue
            if self._is_excluded_url(normalized_url):
                continue

            dedupe_key = normalize_url(normalized_url) or normalized_url
            if self.enable_url_dedupe and dedupe_key in seen:
                continue
            if self.enable_url_dedupe:
                seen.add(dedupe_key)

            normalized_item = dict(item)
            normalized_item["url"] = normalized_url
            target.append(normalized_item)

    @staticmethod
    def _is_excluded_url(url: str) -> bool:
        if not url:
            return False
        try:
            host = (urlsplit(url).hostname or "").lower()
        except Exception:
            return False
        if not host:
            return False
        return host in EXCLUDED_DOMAINS or any(host.endswith(f".{domain}") for domain in EXCLUDED_DOMAINS)

    @staticmethod
    def _decode_transferred_url(value: str) -> str:
        decoded = html.unescape(html.unescape(str(value or ""))).strip()
        decoded = ESCAPED_SLASHES_REGEX.sub("/", decoded)
        for _ in range(3):
            maybe = unquote(decoded)
            if maybe == decoded:
                break
            decoded = maybe
        return decoded.strip()

    @classmethod
    def _extract_http_urls(cls, text: str) -> List[str]:
        if not text:
            return []
        decoded = cls._decode_transferred_url(text)
        candidates = re.findall(r"https?://[^\s\"'<>]+", decoded)
        if decoded.startswith("mp.weixin.qq.com/s?"):
            candidates.append(f"https://{decoded}")
        return candidates

    @classmethod
    def _normalize_wechat_article_url(cls, url: str) -> str:
        candidate = cls._decode_transferred_url(url)
        if not candidate:
            return ""
        if candidate.startswith("//"):
            candidate = f"https:{candidate}"
        if candidate.startswith("mp.weixin.qq.com/s?"):
            candidate = f"https://{candidate}"

        try:
            parts = urlsplit(candidate)
        except Exception:
            return ""
        host = (parts.hostname or "").lower()
        if host != WECHAT_HOST:
            return ""
        if not parts.path.startswith("/s"):
            return ""

        lower_text = candidate.lower()
        if "video?" in lower_text or "show?" in lower_text:
            return ""

        query_pairs = parse_qsl(parts.query, keep_blank_values=False)
        query_map = {}
        for key, value in query_pairs:
            if key in WECHAT_REQUIRED_QUERY_KEYS or key in WECHAT_OPTIONAL_QUERY_KEYS:
                query_map[key] = value

        if any(not query_map.get(key) for key in WECHAT_REQUIRED_QUERY_KEYS):
            return ""

        ordered_items = []
        for key in (*WECHAT_REQUIRED_QUERY_KEYS, *WECHAT_OPTIONAL_QUERY_KEYS):
            if key in query_map:
                ordered_items.append((key, query_map[key]))

        return urlunsplit(("https", WECHAT_HOST, "/s", urlencode(ordered_items), ""))

    @classmethod
    def _is_valid_wechat_article_url(cls, url: str) -> bool:
        if not url:
            return False
        try:
            parts = urlsplit(url)
        except Exception:
            return False
        if (parts.hostname or "").lower() != WECHAT_HOST:
            return False
        if not parts.path.startswith("/s"):
            return False
        query_map = parse_qs(parts.query)
        for key in WECHAT_REQUIRED_QUERY_KEYS:
            if not query_map.get(key):
                return False
        lower_text = url.lower()
        if "video?" in lower_text or "show?" in lower_text:
            return False
        return True

    def _normalize_candidate_url(self, url: str) -> str:
        if not url:
            return ""
        raw_url = str(url).strip()
        if not raw_url:
            return ""
        if raw_url.startswith("//"):
            raw_url = f"https:{raw_url}"

        try:
            host = (urlsplit(raw_url).hostname or "").lower()
        except Exception:
            return self._decode_transferred_url(raw_url)

        if host == WECHAT_HOST:
            normalized_wechat = self._normalize_wechat_article_url(raw_url)
            if normalized_wechat and self._is_valid_wechat_article_url(normalized_wechat):
                return normalized_wechat
            return ""

        if host.endswith(SOGOU_WEIXIN_HOST):
            resolved, _ = self._resolve_sogou_weixin_link(raw_url, allow_network_lookup=False)
            if resolved:
                return resolved
            return self._decode_transferred_url(raw_url)

        return self._decode_transferred_url(raw_url)

    @staticmethod
    def _rebalance_by_source(results: List[Dict[str, str]], limit: int) -> List[Dict[str, str]]:
        if len(results) <= limit:
            return results

        buckets: Dict[str, List[Dict[str, str]]] = {}
        for item in results:
            source = item.get("source", "unknown")
            buckets.setdefault(source, []).append(item)

        sources = sorted(buckets.keys())
        merged: List[Dict[str, str]] = []
        cursor = 0
        while len(merged) < limit and sources:
            source = sources[cursor % len(sources)]
            bucket = buckets.get(source, [])
            if bucket:
                merged.append(bucket.pop(0))
                if len(merged) >= limit:
                    break
            if not bucket:
                sources = [s for s in sources if buckets.get(s)]
                cursor = 0
                continue
            cursor += 1

        return merged[:limit]

    def _search_bing(self, keyword: str, limit: int, domain: str, source: str) -> List[Dict[str, str]]:
        found_results: List[Dict[str, str]] = []
        encoded_kw = quote_plus(keyword)
        pages = 1 if limit <= 10 else 2

        self._warmup_host(domain, f"https://{domain}/")
        for page in range(pages):
            try:
                offset = page * 10 + 1
                url = f"https://{domain}/search?q={encoded_kw}&first={offset}"
                resp = self._safe_get(url, headers=self._build_headers(host=domain, referer="https://www.bing.com/"))

                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    links = [a.get("href", "") for a in soup.select("li.b_algo h2 a")]
                    if not links:
                        links = [a.get("href", "") for a in soup.select("h2 a")]

                    for link in links:
                        if link.startswith("http") and "microsoft.com" not in link and "bing.com" not in link:
                            found_results.append({"url": link, "source": source, "query": keyword})
                            if len(found_results) >= limit:
                                return found_results
            except Exception as exc:
                self.logger.warning("Bing(%s) search failed: %s", domain, exc)
            finally:
                self._sleep()

        return found_results

    def _search_bing_wechat(self, keyword: str, limit: int) -> List[Dict[str, str]]:
        query = f"site:mp.weixin.qq.com {keyword}"
        raw_results = self._search_bing(query, limit, domain="www.bing.com", source="bing_wechat")
        found_results: List[Dict[str, str]] = []
        seen: Set[str] = set()
        for item in raw_results:
            normalized = self._normalize_wechat_article_url(item.get("url", ""))
            if not self._is_valid_wechat_article_url(normalized):
                continue
            dedupe_key = normalize_url(normalized) or normalized
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            found_results.append({"url": normalized, "source": "bing_wechat", "query": keyword})
            if len(found_results) >= limit:
                break
        return found_results

    def _search_baidu(self, keyword: str, limit: int) -> List[Dict[str, str]]:
        found_results: List[Dict[str, str]] = []
        try:
            encoded_kw = quote_plus(keyword)
            self._warmup_host("www.baidu.com", "https://www.baidu.com/")
            url = f"https://www.baidu.com/s?wd={encoded_kw}"
            headers = self._build_headers(host="www.baidu.com", referer="https://www.baidu.com/")
            resp = self._safe_get(url, headers=headers)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                links = [a.get("href", "") for a in soup.select("h3.t a")]
                if not links:
                    links = [a.get("href", "") for a in soup.select("div.result h3 a")]

                for link in links:
                    if link:
                        found_results.append({"url": link, "source": "baidu", "query": keyword})
                        if len(found_results) >= limit:
                            break
        except Exception as exc:
            self.logger.warning("Baidu search failed: %s", exc)
        finally:
            self._sleep()

        return found_results

    def _extract_wechat_urls_from_html(self, html_text: str) -> List[str]:
        urls: List[str] = []
        if not html_text:
            return urls

        decoded = self._decode_transferred_url(html_text)
        urls.extend(WECHAT_URL_REGEX.findall(decoded))
        if "mp.weixin.qq.com/s?" in decoded:
            urls.extend(
                [f"https://{item}" for item in re.findall(r"mp\.weixin\.qq\.com/s\?[^\s\"'<>]+", decoded)]
            )

        if BeautifulSoup is not None:
            soup = BeautifulSoup(decoded, "html.parser")
            for anchor in soup.select("a[href*='mp.weixin.qq.com']"):
                href = anchor.get("href", "")
                if href:
                    urls.append(href)

        deduped = []
        seen = set()
        for item in urls:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    def _resolve_sogou_weixin_link(self, link: str, allow_network_lookup: bool = True) -> Tuple[str, bool]:
        full_link = link if link.startswith("http") else urljoin(f"https://{SOGOU_WEIXIN_HOST}", link)
        full_link = html.unescape(html.unescape(full_link)).strip()
        used_network_lookup = False

        direct = self._normalize_wechat_article_url(full_link)
        if self._is_valid_wechat_article_url(direct):
            return direct, used_network_lookup

        try:
            parsed = urlsplit(full_link)
        except Exception:
            return "", used_network_lookup
        host = (parsed.hostname or "").lower()

        if host.endswith(SOGOU_WEIXIN_HOST):
            query_map = parse_qs(parsed.query)
            for key in ("url", "u", "k", "target"):
                for raw_value in query_map.get(key, []):
                    for candidate in self._extract_http_urls(raw_value):
                        normalized = self._normalize_wechat_article_url(candidate)
                        if self._is_valid_wechat_article_url(normalized):
                            return normalized, used_network_lookup

        if not allow_network_lookup or not host.endswith(SOGOU_WEIXIN_HOST):
            return "", used_network_lookup

        used_network_lookup = True
        try:
            headers = self._build_headers(host=SOGOU_WEIXIN_HOST, referer="https://weixin.sogou.com/")
            resp = self._safe_get(full_link, headers=headers)
        except Exception:
            return "", used_network_lookup

        candidates = [resp.url]
        candidates.extend(self._extract_wechat_urls_from_html(resp.text))
        for candidate in candidates:
            normalized = self._normalize_wechat_article_url(candidate)
            if self._is_valid_wechat_article_url(normalized):
                return normalized, used_network_lookup

        return "", used_network_lookup

    def _search_sogou_weixin(self, keyword: str, limit: int) -> List[Dict[str, str]]:
        found_results: List[Dict[str, str]] = []
        local_seen: Set[str] = set()
        redirect_lookup_budget = max(3, min(10, limit))
        try:
            encoded_kw = quote_plus(keyword)
            self._warmup_host(SOGOU_WEIXIN_HOST, "https://weixin.sogou.com/")
            url = f"https://{SOGOU_WEIXIN_HOST}/weixin?type=2&query={encoded_kw}"
            headers = self._build_headers(host=SOGOU_WEIXIN_HOST, referer="https://weixin.sogou.com/")
            resp = self._safe_get(url, headers=headers)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                links = [a.get("href", "") for a in soup.select("h3 a")]
                if not links:
                    links = [
                        a.get("href", "")
                        for a in soup.select("a[href*='weixin.sogou.com/link?url='], a[href*='mp.weixin.qq.com']")
                    ]

                for direct_candidate in self._extract_wechat_urls_from_html(resp.text):
                    normalized = self._normalize_wechat_article_url(direct_candidate)
                    if not self._is_valid_wechat_article_url(normalized):
                        continue
                    dedupe_key = normalize_url(normalized) or normalized
                    if dedupe_key in local_seen:
                        continue
                    local_seen.add(dedupe_key)
                    found_results.append({"url": normalized, "source": "weixin_sogou", "query": keyword})
                    if len(found_results) >= limit:
                        return found_results

                for link in links:
                    if not link or "javascript:" in link:
                        continue
                    full_link = link if link.startswith("http") else urljoin(f"https://{SOGOU_WEIXIN_HOST}", link)
                    allow_lookup = redirect_lookup_budget > 0
                    resolved, used_lookup = self._resolve_sogou_weixin_link(full_link, allow_network_lookup=allow_lookup)
                    if used_lookup:
                        redirect_lookup_budget = max(0, redirect_lookup_budget - 1)
                    if not resolved:
                        continue

                    dedupe_key = normalize_url(resolved) or resolved
                    if dedupe_key in local_seen:
                        continue
                    local_seen.add(dedupe_key)
                    found_results.append({"url": resolved, "source": "weixin_sogou", "query": keyword})
                    if len(found_results) >= limit:
                        break
        except Exception as exc:
            self.logger.warning("Sogou WeChat search failed: %s", exc)
        finally:
            self._sleep()

        return found_results
