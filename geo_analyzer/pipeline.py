"""Pipeline orchestration for Geo Keyword Analyzer 8.0.

Optimizations vs original:
- Eliminated ~150-line duplicated crawler-construction block; main search now
  calls _crawl_urls_with_mode() — the same factory quality search already used.
- Extracted _build_http_client / _build_async_http_client / _build_searcher /
  _build_rate_limiter factories to remove repeated 9-param config spreading.
- Split run_pipeline() (~500 lines) into stage helpers:
    _stage_search, _stage_crawl, _apply_filters,
    _stage_quality_search, _run_report_only
- content_hash stamping unified in _dedupe_articles_by_content_hash() so it
  always runs once, regardless of whether dedupe is enabled.
- Removed premature JSON write from GeoKeywordAnalyzer.analyze(); pipeline
  now owns all file I/O and writes a single, complete JSON at the end.
- Quality search reuses the existing HTTP connection pool instead of creating
  a redundant WebSearcher with a fresh client.
- _collect_quality_urls: replaced dict(item) + key assignment with {**item}.
- Added Namespace type annotation on run_pipeline args parameter.
"""

from __future__ import annotations

import json
import logging
import os
import time
from argparse import Namespace
from collections import Counter
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from .analyzer import GeoKeywordAnalyzer
from .attachments import annotate_articles_with_attachments
from .crawler import SyncCrawler
from .crawler_async import AsyncCrawler
from .crawler_stealth import StealthCrawler
from .dedupe import SiteCache, compute_content_hash, normalize_url
from .extractors import ContentExtractor
from .http_clients import AsyncHttpClient, HttpClientFacade
from .rate_limiter import AsyncRateLimiter
from .reports import generate_high_quality_reports, generate_reports
from .searcher import WebSearcher
from .storage import GeoMonitorStorage
from .time_window import filter_articles_by_time_window, resolve_time_window
from .viz import draw_visualizations

# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------

def _merge_counter_dict(*dicts: Dict[str, int]) -> Dict[str, int]:
    counter: Counter = Counter()
    for item in dicts:
        if item:
            counter.update(item)
    return dict(counter)


def _basename_dict(mapping: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: (os.path.basename(value) if isinstance(value, str) and value else value)
        for key, value in mapping.items()
    }


def _prepare_run_output_dir(base_outdir: str) -> str:
    run_label = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = os.path.join(base_outdir, "runs")
    os.makedirs(output_root, exist_ok=True)
    run_outdir = os.path.join(output_root, run_label)
    suffix = 1
    while os.path.exists(run_outdir):
        run_outdir = os.path.join(output_root, f"{run_label}_{suffix}")
        suffix += 1
    os.makedirs(run_outdir, exist_ok=True)
    return run_outdir


def _cleanup_intermediate_files(files: List[str], logger: logging.Logger) -> None:
    for path in files:
        if not path:
            continue
        try:
            if os.path.exists(path):
                os.remove(path)
                logger.info("Removed intermediate file: %s", path)
        except Exception as exc:
            logger.warning("Intermediate cleanup failed [%s]: %s", path, exc)


def _load_stealth_proxies(stealth_cfg: Dict[str, Any], logger: logging.Logger) -> List[str]:
    proxies: List[str] = []
    for item in stealth_cfg.get("proxies", []):
        if isinstance(item, str) and item.strip():
            proxies.append(item.strip())

    proxy_file = stealth_cfg.get("proxy_file", "")
    if isinstance(proxy_file, str) and proxy_file.strip():
        proxy_file_path = os.path.abspath(proxy_file.strip())
        if os.path.exists(proxy_file_path):
            try:
                with open(proxy_file_path, "r", encoding="utf-8") as fh:
                    for raw in fh:
                        line = raw.strip()
                        if line and not line.startswith("#"):
                            proxies.append(line)
            except Exception as exc:
                logger.warning("Failed to read stealth proxy file [%s]: %s", proxy_file_path, exc)
        else:
            logger.warning("Stealth proxy file not found: %s", proxy_file_path)

    seen: set = set()
    deduped: List[str] = []
    for p in proxies:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped


def _normalize_input_articles(payload: Any, logger: logging.Logger) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        payload = payload.get("articles", [])
    if not isinstance(payload, list):
        logger.error("Input JSON must be an array or an object with an `articles` array.")
        return []
    normalized: List[Dict[str, Any]] = []
    dropped = 0
    for item in payload:
        if isinstance(item, dict):
            normalized.append(item)
        else:
            dropped += 1
    if dropped:
        logger.warning("Dropped %d non-object records from input JSON.", dropped)
    return normalized


def _demo_articles() -> List[Dict[str, Any]]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return [
        {
            "title": "GIS 数据共享与机器学习应用",
            "content": "数据共享 数据集 遥感 地理信息 机器学习 深度学习 梯度提升树 支持向量机 大模型 前沿动态",
            "url": "https://example.com/demo-1",
            "publish_time": now,
            "extractor": "demo",
            "http_backend": "demo",
            "source": "demo",
            "search_query": "demo",
        },
        {
            "title": "测绘与预测模拟数据发布",
            "content": "测绘 大数据 预测模拟 数据发布 数据产品 前沿科技 附件 下载地址 https://example.com/data.zip",
            "url": "https://example.com/demo-2",
            "publish_time": now,
            "extractor": "demo",
            "http_backend": "demo",
            "source": "demo",
            "search_query": "demo",
        },
    ]


# ---------------------------------------------------------------------------
# Quality search helpers
# ---------------------------------------------------------------------------

def _build_quality_queries(base_query: str, quality_cfg: Dict[str, Any]) -> List[str]:
    seen_keywords: set = set()
    ordered_keywords: List[str] = []
    for key in ("general_keywords", "topic_keywords"):
        for raw in quality_cfg.get(key, []) or []:
            keyword = str(raw or "").strip()
            if keyword and keyword not in seen_keywords:
                seen_keywords.add(keyword)
                ordered_keywords.append(keyword)

    run_standalone = bool(quality_cfg.get("run_standalone_queries", True))
    run_combined = bool(quality_cfg.get("run_combined_queries", True))
    base = str(base_query or "").strip()

    query_list: List[str] = []
    seen_queries: set = set()
    for keyword in ordered_keywords:
        candidates: List[str] = []
        if run_standalone:
            candidates.append(keyword)
        if run_combined and base:
            candidates.append(f"{base} {keyword}")
        for query in candidates:
            normalized = query.strip()
            if normalized and normalized not in seen_queries:
                seen_queries.add(normalized)
                query_list.append(normalized)
    return query_list


def _build_quality_top_summary(top_articles: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
    return [
        {
            "rank": idx,
            "title": str(article.get("title", "") or ""),
            "url": str(article.get("url", "") or ""),
            "source": str(article.get("source", "") or ""),
            "publish_time": str(article.get("publish_time", "") or ""),
            "search_query": str(article.get("search_query", "") or ""),
        }
        for idx, article in enumerate(top_articles[: max(1, int(limit))], 1)
    ]


def _attach_quality_search_payload(output_data: Dict[str, Any], quality_payload: Dict[str, Any]) -> None:
    """Attach quality_search section into output_data in-place.

    Kept as a named function for backward compatibility (imported by tests).
    """
    output_data["quality_search"] = quality_payload


def _collect_quality_urls(
    *,
    searcher: WebSearcher,
    queries: List[str],
    per_query_limit: int,
    max_total_urls: int,
    include_weixin: bool,
    include_overseas: bool,
    time_window: Optional[Dict[str, Any]],
) -> Tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
    collected: List[Dict[str, str]] = []
    query_stats: List[Dict[str, Any]] = []
    seen_urls: set = set()

    for query in queries:
        if len(collected) >= max_total_urls:
            break
        limit = max(1, int(per_query_limit))
        current = searcher.search(
            query,
            limit=limit,
            include_weixin=include_weixin,
            include_overseas=include_overseas,
            time_window=time_window,
        )
        added = 0
        for item in current:
            url = str(item.get("url", "") or "").strip()
            if not url:
                continue
            dedupe_key = normalize_url(url) or url
            if dedupe_key in seen_urls:
                continue
            seen_urls.add(dedupe_key)
            collected.append({**item, "query": query})
            added += 1
            if len(collected) >= max_total_urls:
                break

        query_stats.append({
            "query": query,
            "requested_limit": limit,
            "returned_urls": len(current),
            "added_urls": added,
        })

    return collected, query_stats


# ---------------------------------------------------------------------------
# HTTP client / searcher / rate-limiter factories
# (eliminate repeated 9-param spreading throughout original code)
# ---------------------------------------------------------------------------

def _build_http_client(
    *,
    config: Dict[str, Any],
    timeout_key: str,
    verify_option: Any,
    logger: logging.Logger,
    name: str = "http",
) -> HttpClientFacade:
    retry_cfg = config["network"]["retry"]
    return HttpClientFacade(
        backend=config["network"]["http_backend"],
        timeout=config["search"][timeout_key],
        verify=verify_option,
        retry_max_retries=retry_cfg["max_retries"],
        retry_backoff_factor=retry_cfg["backoff_factor"],
        status_forcelist=retry_cfg["status_forcelist"],
        retry_respect_retry_after=retry_cfg["respect_retry_after"],
        host_cooldown_base_seconds=retry_cfg["host_cooldown_base_seconds"],
        host_cooldown_max_seconds=retry_cfg["host_cooldown_max_seconds"],
        host_forcelist_threshold=retry_cfg["host_forcelist_threshold"],
        logger=logger.getChild(name),
    )


def _build_async_http_client(
    *,
    config: Dict[str, Any],
    verify_option: Any,
    logger: logging.Logger,
) -> AsyncHttpClient:
    retry_cfg = config["network"]["retry"]
    return AsyncHttpClient(
        timeout=config["search"]["crawl_timeout"],
        verify=verify_option,
        retry_max_retries=retry_cfg["max_retries"],
        retry_backoff_factor=retry_cfg["backoff_factor"],
        status_forcelist=retry_cfg["status_forcelist"],
        retry_respect_retry_after=retry_cfg["respect_retry_after"],
        host_cooldown_base_seconds=retry_cfg["host_cooldown_base_seconds"],
        host_cooldown_max_seconds=retry_cfg["host_cooldown_max_seconds"],
        host_forcelist_threshold=retry_cfg["host_forcelist_threshold"],
        logger=logger.getChild("http.async"),
    )


def _build_searcher(
    *,
    http_client: HttpClientFacade,
    config: Dict[str, Any],
    logger: logging.Logger,
) -> WebSearcher:
    serpapi_cfg = config["search"]["providers"]["serpapi"]
    providers_cfg = config["providers"]
    return WebSearcher(
        http_client=http_client,
        timeout=config["search"]["timeout"],
        request_delay=config["search"]["request_delay"],
        enable_url_dedupe=config["dedupe"]["enable_url_dedupe"],
        serpapi_enabled=serpapi_cfg["enabled"],
        serpapi_api_key=serpapi_cfg["api_key"],
        serpapi_engine=serpapi_cfg["engine"],
        serpapi_gl=serpapi_cfg["gl"],
        serpapi_hl=serpapi_cfg["hl"],
        enabled_providers=providers_cfg["enabled"],
        provider_experimental=providers_cfg["experimental"],
        logger=logger,
    )


def _build_rate_limiter(config: Dict[str, Any]) -> AsyncRateLimiter:
    rate_cfg = config["network"]["rate_limit"]
    return AsyncRateLimiter(
        global_rps=rate_cfg["global_rps"],
        per_domain_rps=rate_cfg["per_domain_rps"],
        jitter_ms_min=rate_cfg["jitter_ms_min"],
        jitter_ms_max=rate_cfg["jitter_ms_max"],
    )


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _dedupe_articles_by_content_hash(
    articles: List[Dict[str, Any]],
    enabled: bool,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Stamp content_hash on every article, then optionally remove duplicates.

    The hash is always computed so downstream code can rely on it existing,
    regardless of whether deduplication is enabled.
    """
    for article in articles:
        article["content_hash"] = compute_content_hash(article.get("content", ""))

    if not enabled:
        return articles, {
            "enabled": False,
            "duplicates_removed": 0,
            "unique_hashes": len({a.get("content_hash") for a in articles if a.get("content_hash")}),
        }

    deduped: List[Dict[str, Any]] = []
    seen_hashes: set = set()
    duplicates = 0
    for article in articles:
        h = article["content_hash"]
        if not h:
            deduped.append(article)
            continue
        if h in seen_hashes:
            duplicates += 1
            continue
        seen_hashes.add(h)
        deduped.append(article)

    logger.info(
        "Content-hash dedupe: before=%d after=%d duplicates=%d",
        len(articles), len(deduped), duplicates,
    )
    return deduped, {"enabled": True, "duplicates_removed": duplicates, "unique_hashes": len(seen_hashes)}


# ---------------------------------------------------------------------------
# Unified crawler factory  (was duplicated verbatim for main search + quality)
# ---------------------------------------------------------------------------

def _crawl_urls_with_mode(
    *,
    urls: List[Dict[str, str]],
    config: Dict[str, Any],
    dedupe_cfg: Dict[str, Any],
    retry_cfg: Dict[str, Any],
    verify_option: Any,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    empty_metrics: Dict[str, Any] = {
        "backend_usage": {},
        "extractor_stats": {},
        "crawl_stats": {},
        "rate_limit_stats": {},
        "proxy_stats": {},
    }
    if not urls:
        return [], empty_metrics

    extractor = ContentExtractor(
        primary=config["extraction"]["primary"],
        fallback=config["extraction"]["fallback"],
        min_content_length=config["extraction"]["min_content_length"],
        logger=logger.getChild("extract"),
    )
    site_cache = (
        SiteCache(max_per_domain=dedupe_cfg["site_cache_max_per_domain"])
        if dedupe_cfg["enable_site_cache"]
        else None
    )
    crawl_mode = config["pipeline"]["crawl_mode"]
    rate_limiter = _build_rate_limiter(config)

    if crawl_mode == "async":
        crawler: Any = AsyncCrawler(
            async_http_client=_build_async_http_client(
                config=config, verify_option=verify_option, logger=logger,
            ),
            fallback_http_client=_build_http_client(
                config=config, timeout_key="crawl_timeout",
                verify_option=verify_option, logger=logger, name="http.fallback",
            ),
            extractor=extractor,
            max_concurrency=config["network"]["max_concurrency"],
            per_domain_concurrency=config["network"]["per_domain_concurrency"],
            enable_url_dedupe=dedupe_cfg["enable_url_dedupe"],
            site_cache=site_cache,
            rate_limiter=rate_limiter,
            logger=logger.getChild("crawler_async"),
        )

    elif crawl_mode == "stealth":
        stealth_cfg = config["network"]["stealth"]
        crawler = StealthCrawler(
            extractor=extractor,
            max_concurrency=stealth_cfg["max_concurrency"],
            per_domain_concurrency=stealth_cfg["per_domain_concurrency"],
            max_retries=stealth_cfg["max_retries"],
            backoff_base_seconds=stealth_cfg["backoff_base_seconds"],
            backoff_max_seconds=stealth_cfg["backoff_max_seconds"],
            status_forcelist=stealth_cfg["status_forcelist"],
            proxies=_load_stealth_proxies(stealth_cfg, logger.getChild("proxy")),
            proxy_ban_ttl_seconds=stealth_cfg["proxy_ban_ttl_seconds"],
            browser_name=stealth_cfg["browser"],
            channel=stealth_cfg["channel"],
            executable_path=stealth_cfg["executable_path"],
            headless=stealth_cfg["headless"],
            launch_slow_mo_ms=stealth_cfg["launch_slow_mo_ms"],
            navigation_timeout_ms=stealth_cfg["navigation_timeout_ms"],
            network_idle_timeout_ms=stealth_cfg["network_idle_timeout_ms"],
            humanize=stealth_cfg["humanize"],
            use_stealth_plugin=stealth_cfg["use_stealth_plugin"],
            locale=stealth_cfg["locale"],
            timezone_id=stealth_cfg["timezone_id"],
            user_agent=stealth_cfg["user_agent"],
            viewport=stealth_cfg["viewport"],
            enable_url_dedupe=dedupe_cfg["enable_url_dedupe"],
            site_cache=site_cache,
            rate_limiter=rate_limiter,
            logger=logger.getChild("crawler_stealth"),
        )

    else:  # sync
        crawler = SyncCrawler(
            http_client=_build_http_client(
                config=config, timeout_key="crawl_timeout",
                verify_option=verify_option, logger=logger, name="http.crawl",
            ),
            extractor=extractor,
            request_delay=config["search"]["request_delay"],
            enable_url_dedupe=dedupe_cfg["enable_url_dedupe"],
            site_cache=site_cache,
            logger=logger.getChild("crawler"),
            show_progress=config["ui"]["progress_bar"],
        )

    return crawler.crawl(urls), crawler.metrics()


# ---------------------------------------------------------------------------
# Pipeline stage helpers
# ---------------------------------------------------------------------------

def _stage_search(
    *,
    args: Namespace,
    config: Dict[str, Any],
    verify_option: Any,
    time_window: Optional[Dict[str, Any]],
    logger: logging.Logger,
) -> Tuple[List[Dict[str, str]], Dict[str, Any], HttpClientFacade]:
    """Search the web and return (url_list, provider_stats, search_http_client)."""
    search_http = _build_http_client(
        config=config, timeout_key="timeout",
        verify_option=verify_option, logger=logger, name="http.search",
    )
    searcher = _build_searcher(
        http_client=search_http, config=config, logger=logger.getChild("search"),
    )
    urls = searcher.search(
        args.search,
        limit=config["search"]["limit"],
        include_weixin=not args.no_weixin,
        include_overseas=not args.no_overseas,
        time_window=time_window,
    )
    return urls, searcher.provider_stats(), search_http


def _stage_crawl(
    *,
    urls: List[Dict[str, str]],
    config: Dict[str, Any],
    verify_option: Any,
    run_outdir: str,
    intermediate_files: List[str],
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Crawl URLs and save raw JSON as an intermediate file."""
    articles, crawler_metrics = _crawl_urls_with_mode(
        urls=urls,
        config=config,
        dedupe_cfg=config["dedupe"],
        retry_cfg=config["network"]["retry"],
        verify_option=verify_option,
        logger=logger,
    )
    if articles:
        raw_file = os.path.join(run_outdir, f"raw_crawl_{datetime.now().strftime('%H%M%S')}.json")
        with open(raw_file, "w", encoding="utf-8") as fh:
            json.dump(articles, fh, ensure_ascii=False, indent=2)
        intermediate_files.append(raw_file)
        logger.info("Raw crawl saved: %s", raw_file)
    return articles, crawler_metrics


def _apply_filters(
    articles: List[Dict[str, Any]],
    *,
    config: Dict[str, Any],
    time_window: Optional[Dict[str, Any]],
    logger: logging.Logger,
) -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any], Dict[str, Any]]:
    """Apply the shared filter chain: time-window → content-hash dedupe → attachment annotation.

    Returns (articles_or_None, content_dedupe_stats, attachment_stats).
    Returns None as the first element when no articles survive filtering.
    """
    dedupe_cfg = config["dedupe"]
    attachment_cfg = config["attachment_detection"]

    articles = filter_articles_by_time_window(
        articles,
        time_window=time_window,
        include_undated=config["search"]["include_undated"],
        logger=logger.getChild("time_window"),
    )
    if not articles:
        return None, {}, {}

    articles, content_dedupe_stats = _dedupe_articles_by_content_hash(
        articles,
        enabled=dedupe_cfg["enable_content_hash_dedupe"],
        logger=logger.getChild("dedupe"),
    )
    if not articles:
        return None, content_dedupe_stats, {}

    attachment_stats = annotate_articles_with_attachments(
        articles,
        enabled=attachment_cfg["enabled"],
        min_score=attachment_cfg["min_score"],
    )
    return articles, content_dedupe_stats, attachment_stats


def _stage_quality_search(
    *,
    args: Namespace,
    config: Dict[str, Any],
    search_http: HttpClientFacade,
    time_window: Optional[Dict[str, Any]],
    run_outdir: str,
    timestamp: str,
    verify_option: Any,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Run the quality-search branch and return its payload dict."""
    quality_cfg = config["quality_search"]
    quality_queries = _build_quality_queries(args.search, quality_cfg)

    # Re-use the existing HTTP connection pool; only build a new WebSearcher wrapper.
    quality_searcher = _build_searcher(
        http_client=search_http,
        config=config,
        logger=logger.getChild("quality.search"),
    )
    quality_urls, quality_query_stats = _collect_quality_urls(
        searcher=quality_searcher,
        queries=quality_queries,
        per_query_limit=quality_cfg["per_query_limit"],
        max_total_urls=quality_cfg["max_total_urls"],
        include_weixin=not args.no_weixin,
        include_overseas=not args.no_overseas,
        time_window=time_window,
    )

    quality_articles: List[Dict[str, Any]] = []
    quality_crawler_metrics: Dict[str, Any] = {}
    quality_after_filters: List[Dict[str, Any]] = []
    quality_content_dedupe_stats: Dict[str, Any] = {}
    quality_attachment_stats: Dict[str, Any] = {}
    quality_report_payload: Optional[Dict[str, Any]] = None

    if quality_urls:
        quality_articles, quality_crawler_metrics = _crawl_urls_with_mode(
            urls=quality_urls,
            config=config,
            dedupe_cfg=config["dedupe"],
            retry_cfg=config["network"]["retry"],
            verify_option=verify_option,
            logger=logger.getChild("quality"),
        )
        filtered, quality_content_dedupe_stats, quality_attachment_stats = _apply_filters(
            quality_articles,
            config=config,
            time_window=time_window,
            logger=logger.getChild("quality"),
        )
        if filtered:
            quality_after_filters = filtered
            quality_report_payload = generate_high_quality_reports(
                outdir=run_outdir,
                timestamp=timestamp,
                selected_articles=quality_after_filters,
            )

    return {
        "enabled": True,
        "executed": True,
        "run_standalone_queries": bool(quality_cfg.get("run_standalone_queries", True)),
        "run_combined_queries": bool(quality_cfg.get("run_combined_queries", True)),
        "general_keywords": list(quality_cfg.get("general_keywords", [])),
        "topic_keywords": list(quality_cfg.get("topic_keywords", [])),
        "queries": quality_queries,
        "query_stats": quality_query_stats,
        "query_count": len(quality_queries),
        "total_urls": len(quality_urls),
        "crawled_articles": len(quality_articles),
        "time_filtered_articles": len(quality_after_filters),
        "content_dedupe": quality_content_dedupe_stats,
        "attachment_stats": quality_attachment_stats,
        "crawler_metrics": quality_crawler_metrics,
        "selected_articles": quality_after_filters,
        "files": _basename_dict(quality_report_payload["files"]) if quality_report_payload else {},
        "top_articles_summary": (
            _build_quality_top_summary(quality_report_payload["top_articles"])
            if quality_report_payload else []
        ),
    }


def _run_report_only(
    *,
    storage: GeoMonitorStorage,
    config: Dict[str, Any],
    run_outdir: str,
    run_id: str,
    total_start: float,
    logger: logging.Logger,
    finalize: Callable,
) -> int:
    source_run_id, payload = storage.fetch_latest_success_result()
    if not payload:
        logger.error("No successful result payload in database.")
        finalize(1, "no successful payload in database")
        return 1

    selected_articles = payload.get("selected_articles", [])
    repeated_terms = payload.get("repeated_terms", [])
    if not isinstance(selected_articles, list) or not selected_articles:
        logger.error("Latest payload has no selected_articles.")
        finalize(1, "payload missing selected_articles")
        return 1
    if not isinstance(repeated_terms, list):
        repeated_terms = []

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    perf: Dict[str, float] = {k: 0.0 for k in (
        "search_seconds", "crawl_seconds", "analysis_seconds",
        "report_seconds", "viz_seconds", "quality_search_seconds", "total_seconds",
    )}

    t0 = time.perf_counter()
    report_payload = generate_reports(
        outdir=run_outdir,
        timestamp=timestamp,
        selected_articles=selected_articles,
        repeated_terms=repeated_terms,
    )

    quality_report_payload: Optional[Dict[str, Any]] = None
    quality_selected_articles: List[Dict[str, Any]] = []
    quality_payload = payload.get("quality_search")
    if isinstance(quality_payload, dict):
        candidate = quality_payload.get("selected_articles", [])
        if isinstance(candidate, list) and candidate:
            quality_selected_articles = candidate
    if quality_selected_articles:
        quality_report_payload = generate_high_quality_reports(
            outdir=run_outdir, timestamp=timestamp, selected_articles=quality_selected_articles,
        )
    perf["report_seconds"] = round(time.perf_counter() - t0, 3)

    run_history = storage.fetch_recent_runs(limit=8)
    t1 = time.perf_counter()
    viz_outputs = draw_visualizations(
        keyword_rows=report_payload["keyword_rows"],
        top_articles=report_payload["top_articles"],
        outdir=run_outdir,
        timestamp=timestamp,
        viz_format=config["visualization"]["format"],
        max_words=config["analysis"]["nebula_max_words"],
        report_files=report_payload["files"],
        summary={
            "total_articles": len(selected_articles),
            "attachment_stats": payload.get("meta", {}).get("attachment_stats", {}),
            "run_history": run_history,
        },
        logger=logger.getChild("viz"),
    )
    perf["viz_seconds"] = round(time.perf_counter() - t1, 3)
    perf["total_seconds"] = round(time.perf_counter() - total_start, 3)

    output_data = dict(payload)
    if quality_report_payload is not None:
        quality_output = dict(quality_payload or {})
        quality_output["files"] = _basename_dict(quality_report_payload["files"])
        quality_output["total_articles"] = len(quality_selected_articles)
        quality_output["top_articles_summary"] = _build_quality_top_summary(
            quality_report_payload["top_articles"]
        )
        _attach_quality_search_payload(output_data, quality_output)

    meta = output_data.setdefault("meta", {})
    meta.update({
        "run_id": run_id,
        "report_only_source_run_id": source_run_id,
        "report_files": _basename_dict(report_payload["files"]),
        "viz_files": _basename_dict(viz_outputs),
        "quality_report_files": (
            _basename_dict(quality_report_payload["files"]) if quality_report_payload else {}
        ),
        "output_dir": run_outdir,
        "performance": perf,
    })

    result_file = os.path.join(run_outdir, f"geo_analysis_result_{timestamp}.json")
    with open(result_file, "w", encoding="utf-8") as fh:
        json.dump(output_data, fh, ensure_ascii=False, indent=2)
    finalize(0, result_data=output_data)
    logger.info("Report-only rebuild finished: %s", run_outdir)
    return 0


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

def run_pipeline(args: Namespace, config: Dict[str, Any], logger: logging.Logger, verify_option: Any) -> int:
    os.makedirs(args.outdir, exist_ok=True)
    run_outdir = _prepare_run_output_dir(os.path.abspath(args.outdir))
    run_id = os.path.basename(run_outdir)
    logger.info("Run output directory: %s", run_outdir)

    dedupe_cfg = config["dedupe"]
    total_start = time.perf_counter()
    perf: Dict[str, float] = {
        "search_seconds": 0.0,
        "crawl_seconds": 0.0,
        "analysis_seconds": 0.0,
        "report_seconds": 0.0,
        "viz_seconds": 0.0,
        "quality_search_seconds": 0.0,
        "total_seconds": 0.0,
    }
    intermediate_files: List[str] = []
    provider_stats: Dict[str, Any] = {}
    attachment_stats: Dict[str, Any] = {}

    try:
        time_window = resolve_time_window(
            preset=config["time_window"]["preset"],
            date_from=config["time_window"]["date_from"],
            date_to=config["time_window"]["date_to"],
            recent_months=config["search"]["recent_months"],
        )
    except Exception as exc:
        logger.error("Invalid time-window config: %s", exc)
        return 1

    # ------------------------------------------------------------------
    # Storage init
    # ------------------------------------------------------------------
    storage: Optional[GeoMonitorStorage] = None
    if config["storage"]["enable_db_write"]:
        try:
            storage = GeoMonitorStorage(config["storage"]["db_path"], logger=logger.getChild("storage"))
            logger.info("SQLite enabled: %s", config["storage"]["db_path"])
        except Exception as exc:
            logger.warning("SQLite init failed, continue without DB write: %s", exc)

    mode = "report_only" if args.report_only else ("scrape_only" if args.scrape_only else "full")

    def finalize(status_code: int, error_message: str = "", result_data: Optional[Dict[str, Any]] = None) -> None:
        if storage is None:
            return
        try:
            storage.finalize_run(
                run_id=run_id,
                status_code=status_code,
                error_message=error_message,
                provider_stats=provider_stats,
                attachment_stats=attachment_stats,
                result_data=result_data,
            )
        except Exception as exc:
            logger.warning("Run finalize failed: %s", exc)

    if storage is not None:
        try:
            storage.start_run(
                run_id=run_id,
                run_outdir=run_outdir,
                mode=mode,
                args_data=vars(args),
                config_data=config,
                time_window=time_window,
            )
        except Exception as exc:
            logger.warning("Run start persistence failed: %s", exc)

    # ------------------------------------------------------------------
    # report-only shortcut
    # ------------------------------------------------------------------
    if args.report_only:
        if storage is None:
            logger.error("--report-only requires database persistence.")
            finalize(1, "--report-only requires database")
            return 1
        return _run_report_only(
            storage=storage,
            config=config,
            run_outdir=run_outdir,
            run_id=run_id,
            total_start=total_start,
            logger=logger,
            finalize=finalize,
        )

    # ------------------------------------------------------------------
    # Acquire articles  (demo / input file / search + crawl)
    # ------------------------------------------------------------------
    articles: List[Dict[str, Any]] = []
    crawler_metrics: Dict[str, Any] = {}
    search_http: Optional[HttpClientFacade] = None

    if args.demo:
        provider_stats = {"demo": {"enabled": True, "count": 2, "status": "ok"}}
        articles = _demo_articles()

    elif args.input:
        if not os.path.exists(args.input):
            logger.error("Input file not found: %s", args.input)
            finalize(1, f"input file not found: {args.input}")
            return 1
        try:
            with open(args.input, "r", encoding="utf-8-sig") as fh:
                loaded = json.load(fh)
        except Exception as exc:
            logger.error("Invalid input JSON: %s", exc)
            finalize(1, f"invalid input json: {exc}")
            return 1
        articles = _normalize_input_articles(loaded, logger.getChild("input"))
        if not articles:
            finalize(1, "input has no valid article objects")
            return 1
        provider_stats = {"input": {"enabled": True, "count": len(articles), "status": "ok"}}

    elif args.search:
        t0 = time.perf_counter()
        urls, provider_stats, search_http = _stage_search(
            args=args, config=config, verify_option=verify_option,
            time_window=time_window, logger=logger,
        )
        perf["search_seconds"] = round(time.perf_counter() - t0, 3)

        if not urls:
            logger.error("Search returned no valid urls.")
            finalize(1, "search returned no valid urls")
            return 1

        t1 = time.perf_counter()
        articles, crawler_metrics = _stage_crawl(
            urls=urls, config=config, verify_option=verify_option,
            run_outdir=run_outdir, intermediate_files=intermediate_files, logger=logger,
        )
        perf["crawl_seconds"] = round(time.perf_counter() - t1, 3)

    else:
        logger.error("No data source provided. Use --search/--input/--demo.")
        finalize(1, "no data source provided")
        return 1

    if not articles:
        logger.error("No valid article records.")
        finalize(1, "no valid article records")
        return 1

    # ------------------------------------------------------------------
    # Shared filter chain: time-window → dedupe → attachments
    # ------------------------------------------------------------------
    filter_result = _apply_filters(articles, config=config, time_window=time_window, logger=logger)
    articles, content_dedupe_stats, attachment_stats = filter_result
    if articles is None:
        logger.error("No records left after filtering.")
        finalize(1, "no records after filtering")
        return 1

    # ------------------------------------------------------------------
    # scrape-only shortcut
    # ------------------------------------------------------------------
    if args.scrape_only:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        perf["total_seconds"] = round(time.perf_counter() - total_start, 3)
        output_data: Dict[str, Any] = {
            "meta": {
                "run_id": run_id,
                "mode": "scrape_only",
                "total_articles": len(articles),
                "time_window": time_window,
                "provider_stats": provider_stats,
                "attachment_stats": attachment_stats,
                "performance": perf,
                "output_dir": run_outdir,
            },
            "top_keywords": [],
            "repeated_terms": [],
            "selected_articles": articles,
            "category_details": {},
        }
        result_file = os.path.join(run_outdir, f"geo_analysis_result_{timestamp}.json")
        with open(result_file, "w", encoding="utf-8") as fh:
            json.dump(output_data, fh, ensure_ascii=False, indent=2)
        if storage is not None:
            try:
                output_data["meta"]["db_persisted"] = storage.persist_articles(run_id, articles)
            except Exception as exc:
                logger.warning("Scrape-only DB persistence failed: %s", exc)
                output_data["meta"]["db_persisted"] = 0
        _cleanup_intermediate_files(intermediate_files, logger.getChild("cleanup"))
        finalize(0, result_data=output_data)
        logger.info("Scrape-only run finished: %s", run_outdir)
        return 0

    # ------------------------------------------------------------------
    # Semantic analysis
    # ------------------------------------------------------------------
    analyzer = GeoKeywordAnalyzer(
        min_relevance_score=config["analysis"]["min_relevance_score"],
        advanced_only=config["analysis"]["advanced_only"],
        min_advanced_score=config["analysis"]["min_advanced_score"],
        top_keywords_count=config["analysis"]["top_keywords_count"],
        nebula_max_words=config["analysis"]["nebula_max_words"],
        core_identifiers=config["analysis"]["core_identifiers"],
        marker_weights=config["analysis"]["scoring"]["marker_weights"],
        keyword_categories=config["analysis"]["categories"],
        logger=logger.getChild("analysis"),
    )

    search_backend_usage = search_http.backend_usage() if search_http is not None else {}
    http_backend_used = _merge_counter_dict(search_backend_usage, crawler_metrics.get("backend_usage", {}))

    t0 = time.perf_counter()
    result = analyzer.analyze(
        articles,
        run_outdir,
        extra_meta={
            "run_id": run_id,
            "pipeline_mode": config["pipeline"]["crawl_mode"],
            "http_backend_used": http_backend_used,
            "extractor_stats": crawler_metrics.get("extractor_stats", {}),
            "provider_stats": provider_stats,
            "time_window": time_window,
            "attachment_stats": attachment_stats,
            "performance": perf,
        },
    )
    perf["analysis_seconds"] = round(time.perf_counter() - t0, 3)

    if not result:
        finalize(1, "analysis returned empty result")
        return 1

    # ------------------------------------------------------------------
    # Reports + visualizations
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    report_payload = generate_reports(
        outdir=run_outdir,
        timestamp=result["timestamp"],
        selected_articles=result["selected_articles"],
        repeated_terms=result["output_data"].get("repeated_terms", []),
    )
    perf["report_seconds"] = round(time.perf_counter() - t0, 3)

    run_history = storage.fetch_recent_runs(limit=8) if storage is not None else []

    t0 = time.perf_counter()
    viz_outputs = draw_visualizations(
        keyword_rows=report_payload["keyword_rows"],
        top_articles=report_payload["top_articles"],
        outdir=run_outdir,
        timestamp=result["timestamp"],
        viz_format=config["visualization"]["format"],
        max_words=config["analysis"]["nebula_max_words"],
        report_files=report_payload["files"],
        summary={
            "total_articles": len(result["selected_articles"]),
            "attachment_stats": attachment_stats,
            "run_history": run_history,
        },
        logger=logger.getChild("viz"),
    )
    perf["viz_seconds"] = round(time.perf_counter() - t0, 3)

    # ------------------------------------------------------------------
    # Quality search (optional)
    # ------------------------------------------------------------------
    quality_cfg = config["quality_search"]
    quality_payload_result: Dict[str, Any]

    if args.search and quality_cfg.get("enabled", False) and search_http is not None:
        t0 = time.perf_counter()
        quality_payload_result = _stage_quality_search(
            args=args,
            config=config,
            search_http=search_http,
            time_window=time_window,
            run_outdir=run_outdir,
            timestamp=result["timestamp"],
            verify_option=verify_option,
            logger=logger,
        )
        perf["quality_search_seconds"] = round(time.perf_counter() - t0, 3)
    else:
        quality_payload_result = {
            "enabled": bool(quality_cfg.get("enabled", False)),
            "executed": False,
            "reason": "quality_search only runs in --search full mode",
            "queries": [],
            "query_stats": [],
            "query_count": 0,
            "total_urls": 0,
            "crawled_articles": 0,
            "time_filtered_articles": 0,
            "content_dedupe": {},
            "attachment_stats": {},
            "crawler_metrics": {},
            "selected_articles": [],
            "files": {},
            "top_articles_summary": [],
        }

    perf["total_seconds"] = round(time.perf_counter() - total_start, 3)

    # ------------------------------------------------------------------
    # Assemble final output — pipeline owns all file I/O; single JSON write
    # ------------------------------------------------------------------
    dedupe_stats = {
        "enable_url_dedupe": dedupe_cfg["enable_url_dedupe"],
        "enable_content_hash_dedupe": dedupe_cfg["enable_content_hash_dedupe"],
        "enable_site_cache": dedupe_cfg["enable_site_cache"],
        "site_cache_max_per_domain": dedupe_cfg["site_cache_max_per_domain"],
        "content_hash": content_dedupe_stats,
        "crawler": crawler_metrics.get("crawl_stats", {}),
    }

    db_persisted = 0
    if storage is not None:
        try:
            db_persisted = storage.persist_articles(run_id, result["selected_articles"])
        except Exception as exc:
            logger.warning("DB persistence failed: %s", exc)

    output_data = result["output_data"]
    output_data["meta"].update({
        "run_id": run_id,
        "db_persisted": db_persisted,
        "time_window": time_window,
        "provider_stats": provider_stats,
        "attachment_stats": attachment_stats,
        "performance": perf,
        "dedupe_stats": dedupe_stats,
        "rate_limit_stats": crawler_metrics.get("rate_limit_stats", {}),
        "proxy_stats": crawler_metrics.get("proxy_stats", {}),
        "report_files": _basename_dict(report_payload["files"]),
        "viz_files": _basename_dict(viz_outputs),
        "quality_report_files": quality_payload_result.get("files", {}),
        "output_dir": run_outdir,
        "run_history": run_history,
    })
    _attach_quality_search_payload(output_data, quality_payload_result)

    with open(result["result_file"], "w", encoding="utf-8") as fh:
        json.dump(output_data, fh, ensure_ascii=False, indent=2)

    _cleanup_intermediate_files(intermediate_files, logger.getChild("cleanup"))
    finalize(0, result_data=output_data)
    logger.info("All done, files saved to: %s", run_outdir)
    return 0