"""Configuration utilities for geo analyzer pipeline."""

from __future__ import annotations

import copy
import math
import os
from typing import Any, Dict, Optional

try:
    import yaml
except ImportError:  # pragma: no cover - optional dependency
    yaml = None

SUPPORTED_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}
SUPPORTED_CRAWL_MODES = {"sync", "async", "stealth"}
SUPPORTED_HTTP_BACKENDS = {"auto", "curl_cffi", "requests"}
SUPPORTED_EXTRACTORS = {"auto", "trafilatura", "bs4"}
SUPPORTED_VIZ_FORMATS = {"png", "html", "both"}
SUPPORTED_TIME_PRESETS = {"today", "week", "month"}
SUPPORTED_SEARCH_PROVIDERS = {"bing", "baidu", "wechat", "serpapi", "google", "xiaohongshu", "bilibili", "douyin"}

DEFAULT_SEARCH_TIMEOUT = 15
DEFAULT_CRAWL_TIMEOUT = 20
DEFAULT_REQUEST_DELAY = 1.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_SEARCH_LIMIT = 15
DEFAULT_RECENT_MONTHS = 6
DEFAULT_INCLUDE_UNDATED = False
DEFAULT_MIN_RELEVANCE_SCORE = 1
DEFAULT_ADVANCED_ONLY = True
DEFAULT_MIN_ADVANCED_SCORE = 3
DEFAULT_TOP_KEYWORDS_COUNT = 100
DEFAULT_NEBULA_MAX_WORDS = 80
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE = "geo_analyzer.log"
DEFAULT_PROGRESS_BAR = True
DEFAULT_VERIFY_TLS = True
DEFAULT_CRAWL_MODE = "async"
DEFAULT_HTTP_BACKEND = "auto"
DEFAULT_MAX_CONCURRENCY = 12
DEFAULT_PER_DOMAIN_CONCURRENCY = 4
DEFAULT_RETRY_BACKOFF_FACTOR = 0.8
DEFAULT_STATUS_FORCELIST = [403, 429, 500, 502, 503, 504]
DEFAULT_RETRY_RESPECT_RETRY_AFTER = True
DEFAULT_RETRY_HOST_COOLDOWN_BASE_SECONDS = 5.0
DEFAULT_RETRY_HOST_COOLDOWN_MAX_SECONDS = 180.0
DEFAULT_RETRY_HOST_FORCELIST_THRESHOLD = 3
DEFAULT_EXTRACTION_PRIMARY = "auto"
DEFAULT_EXTRACTION_FALLBACK = "bs4"
DEFAULT_MIN_CONTENT_LENGTH = 120
DEFAULT_VIZ_FORMAT = "both"
DEFAULT_RATE_LIMIT_GLOBAL_RPS = 6.0
DEFAULT_RATE_LIMIT_PER_DOMAIN_RPS = 1.8
DEFAULT_RATE_LIMIT_JITTER_MS_MIN = 50
DEFAULT_RATE_LIMIT_JITTER_MS_MAX = 180
DEFAULT_STEALTH_BROWSER = "chromium"
DEFAULT_STEALTH_CHANNEL = "chrome"
DEFAULT_STEALTH_EXECUTABLE_PATH = ""
DEFAULT_STEALTH_HEADLESS = True
DEFAULT_STEALTH_MAX_CONCURRENCY = 3
DEFAULT_STEALTH_PER_DOMAIN_CONCURRENCY = 2
DEFAULT_STEALTH_MAX_RETRIES = 5
DEFAULT_STEALTH_BACKOFF_BASE_SECONDS = 3.0
DEFAULT_STEALTH_BACKOFF_MAX_SECONDS = 90.0
DEFAULT_STEALTH_PROXY_BAN_TTL_SECONDS = 900.0
DEFAULT_STEALTH_NAVIGATION_TIMEOUT_MS = 45000
DEFAULT_STEALTH_NETWORK_IDLE_TIMEOUT_MS = 10000
DEFAULT_STEALTH_HUMANIZE = True
DEFAULT_STEALTH_USE_PLUGIN = True
DEFAULT_STEALTH_LAUNCH_SLOW_MO_MS = 0
DEFAULT_STEALTH_LOCALE = "zh-CN"
DEFAULT_STEALTH_TIMEZONE_ID = "Asia/Shanghai"
DEFAULT_STEALTH_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
DEFAULT_STEALTH_VIEWPORT = {"width": 1920, "height": 1080}
DEFAULT_STEALTH_PROXIES: list[str] = []
DEFAULT_STEALTH_PROXY_FILE = ""
DEFAULT_STEALTH_STATUS_FORCELIST = [403, 407, 429, 500, 502, 503, 504]
DEFAULT_ENABLE_URL_DEDUPE = True
DEFAULT_ENABLE_CONTENT_HASH_DEDUPE = True
DEFAULT_ENABLE_SITE_CACHE = True
DEFAULT_SITE_CACHE_MAX_PER_DOMAIN = 200
DEFAULT_SERPAPI_ENABLED = False
DEFAULT_SERPAPI_API_KEY = ""
DEFAULT_SERPAPI_ENGINE = "google"
DEFAULT_SERPAPI_GL = "cn"
DEFAULT_SERPAPI_HL = "zh-cn"
DEFAULT_DB_PATH = "geo_monitor_v8.db"
DEFAULT_ENABLE_DB_WRITE = True
DEFAULT_PROVIDERS_ENABLED = ["bing", "baidu", "wechat", "serpapi"]
DEFAULT_PROVIDER_EXPERIMENTAL = {
    "google": False,
    "xiaohongshu": False,
    "bilibili": False,
    "douyin": False,
}
DEFAULT_TIME_PRESET = ""
DEFAULT_DATE_FROM = ""
DEFAULT_DATE_TO = ""
DEFAULT_ATTACHMENT_DETECTION_ENABLED = True
DEFAULT_ATTACHMENT_MIN_SCORE = 1.5
DEFAULT_QUALITY_SEARCH_ENABLED = True
DEFAULT_QUALITY_SEARCH_GENERAL_KEYWORDS = [
    "地理数据",
    "空间数据",
    "GIS数据",
    "数据分享",
    "数据发布",
    "数据共享",
    "数据链接",
    "地理模型",
    "大数据",
]
DEFAULT_QUALITY_SEARCH_TOPIC_KEYWORDS = [
    "基础地理",
    "DEM",
    "地形",
    "地貌",
    "土壤",
    "土地利用",
    "土地覆盖",
    "生态环境",
    "气象",
    "水文",
    "人口",
    "GDP",
    "社会经济",
]
DEFAULT_QUALITY_SEARCH_RUN_STANDALONE_QUERIES = True
DEFAULT_QUALITY_SEARCH_RUN_COMBINED_QUERIES = True
DEFAULT_QUALITY_SEARCH_PER_QUERY_LIMIT = 3
DEFAULT_QUALITY_SEARCH_MAX_TOTAL_URLS = 120

DEFAULT_CORE_IDENTIFIERS = [
    "地理",
    "GIS",
    "遥感",
    "卫星",
    "测绘",
    "地图",
    "空间分析",
    "土地",
    "规划",
    "自然资源",
    "环境",
    "生态",
    "北斗",
    "导航",
    "城市",
    "大数据",
    "数字孪生",
    "实景三维",
    "地质",
    "水文",
]

DEFAULT_POSITIVE_MARKERS = [
    "数据集",
    "数据分享",
    "数据产品",
    "数据发布",
    "数据上线",
    "机器学习",
    "深度学习",
    "随机森林",
    "梯度提升树",
    "支持向量机",
    "预测模拟",
    "地理信息",
    "遥感",
    "测绘",
    "大数据",
    "大模型",
    "前沿科技",
    "前沿动态",
]

DEFAULT_NEGATIVE_MARKERS = [
    "入门",
    "零基础",
    "小白",
    "基础教程",
    "科普",
    "是什么",
    "手把手",
    "一步一步",
    "快速上手",
    "新手",
    "扫盲",
    "必看",
]

DEFAULT_CATEGORIES = {
    "数据资源": ["数据集", "数据分享", "数据产品", "数据发布", "数据上线"],
    "技术方法": ["机器学习", "深度学习", "随机森林", "梯度提升树", "支持向量机", "预测模拟"],
    "技术动态": ["地理信息", "遥感", "测绘", "大数据", "大模型", "前沿科技", "前沿动态"],
}


def _build_default_marker_weights() -> Dict[str, float]:
    marker_weights: Dict[str, float] = {}
    for marker in DEFAULT_POSITIVE_MARKERS:
        marker_weights[marker] = marker_weights.get(marker, 0.0) + 1.0
    for marker in DEFAULT_NEGATIVE_MARKERS:
        marker_weights[marker] = marker_weights.get(marker, 0.0) - 1.0
    return marker_weights


DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
    "search": {
        "timeout": DEFAULT_SEARCH_TIMEOUT,
        "crawl_timeout": DEFAULT_CRAWL_TIMEOUT,
        "request_delay": DEFAULT_REQUEST_DELAY,
        "limit": DEFAULT_SEARCH_LIMIT,
        "recent_months": DEFAULT_RECENT_MONTHS,
        "include_undated": DEFAULT_INCLUDE_UNDATED,
        "providers": {
            "serpapi": {
                "enabled": DEFAULT_SERPAPI_ENABLED,
                "api_key": DEFAULT_SERPAPI_API_KEY,
                "engine": DEFAULT_SERPAPI_ENGINE,
                "gl": DEFAULT_SERPAPI_GL,
                "hl": DEFAULT_SERPAPI_HL,
            }
        },
    },
    "analysis": {
        "min_relevance_score": DEFAULT_MIN_RELEVANCE_SCORE,
        "advanced_only": DEFAULT_ADVANCED_ONLY,
        "min_advanced_score": DEFAULT_MIN_ADVANCED_SCORE,
        "top_keywords_count": DEFAULT_TOP_KEYWORDS_COUNT,
        "nebula_max_words": DEFAULT_NEBULA_MAX_WORDS,
        "core_identifiers": DEFAULT_CORE_IDENTIFIERS,
        "scoring": {
            "marker_weights": _build_default_marker_weights(),
            "positive_markers": DEFAULT_POSITIVE_MARKERS,
            "negative_markers": DEFAULT_NEGATIVE_MARKERS,
        },
        "categories": DEFAULT_CATEGORIES,
    },
    "logging": {
        "level": DEFAULT_LOG_LEVEL,
        "file": DEFAULT_LOG_FILE,
    },
    "security": {
        "verify_tls": DEFAULT_VERIFY_TLS,
    },
    "ui": {
        "progress_bar": DEFAULT_PROGRESS_BAR,
    },
    "network": {
        "http_backend": DEFAULT_HTTP_BACKEND,
        "max_concurrency": DEFAULT_MAX_CONCURRENCY,
        "per_domain_concurrency": DEFAULT_PER_DOMAIN_CONCURRENCY,
        "stealth": {
            "browser": DEFAULT_STEALTH_BROWSER,
            "channel": DEFAULT_STEALTH_CHANNEL,
            "executable_path": DEFAULT_STEALTH_EXECUTABLE_PATH,
            "headless": DEFAULT_STEALTH_HEADLESS,
            "max_concurrency": DEFAULT_STEALTH_MAX_CONCURRENCY,
            "per_domain_concurrency": DEFAULT_STEALTH_PER_DOMAIN_CONCURRENCY,
            "max_retries": DEFAULT_STEALTH_MAX_RETRIES,
            "backoff_base_seconds": DEFAULT_STEALTH_BACKOFF_BASE_SECONDS,
            "backoff_max_seconds": DEFAULT_STEALTH_BACKOFF_MAX_SECONDS,
            "proxy_ban_ttl_seconds": DEFAULT_STEALTH_PROXY_BAN_TTL_SECONDS,
            "navigation_timeout_ms": DEFAULT_STEALTH_NAVIGATION_TIMEOUT_MS,
            "network_idle_timeout_ms": DEFAULT_STEALTH_NETWORK_IDLE_TIMEOUT_MS,
            "humanize": DEFAULT_STEALTH_HUMANIZE,
            "use_stealth_plugin": DEFAULT_STEALTH_USE_PLUGIN,
            "launch_slow_mo_ms": DEFAULT_STEALTH_LAUNCH_SLOW_MO_MS,
            "locale": DEFAULT_STEALTH_LOCALE,
            "timezone_id": DEFAULT_STEALTH_TIMEZONE_ID,
            "user_agent": DEFAULT_STEALTH_USER_AGENT,
            "viewport": DEFAULT_STEALTH_VIEWPORT,
            "proxies": DEFAULT_STEALTH_PROXIES,
            "proxy_file": DEFAULT_STEALTH_PROXY_FILE,
            "status_forcelist": DEFAULT_STEALTH_STATUS_FORCELIST,
        },
        "rate_limit": {
            "global_rps": DEFAULT_RATE_LIMIT_GLOBAL_RPS,
            "per_domain_rps": DEFAULT_RATE_LIMIT_PER_DOMAIN_RPS,
            "jitter_ms_min": DEFAULT_RATE_LIMIT_JITTER_MS_MIN,
            "jitter_ms_max": DEFAULT_RATE_LIMIT_JITTER_MS_MAX,
        },
        "retry": {
            "max_retries": DEFAULT_MAX_RETRIES,
            "backoff_factor": DEFAULT_RETRY_BACKOFF_FACTOR,
            "status_forcelist": DEFAULT_STATUS_FORCELIST,
            "respect_retry_after": DEFAULT_RETRY_RESPECT_RETRY_AFTER,
            "host_cooldown_base_seconds": DEFAULT_RETRY_HOST_COOLDOWN_BASE_SECONDS,
            "host_cooldown_max_seconds": DEFAULT_RETRY_HOST_COOLDOWN_MAX_SECONDS,
            "host_forcelist_threshold": DEFAULT_RETRY_HOST_FORCELIST_THRESHOLD,
        },
    },
    "dedupe": {
        "enable_url_dedupe": DEFAULT_ENABLE_URL_DEDUPE,
        "enable_content_hash_dedupe": DEFAULT_ENABLE_CONTENT_HASH_DEDUPE,
        "enable_site_cache": DEFAULT_ENABLE_SITE_CACHE,
        "site_cache_max_per_domain": DEFAULT_SITE_CACHE_MAX_PER_DOMAIN,
    },
    "extraction": {
        "primary": DEFAULT_EXTRACTION_PRIMARY,
        "fallback": DEFAULT_EXTRACTION_FALLBACK,
        "min_content_length": DEFAULT_MIN_CONTENT_LENGTH,
    },
    "visualization": {
        "format": DEFAULT_VIZ_FORMAT,
    },
    "storage": {
        "db_path": DEFAULT_DB_PATH,
        "enable_db_write": DEFAULT_ENABLE_DB_WRITE,
    },
    "providers": {
        "enabled": DEFAULT_PROVIDERS_ENABLED,
        "experimental": DEFAULT_PROVIDER_EXPERIMENTAL,
    },
    "time_window": {
        "preset": DEFAULT_TIME_PRESET,
        "date_from": DEFAULT_DATE_FROM,
        "date_to": DEFAULT_DATE_TO,
    },
    "attachment_detection": {
        "enabled": DEFAULT_ATTACHMENT_DETECTION_ENABLED,
        "min_score": DEFAULT_ATTACHMENT_MIN_SCORE,
    },
    "quality_search": {
        "enabled": DEFAULT_QUALITY_SEARCH_ENABLED,
        "general_keywords": DEFAULT_QUALITY_SEARCH_GENERAL_KEYWORDS,
        "topic_keywords": DEFAULT_QUALITY_SEARCH_TOPIC_KEYWORDS,
        "run_standalone_queries": DEFAULT_QUALITY_SEARCH_RUN_STANDALONE_QUERIES,
        "run_combined_queries": DEFAULT_QUALITY_SEARCH_RUN_COMBINED_QUERIES,
        "per_query_limit": DEFAULT_QUALITY_SEARCH_PER_QUERY_LIMIT,
        "max_total_urls": DEFAULT_QUALITY_SEARCH_MAX_TOTAL_URLS,
    },
    "pipeline": {
        "crawl_mode": DEFAULT_CRAWL_MODE,
    },
}


def merge_config_with_defaults(defaults: Dict[str, Any], overrides: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = copy.deepcopy(defaults)
    if not overrides:
        return merged

    for key, value in overrides.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_config_with_defaults(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}

    if not os.path.exists(path):
        raise FileNotFoundError(f"配置文件不存在: {path}")

    if yaml is None:
        raise RuntimeError("检测到 --config，但未安装 PyYAML。请运行: pip install pyyaml")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError("配置文件根节点必须为对象/字典")
    return data


def _validate_positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} 必须是正整数，当前值: {value!r}")
    return value


def _validate_non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} 必须是非负整数，当前值: {value!r}")
    return value


def _validate_non_negative_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{field_name} 必须是非负数字，当前值: {value!r}")
    parsed = float(value)
    if math.isnan(parsed) or math.isinf(parsed):
        raise ValueError(f"{field_name} 必须是有限数字")
    return parsed


def _validate_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} 必须是布尔值")
    return value


def _validate_string_choice(value: Any, field_name: str, candidates: set[str]) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} 必须是字符串")
    lowered = value.strip().lower()
    if lowered not in candidates:
        raise ValueError(f"{field_name} 不合法: {value!r}，可选: {sorted(candidates)}")
    return lowered


def _validate_string_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"{field_name} 必须是字符串数组")
    return value


def _normalize_distinct_string_list(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen = set()
    for item in values:
        candidate = str(item or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def _validate_provider_list(value: Any, field_name: str) -> list[str]:
    providers = [item.strip().lower() for item in _validate_string_list(value, field_name) if item.strip()]
    unknown = [item for item in providers if item not in SUPPORTED_SEARCH_PROVIDERS]
    if unknown:
        raise ValueError(f"{field_name} contains unsupported providers: {unknown}")
    if not providers:
        raise ValueError(f"{field_name} cannot be empty")
    deduped: list[str] = []
    seen = set()
    for item in providers:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _validate_provider_experimental(value: Any, field_name: str) -> dict[str, bool]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    parsed: dict[str, bool] = {}
    for key, raw in value.items():
        normalized_key = str(key).strip().lower()
        if normalized_key not in {"google", "xiaohongshu", "bilibili", "douyin"}:
            raise ValueError(f"{field_name} has unsupported key: {key!r}")
        parsed[normalized_key] = _validate_bool(raw, f"{field_name}.{normalized_key}")
    for key in ("google", "xiaohongshu", "bilibili", "douyin"):
        parsed.setdefault(key, False)
    return parsed


def _validate_string(value: Any, field_name: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} 必须是字符串")
    parsed = value.strip()
    if not allow_empty and not parsed:
        raise ValueError(f"{field_name} 不能为空字符串")
    return parsed


def _validate_int_list(value: Any, field_name: str) -> list[int]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, int) for item in value):
        raise ValueError(f"{field_name} must be an integer list")
    return value


def _validate_viewport(value: Any, field_name: str) -> dict[str, int]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    width = value.get("width")
    height = value.get("height")
    if isinstance(width, bool) or not isinstance(width, int) or width <= 0:
        raise ValueError(f"{field_name}.width must be a positive integer")
    if isinstance(height, bool) or not isinstance(height, int) or height <= 0:
        raise ValueError(f"{field_name}.height must be a positive integer")
    return {"width": width, "height": height}


def _validate_categories(value: Any, field_name: str) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} 必须是对象")
    parsed: dict[str, list[str]] = {}
    for key, words in value.items():
        if not isinstance(key, str):
            raise ValueError(f"{field_name} 的 key 必须是字符串")
        if not isinstance(words, list) or any(not isinstance(item, str) for item in words):
            raise ValueError(f"{field_name}.{key} 必须是字符串数组")
        parsed[key] = words
    return parsed


def _validate_marker_weights(value: Any, field_name: str) -> dict[str, float]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} 必须是对象")
    parsed: dict[str, float] = {}
    for key, weight in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{field_name} 的 key 必须是非空字符串")
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise ValueError(f"{field_name}.{key} 必须是数字")
        num = float(weight)
        if math.isnan(num) or math.isinf(num):
            raise ValueError(f"{field_name}.{key} 必须是有限数字")
        parsed[key.strip()] = num
    return parsed


def _build_marker_weights_from_legacy(positive_markers: list[str], negative_markers: list[str]) -> dict[str, float]:
    marker_weights: dict[str, float] = {}
    for marker in positive_markers:
        marker_weights[marker] = marker_weights.get(marker, 0.0) + 1.0
    for marker in negative_markers:
        marker_weights[marker] = marker_weights.get(marker, 0.0) - 1.0
    return marker_weights


def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(config, dict):
        raise ValueError("配置必须为字典")

    expected_sections = (
        "search",
        "analysis",
        "logging",
        "security",
        "ui",
        "network",
        "dedupe",
        "extraction",
        "visualization",
        "storage",
        "providers",
        "time_window",
        "attachment_detection",
        "quality_search",
        "pipeline",
    )
    for section in expected_sections:
        if section not in config or not isinstance(config[section], dict):
            raise ValueError(f"配置缺少或损坏 section: {section}")

    search = config["search"]
    search["timeout"] = _validate_positive_int(search.get("timeout"), "search.timeout")
    search["crawl_timeout"] = _validate_positive_int(search.get("crawl_timeout"), "search.crawl_timeout")
    search["request_delay"] = _validate_non_negative_float(search.get("request_delay"), "search.request_delay")
    search["limit"] = _validate_positive_int(search.get("limit"), "search.limit")
    search["recent_months"] = _validate_non_negative_int(search.get("recent_months"), "search.recent_months")
    search["include_undated"] = _validate_bool(search.get("include_undated"), "search.include_undated")
    providers_cfg = search.get("providers")
    if not isinstance(providers_cfg, dict):
        raise ValueError("search.providers 必须是对象")
    serpapi_cfg = providers_cfg.get("serpapi")
    if not isinstance(serpapi_cfg, dict):
        raise ValueError("search.providers.serpapi 必须是对象")
    serpapi_cfg["enabled"] = _validate_bool(serpapi_cfg.get("enabled"), "search.providers.serpapi.enabled")
    serpapi_cfg["api_key"] = _validate_string(
        serpapi_cfg.get("api_key", ""), "search.providers.serpapi.api_key", allow_empty=True
    )
    serpapi_cfg["engine"] = _validate_string(serpapi_cfg.get("engine"), "search.providers.serpapi.engine")
    serpapi_cfg["gl"] = _validate_string(serpapi_cfg.get("gl", ""), "search.providers.serpapi.gl", allow_empty=True)
    serpapi_cfg["hl"] = _validate_string(serpapi_cfg.get("hl", ""), "search.providers.serpapi.hl", allow_empty=True)
    if serpapi_cfg["enabled"] and not serpapi_cfg["api_key"]:
        raise ValueError("search.providers.serpapi.enabled=true 时必须提供 api_key")
    providers_cfg["serpapi"] = serpapi_cfg
    search["providers"] = providers_cfg

    analysis = config["analysis"]
    analysis["min_relevance_score"] = _validate_positive_int(
        analysis.get("min_relevance_score"), "analysis.min_relevance_score"
    )
    analysis["advanced_only"] = _validate_bool(analysis.get("advanced_only"), "analysis.advanced_only")
    analysis["min_advanced_score"] = _validate_non_negative_int(
        analysis.get("min_advanced_score"), "analysis.min_advanced_score"
    )
    analysis["top_keywords_count"] = _validate_positive_int(
        analysis.get("top_keywords_count"), "analysis.top_keywords_count"
    )
    analysis["nebula_max_words"] = _validate_positive_int(analysis.get("nebula_max_words"), "analysis.nebula_max_words")
    analysis["core_identifiers"] = _validate_string_list(analysis.get("core_identifiers"), "analysis.core_identifiers")

    scoring = analysis.get("scoring")
    if not isinstance(scoring, dict):
        raise ValueError("analysis.scoring 必须是对象")
    positive_markers = _validate_string_list(scoring.get("positive_markers"), "analysis.scoring.positive_markers")
    negative_markers = _validate_string_list(scoring.get("negative_markers"), "analysis.scoring.negative_markers")
    marker_weights_raw = scoring.get("marker_weights")
    if marker_weights_raw is None:
        marker_weights = _build_marker_weights_from_legacy(positive_markers, negative_markers)
    else:
        marker_weights = _validate_marker_weights(marker_weights_raw, "analysis.scoring.marker_weights")
    if not marker_weights:
        raise ValueError("analysis.scoring.marker_weights 不能为空")
    scoring["positive_markers"] = positive_markers
    scoring["negative_markers"] = negative_markers
    scoring["marker_weights"] = marker_weights
    analysis["scoring"] = scoring

    analysis["categories"] = _validate_categories(analysis.get("categories"), "analysis.categories")

    logging_cfg = config["logging"]
    level = str(logging_cfg.get("level", "")).upper()
    if level not in SUPPORTED_LOG_LEVELS:
        raise ValueError(f"logging.level 不合法: {level!r}，可选: {sorted(SUPPORTED_LOG_LEVELS)}")
    logging_cfg["level"] = level
    log_file = logging_cfg.get("file", DEFAULT_LOG_FILE)
    if not isinstance(log_file, str) or not log_file.strip():
        raise ValueError("logging.file 必须是非空字符串")
    logging_cfg["file"] = log_file

    security_cfg = config["security"]
    security_cfg["verify_tls"] = _validate_bool(security_cfg.get("verify_tls"), "security.verify_tls")

    ui_cfg = config["ui"]
    ui_cfg["progress_bar"] = _validate_bool(ui_cfg.get("progress_bar"), "ui.progress_bar")

    network_cfg = config["network"]
    network_cfg["http_backend"] = _validate_string_choice(
        network_cfg.get("http_backend"), "network.http_backend", SUPPORTED_HTTP_BACKENDS
    )
    network_cfg["max_concurrency"] = _validate_positive_int(network_cfg.get("max_concurrency"), "network.max_concurrency")
    network_cfg["per_domain_concurrency"] = _validate_positive_int(
        network_cfg.get("per_domain_concurrency"), "network.per_domain_concurrency"
    )
    retry_cfg = network_cfg.get("retry")
    if not isinstance(retry_cfg, dict):
        raise ValueError("network.retry 必须是对象")
    retry_cfg["max_retries"] = _validate_non_negative_int(retry_cfg.get("max_retries"), "network.retry.max_retries")
    retry_cfg["backoff_factor"] = _validate_non_negative_float(
        retry_cfg.get("backoff_factor"), "network.retry.backoff_factor"
    )
    status_forcelist = retry_cfg.get("status_forcelist")
    if not isinstance(status_forcelist, list) or any(not isinstance(code, int) for code in status_forcelist):
        raise ValueError("network.retry.status_forcelist 必须是整数数组")
    retry_cfg["status_forcelist"] = status_forcelist
    retry_cfg["respect_retry_after"] = _validate_bool(
        retry_cfg.get("respect_retry_after"), "network.retry.respect_retry_after"
    )
    retry_cfg["host_cooldown_base_seconds"] = _validate_non_negative_float(
        retry_cfg.get("host_cooldown_base_seconds"), "network.retry.host_cooldown_base_seconds"
    )
    retry_cfg["host_cooldown_max_seconds"] = _validate_non_negative_float(
        retry_cfg.get("host_cooldown_max_seconds"), "network.retry.host_cooldown_max_seconds"
    )
    if retry_cfg["host_cooldown_max_seconds"] < retry_cfg["host_cooldown_base_seconds"]:
        raise ValueError("network.retry.host_cooldown_max_seconds 必须 >= host_cooldown_base_seconds")
    retry_cfg["host_forcelist_threshold"] = _validate_positive_int(
        retry_cfg.get("host_forcelist_threshold"), "network.retry.host_forcelist_threshold"
    )
    network_cfg["retry"] = retry_cfg

    rate_limit_cfg = network_cfg.get("rate_limit")
    if not isinstance(rate_limit_cfg, dict):
        raise ValueError("network.rate_limit 必须是对象")
    rate_limit_cfg["global_rps"] = _validate_non_negative_float(rate_limit_cfg.get("global_rps"), "network.rate_limit.global_rps")
    rate_limit_cfg["per_domain_rps"] = _validate_non_negative_float(
        rate_limit_cfg.get("per_domain_rps"), "network.rate_limit.per_domain_rps"
    )
    rate_limit_cfg["jitter_ms_min"] = _validate_non_negative_int(
        rate_limit_cfg.get("jitter_ms_min"), "network.rate_limit.jitter_ms_min"
    )
    rate_limit_cfg["jitter_ms_max"] = _validate_non_negative_int(
        rate_limit_cfg.get("jitter_ms_max"), "network.rate_limit.jitter_ms_max"
    )
    network_cfg["rate_limit"] = rate_limit_cfg

    stealth_cfg = network_cfg.get("stealth")
    if not isinstance(stealth_cfg, dict):
        raise ValueError("network.stealth must be an object")
    stealth_cfg["browser"] = _validate_string_choice(
        stealth_cfg.get("browser"), "network.stealth.browser", {"chromium", "firefox", "webkit"}
    )
    stealth_cfg["channel"] = _validate_string(
        stealth_cfg.get("channel", ""), "network.stealth.channel", allow_empty=True
    )
    stealth_cfg["executable_path"] = _validate_string(
        stealth_cfg.get("executable_path", ""), "network.stealth.executable_path", allow_empty=True
    )
    if stealth_cfg["channel"] and stealth_cfg["executable_path"]:
        raise ValueError("network.stealth.channel and executable_path cannot both be set")
    stealth_cfg["headless"] = _validate_bool(stealth_cfg.get("headless"), "network.stealth.headless")
    stealth_cfg["max_concurrency"] = _validate_positive_int(
        stealth_cfg.get("max_concurrency"), "network.stealth.max_concurrency"
    )
    stealth_cfg["per_domain_concurrency"] = _validate_positive_int(
        stealth_cfg.get("per_domain_concurrency"), "network.stealth.per_domain_concurrency"
    )
    if stealth_cfg["per_domain_concurrency"] > stealth_cfg["max_concurrency"]:
        raise ValueError("network.stealth.per_domain_concurrency must be <= max_concurrency")
    stealth_cfg["max_retries"] = _validate_non_negative_int(
        stealth_cfg.get("max_retries"), "network.stealth.max_retries"
    )
    stealth_cfg["backoff_base_seconds"] = _validate_non_negative_float(
        stealth_cfg.get("backoff_base_seconds"), "network.stealth.backoff_base_seconds"
    )
    stealth_cfg["backoff_max_seconds"] = _validate_non_negative_float(
        stealth_cfg.get("backoff_max_seconds"), "network.stealth.backoff_max_seconds"
    )
    if stealth_cfg["backoff_max_seconds"] < stealth_cfg["backoff_base_seconds"]:
        raise ValueError("network.stealth.backoff_max_seconds must be >= backoff_base_seconds")
    stealth_cfg["proxy_ban_ttl_seconds"] = _validate_non_negative_float(
        stealth_cfg.get("proxy_ban_ttl_seconds"), "network.stealth.proxy_ban_ttl_seconds"
    )
    stealth_cfg["navigation_timeout_ms"] = _validate_positive_int(
        stealth_cfg.get("navigation_timeout_ms"), "network.stealth.navigation_timeout_ms"
    )
    stealth_cfg["network_idle_timeout_ms"] = _validate_positive_int(
        stealth_cfg.get("network_idle_timeout_ms"), "network.stealth.network_idle_timeout_ms"
    )
    stealth_cfg["humanize"] = _validate_bool(stealth_cfg.get("humanize"), "network.stealth.humanize")
    stealth_cfg["use_stealth_plugin"] = _validate_bool(
        stealth_cfg.get("use_stealth_plugin"), "network.stealth.use_stealth_plugin"
    )
    stealth_cfg["launch_slow_mo_ms"] = _validate_non_negative_int(
        stealth_cfg.get("launch_slow_mo_ms"), "network.stealth.launch_slow_mo_ms"
    )
    stealth_cfg["locale"] = _validate_string(
        stealth_cfg.get("locale", ""), "network.stealth.locale", allow_empty=True
    )
    stealth_cfg["timezone_id"] = _validate_string(
        stealth_cfg.get("timezone_id", ""), "network.stealth.timezone_id", allow_empty=True
    )
    stealth_cfg["user_agent"] = _validate_string(
        stealth_cfg.get("user_agent", ""), "network.stealth.user_agent", allow_empty=True
    )
    stealth_cfg["viewport"] = _validate_viewport(stealth_cfg.get("viewport"), "network.stealth.viewport")
    stealth_cfg["proxies"] = [item.strip() for item in _validate_string_list(stealth_cfg.get("proxies"), "network.stealth.proxies") if item.strip()]
    stealth_cfg["proxy_file"] = _validate_string(
        stealth_cfg.get("proxy_file", ""), "network.stealth.proxy_file", allow_empty=True
    )
    stealth_cfg["status_forcelist"] = _validate_int_list(
        stealth_cfg.get("status_forcelist"), "network.stealth.status_forcelist"
    )
    if not stealth_cfg["status_forcelist"]:
        raise ValueError("network.stealth.status_forcelist cannot be empty")
    network_cfg["stealth"] = stealth_cfg

    dedupe_cfg = config["dedupe"]
    dedupe_cfg["enable_url_dedupe"] = _validate_bool(dedupe_cfg.get("enable_url_dedupe"), "dedupe.enable_url_dedupe")
    dedupe_cfg["enable_content_hash_dedupe"] = _validate_bool(
        dedupe_cfg.get("enable_content_hash_dedupe"), "dedupe.enable_content_hash_dedupe"
    )
    dedupe_cfg["enable_site_cache"] = _validate_bool(dedupe_cfg.get("enable_site_cache"), "dedupe.enable_site_cache")
    dedupe_cfg["site_cache_max_per_domain"] = _validate_positive_int(
        dedupe_cfg.get("site_cache_max_per_domain"), "dedupe.site_cache_max_per_domain"
    )

    extraction_cfg = config["extraction"]
    extraction_cfg["primary"] = _validate_string_choice(
        extraction_cfg.get("primary"), "extraction.primary", SUPPORTED_EXTRACTORS
    )
    extraction_cfg["fallback"] = _validate_string_choice(
        extraction_cfg.get("fallback"), "extraction.fallback", SUPPORTED_EXTRACTORS
    )
    extraction_cfg["min_content_length"] = _validate_positive_int(
        extraction_cfg.get("min_content_length"), "extraction.min_content_length"
    )

    viz_cfg = config["visualization"]
    viz_cfg["format"] = _validate_string_choice(viz_cfg.get("format"), "visualization.format", SUPPORTED_VIZ_FORMATS)

    storage_cfg = config["storage"]
    storage_cfg["db_path"] = _validate_string(storage_cfg.get("db_path"), "storage.db_path")
    storage_cfg["enable_db_write"] = _validate_bool(storage_cfg.get("enable_db_write"), "storage.enable_db_write")

    providers_cfg = config["providers"]
    providers_cfg["enabled"] = _validate_provider_list(providers_cfg.get("enabled"), "providers.enabled")
    providers_cfg["experimental"] = _validate_provider_experimental(
        providers_cfg.get("experimental", {}),
        "providers.experimental",
    )

    time_window_cfg = config["time_window"]
    preset = _validate_string(time_window_cfg.get("preset", ""), "time_window.preset", allow_empty=True).lower()
    if preset and preset not in SUPPORTED_TIME_PRESETS:
        raise ValueError(f"time_window.preset invalid: {preset!r}, choices={sorted(SUPPORTED_TIME_PRESETS)}")
    time_window_cfg["preset"] = preset
    time_window_cfg["date_from"] = _validate_string(
        time_window_cfg.get("date_from", ""),
        "time_window.date_from",
        allow_empty=True,
    )
    time_window_cfg["date_to"] = _validate_string(
        time_window_cfg.get("date_to", ""),
        "time_window.date_to",
        allow_empty=True,
    )

    attachment_cfg = config["attachment_detection"]
    attachment_cfg["enabled"] = _validate_bool(attachment_cfg.get("enabled"), "attachment_detection.enabled")
    attachment_cfg["min_score"] = _validate_non_negative_float(
        attachment_cfg.get("min_score"),
        "attachment_detection.min_score",
    )

    quality_cfg = config["quality_search"]
    quality_cfg["enabled"] = _validate_bool(quality_cfg.get("enabled"), "quality_search.enabled")
    quality_cfg["general_keywords"] = _normalize_distinct_string_list(
        _validate_string_list(quality_cfg.get("general_keywords"), "quality_search.general_keywords")
    )
    quality_cfg["topic_keywords"] = _normalize_distinct_string_list(
        _validate_string_list(quality_cfg.get("topic_keywords"), "quality_search.topic_keywords")
    )
    quality_cfg["run_standalone_queries"] = _validate_bool(
        quality_cfg.get("run_standalone_queries"),
        "quality_search.run_standalone_queries",
    )
    quality_cfg["run_combined_queries"] = _validate_bool(
        quality_cfg.get("run_combined_queries"),
        "quality_search.run_combined_queries",
    )
    if not quality_cfg["run_standalone_queries"] and not quality_cfg["run_combined_queries"]:
        raise ValueError(
            "quality_search.run_standalone_queries and quality_search.run_combined_queries cannot both be false"
        )
    quality_cfg["per_query_limit"] = _validate_positive_int(
        quality_cfg.get("per_query_limit"),
        "quality_search.per_query_limit",
    )
    quality_cfg["max_total_urls"] = _validate_positive_int(
        quality_cfg.get("max_total_urls"),
        "quality_search.max_total_urls",
    )

    pipeline_cfg = config["pipeline"]
    pipeline_cfg["crawl_mode"] = _validate_string_choice(
        pipeline_cfg.get("crawl_mode"), "pipeline.crawl_mode", SUPPORTED_CRAWL_MODES
    )

    return config


def apply_cli_overrides(config: Dict[str, Any], args) -> Dict[str, Any]:
    overridden = copy.deepcopy(config)

    if args.limit is not None:
        overridden["search"]["limit"] = args.limit
    if args.request_delay is not None:
        overridden["search"]["request_delay"] = args.request_delay
    if args.timeout is not None:
        overridden["search"]["timeout"] = args.timeout
    if args.crawl_timeout is not None:
        overridden["search"]["crawl_timeout"] = args.crawl_timeout
    if args.recent_months is not None:
        overridden["search"]["recent_months"] = args.recent_months
    if args.include_undated:
        overridden["search"]["include_undated"] = True
    if args.time_preset is not None:
        overridden["time_window"]["preset"] = args.time_preset
    if args.date_from is not None:
        overridden["time_window"]["date_from"] = args.date_from
    if args.date_to is not None:
        overridden["time_window"]["date_to"] = args.date_to
    if args.providers is not None:
        parsed_providers = [item.strip().lower() for item in str(args.providers).split(",") if item.strip()]
        overridden["providers"]["enabled"] = parsed_providers

    if args.log_level is not None:
        overridden["logging"]["level"] = args.log_level
    if args.log_file is not None:
        overridden["logging"]["file"] = args.log_file

    if args.db_path is not None:
        overridden["storage"]["db_path"] = args.db_path
    if args.no_db_write:
        overridden["storage"]["enable_db_write"] = False

    if args.no_progress:
        overridden["ui"]["progress_bar"] = False

    if args.http_backend is not None:
        overridden["network"]["http_backend"] = args.http_backend
    if args.max_concurrency is not None:
        overridden["network"]["max_concurrency"] = args.max_concurrency
        overridden["network"]["stealth"]["max_concurrency"] = args.max_concurrency
    if args.max_retries is not None:
        overridden["network"]["retry"]["max_retries"] = args.max_retries
    if args.stealth_max_retries is not None:
        overridden["network"]["stealth"]["max_retries"] = args.stealth_max_retries
    if args.proxy is not None:
        overridden["network"]["stealth"]["proxies"] = args.proxy
    if args.proxy_file is not None:
        overridden["network"]["stealth"]["proxy_file"] = args.proxy_file
    if args.stealth_channel is not None:
        overridden["network"]["stealth"]["channel"] = args.stealth_channel
    if args.stealth_executable_path is not None:
        overridden["network"]["stealth"]["executable_path"] = args.stealth_executable_path
    if args.stealth_headful:
        overridden["network"]["stealth"]["headless"] = False
    if args.disable_humanize:
        overridden["network"]["stealth"]["humanize"] = False
    if args.disable_stealth_plugin:
        overridden["network"]["stealth"]["use_stealth_plugin"] = False

    if args.extractor is not None:
        overridden["extraction"]["primary"] = args.extractor
    if args.min_content_length is not None:
        overridden["extraction"]["min_content_length"] = args.min_content_length

    if args.no_advanced_filter:
        overridden["analysis"]["advanced_only"] = False
    if args.min_advanced_score is not None:
        overridden["analysis"]["min_advanced_score"] = args.min_advanced_score

    if args.crawl_mode is not None:
        overridden["pipeline"]["crawl_mode"] = args.crawl_mode

    if args.viz_format is not None:
        overridden["visualization"]["format"] = args.viz_format
    if args.no_interactive_viz:
        if overridden["visualization"]["format"] in ("both", "html"):
            overridden["visualization"]["format"] = "png"

    return overridden


def build_runtime_config(args) -> Dict[str, Any]:
    config_path = args.config
    if not config_path:
        auto_path = os.path.join(os.getcwd(), "run_config.yaml")
        if os.path.exists(auto_path):
            config_path = auto_path
    config = merge_config_with_defaults(DEFAULT_CONFIG, load_config(config_path))
    config = apply_cli_overrides(config, args)
    if not getattr(args, "db_path", None):
        configured_path = str(config.get("storage", {}).get("db_path", "") or "")
        if configured_path == DEFAULT_DB_PATH:
            outdir = getattr(args, "outdir", ".") or "."
            config["storage"]["db_path"] = os.path.join(outdir, DEFAULT_DB_PATH)
    return validate_config(config)
