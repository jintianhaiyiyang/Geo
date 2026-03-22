from __future__ import annotations

import copy

import pytest

from geo_analyzer.config import DEFAULT_CONFIG, merge_config_with_defaults, validate_config
from geo_analyzer.pipeline import _attach_quality_search_payload, _build_quality_queries, _collect_quality_urls


def _build_validated_config(overrides=None):
    config = merge_config_with_defaults(DEFAULT_CONFIG, overrides or {})
    return validate_config(config)


def test_quality_search_defaults_are_available():
    config = _build_validated_config()
    quality = config["quality_search"]

    assert quality["enabled"] is True
    assert quality["run_standalone_queries"] is True
    assert quality["run_combined_queries"] is True
    assert quality["per_query_limit"] == 3
    assert quality["max_total_urls"] == 120
    assert "地理数据" in quality["general_keywords"]
    assert "DEM" in quality["topic_keywords"]


def test_quality_search_validation_rejects_all_query_modes_disabled():
    config = merge_config_with_defaults(DEFAULT_CONFIG, {})
    config["quality_search"]["run_standalone_queries"] = False
    config["quality_search"]["run_combined_queries"] = False

    with pytest.raises(ValueError):
        validate_config(config)


def test_build_quality_queries_runs_standalone_and_combined():
    quality_cfg = {
        "general_keywords": ["地理数据", "地理数据"],
        "topic_keywords": ["DEM"],
        "run_standalone_queries": True,
        "run_combined_queries": True,
    }
    queries = _build_quality_queries("GIS 遥感", quality_cfg)

    assert queries == [
        "地理数据",
        "GIS 遥感 地理数据",
        "DEM",
        "GIS 遥感 DEM",
    ]


class _DummySearcher:
    def __init__(self, result_map):
        self._result_map = result_map
        self.calls = []

    def search(self, query, limit, include_weixin, include_overseas, time_window):
        self.calls.append((query, limit, include_weixin, include_overseas, bool(time_window)))
        return list(self._result_map.get(query, []))[:limit]


def test_collect_quality_urls_dedupes_and_respects_cap():
    dummy = _DummySearcher(
        {
            "q1": [
                {"url": "https://example.com/a?utm_source=x", "source": "s1", "query": "q1"},
                {"url": "https://example.com/b", "source": "s1", "query": "q1"},
            ],
            "q2": [
                {"url": "https://example.com/a?utm_source=y", "source": "s2", "query": "q2"},
                {"url": "https://example.com/c", "source": "s2", "query": "q2"},
            ],
            "q3": [{"url": "https://example.com/d", "source": "s3", "query": "q3"}],
        }
    )

    urls, stats = _collect_quality_urls(
        searcher=dummy,
        queries=["q1", "q2", "q3"],
        per_query_limit=3,
        max_total_urls=3,
        include_weixin=True,
        include_overseas=False,
        time_window={"mode": "preset", "preset": "week"},
    )

    assert len(urls) == 3
    assert [item["url"] for item in urls] == [
        "https://example.com/a?utm_source=x",
        "https://example.com/b",
        "https://example.com/c",
    ]
    assert len(stats) == 2
    assert stats[0]["added_urls"] == 2
    assert stats[1]["added_urls"] == 1
    assert [call[0] for call in dummy.calls] == ["q1", "q2"]


def test_quality_payload_attachment_does_not_mutate_hot_terms():
    output_data = {
        "top_keywords": [{"word": "主流程词", "count": 3}],
        "repeated_terms": [{"word": "主流程词", "count": 3, "article_hits": 2}],
    }
    baseline = copy.deepcopy(output_data)
    payload = {
        "enabled": True,
        "executed": True,
        "queries": ["地理数据", "GIS 遥感 地理数据"],
        "selected_articles": [{"title": "高质量文章", "url": "https://example.com/hq"}],
    }

    _attach_quality_search_payload(output_data, payload)

    assert output_data["top_keywords"] == baseline["top_keywords"]
    assert output_data["repeated_terms"] == baseline["repeated_terms"]
    assert output_data["quality_search"]["executed"] is True
