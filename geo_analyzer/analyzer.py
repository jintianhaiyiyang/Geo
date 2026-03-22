"""Semantic analyzer and recency filter."""

from __future__ import annotations

import json
import logging
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    import jieba  # type: ignore
except ImportError:  # pragma: no cover
    jieba = None

from .matcher import KeywordMatcher
from .utils import parse_datetime_flexible

_EN_TERM_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+\-]{1,31}")
_CN_TERM_RE = re.compile(r"[\u4e00-\u9fff]{2,12}")
_ASCII_TERM_RE = re.compile(r"^[A-Za-z0-9_+\-]+$")

_DEFAULT_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "this",
    "that",
    "from",
    "http",
    "https",
    "www",
    "com",
    "文章",
    "我们",
    "你们",
    "他们",
    "这个",
    "那个",
    "可以",
    "以及",
    "通过",
    "相关",
    "内容",
    "发布",
    "最新",
    "更多",
    "其中",
    "如果",
    "但是",
    "已经",
    "没有",
    "一种",
    "一个",
}


def filter_articles_by_recency(
    articles: List[Dict[str, Any]],
    recent_months: int,
    include_undated: bool,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    if recent_months <= 0:
        return articles

    log = logger or logging.getLogger("geo_analyzer.recency")
    cutoff = datetime.now() - timedelta(days=recent_months * 30)

    kept: List[Dict[str, Any]] = []
    outdated = 0
    undated = 0

    for article in articles:
        publish_dt = parse_datetime_flexible(article.get("publish_time"))
        if publish_dt is None:
            undated += 1
            if include_undated:
                kept.append(article)
            continue

        article["publish_time"] = publish_dt.strftime("%Y-%m-%d %H:%M:%S")
        if publish_dt >= cutoff:
            kept.append(article)
        else:
            outdated += 1

    log.info(
        "Recency filter(last %d months, cutoff=%s): kept=%d outdated=%d undated=%d include_undated=%s",
        recent_months,
        cutoff.strftime("%Y-%m-%d"),
        len(kept),
        outdated,
        undated,
        include_undated,
    )
    return kept


class GeoKeywordAnalyzer:
    def __init__(
        self,
        min_relevance_score: int,
        advanced_only: bool,
        min_advanced_score: int,
        top_keywords_count: int,
        nebula_max_words: int,
        core_identifiers: List[str],
        marker_weights: Dict[str, float],
        keyword_categories: Dict[str, List[str]],
        repeated_term_min_article_hits: int = 2,
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger("geo_analyzer.analysis")
        self.min_relevance_score = min_relevance_score
        self.advanced_only = advanced_only
        self.min_advanced_score = min_advanced_score
        self.top_keywords_count = top_keywords_count
        self.nebula_max_words = nebula_max_words
        self.repeated_term_min_article_hits = max(1, int(repeated_term_min_article_hits))

        self.core_identifiers = [item for item in core_identifiers if item]
        self.marker_weights = {key: float(value) for key, value in marker_weights.items() if key}
        self.keyword_categories = keyword_categories

        self.core_matcher = KeywordMatcher(self.core_identifiers, ignore_case=True)
        self.marker_matcher = KeywordMatcher(self.marker_weights.keys(), ignore_case=True)

        all_category_terms: List[str] = []
        keyword_to_categories: Dict[str, List[str]] = defaultdict(list)
        for category, words in self.keyword_categories.items():
            for word in words:
                if not word:
                    continue
                all_category_terms.append(word)
                keyword_to_categories[word].append(category)
        self.keyword_matcher = KeywordMatcher(all_category_terms, ignore_case=True)
        self.keyword_to_categories = dict(keyword_to_categories)

        preserved_terms = set(all_category_terms)
        preserved_terms.update(self.marker_weights.keys())
        preserved_terms.update(self.core_identifiers)
        self.preserved_terms = {term for term in preserved_terms if term}
        self.term_stopwords = set(_DEFAULT_STOPWORDS)

    def is_relevant(self, text: str) -> bool:
        if self.min_relevance_score <= 0:
            return True
        hits = self.core_matcher.count(text)
        score = len(hits)
        return score >= self.min_relevance_score

    def calc_advanced_score(self, text: str) -> Tuple[float, List[Dict[str, Any]]]:
        marker_hits = self.marker_matcher.count(text)
        evidence: List[Dict[str, Any]] = []
        score = 0.0
        for marker, hits in marker_hits.items():
            weight = self.marker_weights.get(marker, 0.0)
            if hits <= 0 or weight == 0:
                continue
            contribution = hits * weight
            score += contribution
            evidence.append(
                {
                    "marker": marker,
                    "hits": int(hits),
                    "weight": float(weight),
                    "contribution": float(contribution),
                }
            )
        evidence.sort(key=lambda item: abs(item["contribution"]), reverse=True)
        return score, evidence

    def _normalize_term(self, token: str) -> str:
        normalized = str(token or "").strip()
        if not normalized:
            return ""
        if _ASCII_TERM_RE.match(normalized):
            normalized = normalized.lower()
        return normalized

    def _is_valid_term(self, term: str) -> bool:
        if not term:
            return False
        if term in self.preserved_terms:
            return True
        if len(term) < 2:
            return False
        if len(term) > 24:
            return False
        if term.isdigit():
            return False
        if term in self.term_stopwords:
            return False
        if term.startswith("http"):
            return False
        has_cjk = bool(_CN_TERM_RE.search(term))
        has_ascii = bool(_EN_TERM_RE.search(term))
        return has_cjk or has_ascii

    def _extract_candidate_terms(self, text: str) -> List[str]:
        terms: List[str] = []
        if not text:
            return terms

        if jieba is not None:
            for raw in jieba.cut(text, cut_all=False):
                term = self._normalize_term(raw)
                if self._is_valid_term(term):
                    terms.append(term)
        else:
            for raw in _CN_TERM_RE.findall(text):
                term = self._normalize_term(raw)
                if self._is_valid_term(term):
                    terms.append(term)
            for raw in _EN_TERM_RE.findall(text):
                term = self._normalize_term(raw)
                if self._is_valid_term(term):
                    terms.append(term)

        return terms

    def _resolve_related_types(
        self,
        term: str,
        type_context_counter: Counter,
    ) -> List[str]:
        ordered: List[str] = []
        explicit_types = self.keyword_to_categories.get(term, [])
        for category in explicit_types:
            if category not in ordered:
                ordered.append(category)

        scored_context = []
        for category in self.keyword_categories:
            hits = int(type_context_counter.get((term, category), 0))
            if hits > 0:
                scored_context.append((category, hits))
        scored_context.sort(key=lambda item: (-item[1], item[0]))

        for category, _ in scored_context:
            if category not in ordered:
                ordered.append(category)
        return ordered

    def _build_repeated_term_records(
        self,
        total_hits: Counter,
        article_hits: Counter,
        type_context_counter: Counter,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        min_article_hits = self.repeated_term_min_article_hits

        for term, count in total_hits.items():
            article_hit_count = int(article_hits.get(term, 0))
            if article_hit_count < min_article_hits:
                continue
            related_types = self._resolve_related_types(term, type_context_counter)
            records.append(
                {
                    "word": term,
                    "count": int(count),
                    "article_hits": article_hit_count,
                    "category": related_types[0] if related_types else "跨文章重复词",
                    "related_types": related_types,
                }
            )

        # Fallback for tiny samples where no term reaches the repeated threshold.
        if not records:
            for term, count in total_hits.items():
                article_hit_count = int(article_hits.get(term, 0))
                if article_hit_count <= 0:
                    continue
                related_types = self._resolve_related_types(term, type_context_counter)
                records.append(
                    {
                        "word": term,
                        "count": int(count),
                        "article_hits": article_hit_count,
                        "category": related_types[0] if related_types else "跨文章重复词",
                        "related_types": related_types,
                    }
                )

        records.sort(key=lambda item: (-item["article_hits"], -item["count"], item["word"]))
        return records

    def analyze(self, articles: List[Dict[str, Any]], outdir: str, extra_meta: Optional[Dict] = None) -> Optional[Dict]:
        self.logger.info("Running semantic analysis...")

        valid_articles: List[Dict[str, Any]] = []
        filtered_non_advanced = 0

        keyword_total_hits: Counter = Counter()
        keyword_article_hits: Counter = Counter()
        category_details: Dict[str, Counter] = {category: Counter() for category in self.keyword_categories}

        repeated_total_hits: Counter = Counter()
        repeated_article_hits: Counter = Counter()
        term_type_context: Counter = Counter()

        for article in articles:
            text = f"{article.get('title', '')} {article.get('content', '')}"
            if not self.is_relevant(text):
                continue

            advanced_score, evidence = self.calc_advanced_score(text)
            article["advanced_score"] = round(float(advanced_score), 6)
            article["advanced_score_evidence"] = evidence

            if self.advanced_only and advanced_score < self.min_advanced_score:
                filtered_non_advanced += 1
                continue

            keyword_hits = self.keyword_matcher.count(text)
            matched_type_keywords: Dict[str, Dict[str, int]] = {}
            type_hit_count = 0

            for keyword, count in keyword_hits.items():
                categories = self.keyword_to_categories.get(keyword, [])
                if not categories or count <= 0:
                    continue
                for category in categories:
                    matched_type_keywords.setdefault(category, {})
                    matched_type_keywords[category][keyword] = int(count)
                    category_details[category][keyword] += int(count)
                    keyword_total_hits[(category, keyword)] += int(count)
                    type_hit_count += int(count)

            for category, keyword_map in matched_type_keywords.items():
                for keyword in keyword_map:
                    keyword_article_hits[(category, keyword)] += 1

            matched_types = sorted(
                matched_type_keywords.keys(),
                key=lambda cat: sum(matched_type_keywords[cat].values()),
                reverse=True,
            )

            term_counts = Counter(self._extract_candidate_terms(text))
            for keyword, count in keyword_hits.items():
                if count <= 0:
                    continue
                # Keep exact matcher count for configured keywords.
                term_counts[keyword] = max(int(count), int(term_counts.get(keyword, 0)))

            for term, hits in term_counts.items():
                if hits <= 0:
                    continue
                repeated_total_hits[term] += int(hits)
                repeated_article_hits[term] += 1

                explicit_types = self.keyword_to_categories.get(term, [])
                if explicit_types:
                    for category in explicit_types:
                        term_type_context[(term, category)] += 1
                else:
                    for category in matched_types:
                        term_type_context[(term, category)] += 1

            normalized_article = {
                "title": article.get("title", ""),
                "url": article.get("url", ""),
                "normalized_url": article.get("normalized_url", ""),
                "source": article.get("source", ""),
                "search_query": article.get("search_query", ""),
                "publish_time": article.get("publish_time", ""),
                "advanced_score": article.get("advanced_score", 0),
                "advanced_score_evidence": article.get("advanced_score_evidence", []),
                "extractor": article.get("extractor", ""),
                "http_backend": article.get("http_backend", ""),
                "content_hash": article.get("content_hash", ""),
                "has_attachment": bool(article.get("has_attachment", False)),
                "attachment_score": float(article.get("attachment_score", 0.0) or 0.0),
                "attachment_evidence": article.get("attachment_evidence", []),
                "matched_types": matched_types,
                "matched_type_keywords": matched_type_keywords,
                "type_hit_count": type_hit_count,
            }
            valid_articles.append(normalized_article)

        if not valid_articles:
            self.logger.warning("No strongly relevant articles found after filtering.")
            return None

        self.logger.info("Valid articles: %d / %d", len(valid_articles), len(articles))
        if self.advanced_only:
            self.logger.info(
                "Advanced filter enabled (threshold=%d), removed=%d",
                self.min_advanced_score,
                filtered_non_advanced,
            )

        repeated_terms_records = self._build_repeated_term_records(
            repeated_total_hits,
            repeated_article_hits,
            term_type_context,
        )

        category_details_serialized: Dict[str, Dict[str, int]] = {}
        for category, counter in category_details.items():
            sorted_items = sorted(counter.items(), key=lambda item: item[1], reverse=True)
            category_details_serialized[category] = {word: int(count) for word, count in sorted_items}

        top_keywords_records = repeated_terms_records[: self.top_keywords_count]
        all_keywords: List[Tuple[str, int, str]] = [
            (item["word"], int(item["count"]), item["category"]) for item in repeated_terms_records
        ]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file = os.path.join(outdir, f"geo_analysis_result_{timestamp}.json")

        meta = {
            "timestamp": timestamp,
            "total_articles": len(valid_articles),
            "advanced_only": self.advanced_only,
            "min_advanced_score": self.min_advanced_score,
            "repeated_term_min_article_hits": self.repeated_term_min_article_hits,
        }
        if extra_meta:
            meta.update(extra_meta)

        output_data = {
            "meta": meta,
            "top_keywords": top_keywords_records,
            "repeated_terms": repeated_terms_records,
            "selected_articles": valid_articles,
            "category_details": category_details_serialized,
        }

        with open(result_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        self._print_report(repeated_terms_records[:20], category_details_serialized)
        return {
            "keywords": all_keywords,
            "timestamp": timestamp,
            "result_file": result_file,
            "output_data": output_data,
            "selected_articles": valid_articles,
        }

    def _print_report(self, top_keywords: List[Dict[str, Any]], stats: Dict[str, Dict[str, int]]) -> None:
        lines = []
        lines.append("=" * 50)
        lines.append("Geo Keyword Analyzer Summary")
        lines.append("=" * 50)
        lines.append("Top repeated terms across articles")
        for i, item in enumerate(top_keywords[:15], 1):
            lines.append(
                f" {i:2d}. {item['word']:<16} ({item['article_hits']} articles / {item['count']} hits) [{item['category']}]"
            )
        lines.append("")
        lines.append("Category coverage")
        for category, keywords in stats.items():
            lines.append(f" - {category}: {sum(keywords.values())}")
        lines.append("=" * 50)
        self.logger.info("\n%s", "\n".join(lines))
