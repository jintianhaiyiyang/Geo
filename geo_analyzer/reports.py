"""CSV/Markdown report generation."""

from __future__ import annotations

import csv
import os
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from .utils import parse_datetime_flexible

DATA_SHARE_KEYWORD = "数据分享"


def _flatten_keywords(matched_type_keywords: Dict[str, Dict[str, int]]) -> List[str]:
    words: List[str] = []
    for _, keyword_map in (matched_type_keywords or {}).items():
        words.extend(keyword_map.keys())
    return sorted(set(words))


def _has_data_share(article: Dict[str, Any]) -> bool:
    keyword_map = article.get("matched_type_keywords", {})
    for _, words in keyword_map.items():
        if DATA_SHARE_KEYWORD in words:
            return True
    return False


def rank_top_articles(articles: List[Dict[str, Any]], top_n: int = 100) -> List[Dict[str, Any]]:
    def sort_key(item: Dict[str, Any]) -> Tuple[Any, ...]:
        publish_dt = parse_datetime_flexible(item.get("publish_time"))
        has_data_share = 0 if _has_data_share(item) else 1
        has_attachment = 0 if bool(item.get("has_attachment")) else 1
        type_hit_count = int(item.get("type_hit_count", 0))
        advanced_score = float(item.get("advanced_score", 0.0))
        attachment_score = float(item.get("attachment_score", 0.0))
        has_publish = 0 if publish_dt else 1
        publish_ts = -(publish_dt.timestamp()) if publish_dt else 0.0
        return (has_data_share, has_attachment, -attachment_score, -type_hit_count, -advanced_score, has_publish, publish_ts)

    ranked = sorted(articles, key=sort_key)
    return ranked[:top_n]


def _build_keyword_stats_from_repeated_terms(repeated_terms: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in repeated_terms:
        keyword = str(item.get("word", "")).strip()
        if not keyword:
            continue
        related_types = item.get("related_types", [])
        if isinstance(related_types, list):
            related_text = ";".join(str(x) for x in related_types if x)
        else:
            related_text = str(related_types or "")

        rows.append(
            {
                "category": str(item.get("category", "")).strip() or "跨文章重复词",
                "keyword": keyword,
                "total_hits": int(item.get("count", 0)),
                "article_hits": int(item.get("article_hits", 0)),
                "related_types": related_text,
            }
        )
    rows.sort(key=lambda row: (-row["article_hits"], -row["total_hits"], row["keyword"]))
    return rows


def _build_keyword_stats_from_categories(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    total_hits: Counter = Counter()
    article_hits: Counter = Counter()
    for article in articles:
        matched = article.get("matched_type_keywords", {}) or {}
        for category, keyword_map in matched.items():
            for keyword, count in keyword_map.items():
                key = (category, keyword)
                total_hits[key] += int(count)
                article_hits[key] += 1

    rows: List[Dict[str, Any]] = []
    for (category, keyword), count in total_hits.items():
        rows.append(
            {
                "category": category,
                "keyword": keyword,
                "total_hits": int(count),
                "article_hits": int(article_hits[(category, keyword)]),
                "related_types": category,
            }
        )
    rows.sort(key=lambda item: (-item["article_hits"], -item["total_hits"], item["keyword"]))
    return rows


def build_keyword_stats(
    articles: List[Dict[str, Any]],
    repeated_terms: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    if repeated_terms:
        return _build_keyword_stats_from_repeated_terms(repeated_terms)
    return _build_keyword_stats_from_categories(articles)


def write_keyword_stats_csv(outdir: str, timestamp: str, keyword_rows: List[Dict[str, Any]]) -> str:
    output_path = os.path.join(outdir, f"keyword_stats_{timestamp}.csv")
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["category", "keyword", "total_hits", "article_hits", "related_types"])
        writer.writeheader()
        for row in keyword_rows:
            writer.writerow(row)
    return output_path


def write_article_stats_top100_csv(outdir: str, timestamp: str, top_articles: List[Dict[str, Any]]) -> str:
    output_path = os.path.join(outdir, f"article_stats_top100_{timestamp}.csv")
    fieldnames = [
        "rank",
        "title",
        "url",
        "publish_time",
        "source",
        "search_query",
        "matched_types",
        "matched_keywords",
        "type_hit_count",
        "advanced_score",
        "has_attachment",
        "attachment_score",
        "content_hash",
    ]
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for idx, article in enumerate(top_articles, 1):
            writer.writerow(
                {
                    "rank": idx,
                    "title": article.get("title", ""),
                    "url": article.get("url", ""),
                    "publish_time": article.get("publish_time", ""),
                    "source": article.get("source", ""),
                    "search_query": article.get("search_query", ""),
                    "matched_types": ";".join(article.get("matched_types", [])),
                    "matched_keywords": ";".join(_flatten_keywords(article.get("matched_type_keywords", {}))),
                    "type_hit_count": int(article.get("type_hit_count", 0)),
                    "advanced_score": float(article.get("advanced_score", 0.0)),
                    "has_attachment": bool(article.get("has_attachment", False)),
                    "attachment_score": float(article.get("attachment_score", 0.0)),
                    "content_hash": article.get("content_hash", ""),
                }
            )
    return output_path


def write_top100_markdown(outdir: str, timestamp: str, top_articles: List[Dict[str, Any]]) -> str:
    output_path = os.path.join(outdir, f"top100_links_{timestamp}.md")
    lines = [
        "# Top100 文章链接（数据分享优先）",
        "",
        f"- 文章总数：{len(top_articles)}",
        "- 排序：数据分享优先 > 附件优先 > 分类命中强度 > advanced_score > 发布时间",
        "",
        "| 排名 | 标题 | 来源 | 发布时间 | 分类 | 关键词 | 分数 | 附件 |",
        "|---:|---|---|---|---|---|---:|---:|",
    ]
    for idx, article in enumerate(top_articles, 1):
        title = str(article.get("title", "")).replace("|", "\\|") or "(无标题)"
        url = article.get("url", "")
        link = f"[{title}]({url})" if url else title
        source = str(article.get("source", "")).replace("|", "\\|")
        publish = str(article.get("publish_time", "")).replace("|", "\\|")
        types = ";".join(article.get("matched_types", []))
        keywords = ";".join(_flatten_keywords(article.get("matched_type_keywords", {})))
        score = float(article.get("advanced_score", 0.0))
        attachment_flag = "Y" if bool(article.get("has_attachment")) else ""
        lines.append(f"| {idx} | {link} | {source} | {publish} | {types} | {keywords} | {score:.2f} | {attachment_flag} |")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return output_path


def generate_reports(
    outdir: str,
    timestamp: str,
    selected_articles: List[Dict[str, Any]],
    repeated_terms: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    top_articles = rank_top_articles(selected_articles, top_n=100)
    keyword_rows = build_keyword_stats(selected_articles, repeated_terms=repeated_terms)
    keyword_csv = write_keyword_stats_csv(outdir, timestamp, keyword_rows)
    top100_csv = write_article_stats_top100_csv(outdir, timestamp, top_articles)
    top100_md = write_top100_markdown(outdir, timestamp, top_articles)
    return {
        "top_articles": top_articles,
        "keyword_rows": keyword_rows,
        "files": {
            "keyword_stats_csv": keyword_csv,
            "article_stats_top100_csv": top100_csv,
            "top100_markdown": top100_md,
        },
    }

