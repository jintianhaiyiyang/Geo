#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""微信/全网地理关键词分析工具 v7.0

示例:
1) 全网搜索并分析（默认异步）
   python geo_keyword_analyzer_v6.5.py --search "GIS 遥感 最新技术" --limit 20

2) 指定同步模式 + requests 后端
   python geo_keyword_analyzer_v6.5.py --search "GIS 遥感" --crawl-mode sync --http-backend requests

3) 使用本地 JSON 进行分析
   python geo_keyword_analyzer_v6.5.py --input raw_crawl.json

4) 运行演示数据
   python geo_keyword_analyzer_v6.5.py --demo
   python geo_keyword_analyzer_v6.5.py --search "GIS 遥感 最新技术" --limit 20 --http-backend curl_cffi --max-retries 4 --request-delay 1.2 --timeout 20 --crawl-timeout 20 --include-undated

5) 使用增强 Stealth 模式（Playwright）
   python geo_keyword_analyzer_v6.5.py --search "GIS 遥感 最新技术" --crawl-mode stealth --stealth-channel chrome --proxy-file proxies.txt --max-concurrency 3
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from geo_analyzer.config import (
    DEFAULT_CRAWL_MODE,
    DEFAULT_HTTP_BACKEND,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MIN_ADVANCED_SCORE,
    DEFAULT_MIN_CONTENT_LENGTH,
    DEFAULT_RECENT_MONTHS,
    DEFAULT_SEARCH_LIMIT,
    DEFAULT_VIZ_FORMAT,
    SUPPORTED_CRAWL_MODES,
    SUPPORTED_EXTRACTORS,
    SUPPORTED_HTTP_BACKENDS,
    SUPPORTED_LOG_LEVELS,
    SUPPORTED_VIZ_FORMATS,
    build_runtime_config,
)
from geo_analyzer.pipeline import run_pipeline
from geo_analyzer.utils import resolve_tls_verify, setup_logging


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="全自动地理关键词分析工具 v7.0")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--search", type=str, help='输入搜索关键词（如 "GIS 遥感"），自动全网检索并分析')
    group.add_argument("--input", type=str, help="指定本地 JSON 文件进行分析")
    group.add_argument("--demo", action="store_true", help="使用内置测试数据演示")

    parser.add_argument("--config", type=str, default=None, help="YAML 配置文件路径")
    parser.add_argument("--outdir", type=str, default=".", help="结果输出目录")
    parser.add_argument("--db-path", type=str, default=None, help="SQLite 持久化路径（默认 <outdir>/geo_monitor_v7.db）")
    parser.add_argument("--no-db-write", action="store_true", help="关闭 SQLite 持久化")
    parser.add_argument("--report-only", action="store_true", help="仅基于数据库中最近成功结果重建报表")
    parser.add_argument("--scrape-only", action="store_true", help="仅执行抓取并写入中间产物/数据库，不做分析和报表")

    parser.add_argument("--limit", type=int, default=None, help=f"搜索/抓取数量限制（默认 {DEFAULT_SEARCH_LIMIT}）")
    parser.add_argument(
        "--recent-months",
        type=int,
        default=None,
        help=f"仅保留最近 N 个月文章（默认 {DEFAULT_RECENT_MONTHS}；0 表示不限制）",
    )
    parser.add_argument("--include-undated", action="store_true", help="时效筛选时保留无发布日期文章（默认剔除）")
    parser.add_argument(
        "--time-preset",
        type=str.lower,
        choices=["today", "week", "month"],
        default=None,
        help="时间窗口预设 today|week|month",
    )
    parser.add_argument("--date-from", type=str, default=None, help="时间窗口起始日期（如 2026-03-01）")
    parser.add_argument("--date-to", type=str, default=None, help="时间窗口结束日期（如 2026-03-15）")
    parser.add_argument(
        "--providers",
        type=str,
        default=None,
        help="搜索来源列表，逗号分隔（如 bing,baidu,wechat,serpapi）",
    )

    parser.add_argument("--no-weixin", action="store_true", help="关闭微信公众号来源（默认开启）")
    parser.add_argument("--no-overseas", action="store_true", help="关闭海外扩展检索（默认开启）")

    parser.add_argument(
        "--crawl-mode",
        type=str.lower,
        choices=sorted(SUPPORTED_CRAWL_MODES),
        default=None,
        help=f"抓取模式 sync|async|stealth（默认 {DEFAULT_CRAWL_MODE}）",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help=f"异步/Stealth 抓取最大并发（默认 {DEFAULT_MAX_CONCURRENCY}）",
    )
    parser.add_argument(
        "--http-backend",
        type=str.lower,
        choices=sorted(SUPPORTED_HTTP_BACKENDS),
        default=None,
        help=f"同步 HTTP 后端（默认 {DEFAULT_HTTP_BACKEND}）",
    )

    parser.add_argument(
        "--extractor",
        type=str.lower,
        choices=sorted(SUPPORTED_EXTRACTORS),
        default=None,
        help="正文提取器 auto|trafilatura|bs4",
    )
    parser.add_argument(
        "--min-content-length",
        type=int,
        default=None,
        help=f"最小正文长度（默认 {DEFAULT_MIN_CONTENT_LENGTH}）",
    )

    parser.add_argument("--no-advanced-filter", action="store_true", help="关闭高级内容筛选（默认开启）")
    parser.add_argument(
        "--min-advanced-score",
        type=int,
        default=None,
        help=f"高级内容最低分（默认 {DEFAULT_MIN_ADVANCED_SCORE}）",
    )

    parser.add_argument(
        "--viz-format",
        type=str.lower,
        choices=sorted(SUPPORTED_VIZ_FORMATS),
        default=None,
        help=f"可视化输出格式（默认 {DEFAULT_VIZ_FORMAT}）",
    )
    parser.add_argument("--no-interactive-viz", action="store_true", help="快捷关闭 HTML 交互可视化")

    parser.add_argument("--log-level", type=str.upper, choices=sorted(SUPPORTED_LOG_LEVELS), default=None, help="日志级别")
    parser.add_argument("--log-file", type=str, default=None, help="日志文件路径")
    parser.add_argument("--insecure", action="store_true", help="关闭 TLS 证书校验（仅调试使用）")
    parser.add_argument("--request-delay", type=float, default=None, help="请求间隔秒数")
    parser.add_argument("--max-retries", type=int, default=None, help="HTTP 最大重试次数")
    parser.add_argument("--stealth-max-retries", type=int, default=None, help="Stealth 模式最大重试次数")
    parser.add_argument("--proxy", action="append", default=None, help="Stealth 代理，可重复传入")
    parser.add_argument("--proxy-file", type=str, default=None, help="Stealth 代理文件路径（每行一个代理）")
    parser.add_argument(
        "--stealth-channel",
        type=str,
        default=None,
        help="Stealth 浏览器通道（如 chrome/msedge；为空则使用 Playwright 默认）",
    )
    parser.add_argument(
        "--stealth-executable-path",
        type=str,
        default=None,
        help="Stealth 浏览器可执行文件路径（与 --stealth-channel 二选一）",
    )
    parser.add_argument("--stealth-headful", action="store_true", help="Stealth 模式启用有头浏览器调试")
    parser.add_argument("--disable-humanize", action="store_true", help="Stealth 模式关闭真人行为模拟")
    parser.add_argument(
        "--disable-stealth-plugin",
        action="store_true",
        help="Stealth 模式关闭 playwright-stealth 插件",
    )
    parser.add_argument("--timeout", type=int, default=None, help="搜索请求超时秒数")
    parser.add_argument("--crawl-timeout", type=int, default=None, help="抓取请求超时秒数")
    parser.add_argument("--no-progress", action="store_true", help="关闭抓取进度条")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    effective_argv = argv if argv is not None else sys.argv[1:]
    args = parser.parse_args(effective_argv)
    if args.report_only and args.scrape_only:
        print("参数冲突: --report-only 与 --scrape-only 不能同时使用", file=sys.stderr)
        return 2

    try:
        config = build_runtime_config(args)
    except Exception as exc:
        print(f"配置错误: {exc}", file=sys.stderr)
        return 1

    logger = setup_logging(config["logging"]["level"], config["logging"]["file"])

    if not effective_argv:
        logger.info("欢迎使用地理关键词分析器 v7.0")
        logger.info('1. 全网搜索: python geo_keyword_analyzer_v6.5.py --search "GIS 遥感技术"')
        logger.info("2. 运行演示: python geo_keyword_analyzer_v6.5.py --demo")
        logger.info("3. 使用配置: python geo_keyword_analyzer_v6.5.py --config config.example.yaml --search \"GIS\"")
        logger.info(
            "4. Stealth模式: python geo_keyword_analyzer_v6.5.py --search \"GIS\" --crawl-mode stealth --stealth-channel chrome --proxy-file proxies.txt"
        )
        return 0

    verify_option = resolve_tls_verify(config["security"]["verify_tls"], args.insecure, logger)
    try:
        return run_pipeline(args, config, logger, verify_option)
    except Exception as exc:
        logger.exception("运行失败: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
