#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微信公众号文章批量抓取器
========================
支持两种模式:

模式A — 关键词多页搜索（搜狗微信，翻页爬取）
    python wechat_mass_crawler.py keyword --keywords "GIS 遥感" "地理信息" --pages 30

模式B — 公众号历史文章（指定账号名称，爬取全部历史）
    python wechat_mass_crawler.py account --accounts "地理研究" "遥感与GIS" --max 500

两种模式都支持并发抓取文章正文，输出 articles.json + links.txt。

依赖: pip install requests beautifulsoup4 aiohttp tqdm
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import (
    parse_qs, parse_qsl, quote_plus, urlencode,
    urljoin, urlsplit, urlunsplit,
)

import requests
from bs4 import BeautifulSoup

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

try:
    import trafilatura
    HAS_TRAFILATURA = True
except ImportError:
    HAS_TRAFILATURA = False

# ──────────────────────────────────────────────
# 常量
# ──────────────────────────────────────────────
SOGOU_ARTICLE_SEARCH   = "https://weixin.sogou.com/weixin"   # type=2: 文章搜索
SOGOU_ACCOUNT_SEARCH   = "https://weixin.sogou.com/weixin"   # type=1: 账号搜索
MP_PROFILE_EXT         = "https://mp.weixin.qq.com/mp/profile_ext"  # 账号历史文章 API
WECHAT_HOST            = "mp.weixin.qq.com"
WECHAT_REQUIRED_KEYS   = ("__biz", "mid", "idx")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


# ──────────────────────────────────────────────
# 数据类
# ──────────────────────────────────────────────
@dataclass
class ArticleRef:
    """URL + 从搜索页采集到的元数据（正文还没抓）"""
    url: str
    title: str = ""
    account_name: str = ""
    publish_time: str = ""
    summary: str = ""
    source: str = ""
    query: str = ""


@dataclass
class Article:
    """含正文的完整文章"""
    url: str
    title: str = ""
    account_name: str = ""
    publish_time: str = ""
    content: str = ""
    summary: str = ""
    source: str = ""
    query: str = ""
    crawl_status: str = "pending"   # ok / failed / skip
    error: str = ""


# ──────────────────────────────────────────────
# 工具函数
# ──────────────────────────────────────────────
def rand_ua() -> str:
    return random.choice(USER_AGENTS)


def base_headers(host: str = "", referer: str = "") -> Dict[str, str]:
    h = {
        "User-Agent": rand_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }
    if host:
        h["Host"] = host
    if referer:
        h["Referer"] = referer
    return h


def jitter(base: float, lo: float = 0.7, hi: float = 1.8) -> float:
    return random.uniform(base * lo, base * hi)


def is_valid_wechat_url(url: str) -> bool:
    if not url:
        return False
    try:
        p = urlsplit(url)
    except Exception:
        return False
    if (p.hostname or "").lower() != WECHAT_HOST:
        return False
    if not p.path.startswith("/s"):
        return False
    qm = parse_qs(p.query)
    return all(qm.get(k) for k in WECHAT_REQUIRED_KEYS)


def normalize_wechat_url(url: str) -> str:
    """只保留 __biz/mid/idx/sn，去掉 chksm 等噪声参数"""
    try:
        p = urlsplit(url)
    except Exception:
        return url
    pairs = parse_qsl(p.query, keep_blank_values=False)
    kept = [(k, v) for k, v in pairs if k in ("__biz", "mid", "idx", "sn")]
    return urlunsplit(("https", WECHAT_HOST, "/s", urlencode(kept), ""))


def extract_wechat_urls_from_html(html: str) -> List[str]:
    urls: List[str] = []
    urls += re.findall(r"https?://mp\.weixin\.qq\.com/s\?[^\s\"'<>]+", html)
    urls += [f"https://{m}" for m in re.findall(r"mp\.weixin\.qq\.com/s\?[^\s\"'<>]+", html)]
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a[href*='mp.weixin.qq.com']"):
        h = a.get("href", "")
        if h:
            urls.append(h)
    return list(dict.fromkeys(urls))  # 保序去重


def extract_text(html_body: str, url: str = "") -> str:
    """从 HTML 提取正文，优先 trafilatura，降级 bs4"""
    if HAS_TRAFILATURA:
        text = trafilatura.extract(
            html_body, include_comments=False,
            include_tables=False, favor_recall=True,
        )
        if text and len(text) > 100:
            return text
    # bs4 fallback: 取 <div id="js_content"> 或整个 body
    soup = BeautifulSoup(html_body, "html.parser")
    target = soup.find("div", id="js_content") or soup.find("body")
    if target:
        return target.get_text(separator="\n", strip=True)
    return ""


def parse_publish_time(soup: BeautifulSoup) -> str:
    """从微信文章页解析发布时间"""
    # 方式1: <em id="publish_time">
    tag = soup.find(id="publish_time")
    if tag:
        return tag.get_text(strip=True)
    # 方式2: script 里的 ct 变量（unix timestamp）
    m = re.search(r'var\s+ct\s*=\s*"?(\d{10})"?', str(soup))
    if m:
        try:
            return datetime.fromtimestamp(int(m.group(1))).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return ""


def parse_article_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1", id="activity-name") or soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""


def parse_account_name(soup: BeautifulSoup) -> str:
    tag = soup.find(id="js_name") or soup.find(class_="account_nickname_inner")
    return tag.get_text(strip=True) if tag else ""


# ──────────────────────────────────────────────
# Sogou 搜索层
# ──────────────────────────────────────────────
class SogouScraper:
    """搜狗微信文章/账号搜索，支持翻页"""

    def __init__(
        self,
        session: requests.Session,
        delay: float = 2.0,
        logger: Optional[logging.Logger] = None,
    ):
        self.session = session
        self.delay = delay
        self.log = logger or logging.getLogger("sogou")

    def _get(self, url: str, params: Dict = {}, host: str = "weixin.sogou.com") -> Optional[requests.Response]:
        try:
            resp = self.session.get(
                url, params=params,
                headers=base_headers(host=host, referer="https://weixin.sogou.com/"),
                timeout=15, allow_redirects=True,
            )
            return resp
        except Exception as e:
            self.log.warning("GET failed [%s]: %s", url, e)
            return None

    def _sleep(self) -> None:
        time.sleep(jitter(self.delay))

    # ── 模式A: 关键词搜索（多页） ──
    def search_articles_paged(
        self,
        keyword: str,
        max_pages: int = 10,
    ) -> List[ArticleRef]:
        """搜狗微信文章搜索，翻页采集所有 URL"""
        refs: List[ArticleRef] = []
        seen: Set[str] = set()

        for page in range(1, max_pages + 1):
            self.log.info("  关键词[%s] 第 %d/%d 页", keyword, page, max_pages)
            params = {
                "type": "2",
                "query": keyword,
                "page": str(page),
                "ie": "utf8",
            }
            resp = self._get(SOGOU_ARTICLE_SEARCH, params=params)
            self._sleep()

            if resp is None or resp.status_code != 200:
                self.log.warning("  搜狗返回异常 status=%s，停止翻页",
                                 resp.status_code if resp else "N/A")
                break

            # 搜狗反爬：出现验证码页时停止
            if "请输入验证码" in resp.text or "captcha" in resp.url.lower():
                self.log.warning("  搜狗触发验证码，停止翻页（已收集 %d 条）", len(refs))
                break

            new_refs = self._parse_article_list_page(resp.text, keyword)
            if not new_refs:
                self.log.info("  第 %d 页无新结果，搜索结束", page)
                break

            added = 0
            for ref in new_refs:
                key = normalize_wechat_url(ref.url)
                if key not in seen:
                    seen.add(key)
                    refs.append(ref)
                    added += 1

            self.log.info("  第 %d 页新增 %d 条，累计 %d 条", page, added, len(refs))

            # 如果这一页一条新的都没有，提前退出
            if added == 0:
                break

        return refs

    def _parse_article_list_page(self, html: str, keyword: str) -> List[ArticleRef]:
        """解析搜狗微信文章搜索结果页"""
        refs: List[ArticleRef] = []
        soup = BeautifulSoup(html, "html.parser")

        # 先尝试直接提取 mp.weixin.qq.com 链接
        for url in extract_wechat_urls_from_html(html):
            if is_valid_wechat_url(url):
                refs.append(ArticleRef(url=url, source="sogou_search", query=keyword))

        # 再从结果卡片里补充标题、账号、摘要
        for card in soup.select(".news-list li, .txt-box"):
            h3 = card.find("h3")
            title = h3.get_text(strip=True) if h3 else ""
            a = h3.find("a") if h3 else card.find("a")
            href = a.get("href", "") if a else ""

            account_tag = card.find(class_="account") or card.find(class_="s-p")
            account = account_tag.get_text(strip=True) if account_tag else ""

            time_tag = card.find(class_="s-p") or card.find("span", attrs={"data-key": True})
            pub_time = time_tag.get_text(strip=True) if time_tag else ""

            summary_tag = card.find(class_="txt-info") or card.find("p")
            summary = summary_tag.get_text(strip=True) if summary_tag else ""

            # href 可能是 sogou 跳转链接，尝试解析出真实 wx 链接
            real_url = self._resolve_sogou_link(href) if href else ""
            if real_url and is_valid_wechat_url(real_url):
                # 更新已有 ref 的元数据，或新增
                matched = next((r for r in refs if normalize_wechat_url(r.url) == normalize_wechat_url(real_url)), None)
                if matched:
                    if title:
                        matched.title = title
                    if account:
                        matched.account_name = account
                    if pub_time:
                        matched.publish_time = pub_time
                    if summary:
                        matched.summary = summary
                else:
                    refs.append(ArticleRef(
                        url=real_url, title=title, account_name=account,
                        publish_time=pub_time, summary=summary,
                        source="sogou_search", query=keyword,
                    ))

        return refs

    def _resolve_sogou_link(self, href: str) -> str:
        """把 sogou 跳转链接解析成真实微信链接（优先不发请求）"""
        if not href:
            return ""
        if href.startswith("/link?url="):
            href = f"https://weixin.sogou.com{href}"

        # 从 query string 里提取 url= 参数
        try:
            p = urlsplit(href)
            qm = parse_qs(p.query)
            for k in ("url", "u"):
                for v in qm.get(k, []):
                    if "mp.weixin.qq.com" in v:
                        return v
        except Exception:
            pass

        # 发一次请求跟随跳转
        if "weixin.sogou.com" in href:
            resp = self._get(href)
            self._sleep()
            if resp is not None:
                # 跳转后的最终 URL
                if is_valid_wechat_url(resp.url):
                    return resp.url
                # 页面里找
                for u in extract_wechat_urls_from_html(resp.text):
                    if is_valid_wechat_url(u):
                        return u
        return ""

    # ── 模式B: 公众号账号 → 文章列表 ──
    def find_account_biz(self, account_name: str) -> Tuple[str, str]:
        """在搜狗搜账号，返回 (__biz, profile_url)"""
        params = {"type": "1", "query": account_name, "ie": "utf8"}
        resp = self._get(SOGOU_ACCOUNT_SEARCH, params=params)
        self._sleep()
        if resp is None or resp.status_code != 200:
            return "", ""

        soup = BeautifulSoup(resp.text, "html.parser")
        for card in soup.select(".account-list li, .account_res_wrap"):
            # 账号主页链接包含 __biz
            profile_a = card.find("a", href=re.compile(r"__biz="))
            if profile_a:
                profile_url = profile_a.get("href", "")
                m = re.search(r"__biz=([^&]+)", profile_url)
                biz = m.group(1) if m else ""
                if biz:
                    return biz, profile_url
        return "", ""

    def get_account_article_refs(
        self,
        account_name: str,
        biz: str = "",
        profile_url: str = "",
        max_articles: int = 500,
    ) -> List[ArticleRef]:
        """
        通过搜狗账号主页翻页，获取公众号历史文章列表。
        如果没有 __biz，先搜索账号。
        """
        if not biz or not profile_url:
            self.log.info("搜索账号 [%s] 的 __biz ...", account_name)
            biz, profile_url = self.find_account_biz(account_name)
            if not biz:
                self.log.warning("未找到账号 [%s]，跳过", account_name)
                return []
            self.log.info("找到 __biz=%s", biz)

        refs: List[ArticleRef] = []
        seen: Set[str] = set()
        offset = 0
        page_size = 10
        fail_count = 0

        while len(refs) < max_articles:
            self.log.info("  [%s] offset=%d 已采集=%d", account_name, offset, len(refs))
            page_refs = self._fetch_account_page(
                biz=biz,
                profile_url=profile_url,
                account_name=account_name,
                offset=offset,
            )
            self._sleep()

            if not page_refs:
                fail_count += 1
                if fail_count >= 3:
                    self.log.info("  连续 3 次无结果，停止")
                    break
                continue

            fail_count = 0
            added = 0
            for ref in page_refs:
                key = normalize_wechat_url(ref.url)
                if key not in seen:
                    seen.add(key)
                    refs.append(ref)
                    added += 1

            if added == 0:
                self.log.info("  无新文章，停止翻页")
                break

            offset += page_size

        self.log.info("账号 [%s] 共采集 %d 篇文章链接", account_name, len(refs))
        return refs

    def _fetch_account_page(
        self,
        biz: str,
        profile_url: str,
        account_name: str,
        offset: int,
    ) -> List[ArticleRef]:
        """抓取搜狗账号主页的文章列表（一页 10 条）"""
        # 拼接带 offset 的 profile 页面 URL
        if "?" in profile_url:
            page_url = f"{profile_url}&offset={offset}"
        else:
            page_url = f"{profile_url}?offset={offset}"

        resp = self._get(page_url, host="mp.weixin.qq.com")
        if resp is None or resp.status_code != 200:
            # 降级：直接构造 MP API 请求（需要 cookie，可能失败）
            return self._fetch_mp_profile_ext(biz, account_name, offset)

        refs: List[ArticleRef] = []
        for url in extract_wechat_urls_from_html(resp.text):
            if is_valid_wechat_url(url):
                refs.append(ArticleRef(
                    url=url, account_name=account_name,
                    source="account_page",
                ))

        # 如果页面没有文章（可能是账号简介页），尝试 MP API
        if not refs:
            refs = self._fetch_mp_profile_ext(biz, account_name, offset)

        return refs

    def _fetch_mp_profile_ext(
        self,
        biz: str,
        account_name: str,
        offset: int,
    ) -> List[ArticleRef]:
        """
        微信 MP 历史文章 JSON API（getmsg）。
        无 cookie 时只能拿到部分数据或被 302 重定向到登录。
        有 cookie 时可完整翻页（把 cookie 设在 session 里即可）。
        """
        params = {
            "action": "getmsg",
            "__biz": biz,
            "f": "json",
            "offset": str(offset),
            "count": "10",
            "is_ok": "1",
        }
        try:
            resp = self.session.get(
                MP_PROFILE_EXT, params=params,
                headers=base_headers(host=WECHAT_HOST, referer="https://mp.weixin.qq.com/"),
                timeout=15,
            )
        except Exception as e:
            self.log.warning("MP profile_ext failed: %s", e)
            return []

        if resp.status_code != 200:
            return []

        try:
            data = resp.json()
        except Exception:
            return []

        # 正常响应结构: {"ret": 0, "msg_list": [...]}
        ret = data.get("ret", -1)
        if ret != 0:
            self.log.warning("MP profile_ext ret=%s (可能需要 cookie 登录)", ret)
            return []

        refs: List[ArticleRef] = []
        for msg in data.get("msg_list", []) or []:
            app_msg = msg.get("app_msg_ext_info") or {}
            url = app_msg.get("content_url", "")
            if not url:
                continue
            url = url.replace("\\", "").replace("http://", "https://")
            if not is_valid_wechat_url(url):
                continue
            refs.append(ArticleRef(
                url=url,
                title=app_msg.get("title", ""),
                account_name=account_name,
                publish_time=str(msg.get("datetime", "")),
                summary=app_msg.get("digest", ""),
                source="mp_api",
            ))

        return refs


# ──────────────────────────────────────────────
# 正文抓取层（异步）
# ──────────────────────────────────────────────
class ArticleCrawler:
    """异步并发抓取文章正文"""

    def __init__(
        self,
        concurrency: int = 5,
        delay: float = 1.5,
        timeout: int = 20,
        logger: Optional[logging.Logger] = None,
    ):
        self.concurrency = concurrency
        self.delay = delay
        self.timeout = timeout
        self.log = logger or logging.getLogger("crawler")

    async def crawl_all(self, refs: List[ArticleRef]) -> List[Article]:
        if not HAS_AIOHTTP:
            self.log.warning("aiohttp 未安装，降级为同步抓取")
            return self._crawl_sync(refs)

        sem = asyncio.Semaphore(self.concurrency)
        connector = aiohttp.TCPConnector(ssl=False, limit=self.concurrency * 2)
        timeout_cfg = aiohttp.ClientTimeout(total=self.timeout)

        results: List[Article] = []
        bar = tqdm(total=len(refs), desc="抓取正文", unit="篇") if HAS_TQDM else None

        async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
            tasks = [self._fetch_one(session, sem, ref) for ref in refs]
            for coro in asyncio.as_completed(tasks):
                article = await coro
                results.append(article)
                if bar:
                    bar.update(1)

        if bar:
            bar.close()

        return results

    async def _fetch_one(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        ref: ArticleRef,
    ) -> Article:
        article = Article(
            url=ref.url, title=ref.title, account_name=ref.account_name,
            publish_time=ref.publish_time, summary=ref.summary,
            source=ref.source, query=ref.query,
        )
        async with sem:
            await asyncio.sleep(jitter(self.delay))
            headers = base_headers(host=WECHAT_HOST, referer="https://mp.weixin.qq.com/")
            try:
                async with session.get(ref.url, headers=headers, allow_redirects=True) as resp:
                    if resp.status != 200:
                        article.crawl_status = "failed"
                        article.error = f"HTTP {resp.status}"
                        return article
                    html_body = await resp.text(encoding="utf-8", errors="replace")
            except Exception as e:
                article.crawl_status = "failed"
                article.error = str(e)
                return article

        soup = BeautifulSoup(html_body, "html.parser")

        # 检测"该内容已被删除 / 此内容因违规无法查看"
        page_text = soup.get_text()
        if any(kw in page_text for kw in ("该内容已被删除", "此内容因违规", "内容不存在")):
            article.crawl_status = "skip"
            article.error = "article_deleted"
            return article

        if not article.title:
            article.title = parse_article_title(soup)
        if not article.account_name:
            article.account_name = parse_account_name(soup)
        if not article.publish_time:
            article.publish_time = parse_publish_time(soup)

        article.content = extract_text(html_body, ref.url)
        article.crawl_status = "ok"
        return article

    def _crawl_sync(self, refs: List[ArticleRef]) -> List[Article]:
        """同步降级（无 aiohttp 时）"""
        articles: List[Article] = []
        bar = tqdm(refs, desc="抓取正文（同步）", unit="篇") if HAS_TQDM else refs
        for ref in bar:
            art = Article(
                url=ref.url, title=ref.title, account_name=ref.account_name,
                publish_time=ref.publish_time, summary=ref.summary,
                source=ref.source, query=ref.query,
            )
            try:
                resp = requests.get(
                    ref.url,
                    headers=base_headers(host=WECHAT_HOST, referer="https://mp.weixin.qq.com/"),
                    timeout=20, allow_redirects=True,
                )
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    art.content = extract_text(resp.text, ref.url)
                    if not art.title:
                        art.title = parse_article_title(soup)
                    if not art.account_name:
                        art.account_name = parse_account_name(soup)
                    if not art.publish_time:
                        art.publish_time = parse_publish_time(soup)
                    art.crawl_status = "ok"
                else:
                    art.crawl_status = "failed"
                    art.error = f"HTTP {resp.status_code}"
            except Exception as e:
                art.crawl_status = "failed"
                art.error = str(e)
            time.sleep(jitter(1.5))
            articles.append(art)
        return articles


# ──────────────────────────────────────────────
# 输出
# ──────────────────────────────────────────────
def save_results(articles: List[Article], outdir: Path, logger: logging.Logger) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 完整 JSON
    json_path = outdir / f"articles_{ts}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump([asdict(a) for a in articles], f, ensure_ascii=False, indent=2)
    logger.info("JSON 已保存: %s (%d 篇)", json_path, len(articles))

    # 链接列表
    links_path = outdir / f"links_{ts}.txt"
    with open(links_path, "w", encoding="utf-8") as f:
        for a in articles:
            f.write(f"{a.url}\t{a.title}\t{a.account_name}\t{a.publish_time}\n")
    logger.info("链接列表已保存: %s", links_path)

    # 统计
    ok = sum(1 for a in articles if a.crawl_status == "ok")
    failed = sum(1 for a in articles if a.crawl_status == "failed")
    skipped = sum(1 for a in articles if a.crawl_status == "skip")
    logger.info("完成: 成功=%d 失败=%d 跳过=%d 合计=%d", ok, failed, skipped, len(articles))


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────
def setup_logging(level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger("wechat_mass")


def build_session(cookies_str: str = "") -> requests.Session:
    """
    建立 requests session，可选注入 cookie（用于 mp_profile_ext API）。
    cookie 格式: "key1=val1; key2=val2"
    """
    session = requests.Session()
    if cookies_str:
        for part in cookies_str.split(";"):
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                session.cookies.set(k.strip(), v.strip(), domain=".weixin.qq.com")
    return session


def mode_keyword(args, session: requests.Session, logger: logging.Logger) -> List[ArticleRef]:
    """模式A: 多关键词 × 多页翻页"""
    scraper = SogouScraper(session, delay=args.delay, logger=logger.getChild("sogou"))
    all_refs: List[ArticleRef] = []
    seen: Set[str] = set()

    for kw in args.keywords:
        logger.info("━━ 关键词: [%s]，最多 %d 页", kw, args.pages)
        refs = scraper.search_articles_paged(kw, max_pages=args.pages)
        added = 0
        for ref in refs:
            key = normalize_wechat_url(ref.url)
            if key not in seen:
                seen.add(key)
                all_refs.append(ref)
                added += 1
        logger.info("关键词 [%s] 新增 %d 条，总计 %d 条", kw, added, len(all_refs))

    return all_refs


def mode_account(args, session: requests.Session, logger: logging.Logger) -> List[ArticleRef]:
    """模式B: 指定公众号，抓取历史文章"""
    scraper = SogouScraper(session, delay=args.delay, logger=logger.getChild("sogou"))
    all_refs: List[ArticleRef] = []
    seen: Set[str] = set()

    for account in args.accounts:
        logger.info("━━ 账号: [%s]，最多 %d 篇", account, args.max)
        # 支持 "账号名称" 或 "账号名称:__biz值" 格式
        biz = ""
        if ":" in account:
            account, biz = account.rsplit(":", 1)

        refs = scraper.get_account_article_refs(
            account_name=account, biz=biz, max_articles=args.max,
        )
        added = 0
        for ref in refs:
            key = normalize_wechat_url(ref.url)
            if key not in seen:
                seen.add(key)
                all_refs.append(ref)
                added += 1
        logger.info("账号 [%s] 新增 %d 条，总计 %d 条", account, added, len(all_refs))

    return all_refs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="微信公众号文章批量抓取器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 关键词模式: 搜 "GIS 遥感" 和 "地理信息"，每个关键词翻 30 页（约 300 篇）
  python wechat_mass_crawler.py keyword \\
      --keywords "GIS 遥感" "地理信息" \\
      --pages 30 --no-content --outdir out

  # 账号模式: 抓取两个公众号各自最多 200 篇历史文章
  python wechat_mass_crawler.py account \\
      --accounts "地理研究" "遥感与GIS" \\
      --max 200 --outdir out

  # 账号模式（已知 __biz，更精准）:
  python wechat_mass_crawler.py account \\
      --accounts "地理研究:MzI4NTc5NzU4Mw==" \\
      --max 500 --cookies "pac_uid=xxx; uin=yyy"

提示:
  --no-content  只采集链接，不抓正文（速度快 10 倍）
  --cookies     注入微信 cookie，解锁账号历史 API（翻页无限制）
  --delay       请求间隔秒数（默认 2.0，建议不低于 1.5 避免被封）
""")

    parser.add_argument("mode", choices=["keyword", "account"], help="运行模式")

    # 关键词模式
    parser.add_argument("--keywords", nargs="+", default=[], help="搜索关键词列表（keyword 模式）")
    parser.add_argument("--pages", type=int, default=20, help="每个关键词最多翻几页（默认 20，每页约 10 篇）")

    # 账号模式
    parser.add_argument(
        "--accounts", nargs="+", default=[],
        help='公众号名称列表，格式: "账号名" 或 "账号名:__biz值"（account 模式）',
    )
    parser.add_argument("--max", type=int, default=300, help="每个账号最多抓取文章数（默认 300）")

    # 通用
    parser.add_argument("--outdir", default="wechat_out", help="输出目录（默认 wechat_out）")
    parser.add_argument("--delay", type=float, default=2.0, help="请求间隔秒数（默认 2.0）")
    parser.add_argument("--concurrency", type=int, default=5, help="正文抓取并发数（默认 5）")
    parser.add_argument("--no-content", action="store_true", help="只采集链接，不抓正文正文")
    parser.add_argument("--cookies", default="", help="微信 cookie 字符串（用于账号历史 API）")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])

    args = parser.parse_args()

    logger = setup_logging(args.log_level)
    session = build_session(args.cookies)
    outdir = Path(args.outdir)

    # ── 第一步：采集文章链接 ──
    logger.info("=" * 55)
    logger.info("微信公众号文章批量抓取器 启动")
    logger.info("模式: %s", args.mode)
    logger.info("=" * 55)

    if args.mode == "keyword":
        if not args.keywords:
            logger.error("keyword 模式需要 --keywords")
            return 1
        refs = mode_keyword(args, session, logger)
    else:
        if not args.accounts:
            logger.error("account 模式需要 --accounts")
            return 1
        refs = mode_account(args, session, logger)

    logger.info("共采集到 %d 条文章链接", len(refs))

    if not refs:
        logger.warning("没有采集到任何文章链接，退出")
        return 1

    # 保存纯链接（万一正文抓取中途失败，链接已经有了）
    outdir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    links_only_path = outdir / f"links_only_{ts}.txt"
    with open(links_only_path, "w", encoding="utf-8") as f:
        for ref in refs:
            f.write(f"{ref.url}\t{ref.title}\t{ref.account_name}\n")
    logger.info("链接已预先保存至: %s", links_only_path)

    # ── 第二步：抓取正文（可选跳过） ──
    if args.no_content:
        logger.info("--no-content 模式，跳过正文抓取")
        articles = [
            Article(
                url=r.url, title=r.title, account_name=r.account_name,
                publish_time=r.publish_time, summary=r.summary,
                source=r.source, query=r.query, crawl_status="skip",
            )
            for r in refs
        ]
    else:
        logger.info("开始并发抓取正文（并发=%d，延迟=%.1fs）...", args.concurrency, args.delay)
        crawler = ArticleCrawler(
            concurrency=args.concurrency,
            delay=args.delay,
            logger=logger.getChild("crawler"),
        )
        articles = asyncio.run(crawler.crawl_all(refs))

    # ── 第三步：保存结果 ──
    save_results(articles, outdir, logger)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
