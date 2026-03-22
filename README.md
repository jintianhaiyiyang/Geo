# Geo Keyword Analyzer v8.0

面向 **地理 / GIS / 遥感** 主题的自动化文章采集、语义分析与可视化工具链。

包含两个独立工具：

| 工具 | 用途 |
|---|---|
| `geo_keyword_analyzer_v8_0.py` | 多源搜索 → 抓取 → 语义分析 → 可视化报表 |
| `wechat_mass_crawler.py` | 微信公众号文章**批量**采集（数百篇级别） |

---

## 目录结构

```
Geo/
├── geo_analyzer/                  # 核心包
│   ├── analyzer.py                # 语义分析
│   ├── pipeline.py                # 流程编排
│   ├── searcher.py                # 多源搜索
│   ├── crawler.py / crawler_async.py / crawler_stealth.py
│   ├── config.py                  # 配置与校验
│   └── ...
├── tests/
├── wechat_mass_crawler.py         # 微信批量采集工具（独立）
├── geo_keyword_analyzer_v8_0.py   # 主程序入口
├── geo_keyword_analyzer_v8.0.py   # 主程序（带点文件名）
├── run_config.yaml                # 运行时配置
├── requirements.txt
└── requirements-dev.txt
```

---

## 安装

**Python 3.10+，推荐使用虚拟环境。**

```bash
git clone <repo_url> && cd Geo

python -m venv .venv
source .venv/bin/activate          # Windows: .\.venv\Scripts\Activate.ps1

pip install -r requirements-dev.txt
```

如需使用 Stealth 抓取模式（Playwright）：

```bash
python -m playwright install chromium
```

如需使用微信批量采集工具：

```bash
pip install aiohttp tqdm trafilatura beautifulsoup4 requests
```

---

## 工具一：主分析器

### 快速开始

```bash
# 演示（使用内置样本数据）
python geo_keyword_analyzer_v8_0.py --demo --outdir out

# 全网搜索 + 分析（默认异步模式）
python geo_keyword_analyzer_v8_0.py --search "GIS 遥感 最新技术" --outdir out

# 查看所有参数
python geo_keyword_analyzer_v8_0.py --help
```

### 常用场景

```bash
# 只抓取，不分析（存入数据库备用）
python geo_keyword_analyzer_v8_0.py \
    --search "遥感 数据共享" --scrape-only --outdir out

# 基于上次数据库记录重建报表（不重新抓取）
python geo_keyword_analyzer_v8_0.py --report-only --outdir out

# 分析本地 JSON 文件
python geo_keyword_analyzer_v8_0.py --input raw_crawl.json --outdir out

# 限定时间窗口（只保留最近一个月文章）
python geo_keyword_analyzer_v8_0.py \
    --search "GIS 遥感" --time-preset month --outdir out

# 自定义日期范围
python geo_keyword_analyzer_v8_0.py \
    --search "GIS 遥感" --date-from 2026-01-01 --date-to 2026-03-01 --outdir out

# Stealth 模式 + 代理（绕过反爬）
python geo_keyword_analyzer_v8_0.py \
    --search "GIS 遥感 最新技术" \
    --crawl-mode stealth \
    --stealth-channel chrome \
    --proxy-file proxies.txt \
    --max-concurrency 3 \
    --outdir out
```

### 抓取模式对比

| 模式 | 参数 | 速度 | 适用场景 |
|---|---|---|---|
| `async` | `--crawl-mode async`（默认） | 快 | 大多数公开页面 |
| `sync` | `--crawl-mode sync` | 慢 | 调试、低并发 |
| `stealth` | `--crawl-mode stealth` | 最慢 | 有强反爬的网站 |

### 配置文件（`run_config.yaml`）

关键配置项：

```yaml
search:
  limit: 80              # 每次搜索抓取 URL 数量上限
  recent_months: 6       # 只保留最近 N 个月的文章（0 = 不限）
  request_delay: 1.2     # 请求间隔秒数

pipeline:
  crawl_mode: async      # async | sync | stealth

network:
  max_concurrency: 12    # 异步模式最大并发数
  http_backend: auto     # auto | requests | httpx | curl_cffi

providers:
  enabled:               # 启用的搜索源
    - bing
    - baidu
    - wechat
    - serpapi            # 需要填写 api_key

quality_search:
  enabled: true          # 高质量文章分支（独立统计）
```

启用 SerpApi 时需补充 key：

```yaml
search:
  providers:
    serpapi:
      enabled: true
      api_key: "YOUR_SERPAPI_KEY"
```

### 输出文件

每次运行结果保存至 `<outdir>/runs/<YYYYMMDD_HHMMSS>/`：

```
geo_analysis_result_<ts>.json          # 完整分析结果（含所有元数据）
keyword_stats_<ts>.csv                 # 关键词频率统计
article_stats_top100_<ts>.csv          # Top 100 文章评分
top100_links_<ts>.md                   # Top 100 链接列表
geo_dashboard_<ts>.html                # 交互式可视化仪表盘
wordcloud_<ts>.png                     # 词云图
knowledge_graph_<ts>.png               # 知识图谱
high_quality_article_stats_top100_<ts>.csv   # 高质量分支 Top 100
high_quality_top100_links_<ts>.md            # 高质量分支链接
```

### 数据库持久化

```bash
# 默认数据库路径（运行目录下）
geo_monitor_v8.db

# 自定义路径
python geo_keyword_analyzer_v8_0.py --search "..." --db-path /data/my.db

# 关闭数据库写入
python geo_keyword_analyzer_v8_0.py --search "..." --no-db-write
```

---

## 工具二：微信公众号批量采集

> **解决主分析器"搜不到多少文章"的问题。**  
> 主分析器依赖搜索引擎，每次查询上限约 10 条。批量采集器直接读公众号文章列表，可轻松获取数百篇。

### 模式A：关键词多页搜索

适合：不固定账号，按话题广撒网。

```bash
# 搜索 3 个关键词，每个翻 30 页（约 300 条链接）
python wechat_mass_crawler.py keyword \
    --keywords "GIS 遥感" "地理信息系统" "空间数据" \
    --pages 30 \
    --outdir wechat_out

# 只采集链接，不抓正文（快 10 倍，用于先摸底）
python wechat_mass_crawler.py keyword \
    --keywords "GIS 遥感" \
    --pages 50 --no-content \
    --outdir wechat_out
```

### 模式B：公众号历史文章

适合：已知目标账号，抓取其全部历史文章。

```bash
# 按账号名搜索，最多各取 300 篇
python wechat_mass_crawler.py account \
    --accounts "地理研究" "遥感与GIS" "中国测绘学会" \
    --max 300 \
    --outdir wechat_out

# 指定 __biz 更精准（从该账号任意文章 URL 里复制）
python wechat_mass_crawler.py account \
    --accounts "地理研究:MzI4NTc5NzU4Mw==" \
    --max 500 \
    --outdir wechat_out
```

### 解锁历史文章 API（Cookie 模式）

不带 Cookie 只能通过搜狗拿到最近几十篇。带微信 Cookie 后调用原生接口，可翻全部历史：

1. 浏览器登录 `mp.weixin.qq.com`
2. F12 → Network → 随便点一个请求 → 复制 `Cookie` 请求头的值
3. 传入 `--cookies`：

```bash
python wechat_mass_crawler.py account \
    --accounts "地理研究:MzI4NTc5NzU4Mw==" \
    --max 1000 \
    --cookies "pac_uid=xxx; uin=yyy; skey=zzz" \
    --outdir wechat_out
```

### 参数说明

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--pages` | 20 | 关键词模式每个词最多翻几页（每页约 10 篇） |
| `--max` | 300 | 账号模式每个账号最多采集篇数 |
| `--delay` | 2.0 | 请求间隔秒数，**不建议低于 1.5** |
| `--concurrency` | 5 | 正文抓取异步并发数 |
| `--no-content` | — | 只采集链接，不抓正文 |
| `--cookies` | — | 微信 Cookie 字符串（解锁历史 API） |

### 输出文件

```
wechat_out/
├── links_only_<ts>.txt      # 采集到链接后立即保存（断点保护）
├── articles_<ts>.json       # 完整文章数据（含正文）
└── links_<ts>.txt           # 带标题、账号、时间的链接列表
```

### 与主分析器联动

采集完成后，直接把 JSON 喂给主分析器做语义分析：

```bash
# 第一步：批量采集微信文章
python wechat_mass_crawler.py keyword \
    --keywords "GIS 遥感" --pages 40 --outdir wechat_out

# 第二步：语义分析 + 生成报表
python geo_keyword_analyzer_v8_0.py \
    --input wechat_out/articles_<ts>.json \
    --outdir out
```

---

## 开发与测试

```bash
# 运行测试
pytest -q

# 代码检查
ruff check .

# 类型检查（可选）
mypy geo_analyzer/
```

---

## 常见问题

**搜狗出现验证码，采集中断**  
脚本检测到验证码会自动停止翻页，已采集的链接已写入 `links_only_*.txt`，稍后可继续处理。增大 `--delay`（建议 3.0 以上）可减少触发概率。

**Playwright 找不到浏览器**  
```bash
python -m playwright install chromium
```

**SerpApi 配置报错**  
在 `run_config.yaml` 中填写 `search.providers.serpapi.api_key`，或将 `enabled` 设为 `false`。

**配置校验失败**  
根据报错提示修正参数。常见原因：Stealth 的 `channel` 与 `executable_path` 同时设置、并发数超出范围。

**`--report-only` 报"无数据"**  
需要先有至少一次成功的完整运行（非 `--scrape-only`）写入数据库。

**`aiohttp` 报 ImportError**  
```bash
pip install aiohttp
```
未安装时脚本会自动降级为同步模式，功能不受影响，速度会变慢。

---

## 安全注意事项

- 不要把含有凭证的 `proxies.txt`、Cookie 字符串或 API Key 提交到公开仓库
- `--insecure` 会关闭 TLS 证书校验，仅用于本地调试
- 建议在日志配置中掩码代理凭证等敏感字段