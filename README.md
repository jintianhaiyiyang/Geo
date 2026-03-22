# Geo Keyword Analyzer v8.0 / 地理关键词分析器 v8.0

**一句话**：面向地理 / GIS / 遥感主题的全自动关键词检索、抓取、筛选、语义分析与可视化管道，支持 `sync` / `async` / `stealth`（Playwright）抓取并包含独立的高质量文章搜索分支（`quality_search`）。

---

## 目录
1. [快速上手（Beginner Quick Start）](#快速上手beginner-quick-start)  
2. [功能概览](#功能概览)  
3. [要求与依赖](#要求与依赖)  
4. [安装（推荐流程）](#安装推荐流程)  
5. [常用运行示例](#常用运行示例)  
6. [Stealth（Playwright）模式说明](#stealthplaywright模式说明)  
7. [配置要点（`run_config.yaml`）](#配置要点run_configyaml)  
8. [quality_search（高质量分支）简介](#quality_search高质量分支简介)  
9. [输出与结果文件说明](#输出与结果文件说明)  
10. [数据库与持久化](#数据库与持久化)  
11. [测试与开发建议](#测试与开发建议)  
12. [常见问题与排错](#常见问题与排错)  
13. [安全与隐私注意](#安全与隐私注意)  
14. [发布与版本管理建议（Best Practices）](#发布与版本管理建议best-practices)  
15. [附：参考文件（实现细节）](#附参考文件实现细节)

---

## 快速上手（Beginner Quick Start）
下面是**最小可运行**路径，适合第一次尝试的用户：

1. 克隆仓库并进入：
   ```bash
   git clone <repo_url>
   cd Geo
````

2. 创建并激活虚拟环境（推荐）：

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # macOS / Linux
   # Windows PowerShell: .\.venv\Scripts\Activate.ps1
   ```

3. 安装依赖：

   ```bash
   pip install -r requirements-dev.txt
   ```

4. 运行 demo（内置演示数据）：

   ```bash
   python geo_keyword_analyzer_v8_0.py --demo --outdir out
   ```

   运行后结果将写到 `out/runs/<YYYYMMDD_HHMMSS>/`。如果要做真搜素，请参考下方示例。

> 快速提示：如果计划使用 Stealth 模式（浏览器抓取），请先执行 `python -m playwright install chromium`（见下文）。

---

## 功能概览

* 多源检索（bing / baidu / wechat / serpapi / 等，可配置）。
* 多抓取后端：`sync`、`async`、`stealth`（Playwright）。
* 正文抽取（`trafilatura` / `bs4` / auto）；内容去重（content-hash）。
* 语义分析：相关性过滤、基于 marker 的 advanced score、重复词统计、类别聚合。
* 附件检测、报表生成（CSV / JSON / HTML / PNG）、交互式可视化仪表盘。
* `quality_search`：独立的高质量文章搜索分支（配置驱动、与主流程隔离）。

---

## 要求与依赖

* **Python**: `3.10+`（项目声明） 。
* 依赖安装：

  ```bash
  pip install -r requirements-dev.txt
  ```

  `pyproject.toml` 中列出了关键依赖（例如 `playwright`、`playwright-stealth`、`httpx`、`trafilatura` 等）。若使用 Stealth，请确保 Playwright 与相应浏览器已安装。

---

## 安装（推荐流程）

推荐在虚拟环境中安装，步骤与快速上手相同。额外建议：

* 若使用 Stealth：`python -m playwright install chromium`。
* 若在 CI / 容器中运行，考虑在镜像中包含 Playwright 浏览器安装步骤（见 Docker 章节）。

---

## 常用运行示例

* **Demo（内置示例）：**

  ```bash
  python geo_keyword_analyzer_v8_0.py --demo --outdir out
  ```
* **全网搜索并分析（默认 async）：**

  ```bash
  python geo_keyword_analyzer_v8_0.py --search "GIS 遥感 最新技术" --outdir out
  ```
* **仅抓取（不分析）：**

  ```bash
  python geo_keyword_analyzer_v8_0.py --search "GIS 遥感 数据共享" --scrape-only --outdir out
  ```
* **仅基于数据库重建报表：**

  ```bash
  python geo_keyword_analyzer_v8_0.py --report-only --outdir out
  ```
* **查看完整 CLI：**

  ```bash
  python geo_keyword_analyzer_v8_0.py --help
  ```

主脚本实现了非常多的参数（时间窗口、抓取模式、HTTP 后端、提取器、并发控制、日志等），适合进阶用户定制运行。

---

## Stealth（Playwright）模式说明

**用途**：抓取需要执行 JavaScript、或需要更“真实浏览器”行为以应对反爬策略的页面。

### 必要准备

1. 安装 Playwright 并下载浏览器：

   ```bash
   python -m playwright install chromium
   ```

   （若使用 `--stealth-channel chrome` 且要指定本地 Chrome，可使用 `--stealth-executable-path`。）

2. 运行示例（Stealth + 代理）：

   ```bash
   python geo_keyword_analyzer_v8_0.py \
     --search "GIS 遥感 最新技术" \
     --crawl-mode stealth \
     --stealth-channel chrome \
     --proxy-file proxies.txt \
     --max-concurrency 3
   ```

   该示例来自仓库 README。

### 重要配置（`network.stealth`）

* `browser`（默认 `chromium`）、`channel`（如 `chrome`）、`executable_path`（与 `channel` 二选一）、`headless`、`max_concurrency`、`per_domain_concurrency`、`humanize`（模拟真人行为）、`use_stealth_plugin`、`navigation_timeout_ms` 等。默认值与校验在 `geo_analyzer/config.py` 中定义。校验会拒绝非法组合（如同时设置 `channel` 与 `executable_path`）。

### 代理支持

* 支持通过 `network.stealth.proxies` 或 `--proxy-file` 提供代理，代码会合并、去重并忽略注释/空行（`_load_stealth_proxies`）。代理不可用时会记录警告；项目还支持 `proxy_ban_ttl_seconds`（坏代理冷却）。

---

## 配置要点（`run_config.yaml`）

项目使用 `DEFAULT_CONFIG` 并且在运行前通过 `validate_config` 做严格校验。主要配置段包括：

* `search`：timeout、crawl_timeout、request_delay、limit、recent_months、providers（serpapi）等。
* `analysis`：min_relevance_score、advanced_only、min_advanced_score、top_keywords_count、scoring（marker_weights）、categories 等。
* `network`：http_backend、max_concurrency、stealth（详尽子项）、rate_limit、retry 等。
* `storage`：db_path、enable_db_write。默认 DB 文件名为 `geo_monitor_v8.db`（若使用 `outdir`，默认 DB 会放到 `outdir` 下）。

> 注意：若启用了 `search.providers.serpapi.enabled = true`，必须在 `search.providers.serpapi.api_key` 中提供 `api_key`，否则 `validate_config` 会报错并阻止运行。

---

## `quality_search`（高质量分支）简介

v8.0 引入了独立的 `quality_search` 分支，用于针对高质量文章进行独立检索与报表输出。该分支的关键配置项包括 `general_keywords`、`topic_keywords`、`run_standalone_queries`、`run_combined_queries`、`per_query_limit`、`max_total_urls` 等，且运行与主流程统计互不干扰。

---

## 输出与结果文件说明

每次运行的输出目录：

```
<outdir>/runs/<YYYYMMDD_HHMMSS>/
```

主要文件（举例）：

* `keyword_stats_<timestamp>.csv` — 关键词统计
* `article_stats_top100_<timestamp>.csv` — 文章统计（Top100）
* `top100_links_<timestamp>.md` — Top100 链接列表
* `geo_dashboard_<timestamp>.html` — 交互式 HTML 仪表盘
* `wordcloud_<timestamp>.png`、`knowledge_graph_<timestamp>.png` — 可视化图片
* `geo_analysis_result_<timestamp>.json` — 完整 JSON 结果（包含 meta 信息与 quality_search 区块）
  高质量分支输出（独立文件名）：
* `high_quality_article_stats_top100_<timestamp>.csv`
* `high_quality_top100_links_<timestamp>.md`
  详细字段与含义请查看生成的 JSON 或在需要时联系维护者以获取字段字典。

---

## 数据库与持久化

* 默认 SQLite 路径：`geo_monitor_v8.db`（可通过 CLI `--db-path` 或配置 `storage.db_path` 覆盖）。如果启用 `--report-only`，必须启用数据库持久化（`storage.enable_db_write = true`）。Pipeline 中会在运行结束时将结果与元数据写入数据库。

---

## 测试与开发建议

* 项目使用 `pytest`，仓库包含 `tests/test_v8_quality_search.py` 的示例测试。建议在提交/发布前运行测试套件与静态检查（lint / type check）。
* 运行测试：

  ```bash
  pytest -q
  ```
* 建议在 CI 中配置：`pytest`、`ruff/flake8`、`mypy`（或 ruff 的类型检查）、以及在 tag push 时触发 release 流程。

---

## 常见问题与排错（Troubleshooting）

* **Playwright 报错（找不到浏览器）**：执行 `python -m playwright install chromium`。
* **serpapi 启用但未设置 api_key**：`validate_config` 会报错，需在配置中设置 `search.providers.serpapi.api_key` 或禁用 serpapi。
* **配置校验失败**：`validate_config` 会返回字段名与错误信息（例如 `per_domain_concurrency > max_concurrency`、或 `channel` 与 `executable_path` 同时设置），按提示修正配置。
* **代理不可用 / 被封**：检查 `proxies.txt` 格式并用 `curl` 或 Playwright 单条测试；项目支持 `proxy_ban_ttl_seconds` 用于坏代理冷却。

---

## 安全与隐私注意

* **不要将带凭证的 `proxies.txt` 上传到公共仓库**（将其加入 `.gitignore`）。
* `--insecure`（关闭 TLS 校验）仅用于调试，生产环境禁用。
* 日志中应避免明文输出代理凭证或敏感配置字段，建议在日志层实现掩码处理（production best practice）。

---

## 发布与版本管理建议（Best Practices）

* 使用语义化版本（SemVer）并维护 `CHANGELOG.md`（记录 Breaking / Added / Fixed / Security 等）。
* 在发布前执行 release checklist（测试通过、lint/type 通过、更新 CHANGELOG、更新 `pyproject.toml` 版本并打 tag、在 CI 中构建 wheel/sdist 并上传 release）。
* 若 DB schema 或默认文件名变更（如 `geo_monitor_v8.db`），请在 release notes 中提供迁移/备份脚本或说明。

---

## 附：参考文件（实现细节）

实现细节可参考仓库关键文件：

* 项目 README 与版本说明（仓库原 README）。
* 入口脚本：`geo_keyword_analyzer_v8_0.py` / `geo_keyword_analyzer_v8.0.py`（CLI 定义与主流程）。 
* 配置与校验：`geo_analyzer/config.py`（默认值与 `validate_config`）。
* Pipeline：`geo_analyzer/pipeline.py`（包含质量分支的采集/抓取/报告逻辑）。
* 语义分析器：`geo_analyzer/analyzer.py`（相关性判定、advanced score、term extraction）。
* Packaging & deps：`pyproject.toml`。
* 测试示例：`tests/test_v8_quality_search.py`。

---

### 结束语

此 README 面向两类读者：

* **初学者 / 个人用户**：请优先按“快速上手”执行 demo，逐步尝试 `--search`、`--scrape-only` 与 `--report-only`。
* **进阶用户 / 研发人员**：阅读 `geo_analyzer/*` 源码（pipeline、config、analyzer、crawler 等）以扩展 provider、优化抓取策略或接入企业级运维/CI 流程。

如果你需要，我可以：

* 把上述内容直接生成 `README.md` 文件（替换或追加）；
* 生成 `run_config.minimal.yaml`、`run_demo.sh`、或 GitHub Actions CI workflow 的具体文件；
* 或把 README 的“输出字段字典”补齐为 JSON Schema/字段说明文档。
