# Geo Keyword Analyzer v8.0 / 地理关键词分析器 v8.0

面向地理 / GIS / 遥感主题的自动化关键词检索、抓取、筛选、语义分析与可视化工具链，支持 `sync` / `async` / `stealth`（Playwright）三种抓取模式，并包含独立的高质量文章搜索分支（`quality_search`）。

---

## 功能概览

- 多源检索：`bing` / `baidu` / `wechat` / `serpapi`（可配置）。
- 多抓取后端：`sync`、`async`、`stealth`（Playwright）。
- 正文抽取：`trafilatura` / `bs4` / `auto`，并支持内容去重。
- 语义分析：相关性过滤、advanced score、关键词统计、类别聚合。
- 多格式输出：`CSV` / `JSON` / `HTML` / `PNG`。
- 独立高质量分支：`quality_search`（与主流程统计隔离）。

---

## 环境要求

- Python `3.10+`
- 推荐使用虚拟环境
- 依赖安装：

```bash
pip install -r requirements-dev.txt
```

如果需要使用 Stealth 抓取，请额外安装浏览器：

```bash
python -m playwright install chromium
```

---

## 快速上手

1. 克隆仓库并进入目录：

```bash
git clone <repo_url>
cd <repo_dir>
```

2. 创建并激活虚拟环境：

```bash
python -m venv .venv
source .venv/bin/activate
# Windows PowerShell: .\.venv\Scripts\Activate.ps1
```

3. 安装依赖：

```bash
pip install -r requirements-dev.txt
```

4. 运行演示数据：

```bash
python geo_keyword_analyzer_v8_0.py --demo --outdir out
```

5. 查看完整 CLI 参数：

```bash
python geo_keyword_analyzer_v8_0.py --help
```

---

## 常用运行示例

全网搜索并分析（默认 `async`）：

```bash
python geo_keyword_analyzer_v8_0.py --search "GIS 遥感 最新技术" --outdir out
```

仅抓取（不分析）：

```bash
python geo_keyword_analyzer_v8_0.py --search "GIS 遥感 数据共享" --scrape-only --outdir out
```

仅基于数据库重建报表：

```bash
python geo_keyword_analyzer_v8_0.py --report-only --outdir out
```

Stealth + 代理示例：

```bash
python geo_keyword_analyzer_v8_0.py --search "GIS 遥感 最新技术" --crawl-mode stealth --stealth-channel chrome --proxy-file proxies.txt --max-concurrency 3
```

---

## 配置要点（`run_config.yaml`）

运行时会基于默认配置并执行严格校验（`validate_config`），核心配置段如下：

- `search`：搜索超时、抓取超时、结果数量、时间窗口、provider 配置等。
- `network`：HTTP 后端、并发控制、Stealth 子配置、重试与限流策略。
- `analysis`：相关性与 advanced score 等分析阈值。
- `storage`：SQLite 路径与持久化开关。
- `quality_search`：高质量分支关键词与抓取规模控制。

启用 `serpapi` 时，必须配置 `search.providers.serpapi.api_key`，否则会在配置校验阶段报错并终止运行。

---

## 输出目录与结果文件

每次运行输出到：

```text
<outdir>/runs/<YYYYMMDD_HHMMSS>/
```

常见产物：

- `keyword_stats_<timestamp>.csv`：关键词统计
- `article_stats_top100_<timestamp>.csv`：文章统计（Top100）
- `top100_links_<timestamp>.md`：Top100 链接列表
- `geo_dashboard_<timestamp>.html`：交互式仪表盘
- `wordcloud_<timestamp>.png`：词云
- `knowledge_graph_<timestamp>.png`：知识图谱
- `geo_analysis_result_<timestamp>.json`：完整分析结果（含 `meta` 与 `quality_search` 区块）
- `high_quality_article_stats_top100_<timestamp>.csv`：高质量分支统计
- `high_quality_top100_links_<timestamp>.md`：高质量分支链接

---

## 数据库与持久化

- 默认 SQLite 文件：`geo_monitor_v8.db`
- 可通过 `--db-path` 或 `storage.db_path` 覆盖路径
- `--report-only` 依赖数据库中已有成功运行数据
- `storage.enable_db_write = false` 时不会写入数据库

---

## 测试与开发

运行测试：

```bash
pytest -q
```

建议在 CI 中至少包含：

- `pytest`
- `ruff` 或 `flake8`
- `mypy`（可选）

---

## 常见问题（Troubleshooting）

- Playwright 报错找不到浏览器：执行 `python -m playwright install chromium`
- 启用 `serpapi` 但未设置 `api_key`：补齐 `search.providers.serpapi.api_key`
- 配置校验失败：根据报错修正参数组合（例如并发约束、Stealth 通道与可执行路径冲突）
- 代理不可用：检查 `proxies.txt` 格式，并单独验证代理可连通性

---

## 安全与隐私

- 不要将带凭证的 `proxies.txt` 提交到公共仓库
- `--insecure` 仅用于调试，不建议在生产环境使用
- 建议在日志中掩码敏感字段（代理凭证、密钥等）

---

## 关键文件

- `geo_keyword_analyzer_v8_0.py`：无点文件名入口（兼容启动）
- `geo_keyword_analyzer_v8.0.py`：主 CLI 与运行入口
- `run_config.yaml`：运行时配置
- `geo_analyzer/config.py`：默认配置与校验逻辑
- `geo_analyzer/pipeline.py`：主流程编排
- `geo_analyzer/analyzer.py`：语义分析逻辑
- `tests/test_v8_quality_search.py`：高质量分支测试示例
