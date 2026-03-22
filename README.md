# Geo Keyword Analyzer v8.0 / 地理关键词分析器 v8.0

## 中文说明

### 项目简介
`Geo Keyword Analyzer v8.0` 用于地理/GIS/遥感相关网页检索、抓取、筛选、统计与可视化。  
本版本新增“高质量文章关键词搜索”独立分支，并保持主流程热门词分析不受影响。

### README 维护约束（必须遵守）
- 每次修改 `README.md`，必须按你给定的要求进行更新。
- 每次修改 `README.md`，必须保持中英文双语内容同步更新。
- 每次修改 `README.md`，必须保证版本号、脚本名、命令示例与当前代码一致。

### 版本重点（v8.0）
- 入口脚本统一为：
  - `geo_keyword_analyzer_v8_0.py`（推荐）
  - `geo_keyword_analyzer_v8.0.py`（同样可用）
- 默认数据库文件升级为：`geo_monitor_v8.db`
- 新增 `quality_search` 独立分支：
  - 默认启用
  - 配置驱动（YAML）
  - 同时支持“关键词单跑 + 主查询组合跑”
  - 与主流程热门词统计完全隔离

### 环境要求
- Python `3.10+`
- 安装依赖：

```bash
pip install -r requirements-dev.txt
```

- 如需 Stealth 抓取：

```bash
playwright install chromium
```

### 快速开始
1. 演示数据

```bash
python geo_keyword_analyzer_v8_0.py --demo --outdir out
```

2. 全网搜索

```bash
python geo_keyword_analyzer_v8_0.py --search "GIS 遥感 最新技术" --outdir out
```

3. 仅抓取（不分析）

```bash
python geo_keyword_analyzer_v8_0.py --search "GIS 遥感 数据共享" --scrape-only --outdir out
```

4. 仅基于数据库重建报表

```bash
python geo_keyword_analyzer_v8_0.py --report-only --outdir out
```

5. 时间窗口（最近一周）

```bash
python geo_keyword_analyzer_v8_0.py --search "GIS 遥感 数据共享" --time-preset week --outdir out
```

### 核心配置（run_config.yaml）
`v8.0` 新增 `quality_search` 配置段（无 CLI 覆盖）：

```yaml
quality_search:
  enabled: true
  general_keywords:
    - "地理数据"
    - "空间数据"
    - "GIS数据"
    - "数据分享"
    - "数据发布"
    - "数据共享"
    - "数据链接"
    - "地理模型"
    - "大数据"
  topic_keywords:
    - "基础地理"
    - "DEM"
    - "地形"
    - "地貌"
    - "土壤"
    - "土地利用"
    - "土地覆盖"
    - "生态环境"
    - "气象"
    - "水文"
    - "人口"
    - "GDP"
    - "社会经济"
  run_standalone_queries: true
  run_combined_queries: true
  per_query_limit: 3
  max_total_urls: 120
```

### 结果输出
每次运行输出到：

```text
<outdir>/runs/<YYYYMMDD_HHMMSS>/
```

主流程文件：
- `keyword_stats_<timestamp>.csv`
- `article_stats_top100_<timestamp>.csv`
- `top100_links_<timestamp>.md`
- `geo_dashboard_<timestamp>.html`
- `wordcloud_<timestamp>.png`
- `knowledge_graph_<timestamp>.png`
- `geo_analysis_result_<timestamp>.json`

高质量分支独立文件：
- `high_quality_article_stats_top100_<timestamp>.csv`
- `high_quality_top100_links_<timestamp>.md`

### 独立性说明（重要）
- 高质量关键词搜索分支独立检索、独立抓取、独立报表。
- 主流程的 `top_keywords` / `repeated_terms` 不会被高质量关键词列表直接影响。
- 高质量结果写入 JSON 的 `quality_search` 区块，不覆盖主流程统计结果。

### 测试

```bash
pytest -q
```

---

## English

### Overview
`Geo Keyword Analyzer v8.0` is a GIS/geo-data focused crawler + analyzer pipeline for search, extraction, filtering, ranking, and reporting.  
This release adds an independent high-quality keyword search branch without changing the main hot-term analysis flow.

### README Maintenance Rules (Mandatory)
- Every README update must follow your specified requirements.
- Every README update must keep Chinese and English sections in sync.
- Every README update must keep version/script/command examples aligned with the current code.

### What is new in v8.0
- Unified entry scripts:
  - `geo_keyword_analyzer_v8_0.py` (recommended)
  - `geo_keyword_analyzer_v8.0.py` (also supported)
- Default SQLite path changed to `geo_monitor_v8.db`.
- Added independent `quality_search` branch:
  - enabled by default
  - YAML-configurable
  - runs both standalone keyword queries and combined queries (`main_query + keyword`)
  - fully isolated from the main hot-term statistics

### Requirements
- Python `3.10+`
- Install dependencies:

```bash
pip install -r requirements-dev.txt
```

- For stealth crawling:

```bash
playwright install chromium
```

### Quick Start
1. Demo mode

```bash
python geo_keyword_analyzer_v8_0.py --demo --outdir out
```

2. Real search mode

```bash
python geo_keyword_analyzer_v8_0.py --search "GIS remote sensing latest technology" --outdir out
```

3. Scrape only

```bash
python geo_keyword_analyzer_v8_0.py --search "GIS remote sensing data sharing" --scrape-only --outdir out
```

4. Report only (from latest successful DB run)

```bash
python geo_keyword_analyzer_v8_0.py --report-only --outdir out
```

### Quality Search Config
The `quality_search` YAML section controls the independent high-quality branch:
- `enabled`
- `general_keywords`
- `topic_keywords`
- `run_standalone_queries`
- `run_combined_queries`
- `per_query_limit`
- `max_total_urls`

### Outputs
Run outputs are written to:

```text
<outdir>/runs/<YYYYMMDD_HHMMSS>/
```

Main outputs:
- `keyword_stats_<timestamp>.csv`
- `article_stats_top100_<timestamp>.csv`
- `top100_links_<timestamp>.md`
- `geo_dashboard_<timestamp>.html`
- `wordcloud_<timestamp>.png`
- `knowledge_graph_<timestamp>.png`
- `geo_analysis_result_<timestamp>.json`

Independent high-quality outputs:
- `high_quality_article_stats_top100_<timestamp>.csv`
- `high_quality_top100_links_<timestamp>.md`

### Isolation Guarantee
- High-quality search runs in a separate retrieval/crawl/report path.
- Main `top_keywords` and `repeated_terms` are not directly driven by the high-quality keyword list.
- High-quality results are stored under the `quality_search` block in the result JSON.

### Test

```bash
pytest -q
```
