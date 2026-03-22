# Geo Keyword Analyzer v7.0

Geo keyword analysis tool for GIS/remote-sensing/data-sharing topics.

v7.0 keeps v6.5 CLI compatibility and adds:
- SQLite monitoring persistence (`runs`/`articles`/`article_runs`/`attachments`)
- Time window presets and custom date range
- Attachment evidence detection and scoring
- Provider registry with stable providers (`bing`/`baidu`/`wechat`/`serpapi`)
- `--report-only` and `--scrape-only` modes

## Requirements

- Python `3.10+`
- Install deps:

```bash
pip install -r requirements-dev.txt
```

- For stealth mode:

```bash
playwright install chromium
```

## Quick Start

1. Demo run

```bash
python geo_keyword_analyzer_v6.5.py --demo --outdir out
```

2. Real search

```bash
python geo_keyword_analyzer_v6.5.py --search "GIS remote sensing latest technology" --outdir out
```

3. Time window preset

```bash
python geo_keyword_analyzer_v6.5.py --search "GIS 遥感 数据共享" --time-preset week --outdir out
```

4. Custom date range

```bash
python geo_keyword_analyzer_v6.5.py --search "GIS 遥感 数据共享" --date-from 2026-03-01 --date-to 2026-03-15 --outdir out
```

5. Scrape-only mode

```bash
python geo_keyword_analyzer_v6.5.py --search "GIS 遥感 数据共享" --scrape-only --outdir out
```

6. Report-only mode (from DB latest successful run)

```bash
python geo_keyword_analyzer_v6.5.py --report-only --outdir out
```

## New CLI Options (v7.0)

- `--db-path`: SQLite path (default `<outdir>/geo_monitor_v7.db`)
- `--no-db-write`: disable SQLite persistence
- `--report-only`: rebuild reports from latest DB run
- `--scrape-only`: crawl + filter + dedupe + attachment detection only
- `--time-preset {today,week,month}`
- `--date-from`, `--date-to`
- `--providers`: comma-separated provider list (example: `bing,baidu,wechat,serpapi`)

All v6.5 options remain valid.

## Output

Each run writes to:

```text
<outdir>/runs/<YYYYMMDD_HHMMSS>/
```

Main artifacts:
- `keyword_stats_<timestamp>.csv`
- `article_stats_top100_<timestamp>.csv`
- `top100_links_<timestamp>.md`
- `geo_dashboard_<timestamp>.html`
- `wordcloud_<timestamp>.png`
- `knowledge_graph_<timestamp>.png`
- `geo_analysis_result_<timestamp>.json`

## SQLite Schema (v7.0)

- `runs`: run-level metadata, status, payload snapshot
- `articles`: upserted article snapshots by unique key (`content_hash|normalized_url`)
- `article_runs`: run-to-article mapping with rank and scores
- `attachments`: attachment evidence rows

## Provider Strategy

Stable providers in v7.0:
- `bing`
- `baidu`
- `wechat`
- `serpapi` (optional key)

Experimental placeholders (config only, disabled by default):
- `google`
- `xiaohongshu`
- `bilibili`
- `douyin`

## Config

See `run_config.yaml` for a full v7.0 example with new sections:
- `storage`
- `providers`
- `time_window`
- `attachment_detection`

## Migration Notes (v6.5 -> v7.0)

- Existing commands still work.
- Existing config files still work; missing new sections are auto-filled by defaults.
- `--input` now supports UTF-8 BOM JSON via `utf-8-sig`.
- Non-object items in input arrays are dropped instead of crashing.

## Tests

```bash
pytest -q
```

