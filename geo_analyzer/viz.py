"""Visualization outputs: dashboard HTML + PNG figures."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

try:
    import matplotlib.font_manager as fm
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    fm = None
    plt = None

try:
    import networkx as nx
except ImportError:  # pragma: no cover
    nx = None

try:
    from wordcloud import WordCloud
except ImportError:  # pragma: no cover
    WordCloud = None


def _safe_filename(path: Optional[str]) -> str:
    if not path:
        return ""
    return os.path.basename(path)


def _choose_chinese_font_name() -> str:
    if fm is None:
        return "sans-serif"
    available_fonts = {font_item.name for font_item in fm.fontManager.ttflist}
    preferred_fonts = [
        "Microsoft YaHei",
        "SimHei",
        "Noto Sans CJK SC",
        "PingFang SC",
        "Heiti SC",
        "Arial Unicode MS",
    ]
    return next((font_name for font_name in preferred_fonts if font_name in available_fonts), "sans-serif")


def _choose_chinese_font_path() -> Optional[str]:
    if fm is None:
        return None
    preferred = [
        "Microsoft YaHei",
        "SimHei",
        "Noto Sans CJK SC",
        "PingFang SC",
        "Heiti SC",
        "Arial Unicode MS",
    ]
    for font_item in fm.fontManager.ttflist:
        if font_item.name in preferred:
            return font_item.fname
    return None


def _prepare_graph_data(keyword_rows: List[Dict[str, Any]], max_words: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    sorted_rows = sorted(
        keyword_rows,
        key=lambda item: (int(item.get("article_hits", 0)), int(item.get("total_hits", 0))),
        reverse=True,
    )
    selected = sorted_rows[: max_words] if max_words > 0 else sorted_rows

    nodes: List[Dict[str, Any]] = []
    links: List[Dict[str, Any]] = []

    category_weight: Dict[str, int] = {}
    for row in selected:
        category = row.get("category") or "跨文章重复词"
        category_weight[category] = category_weight.get(category, 0) + int(row.get("total_hits", 0))

    for category, total in category_weight.items():
        nodes.append(
            {
                "id": f"cat::{category}",
                "name": category,
                "symbolSize": 36 + min(total, 80),
                "category": category,
                "value": total,
                "itemStyle": {"color": "#0d6efd"},
            }
        )

    for row in selected:
        category = row.get("category") or "跨文章重复词"
        keyword = row["keyword"]
        total_hits = int(row.get("total_hits", 0))
        article_hits = int(row.get("article_hits", 0))
        nodes.append(
            {
                "id": f"kw::{category}::{keyword}",
                "name": keyword,
                "symbolSize": 14 + min((article_hits * 4) + (total_hits * 1.4), 40),
                "category": category,
                "value": total_hits,
                "itemStyle": {"color": "#20c997"},
            }
        )
        links.append(
            {
                "source": f"cat::{category}",
                "target": f"kw::{category}::{keyword}",
                "value": total_hits,
                "lineStyle": {"width": 1 + min(total_hits / 2.5, 6), "opacity": 0.7},
            }
        )
    return nodes, links


def draw_knowledge_graph_png(
    keyword_rows: List[Dict[str, Any]],
    outdir: str,
    timestamp: str,
    max_words: int = 100,
    logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    log = logger or logging.getLogger("geo_analyzer.viz")
    if plt is None or nx is None:
        log.info("skip knowledge graph PNG: matplotlib/networkx unavailable")
        return None

    nodes, links = _prepare_graph_data(keyword_rows, max_words=max_words)
    if not nodes or not links:
        log.info("skip knowledge graph PNG: empty keyword rows")
        return None

    graph = nx.Graph()
    for node in nodes:
        graph.add_node(node["id"], name=node["name"], color=node["itemStyle"]["color"], size=node["symbolSize"])
    for link in links:
        graph.add_edge(link["source"], link["target"], weight=link["value"])

    pos = nx.spring_layout(graph, seed=42, k=0.9, iterations=120)
    font_name = _choose_chinese_font_name()
    plt.rcParams["font.sans-serif"] = [font_name]
    plt.rcParams["axes.unicode_minus"] = False

    fig = plt.figure(figsize=(14, 9), dpi=180)
    ax = fig.add_subplot(111)
    fig.patch.set_facecolor("#f7fbff")
    ax.set_facecolor("#f7fbff")

    edge_widths = [1.0 + min(graph[u][v]["weight"] / 3.0, 6.0) for u, v in graph.edges()]
    nx.draw_networkx_edges(graph, pos, width=edge_widths, alpha=0.35, edge_color="#0d6efd", ax=ax)

    node_colors = [graph.nodes[node]["color"] for node in graph.nodes()]
    node_sizes = [graph.nodes[node]["size"] * 26 for node in graph.nodes()]
    nx.draw_networkx_nodes(graph, pos, node_size=node_sizes, node_color=node_colors, alpha=0.82, ax=ax)

    labels = {node: graph.nodes[node]["name"] for node in graph.nodes()}
    nx.draw_networkx_labels(graph, pos, labels=labels, font_size=9, font_color="#12263a", ax=ax)

    ax.set_title("Knowledge Graph (Type -> Keyword)", fontsize=14, color="#12263a")
    ax.axis("off")

    output_path = os.path.join(outdir, f"knowledge_graph_{timestamp}.png")
    plt.tight_layout()
    plt.savefig(output_path, facecolor=fig.get_facecolor())
    plt.close(fig)
    log.info("knowledge graph PNG saved: %s", output_path)
    return output_path


def draw_wordcloud_png(
    keyword_rows: List[Dict[str, Any]],
    outdir: str,
    timestamp: str,
    max_words: int = 150,
    logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    log = logger or logging.getLogger("geo_analyzer.viz")
    if plt is None or WordCloud is None:
        log.info("skip wordcloud PNG: matplotlib/wordcloud unavailable")
        return None
    if not keyword_rows:
        log.info("skip wordcloud PNG: empty keyword rows")
        return None

    sorted_rows = sorted(
        keyword_rows,
        key=lambda item: (int(item.get("article_hits", 0)), int(item.get("total_hits", 0))),
        reverse=True,
    )
    selected = sorted_rows[: max_words] if max_words > 0 else sorted_rows
    frequencies = {item["keyword"]: max(1, int(item.get("total_hits", 0))) for item in selected}
    font_path = _choose_chinese_font_path()

    wc = WordCloud(
        width=1800,
        height=960,
        background_color="white",
        max_words=max_words if max_words > 0 else 200,
        font_path=font_path,
        colormap="tab20c",
    )
    wc.generate_from_frequencies(frequencies)

    fig = plt.figure(figsize=(13, 7), dpi=180)
    ax = fig.add_subplot(111)
    fig.patch.set_facecolor("#ffffff")
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")
    ax.set_title("Repeated Terms Wordcloud", fontsize=14, color="#12263a")

    output_path = os.path.join(outdir, f"wordcloud_{timestamp}.png")
    plt.tight_layout()
    plt.savefig(output_path, facecolor=fig.get_facecolor())
    plt.close(fig)
    log.info("wordcloud PNG saved: %s", output_path)
    return output_path


def draw_dashboard_html(
    keyword_rows: List[Dict[str, Any]],
    top_articles: List[Dict[str, Any]],
    outdir: str,
    timestamp: str,
    report_files: Dict[str, str],
    png_files: Dict[str, Optional[str]],
    summary: Dict[str, Any],
    max_words: int,
    logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    log = logger or logging.getLogger("geo_analyzer.viz")
    if not keyword_rows:
        log.info("skip dashboard HTML: empty keyword rows")
        return None

    nodes, links = _prepare_graph_data(keyword_rows, max_words=max_words)
    wordcloud_data = [
        {"name": row["keyword"], "value": int(row.get("total_hits", 0))}
        for row in sorted(
            keyword_rows,
            key=lambda item: (int(item.get("article_hits", 0)), int(item.get("total_hits", 0))),
            reverse=True,
        )[:max_words]
    ]

    table_rows = []
    for idx, article in enumerate(top_articles, 1):
        keyword_text = ";".join(
            sorted({kw for category_map in (article.get("matched_type_keywords", {}) or {}).values() for kw in category_map})
        )
        table_rows.append(
            {
                "rank": idx,
                "title": article.get("title", "") or "(untitled)",
                "url": article.get("url", ""),
                "source": article.get("source", ""),
                "search_query": article.get("search_query", ""),
                "publish_time": article.get("publish_time", ""),
                "matched_types": ";".join(article.get("matched_types", [])),
                "matched_keywords": keyword_text,
                "type_hit_count": int(article.get("type_hit_count", 0)),
                "advanced_score": float(article.get("advanced_score", 0.0)),
                "has_attachment": bool(article.get("has_attachment", False)),
                "attachment_score": float(article.get("attachment_score", 0.0)),
            }
        )

    category_count = len({row["category"] for row in keyword_rows})
    attachment_stats = summary.get("attachment_stats", {}) if isinstance(summary, dict) else {}
    with_attachment = int(attachment_stats.get("with_attachment", 0))
    run_history = summary.get("run_history", []) if isinstance(summary, dict) else []

    file_links = {
        "keyword_stats_csv": _safe_filename(report_files.get("keyword_stats_csv")),
        "article_stats_top100_csv": _safe_filename(report_files.get("article_stats_top100_csv")),
        "top100_markdown": _safe_filename(report_files.get("top100_markdown")),
        "knowledge_graph_png": _safe_filename(png_files.get("knowledge_graph_png")),
        "wordcloud_png": _safe_filename(png_files.get("wordcloud_png")),
    }

    dashboard_path = os.path.join(outdir, f"geo_dashboard_{timestamp}.html")

    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Geo Keyword Dashboard {timestamp}</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/echarts-wordcloud@2/dist/echarts-wordcloud.min.js"></script>
  <style>
    :root {{
      --bg: #f3f7fb;
      --card: #ffffff;
      --ink: #12263a;
      --muted: #567189;
      --accent: #0d6efd;
      --border: #dbe6f2;
      --shadow: 0 10px 26px rgba(18, 38, 58, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      background: radial-gradient(circle at top right, #eaf4ff 0%, #f7fbff 35%, var(--bg) 100%);
      color: var(--ink);
    }}
    .container {{
      width: min(1280px, 94vw);
      margin: 20px auto 40px auto;
      display: grid;
      gap: 16px;
    }}
    .hero {{
      background: linear-gradient(135deg, #ffffff 0%, #edf5ff 100%);
      border: 1px solid var(--border);
      border-radius: 18px;
      box-shadow: var(--shadow);
      padding: 18px 20px;
      display: grid;
      gap: 10px;
    }}
    .title {{
      font-size: 26px;
      font-weight: 700;
      letter-spacing: 0.2px;
    }}
    .subtitle {{
      color: var(--muted);
      font-size: 13px;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 14px;
    }}
    .card .label {{
      color: var(--muted);
      font-size: 12px;
    }}
    .card .value {{
      font-size: 22px;
      font-weight: 700;
      margin-top: 4px;
    }}
    .row {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }}
    .panel {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 12px 12px 8px 12px;
    }}
    .panel h3 {{
      margin: 4px 4px 10px 4px;
      font-size: 16px;
    }}
    #graph, #wordcloud {{
      width: 100%;
      height: 440px;
    }}
    .links {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .links a {{
      display: inline-block;
      text-decoration: none;
      color: white;
      background: linear-gradient(135deg, var(--accent), #4791ff);
      padding: 8px 10px;
      border-radius: 8px;
      font-size: 12px;
    }}
    .run-list {{
      margin-top: 10px;
      font-size: 12px;
      color: var(--muted);
      display: grid;
      gap: 6px;
    }}
    .run-item {{
      display: flex;
      justify-content: space-between;
      border: 1px dashed var(--border);
      border-radius: 8px;
      padding: 6px 8px;
    }}
    .table-panel {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 12px;
    }}
    .toolbar {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 10px;
    }}
    .toolbar input {{
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 8px 10px;
      min-width: 260px;
      font-size: 13px;
    }}
    .toolbar select {{
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 8px 10px;
      font-size: 13px;
      background: white;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    th, td {{
      border-bottom: 1px solid #edf3fa;
      text-align: left;
      padding: 8px 6px;
      vertical-align: top;
    }}
    thead th {{
      background: #f8fbff;
      color: #1b3a57;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    .pager {{
      margin-top: 10px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      font-size: 12px;
      color: var(--muted);
    }}
    .pager button {{
      border: 1px solid var(--border);
      background: white;
      border-radius: 6px;
      padding: 6px 10px;
      cursor: pointer;
    }}
    @media (max-width: 980px) {{
      .cards {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .row {{ grid-template-columns: 1fr; }}
      #graph, #wordcloud {{ height: 360px; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <div class="title">Geo Keyword Dashboard</div>
      <div class="subtitle">Timestamp: {timestamp}</div>
      <div class="cards">
        <div class="card"><div class="label">Valid Articles</div><div class="value">{int(summary.get("total_articles", 0))}</div></div>
        <div class="card"><div class="label">Top100 Rows</div><div class="value">{len(top_articles)}</div></div>
        <div class="card"><div class="label">Repeated Terms</div><div class="value">{len(keyword_rows)}</div></div>
        <div class="card"><div class="label">Categories</div><div class="value">{category_count}</div></div>
        <div class="card"><div class="label">With Attachment</div><div class="value">{with_attachment}</div></div>
        <div class="card"><div class="label">Recent Runs</div><div class="value">{len(run_history)}</div></div>
      </div>
    </section>

    <section class="panel">
      <h3>Artifacts</h3>
      <div class="links">
        <a href="{file_links["keyword_stats_csv"]}" target="_blank">Keyword CSV</a>
        <a href="{file_links["article_stats_top100_csv"]}" target="_blank">Top100 CSV</a>
        <a href="{file_links["top100_markdown"]}" target="_blank">Top100 Markdown</a>
        <a href="{file_links["knowledge_graph_png"]}" target="_blank">Graph PNG</a>
        <a href="{file_links["wordcloud_png"]}" target="_blank">Wordcloud PNG</a>
      </div>
      <div class="run-list" id="runHistory"></div>
    </section>

    <section class="row">
      <div class="panel">
        <h3>Knowledge Graph (Type -> Repeated Term)</h3>
        <div id="graph"></div>
      </div>
      <div class="panel">
        <h3>Repeated Terms Wordcloud</h3>
        <div id="wordcloud"></div>
      </div>
    </section>

    <section class="table-panel">
      <h3 style="margin:4px 0 10px 0;">Top100 Articles (Data Share Prioritized)</h3>
      <div class="toolbar">
        <input id="searchInput" placeholder="Search title/source/types/keywords..." />
        <select id="attachmentFilter">
          <option value="all" selected>All Attachments</option>
          <option value="with">With Attachment</option>
          <option value="without">Without Attachment</option>
        </select>
        <select id="pageSize">
          <option value="10">10 / page</option>
          <option value="20" selected>20 / page</option>
          <option value="50">50 / page</option>
        </select>
      </div>
      <div style="overflow:auto; max-height: 540px;">
        <table id="articleTable">
          <thead>
            <tr>
              <th>Rank</th><th>Title</th><th>Source</th><th>Publish Time</th><th>Types</th><th>Keywords</th><th>TypeHits</th><th>Score</th><th>Attachment</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      <div class="pager">
        <div id="pagerInfo"></div>
        <div>
          <button id="prevBtn">Prev</button>
          <button id="nextBtn">Next</button>
        </div>
      </div>
    </section>
  </div>

  <script>
    const graphNodes = {json.dumps(nodes, ensure_ascii=False)};
    const graphLinks = {json.dumps(links, ensure_ascii=False)};
    const cloudData = {json.dumps(wordcloud_data, ensure_ascii=False)};
    const tableData = {json.dumps(table_rows, ensure_ascii=False)};
    const runHistoryData = {json.dumps(run_history, ensure_ascii=False)};

    const graph = echarts.init(document.getElementById('graph'));
    graph.setOption({{
      animationDuration: 1200,
      tooltip: {{ trigger: 'item' }},
      series: [{{
        type: 'graph',
        layout: 'force',
        data: graphNodes,
        links: graphLinks,
        roam: true,
        draggable: true,
        force: {{ repulsion: 300, edgeLength: [80, 180], gravity: 0.05 }},
        lineStyle: {{ color: '#90b8ff' }},
        label: {{ show: true, color: '#1a3c5a', fontSize: 12 }}
      }}]
    }});

    const cloud = echarts.init(document.getElementById('wordcloud'));
    cloud.setOption({{
      tooltip: {{}},
      series: [{{
        type: 'wordCloud',
        shape: 'circle',
        gridSize: 6,
        sizeRange: [14, 58],
        rotationRange: [-35, 35],
        drawOutOfBound: false,
        textStyle: {{
          color: () => ['#0d6efd', '#20c997', '#146c43', '#6f42c1'][Math.floor(Math.random() * 4)]
        }},
        data: cloudData
      }}]
    }});

    const state = {{
      page: 1,
      pageSize: 20,
      filtered: tableData.slice()
    }};

    function filterRows() {{
      const q = document.getElementById('searchInput').value.trim().toLowerCase();
      const attachmentMode = document.getElementById('attachmentFilter').value;
      state.filtered = tableData.filter(row => {{
        const textHit = !q || [row.title, row.source, row.search_query, row.matched_types, row.matched_keywords].join(' ').toLowerCase().includes(q);
        const attachmentHit =
          attachmentMode === 'all' ||
          (attachmentMode === 'with' && row.has_attachment) ||
          (attachmentMode === 'without' && !row.has_attachment);
        return textHit && attachmentHit;
      }});
      state.page = 1;
      renderTable();
    }}

    function renderTable() {{
      state.pageSize = parseInt(document.getElementById('pageSize').value, 10);
      const total = state.filtered.length;
      const totalPages = Math.max(1, Math.ceil(total / state.pageSize));
      state.page = Math.min(state.page, totalPages);
      const start = (state.page - 1) * state.pageSize;
      const end = start + state.pageSize;
      const rows = state.filtered.slice(start, end);

      const tbody = document.querySelector('#articleTable tbody');
      tbody.innerHTML = rows.map(row => {{
        const safeTitle = String(row.title || '').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        const titleCell = row.url ? `<a href="${{row.url}}" target="_blank">${{safeTitle}}</a>` : safeTitle;
        const attachmentCell = row.has_attachment ? `Y (${{Number(row.attachment_score || 0).toFixed(1)}})` : '';
        return `<tr>
          <td>${{row.rank}}</td>
          <td>${{titleCell}}</td>
          <td>${{row.source || ''}}</td>
          <td>${{row.publish_time || ''}}</td>
          <td>${{row.matched_types || ''}}</td>
          <td>${{row.matched_keywords || ''}}</td>
          <td>${{row.type_hit_count}}</td>
          <td>${{Number(row.advanced_score).toFixed(2)}}</td>
          <td>${{attachmentCell}}</td>
        </tr>`;
      }}).join('');

      document.getElementById('pagerInfo').textContent = `Page ${{state.page}} / ${{totalPages}}, total ${{total}} rows`;
      document.getElementById('prevBtn').disabled = state.page <= 1;
      document.getElementById('nextBtn').disabled = state.page >= totalPages;
    }}

    document.getElementById('searchInput').addEventListener('input', filterRows);
    document.getElementById('attachmentFilter').addEventListener('change', filterRows);
    document.getElementById('pageSize').addEventListener('change', renderTable);
    document.getElementById('prevBtn').addEventListener('click', () => {{ state.page = Math.max(1, state.page - 1); renderTable(); }});
    document.getElementById('nextBtn').addEventListener('click', () => {{ state.page = state.page + 1; renderTable(); }});
    const runHistoryEl = document.getElementById('runHistory');
    if (runHistoryEl) {{
      if (!runHistoryData.length) {{
        runHistoryEl.innerHTML = '<div class="run-item"><span>No run history available</span></div>';
      }} else {{
        runHistoryEl.innerHTML = runHistoryData.slice(0, 6).map(item => {{
          const status = item.status_code === 0 ? 'ok' : 'fail';
          return `<div class="run-item"><span>${{item.run_id}}</span><span>${{status}} • ${{item.updated_at || ''}}</span></div>`;
        }}).join('');
      }}
    }}
    renderTable();

    window.addEventListener('resize', () => {{
      graph.resize();
      cloud.resize();
    }});
  </script>
</body>
</html>
"""

    with open(dashboard_path, "w", encoding="utf-8") as f:
        f.write(html)
    log.info("dashboard HTML saved: %s", dashboard_path)
    return dashboard_path


def draw_visualizations(
    keyword_rows: List[Dict[str, Any]],
    top_articles: List[Dict[str, Any]],
    outdir: str,
    timestamp: str,
    viz_format: str,
    max_words: int,
    report_files: Dict[str, str],
    summary: Dict[str, Any],
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Optional[str]]:
    log = logger or logging.getLogger("geo_analyzer.viz")
    outputs: Dict[str, Optional[str]] = {
        "dashboard_html": None,
        "knowledge_graph_png": None,
        "wordcloud_png": None,
    }

    if viz_format in ("png", "both"):
        outputs["knowledge_graph_png"] = draw_knowledge_graph_png(
            keyword_rows,
            outdir,
            timestamp,
            max_words=max_words,
            logger=log,
        )
        outputs["wordcloud_png"] = draw_wordcloud_png(
            keyword_rows,
            outdir,
            timestamp,
            max_words=max_words,
            logger=log,
        )

    if viz_format in ("html", "both"):
        outputs["dashboard_html"] = draw_dashboard_html(
            keyword_rows,
            top_articles,
            outdir,
            timestamp,
            report_files=report_files,
            png_files=outputs,
            summary=summary,
            max_words=max_words,
            logger=log,
        )

    return outputs
