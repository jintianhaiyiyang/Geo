from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENTRY = REPO_ROOT / "geo_keyword_analyzer_v6.5.py"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from geo_analyzer.attachments import detect_attachments
from geo_analyzer.time_window import resolve_time_window


def test_resolve_time_window_preset_and_custom():
    preset = resolve_time_window(preset="week", date_from="", date_to="", recent_months=6)
    assert preset["mode"] == "preset"
    assert preset["preset"] == "week"
    assert preset["date_from"]
    assert preset["date_to"]

    custom = resolve_time_window(
        preset="",
        date_from="2026-03-01",
        date_to="2026-03-15",
        recent_months=6,
    )
    assert custom["mode"] == "custom"
    assert custom["date_from"].startswith("2026-03-01")
    assert custom["date_to"].startswith("2026-03-15")


def test_attachment_detection_detects_download_evidence():
    article = {
        "title": "开放地理数据集发布",
        "content": "附件下载地址 https://example.com/dataset.zip 网盘链接见文末",
    }
    result = detect_attachments(article, min_score=1.0)
    assert result["has_attachment"] is True
    assert result["attachment_score"] > 0
    assert result["attachment_evidence"]


def test_input_bom_and_non_object_records_are_robust(tmp_path: Path):
    input_file = tmp_path / "input.json"
    payload = [
        "bad_record",
        {
            "title": "测试文章",
            "content": "GIS 遥感 数据共享",
            "url": "https://example.com/a",
            "publish_time": "2026-03-10 12:00:00",
            "source": "input",
            "search_query": "GIS",
        },
    ]
    # Write UTF-8 BOM to verify utf-8-sig input handling.
    input_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8-sig")
    outdir = tmp_path / "out"

    proc = subprocess.run(
        [
            sys.executable,
            str(ENTRY),
            "--input",
            str(input_file),
            "--outdir",
            str(outdir),
            "--viz-format",
            "html",
            "--no-db-write",
            "--no-advanced-filter",
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr


def test_scrape_only_then_report_only_with_db(tmp_path: Path):
    outdir = tmp_path / "out"
    db_path = tmp_path / "monitor.db"

    scrape = subprocess.run(
        [
            sys.executable,
            str(ENTRY),
            "--demo",
            "--scrape-only",
            "--db-path",
            str(db_path),
            "--outdir",
            str(outdir),
            "--no-progress",
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert scrape.returncode == 0, scrape.stderr

    report = subprocess.run(
        [
            sys.executable,
            str(ENTRY),
            "--report-only",
            "--db-path",
            str(db_path),
            "--outdir",
            str(outdir),
            "--viz-format",
            "html",
            "--no-progress",
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert report.returncode == 0, report.stderr
