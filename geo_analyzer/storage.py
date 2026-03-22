"""SQLite persistence layer for monitoring runs."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

SCHEMA_VERSION = 1


def _utcnow_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _safe_json_dumps(data: Any) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def _build_unique_key(article: Dict[str, Any]) -> str:
    content_hash = str(article.get("content_hash", "") or "")
    normalized_url = str(article.get("normalized_url", "") or article.get("url", "") or "")
    return f"{content_hash}|{normalized_url}".strip("|")


class GeoMonitorStorage:
    """Persist run metadata and article snapshots into SQLite."""

    def __init__(self, db_path: str, logger: Optional[logging.Logger] = None):
        self.db_path = os.path.abspath(db_path)
        self.logger = logger or logging.getLogger("geo_analyzer.storage")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    run_outdir TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    args_json TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    time_window_json TEXT NOT NULL,
                    status_code INTEGER,
                    status_text TEXT NOT NULL DEFAULT 'running',
                    error_message TEXT NOT NULL DEFAULT '',
                    provider_stats_json TEXT NOT NULL DEFAULT '{}',
                    attachment_stats_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    unique_key TEXT NOT NULL UNIQUE,
                    content_hash TEXT NOT NULL DEFAULT '',
                    normalized_url TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL DEFAULT '',
                    publish_time TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    search_query TEXT NOT NULL DEFAULT '',
                    has_attachment INTEGER NOT NULL DEFAULT 0,
                    attachment_score REAL NOT NULL DEFAULT 0.0,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS article_runs (
                    run_id TEXT NOT NULL,
                    article_id INTEGER NOT NULL,
                    rank_index INTEGER NOT NULL DEFAULT 0,
                    advanced_score REAL NOT NULL DEFAULT 0.0,
                    matched_types_json TEXT NOT NULL DEFAULT '[]',
                    matched_type_keywords_json TEXT NOT NULL DEFAULT '{}',
                    source TEXT NOT NULL DEFAULT '',
                    search_query TEXT NOT NULL DEFAULT '',
                    publish_time TEXT NOT NULL DEFAULT '',
                    has_attachment INTEGER NOT NULL DEFAULT 0,
                    attachment_score REAL NOT NULL DEFAULT 0.0,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, article_id),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id),
                    FOREIGN KEY (article_id) REFERENCES articles(id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS attachments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    article_id INTEGER NOT NULL,
                    attachment_type TEXT NOT NULL,
                    url TEXT NOT NULL DEFAULT '',
                    evidence_source TEXT NOT NULL DEFAULT '',
                    value_text TEXT NOT NULL DEFAULT '',
                    score REAL NOT NULL DEFAULT 0.0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES runs(run_id),
                    FOREIGN KEY (article_id) REFERENCES articles(id)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_publish_time ON articles(publish_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_search_query ON articles(search_query)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_has_attachment ON articles(has_attachment)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_article_runs_run_id ON article_runs(run_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_run_id ON attachments(run_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_article_id ON attachments(article_id)")

            exists = conn.execute("SELECT 1 FROM schema_version WHERE version = ?", (SCHEMA_VERSION,)).fetchone()
            if exists is None:
                conn.execute(
                    "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
                    (SCHEMA_VERSION, _utcnow_str()),
                )
            conn.commit()

    def start_run(
        self,
        run_id: str,
        run_outdir: str,
        mode: str,
        args_data: Dict[str, Any],
        config_data: Dict[str, Any],
        time_window: Dict[str, Any],
    ) -> None:
        now = _utcnow_str()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs(
                    run_id, run_outdir, mode, args_json, config_json, time_window_json,
                    status_code, status_text, error_message,
                    provider_stats_json, attachment_stats_json, result_json,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, NULL, 'running', '', '{}', '{}', '{}', ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    run_outdir=excluded.run_outdir,
                    mode=excluded.mode,
                    args_json=excluded.args_json,
                    config_json=excluded.config_json,
                    time_window_json=excluded.time_window_json,
                    status_code=NULL,
                    status_text='running',
                    error_message='',
                    provider_stats_json='{}',
                    attachment_stats_json='{}',
                    result_json='{}',
                    updated_at=excluded.updated_at
                """,
                (
                    run_id,
                    run_outdir,
                    mode,
                    _safe_json_dumps(args_data),
                    _safe_json_dumps(config_data),
                    _safe_json_dumps(time_window),
                    now,
                    now,
                ),
            )
            conn.commit()

    def finalize_run(
        self,
        run_id: str,
        status_code: int,
        error_message: str = "",
        provider_stats: Optional[Dict[str, Any]] = None,
        attachment_stats: Optional[Dict[str, Any]] = None,
        result_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET status_code=?,
                    status_text=?,
                    error_message=?,
                    provider_stats_json=?,
                    attachment_stats_json=?,
                    result_json=?,
                    updated_at=?
                WHERE run_id=?
                """,
                (
                    int(status_code),
                    "success" if int(status_code) == 0 else "failed",
                    str(error_message or ""),
                    _safe_json_dumps(provider_stats or {}),
                    _safe_json_dumps(attachment_stats or {}),
                    _safe_json_dumps(result_data or {}),
                    _utcnow_str(),
                    run_id,
                ),
            )
            conn.commit()

    def persist_articles(self, run_id: str, articles: List[Dict[str, Any]]) -> int:
        """Upsert articles and bind them to one run."""
        now = _utcnow_str()
        persisted = 0
        with self._connect() as conn:
            conn.execute("DELETE FROM article_runs WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM attachments WHERE run_id = ?", (run_id,))

            for rank_index, article in enumerate(articles, 1):
                unique_key = _build_unique_key(article)
                if not unique_key:
                    continue

                payload = dict(article)
                conn.execute(
                    """
                    INSERT INTO articles(
                        unique_key, content_hash, normalized_url, url, title, publish_time,
                        source, search_query, has_attachment, attachment_score, payload_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(unique_key) DO UPDATE SET
                        content_hash=excluded.content_hash,
                        normalized_url=excluded.normalized_url,
                        url=excluded.url,
                        title=excluded.title,
                        publish_time=excluded.publish_time,
                        source=excluded.source,
                        search_query=excluded.search_query,
                        has_attachment=excluded.has_attachment,
                        attachment_score=excluded.attachment_score,
                        payload_json=excluded.payload_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        unique_key,
                        str(article.get("content_hash", "") or ""),
                        str(article.get("normalized_url", "") or ""),
                        str(article.get("url", "") or ""),
                        str(article.get("title", "") or ""),
                        str(article.get("publish_time", "") or ""),
                        str(article.get("source", "") or ""),
                        str(article.get("search_query", "") or ""),
                        1 if bool(article.get("has_attachment")) else 0,
                        float(article.get("attachment_score", 0.0) or 0.0),
                        _safe_json_dumps(payload),
                        now,
                        now,
                    ),
                )
                row = conn.execute("SELECT id FROM articles WHERE unique_key = ?", (unique_key,)).fetchone()
                if row is None:
                    continue
                article_id = int(row["id"])

                conn.execute(
                    """
                    INSERT INTO article_runs(
                        run_id, article_id, rank_index, advanced_score, matched_types_json,
                        matched_type_keywords_json, source, search_query, publish_time,
                        has_attachment, attachment_score, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        article_id,
                        rank_index,
                        float(article.get("advanced_score", 0.0) or 0.0),
                        _safe_json_dumps(article.get("matched_types", [])),
                        _safe_json_dumps(article.get("matched_type_keywords", {})),
                        str(article.get("source", "") or ""),
                        str(article.get("search_query", "") or ""),
                        str(article.get("publish_time", "") or ""),
                        1 if bool(article.get("has_attachment")) else 0,
                        float(article.get("attachment_score", 0.0) or 0.0),
                        now,
                    ),
                )

                for evidence in article.get("attachment_evidence", []) or []:
                    if not isinstance(evidence, dict):
                        continue
                    conn.execute(
                        """
                        INSERT INTO attachments(
                            run_id, article_id, attachment_type, url, evidence_source,
                            value_text, score, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            article_id,
                            str(evidence.get("type", "") or ""),
                            str(evidence.get("url", "") or ""),
                            str(evidence.get("source", "") or ""),
                            str(evidence.get("value", "") or ""),
                            float(evidence.get("score", 0.0) or 0.0),
                            now,
                        ),
                    )
                persisted += 1

            conn.commit()
        return persisted

    def fetch_latest_success_result(self) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT run_id, result_json
                FROM runs
                WHERE status_code = 0
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None, None
        run_id = str(row["run_id"])
        raw = str(row["result_json"] or "")
        if not raw:
            return run_id, None
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return run_id, parsed
        except Exception:
            pass
        return run_id, None

    def fetch_recent_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run_id, mode, status_code, status_text, created_at, updated_at,
                       provider_stats_json, attachment_stats_json
                FROM runs
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (int(max(1, limit)),),
            ).fetchall()
        output: List[Dict[str, Any]] = []
        for row in rows:
            output.append(
                {
                    "run_id": str(row["run_id"]),
                    "mode": str(row["mode"] or ""),
                    "status_code": int(row["status_code"]) if row["status_code"] is not None else None,
                    "status_text": str(row["status_text"] or ""),
                    "created_at": str(row["created_at"] or ""),
                    "updated_at": str(row["updated_at"] or ""),
                    "provider_stats": self._parse_json_field(row["provider_stats_json"], {}),
                    "attachment_stats": self._parse_json_field(row["attachment_stats_json"], {}),
                }
            )
        return output

    @staticmethod
    def _parse_json_field(raw: Any, default: Any) -> Any:
        text = str(raw or "")
        if not text:
            return default
        try:
            parsed = json.loads(text)
            return parsed
        except Exception:
            return default

