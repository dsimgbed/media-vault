from __future__ import annotations

import asyncio
import contextlib
import json
import re
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TAG_TOKEN_RE = re.compile(r"[^a-z0-9_-]+")
_FTS_TOKEN_RE = re.compile(r"[A-Za-z0-9_-]+")

_TRACKING_QUERY_PARAMS = {
    "si",
    "feature",
    "fbclid",
    "gclid",
    "igshid",
    "pp",
    "_hsenc",
    "_hsmi",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "s",
    "t",
}
_YOUTUBE_PLAYLIST_PARAMS = {"list", "index", "start_radio", "rv"}
_UNSET = object()
_ACTIVE_JOB_STATUSES = ("queued", "downloading", "allocating")
_TERMINAL_JOB_STATUSES = ("done", "failed", "cancelled")


def normalize_source_url(url: str | None) -> str | None:
    if not url or not str(url).strip():
        return None
    raw = str(url).strip()
    parts = urlsplit(raw)
    if not parts.scheme or not parts.netloc:
        return raw

    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or ""

    if netloc in ("www.youtube.com", "m.youtube.com", "music.youtube.com"):
        netloc = "youtube.com"
    elif netloc in ("x.com", "www.x.com"):
        netloc = "twitter.com"

    if path != "/":
        path = path.rstrip("/")

    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    filtered_params: list[tuple[str, str]] = []
    for k, v in query_pairs:
        k_low = k.lower()
        if k_low.startswith("utm_") or k_low in _TRACKING_QUERY_PARAMS:
            continue
        filtered_params.append((k, v))

    if netloc == "youtu.be":
        segments = [seg for seg in path.split("/") if seg]
        if len(segments) == 1:
            vid = segments[0]
            scheme = "https"
            netloc = "youtube.com"
            path = "/watch"
            filtered_params = [
                (k, v)
                for k, v in filtered_params
                if k.lower() not in _YOUTUBE_PLAYLIST_PARAMS and k.lower() != "v"
            ]
            filtered_params.append(("v", vid))
    elif netloc == "youtube.com":
        segments = [seg for seg in path.split("/") if seg]
        if len(segments) == 2 and segments[0] == "shorts":
            vid = segments[1]
            scheme = "https"
            path = "/watch"
            filtered_params = [
                (k, v)
                for k, v in filtered_params
                if k.lower() not in _YOUTUBE_PLAYLIST_PARAMS and k.lower() != "v"
            ]
            filtered_params.append(("v", vid))
        elif path == "/watch" and any(k.lower() == "v" for k, _ in filtered_params):
            filtered_params = [
                (k, v) for k, v in filtered_params if k.lower() not in _YOUTUBE_PLAYLIST_PARAMS
            ]

    filtered_params.sort(key=lambda kv: (kv[0], kv[1]))
    query = urlencode(filtered_params, doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def normalize_tags(raw: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if not raw:
        return []
    tokens: list[str] = []
    items = [raw] if isinstance(raw, str) else list(raw)
    for item in items:
        for part in re.split(r"[\s,]+", str(item)):
            cleaned = _TAG_TOKEN_RE.sub("", part.strip().lstrip("#").lower())
            if cleaned and cleaned not in tokens:
                tokens.append(cleaned[:48])
    return tokens


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._sub_lock = threading.Lock()
        self._subscribers: set[tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, Any]]]] = set()
        self._init_schema()

    def _ensure_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA foreign_keys=ON;")
            self._conn = conn
        return self._conn

    @contextlib.contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._ensure_conn()
            try:
                yield conn
                if conn.in_transaction:
                    conn.commit()
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise

    @staticmethod
    def _enqueue_nowait(queue: asyncio.Queue[dict[str, Any]], event: dict[str, Any]) -> None:
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            pass

    def subscribe(self) -> tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, Any]]]:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=128)
        sub = (loop, queue)
        with self._sub_lock:
            self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, Any]]]) -> None:
        with self._sub_lock:
            self._subscribers.discard(sub)

    def publish(self, event: dict[str, Any]) -> None:
        with self._sub_lock:
            subs = list(self._subscribers)
        for loop, queue in subs:
            if loop.is_closed():
                self.unsubscribe((loop, queue))
                continue
            try:
                loop.call_soon_threadsafe(self._enqueue_nowait, queue, event)
            except RuntimeError:
                self.unsubscribe((loop, queue))

    def close(self) -> None:
        with self._sub_lock:
            subs = list(self._subscribers)
            self._subscribers.clear()
        for loop, queue in subs:
            if not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(self._enqueue_nowait, queue, {"type": "_shutdown"})
                except RuntimeError:
                    pass
        with self._lock:
            if self._conn is not None:
                try:
                    if self._conn.in_transaction:
                        self._conn.commit()
                finally:
                    self._conn.close()
                    self._conn = None

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS media_items (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    folder TEXT NOT NULL DEFAULT 'General',
                    tags TEXT NOT NULL DEFAULT '',
                    media_type TEXT NOT NULL, -- 'video' | 'image' | 'audio' | 'file'
                    mime_type TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    drive_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    thumb_filename TEXT,
                    source_origin TEXT NOT NULL, -- 'web' | 'telegram'
                    source_kind TEXT NOT NULL,   -- 'ytdlp' | 'ddl' | 'upload'
                    source_url TEXT,
                    sha256 TEXT,
                    normalized_source_url TEXT,
                    telegram_chat_id TEXT,
                    telegram_topic_id INTEGER,
                    telegram_msg_id INTEGER,
                    telegram_worker_name TEXT,
                    part_count INTEGER NOT NULL DEFAULT 1,
                    duration_sec REAL,
                    width INTEGER,
                    height INTEGER,
                    video_codec TEXT,
                    audio_codec TEXT,
                    fps REAL,
                    preview_filename TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_media_created_at ON media_items(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_media_type ON media_items(media_type);
                CREATE INDEX IF NOT EXISTS idx_media_drive ON media_items(drive_name);

                CREATE TABLE IF NOT EXISTS media_tags (
                    media_id TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    PRIMARY KEY (media_id, tag)
                );
                CREATE INDEX IF NOT EXISTS idx_media_tags_tag ON media_tags(tag);
                CREATE INDEX IF NOT EXISTS idx_media_tags_media_id ON media_tags(media_id);

                CREATE VIRTUAL TABLE IF NOT EXISTS media_fts USING fts5(
                    media_id UNINDEXED,
                    title,
                    filename,
                    folder,
                    tags,
                    source_url
                );

                CREATE TABLE IF NOT EXISTS media_parts (
                    media_id TEXT NOT NULL,
                    part_index INTEGER NOT NULL,
                    start_byte INTEGER NOT NULL,
                    end_byte INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    telegram_chat_id TEXT NOT NULL,
                    telegram_topic_id INTEGER,
                    telegram_msg_id INTEGER NOT NULL,
                    telegram_worker_name TEXT,
                    PRIMARY KEY (media_id, part_index)
                );
                CREATE INDEX IF NOT EXISTS idx_media_parts_media_id ON media_parts(media_id);

                CREATE TABLE IF NOT EXISTS ingest_jobs (
                    id TEXT PRIMARY KEY,
                    source_origin TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    status TEXT NOT NULL, -- 'queued' | 'downloading' | 'allocating' | 'done' | 'failed' | 'cancelled'
                    progress REAL NOT NULL DEFAULT 0,
                    message TEXT,
                    media_ids_json TEXT,
                    params_json TEXT,
                    staged_path TEXT,
                    staged_expires_at TEXT,
                    staged_size_bytes INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON ingest_jobs(created_at DESC);

                CREATE TABLE IF NOT EXISTS upload_sessions (
                    id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    title TEXT,
                    folder TEXT,
                    tags TEXT NOT NULL DEFAULT '',
                    forward_to_telegram INTEGER NOT NULL DEFAULT 1,
                    force INTEGER NOT NULL DEFAULT 0,
                    total_size INTEGER NOT NULL,
                    chunk_size INTEGER NOT NULL,
                    total_chunks INTEGER NOT NULL,
                    received_chunks_json TEXT NOT NULL DEFAULT '[]',
                    received_bytes INTEGER NOT NULL DEFAULT 0,
                    expected_sha256 TEXT,
                    fingerprint TEXT,
                    staging_dir TEXT NOT NULL,
                    staging_file TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'uploading',
                    job_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_upload_sessions_status ON upload_sessions(status);
                CREATE INDEX IF NOT EXISTS idx_upload_sessions_expires_at ON upload_sessions(expires_at);

                CREATE TABLE IF NOT EXISTS folder_topics (
                    chat_id TEXT NOT NULL,
                    folder_name TEXT NOT NULL,
                    topic_id INTEGER NOT NULL,
                    PRIMARY KEY (chat_id, folder_name)
                );

                CREATE TABLE IF NOT EXISTS user_prefs (
                    user_id INTEGER PRIMARY KEY,
                    default_folder TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS vault_backups (
                    id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    uncompressed_bytes INTEGER NOT NULL DEFAULT 0,
                    sha256 TEXT,
                    trigger TEXT NOT NULL DEFAULT 'manual',
                    note TEXT,
                    telegram_chat_id TEXT,
                    telegram_topic_id INTEGER,
                    telegram_msg_id INTEGER,
                    telegram_worker_name TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_vault_backups_created_at ON vault_backups(created_at DESC);

                CREATE TABLE IF NOT EXISTS maintenance_runs (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress REAL NOT NULL DEFAULT 0,
                    message TEXT,
                    options_json TEXT,
                    summary_json TEXT,
                    details_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_maintenance_runs_created_at ON maintenance_runs(created_at DESC);

                CREATE TABLE IF NOT EXISTS zip_entries_cache (
                    media_id TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    entry_count INTEGER NOT NULL DEFAULT 0,
                    payload_bytes INTEGER NOT NULL DEFAULT 0,
                    entries_blob BLOB NOT NULL,
                    created_at TEXT NOT NULL,
                    last_accessed_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_zip_entries_cache_accessed
                    ON zip_entries_cache(last_accessed_at ASC);

                CREATE TABLE IF NOT EXISTS subscriptions (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    normalized_source_url TEXT NOT NULL,
                    source_kind TEXT NOT NULL DEFAULT 'auto',
                    folder TEXT NOT NULL DEFAULT 'General',
                    tags TEXT NOT NULL DEFAULT '',
                    format_spec TEXT,
                    forward_to_telegram INTEGER NOT NULL DEFAULT 1,
                    interval_minutes INTEGER NOT NULL DEFAULT 60,
                    max_items_per_check INTEGER NOT NULL DEFAULT 10,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    seed_only INTEGER NOT NULL DEFAULT 1,
                    initial_backfill_limit INTEGER NOT NULL DEFAULT 0,
                    seeded_at TEXT,
                    title_include_regex TEXT,
                    title_exclude_regex TEXT,
                    last_checked_at TEXT,
                    next_check_at TEXT,
                    last_status TEXT NOT NULL DEFAULT 'pending',
                    last_error TEXT,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    items_ingested_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS subscription_entries (
                    subscription_id TEXT NOT NULL,
                    entry_key TEXT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL,
                    normalized_url TEXT,
                    status TEXT NOT NULL DEFAULT 'seen',
                    media_id TEXT,
                    job_id TEXT,
                    error TEXT,
                    published_at TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_attempt_at TEXT,
                    PRIMARY KEY (subscription_id, entry_key),
                    FOREIGN KEY (subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE
                );
                """
            )
            # Safe migration for subscription tables created by an earlier schema revision.
            subscription_cols = {
                r["name"] for r in conn.execute("PRAGMA table_info(subscriptions)").fetchall()
            }
            for col_name, col_ddl in (
                ("normalized_source_url", "TEXT NOT NULL DEFAULT ''"),
                ("source_kind", "TEXT NOT NULL DEFAULT 'auto'"),
                ("folder", "TEXT NOT NULL DEFAULT 'General'"),
                ("tags", "TEXT NOT NULL DEFAULT ''"),
                ("format_spec", "TEXT"),
                ("forward_to_telegram", "INTEGER NOT NULL DEFAULT 1"),
                ("interval_minutes", "INTEGER NOT NULL DEFAULT 60"),
                ("max_items_per_check", "INTEGER NOT NULL DEFAULT 10"),
                ("enabled", "INTEGER NOT NULL DEFAULT 1"),
                ("seed_only", "INTEGER NOT NULL DEFAULT 1"),
                ("initial_backfill_limit", "INTEGER NOT NULL DEFAULT 0"),
                ("seeded_at", "TEXT"),
                ("title_include_regex", "TEXT"),
                ("title_exclude_regex", "TEXT"),
                ("last_checked_at", "TEXT"),
                ("next_check_at", "TEXT"),
                ("last_status", "TEXT NOT NULL DEFAULT 'pending'"),
                ("last_error", "TEXT"),
                ("consecutive_failures", "INTEGER NOT NULL DEFAULT 0"),
                ("items_ingested_count", "INTEGER NOT NULL DEFAULT 0"),
                ("updated_at", "TEXT NOT NULL DEFAULT ''"),
            ):
                if col_name not in subscription_cols:
                    conn.execute(f"ALTER TABLE subscriptions ADD COLUMN {col_name} {col_ddl}")
            subscription_entry_cols = {
                r["name"] for r in conn.execute("PRAGMA table_info(subscription_entries)").fetchall()
            }
            for col_name, col_ddl in (
                ("title", "TEXT NOT NULL DEFAULT ''"),
                ("url", "TEXT NOT NULL DEFAULT ''"),
                ("normalized_url", "TEXT"),
                ("status", "TEXT NOT NULL DEFAULT 'seen'"),
                ("media_id", "TEXT"),
                ("job_id", "TEXT"),
                ("error", "TEXT"),
                ("published_at", "TEXT"),
                ("first_seen_at", "TEXT NOT NULL DEFAULT ''"),
                ("last_attempt_at", "TEXT"),
            ):
                if col_name not in subscription_entry_cols:
                    conn.execute(f"ALTER TABLE subscription_entries ADD COLUMN {col_name} {col_ddl}")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_due ON subscriptions(enabled, next_check_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_created ON subscriptions(created_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sub_entries_sub_seen ON subscription_entries(subscription_id, first_seen_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sub_entries_job ON subscription_entries(job_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sub_entries_norm_url ON subscription_entries(subscription_id, normalized_url)")
            # Safe migration for existing DBs created before folder / Telegram storage support
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(media_items)").fetchall()}
            if "folder" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN folder TEXT NOT NULL DEFAULT 'General'")
            if "tags" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN tags TEXT NOT NULL DEFAULT ''")
            if "sha256" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN sha256 TEXT")
            if "normalized_source_url" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN normalized_source_url TEXT")
            if "telegram_chat_id" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN telegram_chat_id TEXT")
            if "telegram_topic_id" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN telegram_topic_id INTEGER")
            if "telegram_msg_id" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN telegram_msg_id INTEGER")
            if "telegram_worker_name" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN telegram_worker_name TEXT")
            if "part_count" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN part_count INTEGER NOT NULL DEFAULT 1")
            if "duration_sec" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN duration_sec REAL")
            if "width" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN width INTEGER")
            if "height" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN height INTEGER")
            if "video_codec" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN video_codec TEXT")
            if "audio_codec" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN audio_codec TEXT")
            if "fps" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN fps REAL")
            if "preview_filename" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN preview_filename TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_media_folder ON media_items(folder)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_media_sha256 ON media_items(sha256)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_media_norm_url ON media_items(normalized_source_url)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_media_size_bytes ON media_items(size_bytes)")

            url_rows = conn.execute(
                "SELECT id, source_url FROM media_items WHERE source_url IS NOT NULL AND source_url != '' AND normalized_source_url IS NULL"
            ).fetchall()
            if url_rows:
                conn.executemany(
                    "UPDATE media_items SET normalized_source_url = ? WHERE id = ?",
                    [(normalize_source_url(r["source_url"]), r["id"]) for r in url_rows],
                )

            job_cols = {r["name"] for r in conn.execute("PRAGMA table_info(ingest_jobs)").fetchall()}
            if "progress" not in job_cols:
                conn.execute("ALTER TABLE ingest_jobs ADD COLUMN progress REAL NOT NULL DEFAULT 0")
            if "params_json" not in job_cols:
                conn.execute("ALTER TABLE ingest_jobs ADD COLUMN params_json TEXT")
            if "staged_path" not in job_cols:
                conn.execute("ALTER TABLE ingest_jobs ADD COLUMN staged_path TEXT")
            if "staged_expires_at" not in job_cols:
                conn.execute("ALTER TABLE ingest_jobs ADD COLUMN staged_expires_at TEXT")
            if "staged_size_bytes" not in job_cols:
                conn.execute("ALTER TABLE ingest_jobs ADD COLUMN staged_size_bytes INTEGER NOT NULL DEFAULT 0")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON ingest_jobs(status, created_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_staged_expires ON ingest_jobs(staged_expires_at)")

            # Backfill media_tags for legacy DBs
            tag_rows = conn.execute(
                "SELECT id, tags FROM media_items WHERE COALESCE(tags, '') != ''"
            ).fetchall()
            tag_pairs: list[tuple[str, str]] = []
            for r in tag_rows:
                for t in normalize_tags(r["tags"]):
                    tag_pairs.append((r["id"], t))
            if tag_pairs:
                conn.executemany(
                    "INSERT OR IGNORE INTO media_tags (media_id, tag) VALUES (?, ?)",
                    tag_pairs,
                )

            # Backfill media_fts for legacy DBs
            conn.execute(
                """
                INSERT INTO media_fts (media_id, title, filename, folder, tags, source_url)
                SELECT m.id, m.title, m.filename, m.folder, COALESCE(m.tags, ''), COALESCE(m.source_url, '')
                FROM media_items m
                WHERE NOT EXISTS (SELECT 1 FROM media_fts f WHERE f.media_id = m.id)
                """
            )

    def create_job(
        self,
        source_origin: str,
        source_kind: str,
        source_ref: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> str:
        job_id = uuid.uuid4().hex[:12]
        now = utc_now_iso()
        params_str = json.dumps(params or {})
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ingest_jobs (
                    id, source_origin, source_kind, source_ref, status, progress, message, media_ids_json, params_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'queued', 0, 'Queued', '[]', ?, ?, ?)
                """,
                (job_id, source_origin, source_kind, source_ref, params_str, now, now),
            )
        self.publish({"type": "job_update", "job_id": job_id, "status": "queued"})
        return job_id

    def update_job(
        self,
        job_id: str,
        *,
        status: str,
        message: str = "",
        progress: float | None = None,
        media_ids_json: str | None = None,
        staged_path: Any = _UNSET,
        staged_expires_at: Any = _UNSET,
        staged_size_bytes: Any = _UNSET,
        params: dict[str, Any] | None = None,
    ) -> None:
        now = utc_now_iso()
        if progress is None and status == "done":
            progress = 100.0
        with self._connect() as conn:
            sets = ["status = ?", "message = ?", "updated_at = ?"]
            sql_params: list[Any] = [status, message, now]
            if progress is not None:
                clamped = max(0.0, min(100.0, float(progress)))
                sets.append("progress = ?")
                sql_params.append(clamped)
            if media_ids_json is not None:
                sets.append("media_ids_json = ?")
                sql_params.append(media_ids_json)
            if staged_path is not _UNSET:
                sets.append("staged_path = ?")
                sql_params.append(staged_path)
            if staged_expires_at is not _UNSET:
                sets.append("staged_expires_at = ?")
                sql_params.append(staged_expires_at)
            if staged_size_bytes is not _UNSET:
                sets.append("staged_size_bytes = ?")
                sql_params.append(max(0, int(staged_size_bytes or 0)))
            if params is not None:
                sets.append("params_json = ?")
                sql_params.append(json.dumps(params))
            sql_params.append(job_id)
            conn.execute(
                f"UPDATE ingest_jobs SET {', '.join(sets)} WHERE id = ?",
                sql_params,
            )
        self.publish({"type": "job_update", "job_id": job_id, "status": status})

    def claim_job_for_retry(
        self, job_id: str, *, progress: float, message: str
    ) -> dict[str, Any] | None:
        now = utc_now_iso()
        clamped = max(0.0, min(100.0, float(progress)))
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE ingest_jobs
                SET status = 'queued', progress = ?, message = ?, media_ids_json = '[]',
                    staged_expires_at = NULL, updated_at = ?
                WHERE id = ? AND status IN ('failed', 'cancelled')
                """,
                (clamped, message, now, job_id),
            )
            if int(cur.rowcount or 0) != 1:
                return None
            row = conn.execute("SELECT * FROM ingest_jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            result = dict(row)
        self.publish({"type": "job_update", "job_id": job_id, "status": "queued"})
        return result

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM ingest_jobs WHERE id = ?", (job_id,)).fetchone()
            return dict(row) if row else None

    @staticmethod
    def _build_job_status_filter(status: str | None) -> tuple[str, list[Any]]:
        if not status or status == "all":
            return "", []
        if status == "active":
            return "WHERE status IN ('queued', 'downloading', 'allocating')", []
        return "WHERE status = ?", [status]

    def list_jobs(
        self,
        limit: int = 30,
        *,
        offset: int = 0,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        where_sql, sql_params = self._build_job_status_filter(status)
        safe_limit = max(1, int(limit))
        safe_offset = max(0, int(offset))
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM ingest_jobs {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                [*sql_params, safe_limit, safe_offset],
            ).fetchall()
            return [dict(r) for r in rows]

    def list_jobs_for_strip(self, limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, int(limit))
        with self._connect() as conn:
            active_rows = conn.execute(
                """
                SELECT * FROM ingest_jobs
                WHERE status IN ('queued', 'downloading', 'allocating')
                ORDER BY created_at DESC
                """
            ).fetchall()
            remaining = max(0, safe_limit - len(active_rows))
            finished_rows: list[sqlite3.Row] = []
            if remaining > 0:
                finished_rows = conn.execute(
                    """
                    SELECT * FROM ingest_jobs
                    WHERE status NOT IN ('queued', 'downloading', 'allocating')
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (remaining,),
                ).fetchall()
            return [dict(r) for r in active_rows] + [dict(r) for r in finished_rows]

    def count_jobs(self, *, status: str | None = None) -> int:
        where_sql, sql_params = self._build_job_status_filter(status)
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM ingest_jobs {where_sql}",
                sql_params,
            ).fetchone()
            return int(row[0]) if row else 0

    def get_job_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {
            "all": 0,
            "active": 0,
            "done": 0,
            "failed": 0,
            "cancelled": 0,
        }
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS cnt FROM ingest_jobs GROUP BY status"
            ).fetchall()
        for r in rows:
            st = str(r["status"])
            cnt = int(r["cnt"])
            counts["all"] += cnt
            if st in _ACTIVE_JOB_STATUSES:
                counts["active"] += cnt
            elif st in counts:
                counts[st] += cnt
        return counts

    def clear_jobs(self, statuses: list[str]) -> list[dict[str, Any]]:
        target_statuses = [
            s for s in dict.fromkeys(str(x).strip() for x in statuses)
            if s in _TERMINAL_JOB_STATUSES
        ]
        if not target_statuses:
            return []
        placeholders = ",".join("?" for _ in target_statuses)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM ingest_jobs WHERE status IN ({placeholders})",
                target_statuses,
            ).fetchall()
            if not rows:
                return []
            conn.execute(
                f"DELETE FROM ingest_jobs WHERE status IN ({placeholders})",
                target_statuses,
            )
            deleted = [dict(r) for r in rows]
        self.publish({"type": "job_update", "job_id": "*", "status": "cleared"})
        return deleted

    def list_retained_staged_jobs(self, *, include_expired: bool = True) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if include_expired:
                rows = conn.execute(
                    """
                    SELECT * FROM ingest_jobs
                    WHERE staged_path IS NOT NULL AND staged_path != ''
                    ORDER BY staged_expires_at ASC, created_at ASC
                    """
                ).fetchall()
            else:
                now = utc_now_iso()
                rows = conn.execute(
                    """
                    SELECT * FROM ingest_jobs
                    WHERE staged_path IS NOT NULL AND staged_path != ''
                      AND (staged_expires_at IS NULL OR staged_expires_at > ?)
                    ORDER BY staged_expires_at ASC, created_at ASC
                    """,
                    (now,),
                ).fetchall()
            return [dict(r) for r in rows]

    def clear_job_staged_metadata(self, job_id: str) -> None:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingest_jobs
                SET staged_path = NULL, staged_expires_at = NULL, staged_size_bytes = 0, updated_at = ?
                WHERE id = ?
                """,
                (now, job_id),
            )

    def claim_staged_job_for_cleanup(
        self,
        job_id: str,
        *,
        allowed_statuses: tuple[str, ...] = ("failed", "cancelled", "done"),
        require_expired_before: str | None = None,
    ) -> dict[str, Any] | None:
        if not allowed_statuses:
            return None
        now = utc_now_iso()
        placeholders = ",".join("?" for _ in allowed_statuses)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM ingest_jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            job = dict(row)
            status = str(job.get("status") or "")
            staged_path = job.get("staged_path")
            if status not in allowed_statuses or not staged_path:
                return None
            if require_expired_before is not None and status != "done":
                exp = job.get("staged_expires_at")
                if not exp or str(exp) > require_expired_before:
                    return None
            where_clauses = [
                "id = ?",
                f"status IN ({placeholders})",
                "staged_path IS NOT NULL AND staged_path != ''",
            ]
            params: list[Any] = [now, job_id, *allowed_statuses]
            if require_expired_before is not None:
                where_clauses.append(
                    "(status = 'done' OR (staged_expires_at IS NOT NULL AND staged_expires_at <= ?))"
                )
                params.append(require_expired_before)
            cur = conn.execute(
                f"""
                UPDATE ingest_jobs
                SET staged_path = NULL, staged_expires_at = NULL, staged_size_bytes = 0, updated_at = ?
                WHERE {' AND '.join(where_clauses)}
                """,
                params,
            )
            if int(cur.rowcount or 0) != 1:
                return None
            return job

    def recover_interrupted_jobs(
        self,
        message: str = "Interrupted by server restart",
        *,
        staged_retention_sec: int = 3600,
    ) -> int:
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        exp_iso = (
            (now_dt + timedelta(seconds=int(staged_retention_sec))).isoformat()
            if int(staged_retention_sec) > 0
            else now
        )
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE ingest_jobs
                SET status = 'failed',
                    message = ?,
                    staged_expires_at = CASE
                        WHEN staged_path IS NOT NULL AND staged_path != '' AND staged_expires_at IS NULL
                        THEN ?
                        ELSE staged_expires_at
                    END,
                    updated_at = ?
                WHERE status IN ('queued', 'downloading', 'allocating')
                """,
                (message, exp_iso, now),
            )
            count = int(cur.rowcount or 0)
        if count > 0:
            self.publish({"type": "job_update", "job_id": "*", "status": "failed"})
        return count

    @staticmethod
    def _hydrate_media(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        raw_tags = item.get("tags") or ""
        item["tags"] = normalize_tags(raw_tags) if isinstance(raw_tags, str) else normalize_tags(raw_tags)
        return item

    def insert_media(
        self,
        *,
        media_id: str,
        title: str,
        filename: str,
        folder: str,
        media_type: str,
        mime_type: str,
        size_bytes: int,
        drive_name: str,
        file_path: str,
        thumb_filename: str | None,
        source_origin: str,
        source_kind: str,
        source_url: str | None,
        sha256: str | None = None,
        tags: str | list[str] | None = None,
        telegram_chat_id: str | None = None,
        telegram_topic_id: int | None = None,
        telegram_msg_id: int | None = None,
        telegram_worker_name: str | None = None,
        part_count: int = 1,
        parts: list[dict[str, Any]] | None = None,
        duration_sec: float | None = None,
        width: int | None = None,
        height: int | None = None,
        video_codec: str | None = None,
        audio_codec: str | None = None,
        fps: float | None = None,
        preview_filename: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        resolved_part_count = len(parts) if (parts and part_count == 1) else max(1, int(part_count))
        tag_list = normalize_tags(tags)
        normalized_tags = ",".join(tag_list)
        normalized_source_url = normalize_source_url(source_url)
        clean_sha256 = sha256.strip().lower() if sha256 else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO media_items (
                    id, title, filename, folder, tags, media_type, mime_type, size_bytes,
                    drive_name, file_path, thumb_filename, source_origin,
                    source_kind, source_url, sha256, normalized_source_url,
                    telegram_chat_id, telegram_topic_id,
                    telegram_msg_id, telegram_worker_name, part_count,
                    duration_sec, width, height, video_codec, audio_codec, fps, preview_filename,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    media_id,
                    title,
                    filename,
                    folder,
                    normalized_tags,
                    media_type,
                    mime_type,
                    size_bytes,
                    drive_name,
                    file_path,
                    thumb_filename,
                    source_origin,
                    source_kind,
                    source_url,
                    clean_sha256,
                    normalized_source_url,
                    telegram_chat_id,
                    telegram_topic_id,
                    telegram_msg_id,
                    telegram_worker_name,
                    resolved_part_count,
                    duration_sec,
                    width,
                    height,
                    video_codec,
                    audio_codec,
                    fps,
                    preview_filename,
                    now,
                ),
            )
            if tag_list:
                conn.executemany(
                    "INSERT OR IGNORE INTO media_tags (media_id, tag) VALUES (?, ?)",
                    [(media_id, t) for t in tag_list],
                )
            conn.execute(
                """
                INSERT INTO media_fts (media_id, title, filename, folder, tags, source_url)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (media_id, title, filename, folder, normalized_tags, source_url or ""),
            )
            if parts:
                for p in parts:
                    conn.execute(
                        """
                        INSERT INTO media_parts (
                            media_id, part_index, start_byte, end_byte, size_bytes,
                            telegram_chat_id, telegram_topic_id, telegram_msg_id, telegram_worker_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            media_id,
                            int(p["part_index"]),
                            int(p["start_byte"]),
                            int(p["end_byte"]),
                            int(p["size_bytes"]),
                            str(p["telegram_chat_id"]),
                            p.get("telegram_topic_id"),
                            int(p["telegram_msg_id"]),
                            p.get("telegram_worker_name"),
                        ),
                    )
            row = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            item = self._hydrate_media(row)
            item["parts"] = self._resolve_parts_for_item(conn, item)
        self.publish({"type": "media_update", "media_id": media_id, "action": "insert"})
        return item

    def find_media_by_source_url(self, url: str) -> dict[str, Any] | None:
        raw = (url or "").strip()
        norm = normalize_source_url(raw)
        if not raw and not norm:
            return None
        target_norm = norm or raw
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM media_items
                WHERE normalized_source_url = ? OR source_url = ?
                ORDER BY created_at DESC LIMIT 1
                """,
                (target_norm, raw),
            ).fetchone()
            if not row:
                return None
            item = self._hydrate_media(row)
            item["parts"] = self._resolve_parts_for_item(conn, item)
            return item

    def find_media_by_sha256(self, sha256: str) -> dict[str, Any] | None:
        clean = (sha256 or "").strip().lower()
        if not clean:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM media_items WHERE sha256 = ? ORDER BY created_at DESC LIMIT 1",
                (clean,),
            ).fetchone()
            if not row:
                return None
            item = self._hydrate_media(row)
            item["parts"] = self._resolve_parts_for_item(conn, item)
            return item

    def count_media_by_size(self, size_bytes: int) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM media_items WHERE size_bytes = ?",
                (int(size_bytes),),
            ).fetchone()
            return int(row[0]) if row else 0

    def find_media_by_source_urls(self, urls: list[str]) -> dict[str, dict[str, Any]]:
        raw_inputs: list[str] = []
        raw_set: set[str] = set()
        norm_set: set[str] = set()
        norm_by_raw: dict[str, str] = {}
        for u in urls:
            raw = (u or "").strip()
            if not raw:
                continue
            if raw not in raw_set:
                raw_set.add(raw)
                raw_inputs.append(raw)
            norm = normalize_source_url(raw) or raw
            norm_by_raw[raw] = norm
            norm_set.add(norm)

        if not raw_set and not norm_set:
            return {}

        norm_list = sorted(norm_set)
        raw_list = sorted(raw_set)
        batch_size = 200

        result: dict[str, dict[str, Any]] = {}
        with self._connect() as conn:
            matched_rows: list[sqlite3.Row] = []
            seen_ids: set[str] = set()
            for i in range(0, len(norm_list), batch_size):
                chunk = norm_list[i : i + batch_size]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT * FROM media_items WHERE normalized_source_url IN ({placeholders}) ORDER BY created_at DESC",
                    chunk,
                ).fetchall()
                for row in rows:
                    mid = str(row["id"])
                    if mid not in seen_ids:
                        seen_ids.add(mid)
                        matched_rows.append(row)
            for i in range(0, len(raw_list), batch_size):
                chunk = raw_list[i : i + batch_size]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT * FROM media_items WHERE source_url IN ({placeholders}) ORDER BY created_at DESC",
                    chunk,
                ).fetchall()
                for row in rows:
                    mid = str(row["id"])
                    if mid not in seen_ids:
                        seen_ids.add(mid)
                        matched_rows.append(row)

            matched_rows.sort(key=lambda r: str(r["created_at"] or ""), reverse=True)
            for row in matched_rows:
                item = self._hydrate_media(row)
                item["parts"] = self._resolve_parts_for_item(conn, item)
                norm_val = item.get("normalized_source_url")
                raw_val = item.get("source_url")
                if norm_val and norm_val not in result:
                    result[norm_val] = item
                if raw_val and raw_val not in result:
                    result[raw_val] = item

        for raw in raw_inputs:
            if raw in result:
                continue
            norm = norm_by_raw.get(raw)
            if norm and norm in result:
                result[raw] = result[norm]
        return result

    def update_media_topic(self, media_id: str, topic_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE media_items SET telegram_topic_id = ? WHERE id = ?",
                (topic_id, media_id),
            )

    def update_media(
        self,
        media_id: str,
        *,
        title: str | None = None,
        filename: str | None = None,
        folder: str | None = None,
        file_path: str | None = None,
        tags: str | list[str] | None = None,
        telegram_chat_id: str | None = None,
        telegram_topic_id: int | None = None,
        telegram_msg_id: int | None = None,
        telegram_worker_name: str | None = None,
        part_count: int | None = None,
        parts: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            existing = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            if not existing:
                return None

            sets: list[str] = []
            params: list[Any] = []
            if title is not None:
                sets.append("title = ?")
                params.append(title)
            if filename is not None:
                sets.append("filename = ?")
                params.append(filename)
            if folder is not None:
                sets.append("folder = ?")
                params.append(folder)
            if file_path is not None:
                sets.append("file_path = ?")
                params.append(file_path)

            tag_list: list[str] | None = None
            if tags is not None:
                tag_list = normalize_tags(tags)
                sets.append("tags = ?")
                params.append(",".join(tag_list))

            if telegram_chat_id is not None:
                sets.append("telegram_chat_id = ?")
                params.append(telegram_chat_id)
            if telegram_topic_id is not None:
                sets.append("telegram_topic_id = ?")
                params.append(telegram_topic_id)
            if telegram_msg_id is not None:
                sets.append("telegram_msg_id = ?")
                params.append(telegram_msg_id)
            if telegram_worker_name is not None:
                sets.append("telegram_worker_name = ?")
                params.append(telegram_worker_name)

            resolved_part_count = part_count
            if resolved_part_count is None and parts is not None:
                resolved_part_count = max(1, len(parts))
            if resolved_part_count is not None:
                sets.append("part_count = ?")
                params.append(max(1, int(resolved_part_count)))

            if sets:
                params.append(media_id)
                conn.execute(
                    f"UPDATE media_items SET {', '.join(sets)} WHERE id = ?",
                    params,
                )

            if tag_list is not None:
                conn.execute("DELETE FROM media_tags WHERE media_id = ?", (media_id,))
                if tag_list:
                    conn.executemany(
                        "INSERT OR IGNORE INTO media_tags (media_id, tag) VALUES (?, ?)",
                        [(media_id, t) for t in tag_list],
                    )

            if parts is not None:
                conn.execute("DELETE FROM media_parts WHERE media_id = ?", (media_id,))
                for p in parts:
                    conn.execute(
                        """
                        INSERT INTO media_parts (
                            media_id, part_index, start_byte, end_byte, size_bytes,
                            telegram_chat_id, telegram_topic_id, telegram_msg_id, telegram_worker_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            media_id,
                            int(p["part_index"]),
                            int(p["start_byte"]),
                            int(p["end_byte"]),
                            int(p["size_bytes"]),
                            str(p["telegram_chat_id"]),
                            p.get("telegram_topic_id"),
                            int(p["telegram_msg_id"]),
                            p.get("telegram_worker_name"),
                        ),
                    )

            if (
                file_path is not None
                or telegram_chat_id is not None
                or telegram_msg_id is not None
                or part_count is not None
                or parts is not None
            ):
                conn.execute("DELETE FROM zip_entries_cache WHERE media_id = ?", (media_id,))

            row = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            assert row is not None
            conn.execute("DELETE FROM media_fts WHERE media_id = ?", (media_id,))
            conn.execute(
                """
                INSERT INTO media_fts (media_id, title, filename, folder, tags, source_url)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    media_id,
                    row["title"],
                    row["filename"],
                    row["folder"],
                    row["tags"] or "",
                    row["source_url"] or "",
                ),
            )
            item = self._hydrate_media(row)
            item["parts"] = self._resolve_parts_for_item(conn, item)
        self.publish({"type": "media_update", "media_id": media_id, "action": "update"})
        return item

    def update_media_tags(self, media_id: str, tags: str | list[str] | None) -> dict[str, Any] | None:
        tag_list = normalize_tags(tags)
        normalized = ",".join(tag_list)
        with self._connect() as conn:
            conn.execute(
                "UPDATE media_items SET tags = ? WHERE id = ?",
                (normalized, media_id),
            )
            row = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            if not row:
                return None
            conn.execute("DELETE FROM media_tags WHERE media_id = ?", (media_id,))
            if tag_list:
                conn.executemany(
                    "INSERT OR IGNORE INTO media_tags (media_id, tag) VALUES (?, ?)",
                    [(media_id, t) for t in tag_list],
                )
            conn.execute("DELETE FROM media_fts WHERE media_id = ?", (media_id,))
            conn.execute(
                """
                INSERT INTO media_fts (media_id, title, filename, folder, tags, source_url)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    media_id,
                    row["title"],
                    row["filename"],
                    row["folder"],
                    normalized,
                    row["source_url"] or "",
                ),
            )
            item = self._hydrate_media(row)
            item["parts"] = self._resolve_parts_for_item(conn, item)
        self.publish({"type": "media_update", "media_id": media_id, "action": "tags"})
        return item

    def list_media(
        self,
        *,
        media_type: str | None = None,
        drive_name: str | None = None,
        folder: str | None = None,
        tag: str | None = None,
        q: str | None = None,
        sort: str = "newest",
        limit: int = 60,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        base_where: list[str] = []
        base_params: list[Any] = []

        if media_type and media_type != "all":
            base_where.append("media_type = ?")
            base_params.append(media_type)
        if drive_name and drive_name != "all":
            base_where.append("drive_name = ?")
            base_params.append(drive_name)
        if folder and folder != "all":
            base_where.append("LOWER(folder) = LOWER(?)")
            base_params.append(folder)
        if tag and tag != "all":
            clean_tags = normalize_tags(tag)
            for t in clean_tags:
                base_where.append(
                    "EXISTS (SELECT 1 FROM media_tags mt WHERE mt.media_id = media_items.id AND mt.tag = ?)"
                )
                base_params.append(t)

        order_sql = "ORDER BY size_bytes DESC" if sort == "largest" else "ORDER BY created_at DESC"

        def _run_query(
            conn: sqlite3.Connection,
            extra_where: list[str],
            extra_params: list[Any],
        ) -> tuple[list[sqlite3.Row], int]:
            all_where = base_where + extra_where
            all_params = base_params + extra_params
            where_sql = f"WHERE {' AND '.join(all_where)}" if all_where else ""
            total = conn.execute(
                f"SELECT COUNT(*) FROM media_items {where_sql}",
                all_params,
            ).fetchone()[0]
            rows = conn.execute(
                f"SELECT * FROM media_items {where_sql} {order_sql} LIMIT ? OFFSET ?",
                [*all_params, limit, offset],
            ).fetchall()
            return rows, total

        with self._connect() as conn:
            if q:
                tokens = _FTS_TOKEN_RE.findall(q)
                if tokens:
                    fts_query = " AND ".join(f'"{tok}"*' for tok in tokens)
                    rows, total = _run_query(
                        conn,
                        ["media_items.id IN (SELECT media_id FROM media_fts WHERE media_fts MATCH ?)"],
                        [fts_query],
                    )
                    if total > 0:
                        return [self._hydrate_media(r) for r in rows], total

                pattern = f"%{q}%"
                rows, total = _run_query(
                    conn,
                    [
                        "(title LIKE ? OR filename LIKE ? OR folder LIKE ? OR COALESCE(tags, '') LIKE ? OR COALESCE(source_url, '') LIKE ?)"
                    ],
                    [pattern, pattern, pattern, pattern, pattern],
                )
                return [self._hydrate_media(r) for r in rows], total

            rows, total = _run_query(conn, [], [])
            return [self._hydrate_media(r) for r in rows], total

    def list_folders(self) -> list[str]:
        with self._connect() as conn:
            media_rows = conn.execute(
                """
                SELECT folder, COUNT(*) AS cnt
                FROM media_items
                WHERE folder IS NOT NULL AND folder != ''
                GROUP BY folder
                ORDER BY cnt DESC, folder ASC
                """
            ).fetchall()
            topic_rows = conn.execute(
                """
                SELECT DISTINCT folder_name AS folder
                FROM folder_topics
                WHERE folder_name IS NOT NULL AND folder_name != ''
                ORDER BY folder_name ASC
                """
            ).fetchall()

            deduped: dict[str, str] = {}
            for row in media_rows:
                name = str(row["folder"])
                key = name.casefold()
                if key not in deduped:
                    deduped[key] = name
            for row in topic_rows:
                name = str(row["folder"])
                key = name.casefold()
                if key not in deduped:
                    deduped[key] = name
            return sorted(deduped.values(), key=str.casefold)

    def list_webdav_folder_stats(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            media_rows = conn.execute(
                """
                SELECT folder, COUNT(*) AS item_count, MAX(created_at) AS latest_created_at
                FROM media_items
                WHERE folder IS NOT NULL AND folder != ''
                GROUP BY folder
                ORDER BY item_count DESC, folder ASC
                """
            ).fetchall()
            topic_rows = conn.execute(
                """
                SELECT DISTINCT folder_name AS folder
                FROM folder_topics
                WHERE folder_name IS NOT NULL AND folder_name != ''
                ORDER BY folder_name ASC
                """
            ).fetchall()

            merged: dict[str, dict[str, Any]] = {}
            for row in media_rows:
                name = str(row["folder"])
                key = name.casefold()
                item_count = int(row["item_count"] or 0)
                latest_created_at = row["latest_created_at"]
                if key in merged:
                    merged[key]["item_count"] += item_count
                    current_latest = merged[key]["latest_created_at"]
                    if current_latest is None or (
                        latest_created_at is not None and str(latest_created_at) > str(current_latest)
                    ):
                        merged[key]["latest_created_at"] = latest_created_at
                else:
                    merged[key] = {
                        "folder": name,
                        "item_count": item_count,
                        "latest_created_at": latest_created_at,
                    }

            for row in topic_rows:
                name = str(row["folder"])
                key = name.casefold()
                if key not in merged:
                    merged[key] = {
                        "folder": name,
                        "item_count": 0,
                        "latest_created_at": None,
                    }

            stats: list[dict[str, Any]] = []
            for entry in merged.values():
                stats.append(
                    {
                        "folder": entry["folder"],
                        "item_count": int(entry["item_count"]),
                        "latest_created_at": entry["latest_created_at"],
                    }
                )
            stats.sort(key=lambda e: str(e["folder"]).casefold())
            return stats

    def list_webdav_folder_items(self, folder: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM media_items
                WHERE LOWER(folder) = LOWER(?)
                ORDER BY LOWER(filename) ASC, created_at ASC, id ASC
                """,
                (folder,),
            ).fetchall()
            return [self._hydrate_media(r) for r in rows]

    def list_webdav_playlist_items(
        self,
        *,
        folder: str | None = None,
        media_type: str | None = None,
        tag: str | None = None,
    ) -> list[dict[str, Any]]:
        base_where: list[str] = []
        base_params: list[Any] = []

        clean_folder: str | None = None
        if folder and folder != "all":
            clean_folder = str(folder).strip().strip("/")
            if clean_folder:
                escaped_folder = clean_folder.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                base_where.append(
                    "(LOWER(folder) = LOWER(?) OR LOWER(folder) LIKE LOWER(?) ESCAPE '\\')"
                )
                base_params.append(clean_folder)
                base_params.append(f"{escaped_folder}/%")

        if media_type and media_type != "all":
            base_where.append("media_type = ?")
            base_params.append(media_type)

        if tag and tag != "all":
            clean_tags = normalize_tags(tag)
            for t in clean_tags:
                base_where.append(
                    "EXISTS (SELECT 1 FROM media_tags mt WHERE mt.media_id = media_items.id AND mt.tag = ?)"
                )
                base_params.append(t)

        where_sql = f"WHERE {' AND '.join(base_where)}" if base_where else ""
        order_sql = "ORDER BY LOWER(folder) ASC, LOWER(filename) ASC, created_at ASC, id ASC"

        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM media_items {where_sql} {order_sql}",
                base_params,
            ).fetchall()
            return [self._hydrate_media(r) for r in rows]

    def list_tags(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT tag FROM media_tags ORDER BY tag ASC"
            ).fetchall()
            return [r["tag"] for r in rows if r["tag"]]

    def list_tag_counts(self, q: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        norm_q = _TAG_TOKEN_RE.sub("", str(q).strip().lstrip("#").lower()) if q is not None else ""
        where_clauses = ["tag != ''"]
        params: list[Any] = []
        if norm_q:
            escaped_q = norm_q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            where_clauses.append("tag LIKE ? ESCAPE '\\'")
            params.append(f"%{escaped_q}%")

        sql = (
            "SELECT tag, COUNT(*) AS count "
            "FROM media_tags "
            f"WHERE {' AND '.join(where_clauses)} "
            "GROUP BY tag "
            "ORDER BY count DESC, tag ASC"
        )
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [{"name": r["tag"], "count": int(r["count"])} for r in rows if r["tag"]]

    def _get_media_parts_conn(self, conn: sqlite3.Connection, media_id: str) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM media_parts WHERE media_id = ? ORDER BY part_index ASC",
            (media_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def _resolve_parts_for_item(self, conn: sqlite3.Connection, item: dict[str, Any]) -> list[dict[str, Any]]:
        parts = self._get_media_parts_conn(conn, item["id"])
        if parts:
            return parts
        if item.get("telegram_chat_id") and item.get("telegram_msg_id"):
            size_bytes = int(item.get("size_bytes") or 0)
            return [
                {
                    "media_id": item["id"],
                    "part_index": 0,
                    "start_byte": 0,
                    "end_byte": max(0, size_bytes - 1),
                    "size_bytes": size_bytes,
                    "telegram_chat_id": item["telegram_chat_id"],
                    "telegram_topic_id": item.get("telegram_topic_id"),
                    "telegram_msg_id": int(item["telegram_msg_id"]),
                    "telegram_worker_name": item.get("telegram_worker_name"),
                }
            ]
        return []

    def get_media_parts(self, media_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            return self._get_media_parts_conn(conn, media_id)

    def get_media(self, media_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            if not row:
                return None
            item = self._hydrate_media(row)
            item["parts"] = self._resolve_parts_for_item(conn, item)
            return item

    def delete_media(self, media_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            if not row:
                return None
            parts = self._get_media_parts_conn(conn, media_id)
            conn.execute("DELETE FROM media_parts WHERE media_id = ?", (media_id,))
            conn.execute("DELETE FROM media_tags WHERE media_id = ?", (media_id,))
            conn.execute("DELETE FROM media_fts WHERE media_id = ?", (media_id,))
            conn.execute("DELETE FROM zip_entries_cache WHERE media_id = ?", (media_id,))
            conn.execute("DELETE FROM media_items WHERE id = ?", (media_id,))
            item = self._hydrate_media(row)
            item["parts"] = parts
        self.publish({"type": "media_update", "media_id": media_id, "action": "delete"})
        return item

    def get_topic_id(self, chat_id: str, folder_name: str) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT topic_id FROM folder_topics WHERE chat_id = ? AND folder_name = ?",
                (chat_id, folder_name.lower()),
            ).fetchone()
            return int(row["topic_id"]) if row else None

    def save_topic_id(self, chat_id: str, folder_name: str, topic_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO folder_topics (chat_id, folder_name, topic_id)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id, folder_name) DO UPDATE SET topic_id = excluded.topic_id
                """,
                (chat_id, folder_name.lower(), topic_id),
            )

    def get_folder_by_topic_id(self, chat_id: str, topic_id: int) -> str | None:
        with self._connect() as conn:
            media_row = conn.execute(
                """
                SELECT folder FROM media_items
                WHERE telegram_chat_id = ? AND telegram_topic_id = ?
                ORDER BY created_at DESC LIMIT 1
                """,
                (chat_id, topic_id),
            ).fetchone()
            if media_row and media_row["folder"]:
                return str(media_row["folder"])
            topic_row = conn.execute(
                "SELECT folder_name FROM folder_topics WHERE chat_id = ? AND topic_id = ? LIMIT 1",
                (chat_id, topic_id),
            ).fetchone()
            return str(topic_row["folder_name"]) if topic_row and topic_row["folder_name"] else None

    def get_user_folder(self, user_id: int, default: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT default_folder FROM user_prefs WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            return str(row["default_folder"]) if row else default

    def set_user_folder(self, user_id: int, folder: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO user_prefs (user_id, default_folder)
                VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET default_folder = excluded.default_folder
                """,
                (user_id, folder),
            )

    def backup_to(self, dest_path: Path) -> None:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        # Ensure the database file is initialized and any in-flight schema setup completed.
        with self._connect():
            pass
        src = sqlite3.connect(self.db_path, timeout=30.0)
        dst = sqlite3.connect(dest_path, timeout=30.0)
        try:
            src.backup(dst, pages=256, sleep=0.005)
        finally:
            dst.close()
            src.close()

    def get_db_stats(self) -> dict[str, Any]:
        wal_path = Path(f"{self.db_path}-wal")
        size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
        wal_size_bytes = wal_path.stat().st_size if wal_path.exists() else 0
        with self._connect() as conn:
            journal_row = conn.execute("PRAGMA journal_mode").fetchone()
            journal_mode = str(journal_row[0]) if journal_row else "unknown"
            media_count = int(conn.execute("SELECT COUNT(*) FROM media_items").fetchone()[0])
            parts_count = int(conn.execute("SELECT COUNT(*) FROM media_parts").fetchone()[0])
            jobs_count = int(conn.execute("SELECT COUNT(*) FROM ingest_jobs").fetchone()[0])
            tags_count = int(
                conn.execute("SELECT COUNT(DISTINCT tag) FROM media_tags WHERE tag != ''").fetchone()[0]
            )
            backups_count = int(conn.execute("SELECT COUNT(*) FROM vault_backups").fetchone()[0])
            zip_cache_entries = int(
                conn.execute("SELECT COUNT(*) FROM zip_entries_cache").fetchone()[0]
            )
            subscriptions_count = int(
                conn.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0]
            )
        folders_count = len(self.list_folders())
        return {
            "db_path": str(self.db_path),
            "size_bytes": size_bytes,
            "wal_size_bytes": wal_size_bytes,
            "journal_mode": journal_mode,
            "media_count": media_count,
            "parts_count": parts_count,
            "jobs_count": jobs_count,
            "folders_count": folders_count,
            "tags_count": tags_count,
            "backups_count": backups_count,
            "zip_cache_entries": zip_cache_entries,
            "subscriptions_count": subscriptions_count,
        }

    def get_zip_cache_entry(self, media_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT media_id, fingerprint, entry_count, payload_bytes, entries_blob, created_at, last_accessed_at
                FROM zip_entries_cache
                WHERE media_id = ?
                """,
                (media_id,),
            ).fetchone()
            if not row:
                return None
            now = utc_now_iso()
            conn.execute(
                "UPDATE zip_entries_cache SET last_accessed_at = ? WHERE media_id = ?",
                (now, media_id),
            )
            return {
                "media_id": str(row["media_id"]),
                "fingerprint": str(row["fingerprint"]),
                "entry_count": int(row["entry_count"]),
                "payload_bytes": int(row["payload_bytes"]),
                "entries_blob": bytes(row["entries_blob"]),
                "created_at": str(row["created_at"]),
                "last_accessed_at": now,
            }

    def has_zip_cache_entry(self, media_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM zip_entries_cache WHERE media_id = ? LIMIT 1",
                (media_id,),
            ).fetchone()
            return row is not None

    def list_zip_cache_media_ids(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT media_id FROM zip_entries_cache ORDER BY last_accessed_at DESC, media_id ASC"
            ).fetchall()
            return [str(r["media_id"]) for r in rows]

    def put_zip_cache_entry(
        self,
        media_id: str,
        *,
        fingerprint: str,
        entry_count: int,
        entries_blob: bytes,
        max_rows: int = 500,
        max_bytes: int = 16 * 1024 * 1024,
    ) -> int:
        blob = bytes(entries_blob)
        payload_bytes = len(blob)
        with self._connect() as conn:
            if max_rows <= 0 or max_bytes <= 0 or payload_bytes > max_bytes:
                conn.execute("DELETE FROM zip_entries_cache WHERE media_id = ?", (media_id,))
                return 0

            now = utc_now_iso()
            conn.execute(
                """
                INSERT INTO zip_entries_cache (
                    media_id, fingerprint, entry_count, payload_bytes, entries_blob, created_at, last_accessed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(media_id) DO UPDATE SET
                    fingerprint = excluded.fingerprint,
                    entry_count = excluded.entry_count,
                    payload_bytes = excluded.payload_bytes,
                    entries_blob = excluded.entries_blob,
                    created_at = excluded.created_at,
                    last_accessed_at = excluded.last_accessed_at
                """,
                (
                    media_id,
                    fingerprint,
                    int(entry_count),
                    payload_bytes,
                    sqlite3.Binary(blob),
                    now,
                    now,
                ),
            )

            stats_row = conn.execute(
                "SELECT COUNT(*) AS cnt, COALESCE(SUM(payload_bytes), 0) AS total_bytes FROM zip_entries_cache"
            ).fetchone()
            total_rows = int(stats_row["cnt"])
            total_bytes = int(stats_row["total_bytes"])
            evicted = 0
            if total_rows > max_rows or total_bytes > max_bytes:
                candidates = conn.execute(
                    """
                    SELECT media_id, payload_bytes
                    FROM zip_entries_cache
                    WHERE media_id != ?
                    ORDER BY last_accessed_at ASC, media_id ASC
                    """,
                    (media_id,),
                ).fetchall()
                to_delete: list[str] = []
                for cand in candidates:
                    if total_rows <= max_rows and total_bytes <= max_bytes:
                        break
                    to_delete.append(str(cand["media_id"]))
                    total_rows -= 1
                    total_bytes -= int(cand["payload_bytes"])
                for i in range(0, len(to_delete), 500):
                    chunk = to_delete[i : i + 500]
                    placeholders = ",".join("?" for _ in chunk)
                    cur = conn.execute(
                        f"DELETE FROM zip_entries_cache WHERE media_id IN ({placeholders})",
                        chunk,
                    )
                    evicted += int(cur.rowcount or 0)
            return evicted

    def delete_zip_cache_entry(self, media_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM zip_entries_cache WHERE media_id = ?",
                (media_id,),
            )
            return bool(cur.rowcount and cur.rowcount > 0)

    def clear_zip_cache(self) -> int:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM zip_entries_cache")
            return int(cur.rowcount or 0)

    def get_zip_cache_db_stats(self) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt, COALESCE(SUM(payload_bytes), 0) AS total_bytes FROM zip_entries_cache"
            ).fetchone()
            return {
                "entries": int(row["cnt"]) if row else 0,
                "total_bytes": int(row["total_bytes"]) if row else 0,
            }

    def list_all_media_for_audit(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            media_rows = conn.execute(
                "SELECT * FROM media_items ORDER BY created_at DESC"
            ).fetchall()
            part_rows = conn.execute(
                "SELECT * FROM media_parts ORDER BY media_id ASC, part_index ASC"
            ).fetchall()

        parts_by_media: dict[str, list[dict[str, Any]]] = {}
        for pr in part_rows:
            d = dict(pr)
            parts_by_media.setdefault(d["media_id"], []).append(d)

        items: list[dict[str, Any]] = []
        for row in media_rows:
            item = self._hydrate_media(row)
            parts = parts_by_media.get(item["id"])
            if parts:
                item["parts"] = parts
            elif item.get("telegram_chat_id") and item.get("telegram_msg_id"):
                size_bytes = int(item.get("size_bytes") or 0)
                item["parts"] = [
                    {
                        "media_id": item["id"],
                        "part_index": 0,
                        "start_byte": 0,
                        "end_byte": max(0, size_bytes - 1),
                        "size_bytes": size_bytes,
                        "telegram_chat_id": item["telegram_chat_id"],
                        "telegram_topic_id": item.get("telegram_topic_id"),
                        "telegram_msg_id": int(item["telegram_msg_id"]),
                        "telegram_worker_name": item.get("telegram_worker_name"),
                    }
                ]
            else:
                item["parts"] = []
            items.append(item)
        return items

    def update_media_assets(
        self,
        media_id: str,
        *,
        thumb_filename: str | None = None,
        preview_filename: str | None = None,
        duration_sec: float | None = None,
        width: int | None = None,
        height: int | None = None,
        video_codec: str | None = None,
        audio_codec: str | None = None,
        fps: float | None = None,
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            existing = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            if not existing:
                return None

            sets: list[str] = []
            params: list[Any] = []
            if thumb_filename is not None:
                sets.append("thumb_filename = ?")
                params.append(thumb_filename)
            if preview_filename is not None:
                sets.append("preview_filename = ?")
                params.append(preview_filename)
            if duration_sec is not None:
                sets.append("duration_sec = ?")
                params.append(duration_sec)
            if width is not None:
                sets.append("width = ?")
                params.append(width)
            if height is not None:
                sets.append("height = ?")
                params.append(height)
            if video_codec is not None:
                sets.append("video_codec = ?")
                params.append(video_codec)
            if audio_codec is not None:
                sets.append("audio_codec = ?")
                params.append(audio_codec)
            if fps is not None:
                sets.append("fps = ?")
                params.append(fps)

            if sets:
                params.append(media_id)
                conn.execute(
                    f"UPDATE media_items SET {', '.join(sets)} WHERE id = ?",
                    params,
                )

            row = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            assert row is not None
            item = self._hydrate_media(row)
            item["parts"] = self._resolve_parts_for_item(conn, item)
        self.publish({"type": "media_update", "media_id": media_id, "action": "repair"})
        return item

    def insert_backup_record(self, record: dict[str, Any]) -> dict[str, Any]:
        backup_id = str(record.get("id") or uuid.uuid4().hex[:12])
        created_at = str(record.get("created_at") or utc_now_iso())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO vault_backups (
                    id, filename, file_path, size_bytes, uncompressed_bytes, sha256,
                    trigger, note, telegram_chat_id, telegram_topic_id,
                    telegram_msg_id, telegram_worker_name, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    backup_id,
                    str(record["filename"]),
                    str(record["file_path"]),
                    int(record["size_bytes"]),
                    int(record.get("uncompressed_bytes") or 0),
                    record.get("sha256"),
                    str(record.get("trigger") or "manual"),
                    record.get("note"),
                    record.get("telegram_chat_id"),
                    record.get("telegram_topic_id"),
                    record.get("telegram_msg_id"),
                    record.get("telegram_worker_name"),
                    created_at,
                ),
            )
            row = conn.execute("SELECT * FROM vault_backups WHERE id = ?", (backup_id,)).fetchone()
            assert row is not None
            return dict(row)

    def list_backup_records(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM vault_backups ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_backup_record(self, backup_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM vault_backups WHERE id = ?", (backup_id,)).fetchone()
            return dict(row) if row else None

    def delete_backup_record(self, backup_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM vault_backups WHERE id = ?", (backup_id,)).fetchone()
            if not row:
                return None
            conn.execute("DELETE FROM vault_backups WHERE id = ?", (backup_id,))
            return dict(row)

    def get_latest_backup_record(self) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM vault_backups ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None

    @staticmethod
    def _hydrate_maintenance_run(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        raw = dict(row)

        def _parse_dict(val: Any) -> dict[str, Any]:
            if isinstance(val, dict):
                return val
            if not val:
                return {}
            try:
                parsed = json.loads(val)
                return parsed if isinstance(parsed, dict) else {}
            except (json.JSONDecodeError, TypeError):
                return {}

        return {
            "id": raw["id"],
            "kind": raw["kind"],
            "status": raw["status"],
            "progress": float(raw["progress"]),
            "message": raw["message"] or "",
            "options": _parse_dict(raw.get("options_json")),
            "summary": _parse_dict(raw.get("summary_json")),
            "details": _parse_dict(raw.get("details_json")),
            "created_at": raw["created_at"],
            "updated_at": raw["updated_at"],
        }

    def create_maintenance_run(
        self,
        kind: str,
        *,
        status: str = "running",
        message: str = "",
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        run_id = uuid.uuid4().hex[:12]
        now = utc_now_iso()
        options_json = json.dumps(options or {})
        progress = 100.0 if status == "done" else 0.0
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO maintenance_runs (
                    id, kind, status, progress, message, options_json, summary_json, details_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, kind, status, progress, message, options_json, "{}", "{}", now, now),
            )
            row = conn.execute("SELECT * FROM maintenance_runs WHERE id = ?", (run_id,)).fetchone()
            assert row is not None
            run = self._hydrate_maintenance_run(row)
        self.publish({"type": "maintenance_update", "run_id": run_id, "kind": kind, "status": status})
        return run

    def update_maintenance_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        progress: float | None = None,
        message: str | None = None,
        summary: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        now = utc_now_iso()
        if progress is None and status == "done":
            progress = 100.0
        with self._connect() as conn:
            existing = conn.execute("SELECT * FROM maintenance_runs WHERE id = ?", (run_id,)).fetchone()
            if not existing:
                return None

            sets = ["updated_at = ?"]
            params: list[Any] = [now]
            if status is not None:
                sets.append("status = ?")
                params.append(status)
            if progress is not None:
                clamped = max(0.0, min(100.0, float(progress)))
                sets.append("progress = ?")
                params.append(clamped)
            if message is not None:
                sets.append("message = ?")
                params.append(message)
            if summary is not None:
                sets.append("summary_json = ?")
                params.append(json.dumps(summary))
            if details is not None:
                sets.append("details_json = ?")
                params.append(json.dumps(details))

            params.append(run_id)
            conn.execute(
                f"UPDATE maintenance_runs SET {', '.join(sets)} WHERE id = ?",
                params,
            )
            row = conn.execute("SELECT * FROM maintenance_runs WHERE id = ?", (run_id,)).fetchone()
            assert row is not None
            run = self._hydrate_maintenance_run(row)
        self.publish(
            {
                "type": "maintenance_update",
                "run_id": run_id,
                "kind": run["kind"],
                "status": run["status"],
            }
        )
        return run

    def get_maintenance_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM maintenance_runs WHERE id = ?", (run_id,)).fetchone()
            return self._hydrate_maintenance_run(row) if row else None

    def list_maintenance_runs(
        self, *, kind: str | None = None, limit: int = 20
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if kind is not None:
                rows = conn.execute(
                    "SELECT * FROM maintenance_runs WHERE kind = ? ORDER BY created_at DESC LIMIT ?",
                    (kind, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM maintenance_runs ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [self._hydrate_maintenance_run(r) for r in rows]

    def get_latest_maintenance_run(self, kind: str | None = None) -> dict[str, Any] | None:
        with self._connect() as conn:
            if kind is not None:
                row = conn.execute(
                    "SELECT * FROM maintenance_runs WHERE kind = ? ORDER BY created_at DESC LIMIT 1",
                    (kind,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM maintenance_runs ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
            return self._hydrate_maintenance_run(row) if row else None

    def recover_interrupted_maintenance_runs(self) -> int:
        now = utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE maintenance_runs
                SET status = 'failed',
                    message = 'Interrupted by server restart',
                    updated_at = ?
                WHERE status = 'running'
                """,
                (now,),
            )
            return int(cur.rowcount or 0)

    @staticmethod
    def _parse_received_chunks(raw_json: Any, total_chunks: int) -> list[int]:
        if not raw_json:
            return []
        try:
            parsed = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        except (json.JSONDecodeError, TypeError):
            return []
        if not isinstance(parsed, list):
            return []
        valid: set[int] = set()
        for item in parsed:
            try:
                idx = int(item)
            except (TypeError, ValueError):
                continue
            if 0 <= idx < total_chunks:
                valid.add(idx)
        return sorted(valid)

    @staticmethod
    def _compute_received_bytes(
        received_chunks: set[int] | list[int],
        total_size: int,
        chunk_size: int,
        total_chunks: int,
    ) -> int:
        if total_size <= 0 or total_chunks <= 0 or chunk_size <= 0:
            return 0
        total = 0
        for idx in received_chunks:
            if 0 <= idx < total_chunks:
                start = idx * chunk_size
                end = min(total_size, start + chunk_size)
                if end > start:
                    total += end - start
        return total

    @classmethod
    def _hydrate_upload_session(cls, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        total_chunks = max(0, int(item.get("total_chunks") or 0))
        received_chunks = cls._parse_received_chunks(
            item.get("received_chunks_json"), total_chunks
        )
        received_set = set(received_chunks)
        missing_chunks = [i for i in range(total_chunks) if i not in received_set]
        item["total_size"] = int(item.get("total_size") or 0)
        item["chunk_size"] = int(item.get("chunk_size") or 0)
        item["total_chunks"] = total_chunks
        item["received_chunks"] = received_chunks
        item["missing_chunks"] = missing_chunks
        item["received_bytes"] = int(item.get("received_bytes") or 0)
        item["tags"] = normalize_tags(item.get("tags") or "")
        item["forward_to_telegram"] = bool(item.get("forward_to_telegram"))
        item["force"] = bool(item.get("force"))
        return item

    def create_upload_session(
        self,
        *,
        session_id: str | None = None,
        filename: str,
        title: str | None = None,
        folder: str | None = None,
        tags: str | list[str] | None = None,
        forward_to_telegram: bool = True,
        force: bool = False,
        total_size: int,
        chunk_size: int,
        total_chunks: int,
        expected_sha256: str | None = None,
        fingerprint: str | None = None,
        staging_dir: str,
        staging_file: str,
        expires_at: str,
        status: str = "uploading",
    ) -> dict[str, Any]:
        sid = session_id or uuid.uuid4().hex[:16]
        now = utc_now_iso()
        norm_tags = ",".join(normalize_tags(tags))
        clean_sha = expected_sha256.strip().lower() if expected_sha256 else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO upload_sessions (
                    id, filename, title, folder, tags, forward_to_telegram, force,
                    total_size, chunk_size, total_chunks, received_chunks_json, received_bytes,
                    expected_sha256, fingerprint, staging_dir, staging_file, status, job_id,
                    created_at, updated_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', 0, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
                """,
                (
                    sid,
                    filename,
                    title,
                    folder,
                    norm_tags,
                    1 if forward_to_telegram else 0,
                    1 if force else 0,
                    int(total_size),
                    int(chunk_size),
                    int(total_chunks),
                    clean_sha,
                    fingerprint,
                    staging_dir,
                    staging_file,
                    status,
                    now,
                    now,
                    expires_at,
                ),
            )
            row = conn.execute("SELECT * FROM upload_sessions WHERE id = ?", (sid,)).fetchone()
            assert row is not None
            return self._hydrate_upload_session(row)

    def get_upload_session(self, session_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            return self._hydrate_upload_session(row) if row else None

    def list_upload_sessions(
        self, status: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, int(limit))
        with self._connect() as conn:
            if status and status != "all":
                rows = conn.execute(
                    "SELECT * FROM upload_sessions WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM upload_sessions ORDER BY created_at DESC LIMIT ?",
                    (safe_limit,),
                ).fetchall()
            return [self._hydrate_upload_session(r) for r in rows]

    def update_upload_session(
        self, session_id: str, **fields: Any
    ) -> dict[str, Any] | None:
        allowed = {
            "filename",
            "title",
            "folder",
            "tags",
            "forward_to_telegram",
            "force",
            "expected_sha256",
            "fingerprint",
            "received_chunks_json",
            "received_bytes",
            "status",
            "job_id",
            "expires_at",
            "updated_at",
        }
        updates: dict[str, Any] = {}
        for k, v in fields.items():
            if k == "received_chunks" and isinstance(v, (list, set, tuple)):
                updates["received_chunks_json"] = json.dumps(sorted({int(x) for x in v}))
                continue
            if k not in allowed:
                continue
            if k == "tags":
                updates["tags"] = ",".join(normalize_tags(v))
            elif k in ("forward_to_telegram", "force"):
                updates[k] = 1 if v else 0
            elif k == "expected_sha256":
                updates[k] = v.strip().lower() if isinstance(v, str) and v.strip() else None
            else:
                updates[k] = v

        if "updated_at" not in updates:
            updates["updated_at"] = utc_now_iso()

        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM upload_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if not existing:
                return None
            sets = [f"{col} = ?" for col in updates]
            sql_params = list(updates.values()) + [session_id]
            conn.execute(
                f"UPDATE upload_sessions SET {', '.join(sets)} WHERE id = ?",
                sql_params,
            )
            row = conn.execute(
                "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            return self._hydrate_upload_session(row) if row else None

    def mark_upload_chunk_received(
        self,
        session_id: str,
        chunk_index: int,
        chunk_bytes: int,
        expires_at: str,
    ) -> dict[str, Any] | None:
        now = utc_now_iso()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if not row:
                return None
            total_size = int(row["total_size"])
            chunk_size = int(row["chunk_size"])
            total_chunks = int(row["total_chunks"])
            received_set = set(
                self._parse_received_chunks(row["received_chunks_json"], total_chunks)
            )
            if 0 <= chunk_index < total_chunks:
                received_set.add(int(chunk_index))
            sorted_chunks = sorted(received_set)
            received_bytes = self._compute_received_bytes(
                received_set, total_size, chunk_size, total_chunks
            )
            conn.execute(
                """
                UPDATE upload_sessions
                SET received_chunks_json = ?,
                    received_bytes = ?,
                    updated_at = ?,
                    expires_at = ?
                WHERE id = ?
                """,
                (json.dumps(sorted_chunks), received_bytes, now, expires_at, session_id),
            )
            updated_row = conn.execute(
                "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            return self._hydrate_upload_session(updated_row) if updated_row else None

    def count_active_upload_sessions(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM upload_sessions WHERE status IN ('uploading', 'completing')"
            ).fetchone()
            return int(row[0]) if row else 0

    def sum_active_upload_reserved_bytes(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(total_size), 0) FROM upload_sessions WHERE status IN ('uploading', 'completing')"
            ).fetchone()
            return int(row[0]) if row else 0

    def list_expired_upload_sessions(self, now_iso: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM upload_sessions
                WHERE status = 'uploading' AND expires_at <= ?
                ORDER BY expires_at ASC
                """,
                (now_iso,),
            ).fetchall()
            return [self._hydrate_upload_session(r) for r in rows]

    def recover_interrupted_upload_sessions(self) -> int:
        now = utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE upload_sessions
                SET status = 'uploading', updated_at = ?
                WHERE status = 'completing'
                """,
                (now,),
            )
            return int(cur.rowcount or 0)

    @staticmethod
    def _hydrate_subscription(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        item["tags"] = normalize_tags(item.get("tags") or "")
        for key in ("forward_to_telegram", "enabled", "seed_only"):
            item[key] = bool(item.get(key))
        for key in (
            "interval_minutes",
            "max_items_per_check",
            "initial_backfill_limit",
            "consecutive_failures",
            "items_ingested_count",
        ):
            item[key] = int(item.get(key) or 0)
        return item

    def create_subscription(
        self,
        *,
        name: str,
        source_url: str,
        source_kind: str = "auto",
        folder: str = "General",
        tags: str | list[str] | None = None,
        format_spec: str | None = None,
        forward_to_telegram: bool = True,
        interval_minutes: int = 60,
        max_items_per_check: int = 10,
        enabled: bool = True,
        seed_only: bool = True,
        initial_backfill_limit: int = 0,
        title_include_regex: str | None = None,
        title_exclude_regex: str | None = None,
        next_check_at: str | None = None,
    ) -> dict[str, Any]:
        sub_id = uuid.uuid4().hex[:12]
        now = utc_now_iso()
        normalized_source_url = normalize_source_url(source_url) or str(source_url or "").strip()
        if not enabled:
            resolved_next_check_at = None
        elif next_check_at is None:
            resolved_next_check_at = now
        else:
            resolved_next_check_at = next_check_at

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscriptions (
                    id, name, source_url, normalized_source_url, source_kind, folder, tags,
                    format_spec, forward_to_telegram, interval_minutes, max_items_per_check,
                    enabled, seed_only, initial_backfill_limit, next_check_at,
                    title_include_regex, title_exclude_regex, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sub_id,
                    name,
                    source_url,
                    normalized_source_url,
                    source_kind,
                    folder,
                    ",".join(normalize_tags(tags)),
                    format_spec,
                    1 if forward_to_telegram else 0,
                    int(interval_minutes),
                    int(max_items_per_check),
                    1 if enabled else 0,
                    1 if seed_only else 0,
                    int(initial_backfill_limit),
                    resolved_next_check_at,
                    title_include_regex,
                    title_exclude_regex,
                    now,
                    now,
                ),
            )
            row = conn.execute("SELECT * FROM subscriptions WHERE id = ?", (sub_id,)).fetchone()
            assert row is not None
            sub = self._hydrate_subscription(row)
        self.publish({"type": "subscription_update", "subscription_id": sub_id, "action": "created"})
        return sub

    def get_subscription(self, subscription_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM subscriptions WHERE id = ?", (subscription_id,)
            ).fetchone()
            return self._hydrate_subscription(row) if row else None

    def list_subscriptions(self, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if enabled_only:
                rows = conn.execute(
                    "SELECT * FROM subscriptions WHERE enabled = 1 ORDER BY created_at DESC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM subscriptions ORDER BY created_at DESC"
                ).fetchall()
            return [self._hydrate_subscription(r) for r in rows]

    def list_due_subscriptions(self, now_iso: str, *, limit: int = 50) -> list[dict[str, Any]]:
        safe_limit = max(1, int(limit))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM subscriptions
                WHERE enabled = 1 AND (next_check_at IS NULL OR next_check_at <= ?)
                ORDER BY COALESCE(next_check_at, '') ASC, created_at ASC
                LIMIT ?
                """,
                (now_iso, safe_limit),
            ).fetchall()
            return [self._hydrate_subscription(r) for r in rows]

    def update_subscription(
        self, subscription_id: str, *, publish: bool = True, **fields: Any
    ) -> dict[str, Any] | None:
        allowed = {
            "name",
            "source_url",
            "normalized_source_url",
            "source_kind",
            "folder",
            "tags",
            "format_spec",
            "forward_to_telegram",
            "interval_minutes",
            "max_items_per_check",
            "enabled",
            "seed_only",
            "initial_backfill_limit",
            "seeded_at",
            "title_include_regex",
            "title_exclude_regex",
            "last_checked_at",
            "next_check_at",
            "last_status",
            "last_error",
            "consecutive_failures",
            "items_ingested_count",
            "updated_at",
        }
        updates: dict[str, Any] = {}
        for key, value in fields.items():
            if key not in allowed:
                continue
            if key == "source_url" and "normalized_source_url" not in fields:
                updates["normalized_source_url"] = normalize_source_url(value) or str(
                    value or ""
                ).strip()
            if key == "tags":
                updates["tags"] = ",".join(normalize_tags(value))
            elif key in ("forward_to_telegram", "enabled", "seed_only"):
                updates[key] = 1 if value else 0
            else:
                updates[key] = value
        if "updated_at" not in updates:
            updates["updated_at"] = utc_now_iso()

        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM subscriptions WHERE id = ?", (subscription_id,)
            ).fetchone()
            if not existing:
                return None
            sets = [f"{col} = ?" for col in updates]
            params: list[Any] = [*updates.values(), subscription_id]
            conn.execute(
                f"UPDATE subscriptions SET {', '.join(sets)} WHERE id = ?",
                params,
            )
            row = conn.execute(
                "SELECT * FROM subscriptions WHERE id = ?", (subscription_id,)
            ).fetchone()
            sub = self._hydrate_subscription(row) if row else None
        if publish and sub is not None:
            self.publish(
                {
                    "type": "subscription_update",
                    "subscription_id": subscription_id,
                    "action": "updated",
                }
            )
        return sub

    def increment_subscription_ingested(self, subscription_id: str, delta: int = 1) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE subscriptions
                SET items_ingested_count = items_ingested_count + ?, updated_at = ?
                WHERE id = ?
                """,
                (int(delta), utc_now_iso(), subscription_id),
            )

    def delete_subscription(self, subscription_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM subscriptions WHERE id = ?", (subscription_id,)
            ).fetchone()
            if not row:
                return None
            sub = self._hydrate_subscription(row)
            conn.execute(
                "DELETE FROM subscription_entries WHERE subscription_id = ?", (subscription_id,)
            )
            conn.execute("DELETE FROM subscriptions WHERE id = ?", (subscription_id,))
        self.publish(
            {
                "type": "subscription_update",
                "subscription_id": subscription_id,
                "action": "deleted",
            }
        )
        return sub

    def get_subscription_entries_map(
        self,
        subscription_id: str,
        entry_keys: list[str],
        normalized_urls: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        keys = list(dict.fromkeys(str(k) for k in entry_keys if k))
        urls = list(dict.fromkeys(str(u) for u in (normalized_urls or []) if u))
        if not keys and not urls:
            return {}

        matched: dict[str, dict[str, Any]] = {}
        batch_size = 200
        with self._connect() as conn:
            for i in range(0, len(keys), batch_size):
                chunk = keys[i : i + batch_size]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT * FROM subscription_entries
                    WHERE subscription_id = ? AND entry_key IN ({placeholders})
                    """,
                    [subscription_id, *chunk],
                ).fetchall()
                for row in rows:
                    matched.setdefault(str(row["entry_key"]), dict(row))
            for i in range(0, len(urls), batch_size):
                chunk = urls[i : i + batch_size]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT * FROM subscription_entries
                    WHERE subscription_id = ? AND normalized_url IN ({placeholders})
                    """,
                    [subscription_id, *chunk],
                ).fetchall()
                for row in rows:
                    matched.setdefault(str(row["entry_key"]), dict(row))

        result: dict[str, dict[str, Any]] = {}
        for entry_key, entry in matched.items():
            result[entry_key] = entry
            norm = entry.get("normalized_url")
            if norm:
                result.setdefault(str(norm), entry)
        return result

    def upsert_subscription_entry(
        self,
        subscription_id: str,
        entry_key: str,
        *,
        title: str,
        url: str,
        normalized_url: str | None = None,
        status: str = "seen",
        media_id: str | None = None,
        job_id: str | None = None,
        error: str | None = None,
        published_at: str | None = None,
        last_attempt_at: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscription_entries (
                    subscription_id, entry_key, title, url, normalized_url, status,
                    media_id, job_id, error, published_at, first_seen_at, last_attempt_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(subscription_id, entry_key) DO UPDATE SET
                    title = excluded.title,
                    url = excluded.url,
                    normalized_url = excluded.normalized_url,
                    status = excluded.status,
                    media_id = COALESCE(excluded.media_id, subscription_entries.media_id),
                    job_id = excluded.job_id,
                    error = excluded.error,
                    published_at = excluded.published_at,
                    last_attempt_at = excluded.last_attempt_at
                """,
                (
                    subscription_id,
                    entry_key,
                    title,
                    url,
                    normalized_url,
                    status,
                    media_id,
                    job_id,
                    error,
                    published_at,
                    now,
                    last_attempt_at,
                ),
            )
            row = conn.execute(
                """
                SELECT * FROM subscription_entries
                WHERE subscription_id = ? AND entry_key = ?
                """,
                (subscription_id, entry_key),
            ).fetchone()
            assert row is not None
            return dict(row)

    def update_subscription_entry_status(
        self,
        subscription_id: str,
        entry_key: str,
        *,
        status: str,
        media_id: str | None = None,
        job_id: str | None = None,
        error: str | None = None,
        last_attempt_at: str | None = None,
    ) -> None:
        sets = ["status = ?", "error = ?", "last_attempt_at = ?"]
        params: list[Any] = [status, error, last_attempt_at or utc_now_iso()]
        if media_id is not None:
            sets.append("media_id = ?")
            params.append(media_id)
        if job_id is not None:
            sets.append("job_id = ?")
            params.append(job_id)
        params.extend([subscription_id, entry_key])
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE subscription_entries
                SET {', '.join(sets)}
                WHERE subscription_id = ? AND entry_key = ?
                """,
                params,
            )

    def list_subscription_entries(
        self,
        subscription_id: str,
        *,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        where = "subscription_id = ?"
        params: list[Any] = [subscription_id]
        if status and status != "all":
            where += " AND status = ?"
            params.append(status)
        safe_limit = max(1, int(limit))
        safe_offset = max(0, int(offset))
        with self._connect() as conn:
            total = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM subscription_entries WHERE {where}", params
                ).fetchone()[0]
            )
            rows = conn.execute(
                f"""
                SELECT * FROM subscription_entries WHERE {where}
                ORDER BY COALESCE(last_attempt_at, first_seen_at) DESC, first_seen_at DESC
                LIMIT ? OFFSET ?
                """,
                [*params, safe_limit, safe_offset],
            ).fetchall()
            return [dict(r) for r in rows], total

    def reconcile_subscription_entries_with_jobs(self) -> int:
        now = utc_now_iso()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT e.subscription_id, e.entry_key,
                       j.status AS job_status, j.message AS job_message,
                       j.media_ids_json AS job_media_ids_json
                FROM subscription_entries e
                LEFT JOIN ingest_jobs j ON j.id = e.job_id
                WHERE e.status = 'queued' AND e.job_id IS NOT NULL
                """
            ).fetchall()
            reconciled = 0
            for row in rows:
                job_status = str(row["job_status"] or "")
                sub_id = str(row["subscription_id"])
                entry_key = str(row["entry_key"])
                if job_status == "done":
                    media_id: str | None = None
                    raw_ids = row["job_media_ids_json"]
                    if raw_ids:
                        try:
                            parsed = json.loads(raw_ids)
                        except (json.JSONDecodeError, TypeError):
                            parsed = []
                        if isinstance(parsed, list) and parsed:
                            media_id = str(parsed[0])
                    conn.execute(
                        """
                        UPDATE subscription_entries
                        SET status = 'ingested', media_id = ?, error = NULL, last_attempt_at = ?
                        WHERE subscription_id = ? AND entry_key = ?
                        """,
                        (media_id, now, sub_id, entry_key),
                    )
                elif job_status == "cancelled":
                    conn.execute(
                        """
                        UPDATE subscription_entries
                        SET status = 'skipped', error = 'Cancelled', last_attempt_at = ?
                        WHERE subscription_id = ? AND entry_key = ?
                        """,
                        (now, sub_id, entry_key),
                    )
                elif not job_status or job_status == "failed":
                    conn.execute(
                        """
                        UPDATE subscription_entries
                        SET status = 'failed', error = ?, last_attempt_at = ?
                        WHERE subscription_id = ? AND entry_key = ?
                        """,
                        (str(row["job_message"] or "") or "Job failed", now, sub_id, entry_key),
                    )
                else:
                    continue
                reconciled += 1
            return reconciled

    def get_subscriptions_summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END), 0) AS enabled,
                    COALESCE(SUM(CASE WHEN enabled = 0 AND COALESCE(last_status, '') != 'paused_error'
                        THEN 1 ELSE 0 END), 0) AS paused,
                    COALESCE(SUM(CASE WHEN last_status = 'error'
                        OR (enabled = 1 AND consecutive_failures > 0) THEN 1 ELSE 0 END), 0) AS errored,
                    COALESCE(SUM(CASE WHEN last_status = 'paused_error' THEN 1 ELSE 0 END), 0) AS paused_error,
                    COALESCE(SUM(items_ingested_count), 0) AS total_ingested,
                    MIN(CASE WHEN enabled = 1 THEN next_check_at ELSE NULL END) AS next_check_at
                FROM subscriptions
                """
            ).fetchone()
            total_entries = int(
                conn.execute("SELECT COUNT(*) FROM subscription_entries").fetchone()[0]
            )
        return {
            "total": int(row["total"] or 0),
            "enabled": int(row["enabled"] or 0),
            "paused": int(row["paused"] or 0),
            "errored": int(row["errored"] or 0),
            "paused_error": int(row["paused_error"] or 0),
            "total_entries": total_entries,
            "total_ingested": int(row["total_ingested"] or 0),
            "next_check_at": row["next_check_at"] or None,
        }

