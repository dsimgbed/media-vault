from __future__ import annotations

import asyncio
import contextlib
import json
import re
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
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
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON ingest_jobs(created_at DESC);

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
                """
            )
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
    ) -> None:
        now = utc_now_iso()
        if progress is None and status == "done":
            progress = 100.0
        with self._connect() as conn:
            sets = ["status = ?", "message = ?", "updated_at = ?"]
            params: list[Any] = [status, message, now]
            if progress is not None:
                clamped = max(0.0, min(100.0, float(progress)))
                sets.append("progress = ?")
                params.append(clamped)
            if media_ids_json is not None:
                sets.append("media_ids_json = ?")
                params.append(media_ids_json)
            params.append(job_id)
            conn.execute(
                f"UPDATE ingest_jobs SET {', '.join(sets)} WHERE id = ?",
                params,
            )
        self.publish({"type": "job_update", "job_id": job_id, "status": status})

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM ingest_jobs WHERE id = ?", (job_id,)).fetchone()
            return dict(row) if row else None

    def list_jobs(self, limit: int = 30) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM ingest_jobs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def recover_interrupted_jobs(self, message: str = "Interrupted by server restart") -> int:
        now = utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE ingest_jobs
                SET status = 'failed', message = ?, updated_at = ?
                WHERE status IN ('queued', 'downloading', 'allocating')
                """,
                (message, now),
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
            base_where.append("folder = ?")
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
            rows = conn.execute(
                """
                SELECT DISTINCT folder FROM media_items
                UNION
                SELECT DISTINCT folder_name AS folder FROM folder_topics
                ORDER BY folder ASC
                """
            ).fetchall()
            return [r["folder"] for r in rows if r["folder"]]

    def list_tags(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT tag FROM media_tags ORDER BY tag ASC"
            ).fetchall()
            return [r["tag"] for r in rows if r["tag"]]

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
