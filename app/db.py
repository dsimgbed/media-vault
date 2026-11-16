from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS media_items (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    folder TEXT NOT NULL DEFAULT 'General',
                    media_type TEXT NOT NULL, -- 'video' | 'image' | 'audio' | 'file'
                    mime_type TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    drive_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    thumb_filename TEXT,
                    source_origin TEXT NOT NULL, -- 'web' | 'telegram'
                    source_kind TEXT NOT NULL,   -- 'ytdlp' | 'ddl' | 'upload'
                    source_url TEXT,
                    telegram_chat_id TEXT,
                    telegram_topic_id INTEGER,
                    telegram_msg_id INTEGER,
                    telegram_worker_name TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_media_created_at ON media_items(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_media_type ON media_items(media_type);
                CREATE INDEX IF NOT EXISTS idx_media_drive ON media_items(drive_name);

                CREATE TABLE IF NOT EXISTS ingest_jobs (
                    id TEXT PRIMARY KEY,
                    source_origin TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    status TEXT NOT NULL, -- 'queued' | 'downloading' | 'allocating' | 'done' | 'failed'
                    progress REAL NOT NULL DEFAULT 0,
                    message TEXT,
                    media_ids_json TEXT,
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
            if "telegram_chat_id" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN telegram_chat_id TEXT")
            if "telegram_topic_id" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN telegram_topic_id INTEGER")
            if "telegram_msg_id" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN telegram_msg_id INTEGER")
            if "telegram_worker_name" not in cols:
                conn.execute("ALTER TABLE media_items ADD COLUMN telegram_worker_name TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_media_folder ON media_items(folder)")

            job_cols = {r["name"] for r in conn.execute("PRAGMA table_info(ingest_jobs)").fetchall()}
            if "progress" not in job_cols:
                conn.execute("ALTER TABLE ingest_jobs ADD COLUMN progress REAL NOT NULL DEFAULT 0")

    def create_job(self, source_origin: str, source_kind: str, source_ref: str) -> str:
        job_id = uuid.uuid4().hex[:12]
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ingest_jobs (
                    id, source_origin, source_kind, source_ref, status, progress, message, media_ids_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'queued', 0, 'Queued', '[]', ?, ?)
                """,
                (job_id, source_origin, source_kind, source_ref, now, now),
            )
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
        telegram_chat_id: str | None = None,
        telegram_topic_id: int | None = None,
        telegram_msg_id: int | None = None,
        telegram_worker_name: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO media_items (
                    id, title, filename, folder, media_type, mime_type, size_bytes,
                    drive_name, file_path, thumb_filename, source_origin,
                    source_kind, source_url, telegram_chat_id, telegram_topic_id,
                    telegram_msg_id, telegram_worker_name, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    media_id,
                    title,
                    filename,
                    folder,
                    media_type,
                    mime_type,
                    size_bytes,
                    drive_name,
                    file_path,
                    thumb_filename,
                    source_origin,
                    source_kind,
                    source_url,
                    telegram_chat_id,
                    telegram_topic_id,
                    telegram_msg_id,
                    telegram_worker_name,
                    now,
                ),
            )
            row = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            return dict(row)

    def update_media_topic(self, media_id: str, topic_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE media_items SET telegram_topic_id = ? WHERE id = ?",
                (topic_id, media_id),
            )

    def list_media(
        self,
        *,
        media_type: str | None = None,
        drive_name: str | None = None,
        folder: str | None = None,
        q: str | None = None,
        sort: str = "newest",
        limit: int = 60,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        where = []
        params: list[Any] = []

        if media_type and media_type != "all":
            where.append("media_type = ?")
            params.append(media_type)
        if drive_name and drive_name != "all":
            where.append("drive_name = ?")
            params.append(drive_name)
        if folder and folder != "all":
            where.append("folder = ?")
            params.append(folder)
        if q:
            where.append(
                "(title LIKE ? OR filename LIKE ? OR folder LIKE ? OR COALESCE(source_url, '') LIKE ?)"
            )
            pattern = f"%{q}%"
            params.extend([pattern, pattern, pattern, pattern])

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        order_sql = "ORDER BY size_bytes DESC" if sort == "largest" else "ORDER BY created_at DESC"

        with self._connect() as conn:
            total = conn.execute(
                f"SELECT COUNT(*) FROM media_items {where_sql}",
                params,
            ).fetchone()[0]
            rows = conn.execute(
                f"SELECT * FROM media_items {where_sql} {order_sql} LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
            return [dict(r) for r in rows], total

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

    def get_media(self, media_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            return dict(row) if row else None

    def delete_media(self, media_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM media_items WHERE id = ?", (media_id,)).fetchone()
            if not row:
                return None
            conn.execute("DELETE FROM media_items WHERE id = ?", (media_id,))
            return dict(row)

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
