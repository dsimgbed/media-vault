from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

_CAPTURE_CORS_SCHEMES = {"http", "https", "chrome-extension", "moz-extension"}


class DriveConfig(BaseModel):
    name: str
    path: str


class TelegramApiConfig(BaseModel):
    name: str
    api_id: int
    api_hash: str
    bot_token: str = ""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = "0.0.0.0"
    port: int = 8080
    public_base_url: str = "http://localhost:8080"
    web_auth_token: str = ""
    # One-tap capture (bookmarklet / browser extension) CORS allowlist.
    # Comma-separated origins, e.g. "https://vault.example.com,chrome-extension://abc..."
    capture_cors_origins: str = ""
    # Allow any well-formed Chrome/Firefox extension origin (use only if needed).
    capture_cors_allow_extensions: bool = False
    max_upload_mb: int = 20480
    telegram_part_size_mb: int = 1900

    data_dir: Path = Path("./data")
    staging_dir: Path = Path("./data/staging")

    drives_json: str = "[]"
    storage_mode: Literal["telegram", "drives", "both"] = "telegram"
    allocation_strategy: Literal["first_fit", "most_free"] = "first_fit"
    min_free_buffer_mb: int = 500
    default_folder: str = "General"

    telegram_apis_json: str = "[]"
    allowed_telegram_user_ids: str = ""
    telegram_archive_chat_id: str = ""
    telegram_stream_chunk_kb: int = 512
    telegram_stream_cache_mb: int = 64
    telegram_disk_cache_mb: int = 0
    telegram_disk_cache_dir: Path | None = None
    telegram_prefetch_blocks: int = 2
    telegram_prefetch_max_concurrency: int = 4
    telegram_upload_concurrency: int = 4
    telegram_compress_photos: bool = False
    zip_cache_max_items: int = 256
    zip_cache_db_max_rows: int = 500
    zip_cache_db_max_mb: int = 16

    ytdlp_cookies_file: Path = Path("./data/cookies.txt")
    ytdlp_proxy: str = ""
    ingest_concurrency: int = 3
    ytdlp_concurrent_fragments: int = 4

    backup_dir: Path | None = None
    backup_retention_count: int = 10
    backup_interval_hours: float = 0.0
    backup_telegram_enabled: bool = False
    backup_telegram_topic: str = "_VaultBackups"
    integrity_staging_grace_sec: int = 300
    integrity_telegram_batch_size: int = 50
    integrity_telegram_batch_delay_sec: float = 0.25

    upload_chunk_size_mb: int = 8
    upload_max_chunk_size_mb: int = 32
    upload_session_ttl_sec: int = 86400
    upload_max_staging_mb: int = 0
    upload_max_concurrent_sessions: int = 50
    failed_upload_retention_sec: int = 3600
    failed_upload_max_retained_mb: int = 4096
    staging_gc_interval_sec: int = 300

    @property
    def db_path(self) -> Path:
        return self.data_dir / "media.db"

    @property
    def thumbs_dir(self) -> Path:
        return self.data_dir / "thumbs"

    @property
    def sessions_dir(self) -> Path:
        return self.data_dir / "sessions"

    @property
    def backups_dir(self) -> Path:
        return self.backup_dir if self.backup_dir is not None else (self.data_dir / "backups")

    @property
    def stream_cache_dir(self) -> Path:
        return self.telegram_disk_cache_dir if self.telegram_disk_cache_dir is not None else (self.data_dir / "stream_cache")

    @property
    def min_free_buffer_bytes(self) -> int:
        return self.min_free_buffer_mb * 1024 * 1024

    @property
    def max_upload_bytes(self) -> int:
        return self.max_upload_mb * 1024 * 1024

    @property
    def upload_chunk_size_bytes(self) -> int:
        return max(65536, int(self.upload_chunk_size_mb) * 1024 * 1024)

    @property
    def upload_max_chunk_size_bytes(self) -> int:
        return max(self.upload_chunk_size_bytes, int(self.upload_max_chunk_size_mb) * 1024 * 1024)

    @property
    def upload_max_staging_bytes(self) -> int:
        return max(0, int(self.upload_max_staging_mb)) * 1024 * 1024

    @property
    def failed_upload_max_retained_bytes(self) -> int:
        return max(0, int(self.failed_upload_max_retained_mb)) * 1024 * 1024

    @property
    def telegram_part_size_bytes(self) -> int:
        return self.telegram_part_size_mb * 1024 * 1024

    @property
    def telegram_stream_chunk_bytes(self) -> int:
        kb = max(4, min(1024, int(self.telegram_stream_chunk_kb)))
        aligned_kb = max(4, (kb // 4) * 4)
        return aligned_kb * 1024

    @property
    def telegram_stream_cache_bytes(self) -> int:
        return max(0, int(self.telegram_stream_cache_mb)) * 1024 * 1024

    @property
    def telegram_disk_cache_bytes(self) -> int:
        return max(0, int(self.telegram_disk_cache_mb)) * 1024 * 1024

    @property
    def zip_cache_db_max_bytes(self) -> int:
        return max(0, int(self.zip_cache_db_max_mb)) * 1024 * 1024

    def get_drives(self) -> list[DriveConfig]:
        raw = json.loads(self.drives_json or "[]")
        return [DriveConfig(**item) for item in raw]

    def get_telegram_apis(self) -> list[TelegramApiConfig]:
        raw = json.loads(self.telegram_apis_json or "[]")
        return [TelegramApiConfig(**item) for item in raw]

    def get_allowed_telegram_user_ids(self) -> set[int]:
        ids: set[int] = set()
        for part in self.allowed_telegram_user_ids.split(","):
            part = part.strip()
            if part.isdigit():
                ids.add(int(part))
        return ids

    def get_capture_cors_origins(self) -> list[str]:
        """Return a validated, deduplicated CORS origin allowlist.

        Wildcards are never permitted: "*" (or any origin containing "*") is
        skipped with a warning. Origins must be a bare scheme://authority with
        no path, query, or fragment.
        """
        origins: list[str] = []
        seen: set[str] = set()
        for raw in (self.capture_cors_origins or "").split(","):
            origin = raw.strip()
            if not origin:
                continue
            if "*" in origin:
                logger.warning(
                    "Ignoring wildcard capture CORS origin %r; explicit origins required",
                    origin,
                )
                continue
            origin = origin.rstrip("/")
            if not origin:
                continue
            try:
                parts = urlsplit(origin)
            except ValueError:
                logger.warning("Ignoring malformed capture CORS origin %r", raw)
                continue
            if (
                parts.scheme not in _CAPTURE_CORS_SCHEMES
                or not parts.netloc
                or parts.path not in ("", "/")
                or parts.query
                or parts.fragment
            ):
                logger.warning("Ignoring invalid capture CORS origin %r", raw)
                continue
            if origin in seen:
                continue
            seen.add(origin)
            origins.append(origin)
        return origins

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.thumbs_dir.mkdir(parents=True, exist_ok=True)
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.backups_dir.mkdir(parents=True, exist_ok=True)
        if self.telegram_disk_cache_bytes > 0:
            self.stream_cache_dir.mkdir(parents=True, exist_ok=True)
