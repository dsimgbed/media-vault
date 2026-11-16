from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    ytdlp_cookies_file: Path = Path("./data/cookies.txt")
    ytdlp_proxy: str = ""

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
    def min_free_buffer_bytes(self) -> int:
        return self.min_free_buffer_mb * 1024 * 1024

    @property
    def max_upload_bytes(self) -> int:
        return self.max_upload_mb * 1024 * 1024

    @property
    def telegram_part_size_bytes(self) -> int:
        return self.telegram_part_size_mb * 1024 * 1024

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

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.thumbs_dir.mkdir(parents=True, exist_ok=True)
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
