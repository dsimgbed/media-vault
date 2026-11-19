from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class UrlIngestRequest(BaseModel):
    url: str = ""
    urls: list[str] | None = None
    folder: str | None = None
    tags: list[str] | str | None = None
    title: str | None = None
    forward_to_telegram: bool = True
    format_spec: str | None = None
    expand_playlist: bool = False
    use_playlist_folder: bool = True
    max_playlist_items: int = 50
    force: bool = False


class PlaylistRequest(BaseModel):
    url: str
    max_items: int = 50


class CheckDuplicateRequest(BaseModel):
    url: str | None = None
    sha256: str | None = None


class RetryJobRequest(BaseModel):
    force: bool = False


class UpdateMediaTagsRequest(BaseModel):
    tags: list[str] | str = ""


class UpdateMediaRequest(BaseModel):
    title: str | None = None
    folder: str | None = None
    tags: list[str] | str | None = None


class BulkMediaIdsRequest(BaseModel):
    media_ids: list[str]


class BulkMoveRequest(BaseModel):
    media_ids: list[str]
    folder: str


class BulkTagsRequest(BaseModel):
    media_ids: list[str]
    tags: list[str] | str = ""
    mode: Literal["add", "remove", "replace"] = "add"


class FormatsRequest(BaseModel):
    url: str


class CookiesRequest(BaseModel):
    cookies_text: str


class BackupCreateRequest(BaseModel):
    upload_to_telegram: bool | None = None
    note: str | None = Field(default=None, max_length=240)


class IntegrityAuditRequest(BaseModel):
    verify_telegram: bool = False
    staging_grace_sec: int | None = Field(default=None, ge=0, le=86400 * 30)
    issue_sample_limit: int = Field(default=200, ge=1, le=2000)


class IntegrityRepairRequest(BaseModel):
    backfill_thumbs: bool = True
    backfill_previews: bool = True
    backfill_metadata: bool = True
    purge_orphan_thumbs: bool = False
    purge_orphan_staging: bool = False
    staging_grace_sec: int | None = Field(default=None, ge=0, le=86400 * 30)
    max_telegram_downloads: int = Field(default=25, ge=0, le=500)
    max_telegram_item_mb: int = Field(default=250, ge=1, le=2048)
    media_ids: list[str] | None = None

