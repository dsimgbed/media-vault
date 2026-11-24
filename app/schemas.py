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
    max_playlist_items: int = 500
    force: bool = False


class PlaylistRequest(BaseModel):
    url: str
    max_items: int = 200


class QuickCaptureRequest(BaseModel):
    url: str = ""
    text: str | None = None
    folder: str | None = None
    tags: list[str] | str | None = None
    title: str | None = Field(default=None, max_length=300)
    format_spec: str | None = None
    forward_to_telegram: bool = True
    force: bool = False
    source: str = "quick"


class CheckDuplicateRequest(BaseModel):
    url: str | None = None
    urls: list[str] | None = Field(default=None, max_length=500)
    sha256: str | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    filename: str | None = None


class UploadSessionInitRequest(BaseModel):
    filename: str = Field(..., min_length=1, max_length=512)
    total_size: int = Field(..., ge=0)
    chunk_size: int | None = Field(default=None, ge=65536)
    title: str | None = None
    folder: str | None = None
    tags: list[str] | str | None = None
    forward_to_telegram: bool = True
    force: bool = False
    sha256: str | None = None
    fingerprint: str | None = Field(default=None, max_length=512)


class UploadSessionCompleteRequest(BaseModel):
    sha256: str | None = None


class RetryJobRequest(BaseModel):
    force: bool = False


class ClearJobsRequest(BaseModel):
    statuses: list[Literal["done", "cancelled", "failed"]] = Field(
        default_factory=lambda: ["done", "cancelled"]
    )


class RetryFailedJobsRequest(BaseModel):
    force: bool = False
    include_cancelled: bool = False
    limit: int = Field(default=50, ge=1, le=200)


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
    auto_retry_failed: bool = True
    retry_unchanged: bool = True


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


class CachePurgeRequest(BaseModel):
    scope: Literal["all", "stream", "stream_ram", "stream_disk", "zip"] = "all"


class SubscriptionCreateRequest(BaseModel):
    name: str | None = Field(default=None, max_length=200)
    source_url: str = Field(..., min_length=1, max_length=2048)
    source_kind: Literal["auto", "rss", "ytdlp"] = "auto"
    folder: str | None = Field(default=None, max_length=120)
    tags: list[str] | str | None = None
    format_spec: str | None = Field(default=None, max_length=100)
    forward_to_telegram: bool = True
    interval_minutes: int = Field(default=60, ge=5, le=10080)
    max_items_per_check: int = Field(default=10, ge=1, le=100)
    enabled: bool = True
    seed_only: bool = True
    initial_backfill_limit: int = Field(default=0, ge=0, le=50)
    archive_existing: bool = False
    title_include_regex: str | None = Field(default=None, max_length=256)
    title_exclude_regex: str | None = Field(default=None, max_length=256)


class SubscriptionUpdateRequest(BaseModel):
    name: str | None = None
    source_url: str | None = None
    source_kind: Literal["auto", "rss", "ytdlp"] | None = None
    folder: str | None = None
    tags: list[str] | str | None = None
    format_spec: str | None = None
    forward_to_telegram: bool | None = None
    interval_minutes: int | None = Field(default=None, ge=5, le=10080)
    max_items_per_check: int | None = Field(default=None, ge=1, le=100)
    enabled: bool | None = None
    seed_only: bool | None = None
    initial_backfill_limit: int | None = Field(default=None, ge=0, le=50)
    archive_existing: bool | None = None
    title_include_regex: str | None = Field(default=None, max_length=256)
    title_exclude_regex: str | None = Field(default=None, max_length=256)


class SubscriptionCheckRequest(BaseModel):
    dry_run: bool = False
    full_catalog: bool = False
    queue_existing: bool = False


class SubscriptionQueueEntryRequest(BaseModel):
    entry_key: str = Field(..., min_length=1, max_length=2048)


class SubscriptionBackfillRequest(BaseModel):
    limit: int | None = Field(default=None, ge=1, le=5000)
    all: bool = False
    statuses: list[Literal["seen", "skipped", "failed"]] = Field(
        default_factory=lambda: ["seen"]
    )


class TorrentInspectRequest(BaseModel):
    magnet: str = Field(..., min_length=1, max_length=8192)


class TorrentIngestRequest(BaseModel):
    inspect_id: str | None = Field(default=None, max_length=64)
    magnet: str | None = Field(default=None, max_length=8192)
    selected_indices: list[int] | None = None
    folder: str | None = None
    use_torrent_subfolder: bool = True
    tags: list[str] | str | None = None
    forward_to_telegram: bool = True
    force: bool = False


class GalleryIngestRequest(BaseModel):
    url: str = Field(..., min_length=1, max_length=4096)
    folder: str | None = None
    tags: list[str] | str | None = None
    title: str | None = Field(default=None, max_length=300)
    archive_mode: Literal["items", "cbz", "zip"] = "items"
    max_items: int = Field(default=200, ge=1, le=500)
    forward_to_telegram: bool = True
    force: bool = False


class WatchPartyCreateRequest(BaseModel):
    title: str | None = Field(default=None, max_length=120)
    media_ids: list[str] = Field(default_factory=list, max_length=200)
    active_media_id: str | None = Field(default=None, max_length=128)
    pin: str | None = Field(default=None, max_length=32)
    control_mode: Literal["host_only", "everyone"] = "host_only"
    wait_for_buffering: bool = False
    ttl_sec: int | None = Field(default=None, ge=300, le=86400)
    max_participants: int | None = Field(default=None, ge=2, le=100)
    host_name: str = Field(default="Host", min_length=1, max_length=40)
    initial_position_sec: float = Field(default=0.0, ge=0.0)
    initial_is_playing: bool = False
    initial_playback_rate: float = Field(default=1.0, ge=0.25, le=4.0)


class WatchPartyUpdateRequest(BaseModel):
    title: str | None = Field(default=None, max_length=120)
    control_mode: Literal["host_only", "everyone"] | None = None
    wait_for_buffering: bool | None = None
    queue: list[str] | None = Field(default=None, max_length=200)
    add_media_ids: list[str] | None = Field(default=None, max_length=200)
    remove_media_ids: list[str] | None = Field(default=None, max_length=200)
    active_media_id: str | None = Field(default=None, max_length=128)
    pin: str | None = Field(default=None, max_length=32)
    clear_pin: bool = False
    extend_ttl_sec: int | None = Field(default=None, ge=300, le=86400)


class WatchPartyJoinRequest(BaseModel):
    display_name: str = Field(..., min_length=1, max_length=40)
    pin: str | None = Field(default=None, max_length=32)


class WatchPartyHostSessionRequest(BaseModel):
    display_name: str = Field(default="Host", min_length=1, max_length=40)


class WatchPartyKickRequest(BaseModel):
    participant_id: str = Field(..., min_length=1, max_length=128)

