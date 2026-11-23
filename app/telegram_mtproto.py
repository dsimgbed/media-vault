from __future__ import annotations

import asyncio
import inspect
import io
import json
import logging
import math
import re
import shutil
import tempfile
import time
import uuid
import weakref
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from telethon import TelegramClient, events
from telethon.errors import (
    ChatSendPhotosForbiddenError,
    EntityBoundsInvalidError,
    FloodWaitError,
    ImageProcessFailedError,
    MediaCaptionTooLongError,
    MessageTooLongError,
    PhotoContentTypeInvalidError,
    PhotoCropSizeSmallError,
    PhotoExtInvalidError,
    PhotoInvalidDimensionsError,
    PhotoSaveFileInvalidError,
    RPCError,
)

from app.allocator import DriveAllocator, fmt_size, sanitize_folder
from app.config import Settings, TelegramApiConfig
from app.db import Database, normalize_tags
from app.ingest import IMAGE_EXTS, IngestService, classify_media
from app.stream_cache import StreamFetchCoordinator, TelegramBlockCache, TwoTierBlockCache
from app.thumbnails import generate_thumbnail
from app.torrent_ingest import (
    MAX_TORRENT_FILE_BYTES,
    TorrentError,
    TorrentQuotaError,
    TorrentUnavailableError,
    assert_staging_disk_space,
    extract_magnet_infohash,
    find_aria2c,
    parse_torrent_bytes,
    validate_magnet_uri,
)

logger = logging.getLogger(__name__)
URL_REGEX = re.compile(r"https?://\S+")
MAGNET_REGEX = re.compile(r"magnet:\?[^\s<>\"'`]+", re.IGNORECASE)
FOLDER_HASH_RE = re.compile(r"(?:^|\s)#([A-Za-z0-9_/-]{1,64})")
FOLDER_KV_RE = re.compile(r"(?:^|\s)(?:folder|dir):([A-Za-z0-9_./-]{1,64})", re.IGNORECASE)
FORMAT_KV_RE = re.compile(r"(?:^|\s)(?:format|fmt|q):([A-Za-z0-9_+/\[\]<=.,:-]{1,48})", re.IGNORECASE)
TAG_KV_RE = re.compile(r"(?:^|\s)(?:tag|tags):([A-Za-z0-9_#,-]{1,128})", re.IGNORECASE)
FORCE_FLAG_RE = re.compile(r"(?:^|\s)(?:--force|force:true|force:1)(?:\s|$)", re.IGNORECASE)
TELEGRAM_CAPTION_MAX_UTF16 = 1024
TELEGRAM_NON_RETRYABLE_CAPTION_ERRORS = (
    MediaCaptionTooLongError,
    MessageTooLongError,
    EntityBoundsInvalidError,
)
TELEGRAM_PHOTO_FALLBACK_ERRORS = (
    PhotoInvalidDimensionsError,
    ImageProcessFailedError,
    PhotoExtInvalidError,
    PhotoContentTypeInvalidError,
    PhotoSaveFileInvalidError,
    PhotoCropSizeSmallError,
    ChatSendPhotosForbiddenError,
)
TELEGRAM_PHOTO_MAX_BYTES = 10 * 1024 * 1024
TELEGRAM_PHOTO_MAX_DIM_SUM = 10000
TELEGRAM_PHOTO_MAX_SINGLE_DIM = 4096
TELEGRAM_PHOTO_MAX_ASPECT_RATIO = 5.0
TELEGRAM_ARCHIVE_REPLY_MAX_ITEMS = 15


def _is_photo_fallback_error(exc: Exception) -> bool:
    if isinstance(exc, TELEGRAM_PHOTO_FALLBACK_ERRORS):
        return True
    if isinstance(exc, RPCError):
        upper = str(exc).upper()
        return any(
            token in upper
            for token in (
                "PHOTO_INVALID_DIMENSIONS",
                "PHOTO DIMENSIONS ARE INVALID",
                "IMAGE_PROCESS_FAILED",
                "PHOTO_EXT_INVALID",
                "PHOTO_CONTENT_TYPE_INVALID",
                "PHOTO_SAVE_FILE_INVALID",
                "PHOTO_CROP_SIZE_SMALL",
                "CHAT_SEND_PHOTOS_FORBIDDEN",
            )
        )
    return False


def _should_force_document_for_image(
    local_path: Path,
    media_type: str | None = None,
    *,
    compress_photos: bool = False,
) -> bool:
    is_img = (media_type == "image") or (local_path.suffix.lower() in IMAGE_EXTS)
    if not is_img or not local_path.is_file():
        return False
    if not compress_photos:
        return True
    try:
        file_size = local_path.stat().st_size
    except OSError:
        return False
    if file_size > TELEGRAM_PHOTO_MAX_BYTES:
        return True
    try:
        from PIL import Image

        with Image.open(local_path) as img:
            w, h = img.size
            if getattr(img, "is_animated", False) or getattr(img, "n_frames", 1) > 1:
                return True
    except Exception:
        return True

    if w <= 0 or h <= 0:
        return True
    if w > TELEGRAM_PHOTO_MAX_SINGLE_DIM or h > TELEGRAM_PHOTO_MAX_SINGLE_DIM:
        return True
    if (w + h) > TELEGRAM_PHOTO_MAX_DIM_SUM:
        return True
    if max(w / h, h / w) > TELEGRAM_PHOTO_MAX_ASPECT_RATIO:
        return True
    return False


def _utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _truncate_utf16(text: str, max_units: int, suffix: str = "...") -> str:
    if max_units <= 0:
        return ""
    if _utf16_len(text) <= max_units:
        return text
    suffix_units = _utf16_len(suffix)
    if max_units <= suffix_units:
        out: list[str] = []
        used = 0
        for ch in text:
            ch_u = _utf16_len(ch)
            if used + ch_u > max_units:
                break
            out.append(ch)
            used += ch_u
        return "".join(out)
    budget = max_units - suffix_units
    out = []
    used = 0
    for ch in text:
        ch_u = _utf16_len(ch)
        if used + ch_u > budget:
            break
        out.append(ch)
        used += ch_u
    return "".join(out) + suffix


def build_archive_caption(
    *,
    filename: str,
    folder: str,
    size_bytes: int,
    gallery_link: str,
    tags: list[str] | None = None,
    source_url: str | None = None,
    part_index: int | None = None,
    total_parts: int | None = None,
    part_size_bytes: int | None = None,
    max_len: int = TELEGRAM_CAPTION_MAX_UTF16,
) -> str:
    clean_name = " ".join(str(filename or "file").splitlines()).strip() or "file"
    clean_folder = " ".join(str(folder or "General").splitlines()).strip() or "General"
    clean_gallery = " ".join(str(gallery_link or "").splitlines()).strip()
    clean_source = " ".join(str(source_url or "").splitlines()).strip()

    is_multipart = part_index is not None and total_parts is not None
    part_suffix = f" (Part {part_index + 1}/{total_parts})" if is_multipart else ""
    meta_suffix = (
        f" • Part Size: {fmt_size(part_size_bytes or 0)} • Total: {fmt_size(size_bytes)}"
        if is_multipart
        else f" • Size: {fmt_size(size_bytes)}"
    )
    gallery_line = f"Gallery: {clean_gallery}"

    def _render_core(name_val: str, folder_val: str) -> str:
        return "\n".join(
            [
                f"**{name_val}**{part_suffix}",
                f"Folder: `{folder_val}`{meta_suffix}",
                gallery_line,
            ]
        )

    if _utf16_len(_render_core(clean_name, clean_folder)) > max_len:
        overhead = _utf16_len(_render_core("", clean_folder))
        if max_len - overhead < 24:
            folder_budget = max(8, _utf16_len(clean_folder) - (overhead + 24 - max_len))
            clean_folder = _truncate_utf16(clean_folder, folder_budget)
            overhead = _utf16_len(_render_core("", clean_folder))
        name_budget = max(1, max_len - overhead)
        clean_name = _truncate_utf16(clean_name, name_budget)

    lines = [
        f"**{clean_name}**{part_suffix}",
        f"Folder: `{clean_folder}`{meta_suffix}",
    ]

    norm_tags = normalize_tags(tags)
    if norm_tags:
        core_len = _utf16_len("\n".join([*lines, gallery_line]))
        reserve_source = min(_utf16_len(f"\nSource: {clean_source}"), 120) if clean_source else 0
        tag_budget = max(0, max_len - core_len - 1 - reserve_source)
        tag_prefix = "Tags: "
        if tag_budget > _utf16_len(tag_prefix) + 2:
            kept: list[str] = []
            used = _utf16_len(tag_prefix)
            for tag in norm_tags:
                token = f"#{tag}" if not kept else f" #{tag}"
                tok_len = _utf16_len(token)
                if used + tok_len > tag_budget:
                    break
                kept.append(token)
                used += tok_len
            if kept:
                lines.append(tag_prefix + "".join(kept))

    lines.append(gallery_line)

    if clean_source:
        source_prefix = "Source: "
        used_len = _utf16_len("\n".join(lines))
        avail = max_len - used_len - 1 - _utf16_len(source_prefix)
        if _utf16_len(clean_source) <= avail:
            lines.append(f"{source_prefix}{clean_source}")
        elif avail >= 16:
            lines.append(f"{source_prefix}{_truncate_utf16(clean_source, avail)}")

    caption = "\n".join(lines)
    if _utf16_len(caption) > max_len:
        caption = _truncate_utf16(caption, max_len)
    return caption


class FileSliceReader(io.RawIOBase):
    """Zero-copy seekable binary reader over a byte slice [start_byte, start_byte + length)."""

    def __init__(
        self,
        path: Path | str,
        start_byte: int,
        length: int,
        *,
        name: str | None = None,
    ) -> None:
        super().__init__()
        self.path = Path(path)
        self.start_byte = max(0, int(start_byte))
        self.length = max(0, int(length))
        self.name = name or self.path.name
        self._pos = 0
        self._f = open(self.path, "rb")
        self._f.seek(self.start_byte)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = int(offset)
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + int(offset)
        elif whence == io.SEEK_END:
            new_pos = self.length + int(offset)
        else:
            raise ValueError(f"Unsupported whence: {whence}")
        if new_pos < 0:
            raise ValueError("Negative seek position")
        self._pos = new_pos
        self._f.seek(self.start_byte + min(new_pos, self.length))
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if self._f.closed:
            raise ValueError("I/O operation on closed file.")
        remaining = max(0, self.length - self._pos)
        if remaining == 0:
            return b""
        if size is None or size < 0:
            to_read = remaining
        else:
            to_read = min(int(size), remaining)
        if to_read <= 0:
            return b""
        data = self._f.read(to_read)
        self._pos += len(data)
        return data

    def readinto(self, b) -> int:
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def close(self) -> None:
        f = getattr(self, "_f", None)
        if f is not None and not f.closed:
            f.close()
        super().close()

    def __enter__(self) -> FileSliceReader:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


def _extract_filename(message) -> str:
    if message.file and message.file.name:
        return Path(message.file.name).name
    ext = (message.file.ext if message.file and message.file.ext else "") or ""
    if message.photo and not ext:
        ext = ".jpg"
    elif message.video and not ext:
        ext = ".mp4"
    return f"tg_{message.id}{ext}"


def has_explicit_folder(text: str) -> bool:
    if not text:
        return False
    return bool(FOLDER_KV_RE.search(text) or FOLDER_HASH_RE.search(text))


def parse_format_from_text(text: str) -> str | None:
    if not text:
        return None
    m = FORMAT_KV_RE.search(text)
    return m.group(1).strip() if m else None


def parse_force_from_text(text: str) -> bool:
    if not text:
        return False
    return bool(FORCE_FLAG_RE.search(text))


def parse_tags_from_text(text: str) -> list[str]:
    if not text:
        return []
    found: list[str] = []
    for m in TAG_KV_RE.finditer(text):
        found.extend(normalize_tags(m.group(1)))
    if FOLDER_KV_RE.search(text):
        for hm in FOLDER_HASH_RE.finditer(text):
            found.extend(normalize_tags(hm.group(1)))
    return normalize_tags(found)


def parse_folder_from_text(text: str, fallback: str) -> str:
    if not text:
        return fallback
    m = FOLDER_KV_RE.search(text)
    if m:
        return sanitize_folder(m.group(1), fallback)
    m = FOLDER_HASH_RE.search(text)
    if m:
        return sanitize_folder(m.group(1), fallback)
    # If message has no URL and looks like a plain short folder name caption on media
    stripped = text.strip()
    if (
        stripped
        and not URL_REGEX.search(stripped)
        and not MAGNET_REGEX.search(stripped)
        and len(stripped) <= 40
        and "\n" not in stripped
    ):
        return sanitize_folder(stripped, fallback)
    return fallback


class TelegramPool:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        allocator: DriveAllocator,
        ingest: IngestService,
    ) -> None:
        self.settings = settings
        self.db = db
        self.allocator = allocator
        self.ingest = ingest
        self.subscriptions: Any = None
        self.torrent_service: Any = None
        self.clients: list[tuple[TelegramApiConfig, TelegramClient]] = []
        self._tasks: list[asyncio.Task] = []
        self._topic_lock = asyncio.Lock()
        self._claim_lock = asyncio.Lock()
        self._claimed_msgs: dict[tuple[str, int], float] = {}
        self._bot_ids: set[int] = set()
        self._rr_index = 0
        self._active_uploads: dict[str, int] = {}
        self._active_streams: dict[str, int] = {}
        self._cooldown_until: dict[str, float] = {}
        self._worker_meta: dict[str, dict[str, Any]] = {}
        self._flood_wait_counts: dict[str, int] = {}
        self._last_errors: dict[str, dict[str, str]] = {}
        self._stream_media_cache: dict[tuple[str, int, str], tuple[float, Any]] = {}
        self._stream_cache_ttl = 300.0
        self._media_cache_keys: OrderedDict[int, tuple[weakref.ref[Any] | None, tuple[str, int]]] = OrderedDict()
        self._block_cache = TwoTierBlockCache(
            max_bytes=settings.telegram_stream_cache_bytes,
            block_size=settings.telegram_stream_chunk_bytes,
            disk_max_bytes=settings.telegram_disk_cache_bytes,
            disk_dir=settings.stream_cache_dir,
        )
        self._stream_coordinator = StreamFetchCoordinator(
            self._block_cache,
            prefetch_blocks=settings.telegram_prefetch_blocks,
            max_prefetch_concurrency=settings.telegram_prefetch_max_concurrency,
        )

        # Wire mirror hook so both Web and Telegram uploads can forward to the shared Forum Supergroup
        self.ingest.mirror_hook = self.mirror_to_archive_topic

    def _record_flood_wait(self, worker_name: str, seconds: float) -> None:
        self._cooldown_until[worker_name] = time.monotonic() + max(0.0, float(seconds))
        self._flood_wait_counts[worker_name] = self._flood_wait_counts.get(worker_name, 0) + 1
        self._record_worker_error(worker_name, f"FloodWait ({seconds}s)")

    def _record_worker_error(self, worker_name: str, exc: Exception | str) -> None:
        self._last_errors[worker_name] = {
            "message": str(exc)[:300],
            "at": datetime.now(timezone.utc).isoformat(),
        }

    def _worker_name_for_client(self, client: Any) -> str | None:
        for cfg, c in self.clients:
            if c is client:
                return cfg.name
        return None

    def get_telemetry(self) -> dict[str, Any]:
        now = time.monotonic()
        workers: list[dict[str, Any]] = []
        for cfg, client in self.clients:
            meta = self._worker_meta.get(cfg.name, {})
            err = self._last_errors.get(cfg.name)
            is_conn = getattr(client, "is_connected", None)
            online = bool(is_conn()) if callable(is_conn) else True
            cooldown_rem = max(0.0, self._cooldown_until.get(cfg.name, 0.0) - now)
            workers.append(
                {
                    "name": cfg.name,
                    "api_id": cfg.api_id,
                    "username": meta.get("username"),
                    "online": online,
                    "active_uploads": int(self._active_uploads.get(cfg.name, 0)),
                    "active_streams": int(self._active_streams.get(cfg.name, 0)),
                    "in_cooldown": cooldown_rem > 0.0,
                    "cooldown_remaining_sec": round(cooldown_rem, 1),
                    "flood_wait_count": int(self._flood_wait_counts.get(cfg.name, 0)),
                    "last_error": err.get("message") if err else None,
                    "last_error_at": err.get("at") if err else None,
                }
            )
        bc_stats = self._block_cache.stats()
        coord_stats = self._stream_coordinator.stats()
        bc_stats["coalesced_waits"] = coord_stats["coalesced_waits"]
        bc_stats["prefetch"] = coord_stats["prefetch"]
        return {
            "worker_count": len(workers),
            "online_count": sum(1 for w in workers if w["online"]),
            "cooldown_count": sum(1 for w in workers if w["in_cooldown"]),
            "archive_enabled": bool(self.settings.telegram_archive_chat_id.strip() and self.clients),
            "workers": workers,
            "block_cache": bc_stats,
            "stream_media_cache_entries": len(self._stream_media_cache),
        }

    async def start(self) -> None:
        apis = self.settings.get_telegram_apis()
        if not apis:
            logger.info("TELEGRAM_APIS_JSON is empty; Telegram bot integration disabled.")
            return

        for cfg in apis:
            if not cfg.bot_token:
                logger.warning("Skipping Telegram API '%s': missing bot_token.", cfg.name)
                continue
            session_path = str(self.settings.sessions_dir / cfg.name)
            client = TelegramClient(session_path, cfg.api_id, cfg.api_hash)
            self._register_handlers(cfg, client)
            await client.start(bot_token=cfg.bot_token)
            me = await client.get_me()
            if getattr(me, "id", None):
                self._bot_ids.add(int(me.id))
            self._worker_meta[cfg.name] = {
                "username": getattr(me, "username", None),
                "bot_id": getattr(me, "id", None),
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            logger.info(
                "Started Telegram MTProto worker '%s' (@%s, api_id=%s, max 2GB)",
                cfg.name,
                getattr(me, "username", "bot"),
                cfg.api_id,
            )
            self.clients.append((cfg, client))
            self._tasks.append(asyncio.create_task(client.run_until_disconnected()))

    async def stop(self) -> None:
        await self._stream_coordinator.stop()
        for task in self._tasks:
            task.cancel()
        for _, client in self.clients:
            await client.disconnect()
        self._tasks.clear()
        self.clients.clear()
        self._bot_ids.clear()
        self._worker_meta.clear()
        self._active_streams.clear()

    def _is_authorized(self, sender_id: int | None) -> bool:
        allowed = self.settings.get_allowed_telegram_user_ids()
        return bool(sender_id and allowed and sender_id in allowed)

    def _gallery_link(self, media_id: str | None = None) -> str:
        base = self.settings.public_base_url.rstrip("/")
        return f"{base}/#media={media_id}" if media_id else base

    def _parse_archive_target(self) -> int | str | None:
        raw = self.settings.telegram_archive_chat_id.strip()
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return raw

    def _ordered_clients(
        self,
        preferred_worker: str | None = None,
        *,
        purpose: str = "upload",
    ) -> list[tuple[TelegramApiConfig, TelegramClient]]:
        if not self.clients:
            return []
        ordered = list(self.clients)
        idx = self._rr_index % len(ordered)
        self._rr_index += 1
        rotated = ordered[idx:] + ordered[:idx]
        now = time.monotonic()

        def _sort_key(pair: tuple[TelegramApiConfig, TelegramClient]) -> tuple[int, int, int] | tuple[int, int, int, int]:
            name = pair[0].name
            in_cooldown = 1 if now < self._cooldown_until.get(name, 0.0) else 0
            pref_rank = 0 if (preferred_worker and name == preferred_worker and in_cooldown == 0) else 1
            active_uploads = self._active_uploads.get(name, 0)
            active_streams = self._active_streams.get(name, 0)
            if purpose == "stream":
                return (in_cooldown, pref_rank, active_streams + active_uploads * 2)
            return (in_cooldown, pref_rank, active_uploads, active_streams)

        rotated.sort(key=_sort_key)
        return rotated

    async def _ensure_topic_id(
        self, cfg: TelegramApiConfig, chat_key: str, folder_name: str, *, force_recreate: bool = False
    ) -> int | None:
        if folder_name.strip().lower() == "general":
            return 1

        async with self._topic_lock:
            if not force_recreate:
                cached = self.db.get_topic_id(chat_key, folder_name)
                if cached:
                    return cached

            try:
                async with httpx.AsyncClient(timeout=15.0) as http:
                    resp = await http.post(
                        f"https://api.telegram.org/bot{cfg.bot_token}/createForumTopic",
                        json={"chat_id": chat_key, "name": folder_name[:128]},
                    )
                data = resp.json()
                if data.get("ok") and data.get("result", {}).get("message_thread_id"):
                    topic_id = int(data["result"]["message_thread_id"])
                    self.db.save_topic_id(chat_key, folder_name, topic_id)
                    logger.info("Created Forum Topic '%s' (id=%s) in %s", folder_name, topic_id, chat_key)
                    return topic_id
                logger.warning("createForumTopic returned %s", data)
            except Exception as exc:
                logger.warning("Could not create Forum Topic '%s' (posting to General): %s", folder_name, exc)
                return None
        return None

    async def mirror_to_archive_topic(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        target = self._parse_archive_target()
        if target is None or not self.clients:
            return None

        chat_key = str(target)
        folder = payload["folder"]
        tg_source = payload.get("tg_source")
        preferred_worker = tg_source.get("worker_name") if tg_source else None
        size_bytes = int(payload["size_bytes"])
        part_size = max(1, int(self.settings.telegram_part_size_bytes))
        progress_cb = payload.get("progress_cb")

        tags = normalize_tags(payload.get("tags"))
        gallery_link = self._gallery_link(payload["media_id"])

        if size_bytes <= part_size:
            caption = build_archive_caption(
                filename=payload["filename"],
                folder=folder,
                size_bytes=size_bytes,
                gallery_link=gallery_link,
                tags=tags,
                source_url=payload.get("source_url"),
            )
            fallback_caption = _truncate_utf16(
                " ".join(str(payload["filename"] or "file").splitlines()).strip() or "file",
                120,
            )

            last_err: Exception | None = None
            last_ul_report = 0.0

            async def _tg_upload_progress(sent: int, total: int) -> None:
                nonlocal last_ul_report
                if not progress_cb or not total:
                    return
                now = time.monotonic()
                if now - last_ul_report >= 0.9 or sent >= total:
                    last_ul_report = now
                    pct = (sent / total) * 100.0
                    await progress_cb(pct, f"Uploading to Telegram: {pct:.0f}% ({fmt_size(sent)} / {fmt_size(total)})")

            for cfg, client in self._ordered_clients(preferred_worker):
                self._active_uploads[cfg.name] = self._active_uploads.get(cfg.name, 0) + 1
                try:
                    peer = await client.get_input_entity(target)
                    topic_id = await self._ensure_topic_id(cfg, chat_key, folder)

                    async def _ensure_local_file() -> Path:
                        local_path = Path(payload["local_path"])
                        if local_path.is_file():
                            return local_path
                        if tg_source and tg_source.get("message") is not None:
                            dl_fn = tg_source.get("on_demand_download")
                            if callable(dl_fn):
                                downloaded = Path(await dl_fn())
                            else:
                                dl_raw = await tg_source["message"].download_media(file=str(local_path))
                                if not dl_raw:
                                    raise RuntimeError("On-demand Telegram download fallback returned empty path.")
                                downloaded = Path(dl_raw)
                            if not downloaded.is_file():
                                raise RuntimeError("On-demand Telegram download fallback failed to create file.")
                            payload["local_path"] = downloaded
                            return downloaded
                        raise FileNotFoundError(f"Missing staged file for upload: {local_path}")

                    use_zero_copy_media = bool(
                        tg_source
                        and cfg.name == preferred_worker
                        and getattr(tg_source.get("message"), "media", None) is not None
                    )
                    if not use_zero_copy_media:
                        await _ensure_local_file()

                    force_document = False
                    if not use_zero_copy_media:
                        force_document = await asyncio.to_thread(
                            _should_force_document_for_image,
                            Path(payload["local_path"]),
                            payload.get("media_type"),
                            compress_photos=bool(getattr(self.settings, "telegram_compress_photos", False)),
                        )

                    async def _do_send(tid: int | None, send_caption: str, *, force_doc: bool):
                        if use_zero_copy_media:
                            return await client.send_file(
                                peer,
                                tg_source["message"].media,
                                caption=send_caption,
                                reply_to=tid,
                                supports_streaming=True,
                            )
                        send_kwargs: dict[str, Any] = {
                            "caption": send_caption,
                            "reply_to": tid,
                            "progress_callback": _tg_upload_progress if progress_cb else None,
                        }
                        if force_doc:
                            send_kwargs["force_document"] = True
                        else:
                            send_kwargs["supports_streaming"] = True
                        return await client.send_file(
                            peer,
                            str(payload["local_path"]),
                            **send_kwargs,
                        )

                    async def _send_with_topic(tid: int | None, send_caption: str = caption):
                        nonlocal use_zero_copy_media, force_document
                        try:
                            return await _do_send(tid, send_caption, force_doc=force_document)
                        except RPCError as photo_err:
                            if not _is_photo_fallback_error(photo_err):
                                raise
                            if use_zero_copy_media:
                                logger.warning(
                                    "Zero-copy photo send rejected on worker '%s' (%s); falling back to document upload.",
                                    cfg.name,
                                    photo_err,
                                )
                                await _ensure_local_file()
                                use_zero_copy_media = False
                                force_document = True
                                return await _do_send(tid, send_caption, force_doc=True)
                            if not force_document:
                                logger.warning(
                                    "Telegram rejected photo '%s' on worker '%s' (%s); retrying as document.",
                                    payload["filename"],
                                    cfg.name,
                                    photo_err,
                                )
                                force_document = True
                                return await _do_send(tid, send_caption, force_doc=True)
                            raise

                    try:
                        sent = await _send_with_topic(topic_id, caption)
                    except FloodWaitError:
                        raise
                    except TELEGRAM_NON_RETRYABLE_CAPTION_ERRORS:
                        if use_zero_copy_media:
                            logger.warning(
                                "Caption rejected on worker '%s'; retrying zero-copy send with compact caption.",
                                cfg.name,
                            )
                            sent = await _send_with_topic(topic_id, fallback_caption)
                        else:
                            raise
                    except RPCError as rpc_err:
                        err_text = str(rpc_err).upper()
                        if topic_id and topic_id != 1 and ("TOPIC" in err_text or "REPLY" in err_text or "THREAD" in err_text):
                            logger.info("Cached topic %s invalid for '%s'; recreating...", topic_id, folder)
                            topic_id = await self._ensure_topic_id(cfg, chat_key, folder, force_recreate=True)
                            sent = await _send_with_topic(topic_id, caption)
                        elif use_zero_copy_media and len(self.clients) <= 1:
                            logger.warning(
                                "Zero-copy send_file failed on worker '%s' (%s); falling back to disk upload.",
                                cfg.name,
                                rpc_err,
                            )
                            await _ensure_local_file()
                            use_zero_copy_media = False
                            force_document = await asyncio.to_thread(
                                _should_force_document_for_image,
                                Path(payload["local_path"]),
                                payload.get("media_type"),
                                compress_photos=bool(getattr(self.settings, "telegram_compress_photos", False)),
                            )
                            sent = await _send_with_topic(topic_id, caption)
                        else:
                            raise

                    msg_obj = sent[0] if isinstance(sent, list) else sent
                    msg_id = int(msg_obj.id)
                    return {
                        "chat_id": chat_key,
                        "topic_id": topic_id,
                        "message_id": msg_id,
                        "worker_name": cfg.name,
                        "part_count": 1,
                        "parts": [
                            {
                                "part_index": 0,
                                "start_byte": 0,
                                "end_byte": max(0, size_bytes - 1),
                                "size_bytes": size_bytes,
                                "telegram_chat_id": chat_key,
                                "telegram_topic_id": topic_id,
                                "telegram_msg_id": msg_id,
                                "telegram_worker_name": cfg.name,
                            }
                        ],
                    }
                except FloodWaitError as fw:
                    self._record_flood_wait(cfg.name, fw.seconds)
                    logger.warning("Worker '%s' hit FloodWait (%ss) mirroring to topic; trying next worker.", cfg.name, fw.seconds)
                    last_err = fw
                except TELEGRAM_NON_RETRYABLE_CAPTION_ERRORS:
                    raise
                except Exception as exc:
                    self._record_worker_error(cfg.name, exc)
                    logger.warning("Worker '%s' failed mirroring to topic: %s", cfg.name, exc)
                    last_err = exc
                finally:
                    self._active_uploads[cfg.name] = max(0, self._active_uploads.get(cfg.name, 1) - 1)

            if last_err:
                raise last_err
            return None

        # Multi-part concurrent zero-copy upload (> part_size)
        total_parts = math.ceil(size_bytes / part_size)
        first_cfg = self._ordered_clients(preferred_worker)[0][0]
        shared_topic_id = await self._ensure_topic_id(first_cfg, chat_key, folder)

        bytes_sent_per_part = [0] * total_parts
        last_ul_report = 0.0

        async def _part_progress(part_idx: int, sent: int, _total: int) -> None:
            nonlocal last_ul_report
            bytes_sent_per_part[part_idx] = sent
            if not progress_cb or not size_bytes:
                return
            total_sent = sum(bytes_sent_per_part)
            now = time.monotonic()
            if now - last_ul_report >= 0.9 or total_sent >= size_bytes:
                last_ul_report = now
                overall_pct = min(100.0, (total_sent / size_bytes) * 100.0)
                await progress_cb(
                    overall_pct,
                    f"Uploading {total_parts} parts to Telegram: {overall_pct:.0f}% ({fmt_size(total_sent)} / {fmt_size(size_bytes)})",
                )

        concurrency = min(
            total_parts,
            max(1, len(self.clients) * max(1, int(self.settings.telegram_upload_concurrency))),
        )
        sem = asyncio.Semaphore(concurrency)

        async def _upload_part(part_index: int) -> dict[str, Any]:
            nonlocal shared_topic_id
            start_byte = part_index * part_size
            end_byte = min(size_bytes - 1, start_byte + part_size - 1)
            chunk_len = end_byte - start_byte + 1
            part_name = f"{payload['filename']}.part{part_index + 1:03d}of{total_parts:03d}"

            part_caption = build_archive_caption(
                filename=payload["filename"],
                folder=folder,
                size_bytes=size_bytes,
                gallery_link=gallery_link,
                tags=tags,
                source_url=payload.get("source_url"),
                part_index=part_index,
                total_parts=total_parts,
                part_size_bytes=chunk_len,
            )
            fallback_part_caption = _truncate_utf16(
                f"{' '.join(str(payload['filename'] or 'file').splitlines()).strip() or 'file'} (Part {part_index + 1}/{total_parts})",
                160,
            )

            async with sem:
                last_part_err: Exception | None = None
                for cfg, client in self._ordered_clients():
                    self._active_uploads[cfg.name] = self._active_uploads.get(cfg.name, 0) + 1
                    try:
                        peer = await client.get_input_entity(target)
                        with FileSliceReader(
                            payload["local_path"], start_byte, chunk_len, name=part_name
                        ) as reader:
                            uploaded_handle = await client.upload_file(
                                reader,
                                file_size=chunk_len,
                                file_name=part_name,
                                progress_callback=(
                                    lambda s, t, idx=part_index: _part_progress(idx, s, t)
                                ) if progress_cb else None,
                            )

                        part_topic_id = shared_topic_id

                        async def _send_part(tid: int | None, cap: str):
                            return await client.send_file(
                                peer,
                                uploaded_handle,
                                caption=cap,
                                reply_to=tid,
                                force_document=True,
                            )

                        try:
                            sent = await _send_part(part_topic_id, part_caption)
                        except TELEGRAM_NON_RETRYABLE_CAPTION_ERRORS:
                            logger.warning(
                                "Caption rejected for part %s/%s on worker '%s'; retrying with compact caption.",
                                part_index + 1,
                                total_parts,
                                cfg.name,
                            )
                            sent = await _send_part(part_topic_id, fallback_part_caption)
                        except RPCError as rpc_err:
                            err_text = str(rpc_err).upper()
                            if (
                                part_topic_id
                                and part_topic_id != 1
                                and ("TOPIC" in err_text or "REPLY" in err_text or "THREAD" in err_text)
                            ):
                                logger.info(
                                    "Cached topic %s invalid for '%s'; recreating...", part_topic_id, folder
                                )
                                part_topic_id = await self._ensure_topic_id(
                                    cfg, chat_key, folder, force_recreate=True
                                )
                                shared_topic_id = part_topic_id
                                try:
                                    sent = await _send_part(part_topic_id, part_caption)
                                except TELEGRAM_NON_RETRYABLE_CAPTION_ERRORS:
                                    sent = await _send_part(part_topic_id, fallback_part_caption)
                            else:
                                raise

                        msg_obj = sent[0] if isinstance(sent, list) else sent
                        bytes_sent_per_part[part_index] = chunk_len
                        return {
                            "part_index": part_index,
                            "start_byte": start_byte,
                            "end_byte": end_byte,
                            "size_bytes": chunk_len,
                            "telegram_chat_id": chat_key,
                            "telegram_topic_id": part_topic_id,
                            "telegram_msg_id": int(msg_obj.id),
                            "telegram_worker_name": cfg.name,
                        }
                    except FloodWaitError as fw:
                        self._record_flood_wait(cfg.name, fw.seconds)
                        logger.warning(
                            "Worker '%s' hit FloodWait (%ss) uploading part %s/%s; trying next worker.",
                            cfg.name,
                            fw.seconds,
                            part_index + 1,
                            total_parts,
                        )
                        last_part_err = fw
                    except TELEGRAM_NON_RETRYABLE_CAPTION_ERRORS:
                        raise
                    except Exception as exc:
                        self._record_worker_error(cfg.name, exc)
                        logger.warning(
                            "Worker '%s' failed uploading part %s/%s: %s",
                            cfg.name,
                            part_index + 1,
                            total_parts,
                            exc,
                        )
                        last_part_err = exc
                    finally:
                        self._active_uploads[cfg.name] = max(0, self._active_uploads.get(cfg.name, 1) - 1)

                if last_part_err:
                    raise last_part_err
                raise RuntimeError(f"Failed uploading part {part_index + 1}/{total_parts} to Telegram")

        parts = list(await asyncio.gather(*[_upload_part(idx) for idx in range(total_parts)]))
        parts.sort(key=lambda p: p["part_index"])
        return {
            "chat_id": chat_key,
            "topic_id": parts[0]["telegram_topic_id"],
            "message_id": parts[0]["telegram_msg_id"],
            "worker_name": parts[0]["telegram_worker_name"],
            "part_count": total_parts,
            "parts": parts,
        }

    def _prune_stream_cache(self, now: float) -> None:
        if len(self._stream_media_cache) <= 512:
            return
        expired = [k for k, (exp, _) in self._stream_media_cache.items() if now >= exp]
        for k in expired:
            self._stream_media_cache.pop(k, None)

    def _sync_block_cache_limits(self) -> None:
        self._block_cache.configure(
            max_bytes=self.settings.telegram_stream_cache_bytes,
            block_size=self.settings.telegram_stream_chunk_bytes,
            disk_max_bytes=self.settings.telegram_disk_cache_bytes,
            disk_dir=self.settings.stream_cache_dir,
        )
        self._stream_coordinator.configure(
            prefetch_blocks=self.settings.telegram_prefetch_blocks,
            max_prefetch_concurrency=self.settings.telegram_prefetch_max_concurrency,
        )

    def _remember_media_key(self, media: Any, chat_id: str, msg_id: int) -> None:
        ns = (str(chat_id), int(msg_id))
        obj_id = id(media)
        try:
            ref: weakref.ref[Any] | None = weakref.ref(media)
        except TypeError:
            ref = None
        self._media_cache_keys[obj_id] = (ref, ns)
        self._media_cache_keys.move_to_end(obj_id)
        while len(self._media_cache_keys) > 1024:
            self._media_cache_keys.popitem(last=False)

    def _resolve_media_cache_key(self, media: Any, cache_key: Any = None) -> Any:
        if cache_key is not None:
            return cache_key
        entry = self._media_cache_keys.get(id(media))
        if entry is not None:
            ref, ns = entry
            if ref is None or ref() is media:
                return ns
        return ("media", id(media))

    def invalidate_stream_cache(self, chat_id: str, msg_id: int) -> None:
        chat_str = str(chat_id)
        mid_int = int(msg_id)
        target_ns = (chat_str, mid_int)
        self._stream_coordinator.invalidate_namespace(target_ns)
        self._block_cache.invalidate(target_ns)
        for key in [k for k in self._stream_media_cache if k[0] == chat_str and k[1] == mid_int]:
            self._stream_media_cache.pop(key, None)
        for obj_id in [k for k, (_, ns) in self._media_cache_keys.items() if ns == target_ns]:
            self._media_cache_keys.pop(obj_id, None)

    async def purge_stream_caches(
        self, *, clear_ram: bool = True, clear_disk: bool = True
    ) -> dict[str, Any]:
        await self._stream_coordinator.cancel_all_background()
        if clear_disk:
            result: dict[str, Any] = await asyncio.to_thread(
                self._block_cache.purge, clear_ram=clear_ram, clear_disk=clear_disk
            )
        else:
            result = self._block_cache.purge(clear_ram=clear_ram, clear_disk=False)
        if clear_ram:
            media_entries = len(self._stream_media_cache)
            self._stream_media_cache.clear()
            self._media_cache_keys.clear()
            result["stream_media_cache_cleared"] = media_entries
        else:
            result["stream_media_cache_cleared"] = 0
        return result

    async def prepare_stream(
        self, chat_id: str, msg_id: int, worker_name: str | None = None
    ) -> tuple[TelegramClient, Any]:
        if not self.clients:
            raise RuntimeError("No Telegram workers are online to stream this file.")
        try:
            target: int | str = int(chat_id)
        except ValueError:
            target = chat_id

        now = time.monotonic()
        self._prune_stream_cache(now)
        ordered = self._ordered_clients(preferred_worker=worker_name, purpose="stream")

        for cfg, client in ordered:
            if now < self._cooldown_until.get(cfg.name, 0.0):
                continue
            cache_key = (str(chat_id), int(msg_id), cfg.name)
            cached = self._stream_media_cache.get(cache_key)
            if cached and now < cached[0]:
                self._remember_media_key(cached[1], str(chat_id), int(msg_id))
                return client, cached[1]

        last_err: Exception | None = None
        last_flood_wait: FloodWaitError | None = None
        attempted = 0
        for cfg, client in ordered:
            if time.monotonic() < self._cooldown_until.get(cfg.name, 0.0):
                continue
            attempted += 1
            try:
                peer = await client.get_input_entity(target)
                msg = await client.get_messages(peer, ids=msg_id)
                if msg and msg.media:
                    cache_key = (str(chat_id), int(msg_id), cfg.name)
                    self._stream_media_cache[cache_key] = (
                        time.monotonic() + self._stream_cache_ttl,
                        msg.media,
                    )
                    self._remember_media_key(msg.media, str(chat_id), int(msg_id))
                    return client, msg.media
            except FloodWaitError as fw:
                self._record_flood_wait(cfg.name, fw.seconds)
                logger.warning("Worker '%s' hit FloodWait (%ss) fetching stream; trying next.", cfg.name, fw.seconds)
                last_flood_wait = fw
                last_err = fw
            except Exception as exc:
                self._record_worker_error(cfg.name, exc)
                logger.warning("Worker '%s' failed resolving stream message %s: %s", cfg.name, msg_id, exc)
                last_err = exc

        if attempted == 0 and ordered:
            now_check = time.monotonic()
            min_wait = min(
                max(1, int(self._cooldown_until.get(cfg.name, 0.0) - now_check))
                for cfg, _ in ordered
            )
            raise FloodWaitError(request=None, capture=min_wait)
        if last_flood_wait is not None and isinstance(last_err, FloodWaitError):
            raise last_flood_wait
        if last_err:
            raise RuntimeError(str(last_err))
        raise FileNotFoundError("Media message no longer exists in Telegram archive.")

    @staticmethod
    async def _close_download_iterator(iterator: Any) -> None:
        close_fn = getattr(iterator, "aclose", None)
        if not callable(close_fn):
            close_fn = getattr(iterator, "close", None)
        if callable(close_fn):
            res = close_fn()
            if inspect.isawaitable(res):
                await res

    async def iter_telegram_range(
        self,
        client: TelegramClient,
        media: Any,
        start: int,
        end: int,
        *,
        cache_key: Any = None,
        total_size: int | None = None,
    ):
        if end < start:
            return
        self._sync_block_cache_limits()
        wname = self._worker_name_for_client(client)

        if not self._block_cache.enabled:
            remaining = end - start + 1
            aligned_offset = (start // 4096) * 4096
            skip = start - aligned_offset
            request_size = self.settings.telegram_stream_chunk_bytes
            iterator = client.iter_download(media, offset=aligned_offset, request_size=request_size)
            if wname:
                self._active_streams[wname] = self._active_streams.get(wname, 0) + 1
            try:
                async for chunk in iterator:
                    if remaining <= 0:
                        break
                    data = bytes(chunk)
                    if not data:
                        continue
                    if skip > 0:
                        if len(data) <= skip:
                            skip -= len(data)
                            continue
                        data = data[skip:]
                        skip = 0
                    if len(data) > remaining:
                        yield data[:remaining]
                        remaining = 0
                        break
                    remaining -= len(data)
                    yield data
            except FloodWaitError as fw:
                if wname:
                    self._record_flood_wait(wname, fw.seconds)
                raise
            except Exception as exc:
                if wname:
                    self._record_worker_error(wname, exc)
                raise
            finally:
                if wname:
                    self._active_streams[wname] = max(0, self._active_streams.get(wname, 1) - 1)
                await self._close_download_iterator(iterator)
            return

        resolved_key = self._resolve_media_cache_key(media, cache_key=cache_key)
        block_size = self._block_cache.block_size

        async def _download_canonical_block(block_offset: int) -> bytes:
            iterator = client.iter_download(media, offset=block_offset, request_size=block_size)
            buf = bytearray()
            if wname:
                self._active_streams[wname] = self._active_streams.get(wname, 0) + 1
            try:
                async for chunk in iterator:
                    if not chunk:
                        continue
                    buf.extend(chunk)
                    if len(buf) >= block_size:
                        break
                return bytes(buf[:block_size])
            except FloodWaitError as fw:
                if wname:
                    self._record_flood_wait(wname, fw.seconds)
                raise
            except Exception as exc:
                if wname:
                    self._record_worker_error(wname, exc)
                raise
            finally:
                if wname:
                    self._active_streams[wname] = max(0, self._active_streams.get(wname, 1) - 1)
                await self._close_download_iterator(iterator)

        def _can_prefetch_worker() -> bool:
            if wname:
                return time.monotonic() >= self._cooldown_until.get(wname, 0.0)
            return True

        def _on_prefetch_error(exc: Exception) -> None:
            logger.debug("Stream prefetch error for %s: %s", resolved_key, exc)

        should_prefetch = self._stream_coordinator.should_prefetch(
            resolved_key, start, end, block_size
        )
        prefetch_ahead = max(0, int(self.settings.telegram_prefetch_blocks))
        cursor = start
        reached_eof = False

        try:
            while cursor <= end:
                block_idx = cursor // block_size
                block_offset = block_idx * block_size
                data, _tier = await self._block_cache.get_block_async(resolved_key, block_offset)
                if data is not None:
                    if (
                        len(data) < block_size
                        and total_size is not None
                        and (block_offset + len(data)) < total_size
                    ):
                        # Stale/truncated cached block; re-fetch cleanly from upstream
                        data, _ = await self._stream_coordinator.fetch_block(
                            resolved_key,
                            block_offset,
                            lambda off=block_offset: _download_canonical_block(off),
                        )
                    else:
                        self._stream_coordinator.note_cache_hit(resolved_key, block_offset)
                else:
                    data, _ = await self._stream_coordinator.fetch_block(
                        resolved_key,
                        block_offset,
                        lambda off=block_offset: _download_canonical_block(off),
                    )

                if not data:
                    reached_eof = True
                    break

                if should_prefetch and prefetch_ahead > 0:
                    for step in range(1, prefetch_ahead + 1):
                        next_off = (block_idx + step) * block_size
                        if total_size is not None and next_off >= total_size:
                            break
                        self._stream_coordinator.schedule_prefetch(
                            resolved_key,
                            next_off,
                            lambda off=next_off: _download_canonical_block(off),
                            can_run_fn=_can_prefetch_worker,
                            on_error=_on_prefetch_error,
                        )

                rel_start = cursor - block_offset
                rel_end = min(len(data), end - block_offset + 1)
                if rel_end <= rel_start:
                    reached_eof = True
                    break
                out_slice = data[rel_start:rel_end]
                cursor += len(out_slice)
                yield out_slice
                if len(data) < block_size:
                    reached_eof = True
                    break
        finally:
            if should_prefetch and cursor <= end and not reached_eof:
                self._stream_coordinator.cancel_prefetch_for_namespace(resolved_key)

    async def iter_telegram_multipart_range(
        self,
        parts: list[dict[str, Any]],
        start: int,
        end: int,
        *,
        prefetched: dict[int, tuple[Any, Any]] | None = None,
    ):
        if end < start:
            return
        self._sync_block_cache_limits()
        ordered_parts = sorted(parts, key=lambda p: int(p["part_index"]))
        for idx_pos, part in enumerate(ordered_parts):
            p_start = int(part["start_byte"])
            p_end = int(part["end_byte"])
            p_size = int(part["size_bytes"])
            if p_size <= 0 or p_end < start or p_start > end:
                continue
            local_start = max(0, start - p_start)
            local_end = min(p_size - 1, end - p_start)
            if local_end < local_start:
                continue
            p_idx = int(part["part_index"])
            if prefetched and p_idx in prefetched:
                client, media = prefetched[p_idx]
            else:
                client, media = await self.prepare_stream(
                    part["telegram_chat_id"],
                    int(part["telegram_msg_id"]),
                    worker_name=None,
                )

            next_part = ordered_parts[idx_pos + 1] if (idx_pos + 1) < len(ordered_parts) else None
            prefetch_blocks = max(0, int(self.settings.telegram_prefetch_blocks))
            if next_part is not None and prefetch_blocks > 0:
                next_idx = int(next_part["part_index"])
                next_chat = str(next_part["telegram_chat_id"])
                next_mid = int(next_part["telegram_msg_id"])
                near_part_end = local_end >= max(
                    0, p_size - self._block_cache.block_size * max(1, prefetch_blocks)
                )
                if end > p_end or near_part_end:
                    now = time.monotonic()
                    in_prefetched = bool(prefetched and next_idx in prefetched)
                    already_warm = any(
                        k[0] == next_chat
                        and k[1] == next_mid
                        and now < exp
                        and now >= self._cooldown_until.get(k[2], 0.0)
                        for k, (exp, _) in self._stream_media_cache.items()
                    )
                    has_available_worker = any(
                        now >= self._cooldown_until.get(cfg.name, 0.0)
                        for cfg, _ in self.clients
                    )
                    if not in_prefetched and not already_warm and has_available_worker:
                        self._stream_coordinator.schedule_background_warm(
                            key=("prepare_part", next_chat, next_mid),
                            coro_factory=lambda c=next_chat, m=next_mid: self.prepare_stream(
                                c, m, worker_name=None
                            ),
                            namespace=(str(part["telegram_chat_id"]), int(part["telegram_msg_id"])),
                        )

            inner_iter = self.iter_telegram_range(
                client, media, local_start, local_end, total_size=p_size
            )
            try:
                async for chunk in inner_iter:
                    yield chunk
            finally:
                aclose_fn = getattr(inner_iter, "aclose", None)
                if callable(aclose_fn):
                    res = aclose_fn()
                    if inspect.isawaitable(res):
                        await res

    async def delete_archive_message(
        self, chat_id: str, msg_id: int, worker_name: str | None = None
    ) -> None:
        self.invalidate_stream_cache(chat_id, msg_id)
        if not self.clients:
            return
        try:
            target: int | str = int(chat_id)
        except ValueError:
            target = chat_id
        for cfg, client in self._ordered_clients(preferred_worker=worker_name):
            try:
                peer = await client.get_input_entity(target)
                res = await client.delete_messages(peer, [msg_id])
                if res:
                    return
            except FloodWaitError as fw:
                self._record_flood_wait(cfg.name, fw.seconds)
                logger.warning("Could not delete archive msg %s: %s", msg_id, fw)
            except Exception as exc:
                self._record_worker_error(cfg.name, exc)
                logger.warning("Could not delete archive msg %s: %s", msg_id, exc)

    async def migrate_media_topic(
        self,
        item: dict[str, Any],
        new_folder: str,
        *,
        new_title: str | None = None,
        new_tags: list[str] | None = None,
    ) -> dict[str, Any] | None:
        raw_chat = item.get("telegram_chat_id")
        if not self.clients or not raw_chat:
            return None

        chat_key = str(raw_chat)
        try:
            target: int | str = int(chat_key)
        except ValueError:
            target = chat_key

        ordered_top = self._ordered_clients(preferred_worker=item.get("telegram_worker_name"))
        if not ordered_top:
            return None
        first_cfg = ordered_top[0][0]
        new_topic_id = await self._ensure_topic_id(first_cfg, chat_key, new_folder)
        if new_topic_id == item.get("telegram_topic_id"):
            return None

        parts = list(item.get("parts") or [])
        if not parts and item.get("telegram_msg_id"):
            size_bytes = int(item.get("size_bytes") or 0)
            parts = [
                {
                    "part_index": 0,
                    "start_byte": 0,
                    "end_byte": max(0, size_bytes - 1),
                    "size_bytes": size_bytes,
                    "telegram_chat_id": chat_key,
                    "telegram_topic_id": item.get("telegram_topic_id"),
                    "telegram_msg_id": int(item["telegram_msg_id"]),
                    "telegram_worker_name": item.get("telegram_worker_name"),
                }
            ]
        if not parts:
            return None

        parts.sort(key=lambda p: int(p["part_index"]))
        total_parts = len(parts)
        is_multipart = total_parts > 1 or int(item.get("part_count") or 1) > 1
        total_size = int(item.get("size_bytes") or 0)
        filename = item.get("filename") or new_title or item.get("title") or "file"
        gallery_link = self._gallery_link(item["id"])
        resolved_tags = normalize_tags(new_tags if new_tags is not None else item.get("tags"))

        migrated_parts: list[dict[str, Any]] = []
        for p in parts:
            p_chat_key = str(p.get("telegram_chat_id") or chat_key)
            try:
                p_target: int | str = int(p_chat_key)
            except ValueError:
                p_target = p_chat_key
            old_msg_id = int(p["telegram_msg_id"])
            pref_worker = p.get("telegram_worker_name") or item.get("telegram_worker_name")

            old_msg = None
            src_cfg = None
            src_client = None
            src_peer = None
            last_err: Exception | None = None
            for cfg, client in self._ordered_clients(preferred_worker=pref_worker):
                try:
                    peer = await client.get_input_entity(p_target)
                    msg = await client.get_messages(peer, ids=old_msg_id)
                    if msg and getattr(msg, "media", None) is not None:
                        old_msg = msg
                        src_cfg = cfg
                        src_client = client
                        src_peer = peer
                        break
                except FloodWaitError as fw:
                    self._record_flood_wait(cfg.name, fw.seconds)
                    last_err = fw
                except Exception as exc:
                    self._record_worker_error(cfg.name, exc)
                    last_err = exc

            if old_msg is None or src_client is None or src_cfg is None:
                if last_err:
                    raise last_err
                raise FileNotFoundError(f"Telegram archive message {old_msg_id} not found for topic migration")

            p_idx = int(p["part_index"])
            p_size = int(p["size_bytes"])
            caption = build_archive_caption(
                filename=filename,
                folder=new_folder,
                size_bytes=total_size,
                gallery_link=gallery_link,
                tags=resolved_tags,
                source_url=item.get("source_url"),
                part_index=p_idx if is_multipart else None,
                total_parts=total_parts if is_multipart else None,
                part_size_bytes=p_size if is_multipart else None,
            )
            compact_caption = _truncate_utf16(
                f"{' '.join(str(filename).splitlines()).strip() or 'file'}"
                + (f" (Part {p_idx + 1}/{total_parts})" if is_multipart else ""),
                160,
            )

            send_kwargs: dict[str, Any] = {
                "caption": caption,
                "reply_to": new_topic_id,
            }
            if is_multipart:
                send_kwargs["force_document"] = True
            else:
                send_kwargs["supports_streaming"] = True

            try:
                sent = await src_client.send_file(src_peer, old_msg.media, **send_kwargs)
            except TELEGRAM_NON_RETRYABLE_CAPTION_ERRORS:
                send_kwargs["caption"] = compact_caption
                sent = await src_client.send_file(src_peer, old_msg.media, **send_kwargs)
            except RPCError as rpc_err:
                err_text = str(rpc_err).upper()
                if (
                    new_topic_id
                    and new_topic_id != 1
                    and ("TOPIC" in err_text or "REPLY" in err_text or "THREAD" in err_text)
                ):
                    new_topic_id = await self._ensure_topic_id(
                        src_cfg, chat_key, new_folder, force_recreate=True
                    )
                    send_kwargs["reply_to"] = new_topic_id
                    try:
                        sent = await src_client.send_file(src_peer, old_msg.media, **send_kwargs)
                    except TELEGRAM_NON_RETRYABLE_CAPTION_ERRORS:
                        send_kwargs["caption"] = compact_caption
                        sent = await src_client.send_file(src_peer, old_msg.media, **send_kwargs)
                else:
                    raise

            msg_obj = sent[0] if isinstance(sent, list) else sent
            new_msg_id = int(msg_obj.id)

            await self.delete_archive_message(p_chat_key, old_msg_id, worker_name=pref_worker)

            migrated_parts.append(
                {
                    "part_index": p_idx,
                    "start_byte": int(p["start_byte"]),
                    "end_byte": int(p["end_byte"]),
                    "size_bytes": p_size,
                    "telegram_chat_id": chat_key,
                    "telegram_topic_id": new_topic_id,
                    "telegram_msg_id": new_msg_id,
                    "telegram_worker_name": src_cfg.name,
                }
            )

        return {
            "chat_id": chat_key,
            "topic_id": new_topic_id,
            "message_id": migrated_parts[0]["telegram_msg_id"],
            "worker_name": migrated_parts[0]["telegram_worker_name"],
            "part_count": len(migrated_parts),
            "parts": migrated_parts,
        }

    async def upload_backup_file(
        self,
        local_path: Path,
        *,
        topic_name: str = "_VaultBackups",
        caption: str = "",
    ) -> dict[str, Any] | None:
        target = self._parse_archive_target()
        if target is None or not self.clients:
            return None
        local_path = Path(local_path)
        if not local_path.is_file():
            raise FileNotFoundError(f"Backup file not found: {local_path}")

        chat_key = str(target)
        send_caption = _truncate_utf16(caption or local_path.name, TELEGRAM_CAPTION_MAX_UTF16)
        last_err: Exception | None = None

        for cfg, client in self._ordered_clients():
            self._active_uploads[cfg.name] = self._active_uploads.get(cfg.name, 0) + 1
            try:
                peer = await client.get_input_entity(target)
                topic_id = await self._ensure_topic_id(cfg, chat_key, topic_name)
                try:
                    sent = await client.send_file(
                        peer,
                        str(local_path),
                        caption=send_caption,
                        reply_to=topic_id,
                        force_document=True,
                    )
                except RPCError as rpc_err:
                    err_text = str(rpc_err).upper()
                    if (
                        topic_id
                        and topic_id != 1
                        and ("TOPIC" in err_text or "REPLY" in err_text or "THREAD" in err_text)
                    ):
                        logger.info("Cached topic %s invalid for '%s'; recreating...", topic_id, topic_name)
                        topic_id = await self._ensure_topic_id(
                            cfg, chat_key, topic_name, force_recreate=True
                        )
                        sent = await client.send_file(
                            peer,
                            str(local_path),
                            caption=send_caption,
                            reply_to=topic_id,
                            force_document=True,
                        )
                    else:
                        raise
                msg_obj = sent[0] if isinstance(sent, list) else sent
                return {
                    "chat_id": chat_key,
                    "topic_id": topic_id,
                    "message_id": int(msg_obj.id),
                    "worker_name": cfg.name,
                }
            except FloodWaitError as fw:
                self._record_flood_wait(cfg.name, fw.seconds)
                logger.warning(
                    "Worker '%s' hit FloodWait (%ss) uploading backup; trying next worker.",
                    cfg.name,
                    fw.seconds,
                )
                last_err = fw
            except Exception as exc:
                self._record_worker_error(cfg.name, exc)
                logger.warning("Worker '%s' failed uploading backup: %s", cfg.name, exc)
                last_err = exc
            finally:
                self._active_uploads[cfg.name] = max(0, self._active_uploads.get(cfg.name, 1) - 1)

        if last_err:
            raise last_err
        return None

    async def verify_archive_messages(
        self,
        chat_id: str,
        msg_ids: list[int],
        *,
        batch_size: int = 50,
        delay_sec: float = 0.25,
    ) -> dict[str, Any]:
        clean_ids = sorted({int(mid) for mid in msg_ids if int(mid) > 0})
        if not clean_ids:
            return {"present": [], "missing": [], "unchecked_rate_limited": [], "errors": []}
        if not self.clients:
            return {
                "present": [],
                "missing": [],
                "unchecked_rate_limited": clean_ids,
                "errors": ["No Telegram workers online"],
            }

        try:
            target: int | str = int(chat_id)
        except ValueError:
            target = chat_id

        effective_batch = max(1, min(100, int(batch_size)))
        batches = [clean_ids[i : i + effective_batch] for i in range(0, len(clean_ids), effective_batch)]

        present: set[int] = set()
        missing: set[int] = set()
        unchecked: set[int] = set()
        errors: list[str] = []

        for b_idx, batch in enumerate(batches):
            now = time.monotonic()
            candidates = [
                (cfg, client)
                for cfg, client in self._ordered_clients()
                if now >= self._cooldown_until.get(cfg.name, 0.0)
            ]
            if not candidates:
                for rem_batch in batches[b_idx:]:
                    unchecked.update(rem_batch)
                errors.append("All Telegram workers are in FloodWait cooldown")
                break

            resolved = False
            for cfg, client in candidates:
                try:
                    peer = await client.get_input_entity(target)
                    msgs = await client.get_messages(peer, ids=batch)
                    if msgs is None:
                        normalized: list[Any] = [None] * len(batch)
                    elif isinstance(msgs, (list, tuple)):
                        normalized = list(msgs)
                    else:
                        normalized = [msgs]

                    if len(normalized) == len(batch):
                        paired = list(zip(batch, normalized))
                    else:
                        by_id = {
                            int(m.id): m
                            for m in normalized
                            if m is not None and getattr(m, "id", None) is not None
                        }
                        paired = [(mid, by_id.get(mid)) for mid in batch]

                    for mid, msg in paired:
                        if (
                            msg is not None
                            and getattr(msg, "media", None) is not None
                            and not getattr(msg, "empty", False)
                        ):
                            present.add(mid)
                        else:
                            missing.add(mid)
                    resolved = True
                    break
                except FloodWaitError as fw:
                    self._record_flood_wait(cfg.name, fw.seconds)
                    logger.warning(
                        "Worker '%s' hit FloodWait (%ss) verifying archive batch; trying next worker.",
                        cfg.name,
                        fw.seconds,
                    )
                except Exception as exc:
                    self._record_worker_error(cfg.name, exc)
                    errors.append(f"{cfg.name}: {exc}")
                    logger.warning(
                        "Worker '%s' failed verifying archive batch: %s",
                        cfg.name,
                        exc,
                    )

            if not resolved:
                now_after = time.monotonic()
                all_in_cooldown = all(
                    now_after < self._cooldown_until.get(cfg.name, 0.0) for cfg, _ in self.clients
                )
                if all_in_cooldown:
                    for rem_batch in batches[b_idx:]:
                        unchecked.update(rem_batch)
                    if "All Telegram workers are in FloodWait cooldown" not in errors:
                        errors.append("All Telegram workers are in FloodWait cooldown")
                    break
                unchecked.update(batch)

            if delay_sec > 0 and b_idx + 1 < len(batches):
                await asyncio.sleep(delay_sec)

        return {
            "present": sorted(present),
            "missing": sorted(missing),
            "unchecked_rate_limited": sorted(unchecked),
            "errors": errors,
        }

    async def download_media_to_path(
        self,
        item: dict[str, Any],
        dest_path: Path,
        *,
        max_bytes: int | None = None,
    ) -> int:
        dest_path = Path(dest_path)
        size_bytes = int(item.get("size_bytes") or 0)
        if max_bytes is not None and size_bytes > max_bytes:
            raise ValueError(f"Media size {size_bytes} exceeds repair download limit {max_bytes}")

        parts = list(item.get("parts") or [])
        if len(parts) > 1:
            total_part_bytes = sum(int(p.get("size_bytes") or 0) for p in parts)
            effective_size = size_bytes if size_bytes > 0 else total_part_bytes
            if max_bytes is not None and effective_size > max_bytes:
                raise ValueError(
                    f"Media size {effective_size} exceeds repair download limit {max_bytes}"
                )
            end = effective_size - 1 if effective_size > 0 else -1
            chunk_iter = self.iter_telegram_multipart_range(parts, 0, end)
        else:
            chat_id = item.get("telegram_chat_id") or (parts[0].get("telegram_chat_id") if parts else None)
            msg_id = item.get("telegram_msg_id") or (parts[0].get("telegram_msg_id") if parts else None)
            worker_name = item.get("telegram_worker_name") or (parts[0].get("telegram_worker_name") if parts else None)
            if not chat_id or not msg_id:
                raise ValueError("Media item is missing Telegram archive reference.")
            client, media = await self.prepare_stream(str(chat_id), int(msg_id), worker_name=worker_name)
            end = (size_bytes - 1) if size_bytes > 0 else (max_bytes if max_bytes is not None else (1 << 50))
            chunk_iter = self.iter_telegram_range(client, media, 0, end)

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        try:
            with dest_path.open("wb") as f:
                async for chunk in chunk_iter:
                    if not chunk:
                        continue
                    written += len(chunk)
                    if max_bytes is not None and written > max_bytes:
                        raise ValueError(
                            f"Downloaded size {written} exceeds repair download limit {max_bytes}"
                        )
                    await asyncio.to_thread(f.write, chunk)
        except BaseException:
            dest_path.unlink(missing_ok=True)
            raise
        finally:
            aclose = getattr(chunk_iter, "aclose", None)
            if callable(aclose):
                await aclose()
        return written

    async def _claim_group_message(self, chat_id: int | str, msg_id: int) -> bool:
        now = time.monotonic()
        key = (str(chat_id), int(msg_id))
        async with self._claim_lock:
            stale = [k for k, ts in self._claimed_msgs.items() if now - ts > 300]
            for k in stale:
                self._claimed_msgs.pop(k, None)
            if key in self._claimed_msgs:
                return False
            self._claimed_msgs[key] = now
            return True

    def _is_archive_chat(self, chat_id: int | str | None) -> bool:
        archive_target = self._parse_archive_target()
        return archive_target is not None and chat_id is not None and str(chat_id) == str(archive_target)

    async def _resolve_group_folder(self, client: TelegramClient, event, text: str, user_default: str) -> str:
        if has_explicit_folder(text):
            return parse_folder_from_text(text, user_default)
        reply_to = getattr(event.message, "reply_to", None)
        if reply_to and getattr(reply_to, "forum_topic", False):
            topic_id = getattr(reply_to, "reply_to_top_id", None) or getattr(reply_to, "reply_to_msg_id", None)
            if topic_id:
                chat_key = str(self._parse_archive_target())
                known = self.db.get_folder_by_topic_id(chat_key, int(topic_id))
                if known:
                    return known
                try:
                    topic_msg = await client.get_messages(event.chat_id, ids=int(topic_id))
                    topic_title = getattr(getattr(topic_msg, "action", None), "title", None)
                    if topic_title:
                        folder_name = sanitize_folder(str(topic_title), user_default)
                        self.db.save_topic_id(chat_key, folder_name, int(topic_id))
                        return folder_name
                except Exception:
                    pass
        return user_default

    async def handle_subs_command(self, raw_args: str = "") -> str:
        svc = self.subscriptions
        if svc is None:
            return "Subscriptions service is unavailable."
        parts = raw_args.strip().split()
        if not parts:
            subs = svc.list_subscriptions()
            if not subs:
                return "No subscriptions configured yet."
            lines = [f"**Subscriptions ({len(subs)}):**"]
            for s in subs[:25]:
                status = str(s.get("last_status") or "pending")
                state_lbl = status if s.get("enabled") else f"paused/{status}"
                short_id = str(s.get("id") or "")[:12]
                lines.append(
                    f"• `{short_id}` **{s.get('name') or s.get('source_url')}** "
                    f"(`{state_lbl}`, every {s.get('interval_minutes')}m, "
                    f"{int(s.get('items_ingested_count') or 0)} archived)"
                )
            lines.append(
                "\nRun `/subs check <id>` to check now or `/subs archive <id>` to download all existing items."
            )
            return "\n".join(lines)

        cmd = parts[0].lower()
        if cmd in ("check", "archive") and len(parts) >= 2:
            target = parts[1].strip()
            subs = svc.list_subscriptions()
            match = next(
                (s for s in subs if str(s.get("id")) == target or str(s.get("id", "")).startswith(target)),
                None,
            )
            if not match:
                return f"Subscription `{target}` not found."
            is_archive = cmd == "archive"
            try:
                res = await svc.check_subscription(
                    match["id"],
                    dry_run=False,
                    trigger="telegram",
                    full_catalog=is_archive,
                    queue_existing=is_archive,
                )
                label = "Archived" if is_archive else "Checked"
                return (
                    f"{label} **{match.get('name') or match['id']}**: "
                    f"{int(res.get('discovered_count', 0))} discovered, "
                    f"{int(res.get('queued_count', 0))} queued, "
                    f"{int(res.get('seeded_count', 0))} seeded."
                )
            except Exception as exc:
                action_lbl = "Archive" if is_archive else "Check"
                return f"{action_lbl} failed for `{match['id']}`: {exc}"

        return "Usage: `/subs`, `/subs check <id>`, or `/subs archive <id>`"

    def _register_handlers(self, cfg: TelegramApiConfig, client: TelegramClient) -> None:
        @client.on(events.NewMessage(pattern=r"^/(start|help)$"))
        async def on_help(event):
            if not self._is_authorized(event.sender_id):
                if not self._is_archive_chat(event.chat_id):
                    await event.reply(
                        f"Unauthorized. Your Telegram ID is `{event.sender_id}`.\n"
                        "Add it to `ALLOWED_TELEGRAM_USER_IDS` in `.env`."
                    )
                return
            if self._is_archive_chat(event.chat_id) and not await self._claim_group_message(event.chat_id, event.id):
                return
            cur_folder = self.db.get_user_folder(event.sender_id, self.settings.default_folder)
            await event.reply(
                f"Connected via worker `{cfg.name}` (MTProto up to 2 GB, mode: `{self.settings.storage_mode}`).\n"
                f"Active folder: `{cur_folder}`\n\n"
                "• Send any URL or photo/video/file up to 2 GB (in DM or in the archive group).\n"
                "• Send magnet links (`magnet:?xt=...`) or `.torrent` files to leech via aria2c into the vault.\n"
                "• Specify a folder per message with `#FolderName` or `folder:FolderName` (or post inside a Topic).\n"
                "• Optional quality: `format:1080p`, `format:720p`, `format:480p`, `format:audio`.\n"
                "• Bypass duplicate check with `--force` or `force:true`.\n"
                "• Set sticky folder: `/folder Movies`\n"
                "• List folders: `/folders` | Status: `/status` | Recent: `/recent` | Subscriptions: `/subs`\n"
                "• Cancel active job: `/cancel [job_id]` | Retry job: `/retry [job_id]`\n\n"
                f"Gallery: {self._gallery_link()}"
            )

        @client.on(events.NewMessage(pattern=r"^/subs(?:\s+(.*))?$"))
        async def on_subs(event):
            if not self._is_authorized(event.sender_id):
                return
            if self._is_archive_chat(event.chat_id) and not await self._claim_group_message(event.chat_id, event.id):
                return
            reply = await self.handle_subs_command(event.pattern_match.group(1) or "")
            await event.reply(reply)

        @client.on(events.NewMessage(pattern=r"^/folder(?:\s+(.+))?$"))
        async def on_folder(event):
            if not self._is_authorized(event.sender_id):
                return
            if self._is_archive_chat(event.chat_id) and not await self._claim_group_message(event.chat_id, event.id):
                return
            arg = (event.pattern_match.group(1) or "").strip()
            if not arg:
                cur = self.db.get_user_folder(event.sender_id, self.settings.default_folder)
                await event.reply(f"Active folder is `{cur}`. Usage: `/folder Movies`")
                return
            chosen = sanitize_folder(arg, self.settings.default_folder)
            self.db.set_user_folder(event.sender_id, chosen)
            await event.reply(f"Active folder set to `{chosen}`.")

        @client.on(events.NewMessage(pattern=r"^/folders$"))
        async def on_folders(event):
            if not self._is_authorized(event.sender_id):
                return
            if self._is_archive_chat(event.chat_id) and not await self._claim_group_message(event.chat_id, event.id):
                return
            folders = self.db.list_folders()
            cur = self.db.get_user_folder(event.sender_id, self.settings.default_folder)
            if not folders:
                await event.reply(f"No folders yet. Active default: `{cur}`")
                return
            lines = [f"**Folders** (active: `{cur}`):"] + [f"• `{f}`" for f in folders]
            await event.reply("\n".join(lines))

        @client.on(events.NewMessage(pattern=r"^/status$"))
        async def on_status(event):
            if not self._is_authorized(event.sender_id):
                return
            if self._is_archive_chat(event.chat_id) and not await self._claim_group_message(event.chat_id, event.id):
                return
            lines = [f"**Storage Mode:** `{self.settings.storage_mode}`"]
            if self.settings.storage_mode != "telegram":
                statuses = self.allocator.get_drive_statuses()
                if statuses:
                    lines.append("**Mounted Drives:**")
                    for d in statuses:
                        if d.mounted:
                            lines.append(f"• `{d.name}`: {fmt_size(d.free_bytes)} free / {fmt_size(d.total_bytes)}")
                        else:
                            lines.append(f"• `{d.name}`: _not mounted_ (`{d.path}`)")
            if self.settings.telegram_archive_chat_id.strip():
                lines.append(f"**Topic Archive:** `{self.settings.telegram_archive_chat_id}`")
            lines.append(f"**Gallery:** {self._gallery_link()}")
            await event.reply("\n".join(lines))

        @client.on(events.NewMessage(pattern=r"^/recent$"))
        async def on_recent(event):
            if not self._is_authorized(event.sender_id):
                return
            if self._is_archive_chat(event.chat_id) and not await self._claim_group_message(event.chat_id, event.id):
                return
            items, _ = self.db.list_media(limit=5)
            if not items:
                await event.reply("No media uploaded yet.")
                return
            lines = ["**Recent Media:**"]
            for it in items:
                lines.append(
                    f"• **{it['filename']}** (`{it['folder']}` • {fmt_size(it['size_bytes'])}, `{it['drive_name']}`)\n"
                    f"  {self._gallery_link(it['id'])}"
                )
            await event.reply("\n".join(lines))

        @client.on(events.NewMessage(pattern=r"^/cancel(?:\s+([A-Za-z0-9_-]+))?$"))
        async def on_cancel(event):
            if not self._is_authorized(event.sender_id):
                return
            if self._is_archive_chat(event.chat_id) and not await self._claim_group_message(event.chat_id, event.id):
                return
            target_job_id = (event.pattern_match.group(1) or "").strip()
            if not target_job_id:
                jobs = self.db.list_jobs(limit=30)
                active_job = next(
                    (j for j in jobs if j.get("status") in ("queued", "downloading", "allocating")),
                    None,
                )
                if not active_job:
                    await event.reply("No active jobs to cancel.")
                    return
                target_job_id = active_job["id"]
            try:
                cancelled = await self.ingest.cancel_job(target_job_id)
                await event.reply(
                    f"Cancelled job `{cancelled['id']}` ({cancelled['source_ref']})."
                )
            except (KeyError, ValueError) as exc:
                await event.reply(f"Could not cancel job `{target_job_id}`: {exc}")

        @client.on(events.NewMessage(pattern=r"^/retry(?:\s+([A-Za-z0-9_-]+))?$"))
        async def on_retry(event):
            if not self._is_authorized(event.sender_id):
                return
            if self._is_archive_chat(event.chat_id) and not await self._claim_group_message(event.chat_id, event.id):
                return
            target_job_id = (event.pattern_match.group(1) or "").strip()
            if not target_job_id:
                jobs = self.db.list_jobs(limit=30)
                retryable = next(
                    (
                        j
                        for j in jobs
                        if j.get("status") in ("failed", "cancelled")
                        and (
                            j.get("source_kind") == "url"
                            or str(j.get("source_ref") or "").startswith(("http://", "https://"))
                        )
                    ),
                    None,
                )
                if not retryable:
                    await event.reply("No failed or cancelled URL jobs to retry.")
                    return
                target_job_id = retryable["id"]
            try:
                retried = await self.ingest.retry_job(target_job_id)
                await event.reply(
                    f"Retrying job `{retried['id']}` ({retried['source_ref']})."
                )
            except (KeyError, ValueError) as exc:
                await event.reply(f"Could not retry job `{target_job_id}`: {exc}")

        @client.on(events.NewMessage)
        async def on_message(event):
            text = event.raw_text or ""
            if text.startswith("/"):
                return
            if event.out or (event.sender_id and event.sender_id in self._bot_ids):
                return

            in_archive_group = self._is_archive_chat(event.chat_id)
            if not self._is_authorized(event.sender_id):
                if not in_archive_group and not self.settings.get_allowed_telegram_user_ids():
                    await event.reply(
                        f"Unauthorized. Add `{event.sender_id}` to `ALLOWED_TELEGRAM_USER_IDS`."
                    )
                return

            try:
                sender = await event.get_sender()
                if getattr(sender, "bot", False):
                    return
            except Exception:
                pass

            urls = URL_REGEX.findall(text)
            magnets = MAGNET_REGEX.findall(text)
            user_default = self.db.get_user_folder(event.sender_id, self.settings.default_folder)

            if in_archive_group:
                # Inside the shared archive supergroup, ingest URLs / magnet links (with #Folder / folder:Name / current Topic),
                # but do not re-ingest raw media uploads already posted into the group.
                if not urls and not magnets:
                    return
                if not await self._claim_group_message(event.chat_id, event.id):
                    return
                folder = await self._resolve_group_folder(client, event, text, user_default)
                fmt_spec = parse_format_from_text(text)
                tags = parse_tags_from_text(text)
                force = parse_force_from_text(text)
                for url in urls:
                    await self._handle_telegram_url(
                        cfg, event, url, folder, format_spec=fmt_spec, tags=tags, force=force
                    )
                for magnet in magnets:
                    await self._handle_telegram_magnet(cfg, event, magnet, folder, tags=tags, force=force)
                return

            folder = parse_folder_from_text(text, user_default)
            tags = parse_tags_from_text(text)

            # 1. Direct Telegram media/file upload (up to 2 GB via MTProto)
            if event.message.media and (
                event.message.photo or event.message.video or event.message.document
            ):
                if (
                    bool(event.message.document)
                    and _extract_filename(event.message).lower().endswith(".torrent")
                    and self.torrent_service is not None
                    and self.settings.torrent_enabled
                ):
                    await self._handle_telegram_torrent_file(
                        cfg, event, folder, tags=tags, force=parse_force_from_text(text)
                    )
                    return
                await self._handle_telegram_media(cfg, event, folder, tags=tags)
                return

            # 2. Magnets / URLs in DM text
            force = parse_force_from_text(text)
            for magnet in magnets:
                await self._handle_telegram_magnet(cfg, event, magnet, folder, tags=tags, force=force)
            if urls:
                fmt_spec = parse_format_from_text(text)
                for url in urls:
                    await self._handle_telegram_url(
                        cfg, event, url, folder, format_spec=fmt_spec, tags=tags, force=force
                    )

    def _format_telegram_archive_reply(
        self, cfg: TelegramApiConfig, items: list[dict[str, Any]], folder: str
    ) -> str:
        safe_folder = str(folder or "").replace("`", "'")
        if items and all(it.get("deduplicated") for it in items):
            lines = [f"[{cfg.name}] Skipped (duplicate skipped — send with --force to re-ingest):"]
        else:
            lines = [f"[{cfg.name}] Saved to `{safe_folder}`:"]
        shown = items[:TELEGRAM_ARCHIVE_REPLY_MAX_ITEMS]
        for it in shown:
            safe_name = str(it.get("filename") or "").replace("`", "'")
            safe_drive = str(it.get("drive_name") or "").replace("`", "'")
            topic_note = f" + Topic #{it['telegram_topic_id']}" if it.get("telegram_topic_id") else ""
            dup_note = " (duplicate skipped — send with --force to re-ingest)" if it.get("deduplicated") else ""
            lines.append(
                f"• `{safe_name}` ({fmt_size(it['size_bytes'])}) → `{safe_drive}`{topic_note}{dup_note}\n"
                f"  View: {self._gallery_link(it['id'])}"
            )
        if len(items) > len(shown):
            lines.append(f"... and {len(items) - len(shown)} more file(s)")
        return _truncate_utf16("\n".join(lines), 4000)

    async def _handle_telegram_magnet(
        self,
        cfg: TelegramApiConfig,
        event,
        magnet: str,
        folder: str,
        *,
        tags: list[str] | None = None,
        force: bool = False,
    ) -> None:
        if self.torrent_service is None or not self.settings.torrent_enabled or find_aria2c() is None:
            await event.reply(f"[{cfg.name}] Torrent leeching is disabled or aria2c is not installed.")
            return
        try:
            clean_magnet = validate_magnet_uri(magnet)
        except ValueError as exc:
            await event.reply(f"[{cfg.name}] Invalid magnet link: {exc}")
            return
        infohash = extract_magnet_infohash(clean_magnet) or "magnet"
        try:
            assert_staging_disk_space(
                self.settings.staging_dir,
                0,
                self.settings.torrent_max_total_bytes,
                self.settings.torrent_min_free_buffer_bytes,
            )
        except TorrentQuotaError as exc:
            await event.reply(f"[{cfg.name}] Torrent quota exceeded: {exc}")
            return

        display_ref = _truncate_utf16(clean_magnet, 600)
        status_msg = await event.reply(f"[{cfg.name}] Queued magnet `{infohash[:12]}…` → `{folder}`...")
        job_id = self.db.create_job(
            "telegram",
            "torrent",
            clean_magnet,
            params={
                "magnet": clean_magnet,
                "infohash": infohash,
                "folder": folder,
                "use_torrent_subfolder": True,
                "tags": tags,
                "forward_to_telegram": True,
                "force": force,
                "source_origin": "telegram",
            },
        )
        last_edit = 0.0

        async def progress_edit(pct: float, msg: str) -> None:
            nonlocal last_edit
            now = time.monotonic()
            if now - last_edit >= 3.0:
                last_edit = now
                try:
                    await status_msg.edit(f"[{cfg.name}] `{folder}` • {pct:.0f}%\n{msg}")
                except Exception:
                    pass

        try:
            task = self.ingest.submit_job(
                job_id,
                self.torrent_service.process_torrent_job(
                    job_id,
                    magnet=clean_magnet,
                    source_origin="telegram",
                    folder=folder,
                    use_torrent_subfolder=True,
                    tags=tags,
                    forward_to_telegram=True,
                    force=force,
                    on_progress=progress_edit,
                ),
            )
            items = await task
            if items is None:
                job = self.db.get_job(job_id)
                if job and job.get("status") == "cancelled":
                    await status_msg.edit(f"[{cfg.name}] Cancelled: {display_ref}")
                    return
                raise RuntimeError((job.get("message") if job else None) or "Ingest failed")

            await status_msg.edit(self._format_telegram_archive_reply(cfg, items, folder))
        except asyncio.CancelledError:
            await status_msg.edit(f"[{cfg.name}] Cancelled: {display_ref}")
        except FloodWaitError as fw:
            self._record_flood_wait(cfg.name, fw.seconds)
            self.db.update_job(job_id, status="failed", message=f"Telegram flood wait: {fw.seconds}s")
            await status_msg.edit(f"[{cfg.name}] Rate limited by Telegram for {fw.seconds}s. Try another worker/bot.")
        except Exception as exc:
            err_msg = _truncate_utf16(str(exc), 1500)
            await status_msg.edit(f"[{cfg.name}] Failed:\n`{err_msg}`")

    async def _download_bounded_torrent_bytes(self, event) -> bytes | None:
        """Download a .torrent attachment with a strict byte ceiling and timeout."""
        client = getattr(event, "client", None)
        doc = getattr(event.message, "document", None)
        if client is not None and doc is not None and hasattr(client, "iter_download"):
            buf = bytearray()

            async def _stream() -> bytes | None:
                async for chunk in client.iter_download(doc):
                    buf.extend(chunk)
                    if len(buf) > MAX_TORRENT_FILE_BYTES:
                        return None
                return bytes(buf)

            return await asyncio.wait_for(_stream(), timeout=30.0)

        raw = await asyncio.wait_for(event.message.download_media(file=bytes), timeout=30.0)
        if not isinstance(raw, (bytes, bytearray)) or not raw or len(raw) > MAX_TORRENT_FILE_BYTES:
            return None
        return bytes(raw)

    async def _handle_telegram_torrent_file(
        self,
        cfg: TelegramApiConfig,
        event,
        folder: str,
        *,
        tags: list[str] | None = None,
        force: bool = False,
    ) -> None:
        if self.torrent_service is None or not self.settings.torrent_enabled or find_aria2c() is None:
            await event.reply(f"[{cfg.name}] Torrent leeching is disabled or aria2c is not installed.")
            return
        total_size = event.message.file.size if event.message.file else 0
        if total_size > MAX_TORRENT_FILE_BYTES:
            await event.reply(f"[{cfg.name}] .torrent file exceeds maximum allowed metadata size (20 MB).")
            return
        try:
            raw = await self._download_bounded_torrent_bytes(event)
        except asyncio.TimeoutError:
            await event.reply(f"[{cfg.name}] Timed out downloading .torrent metadata file.")
            return
        if not raw or len(raw) > MAX_TORRENT_FILE_BYTES:
            await event.reply(f"[{cfg.name}] Failed to download .torrent metadata file (empty or exceeds 20 MB limit).")
            return
        try:
            parsed = await asyncio.to_thread(parse_torrent_bytes, raw)
        except (TorrentError, ValueError) as exc:
            await event.reply(f"[{cfg.name}] Invalid .torrent file: {exc}")
            return
        selected_indices = [f["index"] for f in parsed["files"] if f.get("selected_default")]
        if not selected_indices:
            selected_indices = [f["index"] for f in parsed["files"]]
        selected_set = set(selected_indices)
        selected_bytes = sum(f["size_bytes"] for f in parsed["files"] if f["index"] in selected_set)
        try:
            assert_staging_disk_space(
                self.settings.staging_dir,
                selected_bytes,
                self.settings.torrent_max_total_bytes,
                self.settings.torrent_min_free_buffer_bytes,
            )
        except TorrentQuotaError as exc:
            await event.reply(f"[{cfg.name}] Torrent quota exceeded: {exc}")
            return

        status_msg = await event.reply(
            f"[{cfg.name}] Leeching `{parsed['name']}` ({len(selected_indices)}/{parsed['file_count']} file(s), {fmt_size(selected_bytes)}) → `{folder}`..."
        )
        job_ref = f"torrent:{parsed['infohash']}:{parsed['name']}"
        job_id = self.db.create_job(
            "telegram",
            "torrent",
            job_ref,
            params={
                "infohash": parsed["infohash"],
                "name": parsed["name"],
                "selected_indices": selected_indices,
                "folder": folder,
                "use_torrent_subfolder": True,
                "tags": tags,
                "forward_to_telegram": True,
                "force": force,
                "source_origin": "telegram",
            },
        )
        last_edit = 0.0

        async def progress_edit(pct: float, msg: str) -> None:
            nonlocal last_edit
            now = time.monotonic()
            if now - last_edit >= 3.0:
                last_edit = now
                try:
                    await status_msg.edit(f"[{cfg.name}] `{folder}` • {pct:.0f}%\n{msg}")
                except Exception:
                    pass

        display_ref = _truncate_utf16(job_ref, 600)

        try:
            task = self.ingest.submit_job(
                job_id,
                self.torrent_service.process_torrent_job(
                    job_id,
                    torrent_bytes=bytes(raw),
                    selected_indices=selected_indices,
                    source_origin="telegram",
                    folder=folder,
                    use_torrent_subfolder=True,
                    tags=tags,
                    forward_to_telegram=True,
                    force=force,
                    on_progress=progress_edit,
                ),
            )
            items = await task
            if items is None:
                job = self.db.get_job(job_id)
                if job and job.get("status") == "cancelled":
                    await status_msg.edit(f"[{cfg.name}] Cancelled: {display_ref}")
                    return
                raise RuntimeError((job.get("message") if job else None) or "Ingest failed")

            await status_msg.edit(self._format_telegram_archive_reply(cfg, items, folder))
        except asyncio.CancelledError:
            await status_msg.edit(f"[{cfg.name}] Cancelled: {display_ref}")
        except FloodWaitError as fw:
            self._record_flood_wait(cfg.name, fw.seconds)
            self.db.update_job(job_id, status="failed", message=f"Telegram flood wait: {fw.seconds}s")
            await status_msg.edit(f"[{cfg.name}] Rate limited by Telegram for {fw.seconds}s. Try another worker/bot.")
        except Exception as exc:
            err_msg = _truncate_utf16(str(exc), 1500)
            await status_msg.edit(f"[{cfg.name}] Failed:\n`{err_msg}`")

    async def _handle_telegram_url(
        self,
        cfg: TelegramApiConfig,
        event,
        url: str,
        folder: str,
        *,
        format_spec: str | None = None,
        tags: list[str] | None = None,
        force: bool = False,
    ) -> None:
        display_url = _truncate_utf16(url, 600)
        status_msg = await event.reply(f"[{cfg.name}] Queued → `{folder}`: {display_url}")
        job_id = self.db.create_job(
            "telegram",
            "url",
            url,
            params={
                "url": url,
                "folder": folder,
                "tags": tags,
                "format_spec": format_spec,
                "force": force,
                "source_origin": "telegram",
            },
        )
        last_edit = 0.0

        async def progress_edit(pct: float, msg: str) -> None:
            nonlocal last_edit
            now = time.monotonic()
            if now - last_edit >= 3.0:
                last_edit = now
                try:
                    await status_msg.edit(f"[{cfg.name}] `{folder}` • {pct:.0f}%\n{msg}")
                except Exception:
                    pass

        try:
            try:
                await status_msg.edit(f"[{cfg.name}] Downloading → `{folder}`: {display_url}")
            except Exception:
                pass
            task = self.ingest.submit_job(
                job_id,
                self.ingest.process_url_job(
                    job_id,
                    url,
                    source_origin="telegram",
                    folder=folder,
                    tags=tags,
                    forward_to_telegram=True,
                    format_spec=format_spec,
                    on_progress=progress_edit,
                    force=force,
                ),
            )
            items = await task
            if items is None:
                job = self.db.get_job(job_id)
                if job and job.get("status") == "cancelled":
                    await status_msg.edit(f"[{cfg.name}] Cancelled: {display_url}")
                    return
                raise RuntimeError((job.get("message") if job else None) or "Ingest failed")

            if items and all(it.get("deduplicated") for it in items):
                lines = [f"[{cfg.name}] Skipped (duplicate skipped — send with --force to re-ingest):"]
            else:
                lines = [f"[{cfg.name}] Saved to `{folder}`:"]
            for it in items:
                topic_note = f" + Topic #{it['telegram_topic_id']}" if it.get("telegram_topic_id") else ""
                dup_note = " (duplicate skipped — send with --force to re-ingest)" if it.get("deduplicated") else ""
                lines.append(
                    f"• `{it['filename']}` ({fmt_size(it['size_bytes'])}) → `{it['drive_name']}`{topic_note}{dup_note}\n"
                    f"  View: {self._gallery_link(it['id'])}"
                )
            await status_msg.edit(_truncate_utf16("\n".join(lines), 4000))
        except asyncio.CancelledError:
            await status_msg.edit(f"[{cfg.name}] Cancelled: {display_url}")
        except FloodWaitError as fw:
            self._record_flood_wait(cfg.name, fw.seconds)
            self.db.update_job(job_id, status="failed", message=f"Telegram flood wait: {fw.seconds}s")
            await status_msg.edit(f"[{cfg.name}] Rate limited by Telegram for {fw.seconds}s. Try another worker/bot.")
        except Exception as exc:
            err_msg = _truncate_utf16(str(exc), 1500)
            await status_msg.edit(f"[{cfg.name}] Failed:\n`{err_msg}`")

    @staticmethod
    def _classify_telegram_media(filename: str, message: Any) -> tuple[str, str]:
        media_type, mime_type = classify_media(Path(filename))
        msg_mime = getattr(getattr(message, "file", None), "mime_type", None) or ""
        if media_type == "file" and msg_mime:
            if msg_mime.startswith("image/"):
                return "image", msg_mime
            if msg_mime.startswith("video/"):
                return "video", msg_mime
            if msg_mime.startswith("audio/"):
                return "audio", msg_mime
            mime_type = msg_mime
        if media_type == "file":
            if getattr(message, "photo", None):
                return "image", msg_mime or "image/jpeg"
            if getattr(message, "video", None):
                return "video", msg_mime or "video/mp4"
        return media_type, mime_type

    async def _handle_telegram_media(
        self, cfg: TelegramApiConfig, event, folder: str, *, tags: list[str] | None = None
    ) -> None:
        filename = _extract_filename(event.message)
        total_size = event.message.file.size if event.message.file else 0
        if total_size > 2 * 1024 * 1024 * 1024:
            await event.reply("File exceeds 2 GB Telegram bot transfer limit.")
            return

        use_zero_copy = (
            self.settings.storage_mode == "telegram"
            and 0 < total_size <= self.settings.telegram_part_size_bytes
            and bool(self.settings.telegram_archive_chat_id.strip())
        )

        initial_verb = "Forwarding" if use_zero_copy else "Downloading"
        status_msg = await event.reply(
            f"[{cfg.name}] {initial_verb} `{filename}` ({fmt_size(total_size)}) → `{folder}`..."
        )
        job_id = self.db.create_job("telegram", "upload", filename)
        job_dir = Path(tempfile.mkdtemp(dir=self.settings.staging_dir))
        self.ingest.register_staging_dir(job_dir)
        staged_path = job_dir / filename

        last_update = 0.0

        async def progress_cb(received: int, total: int):
            nonlocal last_update
            now = time.monotonic()
            if total and now - last_update >= 3.0:
                last_update = now
                pct = (received / total) * 100
                try:
                    await status_msg.edit(
                        f"[{cfg.name}] Downloading `{filename}`: {pct:.0f}% ({fmt_size(received)} / {fmt_size(total)})"
                    )
                except Exception:
                    pass

        try:
            if use_zero_copy:
                safe_folder = sanitize_folder(folder, self.settings.default_folder)
                normalized_tags = normalize_tags(tags)
                self.db.update_job(
                    job_id,
                    status="allocating",
                    progress=10.0,
                    message="Forwarding directly to Telegram archive (zero-copy)...",
                )

                media_id = uuid.uuid4().hex[:12]
                media_type, mime_type = self._classify_telegram_media(filename, event.message)
                thumb_filename: str | None = None

                temp_thumb_path = job_dir / f"{media_id}_thumb.jpg"
                downloaded_thumb = None
                try:
                    downloaded_thumb = await event.message.download_media(
                        file=str(temp_thumb_path),
                        thumb=-1,
                    )
                except Exception as thumb_exc:
                    logger.debug("Embedded thumbnail download failed for %s: %s", filename, thumb_exc)

                if downloaded_thumb:
                    thumb_src = Path(downloaded_thumb)
                    if thumb_src.is_file() and thumb_src.stat().st_size > 0:
                        thumb_filename = await generate_thumbnail(
                            thumb_src, "image", self.settings.thumbs_dir, media_id
                        )

                if not thumb_filename and media_type == "image" and total_size <= 10 * 1024 * 1024:
                    dl_img = await event.message.download_media(
                        file=str(staged_path),
                        progress_callback=progress_cb,
                    )
                    if dl_img and Path(dl_img).is_file():
                        staged_path = Path(dl_img)
                        thumb_filename = await generate_thumbnail(
                            staged_path, "image", self.settings.thumbs_dir, media_id
                        )

                async def _on_demand_download() -> Path:
                    if staged_path.is_file():
                        return staged_path
                    self.db.update_job(
                        job_id,
                        status="downloading",
                        progress=25.0,
                        message=f"Downloading {filename} via MTProto (fallback)...",
                    )
                    dl_full = await event.message.download_media(
                        file=str(staged_path),
                        progress_callback=progress_cb,
                    )
                    if not dl_full or not Path(dl_full).is_file():
                        raise RuntimeError("Telegram download returned empty path.")
                    nonlocal thumb_filename
                    dl_p = Path(dl_full)
                    if not thumb_filename:
                        thumb_filename = await generate_thumbnail(
                            dl_p, media_type, self.settings.thumbs_dir, media_id
                        )
                    self.db.update_job(
                        job_id,
                        status="allocating",
                        progress=60.0,
                        message=f"Uploading {filename} to Telegram archive...",
                    )
                    return dl_p

                async def _report_ul(pct: float, msg: str) -> None:
                    overall = 50.0 + (pct / 100.0) * 49.0
                    self.db.update_job(job_id, status="allocating", progress=overall, message=msg)

                payload = {
                    "media_id": media_id,
                    "title": Path(filename).stem,
                    "filename": filename,
                    "folder": safe_folder,
                    "tags": normalized_tags,
                    "media_type": media_type,
                    "size_bytes": total_size,
                    "local_path": staged_path,
                    "source_url": None,
                    "tg_source": {
                        "worker_name": cfg.name,
                        "message": event.message,
                        "on_demand_download": _on_demand_download,
                    },
                    "progress_cb": _report_ul,
                }
                mirror_info = await self.mirror_to_archive_topic(payload)
                if not mirror_info or not mirror_info.get("message_id"):
                    raise RuntimeError(
                        "Telegram storage mode requires TELEGRAM_APIS_JSON and TELEGRAM_ARCHIVE_CHAT_ID to be configured and reachable."
                    )

                msg_file = getattr(event.message, "file", None)
                raw_dur = getattr(msg_file, "duration", None) if msg_file else None
                raw_w = getattr(msg_file, "width", None) if msg_file else None
                raw_h = getattr(msg_file, "height", None) if msg_file else None
                duration_sec = float(raw_dur) if raw_dur is not None and float(raw_dur) > 0 else None
                width = int(raw_w) if raw_w is not None and int(raw_w) > 0 else None
                height = int(raw_h) if raw_h is not None and int(raw_h) > 0 else None

                item = self.ingest.record_media(
                    media_id=media_id,
                    title=Path(filename).stem,
                    filename=filename,
                    folder=safe_folder,
                    tags=normalized_tags,
                    media_type=media_type,
                    mime_type=mime_type,
                    size_bytes=total_size,
                    drive_name="telegram",
                    file_path="",
                    thumb_filename=thumb_filename,
                    source_origin="telegram",
                    source_kind="upload",
                    source_url=None,
                    mirror_info=mirror_info,
                    duration_sec=duration_sec,
                    width=width,
                    height=height,
                )
                self.db.update_job(
                    job_id,
                    status="done",
                    progress=100.0,
                    message=f"Saved to telegram/{safe_folder}",
                    media_ids_json=json.dumps([item["id"]]),
                )
            else:
                self.db.update_job(job_id, status="downloading", message=f"Downloading {filename} via MTProto...")
                downloaded = await event.message.download_media(
                    file=str(staged_path),
                    progress_callback=progress_cb,
                )
                if not downloaded:
                    raise RuntimeError("Telegram download returned empty path.")
                item = await self.ingest.process_staged_upload_job(
                    job_id,
                    Path(downloaded),
                    source_origin="telegram",
                    folder=folder,
                    tags=tags,
                    title=Path(filename).stem,
                    forward_to_telegram=True,
                    tg_source={"worker_name": cfg.name, "message": event.message},
                )

            topic_note = f" + Topic #{item['telegram_topic_id']}" if item.get("telegram_topic_id") else ""
            await status_msg.edit(
                f"[{cfg.name}] Saved `{item['filename']}` ({fmt_size(item['size_bytes'])}) → `{item['drive_name']}/{item['folder']}`{topic_note}\n"
                f"View: {self._gallery_link(item['id'])}"
            )
        except FloodWaitError as fw:
            self._record_flood_wait(cfg.name, fw.seconds)
            self.db.update_job(job_id, status="failed", message=f"Telegram flood wait: {fw.seconds}s")
            await status_msg.edit(f"[{cfg.name}] Rate limited by Telegram for {fw.seconds}s. Try another worker/bot.")
        except Exception as exc:
            self._record_worker_error(cfg.name, exc)
            self.db.update_job(job_id, status="failed", message=str(exc))
            err_msg = _truncate_utf16(str(exc), 1500)
            await status_msg.edit(f"[{cfg.name}] Failed:\n`{err_msg}`")
        finally:
            shutil.rmtree(job_dir, ignore_errors=True)
            self.ingest.unregister_staging_dir(job_dir)
