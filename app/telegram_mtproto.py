from __future__ import annotations

import asyncio
import io
import logging
import math
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError

from app.allocator import DriveAllocator, fmt_size, sanitize_folder
from app.config import Settings, TelegramApiConfig
from app.db import Database
from app.ingest import IngestService

logger = logging.getLogger(__name__)
URL_REGEX = re.compile(r"https?://\S+")
FOLDER_HASH_RE = re.compile(r"(?:^|\s)#([A-Za-z0-9_/-]{1,64})")
FOLDER_KV_RE = re.compile(r"(?:^|\s)(?:folder|dir):([A-Za-z0-9_./-]{1,64})", re.IGNORECASE)
FORMAT_KV_RE = re.compile(r"(?:^|\s)(?:format|fmt|q):([A-Za-z0-9_+/\[\]<=.,:-]{1,48})", re.IGNORECASE)


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
    if stripped and not URL_REGEX.search(stripped) and len(stripped) <= 40 and "\n" not in stripped:
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
        self.clients: list[tuple[TelegramApiConfig, TelegramClient]] = []
        self._tasks: list[asyncio.Task] = []
        self._topic_lock = asyncio.Lock()
        self._claim_lock = asyncio.Lock()
        self._claimed_msgs: dict[tuple[str, int], float] = {}
        self._bot_ids: set[int] = set()
        self._rr_index = 0
        self._active_uploads: dict[str, int] = {}
        self._cooldown_until: dict[str, float] = {}
        self._stream_media_cache: dict[tuple[str, int, str], tuple[float, Any]] = {}
        self._stream_cache_ttl = 300.0

        # Wire mirror hook so both Web and Telegram uploads can forward to the shared Forum Supergroup
        self.ingest.mirror_hook = self.mirror_to_archive_topic

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
            logger.info(
                "Started Telegram MTProto worker '%s' (@%s, api_id=%s, max 2GB)",
                cfg.name,
                getattr(me, "username", "bot"),
                cfg.api_id,
            )
            self.clients.append((cfg, client))
            self._tasks.append(asyncio.create_task(client.run_until_disconnected()))

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for _, client in self.clients:
            await client.disconnect()
        self._tasks.clear()
        self.clients.clear()
        self._bot_ids.clear()

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

    def _ordered_clients(self, preferred_worker: str | None = None) -> list[tuple[TelegramApiConfig, TelegramClient]]:
        if not self.clients:
            return []
        ordered = list(self.clients)
        idx = self._rr_index % len(ordered)
        self._rr_index += 1
        rotated = ordered[idx:] + ordered[:idx]
        now = time.monotonic()

        def _sort_key(pair: tuple[TelegramApiConfig, TelegramClient]) -> tuple[int, int, int]:
            name = pair[0].name
            in_cooldown = 1 if now < self._cooldown_until.get(name, 0.0) else 0
            pref_rank = 0 if (preferred_worker and name == preferred_worker and in_cooldown == 0) else 1
            active = self._active_uploads.get(name, 0)
            return (in_cooldown, pref_rank, active)

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

        if size_bytes <= part_size:
            caption_lines = [
                f"**{payload['filename']}**",
                f"Folder: `{folder}` • Size: {fmt_size(size_bytes)}",
                f"Gallery: {self._gallery_link(payload['media_id'])}",
            ]
            if payload.get("source_url"):
                caption_lines.append(f"Source: {payload['source_url']}")
            caption = "\n".join(caption_lines)

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

                    async def _send_with_topic(tid: int | None):
                        if tg_source and cfg.name == preferred_worker and getattr(tg_source.get("message"), "media", None):
                            return await client.send_file(
                                peer,
                                tg_source["message"].media,
                                caption=caption,
                                reply_to=tid,
                                supports_streaming=True,
                            )
                        return await client.send_file(
                            peer,
                            str(payload["local_path"]),
                            caption=caption,
                            reply_to=tid,
                            supports_streaming=True,
                            progress_callback=_tg_upload_progress if progress_cb else None,
                        )

                    try:
                        sent = await _send_with_topic(topic_id)
                    except RPCError as rpc_err:
                        err_text = str(rpc_err).upper()
                        if topic_id and topic_id != 1 and ("TOPIC" in err_text or "REPLY" in err_text or "THREAD" in err_text):
                            logger.info("Cached topic %s invalid for '%s'; recreating...", topic_id, folder)
                            topic_id = await self._ensure_topic_id(cfg, chat_key, folder, force_recreate=True)
                            sent = await _send_with_topic(topic_id)
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
                    self._cooldown_until[cfg.name] = time.monotonic() + fw.seconds
                    logger.warning("Worker '%s' hit FloodWait (%ss) mirroring to topic; trying next worker.", cfg.name, fw.seconds)
                    last_err = fw
                except Exception as exc:
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

        sem = asyncio.Semaphore(max(1, len(self.clients)))

        async def _upload_part(part_index: int) -> dict[str, Any]:
            nonlocal shared_topic_id
            start_byte = part_index * part_size
            end_byte = min(size_bytes - 1, start_byte + part_size - 1)
            chunk_len = end_byte - start_byte + 1
            part_name = f"{payload['filename']}.part{part_index + 1:03d}of{total_parts:03d}"

            caption_lines = [
                f"**{payload['filename']}** (Part {part_index + 1}/{total_parts})",
                f"Folder: `{folder}` • Part Size: {fmt_size(chunk_len)} • Total: {fmt_size(size_bytes)}",
                f"Gallery: {self._gallery_link(payload['media_id'])}",
            ]
            if payload.get("source_url"):
                caption_lines.append(f"Source: {payload['source_url']}")
            part_caption = "\n".join(caption_lines)

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
                        try:
                            sent = await client.send_file(
                                peer,
                                uploaded_handle,
                                caption=part_caption,
                                reply_to=part_topic_id,
                                force_document=True,
                            )
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
                                sent = await client.send_file(
                                    peer,
                                    uploaded_handle,
                                    caption=part_caption,
                                    reply_to=part_topic_id,
                                    force_document=True,
                                )
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
                        self._cooldown_until[cfg.name] = time.monotonic() + fw.seconds
                        logger.warning(
                            "Worker '%s' hit FloodWait (%ss) uploading part %s/%s; trying next worker.",
                            cfg.name,
                            fw.seconds,
                            part_index + 1,
                            total_parts,
                        )
                        last_part_err = fw
                    except Exception as exc:
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
        ordered = self._ordered_clients(preferred_worker=worker_name)

        for cfg, client in ordered:
            cache_key = (str(chat_id), int(msg_id), cfg.name)
            cached = self._stream_media_cache.get(cache_key)
            if cached and now < cached[0]:
                return client, cached[1]

        last_err: Exception | None = None
        for cfg, client in ordered:
            try:
                peer = await client.get_input_entity(target)
                msg = await client.get_messages(peer, ids=msg_id)
                if msg and msg.media:
                    cache_key = (str(chat_id), int(msg_id), cfg.name)
                    self._stream_media_cache[cache_key] = (
                        time.monotonic() + self._stream_cache_ttl,
                        msg.media,
                    )
                    return client, msg.media
            except FloodWaitError as fw:
                self._cooldown_until[cfg.name] = time.monotonic() + fw.seconds
                logger.warning("Worker '%s' hit FloodWait (%ss) fetching stream; trying next.", cfg.name, fw.seconds)
                last_err = fw
            except Exception as exc:
                logger.warning("Worker '%s' failed resolving stream message %s: %s", cfg.name, msg_id, exc)
                last_err = exc

        if last_err:
            raise RuntimeError(str(last_err))
        raise FileNotFoundError("Media message no longer exists in Telegram archive.")

    @staticmethod
    async def iter_telegram_range(client: TelegramClient, media: Any, start: int, end: int):
        remaining = end - start + 1
        if remaining <= 0:
            return
        aligned_offset = (start // 4096) * 4096
        skip = start - aligned_offset
        iterator = client.iter_download(media, offset=aligned_offset, request_size=524288)
        try:
            async for chunk in iterator:
                if remaining <= 0:
                    break
                data = bytes(chunk)
                if skip > 0:
                    if len(data) <= skip:
                        skip -= len(data)
                        continue
                    data = data[skip:]
                    skip = 0
                if len(data) > remaining:
                    yield data[:remaining]
                    break
                remaining -= len(data)
                yield data
        finally:
            close_fn = getattr(iterator, "close", None)
            if callable(close_fn):
                res = close_fn()
                if asyncio.iscoroutine(res):
                    await res

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
        ordered_parts = sorted(parts, key=lambda p: int(p["part_index"]))
        for part in ordered_parts:
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
            async for chunk in self.iter_telegram_range(client, media, local_start, local_end):
                yield chunk

    async def delete_archive_message(
        self, chat_id: str, msg_id: int, worker_name: str | None = None
    ) -> None:
        if not self.clients:
            return
        try:
            target: int | str = int(chat_id)
        except ValueError:
            target = chat_id
        for _, client in self._ordered_clients(preferred_worker=worker_name):
            try:
                peer = await client.get_input_entity(target)
                res = await client.delete_messages(peer, [msg_id])
                if res:
                    return
            except Exception as exc:
                logger.warning("Could not delete archive msg %s: %s", msg_id, exc)

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
                "• Specify a folder per message with `#FolderName` or `folder:FolderName` (or post inside a Topic).\n"
                "• Optional quality: `format:1080p`, `format:720p`, `format:480p`, `format:audio`.\n"
                "• Set sticky folder: `/folder Movies`\n"
                "• List folders: `/folders` | Status: `/status` | Recent: `/recent`\n\n"
                f"Gallery: {self._gallery_link()}"
            )

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
            user_default = self.db.get_user_folder(event.sender_id, self.settings.default_folder)

            if in_archive_group:
                # Inside the shared archive supergroup, ingest URLs (with #Folder / folder:Name / current Topic),
                # but do not re-ingest raw media uploads already posted into the group.
                if not urls:
                    return
                if not await self._claim_group_message(event.chat_id, event.id):
                    return
                folder = await self._resolve_group_folder(client, event, text, user_default)
                fmt_spec = parse_format_from_text(text)
                for url in urls:
                    await self._handle_telegram_url(cfg, event, url, folder, format_spec=fmt_spec)
                return

            folder = parse_folder_from_text(text, user_default)

            # 1. Direct Telegram media/file upload (up to 2 GB via MTProto)
            if event.message.media and (
                event.message.photo or event.message.video or event.message.document
            ):
                await self._handle_telegram_media(cfg, event, folder)
                return

            # 2. URLs in DM text
            if urls:
                fmt_spec = parse_format_from_text(text)
                for url in urls:
                    await self._handle_telegram_url(cfg, event, url, folder, format_spec=fmt_spec)

    async def _handle_telegram_url(
        self,
        cfg: TelegramApiConfig,
        event,
        url: str,
        folder: str,
        *,
        format_spec: str | None = None,
    ) -> None:
        status_msg = await event.reply(f"[{cfg.name}] Queued → `{folder}`: {url}")
        job_id = self.db.create_job("telegram", "url", url)
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
            await status_msg.edit(f"[{cfg.name}] Downloading → `{folder}`: {url}")
            items = await self.ingest.process_url_job(
                job_id,
                url,
                source_origin="telegram",
                folder=folder,
                forward_to_telegram=True,
                format_spec=format_spec,
                on_progress=progress_edit,
            )
            lines = [f"[{cfg.name}] Saved to `{folder}`:"]
            for it in items:
                topic_note = f" + Topic #{it['telegram_topic_id']}" if it.get("telegram_topic_id") else ""
                lines.append(
                    f"• `{it['filename']}` ({fmt_size(it['size_bytes'])}) → `{it['drive_name']}`{topic_note}\n"
                    f"  View: {self._gallery_link(it['id'])}"
                )
            await status_msg.edit("\n".join(lines))
        except FloodWaitError as fw:
            self.db.update_job(job_id, status="failed", message=f"Telegram flood wait: {fw.seconds}s")
            await status_msg.edit(f"[{cfg.name}] Rate limited by Telegram for {fw.seconds}s. Try another worker/bot.")
        except Exception as exc:
            await status_msg.edit(f"[{cfg.name}] Failed:\n`{exc}`")

    async def _handle_telegram_media(self, cfg: TelegramApiConfig, event, folder: str) -> None:
        filename = _extract_filename(event.message)
        total_size = event.message.file.size if event.message.file else 0
        if total_size > 2 * 1024 * 1024 * 1024:
            await event.reply("File exceeds 2 GB Telegram bot transfer limit.")
            return

        status_msg = await event.reply(
            f"[{cfg.name}] Downloading `{filename}` ({fmt_size(total_size)}) → `{folder}`..."
        )
        job_id = self.db.create_job("telegram", "upload", filename)
        job_dir = Path(tempfile.mkdtemp(dir=self.settings.staging_dir))
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
            shutil.rmtree(job_dir, ignore_errors=True)
            self.db.update_job(job_id, status="failed", message=f"Telegram flood wait: {fw.seconds}s")
            await status_msg.edit(f"[{cfg.name}] Rate limited by Telegram for {fw.seconds}s. Try another worker/bot.")
        except Exception as exc:
            shutil.rmtree(job_dir, ignore_errors=True)
            self.db.update_job(job_id, status="failed", message=str(exc))
            await status_msg.edit(f"[{cfg.name}] Failed:\n`{exc}`")
