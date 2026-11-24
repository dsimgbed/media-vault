from __future__ import annotations

import asyncio
import inspect
import io
import logging
import shutil
import threading
import uuid
import zipfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from app.allocator import sanitize_folder
from app.context import AppContext
from app.db import normalize_tags
from app.ingest import classify_media
from app.streaming_zip import (
    StreamingZipEntry,
    deduplicate_arcname,
    iter_streaming_zip64,
)
from app.zip_cache import compute_zip_fingerprint

logger = logging.getLogger(__name__)


class _NeedEarlierOffset(Exception):
    def __init__(self, needed_offset: int) -> None:
        super().__init__(f"Need offset {needed_offset}")
        self.needed_offset = needed_offset


class _TailBackedZipIO(io.RawIOBase):
    """Seekable read-only stream backed by a tail buffer [tail_start, total_size)."""

    def __init__(self, total_size: int, tail_start: int, tail_bytes: bytes) -> None:
        super().__init__()
        self._size = max(0, int(total_size))
        self._tail_start = max(0, int(tail_start))
        self._buf = tail_bytes
        self._pos = 0

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
            new_pos = self._size + int(offset)
        else:
            raise ValueError(f"Unsupported whence: {whence}")
        if new_pos < 0:
            raise ValueError("Negative seek position")
        self._pos = new_pos
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if self._pos >= self._size:
            return b""
        if self._pos < self._tail_start:
            raise _NeedEarlierOffset(self._pos)
        rel = self._pos - self._tail_start
        if rel >= len(self._buf):
            return b""
        if size is None or size < 0:
            chunk = self._buf[rel:]
        else:
            chunk = self._buf[rel : rel + int(size)]
        self._pos += len(chunk)
        return chunk

    def readinto(self, b) -> int:
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n


def _parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    # Format: bytes=start-end
    if not range_header.startswith("bytes="):
        raise ValueError("Invalid range unit")
    spec = range_header[6:].strip()
    if "," in spec:
        spec = spec.split(",", 1)[0].strip()
    start_str, _, end_str = spec.partition("-")
    if not start_str and not end_str:
        raise ValueError("Empty range")
    if not start_str:
        # Suffix range: bytes=-N
        suffix_len = int(end_str)
        start = max(0, file_size - suffix_len)
        end = file_size - 1
    else:
        start = int(start_str)
        end = int(end_str) if end_str else file_size - 1

    if start < 0 or end < start or start >= file_size:
        raise ValueError("Range out of bounds")
    end = min(end, file_size - 1)
    return start, end


def _iter_file_range(path: Path, start: int, end: int, chunk_size: int = 1024 * 1024):
    remaining = end - start + 1
    with path.open("rb") as f:
        f.seek(start)
        while remaining > 0:
            data = f.read(min(chunk_size, remaining))
            if not data:
                break
            remaining -= len(data)
            yield data


async def _aiter_file_range(
    path: Path, start: int, end: int, chunk_size: int = 1024 * 1024
) -> AsyncIterator[bytes]:
    remaining = end - start + 1
    if remaining <= 0:
        return
    fh = await asyncio.to_thread(path.open, "rb")
    fh_lock = threading.Lock()
    closed = False

    def _seek_sync(pos: int) -> None:
        with fh_lock:
            if not closed:
                fh.seek(pos)

    def _read_sync(n: int) -> bytes:
        with fh_lock:
            if closed:
                return b""
            return fh.read(n)

    def _close_sync() -> None:
        nonlocal closed
        with fh_lock:
            if not closed:
                closed = True
                fh.close()

    try:
        if start > 0:
            await asyncio.to_thread(_seek_sync, start)
        while remaining > 0:
            data = await asyncio.to_thread(_read_sync, min(chunk_size, remaining))
            if not data:
                break
            remaining -= len(data)
            yield data
    finally:
        try:
            await asyncio.shield(asyncio.to_thread(_close_sync))
        except BaseException:
            # Ensure descriptor closes off the event loop even if outer cancellation fires again
            try:
                asyncio.get_running_loop().run_in_executor(None, _close_sync)
            except RuntimeError:
                _close_sync()
            raise


def _call_iter_telegram_range(
    tg_pool: Any,
    client: Any,
    media: Any,
    start: int,
    end: int,
    total_size: int | None = None,
):
    fn = tg_pool.iter_telegram_range
    if total_size is not None:
        try:
            sig = inspect.signature(fn)
            if "total_size" in sig.parameters or any(
                p.kind == inspect.Parameter.VAR_KEYWORD
                for p in sig.parameters.values()
            ):
                return fn(client, media, start, end, total_size=total_size)
        except (TypeError, ValueError):
            pass
    return fn(client, media, start, end)


def _sanitize_archive_relpath(raw_rel: str, fallback_name: str) -> str:
    parts = [
        p.strip()
        for p in raw_rel.replace("\\", "/").split("/")
        if p.strip() and p.strip() not in (".", "..")
    ]
    if not parts:
        safe_fallback = Path(fallback_name or "file.bin").name
        return safe_fallback or "file.bin"
    return "/".join(parts)


def _telegram_media_actual_size(tg_media: Any) -> int | None:
    if tg_media is None:
        return None
    try:
        from telethon import utils as tg_utils
        from telethon.tl import types as tg_types

        if isinstance(
            tg_media,
            (
                tg_types.MessageMediaPhoto,
                tg_types.Photo,
                tg_types.MessageMediaDocument,
                tg_types.Document,
            ),
        ):
            info = tg_utils._get_file_info(tg_media)
            if info.size and int(info.size) > 0:
                return int(info.size)
    except Exception:
        pass
    return None


async def resolve_media_source(ctx: AppContext, item: dict[str, Any]) -> dict[str, Any]:
    raw_path = (item.get("file_path") or "").strip()
    file_path = Path(raw_path) if raw_path else None
    if file_path and file_path.is_file():
        return {"kind": "local", "path": file_path, "size": file_path.stat().st_size}

    parts = item.get("parts") or []
    if len(parts) > 1:
        return {"kind": "multipart", "parts": parts, "size": int(item["size_bytes"])}

    if item.get("telegram_chat_id") and item.get("telegram_msg_id"):
        try:
            tg_client, tg_media = await ctx.tg_pool.prepare_stream(
                item["telegram_chat_id"],
                int(item["telegram_msg_id"]),
                worker_name=item.get("telegram_worker_name"),
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Telegram stream error: {exc}")
        return {
            "kind": "telegram",
            "client": tg_client,
            "media": tg_media,
            "size": _telegram_media_actual_size(tg_media) or int(item["size_bytes"]),
        }

    raise HTTPException(status_code=404, detail="File missing on drive (drive may be unmounted)")


async def prefetch_multipart_first(
    ctx: AppContext, source: dict[str, Any], start: int, end: int
) -> dict[int, tuple[Any, Any]] | None:
    if source["kind"] != "multipart" or source["size"] <= 0 or end < start:
        return None
    parts = source["parts"]
    overlapping = [
        p
        for p in sorted(parts, key=lambda x: int(x["part_index"]))
        if int(p["end_byte"]) >= start and int(p["start_byte"]) <= end and int(p["size_bytes"]) > 0
    ]
    if not overlapping:
        raise HTTPException(status_code=404, detail="Requested range has no matching media parts")
    first_part = overlapping[0]
    try:
        first_stream = await ctx.tg_pool.prepare_stream(
            first_part["telegram_chat_id"],
            int(first_part["telegram_msg_id"]),
            worker_name=None,
        )
        return {int(first_part["part_index"]): first_stream}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Telegram stream error: {exc}")


async def read_source_range(
    ctx: AppContext, source: dict[str, Any], start: int, end: int
) -> bytes:
    if end < start:
        return b""
    if source["kind"] == "local":
        path: Path = source["path"]

        def _read_slice() -> bytes:
            with path.open("rb") as f:
                f.seek(start)
                return f.read(end - start + 1)

        return await asyncio.to_thread(_read_slice)

    buf = bytearray()
    if source["kind"] == "multipart":
        async for chunk in ctx.tg_pool.iter_telegram_multipart_range(source["parts"], start, end):
            buf.extend(chunk)
    else:
        async for chunk in ctx.tg_pool.iter_telegram_range(
            source["client"], source["media"], start, end
        ):
            buf.extend(chunk)
    return bytes(buf)


def iter_source_range(
    ctx: AppContext,
    source: dict[str, Any],
    start: int,
    end: int,
    prefetched: dict[int, tuple[Any, Any]] | None = None,
):
    if end < start:
        return iter([b""])
    if source["kind"] == "local":
        return _iter_file_range(source["path"], start, end)
    if source["kind"] == "multipart":
        return ctx.tg_pool.iter_telegram_multipart_range(
            source["parts"], start, end, prefetched=prefetched
        )
    return _call_iter_telegram_range(
        ctx.tg_pool,
        source["client"],
        source["media"],
        start,
        end,
        total_size=source.get("size"),
    )


async def get_zip_entries(
    ctx: AppContext, item: dict[str, Any], source: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    media_id = item["id"]
    fp = compute_zip_fingerprint(item)
    get_entries_fn = getattr(ctx.zip_cache, "get_entries", None)
    if callable(get_entries_fn):
        cached = get_entries_fn(media_id, fingerprint=fp)
    else:
        cached = ctx.zip_cache.get(media_id)
    if cached is not None:
        return cached

    if source is None:
        source = await resolve_media_source(ctx, item)

    file_size = int(source["size"])
    if file_size < 22:
        raise HTTPException(status_code=400, detail="Not a valid ZIP archive")

    tail_len = min(file_size, 131072)
    tail_start = file_size - tail_len
    tail_bytes = await read_source_range(ctx, source, tail_start, file_size - 1)

    for _ in range(6):
        try:
            stream = _TailBackedZipIO(file_size, tail_start, tail_bytes)
            with zipfile.ZipFile(stream, "r") as zf:
                entries: list[dict[str, Any]] = []
                for info in zf.infolist():
                    if info.is_dir() or info.filename.endswith("/"):
                        continue
                    entry_path = info.filename
                    entry_name = Path(entry_path).name or entry_path
                    mtype, mime = classify_media(Path(entry_name))
                    entries.append(
                        {
                            "path": entry_path,
                            "name": entry_name,
                            "size_bytes": int(info.file_size),
                            "compress_size": int(info.compress_size),
                            "compress_type": int(info.compress_type),
                            "header_offset": int(info.header_offset),
                            "media_type": mtype,
                            "mime_type": mime,
                        }
                    )
                set_entries_fn = getattr(ctx.zip_cache, "set_entries", None)
                if callable(set_entries_fn):
                    set_entries_fn(media_id, entries, fingerprint=fp)
                else:
                    ctx.zip_cache[media_id] = entries
                return entries
        except _NeedEarlierOffset as need:
            new_start = max(0, min(need.needed_offset, tail_start))
            if new_start >= tail_start or (file_size - new_start) > 64 * 1024 * 1024:
                raise HTTPException(
                    status_code=400, detail="ZIP Central Directory is too large or invalid"
                )
            prefix = await read_source_range(ctx, source, new_start, tail_start - 1)
            tail_bytes = prefix + tail_bytes
            tail_start = new_start
        except zipfile.BadZipFile:
            raise HTTPException(status_code=400, detail="Not a valid ZIP archive")

    raise HTTPException(status_code=400, detail="Could not parse ZIP Central Directory")


async def delete_media_item(ctx: AppContext, media_id: str) -> dict[str, Any] | None:
    item = ctx.db.delete_media(media_id)
    if not item:
        return None
    invalidate_fn = getattr(ctx.zip_cache, "invalidate", None)
    if callable(invalidate_fn):
        invalidate_fn(media_id)
    else:
        ctx.zip_cache.pop(media_id, None)

    raw_path = (item.get("file_path") or "").strip()
    if raw_path:
        file_path = Path(raw_path)
        if file_path.is_file():
            try:
                file_path.unlink()
            except OSError as exc:
                logger.warning("Could not delete %s: %s", file_path, exc)

    parts = item.get("parts") or []
    if parts:
        for p in parts:
            if p.get("telegram_chat_id") and p.get("telegram_msg_id"):
                await ctx.tg_pool.delete_archive_message(
                    p["telegram_chat_id"],
                    int(p["telegram_msg_id"]),
                    worker_name=p.get("telegram_worker_name"),
                )
    elif item.get("telegram_chat_id") and item.get("telegram_msg_id"):
        await ctx.tg_pool.delete_archive_message(
            item["telegram_chat_id"],
            int(item["telegram_msg_id"]),
            worker_name=item.get("telegram_worker_name"),
        )

    if item.get("thumb_filename"):
        thumb_path = ctx.settings.thumbs_dir / item["thumb_filename"]
        if thumb_path.exists():
            thumb_path.unlink(missing_ok=True)

    preview_candidates = [ctx.settings.thumbs_dir / f"{media_id}_preview.webp"]
    if item.get("preview_filename"):
        preview_candidates.append(ctx.settings.thumbs_dir / item["preview_filename"])
    for preview_path in preview_candidates:
        if preview_path.exists():
            preview_path.unlink(missing_ok=True)

    if getattr(ctx, "watch_party", None) is not None:
        try:
            await ctx.watch_party.handle_media_deleted(media_id)
        except Exception as exc:
            logger.warning("Watch party cleanup failed for deleted media %s: %s", media_id, exc)

    return item


async def move_or_rename_media_item(
    ctx: AppContext,
    item: dict[str, Any],
    *,
    title: str | None = None,
    folder: str | None = None,
    tags: list[str] | str | None = None,
) -> dict[str, Any]:
    media_id = item["id"]
    new_title = title.strip() if isinstance(title, str) and title.strip() else None
    safe_folder = (
        sanitize_folder(folder, ctx.settings.default_folder)
        if folder is not None
        else item["folder"]
    )

    new_file_path: str | None = None
    new_filename: str | None = None
    tg_updates: dict[str, Any] = {}

    if safe_folder != item["folder"]:
        raw_path = (item.get("file_path") or "").strip()
        if raw_path:
            src_path = Path(raw_path)
            if src_path.is_file():
                drive_root: Path | None = None
                for d in ctx.settings.get_drives():
                    if d.name == item.get("drive_name"):
                        drive_root = Path(d.path)
                        break
                if drive_root is None:
                    old_parts = [
                        p
                        for p in str(item.get("folder") or "").replace("\\", "/").split("/")
                        if p
                    ]
                    cand = src_path.parent
                    if old_parts and len(src_path.parents) > len(old_parts):
                        if list(src_path.parent.parts[-len(old_parts) :]) == old_parts:
                            cand = src_path.parents[len(old_parts)]
                    drive_root = cand

                dest_dir = drive_root / safe_folder
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest_path = dest_dir / src_path.name
                if dest_path.exists() and dest_path.resolve() != src_path.resolve():
                    dest_path = (
                        dest_dir / f"{src_path.stem}_{uuid.uuid4().hex[:6]}{src_path.suffix}"
                    )
                if dest_path.resolve() != src_path.resolve():
                    await asyncio.to_thread(shutil.move, str(src_path), str(dest_path))
                new_file_path = str(dest_path)
                new_filename = dest_path.name

        if item.get("telegram_chat_id"):
            try:
                effective_tags = (
                    normalize_tags(tags)
                    if tags is not None
                    else normalize_tags(item.get("tags"))
                )
                migrated = await ctx.tg_pool.migrate_media_topic(
                    item,
                    safe_folder,
                    new_title=new_title or item["title"],
                    new_tags=effective_tags,
                )
                if migrated:
                    tg_updates = {
                        "telegram_chat_id": migrated["chat_id"],
                        "telegram_topic_id": migrated["topic_id"],
                        "telegram_msg_id": migrated["message_id"],
                        "telegram_worker_name": migrated["worker_name"],
                        "part_count": migrated["part_count"],
                        "parts": migrated["parts"],
                    }
            except Exception as exc:
                logger.warning("Telegram topic migration failed for %s: %s", media_id, exc)

    updated = ctx.db.update_media(
        media_id,
        title=new_title,
        filename=new_filename,
        folder=safe_folder if folder is not None else None,
        file_path=new_file_path,
        tags=tags,
        **tg_updates,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Media item not found")
    if new_file_path or tg_updates:
        invalidate_fn = getattr(ctx.zip_cache, "invalidate", None)
        if callable(invalidate_fn):
            invalidate_fn(media_id)
        else:
            ctx.zip_cache.pop(media_id, None)
    return updated


async def build_bulk_download_response(
    ctx: AppContext, media_ids: list[str]
) -> StreamingResponse:
    cleaned_ids = list(dict.fromkeys(mid.strip() for mid in media_ids if mid and mid.strip()))
    if not cleaned_ids:
        raise HTTPException(status_code=400, detail="No media IDs provided")

    resolved: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for mid in cleaned_ids:
        item = ctx.db.get_media(mid)
        if not item:
            continue
        src = await resolve_media_source(ctx, item)
        resolved.append((item, src))

    if not resolved:
        raise HTTPException(status_code=404, detail="No matching media items found")

    entries: list[StreamingZipEntry] = []
    used_names: set[str] = set()
    for idx, (item, src) in enumerate(resolved):
        raw_name = item.get("filename") or f"{item['id']}.bin"
        arcname = deduplicate_arcname(
            raw_name,
            used_names,
            index=idx,
            media_id=item["id"],
        )
        size = int(src["size"])

        def _make_factory(source: dict[str, Any] = src, expected_size: int = size):
            if expected_size <= 0:
                async def _empty_stream() -> AsyncIterator[bytes]:
                    if False:
                        yield b""

                return _empty_stream()
            if source["kind"] == "local":
                return _aiter_file_range(source["path"], 0, expected_size - 1)
            if source["kind"] == "multipart":
                async def _multipart_stream() -> AsyncIterator[bytes]:
                    prefetched = await prefetch_multipart_first(
                        ctx, source, 0, expected_size - 1
                    )
                    return ctx.tg_pool.iter_telegram_multipart_range(
                        source["parts"],
                        0,
                        expected_size - 1,
                        prefetched=prefetched,
                    )

                return _multipart_stream()
            return _call_iter_telegram_range(
                ctx.tg_pool,
                source["client"],
                source["media"],
                0,
                expected_size - 1,
                total_size=expected_size,
            )

        entries.append(
            StreamingZipEntry(
                arcname=arcname,
                stream_factory=_make_factory,
                expected_size=size,
            )
        )

    return StreamingResponse(
        iter_streaming_zip64(entries),
        media_type="application/zip",
        headers={
            "Content-Disposition": 'attachment; filename="media_bundle.zip"',
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
        },
    )
