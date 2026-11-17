from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import mimetypes
import re
import shutil
import struct
import tempfile
import uuid
import zipfile
import zlib
from contextlib import asynccontextmanager
from dataclasses import asdict
from importlib.metadata import PackageNotFoundError, version as pkg_version
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlencode

from fastapi import Body, Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.allocator import DriveAllocator, sanitize_folder
from app.config import Settings
from app.db import Database, normalize_tags
from app.ingest import (
    IngestService,
    classify_media,
    expand_ytdlp_playlist,
    is_direct_download,
    list_ytdlp_formats,
    resolve_format_spec,
    update_ytdlp,
)
from app.telegram_mtproto import TelegramPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

mimetypes.add_type("text/javascript", ".js")
mimetypes.add_type("text/javascript", ".mjs")
mimetypes.add_type("application/manifest+json", ".webmanifest")
mimetypes.add_type("image/svg+xml", ".svg")


class RevalidatingStaticFiles(StaticFiles):
    def file_response(self, *args: Any, **kwargs: Any) -> Response:
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "no-cache"
        return response


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


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or Settings()
    cfg.ensure_dirs()

    db = Database(cfg.db_path)
    allocator = DriveAllocator(cfg)
    ingest = IngestService(cfg, db, allocator)
    tg_pool = TelegramPool(cfg, db, allocator, ingest)
    ingest.recover_on_startup()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        ingest.recover_on_startup()
        await tg_pool.start()
        try:
            yield
        finally:
            await ingest.shutdown()
            await tg_pool.stop()
            db.close()

    app = FastAPI(title="Media Ingest & Gallery", lifespan=lifespan)
    app.state.settings = cfg
    app.state.db = db
    app.state.allocator = allocator
    app.state.ingest = ingest
    app.state.tg_pool = tg_pool

    def require_auth(request: Request, token: str | None = Query(default=None)):
        expected = cfg.web_auth_token.strip()
        if not expected:
            return
        auth_header = request.headers.get("authorization", "")
        bearer = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else ""
        if bearer != expected and (token or "") != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")

    @app.get("/api/health")
    async def health():
        cookies_ok = bool(cfg.ytdlp_cookies_file.is_file() and cfg.ytdlp_cookies_file.stat().st_size > 0)
        try:
            ytdlp_ver = pkg_version("yt-dlp")
        except PackageNotFoundError:
            ytdlp_ver = "unknown"
        return {
            "ok": True,
            "storage_mode": cfg.storage_mode,
            "telegram_workers": len(tg_pool.clients),
            "telegram_archive_enabled": bool(cfg.telegram_archive_chat_id.strip() and tg_pool.clients),
            "default_folder": cfg.default_folder,
            "auth_required": bool(cfg.web_auth_token.strip()),
            "max_upload_mb": cfg.max_upload_mb,
            "telegram_part_size_mb": cfg.telegram_part_size_mb,
            "ingest_concurrency": cfg.ingest_concurrency,
            "ytdlp_cookies_configured": cookies_ok,
            "ytdlp_version": ytdlp_ver,
        }

    @app.get("/api/drives", dependencies=[Depends(require_auth)])
    async def list_drives():
        return {
            "storage_mode": cfg.storage_mode,
            "strategy": cfg.allocation_strategy,
            "min_free_buffer_mb": cfg.min_free_buffer_mb,
            "drives": [asdict(d) for d in allocator.get_drive_statuses()],
        }

    @app.get("/api/folders", dependencies=[Depends(require_auth)])
    async def list_folders():
        folders = db.list_folders()
        if cfg.default_folder not in folders:
            folders.insert(0, cfg.default_folder)
        return {"folders": folders, "default_folder": cfg.default_folder}

    @app.get("/api/tags", dependencies=[Depends(require_auth)])
    async def list_tags(
        q: str = Query(default=""),
        limit: int | None = Query(default=None, ge=1, le=5000),
    ):
        return {"tags": db.list_tag_counts(q=q.strip() or None, limit=limit)}

    @app.get("/api/jobs", dependencies=[Depends(require_auth)])
    async def list_jobs(limit: int = Query(default=25, ge=1, le=100)):
        return {"jobs": db.list_jobs(limit=limit)}

    @app.get("/api/jobs/{job_id}", dependencies=[Depends(require_auth)])
    async def get_job(job_id: str):
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return job

    @app.post("/api/jobs/{job_id}/cancel", dependencies=[Depends(require_auth)])
    async def cancel_job(job_id: str):
        try:
            return await ingest.cancel_job(job_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.post("/api/jobs/{job_id}/retry", status_code=202, dependencies=[Depends(require_auth)])
    async def retry_job(
        job_id: str,
        force: bool = Query(default=False),
        payload: RetryJobRequest | None = Body(default=None),
    ):
        effective_force = bool(force or (payload.force if payload else False))
        try:
            return await ingest.retry_job(job_id, force=effective_force)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.get("/api/events", dependencies=[Depends(require_auth)])
    async def stream_events(request: Request):
        async def event_stream():
            sub = db.subscribe()
            _, queue = sub
            try:
                yield ": connected\n\n"
                init_ev = {
                    "type": "connected",
                    "ok": True,
                    "jobs": db.list_jobs(limit=8),
                }
                yield f"data: {json.dumps(init_ev)}\n\n"
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
                        continue
                    if not isinstance(event, dict) or event.get("type") == "_shutdown":
                        break
                    payload = dict(event)
                    if payload.get("type") == "job_update" and "jobs" not in payload:
                        payload["jobs"] = db.list_jobs(limit=8)
                    yield f"data: {json.dumps(payload)}\n\n"
            except asyncio.CancelledError:
                pass
            finally:
                db.unsubscribe(sub)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.post("/api/ytdlp/update", dependencies=[Depends(require_auth)])
    async def trigger_ytdlp_update():
        try:
            return await update_ytdlp()
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    @app.post("/api/ytdlp/formats", dependencies=[Depends(require_auth)])
    async def inspect_ytdlp_formats(payload: FormatsRequest):
        url = payload.url.strip()
        if not url.startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
        try:
            return await list_ytdlp_formats(
                url,
                cookies_file=cfg.ytdlp_cookies_file,
                proxy=cfg.ytdlp_proxy,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @app.post("/api/ytdlp/playlist", dependencies=[Depends(require_auth)])
    async def inspect_ytdlp_playlist(payload: PlaylistRequest):
        url = payload.url.strip()
        if not url.startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
        try:
            return await expand_ytdlp_playlist(
                url,
                cookies_file=cfg.ytdlp_cookies_file,
                proxy=cfg.ytdlp_proxy,
                max_items=max(1, min(500, int(payload.max_items))),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @app.post("/api/ytdlp/cookies", dependencies=[Depends(require_auth)])
    async def save_ytdlp_cookies(payload: CookiesRequest):
        raw = payload.cookies_text.replace("\r\n", "\n").strip()
        if not raw:
            raise HTTPException(status_code=400, detail="Cookies text cannot be empty.")
        has_cookie_line = any(
            line and not line.startswith("#") and "\t" in line
            for line in raw.splitlines()
        )
        if not has_cookie_line:
            raise HTTPException(
                status_code=400,
                detail="Expected Netscape cookies.txt format (tab-separated cookie rows).",
            )
        cfg.ytdlp_cookies_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.ytdlp_cookies_file.write_text(raw + "\n", encoding="utf-8")
        return {"saved": True, "ytdlp_cookies_configured": True}

    @app.delete("/api/ytdlp/cookies", dependencies=[Depends(require_auth)])
    async def clear_ytdlp_cookies():
        if cfg.ytdlp_cookies_file.exists():
            cfg.ytdlp_cookies_file.unlink(missing_ok=True)
        return {"deleted": True, "ytdlp_cookies_configured": False}

    @app.post("/api/ingest/check-duplicate", dependencies=[Depends(require_auth)])
    async def check_duplicate(payload: CheckDuplicateRequest):
        existing: dict[str, Any] | None = None
        if payload.sha256:
            existing = db.find_media_by_sha256(payload.sha256)
        if not existing and payload.url:
            existing = db.find_media_by_source_url(payload.url)
        return {"duplicate": existing is not None, "item": existing}

    @app.post("/api/ingest/url", status_code=202, dependencies=[Depends(require_auth)])
    async def ingest_url(payload: UrlIngestRequest):
        raw_urls: list[str] = []
        if payload.urls:
            for item in payload.urls:
                for line in re.split(r"\r?\n", str(item)):
                    s = line.strip()
                    if s and not s.startswith("#"):
                        raw_urls.append(s)
        if payload.url:
            for line in re.split(r"\r?\n", payload.url):
                s = line.strip()
                if s and not s.startswith("#"):
                    raw_urls.append(s)

        if not raw_urls:
            raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
        for u in raw_urls:
            if not u.startswith(("http://", "https://")):
                raise HTTPException(status_code=400, detail="URL must start with http:// or https://")

        try:
            resolve_format_spec(payload.format_spec)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        base_folder = payload.folder
        queue_items: list[tuple[str, str | None, str | None]] = []
        for u in raw_urls:
            if payload.expand_playlist and not await is_direct_download(u):
                try:
                    pl_info = await expand_ytdlp_playlist(
                        u,
                        cookies_file=cfg.ytdlp_cookies_file,
                        proxy=cfg.ytdlp_proxy,
                        max_items=max(1, min(500, int(payload.max_playlist_items))),
                    )
                except Exception as exc:
                    raise HTTPException(status_code=400, detail=str(exc))
                if pl_info.get("is_playlist") and pl_info.get("entries"):
                    pl_title = (pl_info.get("playlist_title") or "").strip()
                    entry_folder = base_folder
                    if payload.use_playlist_folder and pl_title:
                        clean_base = (base_folder or "").strip()
                        if (
                            not clean_base
                            or sanitize_folder(clean_base, cfg.default_folder) == cfg.default_folder
                        ):
                            entry_folder = sanitize_folder(pl_title, cfg.default_folder)
                        else:
                            entry_folder = sanitize_folder(
                                f"{clean_base}/{pl_title}", cfg.default_folder
                            )
                    for entry in pl_info["entries"]:
                        queue_items.append((entry["url"], entry.get("title"), entry_folder))
                    continue
            queue_items.append((u, payload.title if len(raw_urls) == 1 else None, base_folder))

        job_ids: list[str] = []
        for target_url, target_title, target_folder in queue_items:
            job_params = {
                "url": target_url,
                "folder": target_folder,
                "tags": payload.tags,
                "title": target_title,
                "forward_to_telegram": payload.forward_to_telegram,
                "format_spec": payload.format_spec,
                "force": payload.force,
                "source_origin": "web",
            }
            job_id = db.create_job("web", "url", target_url, params=job_params)
            ingest.submit_job(
                job_id,
                ingest.process_url_job(
                    job_id,
                    target_url,
                    source_origin="web",
                    folder=target_folder,
                    tags=payload.tags,
                    title=target_title,
                    forward_to_telegram=payload.forward_to_telegram,
                    format_spec=payload.format_spec,
                    force=payload.force,
                ),
            )
            job_ids.append(job_id)

        return {
            "job_id": job_ids[0],
            "job_ids": job_ids,
            "count": len(job_ids),
            "status": "queued",
        }

    @app.post("/api/ingest/upload", status_code=202, dependencies=[Depends(require_auth)])
    async def ingest_upload(
        file: UploadFile = File(...),
        title: str | None = Form(default=None),
        folder: str | None = Form(default=None),
        tags: str | None = Form(default=None),
        forward_to_telegram: bool = Form(default=True),
        force: bool = Form(default=False),
    ):
        raw_name = Path(file.filename or "upload.bin").name
        resolved_title = title or Path(raw_name).stem
        job_id = db.create_job(
            "web",
            "upload",
            raw_name,
            params={
                "folder": folder,
                "tags": tags,
                "title": resolved_title,
                "forward_to_telegram": forward_to_telegram,
                "force": force,
                "source_origin": "web",
            },
        )
        job_dir = Path(tempfile.mkdtemp(dir=cfg.staging_dir))
        staged_path = job_dir / raw_name
        written = 0
        hasher = hashlib.sha256()

        try:
            db.update_job(job_id, status="downloading", message=f"Receiving {raw_name}...")
            with staged_path.open("wb") as out:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > cfg.max_upload_bytes:
                        raise HTTPException(
                            status_code=413,
                            detail=f"File exceeds {cfg.max_upload_mb} MB limit.",
                        )
                    hasher.update(chunk)
                    await asyncio.to_thread(out.write, chunk)

            db.update_job(
                job_id,
                status="queued",
                progress=50.0,
                message=f"Staged {raw_name}, queued for processing...",
            )
            ingest.submit_job(
                job_id,
                ingest.process_staged_upload_job(
                    job_id,
                    staged_path,
                    source_origin="web",
                    folder=folder,
                    tags=tags,
                    title=resolved_title,
                    forward_to_telegram=forward_to_telegram,
                    force=force,
                    precomputed_sha256=hasher.hexdigest(),
                ),
            )
            return {"job_id": job_id, "status": "queued"}
        except HTTPException as he:
            shutil.rmtree(job_dir, ignore_errors=True)
            db.update_job(job_id, status="failed", message=str(he.detail))
            raise
        except Exception as exc:
            shutil.rmtree(job_dir, ignore_errors=True)
            db.update_job(job_id, status="failed", message=str(exc))
            raise HTTPException(status_code=500, detail=str(exc))

    @app.post("/api/ingest/upload-folder", status_code=202, dependencies=[Depends(require_auth)])
    async def ingest_upload_folder(
        files: list[UploadFile] = File(...),
        relative_paths: list[str] = Form(default=[]),
        archive_name: str | None = Form(default=None),
        folder: str | None = Form(default=None),
        tags: str | None = Form(default=None),
        forward_to_telegram: bool = Form(default=True),
        force: bool = Form(default=False),
    ):
        if not files:
            raise HTTPException(status_code=400, detail="No files provided.")

        raw_archive = (archive_name or "").strip()
        if not raw_archive and relative_paths:
            first_rel = relative_paths[0].replace("\\", "/").strip("/")
            if "/" in first_rel:
                raw_archive = first_rel.split("/", 1)[0]
        safe_archive = Path(raw_archive or "folder_archive").name.strip(" .") or "folder_archive"
        if safe_archive.lower().endswith(".zip"):
            folder_title = safe_archive[:-4].strip(" .") or "folder_archive"
            zip_filename = f"{folder_title}.zip"
        else:
            folder_title = safe_archive
            zip_filename = f"{safe_archive}.zip"

        job_id = db.create_job(
            "web",
            "upload",
            zip_filename,
            params={
                "folder": folder,
                "tags": tags,
                "title": folder_title,
                "forward_to_telegram": forward_to_telegram,
                "force": force,
                "source_origin": "web",
            },
        )
        job_dir = Path(tempfile.mkdtemp(dir=cfg.staging_dir))
        zip_path = job_dir / zip_filename
        total_written = 0
        seen_arcnames: set[str] = set()

        try:
            db.update_job(
                job_id,
                status="downloading",
                progress=5.0,
                message=f"Packing {len(files)} file(s) into {zip_filename}...",
            )
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
                for idx, upload_file in enumerate(files):
                    rel_raw = relative_paths[idx] if idx < len(relative_paths) else (upload_file.filename or f"file_{idx}")
                    arcname = _sanitize_archive_relpath(rel_raw, upload_file.filename or f"file_{idx}")
                    if arcname in seen_arcnames:
                        stem = Path(arcname).stem
                        suffix = Path(arcname).suffix
                        parent = Path(arcname).parent
                        dedup_name = f"{stem}_{idx}{suffix}"
                        arcname = f"{parent.as_posix()}/{dedup_name}" if parent.as_posix() != "." else dedup_name
                    seen_arcnames.add(arcname)

                    with zf.open(arcname, "w", force_zip64=True) as z_entry:
                        while True:
                            chunk = await upload_file.read(1024 * 1024)
                            if not chunk:
                                break
                            total_written += len(chunk)
                            if total_written > cfg.max_upload_bytes:
                                raise HTTPException(
                                    status_code=413,
                                    detail=f"Folder archive exceeds {cfg.max_upload_mb} MB limit.",
                                )
                            await asyncio.to_thread(z_entry.write, chunk)

            if zip_path.stat().st_size > cfg.max_upload_bytes:
                raise HTTPException(
                    status_code=413,
                    detail=f"Folder archive exceeds {cfg.max_upload_mb} MB limit.",
                )

            db.update_job(
                job_id,
                status="queued",
                progress=50.0,
                message=f"Staged {zip_filename}, queued for processing...",
            )
            ingest.submit_job(
                job_id,
                ingest.process_staged_upload_job(
                    job_id,
                    zip_path,
                    source_origin="web",
                    folder=folder,
                    tags=tags,
                    title=folder_title,
                    forward_to_telegram=forward_to_telegram,
                    force=force,
                ),
            )
            return {"job_id": job_id, "status": "queued"}
        except HTTPException as he:
            shutil.rmtree(job_dir, ignore_errors=True)
            db.update_job(job_id, status="failed", message=str(he.detail))
            raise
        except Exception as exc:
            shutil.rmtree(job_dir, ignore_errors=True)
            db.update_job(job_id, status="failed", message=str(exc))
            raise HTTPException(status_code=500, detail=str(exc))

    zip_cache: dict[str, list[dict[str, Any]]] = {}
    app.state.zip_cache = zip_cache

    async def _resolve_media_source(item: dict[str, Any]) -> dict[str, Any]:
        raw_path = (item.get("file_path") or "").strip()
        file_path = Path(raw_path) if raw_path else None
        if file_path and file_path.is_file():
            return {"kind": "local", "path": file_path, "size": file_path.stat().st_size}

        parts = item.get("parts") or []
        if len(parts) > 1:
            return {"kind": "multipart", "parts": parts, "size": int(item["size_bytes"])}

        if item.get("telegram_chat_id") and item.get("telegram_msg_id"):
            try:
                tg_client, tg_media = await tg_pool.prepare_stream(
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

    async def _prefetch_multipart_first(
        source: dict[str, Any], start: int, end: int
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
            first_stream = await tg_pool.prepare_stream(
                first_part["telegram_chat_id"],
                int(first_part["telegram_msg_id"]),
                worker_name=None,
            )
            return {int(first_part["part_index"]): first_stream}
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Telegram stream error: {exc}")

    async def _read_source_range(source: dict[str, Any], start: int, end: int) -> bytes:
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
            async for chunk in tg_pool.iter_telegram_multipart_range(source["parts"], start, end):
                buf.extend(chunk)
        else:
            async for chunk in tg_pool.iter_telegram_range(
                source["client"], source["media"], start, end
            ):
                buf.extend(chunk)
        return bytes(buf)

    def _iter_source_range(
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
            return tg_pool.iter_telegram_multipart_range(
                source["parts"], start, end, prefetched=prefetched
            )
        return tg_pool.iter_telegram_range(source["client"], source["media"], start, end)

    async def _get_zip_entries(
        item: dict[str, Any], source: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        media_id = item["id"]
        cached = zip_cache.get(media_id)
        if cached is not None:
            return cached

        if source is None:
            source = await _resolve_media_source(item)

        file_size = int(source["size"])
        if file_size < 22:
            raise HTTPException(status_code=400, detail="Not a valid ZIP archive")

        tail_len = min(file_size, 131072)
        tail_start = file_size - tail_len
        tail_bytes = await _read_source_range(source, tail_start, file_size - 1)

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
                    zip_cache[media_id] = entries
                    return entries
            except _NeedEarlierOffset as need:
                new_start = max(0, min(need.needed_offset, tail_start))
                if new_start >= tail_start or (file_size - new_start) > 64 * 1024 * 1024:
                    raise HTTPException(
                        status_code=400, detail="ZIP Central Directory is too large or invalid"
                    )
                prefix = await _read_source_range(source, new_start, tail_start - 1)
                tail_bytes = prefix + tail_bytes
                tail_start = new_start
            except zipfile.BadZipFile:
                raise HTTPException(status_code=400, detail="Not a valid ZIP archive")

        raise HTTPException(status_code=400, detail="Could not parse ZIP Central Directory")

    async def _delete_media_item(media_id: str) -> dict[str, Any] | None:
        item = db.delete_media(media_id)
        if not item:
            return None
        zip_cache.pop(media_id, None)

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
                    await tg_pool.delete_archive_message(
                        p["telegram_chat_id"],
                        int(p["telegram_msg_id"]),
                        worker_name=p.get("telegram_worker_name"),
                    )
        elif item.get("telegram_chat_id") and item.get("telegram_msg_id"):
            await tg_pool.delete_archive_message(
                item["telegram_chat_id"],
                int(item["telegram_msg_id"]),
                worker_name=item.get("telegram_worker_name"),
            )

        if item.get("thumb_filename"):
            thumb_path = cfg.thumbs_dir / item["thumb_filename"]
            if thumb_path.exists():
                thumb_path.unlink(missing_ok=True)

        preview_candidates = [cfg.thumbs_dir / f"{media_id}_preview.webp"]
        if item.get("preview_filename"):
            preview_candidates.append(cfg.thumbs_dir / item["preview_filename"])
        for preview_path in preview_candidates:
            if preview_path.exists():
                preview_path.unlink(missing_ok=True)

        return item

    async def _move_or_rename_media_item(
        item: dict[str, Any],
        *,
        title: str | None = None,
        folder: str | None = None,
        tags: list[str] | str | None = None,
    ) -> dict[str, Any]:
        media_id = item["id"]
        new_title = title.strip() if isinstance(title, str) and title.strip() else None
        safe_folder = (
            sanitize_folder(folder, cfg.default_folder)
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
                    for d in cfg.get_drives():
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
                    migrated = await tg_pool.migrate_media_topic(
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

        updated = db.update_media(
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
        return updated

    async def _build_bulk_download_response(media_ids: list[str]) -> StreamingResponse:
        cleaned_ids = list(dict.fromkeys(mid.strip() for mid in media_ids if mid and mid.strip()))
        if not cleaned_ids:
            raise HTTPException(status_code=400, detail="No media IDs provided")

        resolved: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for mid in cleaned_ids:
            item = db.get_media(mid)
            if not item:
                continue
            src = await _resolve_media_source(item)
            resolved.append((item, src))

        if not resolved:
            raise HTTPException(status_code=404, detail="No matching media items found")

        spooled = tempfile.SpooledTemporaryFile(max_size=32 * 1024 * 1024)
        used_names: set[str] = set()
        try:
            with zipfile.ZipFile(
                spooled, "w", compression=zipfile.ZIP_STORED, allowZip64=True
            ) as zf:
                for idx, (item, src) in enumerate(resolved):
                    raw_name = Path(item.get("filename") or f"{item['id']}.bin").name
                    arcname = raw_name
                    if arcname in used_names:
                        p = Path(raw_name)
                        arcname = f"{p.stem}_{idx + 1}_{item['id'][:6]}{p.suffix}"
                    used_names.add(arcname)

                    with zf.open(arcname, "w", force_zip64=True) as z_entry:
                        size = int(src["size"])
                        if size > 0:
                            if src["kind"] == "local":
                                for chunk in _iter_file_range(src["path"], 0, size - 1):
                                    await asyncio.to_thread(z_entry.write, chunk)
                            elif src["kind"] == "multipart":
                                prefetched = await _prefetch_multipart_first(src, 0, size - 1)
                                async for chunk in tg_pool.iter_telegram_multipart_range(
                                    src["parts"], 0, size - 1, prefetched=prefetched
                                ):
                                    await asyncio.to_thread(z_entry.write, chunk)
                            else:
                                async for chunk in tg_pool.iter_telegram_range(
                                    src["client"], src["media"], 0, size - 1
                                ):
                                    await asyncio.to_thread(z_entry.write, chunk)

            spooled.seek(0, io.SEEK_END)
            bundle_size = spooled.tell()
            spooled.seek(0)
        except Exception:
            spooled.close()
            raise

        def _iter_bundle():
            try:
                while True:
                    chunk = spooled.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk
            finally:
                spooled.close()

        return StreamingResponse(
            _iter_bundle(),
            media_type="application/zip",
            headers={
                "Content-Disposition": 'attachment; filename="media_bundle.zip"',
                "Content-Length": str(bundle_size),
            },
        )

    @app.get("/api/media", dependencies=[Depends(require_auth)])
    async def list_media(
        media_type: str = Query(default="all"),
        drive_name: str = Query(default="all"),
        folder: str = Query(default="all"),
        tag: str = Query(default="all"),
        tags: str = Query(default=""),
        q: str = Query(default=""),
        sort: str = Query(default="newest"),
        limit: int = Query(default=60, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ):
        effective_tag = tag
        if tags.strip():
            effective_tag = f"{tag},{tags}" if (tag and tag != "all") else tags
        items, total = db.list_media(
            media_type=media_type,
            drive_name=drive_name,
            folder=folder,
            tag=effective_tag,
            q=q.strip() or None,
            sort=sort,
            limit=limit,
            offset=offset,
        )
        return {"items": items, "total": total}

    @app.post("/api/media/bulk/delete", dependencies=[Depends(require_auth)])
    async def bulk_delete_media(payload: BulkMediaIdsRequest):
        deleted_ids: list[str] = []
        for mid in dict.fromkeys(m.strip() for m in payload.media_ids if m and m.strip()):
            removed = await _delete_media_item(mid)
            if removed:
                deleted_ids.append(mid)
        return {"deleted": len(deleted_ids), "ids": deleted_ids}

    @app.post("/api/media/bulk/move", dependencies=[Depends(require_auth)])
    async def bulk_move_media(payload: BulkMoveRequest):
        safe_folder = sanitize_folder(payload.folder, cfg.default_folder)
        updated_items: list[dict[str, Any]] = []
        for mid in dict.fromkeys(m.strip() for m in payload.media_ids if m and m.strip()):
            item = db.get_media(mid)
            if not item:
                continue
            updated = await _move_or_rename_media_item(item, folder=safe_folder)
            updated_items.append(updated)
        return {"moved": len(updated_items), "folder": safe_folder, "items": updated_items}

    @app.post("/api/media/bulk/tags", dependencies=[Depends(require_auth)])
    async def bulk_tags_media(payload: BulkTagsRequest):
        incoming = normalize_tags(payload.tags)
        remove_set = set(incoming)
        updated_items: list[dict[str, Any]] = []
        for mid in dict.fromkeys(m.strip() for m in payload.media_ids if m and m.strip()):
            item = db.get_media(mid)
            if not item:
                continue
            existing = normalize_tags(item.get("tags"))
            if payload.mode == "replace":
                next_tags = incoming
            elif payload.mode == "remove":
                next_tags = [t for t in existing if t not in remove_set]
            else:
                next_tags = normalize_tags([*existing, *incoming])
            updated = db.update_media_tags(mid, next_tags)
            if updated:
                updated_items.append(updated)
        return {"updated": len(updated_items), "items": updated_items}

    @app.get("/api/media/bulk/download", dependencies=[Depends(require_auth)])
    async def bulk_download_media_get(ids: str = Query(...)):
        return await _build_bulk_download_response(ids.split(","))

    @app.post("/api/media/bulk/download", dependencies=[Depends(require_auth)])
    async def bulk_download_media_post(payload: BulkMediaIdsRequest):
        return await _build_bulk_download_response(payload.media_ids)

    @app.get("/api/media/{media_id}", dependencies=[Depends(require_auth)])
    async def get_media_item(media_id: str):
        item = db.get_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")
        return item

    @app.patch("/api/media/{media_id}", dependencies=[Depends(require_auth)])
    async def patch_media_item(media_id: str, payload: UpdateMediaRequest):
        item = db.get_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")
        return await _move_or_rename_media_item(
            item,
            title=payload.title,
            folder=payload.folder,
            tags=payload.tags,
        )

    @app.patch("/api/media/{media_id}/tags", dependencies=[Depends(require_auth)])
    async def update_media_tags(media_id: str, payload: UpdateMediaTagsRequest):
        item = db.update_media_tags(media_id, payload.tags)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")
        return item

    @app.get("/api/media/{media_id}/thumb", dependencies=[Depends(require_auth)])
    async def get_media_thumb(media_id: str):
        item = db.get_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Thumbnail not found")

        candidates: list[Path] = []
        if item.get("thumb_filename"):
            candidates.append(cfg.thumbs_dir / item["thumb_filename"])
        candidates.extend(
            [
                cfg.thumbs_dir / f"{media_id}.webp",
                cfg.thumbs_dir / f"{media_id}.jpg",
                cfg.thumbs_dir / f"{media_id}.jpeg",
                cfg.thumbs_dir / f"{media_id}.png",
            ]
        )

        thumb_path = next((p for p in candidates if p.is_file()), None)
        if thumb_path is None:
            detail = "Thumbnail not found" if not item.get("thumb_filename") else "Thumbnail file missing"
            raise HTTPException(status_code=404, detail=detail)

        media_type = {
            ".webp": "image/webp",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
        }.get(thumb_path.suffix.lower(), "image/webp")
        return FileResponse(
            thumb_path,
            media_type=media_type,
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )

    @app.get("/api/media/{media_id}/preview", dependencies=[Depends(require_auth)])
    async def get_media_preview(media_id: str):
        item = db.get_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Preview not found")

        candidates: list[Path] = []
        if item.get("preview_filename"):
            candidates.append(cfg.thumbs_dir / item["preview_filename"])
        candidates.append(cfg.thumbs_dir / f"{media_id}_preview.webp")

        preview_path = next((p for p in candidates if p.is_file()), None)
        if preview_path is None:
            raise HTTPException(status_code=404, detail="Preview not found")

        return FileResponse(
            preview_path,
            media_type="image/webp",
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )

    @app.get("/api/media/{media_id}/zip/entries", dependencies=[Depends(require_auth)])
    async def get_zip_entries(media_id: str):
        item = db.get_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")
        entries = await _get_zip_entries(item)
        return {
            "media_id": media_id,
            "filename": item["filename"],
            "total_entries": len(entries),
            "entries": [
                {
                    "path": e["path"],
                    "name": e["name"],
                    "size_bytes": e["size_bytes"],
                    "compress_size": e["compress_size"],
                    "compress_type": e["compress_type"],
                    "media_type": e["media_type"],
                    "mime_type": e["mime_type"],
                }
                for e in entries
            ],
        }

    @app.get("/api/media/{media_id}/zip/stream", dependencies=[Depends(require_auth)])
    async def stream_zip_entry(
        media_id: str,
        request: Request,
        path: str = Query(...),
        download: bool = Query(default=False),
    ):
        item = db.get_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")

        source = await _resolve_media_source(item)
        entries = await _get_zip_entries(item, source=source)
        norm_target = path.lstrip("/")
        entry = next(
            (e for e in entries if e["path"] == path or e["path"].lstrip("/") == norm_target),
            None,
        )
        if entry is None:
            raise HTTPException(status_code=404, detail="Entry not found in ZIP archive")

        header_offset = int(entry["header_offset"])
        lfh = await _read_source_range(source, header_offset, header_offset + 29)
        if len(lfh) != 30 or lfh[:4] != b"PK\x03\x04":
            raise HTTPException(status_code=400, detail="Invalid ZIP local file header")

        fname_len, extra_len = struct.unpack("<HH", lfh[26:30])
        data_start = header_offset + 30 + fname_len + extra_len
        compress_size = int(entry["compress_size"])
        size_bytes = int(entry["size_bytes"])
        compress_type = int(entry["compress_type"])
        data_end = data_start + compress_size - 1

        mime_type = entry["mime_type"] or "application/octet-stream"
        headers: dict[str, str] = {}
        if download:
            headers["Content-Disposition"] = f'attachment; filename="{entry["name"]}"'

        range_header = request.headers.get("range")
        if range_header:
            try:
                req_start, req_end = _parse_range_header(range_header, size_bytes)
            except ValueError:
                raise HTTPException(
                    status_code=416,
                    headers={"Content-Range": f"bytes */{size_bytes}"},
                    detail="Invalid Range header",
                )
        else:
            req_start, req_end = 0, max(0, size_bytes - 1)

        if compress_type == zipfile.ZIP_STORED:
            headers["Accept-Ranges"] = "bytes"
            if range_header:
                arc_start = data_start + req_start
                arc_end = data_start + req_end
                prefetched = await _prefetch_multipart_first(source, arc_start, arc_end)
                headers["Content-Range"] = f"bytes {req_start}-{req_end}/{size_bytes}"
                headers["Content-Length"] = str(req_end - req_start + 1)
                return StreamingResponse(
                    _iter_source_range(source, arc_start, arc_end, prefetched=prefetched),
                    status_code=206,
                    media_type=mime_type,
                    headers=headers,
                )
            headers["Content-Length"] = str(size_bytes)
            if size_bytes == 0:
                return StreamingResponse(
                    iter([b""]), status_code=200, media_type=mime_type, headers=headers
                )
            arc_start = data_start
            arc_end = data_start + size_bytes - 1
            prefetched = await _prefetch_multipart_first(source, arc_start, arc_end)
            return StreamingResponse(
                _iter_source_range(source, arc_start, arc_end, prefetched=prefetched),
                status_code=200,
                media_type=mime_type,
                headers=headers,
            )

        if compress_type == zipfile.ZIP_DEFLATED:
            if compress_size == 0 or size_bytes == 0:
                headers["Content-Length"] = "0"
                return StreamingResponse(
                    iter([b""]), status_code=200, media_type=mime_type, headers=headers
                )

            prefetched = await _prefetch_multipart_first(source, data_start, data_end)

            async def _iter_deflated():
                decomp = zlib.decompressobj(-zlib.MAX_WBITS)
                raw_iter = _iter_source_range(source, data_start, data_end, prefetched=prefetched)
                pos = 0

                def _slice_out(chunk: bytes) -> bytes | None:
                    nonlocal pos
                    if not chunk:
                        return None
                    c_start = pos
                    c_end = pos + len(chunk) - 1
                    pos += len(chunk)
                    if not range_header:
                        return chunk
                    if c_end < req_start or c_start > req_end:
                        return None
                    s0 = max(0, req_start - c_start)
                    s1 = min(len(chunk), req_end - c_start + 1)
                    return chunk[s0:s1]

                if hasattr(raw_iter, "__aiter__"):
                    async for raw_chunk in raw_iter:
                        out = decomp.decompress(raw_chunk)
                        sliced = _slice_out(out)
                        if sliced:
                            yield sliced
                        if range_header and pos > req_end:
                            break
                else:
                    for raw_chunk in raw_iter:
                        out = decomp.decompress(raw_chunk)
                        sliced = _slice_out(out)
                        if sliced:
                            yield sliced
                        if range_header and pos > req_end:
                            break

                if not (range_header and pos > req_end):
                    tail = decomp.flush()
                    sliced = _slice_out(tail)
                    if sliced:
                        yield sliced

            if range_header:
                headers["Accept-Ranges"] = "bytes"
                headers["Content-Range"] = f"bytes {req_start}-{req_end}/{size_bytes}"
                headers["Content-Length"] = str(req_end - req_start + 1)
                return StreamingResponse(
                    _iter_deflated(),
                    status_code=206,
                    media_type=mime_type,
                    headers=headers,
                )

            headers["Content-Length"] = str(size_bytes)
            return StreamingResponse(
                _iter_deflated(),
                status_code=200,
                media_type=mime_type,
                headers=headers,
            )

        raise HTTPException(
            status_code=400, detail=f"Unsupported ZIP compression method: {compress_type}"
        )

    @app.get("/api/media/{media_id}/stream", dependencies=[Depends(require_auth)])
    async def stream_media(media_id: str, request: Request, download: bool = Query(default=False)):
        item = db.get_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")

        raw_path = (item.get("file_path") or "").strip()
        file_path = Path(raw_path) if raw_path else None
        use_local = bool(file_path and file_path.is_file())
        parts = item.get("parts") or []
        is_multipart = not use_local and len(parts) > 1

        tg_client = None
        tg_media = None
        if use_local:
            assert file_path is not None
            file_size = file_path.stat().st_size
        elif is_multipart:
            file_size = int(item["size_bytes"])
        elif item.get("telegram_chat_id") and item.get("telegram_msg_id"):
            try:
                tg_client, tg_media = await tg_pool.prepare_stream(
                    item["telegram_chat_id"],
                    int(item["telegram_msg_id"]),
                    worker_name=item.get("telegram_worker_name"),
                )
            except FileNotFoundError as exc:
                raise HTTPException(status_code=404, detail=str(exc))
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Telegram stream error: {exc}")
            file_size = _telegram_media_actual_size(tg_media) or int(item["size_bytes"])
        else:
            raise HTTPException(status_code=404, detail="File missing on drive (drive may be unmounted)")

        mime_type = item.get("mime_type") or "application/octet-stream"
        headers = {"Accept-Ranges": "bytes"}
        if download:
            headers["Content-Disposition"] = f'attachment; filename="{item["filename"]}"'

        range_header = request.headers.get("range")
        if range_header:
            try:
                start, end = _parse_range_header(range_header, file_size)
            except ValueError:
                raise HTTPException(
                    status_code=416,
                    headers={"Content-Range": f"bytes */{file_size}"},
                    detail="Invalid Range header",
                )
        else:
            start, end = 0, max(0, file_size - 1)

        prefetched_parts: dict[int, tuple[Any, Any]] | None = None
        if is_multipart and file_size > 0:
            overlapping = [
                p for p in sorted(parts, key=lambda x: int(x["part_index"]))
                if int(p["end_byte"]) >= start and int(p["start_byte"]) <= end and int(p["size_bytes"]) > 0
            ]
            if not overlapping:
                raise HTTPException(status_code=404, detail="Requested range has no matching media parts")
            first_part = overlapping[0]
            try:
                first_stream = await tg_pool.prepare_stream(
                    first_part["telegram_chat_id"],
                    int(first_part["telegram_msg_id"]),
                    worker_name=None,
                )
                prefetched_parts = {int(first_part["part_index"]): first_stream}
            except FileNotFoundError as exc:
                raise HTTPException(status_code=404, detail=str(exc))
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Telegram stream error: {exc}")

        if range_header:
            content_length = end - start + 1
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
            headers["Content-Length"] = str(content_length)
            if use_local and file_path is not None:
                body = _iter_file_range(file_path, start, end)
            elif is_multipart:
                body = tg_pool.iter_telegram_multipart_range(parts, start, end, prefetched=prefetched_parts)
            else:
                body = tg_pool.iter_telegram_range(tg_client, tg_media, start, end)
            return StreamingResponse(
                body,
                status_code=206,
                media_type=mime_type,
                headers=headers,
            )

        headers["Content-Length"] = str(file_size)
        if file_size == 0:
            body = iter([b""])
        elif use_local and file_path is not None:
            body = _iter_file_range(file_path, 0, file_size - 1)
        elif is_multipart:
            body = tg_pool.iter_telegram_multipart_range(parts, 0, file_size - 1, prefetched=prefetched_parts)
        else:
            body = tg_pool.iter_telegram_range(tg_client, tg_media, 0, file_size - 1)

        return StreamingResponse(
            body,
            status_code=200,
            media_type=mime_type,
            headers=headers,
        )

    @app.delete("/api/media/{media_id}", dependencies=[Depends(require_auth)])
    async def delete_media(media_id: str):
        item = await _delete_media_item(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")
        return {"deleted": True, "id": media_id}

    if STATIC_DIR.exists():
        app.mount("/static", RevalidatingStaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/sw.js")
    async def service_worker():
        sw_file = STATIC_DIR / "sw.js"
        if not sw_file.is_file():
            raise HTTPException(status_code=404, detail="Service worker not found")
        return FileResponse(
            sw_file,
            media_type="text/javascript",
            headers={
                "Cache-Control": "no-cache",
                "Service-Worker-Allowed": "/",
            },
        )

    @app.get("/manifest.webmanifest")
    async def web_manifest():
        manifest_file = STATIC_DIR / "manifest.webmanifest"
        if not manifest_file.is_file():
            raise HTTPException(status_code=404, detail="Manifest not found")
        return FileResponse(
            manifest_file,
            media_type="application/manifest+json",
            headers={"Cache-Control": "no-cache"},
        )

    @app.get("/manifest.json")
    async def web_manifest_json():
        manifest_file = STATIC_DIR / "manifest.json"
        if not manifest_file.is_file():
            manifest_file = STATIC_DIR / "manifest.webmanifest"
        if not manifest_file.is_file():
            raise HTTPException(status_code=404, detail="Manifest not found")
        return FileResponse(
            manifest_file,
            media_type="application/manifest+json",
            headers={"Cache-Control": "no-cache"},
        )

    @app.get("/favicon.ico")
    async def favicon():
        icon_file = STATIC_DIR / "icons" / "icon-192.png"
        if not icon_file.is_file():
            raise HTTPException(status_code=404, detail="Favicon not found")
        return FileResponse(
            icon_file,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    @app.api_route("/share-target", methods=["GET", "POST"])
    async def share_target_fallback(request: Request):
        # Fallback only reached when the PWA service worker did not intercept an
        # Android share. Unauthenticated, so it must not read the request body
        # or ingest anything — only redirect into the uploader mode.
        if request.method == "POST":
            return RedirectResponse(url="/?mode=uploader&share=fallback", status_code=303)
        params: list[tuple[str, str]] = [("mode", "uploader")]
        shared_pairs = [
            (key, val)
            for key in ("url", "text", "title")
            if (val := request.query_params.get(key)) is not None and val != ""
        ]
        if shared_pairs:
            params.append(("shared", "1"))
            params.extend(shared_pairs)
        return RedirectResponse(url=f"/?{urlencode(params)}", status_code=303)

    @app.get("/", response_class=HTMLResponse)
    async def index():
        index_file = STATIC_DIR / "index.html"
        if not index_file.exists():
            return HTMLResponse(
                "<h1>Static UI missing</h1>",
                status_code=404,
                headers={"Cache-Control": "no-cache"},
            )
        return HTMLResponse(
            index_file.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-cache"},
        )

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    s = app.state.settings
    uvicorn.run(app, host=s.host, port=s.port)
