from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from contextlib import asynccontextmanager
from dataclasses import asdict
from importlib.metadata import PackageNotFoundError, version as pkg_version
from pathlib import Path

from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.allocator import DriveAllocator
from app.config import Settings
from app.db import Database
from app.ingest import IngestService, list_ytdlp_formats, resolve_format_spec, update_ytdlp
from app.telegram_mtproto import TelegramPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


class UrlIngestRequest(BaseModel):
    url: str
    folder: str | None = None
    forward_to_telegram: bool = True
    format_spec: str | None = None


class FormatsRequest(BaseModel):
    url: str


class CookiesRequest(BaseModel):
    cookies_text: str


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


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or Settings()
    cfg.ensure_dirs()

    db = Database(cfg.db_path)
    allocator = DriveAllocator(cfg)
    ingest = IngestService(cfg, db, allocator)
    tg_pool = TelegramPool(cfg, db, allocator, ingest)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await tg_pool.start()
        try:
            yield
        finally:
            await tg_pool.stop()

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

    @app.get("/api/jobs", dependencies=[Depends(require_auth)])
    async def list_jobs(limit: int = Query(default=25, ge=1, le=100)):
        return {"jobs": db.list_jobs(limit=limit)}

    @app.get("/api/jobs/{job_id}", dependencies=[Depends(require_auth)])
    async def get_job(job_id: str):
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return job

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

    @app.post("/api/ingest/url", status_code=202, dependencies=[Depends(require_auth)])
    async def ingest_url(payload: UrlIngestRequest, background_tasks: BackgroundTasks):
        url = payload.url.strip()
        if not url.startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
        try:
            resolve_format_spec(payload.format_spec)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        job_id = db.create_job("web", "url", url)

        async def runner():
            try:
                await ingest.process_url_job(
                    job_id,
                    url,
                    source_origin="web",
                    folder=payload.folder,
                    forward_to_telegram=payload.forward_to_telegram,
                    format_spec=payload.format_spec,
                )
            except Exception as exc:
                logger.warning("URL job %s failed: %s", job_id, exc)

        background_tasks.add_task(runner)
        return {"job_id": job_id, "status": "queued"}

    @app.post("/api/ingest/upload", status_code=201, dependencies=[Depends(require_auth)])
    async def ingest_upload(
        file: UploadFile = File(...),
        title: str | None = Form(default=None),
        folder: str | None = Form(default=None),
        forward_to_telegram: bool = Form(default=True),
    ):
        raw_name = Path(file.filename or "upload.bin").name
        job_id = db.create_job("web", "upload", raw_name)
        job_dir = Path(tempfile.mkdtemp(dir=cfg.staging_dir))
        staged_path = job_dir / raw_name
        written = 0

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
                    await asyncio.to_thread(out.write, chunk)

            item = await ingest.process_staged_upload_job(
                job_id,
                staged_path,
                source_origin="web",
                folder=folder,
                title=title or Path(raw_name).stem,
                forward_to_telegram=forward_to_telegram,
            )
            return {"job_id": job_id, "item": item}
        except HTTPException as he:
            shutil.rmtree(job_dir, ignore_errors=True)
            db.update_job(job_id, status="failed", message=str(he.detail))
            raise
        except Exception as exc:
            shutil.rmtree(job_dir, ignore_errors=True)
            db.update_job(job_id, status="failed", message=str(exc))
            raise HTTPException(status_code=500, detail=str(exc))

    @app.get("/api/media", dependencies=[Depends(require_auth)])
    async def list_media(
        media_type: str = Query(default="all"),
        drive_name: str = Query(default="all"),
        folder: str = Query(default="all"),
        q: str = Query(default=""),
        sort: str = Query(default="newest"),
        limit: int = Query(default=60, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ):
        items, total = db.list_media(
            media_type=media_type,
            drive_name=drive_name,
            folder=folder,
            q=q.strip() or None,
            sort=sort,
            limit=limit,
            offset=offset,
        )
        return {"items": items, "total": total}

    @app.get("/api/media/{media_id}", dependencies=[Depends(require_auth)])
    async def get_media_item(media_id: str):
        item = db.get_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")
        return item

    @app.get("/api/media/{media_id}/thumb", dependencies=[Depends(require_auth)])
    async def get_media_thumb(media_id: str):
        item = db.get_media(media_id)
        if not item or not item.get("thumb_filename"):
            raise HTTPException(status_code=404, detail="Thumbnail not found")
        thumb_path = cfg.thumbs_dir / item["thumb_filename"]
        if not thumb_path.exists():
            raise HTTPException(status_code=404, detail="Thumbnail file missing")
        return FileResponse(thumb_path, media_type="image/jpeg")

    @app.get("/api/media/{media_id}/stream", dependencies=[Depends(require_auth)])
    async def stream_media(media_id: str, request: Request, download: bool = Query(default=False)):
        item = db.get_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")

        raw_path = (item.get("file_path") or "").strip()
        file_path = Path(raw_path) if raw_path else None
        use_local = bool(file_path and file_path.is_file())

        tg_client = None
        tg_media = None
        if use_local:
            assert file_path is not None
            file_size = file_path.stat().st_size
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
            file_size = int(item["size_bytes"])
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
            content_length = end - start + 1
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
            headers["Content-Length"] = str(content_length)
            body = (
                _iter_file_range(file_path, start, end)
                if use_local and file_path is not None
                else tg_pool.iter_telegram_range(tg_client, tg_media, start, end)
            )
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
        item = db.delete_media(media_id)
        if not item:
            raise HTTPException(status_code=404, detail="Media item not found")

        raw_path = (item.get("file_path") or "").strip()
        if raw_path:
            file_path = Path(raw_path)
            if file_path.is_file():
                try:
                    file_path.unlink()
                except OSError as exc:
                    logger.warning("Could not delete %s: %s", file_path, exc)

        if item.get("telegram_chat_id") and item.get("telegram_msg_id"):
            await tg_pool.delete_archive_message(
                item["telegram_chat_id"],
                int(item["telegram_msg_id"]),
                worker_name=item.get("telegram_worker_name"),
            )

        if item.get("thumb_filename"):
            thumb_path = cfg.thumbs_dir / item["thumb_filename"]
            if thumb_path.exists():
                thumb_path.unlink(missing_ok=True)

        return {"deleted": True, "id": media_id}

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        index_file = STATIC_DIR / "index.html"
        if not index_file.exists():
            return HTMLResponse("<h1>Static UI missing</h1>", status_code=404)
        return HTMLResponse(index_file.read_text(encoding="utf-8"))

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    s = app.state.settings
    uvicorn.run(app, host=s.host, port=s.port)
