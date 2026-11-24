from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile

from app.context import AppContext
from app.routers.deps import get_ctx, require_auth
from app.schemas import GalleryIngestRequest, TorrentIngestRequest, TorrentInspectRequest
from app.torrent_ingest import (
    BencodeError,
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
from app.gallery_ingest import (
    GalleryUnavailableError,
    find_gallery_dl_cmd,
    get_gallery_dl_status_async,
    update_gallery_dl,
    validate_gallery_url,
)

router = APIRouter()


def _parse_bool(raw: Any, default: bool = False) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    s = str(raw).strip().lower()
    if s in ("", "none"):
        return default
    return s in ("1", "true", "yes", "on", "y")


def _parse_selected_indices(raw: Any) -> list[int] | None:
    if raw is None:
        return None
    if isinstance(raw, list):
        out: list[int] = []
        for x in raw:
            out.append(int(x))
        return out
    s = str(raw).strip()
    if not s:
        return None
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [int(x) for x in parsed]
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return [int(x.strip()) for x in s.split(",") if x.strip()]


@router.get("/api/leech/status", dependencies=[Depends(require_auth)])
async def leech_status(ctx: AppContext = Depends(get_ctx)):
    torrent_st = await ctx.torrent.get_status_async() if ctx.torrent else {}
    gallery_st = (
        await ctx.gallery.get_status_async()
        if ctx.gallery
        else await get_gallery_dl_status_async(ctx.settings)
    )
    return {
        "torrent": torrent_st,
        "gallery_dl": gallery_st,
    }


@router.post("/api/ingest/torrent/inspect", dependencies=[Depends(require_auth)])
async def inspect_torrent(request: Request, ctx: AppContext = Depends(get_ctx)):
    cfg = ctx.settings
    content_type = (request.headers.get("content-type") or "").lower()

    magnet: str | None = None
    file_bytes: bytes | None = None

    if "application/json" in content_type:
        try:
            req = TorrentInspectRequest.model_validate(await request.json())
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        magnet = req.magnet
    elif "multipart/form-data" in content_type:
        form = await request.form()
        f = form.get("file")
        if f is not None and hasattr(f, "read"):
            raw = await f.read(MAX_TORRENT_FILE_BYTES + 1)
            if len(raw) > MAX_TORRENT_FILE_BYTES:
                raise HTTPException(status_code=413, detail="Torrent file too large")
            if not raw:
                raise HTTPException(status_code=400, detail="Empty torrent file")
            file_bytes = raw
        else:
            m = form.get("magnet")
            if m is not None:
                magnet = str(m)
    else:
        # try json fallback
        try:
            req = TorrentInspectRequest.model_validate(await request.json())
            magnet = req.magnet
        except Exception:
            raise HTTPException(status_code=400, detail="Unsupported content type")

    if ctx.torrent is None or not cfg.torrent_enabled:
        raise HTTPException(status_code=503, detail="Torrent ingestion is disabled or unavailable")

    try:
        if file_bytes is not None:
            return await asyncio.to_thread(ctx.torrent.inspect_torrent_bytes, file_bytes)
        if magnet:
            clean = validate_magnet_uri(magnet)
            return await ctx.torrent.inspect_magnet(clean)
        raise HTTPException(status_code=400, detail="Provide either a .torrent file or a magnet URI")
    except TorrentUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except (BencodeError, ValueError, TorrentQuotaError, TorrentError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/api/ingest/torrent", status_code=202, dependencies=[Depends(require_auth)])
async def ingest_torrent(request: Request, ctx: AppContext = Depends(get_ctx)):
    cfg = ctx.settings
    content_type = (request.headers.get("content-type") or "").lower()

    inspect_id: str | None = None
    magnet: str | None = None
    selected_indices: list[int] | None = None
    folder: str | None = None
    use_torrent_subfolder: bool = True
    tags: Any = None
    forward_to_telegram: bool = True
    force: bool = False
    torrent_bytes: bytes | None = None

    if "application/json" in content_type:
        try:
            req = TorrentIngestRequest.model_validate(await request.json())
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        inspect_id = req.inspect_id
        magnet = req.magnet
        selected_indices = req.selected_indices
        folder = req.folder
        use_torrent_subfolder = bool(req.use_torrent_subfolder)
        tags = req.tags
        forward_to_telegram = bool(req.forward_to_telegram)
        force = bool(req.force)
    elif "multipart/form-data" in content_type:
        form = await request.form()
        f = form.get("file")
        if f is not None and hasattr(f, "read"):
            raw = await f.read(MAX_TORRENT_FILE_BYTES + 1)
            if len(raw) > MAX_TORRENT_FILE_BYTES:
                raise HTTPException(status_code=413, detail="Torrent file too large")
            if not raw:
                raise HTTPException(status_code=400, detail="Empty torrent file")
            torrent_bytes = raw
        m = form.get("magnet")
        if m is not None:
            magnet = str(m)
        inspect_id = form.get("inspect_id")
        if inspect_id is not None:
            inspect_id = str(inspect_id)
        try:
            selected_indices = _parse_selected_indices(form.get("selected_indices"))
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="selected_indices must be a list of positive integers")
        folder = form.get("folder")
        if folder is not None:
            folder = str(folder)
        use_torrent_subfolder = _parse_bool(form.get("use_torrent_subfolder"), True)
        tags = form.get("tags")
        forward_to_telegram = _parse_bool(form.get("forward_to_telegram"), True)
        force = _parse_bool(form.get("force"), False)
    else:
        raise HTTPException(status_code=400, detail="Unsupported content type")

    if ctx.torrent is None or not cfg.torrent_enabled or find_aria2c() is None:
        raise HTTPException(
            status_code=503,
            detail="Torrent leeching unavailable: aria2c is not installed or torrent support is disabled.",
        )

    # Resolve source
    parsed = None
    req_magnet = magnet
    if inspect_id:
        entry = ctx.torrent.get_cached_inspect(inspect_id)
        if entry is None:
            raise HTTPException(status_code=404, detail="Torrent inspect session expired or not found; please re-inspect.")
        torrent_bytes = entry["torrent_bytes"]
        parsed = entry["parsed"]
        magnet = entry.get("magnet") or req_magnet
    elif torrent_bytes is not None:
        try:
            parsed = await asyncio.to_thread(parse_torrent_bytes, torrent_bytes)
        except (BencodeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    elif magnet:
        try:
            magnet = validate_magnet_uri(magnet)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    else:
        raise HTTPException(status_code=400, detail="Provide inspect_id, .torrent file, or magnet URI")

    # Validate selected_indices & disk quota before job
    effective_indices = None
    name = None
    infohash = None
    selected_count = 0

    if parsed is not None:
        if selected_indices is None:
            defaults = [f["index"] for f in parsed["files"] if f.get("selected_default")]
            if not defaults:
                defaults = [f["index"] for f in parsed["files"]]
            effective_indices = defaults
        else:
            if not selected_indices:
                raise HTTPException(status_code=400, detail="selected_indices must be non-empty")
            valid = {f["index"] for f in parsed["files"]}
            if any(i not in valid for i in selected_indices):
                raise HTTPException(status_code=400, detail="selected_indices contains invalid index")
            effective_indices = list(dict.fromkeys(selected_indices))
        selected_bytes = sum(f["size_bytes"] for f in parsed["files"] if f["index"] in set(effective_indices))
        try:
            await asyncio.to_thread(
                assert_staging_disk_space,
                cfg.staging_dir,
                selected_bytes,
                cfg.torrent_max_total_bytes,
                cfg.torrent_min_free_buffer_bytes,
            )
        except TorrentQuotaError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        name = parsed["name"]
        infohash = parsed["infohash"]
        selected_count = len(effective_indices)
    else:
        try:
            await asyncio.to_thread(
                assert_staging_disk_space,
                cfg.staging_dir,
                0,
                cfg.torrent_max_total_bytes,
                cfg.torrent_min_free_buffer_bytes,
            )
        except TorrentQuotaError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        infohash = extract_magnet_infohash(magnet) if magnet else None
        name = infohash or "magnet"
        effective_indices = None
        selected_count = 0

    source_ref = magnet or (f"torrent:{infohash}:{name}" if infohash else name)
    job_params = {
        "magnet": magnet,
        "infohash": infohash,
        "name": name,
        "selected_indices": effective_indices,
        "folder": folder,
        "use_torrent_subfolder": use_torrent_subfolder,
        "tags": tags,
        "forward_to_telegram": forward_to_telegram,
        "force": force,
        "source_origin": "web",
    }
    job_id = ctx.db.create_job("web", "torrent", source_ref, params=job_params)
    ctx.ingest.submit_job(
        job_id,
        ctx.torrent.process_torrent_job(
            job_id,
            torrent_bytes=torrent_bytes,
            magnet=magnet,
            selected_indices=effective_indices,
            source_origin="web",
            folder=folder,
            use_torrent_subfolder=use_torrent_subfolder,
            tags=tags,
            forward_to_telegram=forward_to_telegram,
            force=force,
        ),
    )
    return {
        "job_id": job_id,
        "status": "queued",
        "name": name,
        "infohash": infohash,
        "selected_count": selected_count,
    }


@router.post("/api/ingest/gallery", status_code=202, dependencies=[Depends(require_auth)])
async def ingest_gallery(payload: GalleryIngestRequest, ctx: AppContext = Depends(get_ctx)):
    cfg = ctx.settings
    if ctx.gallery is None or not cfg.gallery_dl_enabled or find_gallery_dl_cmd() is None:
        if not cfg.gallery_dl_enabled:
            raise HTTPException(status_code=503, detail="gallery-dl is disabled. Set GALLERY_DL_ENABLED=true to enable it.")
        raise HTTPException(status_code=503, detail="gallery-dl is not installed. Install it with: pip install gallery-dl")

    try:
        clean_url = validate_gallery_url(payload.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    bounded_max = max(1, min(cfg.gallery_dl_max_items_cap, int(payload.max_items or cfg.gallery_dl_default_max_items)))

    job_params = {
        "url": clean_url,
        "folder": payload.folder,
        "tags": payload.tags,
        "title": payload.title,
        "archive_mode": payload.archive_mode,
        "max_items": bounded_max,
        "forward_to_telegram": payload.forward_to_telegram,
        "force": payload.force,
        "source_origin": "web",
    }
    job_id = ctx.db.create_job("web", "gallery", clean_url, params=job_params)
    ctx.ingest.submit_job(
        job_id,
        ctx.gallery.ingest_gallery_job(
            job_id,
            clean_url,
            source_origin="web",
            folder=payload.folder,
            tags=payload.tags,
            title=payload.title,
            archive_mode=payload.archive_mode,
            max_items=bounded_max,
            forward_to_telegram=payload.forward_to_telegram,
            force=payload.force,
        ),
    )
    return {
        "job_id": job_id,
        "status": "queued",
        "archive_mode": payload.archive_mode,
        "max_items": bounded_max,
    }


@router.post("/api/gallery-dl/update", dependencies=[Depends(require_auth)])
async def update_gallery():
    try:
        return await update_gallery_dl()
    except GalleryUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
