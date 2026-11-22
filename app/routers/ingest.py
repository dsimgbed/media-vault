from __future__ import annotations

import asyncio
import hashlib
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from app.allocator import sanitize_folder
from app.context import AppContext
from app.ingest import _accepts_kwarg, expand_ytdlp_playlist, is_direct_download, resolve_format_spec
from app.media_ops import _sanitize_archive_relpath
from app.routers.deps import get_ctx, require_auth
from app.schemas import CheckDuplicateRequest, UrlIngestRequest

router = APIRouter()


@router.post("/api/ingest/check-duplicate", dependencies=[Depends(require_auth)])
async def check_duplicate(
    payload: CheckDuplicateRequest,
    ctx: AppContext = Depends(get_ctx),
):
    existing: dict[str, Any] | None = None
    match_type: str | None = None
    if payload.sha256:
        existing = ctx.db.find_media_by_sha256(payload.sha256)
        if existing:
            match_type = "sha256"
    if not existing and payload.url:
        existing = ctx.db.find_media_by_source_url(payload.url)
        if existing:
            match_type = "url"

    response: dict[str, Any] = {
        "duplicate": existing is not None,
        "item": existing,
        "match_type": match_type,
    }

    if payload.urls:
        matches_by_url = ctx.db.find_media_by_source_urls(payload.urls)
        url_matches: dict[str, dict[str, Any]] = {}
        for u in payload.urls:
            m = matches_by_url.get(u.strip())
            url_matches[u] = {
                "duplicate": m is not None,
                "item": m,
            }
        response["url_matches"] = url_matches

    if payload.size_bytes is not None and not payload.sha256 and not payload.url:
        count = ctx.db.count_media_by_size(payload.size_bytes)
        response["size_match_count"] = count
        response["needs_hash_verification"] = count > 0

    return response


@router.post("/api/ingest/url", status_code=202, dependencies=[Depends(require_auth)])
async def ingest_url(
    payload: UrlIngestRequest,
    ctx: AppContext = Depends(get_ctx),
):
    cfg = ctx.settings
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
    effective_proxy = (
        await ctx.ingest.get_effective_proxy() if payload.expand_playlist else ""
    )
    for u in raw_urls:
        if payload.expand_playlist:
            is_ddl = (
                await is_direct_download(u, proxy=effective_proxy)
                if effective_proxy and _accepts_kwarg(is_direct_download, "proxy")
                else await is_direct_download(u)
            )
            if not is_ddl:
                try:
                    pl_info = await expand_ytdlp_playlist(
                        u,
                        cookies_file=cfg.ytdlp_cookies_file,
                        proxy=effective_proxy,
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
        job_id = ctx.db.create_job("web", "url", target_url, params=job_params)
        ctx.ingest.submit_job(
            job_id,
            ctx.ingest.process_url_job(
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


@router.post("/api/ingest/upload", status_code=202, dependencies=[Depends(require_auth)])
async def ingest_upload(
    file: UploadFile = File(...),
    title: str | None = Form(default=None),
    folder: str | None = Form(default=None),
    tags: str | None = Form(default=None),
    forward_to_telegram: bool = Form(default=True),
    force: bool = Form(default=False),
    ctx: AppContext = Depends(get_ctx),
):
    cfg = ctx.settings
    cleaned_raw = "".join(
        ch for ch in (file.filename or "upload.bin") if ord(ch) >= 32 and ord(ch) != 127
    ).strip()
    parts = [seg for seg in cleaned_raw.replace("\\", "/").split("/") if seg]
    raw_name = parts[-1] if parts else "upload.bin"
    if raw_name in (".", "..") or not raw_name:
        raise HTTPException(status_code=400, detail="Invalid upload filename")
    resolved_title = title or Path(raw_name).stem
    job_id = ctx.db.create_job(
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
    ctx.ingest.register_staging_dir(job_dir)
    staged_path = job_dir / raw_name
    written = 0
    hasher = hashlib.sha256()

    try:
        ctx.db.update_job(job_id, status="downloading", message=f"Receiving {raw_name}...")
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

        sha256_hex = hasher.hexdigest()
        resolved_staged = str(staged_path.resolve())
        ctx.db.update_job(
            job_id,
            status="queued",
            progress=50.0,
            message=f"Staged {raw_name}, queued for processing...",
            staged_path=resolved_staged,
            staged_size_bytes=written,
            params={
                "folder": folder,
                "tags": tags,
                "title": resolved_title,
                "forward_to_telegram": forward_to_telegram,
                "force": force,
                "source_origin": "web",
                "staged_path": resolved_staged,
                "precomputed_sha256": sha256_hex,
            },
        )
        ctx.ingest.submit_job(
            job_id,
            ctx.ingest.process_staged_upload_job(
                job_id,
                staged_path,
                source_origin="web",
                folder=folder,
                tags=tags,
                title=resolved_title,
                forward_to_telegram=forward_to_telegram,
                force=force,
                precomputed_sha256=sha256_hex,
            ),
        )
        return {"job_id": job_id, "status": "queued"}
    except asyncio.CancelledError:
        shutil.rmtree(job_dir, ignore_errors=True)
        ctx.ingest.unregister_staging_dir(job_dir)
        ctx.db.update_job(job_id, status="cancelled", message="Upload cancelled")
        raise
    except HTTPException as he:
        shutil.rmtree(job_dir, ignore_errors=True)
        ctx.ingest.unregister_staging_dir(job_dir)
        ctx.db.update_job(job_id, status="failed", message=str(he.detail))
        raise
    except Exception as exc:
        shutil.rmtree(job_dir, ignore_errors=True)
        ctx.ingest.unregister_staging_dir(job_dir)
        ctx.db.update_job(job_id, status="failed", message=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/ingest/upload-folder", status_code=202, dependencies=[Depends(require_auth)])
async def ingest_upload_folder(
    files: list[UploadFile] = File(...),
    relative_paths: list[str] = Form(default=[]),
    archive_name: str | None = Form(default=None),
    folder: str | None = Form(default=None),
    tags: str | None = Form(default=None),
    forward_to_telegram: bool = Form(default=True),
    force: bool = Form(default=False),
    ctx: AppContext = Depends(get_ctx),
):
    cfg = ctx.settings
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

    job_id = ctx.db.create_job(
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
    ctx.ingest.register_staging_dir(job_dir)
    zip_path = job_dir / zip_filename
    total_written = 0
    seen_arcnames: set[str] = set()

    try:
        ctx.db.update_job(
            job_id,
            status="downloading",
            progress=5.0,
            message=f"Packing {len(files)} file(s) into {zip_filename}...",
        )
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
            for idx, upload_file in enumerate(files):
                rel_raw = (
                    relative_paths[idx]
                    if idx < len(relative_paths)
                    else (upload_file.filename or f"file_{idx}")
                )
                arcname = _sanitize_archive_relpath(rel_raw, upload_file.filename or f"file_{idx}")
                if arcname in seen_arcnames:
                    stem = Path(arcname).stem
                    suffix = Path(arcname).suffix
                    parent = Path(arcname).parent
                    dedup_name = f"{stem}_{idx}{suffix}"
                    arcname = (
                        f"{parent.as_posix()}/{dedup_name}"
                        if parent.as_posix() != "."
                        else dedup_name
                    )
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

        zip_size = int(zip_path.stat().st_size)
        if zip_size > cfg.max_upload_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"Folder archive exceeds {cfg.max_upload_mb} MB limit.",
            )

        resolved_zip = str(zip_path.resolve())
        ctx.db.update_job(
            job_id,
            status="queued",
            progress=50.0,
            message=f"Staged {zip_filename}, queued for processing...",
            staged_path=resolved_zip,
            staged_size_bytes=zip_size,
            params={
                "folder": folder,
                "tags": tags,
                "title": folder_title,
                "forward_to_telegram": forward_to_telegram,
                "force": force,
                "source_origin": "web",
                "staged_path": resolved_zip,
            },
        )
        ctx.ingest.submit_job(
            job_id,
            ctx.ingest.process_staged_upload_job(
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
    except asyncio.CancelledError:
        shutil.rmtree(job_dir, ignore_errors=True)
        ctx.ingest.unregister_staging_dir(job_dir)
        ctx.db.update_job(job_id, status="cancelled", message="Upload cancelled")
        raise
    except HTTPException as he:
        shutil.rmtree(job_dir, ignore_errors=True)
        ctx.ingest.unregister_staging_dir(job_dir)
        ctx.db.update_job(job_id, status="failed", message=str(he.detail))
        raise
    except Exception as exc:
        shutil.rmtree(job_dir, ignore_errors=True)
        ctx.ingest.unregister_staging_dir(job_dir)
        ctx.db.update_job(job_id, status="failed", message=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))
