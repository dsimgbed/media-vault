from __future__ import annotations

from dataclasses import asdict
from importlib.metadata import PackageNotFoundError, version as pkg_version
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.allocator import sanitize_folder
from app.context import AppContext
from app.db import normalize_source_url
from app.gallery_ingest import get_gallery_dl_status
from app.ingest import expand_ytdlp_playlist, list_ytdlp_formats
from app.torrent_ingest import find_aria2c, get_aria2c_version
from app.routers.deps import get_ctx, require_auth
from app.schemas import CookiesRequest, FormatsRequest, PlaylistRequest

router = APIRouter()


@router.get("/api/health")
async def health(ctx: AppContext = Depends(get_ctx)):
    cfg = ctx.settings
    cookies_ok = bool(
        cfg.ytdlp_cookies_file.is_file() and cfg.ytdlp_cookies_file.stat().st_size > 0
    )
    try:
        ytdlp_ver = pkg_version("yt-dlp")
    except PackageNotFoundError:
        ytdlp_ver = "unknown"
    telemetry = ctx.tg_pool.get_telemetry()
    latest_backup = ctx.db.get_latest_backup_record()
    gallery_st = ctx.gallery.get_status() if ctx.gallery else get_gallery_dl_status(cfg)
    webdav_summary = cfg.get_webdav_status_summary()
    return {
        "ok": True,
        "storage_mode": cfg.storage_mode,
        "telegram_workers": len(ctx.tg_pool.clients),
        "telegram_workers_online": telemetry["online_count"],
        "telegram_workers_in_cooldown": telemetry["cooldown_count"],
        "telegram_archive_enabled": bool(
            cfg.telegram_archive_chat_id.strip() and ctx.tg_pool.clients
        ),
        "block_cache_utilization_pct": telemetry["block_cache"]["utilization_pct"],
        "last_backup_at": latest_backup["created_at"] if latest_backup else None,
        "default_folder": cfg.default_folder,
        "auth_required": bool(cfg.web_auth_token.strip()),
        "max_upload_mb": cfg.max_upload_mb,
        "upload_chunk_size_mb": cfg.upload_chunk_size_mb,
        "failed_upload_retention_sec": cfg.failed_upload_retention_sec,
        "telegram_part_size_mb": cfg.telegram_part_size_mb,
        "ingest_concurrency": cfg.ingest_concurrency,
        "ytdlp_cookies_configured": cookies_ok,
        "ytdlp_version": ytdlp_ver,
        "subscriptions_enabled": bool(cfg.subscriptions_enabled),
        "torrent_enabled": bool(cfg.torrent_enabled),
        "aria2c_installed": bool(ctx.torrent.get_status()["aria2c_installed"]) if ctx.torrent else (find_aria2c() is not None),
        "aria2c_version": ctx.torrent.get_status()["aria2c_version"] if ctx.torrent else get_aria2c_version(),
        "gallery_dl_enabled": bool(cfg.gallery_dl_enabled),
        "gallery_dl_installed": bool(gallery_st["installed"]),
        "gallery_dl_version": gallery_st["version"],
        "webdav_enabled": webdav_summary["enabled"],
        "webdav_auth_required": webdav_summary["auth_required"],
        "webdav_configured": webdav_summary["configured"],
    }


@router.get("/api/system/status", dependencies=[Depends(require_auth)])
async def system_status(ctx: AppContext = Depends(get_ctx)):
    cfg = ctx.settings
    backups = ctx.backup.list_backups(limit=100) if ctx.backup else []
    latest_backup = backups[0] if backups else ctx.db.get_latest_backup_record()
    return {
        "ok": True,
        "storage_mode": cfg.storage_mode,
        "telegram": ctx.tg_pool.get_telemetry(),
        "database": ctx.db.get_db_stats(),
        "storage": ctx.integrity.get_storage_footprint() if ctx.integrity else {},
        "backups": {
            "config": ctx.backup.get_config_summary() if ctx.backup else {},
            "count": len(backups),
            "latest": latest_backup,
        },
        "integrity": ctx.integrity.get_status() if ctx.integrity else {},
        "uploads": ctx.uploads.get_status_summary() if ctx.uploads else {},
        "subscriptions": ctx.subscriptions.get_health_summary() if ctx.subscriptions else {},
        "zip_cache": (
            ctx.zip_cache.stats()
            if hasattr(ctx.zip_cache, "stats")
            else {"l1_entries": len(ctx.zip_cache)}
        ),
        "leech": {
            "torrent": ctx.torrent.get_status() if ctx.torrent else {},
            "gallery_dl": ctx.gallery.get_status() if ctx.gallery else get_gallery_dl_status(cfg),
        },
        "ingest": {
            "concurrency": cfg.ingest_concurrency,
            "active_jobs": ctx.ingest.has_active_jobs(),
            "active_staging_dirs": len(ctx.ingest.get_active_staging_dirs()),
        },
        "webdav": cfg.get_webdav_status_summary(),
    }



@router.get("/api/drives", dependencies=[Depends(require_auth)])
async def list_drives(ctx: AppContext = Depends(get_ctx)):
    cfg = ctx.settings
    return {
        "storage_mode": cfg.storage_mode,
        "strategy": cfg.allocation_strategy,
        "min_free_buffer_mb": cfg.min_free_buffer_mb,
        "drives": [asdict(d) for d in ctx.allocator.get_drive_statuses()],
    }


@router.get("/api/folders", dependencies=[Depends(require_auth)])
async def list_folders(ctx: AppContext = Depends(get_ctx)):
    cfg = ctx.settings
    folders = ctx.db.list_folders()
    if not any(f.casefold() == cfg.default_folder.casefold() for f in folders):
        folders.insert(0, cfg.default_folder)
    return {"folders": folders, "default_folder": cfg.default_folder}


@router.get("/api/tags", dependencies=[Depends(require_auth)])
async def list_tags(
    q: str = Query(default=""),
    limit: int | None = Query(default=None, ge=1, le=5000),
    ctx: AppContext = Depends(get_ctx),
):
    return {"tags": ctx.db.list_tag_counts(q=q.strip() or None, limit=limit)}


@router.post("/api/ytdlp/update", dependencies=[Depends(require_auth)])
async def trigger_ytdlp_update():
    try:
        import app.main as main_mod

        return await main_mod.update_ytdlp()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/ytdlp/formats", dependencies=[Depends(require_auth)])
async def inspect_ytdlp_formats(
    payload: FormatsRequest,
    ctx: AppContext = Depends(get_ctx),
):
    url = payload.url.strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
    try:
        effective_proxy = await ctx.ingest.get_effective_proxy()
        return await list_ytdlp_formats(
            url,
            cookies_file=ctx.settings.ytdlp_cookies_file,
            proxy=effective_proxy,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/api/ytdlp/playlist", dependencies=[Depends(require_auth)])
async def inspect_ytdlp_playlist(
    payload: PlaylistRequest,
    ctx: AppContext = Depends(get_ctx),
):
    url = payload.url.strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
    try:
        effective_proxy = await ctx.ingest.get_effective_proxy()
        res = await expand_ytdlp_playlist(
            url,
            cookies_file=ctx.settings.ytdlp_cookies_file,
            proxy=effective_proxy,
            max_items=max(1, min(500, int(payload.max_items))),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    entries = res.get("entries") or []
    matches = ctx.db.find_media_by_source_urls(
        [str(e["url"]) for e in entries if isinstance(e, dict) and e.get("url")]
    )
    dup_count = 0
    enriched_entries: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_url = str(entry.get("url") or "")
        norm_url = normalize_source_url(entry_url)
        existing = matches.get(entry_url) or (matches.get(norm_url) if norm_url else None)
        already = existing is not None
        if already:
            dup_count += 1
        enriched = dict(entry)
        enriched["normalized_url"] = norm_url
        enriched["already_in_vault"] = already
        enriched["existing_media_id"] = existing["id"] if existing else None
        enriched["existing_folder"] = existing["folder"] if existing else None
        enriched_entries.append(enriched)

    pl_title = str(res.get("playlist_title") or "").strip()
    res["entries"] = enriched_entries
    res["duplicate_count"] = dup_count
    res["suggested_folder"] = (
        sanitize_folder(pl_title, ctx.settings.default_folder)
        if pl_title
        else ctx.settings.default_folder
    )
    return res


@router.post("/api/ytdlp/cookies", dependencies=[Depends(require_auth)])
async def save_ytdlp_cookies(
    payload: CookiesRequest,
    ctx: AppContext = Depends(get_ctx),
):
    raw = payload.cookies_text.replace("\r\n", "\n").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="Cookies text cannot be empty.")
    def _is_valid_cookie_row(line: str) -> bool:
        if not line:
            return False
        if line.startswith("#HttpOnly_"):
            row = line[len("#HttpOnly_"):]
        elif line.startswith("#"):
            return False
        else:
            row = line
        cols = row.split("\t")
        return len(cols) == 7 and bool(cols[0].strip()) and bool(cols[5].strip())

    has_cookie_line = any(_is_valid_cookie_row(line) for line in raw.splitlines())
    if not has_cookie_line:
        raise HTTPException(
            status_code=400,
            detail="Expected Netscape cookies.txt format (7-column tab-separated cookie rows).",
        )
    cookies_file = ctx.settings.ytdlp_cookies_file
    cookies_file.parent.mkdir(parents=True, exist_ok=True)
    cookies_file.write_text(raw + "\n", encoding="utf-8")
    return {"saved": True, "ytdlp_cookies_configured": True}


@router.delete("/api/ytdlp/cookies", dependencies=[Depends(require_auth)])
async def clear_ytdlp_cookies(ctx: AppContext = Depends(get_ctx)):
    cookies_file = ctx.settings.ytdlp_cookies_file
    if cookies_file.exists():
        cookies_file.unlink(missing_ok=True)
    return {"deleted": True, "ytdlp_cookies_configured": False}
