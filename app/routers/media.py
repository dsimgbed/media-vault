from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse

from app.allocator import sanitize_folder
from app.context import AppContext
from app.db import normalize_tags
from app.media_ops import (
    build_bulk_download_response,
    build_media_stream_response,
    delete_media_item,
    move_or_rename_media_item,
)
from app.routers.deps import get_ctx, require_auth
from app.schemas import (
    BulkMediaIdsRequest,
    BulkMoveRequest,
    BulkTagsRequest,
    UpdateMediaRequest,
    UpdateMediaTagsRequest,
)

router = APIRouter()


@router.get("/api/media", dependencies=[Depends(require_auth)])
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
    ctx: AppContext = Depends(get_ctx),
):
    effective_tag = tag
    if tags.strip():
        effective_tag = f"{tag},{tags}" if (tag and tag != "all") else tags
    items, total = ctx.db.list_media(
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


@router.post("/api/media/bulk/delete", dependencies=[Depends(require_auth)])
async def bulk_delete_media(
    payload: BulkMediaIdsRequest,
    ctx: AppContext = Depends(get_ctx),
):
    deleted_ids: list[str] = []
    for mid in dict.fromkeys(m.strip() for m in payload.media_ids if m and m.strip()):
        removed = await delete_media_item(ctx, mid)
        if removed:
            deleted_ids.append(mid)
    return {"deleted": len(deleted_ids), "ids": deleted_ids}


@router.post("/api/media/bulk/move", dependencies=[Depends(require_auth)])
async def bulk_move_media(
    payload: BulkMoveRequest,
    ctx: AppContext = Depends(get_ctx),
):
    safe_folder = sanitize_folder(payload.folder, ctx.settings.default_folder)
    updated_items: list[dict[str, Any]] = []
    for mid in dict.fromkeys(m.strip() for m in payload.media_ids if m and m.strip()):
        item = ctx.db.get_media(mid)
        if not item:
            continue
        updated = await move_or_rename_media_item(ctx, item, folder=safe_folder)
        updated_items.append(updated)
    return {"moved": len(updated_items), "folder": safe_folder, "items": updated_items}


@router.post("/api/media/bulk/tags", dependencies=[Depends(require_auth)])
async def bulk_tags_media(
    payload: BulkTagsRequest,
    ctx: AppContext = Depends(get_ctx),
):
    incoming = normalize_tags(payload.tags)
    remove_set = set(incoming)
    updated_items: list[dict[str, Any]] = []
    for mid in dict.fromkeys(m.strip() for m in payload.media_ids if m and m.strip()):
        item = ctx.db.get_media(mid)
        if not item:
            continue
        existing = normalize_tags(item.get("tags"))
        if payload.mode == "replace":
            next_tags = incoming
        elif payload.mode == "remove":
            next_tags = [t for t in existing if t not in remove_set]
        else:
            next_tags = normalize_tags([*existing, *incoming])
        updated = ctx.db.update_media_tags(mid, next_tags)
        if updated:
            updated_items.append(updated)
    return {"updated": len(updated_items), "items": updated_items}


@router.get("/api/media/bulk/download", dependencies=[Depends(require_auth)])
async def bulk_download_media_get(
    ids: str = Query(...),
    ctx: AppContext = Depends(get_ctx),
):
    return await build_bulk_download_response(ctx, ids.split(","))


@router.post("/api/media/bulk/download", dependencies=[Depends(require_auth)])
async def bulk_download_media_post(
    payload: BulkMediaIdsRequest,
    ctx: AppContext = Depends(get_ctx),
):
    return await build_bulk_download_response(ctx, payload.media_ids)


@router.get("/api/media/{media_id}", dependencies=[Depends(require_auth)])
async def get_media_item(media_id: str, ctx: AppContext = Depends(get_ctx)):
    item = ctx.db.get_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media item not found")
    return item


@router.patch("/api/media/{media_id}", dependencies=[Depends(require_auth)])
async def patch_media_item(
    media_id: str,
    payload: UpdateMediaRequest,
    ctx: AppContext = Depends(get_ctx),
):
    item = ctx.db.get_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media item not found")
    return await move_or_rename_media_item(
        ctx,
        item,
        title=payload.title,
        folder=payload.folder,
        tags=payload.tags,
    )


@router.patch("/api/media/{media_id}/tags", dependencies=[Depends(require_auth)])
async def update_media_tags(
    media_id: str,
    payload: UpdateMediaTagsRequest,
    ctx: AppContext = Depends(get_ctx),
):
    item = ctx.db.update_media_tags(media_id, payload.tags)
    if not item:
        raise HTTPException(status_code=404, detail="Media item not found")
    return item


@router.get("/api/media/{media_id}/thumb", dependencies=[Depends(require_auth)])
async def get_media_thumb(media_id: str, ctx: AppContext = Depends(get_ctx)):
    item = ctx.db.get_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Thumbnail not found")

    thumbs_dir = ctx.settings.thumbs_dir
    candidates: list[Path] = []
    if item.get("thumb_filename"):
        candidates.append(thumbs_dir / item["thumb_filename"])
    candidates.extend(
        [
            thumbs_dir / f"{media_id}.webp",
            thumbs_dir / f"{media_id}.jpg",
            thumbs_dir / f"{media_id}.jpeg",
            thumbs_dir / f"{media_id}.png",
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


@router.get("/api/media/{media_id}/preview", dependencies=[Depends(require_auth)])
async def get_media_preview(media_id: str, ctx: AppContext = Depends(get_ctx)):
    item = ctx.db.get_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Preview not found")

    thumbs_dir = ctx.settings.thumbs_dir
    candidates: list[Path] = []
    if item.get("preview_filename"):
        candidates.append(thumbs_dir / item["preview_filename"])
    candidates.append(thumbs_dir / f"{media_id}_preview.webp")

    preview_path = next((p for p in candidates if p.is_file()), None)
    if preview_path is None:
        raise HTTPException(status_code=404, detail="Preview not found")

    return FileResponse(
        preview_path,
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


@router.get("/api/media/{media_id}/stream", dependencies=[Depends(require_auth)])
@router.head(
    "/api/media/{media_id}/stream",
    dependencies=[Depends(require_auth)],
    include_in_schema=False,
)
async def stream_media(
    media_id: str,
    request: Request,
    download: bool = Query(default=False),
    ctx: AppContext = Depends(get_ctx),
):
    item = ctx.db.get_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media item not found")
    return await build_media_stream_response(request, ctx, item, download=download)


@router.delete("/api/media/{media_id}", dependencies=[Depends(require_auth)])
async def delete_media(media_id: str, ctx: AppContext = Depends(get_ctx)):
    item = await delete_media_item(ctx, media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media item not found")
    return {"deleted": True, "id": media_id}
