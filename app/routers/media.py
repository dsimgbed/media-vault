from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse

from app.allocator import sanitize_folder
from app.context import AppContext
from app.db import normalize_tags
from app.media_ops import (
    _iter_file_range,
    _parse_range_header,
    _telegram_media_actual_size,
    build_bulk_download_response,
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
async def stream_media(
    media_id: str,
    request: Request,
    download: bool = Query(default=False),
    ctx: AppContext = Depends(get_ctx),
):
    item = ctx.db.get_media(media_id)
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
            tg_client, tg_media = await ctx.tg_pool.prepare_stream(
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
            body = ctx.tg_pool.iter_telegram_multipart_range(
                parts, start, end, prefetched=prefetched_parts
            )
        else:
            body = ctx.tg_pool.iter_telegram_range(tg_client, tg_media, start, end)
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
        body = ctx.tg_pool.iter_telegram_multipart_range(
            parts, 0, file_size - 1, prefetched=prefetched_parts
        )
    else:
        body = ctx.tg_pool.iter_telegram_range(tg_client, tg_media, 0, file_size - 1)

    return StreamingResponse(
        body,
        status_code=200,
        media_type=mime_type,
        headers=headers,
    )


@router.delete("/api/media/{media_id}", dependencies=[Depends(require_auth)])
async def delete_media(media_id: str, ctx: AppContext = Depends(get_ctx)):
    item = await delete_media_item(ctx, media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media item not found")
    return {"deleted": True, "id": media_id}
