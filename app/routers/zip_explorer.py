from __future__ import annotations

import struct
import zipfile
import zlib

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.context import AppContext
from app.media_ops import (
    _parse_range_header,
    get_zip_entries as load_zip_entries,
    iter_source_range,
    prefetch_multipart_first,
    read_source_range,
    resolve_media_source,
)
from app.routers.deps import get_ctx, require_auth

router = APIRouter()


@router.get("/api/media/{media_id}/zip/entries", dependencies=[Depends(require_auth)])
async def get_zip_entries(media_id: str, ctx: AppContext = Depends(get_ctx)):
    item = ctx.db.get_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media item not found")
    entries = await load_zip_entries(ctx, item)
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


@router.get("/api/media/{media_id}/zip/stream", dependencies=[Depends(require_auth)])
async def stream_zip_entry(
    media_id: str,
    request: Request,
    path: str = Query(...),
    download: bool = Query(default=False),
    ctx: AppContext = Depends(get_ctx),
):
    item = ctx.db.get_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media item not found")

    source = await resolve_media_source(ctx, item)
    entries = await load_zip_entries(ctx, item, source=source)
    norm_target = path.lstrip("/")
    entry = next(
        (e for e in entries if e["path"] == path or e["path"].lstrip("/") == norm_target),
        None,
    )
    if entry is None:
        raise HTTPException(status_code=404, detail="Entry not found in ZIP archive")

    header_offset = int(entry["header_offset"])
    lfh = await read_source_range(ctx, source, header_offset, header_offset + 29)
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
            prefetched = await prefetch_multipart_first(ctx, source, arc_start, arc_end)
            headers["Content-Range"] = f"bytes {req_start}-{req_end}/{size_bytes}"
            headers["Content-Length"] = str(req_end - req_start + 1)
            return StreamingResponse(
                iter_source_range(ctx, source, arc_start, arc_end, prefetched=prefetched),
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
        prefetched = await prefetch_multipart_first(ctx, source, arc_start, arc_end)
        return StreamingResponse(
            iter_source_range(ctx, source, arc_start, arc_end, prefetched=prefetched),
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

        prefetched = await prefetch_multipart_first(ctx, source, data_start, data_end)

        async def _iter_deflated():
            decomp = zlib.decompressobj(-zlib.MAX_WBITS)
            raw_iter = iter_source_range(ctx, source, data_start, data_end, prefetched=prefetched)
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
                try:
                    async for raw_chunk in raw_iter:
                        out = decomp.decompress(raw_chunk)
                        sliced = _slice_out(out)
                        if sliced:
                            yield sliced
                        if range_header and pos > req_end:
                            break
                finally:
                    aclose = getattr(raw_iter, "aclose", None)
                    if callable(aclose):
                        await aclose()
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
