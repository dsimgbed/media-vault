from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from app.context import AppContext
from app.media_ops import (
    _parse_range_header,
    build_range_headers,
    iter_source_range,
    normalize_range_header,
    prefetch_multipart_first,
    resolve_head_file_size,
    resolve_media_source,
)
from app.routers.deps import get_ctx
from app.webdav import (
    WebDavNode,
    build_finite_depth_error_xml,
    build_m3u_playlist,
    build_multistatus_xml,
    build_webdav_tree,
    etag_matches,
    parse_propfind_request,
    parse_webdav_segments,
    render_collection_html,
    resolve_webdav_node,
    verify_webdav_auth,
    webdav_etag,
    webdav_last_modified,
)

router = APIRouter()

_ALLOWED_METHODS = ("OPTIONS", "GET", "HEAD", "PROPFIND")
_REJECTED_METHODS = (
    "PUT",
    "POST",
    "DELETE",
    "PATCH",
    "MKCOL",
    "COPY",
    "MOVE",
    "LOCK",
    "UNLOCK",
    "PROPPATCH",
)
_ALL_METHODS = list(_ALLOWED_METHODS) + list(_REJECTED_METHODS)

_ALLOW_HEADER = ", ".join(_ALLOWED_METHODS)
_XML_CONTENT_TYPE = 'application/xml; charset="utf-8"'


def _text_response(status_code: int, message: str, **extra_headers: str) -> Response:
    headers = {"DAV": "1", **extra_headers}
    return Response(
        status_code=status_code,
        content=message,
        media_type="text/plain; charset=utf-8",
        headers=headers,
    )


def _webdav_prefix(request: Request) -> str:
    root_path = str(request.scope.get("root_path") or "").rstrip("/")
    if root_path.endswith("/webdav"):
        return root_path
    return f"{root_path}/webdav"


def _node_metadata(node: WebDavNode) -> tuple[str, str]:
    return webdav_etag(node), webdav_last_modified(node)


def _resolve(ctx: AppContext, segments: list[str]) -> WebDavNode | None:
    return resolve_webdav_node(ctx, segments, tree=build_webdav_tree(ctx))


_MAX_PROPFIND_BODY_BYTES = 65536


async def _read_bounded_body(request: Request, max_bytes: int = _MAX_PROPFIND_BODY_BYTES) -> bytes:
    """Read request body up to ``max_bytes`` without buffering unbounded payloads."""
    raw_cl = (request.headers.get("content-length") or "").strip()
    if raw_cl:
        try:
            if int(raw_cl) > max_bytes:
                raise ValueError("PROPFIND request body is too large")
        except ValueError as exc:
            if "too large" in str(exc):
                raise
    buf = bytearray()
    async for chunk in request.stream():
        if not chunk:
            continue
        if len(buf) + len(chunk) > max_bytes:
            raise ValueError("PROPFIND request body is too large")
        buf.extend(chunk)
    return bytes(buf)


def _if_range_matches(header_value: str | None, etag: str, last_modified: str) -> bool:
    """RFC 9110 §13.1.5: return True if If-Range is absent or matches ETag/Last-Modified."""
    if not header_value:
        return True
    candidate = header_value.strip()
    if not candidate:
        return True
    # Strong ETag comparison (weak ETags W/"..." never match If-Range per RFC 9110)
    if candidate.startswith('"'):
        return candidate == etag.strip()
    if candidate.startswith("W/"):
        return False
    # Exact HTTP-date match against Last-Modified
    return candidate == last_modified.strip()


async def _handle_propfind(
    request: Request, segments: list[str], ctx: AppContext
) -> Response:
    depth = (request.headers.get("depth") or "1").strip().lower()
    if depth == "infinity":
        return Response(
            status_code=403,
            content=build_finite_depth_error_xml(),
            headers={
                "Content-Type": _XML_CONTENT_TYPE,
                "DAV": "1",
                "Cache-Control": "no-cache",
            },
        )
    if depth not in ("0", "1"):
        return _text_response(400, "Invalid Depth header")

    try:
        raw_body = await _read_bounded_body(request)
        prop_mode, requested_props = parse_propfind_request(raw_body)
    except ValueError:
        return _text_response(400, "Invalid PROPFIND request body")

    tree = build_webdav_tree(ctx)
    node = resolve_webdav_node(ctx, segments, tree=tree)
    if node is None:
        return _text_response(404, "Not Found")

    resources: list[WebDavNode] = [node]
    if depth == "1" and node.is_collection and node.folder is not None:
        child_folders = sorted(
            node.folder.children.values(),
            key=lambda folder: (folder.display.casefold(), folder.display),
        )
        for folder in child_folders:
            resources.append(
                WebDavNode(
                    segments=folder.segments,
                    is_collection=True,
                    folder=folder,
                )
            )
        for entry in node.entries:
            resources.append(
                WebDavNode(
                    segments=(*node.segments, entry.name),
                    is_collection=False,
                    item=entry.item,
                    webdav_name=entry.name,
                )
            )

    body = build_multistatus_xml(
        resources,
        prop_mode,
        requested_props,
        _webdav_prefix(request),
    )
    return Response(
        status_code=207,
        content=body,
        headers={
            "Content-Type": _XML_CONTENT_TYPE,
            "DAV": "1",
            "Cache-Control": "no-cache",
        },
    )


def _head_file_metadata(
    ctx: AppContext, item: dict[str, Any]
) -> tuple[dict[str, Any], int] | None:
    """Check a backing source using SQLite/local stats only (no Telegram RPC)."""
    full_item = ctx.db.get_media(str(item.get("id") or ""))
    if full_item is None:
        return None
    file_size = resolve_head_file_size(full_item)
    if file_size is None:
        return None
    return full_item, file_size


def _range_not_satisfiable(file_size: int) -> Response:
    return Response(
        status_code=416,
        content=b"",
        headers={
            "Content-Range": f"bytes */{file_size}",
            "Accept-Ranges": "bytes",
            "DAV": "1",
            "Cache-Control": "no-cache",
        },
    )


async def _handle_head(
    request: Request, segments: list[str], ctx: AppContext
) -> Response:
    node = _resolve(ctx, segments)
    if node is None:
        return _text_response(404, "Not Found")
    etag, last_modified = _node_metadata(node)

    if node.is_collection:
        return Response(
            status_code=200,
            content=b"",
            headers={
                "Content-Type": "text/html; charset=utf-8",
                "ETag": etag,
                "Last-Modified": last_modified,
                "DAV": "1",
                "Content-Length": "0",
            },
        )

    metadata = _head_file_metadata(ctx, node.item or {})
    if metadata is None:
        return _text_response(404, "File missing on drive")
    full_item, file_size = metadata

    range_header = request.headers.get("range")
    if range_header and not _if_range_matches(
        request.headers.get("if-range"), etag, last_modified
    ):
        range_header = None
    if not range_header and etag_matches(request.headers.get("if-none-match"), etag):
        return Response(
            status_code=304,
            content=b"",
            headers={
                "ETag": etag,
                "Last-Modified": last_modified,
                "DAV": "1",
            },
        )

    bounds: tuple[int, int] | None = None
    if range_header:
        try:
            bounds = _parse_range_header(
                normalize_range_header(range_header), file_size
            )
        except (ValueError, TypeError):
            return _range_not_satisfiable(file_size)

    status_code, headers = build_range_headers(
        file_size,
        range_bounds=bounds,
        base_headers={
            "Content-Type": str(full_item.get("mime_type") or "application/octet-stream"),
            "ETag": etag,
            "Last-Modified": last_modified,
            "DAV": "1",
            "Cache-Control": "private, no-cache",
        },
    )
    return Response(
        status_code=status_code,
        content=b"",
        headers=headers,
    )


async def _handle_get(
    request: Request, segments: list[str], ctx: AppContext
) -> Response:
    node = _resolve(ctx, segments)
    if node is None:
        return _text_response(404, "Not Found")
    etag, last_modified = _node_metadata(node)

    if node.is_collection:
        body = render_collection_html(node, base_href_prefix=_webdav_prefix(request))
        return Response(
            status_code=200,
            content=body,
            headers={
                "Content-Type": "text/html; charset=utf-8",
                "ETag": etag,
                "Last-Modified": last_modified,
                "DAV": "1",
                "Cache-Control": "no-cache",
            },
        )

    full_item = ctx.db.get_media(str((node.item or {}).get("id") or ""))
    if full_item is None:
        return _text_response(404, "Not Found")

    range_header = request.headers.get("range")
    if range_header and not _if_range_matches(
        request.headers.get("if-range"), etag, last_modified
    ):
        range_header = None
    if not range_header and etag_matches(request.headers.get("if-none-match"), etag):
        return Response(
            status_code=304,
            content=b"",
            headers={
                "ETag": etag,
                "Last-Modified": last_modified,
                "DAV": "1",
            },
        )

    try:
        source = await resolve_media_source(ctx, full_item)
        file_size = int(source["size"])
    except HTTPException as exc:
        return _text_response(
            int(exc.status_code), str(exc.detail), **{"Cache-Control": "no-store"}
        )

    bounds: tuple[int, int] | None = None
    if range_header:
        try:
            bounds = _parse_range_header(
                normalize_range_header(range_header), file_size
            )
        except (ValueError, TypeError):
            return _range_not_satisfiable(file_size)

    start, end = bounds if bounds is not None else (0, max(0, file_size - 1))

    prefetched: dict[int, tuple[Any, Any]] | None = None
    if source["kind"] == "multipart" and file_size > 0:
        try:
            prefetched = await prefetch_multipart_first(ctx, source, start, end)
        except HTTPException as exc:
            return _text_response(
                int(exc.status_code), str(exc.detail), **{"Cache-Control": "no-store"}
            )

    if file_size > 0:
        body = iter_source_range(ctx, source, start, end, prefetched=prefetched)
    else:
        body = iter([b""])

    status_code, headers = build_range_headers(
        file_size,
        range_bounds=bounds,
        base_headers={
            "ETag": etag,
            "Last-Modified": last_modified,
            "DAV": "1",
            "Cache-Control": "private, no-cache",
        },
    )

    return StreamingResponse(
        body,
        status_code=status_code,
        media_type=str(full_item.get("mime_type") or "application/octet-stream"),
        headers=headers,
    )


async def _dispatch_webdav(
    request: Request, subpath: str, ctx: AppContext
) -> Response:
    auth_response = verify_webdav_auth(request, ctx)
    if auth_response is not None:
        return auth_response

    if request.method in _REJECTED_METHODS:
        return _text_response(
            405,
            "Media Vault WebDAV is read-only",
            **{"Allow": _ALLOW_HEADER},
        )

    if request.method == "OPTIONS":
        return Response(
            status_code=200,
            content=b"",
            headers={
                "DAV": "1",
                "Allow": _ALLOW_HEADER,
                "MS-Author-Via": "DAV",
                "Accept-Ranges": "bytes",
                "Cache-Control": "no-cache",
                "Content-Length": "0",
            },
        )

    try:
        segments = parse_webdav_segments(request, subpath)
    except ValueError:
        return _text_response(400, "Bad Request")

    if request.method == "PROPFIND":
        return await _handle_propfind(request, segments, ctx)
    if request.method == "HEAD":
        return await _handle_head(request, segments, ctx)
    if request.method == "GET":
        return await _handle_get(request, segments, ctx)

    return _text_response(405, "Method Not Allowed", **{"Allow": _ALLOW_HEADER})


@router.api_route("/webdav", methods=_ALL_METHODS, include_in_schema=False)
@router.api_route("/webdav/", methods=_ALL_METHODS, include_in_schema=False)
@router.api_route("/webdav/{subpath:path}", methods=_ALL_METHODS, include_in_schema=False)
async def webdav_endpoint(
    request: Request,
    subpath: str = "",
    ctx: AppContext = Depends(get_ctx),
) -> Response:
    return await _dispatch_webdav(request, subpath, ctx)


@router.api_route("/webdav.m3u", methods=["GET", "HEAD"], include_in_schema=False)
async def webdav_playlist(
    request: Request,
    ctx: AppContext = Depends(get_ctx),
) -> Response:
    auth_response = verify_webdav_auth(request, ctx)
    if auth_response is not None:
        return auth_response

    params = request.query_params
    text = build_m3u_playlist(
        ctx,
        request,
        folder=params.get("folder") or None,
        media_type=params.get("media_type") or params.get("type") or None,
        tag=params.get("tag") or None,
    )
    payload = text.encode("utf-8")
    return Response(
        status_code=200,
        content=payload if request.method == "GET" else b"",
        headers={
            "Content-Type": "audio/x-mpegurl; charset=utf-8",
            "Content-Disposition": 'inline; filename="vault.m3u"',
            "Content-Length": str(len(payload)),
            "DAV": "1",
            "Cache-Control": "no-cache",
        },
    )
