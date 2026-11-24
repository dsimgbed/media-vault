"""HTTP and WebSocket surface for Live Watch Party Rooms.

Host management endpoints use the regular web auth token; guest endpoints and
the media/thumbnail proxies are strictly room-scoped and use short-lived room
session tokens.  The application-wide web token is never accepted here as a
watch-party token.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, HTMLResponse, Response

from app.context import STATIC_DIR, AppContext
from app.media_ops import build_media_stream_response
from app.routers.deps import get_ctx, read_cached_html, require_auth
from app.schemas import (
    WatchPartyCreateRequest,
    WatchPartyHostSessionRequest,
    WatchPartyJoinRequest,
    WatchPartyKickRequest,
    WatchPartyUpdateRequest,
)
from app.watch_party import (
    MAX_WS_MESSAGE_BYTES,
    ROOM_CODE_RE,
    WS_CLOSE_ROOM_CLOSED,
    WS_CLOSE_UNAUTHORIZED,
    AuthError,
    RoomNotFoundError,
    WatchPartyError,
    WatchPartyService,
)

logger = logging.getLogger(__name__)

router = APIRouter()

_WATCH_PARTY_SECURITY_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "X-Robots-Tag": "noindex, nofollow",
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "img-src 'self' data: blob:; media-src 'self' blob:; "
        "connect-src 'self' ws: wss:; base-uri 'none'; "
        "form-action 'self'; frame-ancestors 'none'"
    ),
}

_MEDIA_PROXY_HEADERS = {
    "Accept-Ranges": "bytes",
    "Cache-Control": "private, no-store",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Robots-Tag": "noindex, nofollow",
}

_THUMB_HEADERS = {
    "Cache-Control": "private, max-age=300",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
}

_THUMB_MEDIA_TYPES = {
    ".webp": "image/webp",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raise_http(exc: WatchPartyError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail=exc.detail,
        headers=exc.headers or None,
    )


def _get_wp(ctx: AppContext) -> WatchPartyService:
    wp = getattr(ctx, "watch_party", None)
    if wp is None:
        raise HTTPException(status_code=503, detail="Watch party service unavailable")
    return wp


def _client_ip(request: Request) -> str:
    client = request.client
    return client.host if client is not None and client.host else "unknown"


def _extract_room_token(request: Request, wp_token: str | None) -> str:
    """Return the room session token from the bearer header or query string.

    The global ``WEB_AUTH_TOKEN`` is deliberately never consulted here.
    """
    auth_header = request.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        bearer = auth_header[7:].strip()
        if bearer:
            return bearer
    if wp_token:
        return wp_token
    raise HTTPException(status_code=401, detail="A watch party session token is required")


def _authorized_media_item(
    ctx: AppContext,
    room_code: str,
    media_id: str,
    token: str,
) -> dict[str, Any]:
    wp = _get_wp(ctx)
    try:
        wp.authorize_media_access(room_code, token, media_id)
    except WatchPartyError as exc:
        _raise_http(exc)
    item = ctx.db.get_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media item not found")
    return item


async def _reject_ws(websocket: WebSocket, message: str, close_code: int) -> None:
    """Send a uniform auth failure frame and close the socket."""
    try:
        await websocket.send_json(
            {"type": "error", "code": "unauthorized", "message": message}
        )
    except Exception:
        pass
    try:
        reason = "Room closed" if close_code == WS_CLOSE_ROOM_CLOSED else "Unauthorized"
        await websocket.close(code=close_code, reason=reason)
    except Exception:
        pass


def _ws_auth_failure(exc: Exception) -> tuple[str, int]:
    if isinstance(exc, RoomNotFoundError):
        message = exc.detail if isinstance(exc, WatchPartyError) and exc.detail else "Watch party room not found"
        return message, WS_CLOSE_ROOM_CLOSED
    if isinstance(exc, WatchPartyError):
        return exc.detail, WS_CLOSE_UNAUTHORIZED
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "Authentication timed out", WS_CLOSE_UNAUTHORIZED
    return "Authentication failed", WS_CLOSE_UNAUTHORIZED


# ---------------------------------------------------------------------------
# Standalone guest page
# ---------------------------------------------------------------------------


@router.get("/watch/{room_code}", response_class=HTMLResponse)
async def watch_party_page(room_code: str):
    if not ROOM_CODE_RE.fullmatch(room_code):
        return HTMLResponse(
            "<h1>Invalid watch party room code</h1>",
            status_code=400,
            headers=_WATCH_PARTY_SECURITY_HEADERS,
        )
    html = await read_cached_html(STATIC_DIR / "watch-party.html")
    if html is None:
        return HTMLResponse(
            "<h1>Watch party page missing</h1>",
            status_code=404,
            headers=_WATCH_PARTY_SECURITY_HEADERS,
        )
    return HTMLResponse(
        html,
        headers=_WATCH_PARTY_SECURITY_HEADERS,
    )


# ---------------------------------------------------------------------------
# Host management API (web auth required)
# ---------------------------------------------------------------------------


@router.post(
    "/api/watch-party/rooms",
    status_code=201,
    dependencies=[Depends(require_auth)],
)
async def create_watch_party_room(
    payload: WatchPartyCreateRequest,
    ctx: AppContext = Depends(get_ctx),
):
    wp = _get_wp(ctx)
    try:
        return wp.create_room(
            title=payload.title,
            media_ids=payload.media_ids,
            active_media_id=payload.active_media_id,
            pin=payload.pin,
            control_mode=payload.control_mode,
            wait_for_buffering=payload.wait_for_buffering,
            ttl_sec=payload.ttl_sec,
            max_participants=payload.max_participants,
            host_name=payload.host_name,
            initial_position_sec=payload.initial_position_sec,
            initial_is_playing=payload.initial_is_playing,
            initial_playback_rate=payload.initial_playback_rate,
        )
    except WatchPartyError as exc:
        _raise_http(exc)


@router.get("/api/watch-party/rooms", dependencies=[Depends(require_auth)])
async def list_watch_party_rooms(ctx: AppContext = Depends(get_ctx)):
    wp = _get_wp(ctx)
    try:
        return {"rooms": wp.list_rooms()}
    except WatchPartyError as exc:
        _raise_http(exc)


@router.get("/api/watch-party/rooms/{room_code}", dependencies=[Depends(require_auth)])
async def get_watch_party_room(
    room_code: str,
    ctx: AppContext = Depends(get_ctx),
):
    wp = _get_wp(ctx)
    try:
        return wp.get_room_host_view(room_code)
    except WatchPartyError as exc:
        _raise_http(exc)


@router.post(
    "/api/watch-party/rooms/{room_code}/host-session",
    dependencies=[Depends(require_auth)],
)
async def create_watch_party_host_session(
    room_code: str,
    payload: WatchPartyHostSessionRequest | None = None,
    ctx: AppContext = Depends(get_ctx),
):
    wp = _get_wp(ctx)
    display_name = payload.display_name if payload is not None else "Host"
    try:
        return wp.create_or_get_host_session(room_code, display_name=display_name)
    except WatchPartyError as exc:
        _raise_http(exc)


@router.patch("/api/watch-party/rooms/{room_code}", dependencies=[Depends(require_auth)])
async def update_watch_party_room(
    room_code: str,
    payload: WatchPartyUpdateRequest,
    ctx: AppContext = Depends(get_ctx),
):
    wp = _get_wp(ctx)
    # Only fields the caller actually sent are forwarded: an omitted ``title``
    # or ``active_media_id`` must never be interpreted as "clear this field".
    updates = payload.model_dump(exclude_unset=True)
    try:
        return await wp.update_room(room_code, **updates)
    except WatchPartyError as exc:
        _raise_http(exc)


@router.post(
    "/api/watch-party/rooms/{room_code}/kick",
    dependencies=[Depends(require_auth)],
)
async def kick_watch_party_participant(
    room_code: str,
    payload: WatchPartyKickRequest,
    ctx: AppContext = Depends(get_ctx),
):
    wp = _get_wp(ctx)
    try:
        return await wp.kick_participant(
            room_code,
            payload.participant_id,
            by_host_api=True,
        )
    except WatchPartyError as exc:
        _raise_http(exc)


@router.delete(
    "/api/watch-party/rooms/{room_code}",
    dependencies=[Depends(require_auth)],
)
async def close_watch_party_room(
    room_code: str,
    ctx: AppContext = Depends(get_ctx),
):
    wp = _get_wp(ctx)
    try:
        closed = await wp.close_room(room_code, reason="Room closed by host")
    except WatchPartyError as exc:
        _raise_http(exc)
    if not closed:
        raise HTTPException(status_code=404, detail="Watch party room not found")
    return {"closed": True, "room_code": room_code}


# ---------------------------------------------------------------------------
# Public / guest room endpoints (no web auth; room token only where required)
# ---------------------------------------------------------------------------


@router.get("/api/watch-party/{room_code}/info")
async def get_watch_party_public_info(
    room_code: str,
    ctx: AppContext = Depends(get_ctx),
):
    wp = _get_wp(ctx)
    try:
        return wp.get_public_info(room_code)
    except WatchPartyError as exc:
        _raise_http(exc)


@router.post("/api/watch-party/{room_code}/join")
async def join_watch_party_room(
    room_code: str,
    payload: WatchPartyJoinRequest,
    request: Request,
    ctx: AppContext = Depends(get_ctx),
):
    wp = _get_wp(ctx)
    try:
        return wp.join_room(
            room_code,
            display_name=payload.display_name,
            pin=payload.pin,
            client_ip=_client_ip(request),
        )
    except WatchPartyError as exc:
        _raise_http(exc)


# ---------------------------------------------------------------------------
# Scoped media proxy
# ---------------------------------------------------------------------------


@router.get("/api/watch-party/{room_code}/media/{media_id}/stream")
@router.head("/api/watch-party/{room_code}/media/{media_id}/stream", include_in_schema=False)
async def stream_watch_party_media(
    room_code: str,
    media_id: str,
    request: Request,
    wp_token: str | None = Query(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    token = _extract_room_token(request, wp_token)
    item = _authorized_media_item(ctx, room_code, media_id, token)
    return await build_media_stream_response(
        request,
        ctx,
        item,
        extra_headers=_MEDIA_PROXY_HEADERS,
    )


# ---------------------------------------------------------------------------
# Scoped thumbnail proxy
# ---------------------------------------------------------------------------


@router.get("/api/watch-party/{room_code}/media/{media_id}/thumb")
@router.head("/api/watch-party/{room_code}/media/{media_id}/thumb", include_in_schema=False)
async def get_watch_party_media_thumb(
    room_code: str,
    media_id: str,
    request: Request,
    wp_token: str | None = Query(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    token = _extract_room_token(request, wp_token)
    item = _authorized_media_item(ctx, room_code, media_id, token)

    thumbs_dir = Path(ctx.settings.thumbs_dir)
    thumbs_root = thumbs_dir.resolve()
    candidates: list[Path] = []
    thumb_filename = item.get("thumb_filename")
    if isinstance(thumb_filename, str) and thumb_filename.strip():
        clean_thumb = thumb_filename.strip()
        if (
            "/" not in clean_thumb
            and "\\" not in clean_thumb
            and Path(clean_thumb).name == clean_thumb
        ):
            candidates.append(thumbs_dir / clean_thumb)
    for suffix in (".webp", ".jpg", ".jpeg", ".png"):
        candidates.append(thumbs_dir / f"{media_id}{suffix}")

    thumb_path: Path | None = None
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except (OSError, RuntimeError):
            continue
        if resolved.is_relative_to(thumbs_root) and resolved.is_file():
            thumb_path = resolved
            break

    if thumb_path is None:
        raise HTTPException(status_code=404, detail="Thumbnail not found")

    media_type = _THUMB_MEDIA_TYPES.get(thumb_path.suffix.lower(), "image/webp")
    headers = dict(_THUMB_HEADERS)
    if request.method == "HEAD":
        headers["Content-Length"] = str(thumb_path.stat().st_size)
        return Response(
            status_code=200,
            content=b"",
            media_type=media_type,
            headers=headers,
        )
    return FileResponse(thumb_path, media_type=media_type, headers=headers)


# ---------------------------------------------------------------------------
# Realtime WebSocket transport
# ---------------------------------------------------------------------------


@router.websocket("/api/watch-party/{room_code}/ws")
async def watch_party_websocket(websocket: WebSocket, room_code: str):
    ctx: AppContext = websocket.app.state.ctx
    wp = getattr(ctx, "watch_party", None)
    if wp is None:
        raise HTTPException(status_code=503, detail="Watch party service unavailable")

    await websocket.accept()

    participant_id: str | None = None
    connected = False
    try:
        wp_token = websocket.query_params.get("wp_token")
        try:
            if wp_token:
                _, participant = await wp.connect_ws(room_code, wp_token, websocket)
            else:
                # Primary flow: the client authenticates with its first frame.
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
                if len(raw.encode("utf-8")) > MAX_WS_MESSAGE_BYTES:
                    raise AuthError("Authentication message is too large")
                try:
                    payload = json.loads(raw)
                except (TypeError, ValueError) as exc:
                    raise AuthError("Authentication message must be valid JSON") from exc
                if not isinstance(payload, dict) or payload.get("type") != "auth":
                    raise AuthError("Expected an auth message")
                token = payload.get("token")
                if not isinstance(token, str) or not token:
                    raise AuthError("A watch party session token is required")
                _, participant = await wp.connect_ws(room_code, token, websocket)
            participant_id = participant.participant_id
            connected = True
        except WebSocketDisconnect:
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            message, close_code = _ws_auth_failure(exc)
            await _reject_ws(websocket, message, close_code)
            return

        while True:
            try:
                raw = await websocket.receive_text()
            except (WebSocketDisconnect, RuntimeError):
                break
            except KeyError:
                await _reject_ws(websocket, "Text WebSocket frames are required", 1003)
                break
            await wp.handle_ws_message(room_code, participant_id, raw, websocket)
    finally:
        if connected and participant_id is not None:
            try:
                await wp.disconnect_ws(room_code, participant_id, websocket)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Watch party WebSocket cleanup failed: %s",
                    exc,
                    extra={"room_code": room_code},
                )
