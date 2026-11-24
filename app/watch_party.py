"""Live Watch Party Rooms (multiplayer synced cinema).

This module owns the in-memory watch-party registry: room lifecycle, PIN and
token authentication, authoritative playback state, buffering coordination,
WebSocket fan-out, chat/reactions and host moderation.

Security invariants:

* Room codes are generated with :func:`secrets.token_urlsafe`.
* Optional PINs are stored only as PBKDF2-HMAC-SHA256 digests with a per-room
  random salt.  The plaintext PIN is never retained or returned.
* Host/guest session tokens are returned exactly once and stored only as
  SHA-256 digests.  They are compared in constant time.
* The application-wide ``WEB_AUTH_TOKEN`` is never accepted as a watch-party
  token and is never exposed by this module.
* Media metadata is sanitized before leaving the service; filesystem paths,
  drive names, Telegram identifiers and source URLs are never exposed.

All methods are safe to call from the event loop.  State mutations are guarded
by a re-entrant lock so synchronous API handlers and WebSocket handlers can
share the registry safely.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
import logging
import math
import re
import secrets
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Literal

from app.config import Settings
from app.db import Database

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Frozen constants
# ---------------------------------------------------------------------------

ROOM_CODE_RE = re.compile(r"^[A-Za-z0-9_-]{4,64}$")
ALLOWED_REACTIONS = ("👍", "❤️", "😂", "😮", "🔥", "👏", "🎬", "🍿", "😢", "🎉")
MAX_WS_MESSAGE_BYTES = 8192
MAX_QUEUE_ITEMS = 200
MAX_CHAT_LENGTH = 500
MIN_PLAYBACK_RATE = 0.25
MAX_PLAYBACK_RATE = 4.0

WS_CLOSE_UNAUTHORIZED = 4001
WS_CLOSE_KICKED = 4003
WS_CLOSE_ROOM_CLOSED = 4004
WS_CLOSE_REPLACED = 4005

# Internal bounds.  These keep memory strictly bounded even under abuse.
_MAX_HOST_SESSIONS = 5
_MAX_PIN_ATTEMPTS_KEYS = 4096
_MAX_JOIN_KEYS = 4096
_MAX_KICKED_TOMBSTONES = 100
_JOIN_RATE_COUNT = 20
_JOIN_RATE_WINDOW_SEC = 60.0
_WS_FRAME_RATE_COUNT = 30
_WS_FRAME_RATE_WINDOW_SEC = 5.0
_REACTION_RATE_COUNT = 10
_REACTION_RATE_WINDOW_SEC = 5.0
_WS_SEND_TIMEOUT_SEC = 2.0
_WS_CLOSE_TIMEOUT_SEC = 2.0
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class WatchPartyError(Exception):
    """Base class for watch-party errors with transport-friendly metadata."""

    status_code: int = 500
    detail: str = "Watch party error"
    headers: dict[str, str] = {}

    def __init__(
        self,
        detail: str | None = None,
        *,
        status_code: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        resolved = self.__class__.detail if detail is None else str(detail)
        super().__init__(resolved)
        self.detail = resolved
        if status_code is not None:
            self.status_code = status_code
        self.headers = dict(headers) if headers else {}


class WatchPartyDisabledError(WatchPartyError):
    status_code = 503
    detail = "Watch party rooms are disabled"

    def __init__(self, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(detail, status_code=503, **kwargs)


class RoomNotFoundError(WatchPartyError):
    status_code = 404
    detail = "Watch party room not found"

    def __init__(self, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(detail, status_code=404, **kwargs)


class RoomCapacityError(WatchPartyError):
    status_code = 409
    detail = "Watch party room capacity reached"

    def __init__(self, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(detail, status_code=409, **kwargs)


class PinRequiredError(WatchPartyError):
    status_code = 401
    detail = "A PIN is required to join this watch party"

    def __init__(self, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(detail, status_code=401, **kwargs)


class InvalidPinError(WatchPartyError):
    status_code = 403
    detail = "Incorrect PIN"

    def __init__(self, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(detail, status_code=403, **kwargs)


class RateLimitError(WatchPartyError):
    status_code = 429
    detail = "Too many requests"

    def __init__(
        self,
        detail: str | None = None,
        retry_after_sec: float = 1.0,
        **kwargs: Any,
    ) -> None:
        try:
            retry = float(retry_after_sec)
        except (TypeError, ValueError):
            retry = 1.0
        if not math.isfinite(retry):
            retry = 1.0
        retry_int = max(1, int(math.ceil(retry)))
        headers = {"Retry-After": str(retry_int)}
        super().__init__(detail, status_code=429, headers=headers, **kwargs)
        self.retry_after_sec = float(retry_int)


class AuthError(WatchPartyError):
    status_code = 401
    detail = "Watch party authentication required"

    def __init__(self, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(detail, status_code=401, **kwargs)


class ForbiddenError(WatchPartyError):
    status_code = 403
    detail = "You do not have permission to do that"

    def __init__(self, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(detail, status_code=403, **kwargs)


class BadRequestError(WatchPartyError):
    status_code = 400
    detail = "Invalid watch party request"

    def __init__(self, detail: str | None = None, **kwargs: Any) -> None:
        super().__init__(detail, status_code=400, **kwargs)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _setting(settings: Any, name: str, default: Any) -> Any:
    value = getattr(settings, name, default)
    return default if value is None else value


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off", ""}:
            return False
    if value is None:
        return default
    return bool(value)


def _as_int(value: Any, default: int, *, minimum: int | None = None) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = default
    if minimum is not None:
        result = max(minimum, result)
    return result


def _as_float(value: Any, default: float, *, minimum: float | None = None) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        result = default
    if not math.isfinite(result):
        result = default
    if minimum is not None:
        result = max(minimum, result)
    return result


def _clean_control_chars(value: Any) -> str:
    return _CONTROL_CHARS_RE.sub("", str(value or "")).strip()


def _clean_text(
    value: Any,
    *,
    field_name: str,
    min_length: int = 1,
    max_length: int,
) -> str:
    text = _clean_control_chars(value)
    if len(text) < min_length:
        raise BadRequestError(f"{field_name} must be between {min_length} and {max_length} characters")
    if len(text) > max_length:
        raise BadRequestError(f"{field_name} must be between {min_length} and {max_length} characters")
    return text


def _coerce_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        return result if math.isfinite(result) else None
    if isinstance(value, str):
        try:
            result = float(value.strip())
        except ValueError:
            return None
        return result if math.isfinite(result) else None
    return None


def _token_digest(token: str) -> bytes:
    return hashlib.sha256(token.encode("utf-8")).digest()


def _hash_pin(pin: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", pin.encode("utf-8"), salt, 100_000)


def _valid_room_code(room_code: Any) -> bool:
    return isinstance(room_code, str) and bool(ROOM_CODE_RE.fullmatch(room_code))


def _serialize_media_item(item: dict[str, Any] | None) -> dict[str, Any]:
    """Return the sanitized, allow-listed view of a media row.

    Only metadata required by the player and queue UI is exposed.  Paths,
    drive names, Telegram identifiers, source URLs, tags and hashes never
    leave this function.
    """

    raw: dict[str, Any] = item if isinstance(item, dict) else {}

    title = _clean_control_chars(raw.get("title"))
    if not title:
        # Only permitted use of the bare filename: as a display fallback.
        title = _clean_control_chars(raw.get("filename"))
    if not title:
        title = "Untitled"

    duration: float | None = None
    if raw.get("duration_sec") is not None:
        parsed = _coerce_number(raw.get("duration_sec"))
        if parsed is not None and parsed >= 0:
            duration = parsed

    width: int | None = None
    if raw.get("width") is not None:
        parsed_width = _as_int(raw.get("width"), 0)
        if parsed_width > 0:
            width = parsed_width

    height: int | None = None
    if raw.get("height") is not None:
        parsed_height = _as_int(raw.get("height"), 0)
        if parsed_height > 0:
            height = parsed_height

    media_id = raw.get("id")
    media_type = raw.get("media_type")
    mime_type = raw.get("mime_type")

    return {
        "id": str(media_id) if media_id is not None else "",
        "title": title,
        "media_type": str(media_type) if media_type else "file",
        "mime_type": str(mime_type) if mime_type else "application/octet-stream",
        "duration_sec": duration,
        "width": width,
        "height": height,
        "has_thumb": bool(raw.get("thumb_filename")),
    }


# ---------------------------------------------------------------------------
# State containers
# ---------------------------------------------------------------------------


@dataclass
class Participant:
    participant_id: str
    display_name: str
    role: Literal["host", "guest"] = "guest"
    token_digest: bytes = b""
    connected: bool = False
    ever_connected: bool = False
    kicked: bool = False
    buffering: bool = False
    websocket: Any = None
    joined_at: float = field(default_factory=time.time)
    last_seen_mono: float = field(default_factory=time.monotonic)
    chat_timestamps: deque[float] = field(default_factory=deque)
    reaction_timestamps: deque[float] = field(default_factory=deque)
    frame_timestamps: deque[float] = field(default_factory=deque)

    @property
    def is_host(self) -> bool:
        return self.role == "host"

    def public_dict(self) -> dict[str, Any]:
        # ``buffering``/``is_buffering`` are deliberately duplicated: older
        # clients read one, the current web client reads the other.
        return {
            "participant_id": self.participant_id,
            "display_name": self.display_name,
            "role": self.role,
            "connected": self.connected,
            "buffering": self.buffering,
            "is_buffering": self.buffering,
            "joined_at": self.joined_at,
            "kicked": self.kicked,
        }


@dataclass
class WatchPartyRoom:
    room_code: str
    title: str = "Watch Party"
    host_name: str = "Host"
    control_mode: Literal["host_only", "everyone"] = "host_only"
    wait_for_buffering: bool = False
    ttl_sec: int = 21600
    max_participants: int = 20
    created_at: float = field(default_factory=time.time)
    created_mono: float = field(default_factory=time.monotonic)
    expires_at: float = 0.0
    expires_mono: float = 0.0
    last_activity_mono: float = field(default_factory=time.monotonic)
    closed: bool = False
    closed_reason: str = ""

    pin_salt: bytes | None = None
    pin_hash: bytes | None = None

    participants: dict[str, Participant] = field(default_factory=dict)
    queue: list[str] = field(default_factory=list)
    active_media_id: str | None = None
    active_duration_sec: float | None = None

    is_playing: bool = False
    playback_rate: float = 1.0
    position_sec: float = 0.0
    state_updated_mono: float = field(default_factory=time.monotonic)
    state_updated_at: float = field(default_factory=time.time)
    state_seq: int = 0
    paused_for_buffering: bool = False

    chat: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=100))
    next_chat_id: int = 1

    @property
    def requires_pin(self) -> bool:
        return self.pin_hash is not None

    @property
    def duration_sec(self) -> float | None:
        """Duration of the active item, mirrored for convenience."""

        return self.active_duration_sec

    @duration_sec.setter
    def duration_sec(self, value: float | None) -> None:
        self.active_duration_sec = value

    @property
    def allow_guest_controls(self) -> bool:
        return self.control_mode == "everyone"

    @property
    def participant_count(self) -> int:
        return sum(
            1
            for participant in self.participants.values()
            if participant.connected and not participant.kicked
        )

    @property
    def active(self) -> bool:
        if self.closed:
            return False
        now_wall = time.time()
        now_mono = time.monotonic()
        if self.expires_at and now_wall >= self.expires_at:
            return False
        if self.expires_mono and now_mono >= self.expires_mono:
            return False
        return True

    @property
    def chat_history(self) -> list[dict[str, Any]]:
        return list(self.chat)

    def connected_participants(self) -> list[Participant]:
        return [
            participant
            for participant in self.participants.values()
            if participant.connected and participant.websocket is not None and not participant.kicked
        ]

    def allowed_media_ids(self) -> set[str]:
        allowed: set[str] = set(self.queue)
        if self.active_media_id:
            allowed.add(self.active_media_id)
        return allowed

    def effective_position(self, now_mono: float | None = None) -> float:
        now = time.monotonic() if now_mono is None else float(now_mono)
        position = float(self.position_sec)
        if self.is_playing:
            elapsed = max(0.0, now - float(self.state_updated_mono))
            position += elapsed * float(self.playback_rate)
            position = max(0.0, position)
            if self.active_duration_sec is not None and self.active_duration_sec > 0:
                position = min(float(self.active_duration_sec), position)
            return position
        return max(0.0, position)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class WatchPartyService:
    """In-memory registry and realtime coordinator for watch parties."""

    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db
        # Public registry name for observability and straightforward testing.
        self.rooms: dict[str, WatchPartyRoom] = {}
        # Private alias retained for callers that prefer conventional naming.
        self._rooms = self.rooms
        self._lock = threading.RLock()
        self._issued_room_codes: set[str] = set()
        self._pin_attempts: dict[tuple[str, str], deque[float]] = {}
        self._join_attempts: dict[str, deque[float]] = {}
        self._reaper_task: asyncio.Task[None] | None = None

    # -- settings ---------------------------------------------------------

    @property
    def enabled(self) -> bool:
        return _as_bool(_setting(self.settings, "watch_party_enabled", True), True)

    def _max_rooms(self) -> int:
        return _as_int(
            _setting(self.settings, "watch_party_max_rooms", 20),
            20,
            minimum=0,
        )

    def _default_max_participants(self) -> int:
        return _as_int(
            _setting(self.settings, "watch_party_max_participants", 20),
            20,
            minimum=1,
        )

    def _default_ttl_sec(self) -> int:
        return _as_int(
            _setting(self.settings, "watch_party_default_ttl_sec", 21600),
            21600,
            minimum=0,
        )

    def _idle_timeout_sec(self) -> float:
        return _as_float(
            _setting(self.settings, "watch_party_idle_timeout_sec", 3600),
            3600.0,
            minimum=0.0,
        )

    def _reaper_interval_sec(self) -> float:
        return _as_float(
            _setting(self.settings, "watch_party_reaper_interval_sec", 60),
            60.0,
            minimum=0.01,
        )

    def _pin_max_attempts(self) -> int:
        return _as_int(
            _setting(self.settings, "watch_party_pin_max_attempts", 5),
            5,
            minimum=1,
        )

    def _pin_window_sec(self) -> float:
        return _as_float(
            _setting(self.settings, "watch_party_pin_window_sec", 300),
            300.0,
            minimum=0.01,
        )

    def _chat_history_limit(self) -> int:
        return _as_int(
            _setting(self.settings, "watch_party_chat_history_limit", 100),
            100,
            minimum=1,
        )

    def _chat_rate_limit_count(self) -> int:
        return _as_int(
            _setting(self.settings, "watch_party_chat_rate_limit_count", 5),
            5,
            minimum=1,
        )

    def _chat_rate_limit_sec(self) -> float:
        return _as_float(
            _setting(self.settings, "watch_party_chat_rate_limit_sec", 5.0),
            5.0,
            minimum=0.01,
        )

    def _join_url(self, room_code: str) -> str:
        path = f"/watch/{room_code}"
        base = str(_setting(self.settings, "public_base_url", "") or "").strip().rstrip("/")
        return f"{base}{path}" if base else path

    # -- room lookup / pruning -------------------------------------------

    def _now(self) -> tuple[float, float]:
        return time.time(), time.monotonic()

    def _room_is_expired(self, room: WatchPartyRoom, now_wall: float, now_mono: float) -> tuple[bool, str]:
        if room.closed:
            return True, room.closed_reason or "Room closed"
        if room.expires_at and now_wall >= room.expires_at:
            return True, "Room expired"
        if room.expires_mono and now_mono >= room.expires_mono:
            return True, "Room expired"
        idle_timeout = self._idle_timeout_sec()
        if idle_timeout > 0 and now_mono - room.last_activity_mono >= idle_timeout:
            return True, "Room idle timeout"
        return False, ""

    def _expired_room_codes(self) -> list[tuple[str, str]]:
        now_wall, now_mono = self._now()
        expired: list[tuple[str, str]] = []
        for code, room in list(self.rooms.items()):
            is_expired, reason = self._room_is_expired(room, now_wall, now_mono)
            if is_expired:
                expired.append((code, reason))
        return expired

    def _forget_room_locked(self, room: WatchPartyRoom, reason: str) -> None:
        room.closed = True
        room.closed_reason = reason
        self.rooms.pop(room.room_code, None)
        for participant in room.participants.values():
            participant.token_digest = b""
            participant.connected = False
            participant.buffering = False
            participant.websocket = None

    def _prune_expired_rooms(self) -> list[str]:
        """Remove expired/closed rooms and revoke their tokens.

        Connected sockets (if any) are notified on a best-effort basis; callers
        that can await should prefer :meth:`close_room` or the reaper.
        """

        removed: list[str] = []
        with self._lock:
            now_wall, now_mono = self._now()
            for code, room in list(self.rooms.items()):
                is_expired, reason = self._room_is_expired(room, now_wall, now_mono)
                if not is_expired:
                    continue
                sockets = [p.websocket for p in room.connected_participants()]
                self._forget_room_locked(room, reason)
                removed.append(code)
                for websocket in sockets:
                    self._spawn(self._notify_room_closed(websocket, reason))
        return removed

    def _get_room_locked(self, room_code: str) -> WatchPartyRoom | None:
        if not _valid_room_code(room_code):
            return None
        room = self.rooms.get(room_code)
        if room is None:
            return None
        now_wall, now_mono = self._now()
        is_expired, reason = self._room_is_expired(room, now_wall, now_mono)
        if is_expired:
            sockets = [p.websocket for p in room.connected_participants()]
            self._forget_room_locked(room, reason)
            for websocket in sockets:
                self._spawn(self._notify_room_closed(websocket, reason))
            return None
        return room

    def _require_room_locked(self, room_code: str) -> WatchPartyRoom:
        room = self._get_room_locked(room_code)
        if room is None:
            raise RoomNotFoundError()
        return room

    # -- task helpers -----------------------------------------------------

    def _spawn(self, coro: Any) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            return
        task = loop.create_task(coro)

        def _consume(_: asyncio.Task[Any]) -> None:
            try:
                task.exception()
            except (asyncio.CancelledError, Exception):
                pass

        task.add_done_callback(_consume)

    async def _notify_room_closed(self, websocket: Any, reason: str) -> None:
        await self._ws_send(
            websocket,
            {"type": "room_closed", "reason": reason},
        )
        await self._ws_close(websocket, WS_CLOSE_ROOM_CLOSED, reason)

    # -- WebSocket helpers ------------------------------------------------

    @staticmethod
    async def _await_maybe(value: Any, timeout: float) -> None:
        if inspect.isawaitable(value):
            await asyncio.wait_for(value, timeout=timeout)

    async def _ws_send(self, websocket: Any, payload: dict[str, Any]) -> bool:
        if websocket is None:
            return False
        try:
            sender = getattr(websocket, "send_json", None)
            if callable(sender):
                await self._await_maybe(sender(payload), _WS_SEND_TIMEOUT_SEC)
                return True
            sender = getattr(websocket, "send_text", None)
            if callable(sender):
                await self._await_maybe(
                    sender(json.dumps(payload, ensure_ascii=False)),
                    _WS_SEND_TIMEOUT_SEC,
                )
                return True
            sender = getattr(websocket, "send", None)
            if callable(sender):
                await self._await_maybe(
                    sender(
                        {
                            "type": "websocket.send",
                            "text": json.dumps(payload, ensure_ascii=False),
                        }
                    ),
                    _WS_SEND_TIMEOUT_SEC,
                )
                return True
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
        return False

    async def _ws_close(self, websocket: Any, code: int, reason: str = "") -> bool:
        if websocket is None:
            return False
        closer = getattr(websocket, "close", None)
        if not callable(closer):
            return False
        try:
            try:
                result = closer(code=code, reason=reason)
            except TypeError:
                try:
                    result = closer(code=code)
                except TypeError:
                    result = closer()
            await self._await_maybe(result, _WS_CLOSE_TIMEOUT_SEC)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            return False

    async def _send_error(
        self,
        websocket: Any,
        code: str,
        message: str,
        *,
        retry_after_sec: float | None = None,
    ) -> None:
        payload: dict[str, Any] = {"type": "error", "code": code, "message": message}
        if retry_after_sec is not None:
            payload["retry_after_sec"] = retry_after_sec
        await self._ws_send(websocket, payload)

    async def _broadcast(
        self,
        room: WatchPartyRoom,
        payload: dict[str, Any],
        *,
        exclude_participant_id: str | None = None,
    ) -> None:
        targets = [
            participant.websocket
            for participant in room.connected_participants()
            if exclude_participant_id is None or participant.participant_id != exclude_participant_id
        ]
        if not targets:
            return
        await asyncio.gather(
            *(self._ws_send(target, payload) for target in targets),
            return_exceptions=True,
        )

    # -- serialization ----------------------------------------------------

    @staticmethod
    def _serialize_media_item(item: dict[str, Any] | None) -> dict[str, Any]:
        return _serialize_media_item(item)

    def _media_row(self, media_id: str | None) -> dict[str, Any] | None:
        if not media_id:
            return None
        try:
            return self.db.get_media(media_id)
        except Exception:
            logger.warning("watch party media lookup failed", extra={"media_id": media_id})
            return None

    def _active_duration(self, media_id: str | None) -> float | None:
        item = self._media_row(media_id)
        if not item:
            return None
        duration = _coerce_number(item.get("duration_sec"))
        if duration is None or duration < 0:
            return None
        return duration

    def _serialize_room_state(self, room: WatchPartyRoom) -> dict[str, Any]:
        now_mono = time.monotonic()
        now_wall = time.time()
        queue_items: list[dict[str, Any]] = []
        for media_id in room.queue:
            item = self._media_row(media_id) or {"id": media_id}
            queue_items.append(_serialize_media_item(item))
        active_item = None
        if room.active_media_id:
            active_item = _serialize_media_item(
                self._media_row(room.active_media_id) or {"id": room.active_media_id}
            )
        participants = [participant.public_dict() for participant in room.connected_participants()]
        return {
            "room_code": room.room_code,
            "title": room.title,
            "active": not room.closed,
            "requires_pin": room.requires_pin,
            "control_mode": room.control_mode,
            "allow_guest_controls": room.allow_guest_controls,
            "wait_for_buffering": room.wait_for_buffering,
            "paused_for_buffering": room.paused_for_buffering,
            "is_playing": room.is_playing,
            "playback_rate": room.playback_rate,
            # ``position_sec`` is the authoritative base captured at
            # ``state_updated_at``; the web client adds elapsed time using
            # ``server_time_ms`` (and the clock offset learned from pong).
            "position_sec": max(0.0, float(room.position_sec)),
            "effective_position_sec": room.effective_position(now_mono),
            "state_updated_at": room.state_updated_at,
            "state_updated_at_ms": int(room.state_updated_at * 1000),
            "state_seq": room.state_seq,
            "active_media_id": room.active_media_id,
            "active_media": active_item,
            "active_item": active_item,
            # The web client renders ``queue`` as media objects (``id``,
            # ``title``, ...).  Raw ids remain available via ``media_ids``.
            "queue": queue_items,
            "queue_items": list(queue_items),
            "media_ids": list(room.queue),
            "queue_ids": list(room.queue),
            "queue_length": len(room.queue),
            "participants": participants,
            "participant_count": room.participant_count,
            "viewer_count": room.participant_count,
            "max_participants": room.max_participants,
            "created_at": room.created_at,
            "expires_at": room.expires_at,
            "ttl_sec": room.ttl_sec,
            "server_time": now_wall,
            "server_time_ms": int(now_wall * 1000),
        }

    def _serialize_room_summary(self, room: WatchPartyRoom) -> dict[str, Any]:
        return {
            "room_code": room.room_code,
            "title": room.title,
            "active": not room.closed,
            "requires_pin": room.requires_pin,
            "participant_count": room.participant_count,
            "max_participants": room.max_participants,
            "queue_length": len(room.queue),
            "active_media_id": room.active_media_id,
            "is_playing": room.is_playing,
            "playback_rate": room.playback_rate,
            "control_mode": room.control_mode,
            "wait_for_buffering": room.wait_for_buffering,
            "state_seq": room.state_seq,
            "created_at": room.created_at,
            "expires_at": room.expires_at,
            "ttl_sec": room.ttl_sec,
            "join_path": f"/watch/{room.room_code}",
            "join_url": self._join_url(room.room_code),
        }

    # -- room creation ----------------------------------------------------

    def _validate_media_id(self, media_id: Any) -> str:
        if not isinstance(media_id, str) or not media_id.strip():
            raise BadRequestError("Media id must be a non-empty string")
        cleaned = media_id.strip()
        if self._media_row(cleaned) is None:
            raise BadRequestError(f"Unknown media item: {cleaned}")
        return cleaned

    def _normalize_queue(self, media_ids: Any) -> list[str]:
        if media_ids is None:
            return []
        if isinstance(media_ids, (str, bytes)) or not isinstance(media_ids, (list, tuple, set)):
            raise BadRequestError("media_ids must be a list of media ids")
        result: list[str] = []
        seen: set[str] = set()
        for raw_id in media_ids:
            media_id = self._validate_media_id(raw_id)
            if media_id in seen:
                continue
            seen.add(media_id)
            result.append(media_id)
        if len(result) > MAX_QUEUE_ITEMS:
            raise BadRequestError(f"Queue cannot contain more than {MAX_QUEUE_ITEMS} items")
        return result

    def _validate_playback_rate(self, value: Any) -> float:
        rate = _coerce_number(value)
        if rate is None or rate < MIN_PLAYBACK_RATE or rate > MAX_PLAYBACK_RATE:
            raise BadRequestError(
                f"Playback rate must be between {MIN_PLAYBACK_RATE} and {MAX_PLAYBACK_RATE}"
            )
        return rate

    def _validate_position(self, value: Any) -> float:
        position = _coerce_number(value)
        if position is None or position < 0:
            raise BadRequestError("Playback position must be a non-negative number")
        return position

    def _pin_material(self, pin: Any) -> tuple[str, bytes, bytes]:
        if not isinstance(pin, str):
            raise BadRequestError("PIN must be a string")
        cleaned = pin.strip()
        if not 4 <= len(cleaned) <= 32:
            raise BadRequestError("PIN must be between 4 and 32 characters")
        salt = secrets.token_bytes(16)
        return cleaned, salt, _hash_pin(cleaned, salt)

    def _new_participant(
        self,
        *,
        display_name: str,
        role: Literal["host", "guest"],
    ) -> tuple[Participant, str]:
        token = secrets.token_urlsafe(32)
        participant = Participant(
            participant_id=uuid.uuid4().hex,
            display_name=display_name,
            role=role,
            token_digest=_token_digest(token),
        )
        return participant, token

    def _generate_room_code_locked(self) -> str:
        for _ in range(64):
            code = secrets.token_urlsafe(9)
            if code in self.rooms or code in self._issued_room_codes:
                continue
            if not ROOM_CODE_RE.fullmatch(code):
                continue
            self._issued_room_codes.add(code)
            if len(self._issued_room_codes) > 8192:
                self._issued_room_codes = set(self.rooms)
                self._issued_room_codes.add(code)
            return code
        raise RoomCapacityError("Unable to allocate a unique watch party room code")

    def create_room(
        self,
        *,
        title: str | None = None,
        media_ids: list[str] | None = None,
        active_media_id: str | None = None,
        pin: str | None = None,
        control_mode: Literal["host_only", "everyone"] = "host_only",
        wait_for_buffering: bool = False,
        ttl_sec: int | None = None,
        max_participants: int | None = None,
        host_name: str = "Host",
        initial_position_sec: float = 0.0,
        initial_is_playing: bool = False,
        initial_playback_rate: float = 1.0,
    ) -> dict[str, Any]:
        if not self.enabled:
            raise WatchPartyDisabledError()

        clean_host_name = _clean_text(host_name, field_name="Host name", max_length=40)
        room_title = (
            _clean_text(title, field_name="Room title", max_length=120)
            if title is not None
            else "Watch Party"
        )
        if control_mode not in ("host_only", "everyone"):
            raise BadRequestError("control_mode must be 'host_only' or 'everyone'")

        rate = self._validate_playback_rate(initial_playback_rate)
        position = self._validate_position(initial_position_sec)

        if ttl_sec is None:
            resolved_ttl = self._default_ttl_sec()
        else:
            resolved_ttl = _as_int(ttl_sec, -1)
            if resolved_ttl < 0:
                raise BadRequestError("ttl_sec must be a non-negative integer")

        if max_participants is None:
            resolved_max = self._default_max_participants()
        else:
            resolved_max = _as_int(max_participants, 0)
            if resolved_max < 1:
                raise BadRequestError("max_participants must be at least 1")

        with self._lock:
            self._prune_expired_rooms()
            max_rooms = self._max_rooms()
            if len(self.rooms) >= max_rooms:
                raise RoomCapacityError("Maximum number of active watch party rooms reached")

            queue = self._normalize_queue(media_ids)

            if active_media_id is not None and str(active_media_id).strip():
                resolved_active = self._validate_media_id(active_media_id)
            else:
                resolved_active = queue[0] if queue else None

            pin_salt: bytes | None = None
            pin_hash: bytes | None = None
            if pin is not None:
                _, pin_salt, pin_hash = self._pin_material(pin)

            room_code = self._generate_room_code_locked()
            now_wall, now_mono = self._now()
            room = WatchPartyRoom(
                room_code=room_code,
                title=room_title,
                host_name=clean_host_name,
                control_mode=control_mode,
                wait_for_buffering=bool(wait_for_buffering),
                ttl_sec=resolved_ttl,
                max_participants=resolved_max,
                created_at=now_wall,
                created_mono=now_mono,
                expires_at=now_wall + resolved_ttl if resolved_ttl > 0 else 0.0,
                expires_mono=now_mono + resolved_ttl if resolved_ttl > 0 else 0.0,
                last_activity_mono=now_mono,
                pin_salt=pin_salt,
                pin_hash=pin_hash,
                queue=queue,
                active_media_id=resolved_active,
                active_duration_sec=self._active_duration(resolved_active),
                is_playing=bool(initial_is_playing) and resolved_active is not None,
                playback_rate=rate,
                position_sec=position,
                state_updated_mono=now_mono,
                state_updated_at=now_wall,
                state_seq=0,
                chat=deque(maxlen=self._chat_history_limit()),
            )

            host_participant, host_token = self._new_participant(
                display_name=clean_host_name,
                role="host",
            )
            room.participants[host_participant.participant_id] = host_participant
            self.rooms[room_code] = room

            return {
                "room_code": room_code,
                "join_path": f"/watch/{room_code}",
                "join_url": self._join_url(room_code),
                "host_participant_id": host_participant.participant_id,
                "host_token": host_token,
                "requires_pin": room.requires_pin,
                "room_state": self._serialize_room_state(room),
            }

    def list_rooms(self) -> list[dict[str, Any]]:
        with self._lock:
            self._prune_expired_rooms()
            return [self._serialize_room_summary(room) for room in self.rooms.values()]

    def get_room_host_view(self, room_code: str) -> dict[str, Any]:
        with self._lock:
            room = self._require_room_locked(room_code)
            summary = self._serialize_room_summary(room)
            summary.update(
                {
                    "room_state": self._serialize_room_state(room),
                    "participants": [
                        participant.public_dict()
                        for participant in room.participants.values()
                        if not participant.kicked
                    ],
                    "chat_history": list(room.chat),
                }
            )
            return summary

    def create_or_get_host_session(
        self,
        room_code: str,
        display_name: str = "Host",
    ) -> dict[str, Any]:
        if not self.enabled:
            raise WatchPartyDisabledError()
        clean_name = _clean_text(display_name, field_name="Display name", max_length=40)

        evicted_socket: Any = None
        with self._lock:
            room = self._require_room_locked(room_code)
            hosts = [
                participant
                for participant in room.participants.values()
                if participant.role == "host" and not participant.kicked
            ]
            if len(hosts) >= _MAX_HOST_SESSIONS:
                disconnected = [p for p in hosts if not p.connected]
                evicted = min(disconnected or hosts, key=lambda p: p.joined_at)
                evicted_socket = evicted.websocket
                room.participants.pop(evicted.participant_id, None)
                evicted.token_digest = b""
                evicted.websocket = None
                evicted.connected = False

            participant, token = self._new_participant(display_name=clean_name, role="host")
            room.participants[participant.participant_id] = participant
            room.last_activity_mono = time.monotonic()

            if evicted_socket is not None:
                self._spawn(
                    self._ws_close(
                        evicted_socket,
                        WS_CLOSE_REPLACED,
                        "Host session replaced",
                    )
                )

            return {
                "room_code": room.room_code,
                "participant_id": participant.participant_id,
                "host_token": token,
                "role": "host",
                "display_name": participant.display_name,
                "room_state": self._serialize_room_state(room),
                "chat_history": list(room.chat),
            }

    # -- room updates -----------------------------------------------------

    def _apply_active_media_locked(
        self,
        room: WatchPartyRoom,
        media_id: str | None,
        *,
        reset_playback: bool,
        now_wall: float,
        now_mono: float,
    ) -> None:
        room.active_media_id = media_id
        room.active_duration_sec = self._active_duration(media_id)
        if reset_playback:
            room.position_sec = 0.0
            room.is_playing = False
            room.paused_for_buffering = False
            room.state_updated_mono = now_mono
            room.state_updated_at = now_wall
            room.state_seq += 1

    def _apply_queue_locked(
        self,
        room: WatchPartyRoom,
        new_queue: list[str],
        *,
        now_wall: float,
        now_mono: float,
    ) -> bool:
        if new_queue == room.queue:
            return False
        active_removed = bool(room.active_media_id) and room.active_media_id not in new_queue
        room.queue = list(new_queue)
        if active_removed:
            self._apply_active_media_locked(
                room,
                room.queue[0] if room.queue else None,
                reset_playback=True,
                now_wall=now_wall,
                now_mono=now_mono,
            )
        else:
            room.state_seq += 1
        return True

    async def update_room(
        self,
        room_code: str,
        *,
        title: str | None = None,
        control_mode: Literal["host_only", "everyone"] | None = None,
        wait_for_buffering: bool | None = None,
        queue: list[str] | None = None,
        add_media_ids: list[str] | None = None,
        remove_media_ids: list[str] | None = None,
        active_media_id: str | None = None,
        pin: str | None = None,
        clear_pin: bool = False,
        extend_ttl_sec: int | None = None,
    ) -> dict[str, Any]:
        if pin is not None and clear_pin:
            raise BadRequestError("Cannot set and clear a PIN in the same request")

        pin_salt: bytes | None = None
        pin_hash: bytes | None = None
        if pin is not None:
            _, pin_salt, pin_hash = self._pin_material(pin)

        if control_mode is not None and control_mode not in ("host_only", "everyone"):
            raise BadRequestError("control_mode must be 'host_only' or 'everyone'")

        resolved_ttl: int | None = None
        if extend_ttl_sec is not None:
            resolved_ttl = _as_int(extend_ttl_sec, -1)
            if resolved_ttl < 0:
                raise BadRequestError("extend_ttl_sec must be a non-negative integer")

        clean_title: str | None = None
        if title is not None:
            clean_title = _clean_text(title, field_name="Room title", max_length=120)

        # Validate every input before mutating the room so a rejected update
        # cannot leave a partially-applied state.
        prepared_queue = self._normalize_queue(queue) if queue is not None else None
        prepared_add = (
            self._normalize_queue(add_media_ids) if add_media_ids is not None else None
        )
        prepared_remove: set[str] | None = None
        if remove_media_ids is not None:
            if isinstance(remove_media_ids, (str, bytes)) or not isinstance(
                remove_media_ids, (list, tuple, set)
            ):
                raise BadRequestError("remove_media_ids must be a list of media ids")
            prepared_remove = {str(media_id).strip() for media_id in remove_media_ids}

        active_provided = active_media_id is not None
        clear_active = False
        prepared_active: str | None = None
        if active_media_id is not None:
            if isinstance(active_media_id, str) and not active_media_id.strip():
                clear_active = True
            else:
                prepared_active = self._validate_media_id(active_media_id)

        with self._lock:
            room = self._require_room_locked(room_code)
            now_wall, now_mono = self._now()
            queue_changed = False

            new_queue: list[str] | None = prepared_queue
            working: list[str] | None = None
            if prepared_queue is None and (
                prepared_add is not None or prepared_remove is not None
            ):
                working = list(room.queue)
                if prepared_add:
                    for media_id in prepared_add:
                        if media_id not in working:
                            working.append(media_id)
                if len(working) > MAX_QUEUE_ITEMS:
                    raise BadRequestError(
                        f"Queue cannot contain more than {MAX_QUEUE_ITEMS} items"
                    )
                if prepared_remove:
                    working = [
                        media_id for media_id in working if media_id not in prepared_remove
                    ]

            active_before = room.active_media_id
            active_was_in_queue = bool(active_before) and active_before in room.queue
            active_removed_outside_queue = bool(
                active_before
                and prepared_remove
                and active_before in prepared_remove
                and not active_was_in_queue
            )

            if clean_title is not None:
                room.title = clean_title
            if control_mode is not None and control_mode != room.control_mode:
                room.control_mode = control_mode
                room.state_seq += 1
            if (
                wait_for_buffering is not None
                and bool(wait_for_buffering) != room.wait_for_buffering
            ):
                room.wait_for_buffering = bool(wait_for_buffering)
                room.state_seq += 1

            if new_queue is not None:
                queue_changed = self._apply_queue_locked(
                    room, new_queue, now_wall=now_wall, now_mono=now_mono
                )
            elif working is not None:
                queue_changed = self._apply_queue_locked(
                    room, working, now_wall=now_wall, now_mono=now_mono
                )

            if (
                active_removed_outside_queue
                and room.active_media_id == active_before
            ):
                # The active item lived outside the queue; removal should still
                # advance the room to the first remaining item.
                self._apply_active_media_locked(
                    room,
                    room.queue[0] if room.queue else None,
                    reset_playback=True,
                    now_wall=now_wall,
                    now_mono=now_mono,
                )

            if active_provided:
                if clear_active:
                    if room.active_media_id is not None:
                        self._apply_active_media_locked(
                            room,
                            None,
                            reset_playback=True,
                            now_wall=now_wall,
                            now_mono=now_mono,
                        )
                elif prepared_active is not None and prepared_active != room.active_media_id:
                    self._apply_active_media_locked(
                        room,
                        prepared_active,
                        reset_playback=True,
                        now_wall=now_wall,
                        now_mono=now_mono,
                    )

            if clear_pin:
                room.pin_salt = None
                room.pin_hash = None
            elif pin_hash is not None:
                room.pin_salt = pin_salt
                room.pin_hash = pin_hash
            if clear_pin or pin_hash is not None:
                for key in [key for key in self._pin_attempts if key[0] == room.room_code]:
                    self._pin_attempts.pop(key, None)

            if resolved_ttl is not None and resolved_ttl > 0:
                if room.expires_at:
                    room.expires_at += resolved_ttl
                else:
                    room.expires_at = now_wall + resolved_ttl
                if room.expires_mono:
                    room.expires_mono += resolved_ttl
                else:
                    room.expires_mono = now_mono + resolved_ttl

            room.last_activity_mono = now_mono
            state = self._serialize_room_state(room)

        await self._broadcast(
            room,
            {
                "type": "sync_state",
                "reason": "queue_update" if queue_changed else "settings",
                "room_state": state,
                "state_seq": state["state_seq"],
            },
        )
        return state

    async def close_room(self, room_code: str, reason: str = "Room closed by host") -> bool:
        with self._lock:
            room = self.rooms.get(room_code) if _valid_room_code(room_code) else None
            if room is None:
                return False
            clean_reason = _clean_control_chars(reason) or "Room closed"
            sockets = [p.websocket for p in room.connected_participants()]
            self._forget_room_locked(room, clean_reason)

        payload = {"type": "room_closed", "reason": clean_reason}
        if sockets:
            await asyncio.gather(
                *(self._notify_room_closed_with_payload(socket, payload, clean_reason) for socket in sockets),
                return_exceptions=True,
            )
        return True

    async def _notify_room_closed_with_payload(
        self,
        websocket: Any,
        payload: dict[str, Any],
        reason: str,
    ) -> None:
        await self._ws_send(websocket, payload)
        await self._ws_close(websocket, WS_CLOSE_ROOM_CLOSED, reason)

    # -- public room info -------------------------------------------------

    def get_public_info(self, room_code: str) -> dict[str, Any]:
        raw_code = room_code if isinstance(room_code, str) else str(room_code or "")
        inactive = {
            "room_code": raw_code,
            "active": False,
            "requires_pin": False,
            "title": "",
            "participant_count": 0,
            "max_participants": 0,
        }
        with self._lock:
            room = self._get_room_locked(raw_code)
            if room is None:
                return inactive
            if room.requires_pin:
                return {
                    "room_code": room.room_code,
                    "active": True,
                    "requires_pin": True,
                    "title": "Protected Watch Party",
                    "participant_count": room.participant_count,
                    "max_participants": room.max_participants,
                }
            return {
                "room_code": room.room_code,
                "active": True,
                "requires_pin": False,
                "title": room.title,
                "participant_count": room.participant_count,
                "max_participants": room.max_participants,
            }

    # -- rate limiting ----------------------------------------------------

    @staticmethod
    def _prune_bucket(bucket: deque[float], cutoff: float) -> None:
        while bucket and bucket[0] < cutoff:
            bucket.popleft()

    def _prune_rate_maps(self, now: float) -> None:
        pin_window = self._pin_window_sec()
        for key in list(self._pin_attempts):
            bucket = self._pin_attempts[key]
            self._prune_bucket(bucket, now - pin_window)
            if not bucket:
                self._pin_attempts.pop(key, None)
        for key in list(self._join_attempts):
            bucket = self._join_attempts[key]
            self._prune_bucket(bucket, now - _JOIN_RATE_WINDOW_SEC)
            if not bucket:
                self._join_attempts.pop(key, None)

    def _check_join_rate(self, client_ip: str, now: float) -> None:
        bucket = self._join_attempts.get(client_ip)
        if bucket is None:
            if len(self._join_attempts) >= _MAX_JOIN_KEYS:
                raise RateLimitError(
                    "Too many join attempts; try again shortly",
                    retry_after_sec=5,
                )
            bucket = deque()
            self._join_attempts[client_ip] = bucket
        self._prune_bucket(bucket, now - _JOIN_RATE_WINDOW_SEC)
        if len(bucket) >= _JOIN_RATE_COUNT:
            retry = _JOIN_RATE_WINDOW_SEC - (now - bucket[0])
            raise RateLimitError(
                "Too many join attempts from this address",
                retry_after_sec=max(1, int(math.ceil(retry))),
            )
        bucket.append(now)

    def _pin_bucket(self, room_code: str, client_ip: str, now: float) -> deque[float]:
        key = (room_code, client_ip)
        bucket = self._pin_attempts.get(key)
        if bucket is None:
            if len(self._pin_attempts) >= _MAX_PIN_ATTEMPTS_KEYS:
                raise RateLimitError(
                    "Too many PIN attempts; try again later",
                    retry_after_sec=10,
                )
            bucket = deque()
            self._pin_attempts[key] = bucket
        self._prune_bucket(bucket, now - self._pin_window_sec())
        return bucket

    # -- joining ----------------------------------------------------------

    def _ensure_guest_capacity_locked(self, room: WatchPartyRoom, now_mono: float) -> None:
        def _active_count() -> int:
            return sum(
                1 for participant in room.participants.values() if not participant.kicked
            )

        ghost_grace = float(getattr(self.settings, "watch_party_ghost_grace_sec", 30.0))
        while _active_count() >= room.max_participants:
            disconnected_guests = [
                participant
                for participant in room.participants.values()
                if participant.role == "guest"
                and not participant.kicked
                and not participant.connected
                and (
                    participant.ever_connected
                    or (now_mono - participant.last_seen_mono) >= ghost_grace
                )
            ]
            if not disconnected_guests:
                raise RoomCapacityError("Watch party room is full")
            # Reclaim stale never-connected ghost joins first, then oldest disconnected guests
            oldest = min(
                disconnected_guests,
                key=lambda p: (1 if p.ever_connected else 0, p.last_seen_mono, p.joined_at),
            )
            room.participants.pop(oldest.participant_id, None)
            oldest.token_digest = b""

    def _prune_kicked_tombstones_locked(self, room: WatchPartyRoom) -> None:
        kicked = [
            participant
            for participant in room.participants.values()
            if participant.kicked and not participant.connected
        ]
        if len(kicked) <= _MAX_KICKED_TOMBSTONES:
            return
        kicked.sort(key=lambda p: p.last_seen_mono)
        for participant in kicked[: len(kicked) - _MAX_KICKED_TOMBSTONES]:
            room.participants.pop(participant.participant_id, None)

    def join_room(
        self,
        room_code: str,
        *,
        display_name: str,
        pin: str | None = None,
        client_ip: str = "unknown",
    ) -> dict[str, Any]:
        if not self.enabled:
            raise WatchPartyDisabledError()

        clean_name = _clean_text(display_name, field_name="Display name", max_length=40)
        clean_ip = _clean_control_chars(client_ip)[:128] or "unknown"
        normalized_pin = pin.strip() if isinstance(pin, str) else None
        if normalized_pin == "":
            normalized_pin = None

        with self._lock:
            room = self._require_room_locked(room_code)
            now_mono = time.monotonic()
            self._prune_rate_maps(now_mono)
            self._check_join_rate(clean_ip, now_mono)

            if room.pin_hash is not None:
                bucket = self._pin_bucket(room.room_code, clean_ip, now_mono)
                max_attempts = self._pin_max_attempts()
                if len(bucket) >= max_attempts:
                    retry = self._pin_window_sec() - (now_mono - bucket[0])
                    raise RateLimitError(
                        "Too many PIN attempts; try again later",
                        retry_after_sec=max(1, int(math.ceil(retry))),
                    )
                if normalized_pin is None:
                    bucket.append(now_mono)
                    raise PinRequiredError()
                candidate = _hash_pin(normalized_pin, room.pin_salt or b"")
                if not hmac.compare_digest(candidate, room.pin_hash):
                    bucket.append(now_mono)
                    raise InvalidPinError()
                self._pin_attempts.pop((room.room_code, clean_ip), None)

            self._ensure_guest_capacity_locked(room, now_mono)
            self._prune_kicked_tombstones_locked(room)

            participant, token = self._new_participant(
                display_name=clean_name,
                role="guest",
            )
            room.participants[participant.participant_id] = participant
            room.last_activity_mono = now_mono

            return {
                "room_code": room.room_code,
                "participant_id": participant.participant_id,
                "guest_token": token,
                "role": "guest",
                "display_name": participant.display_name,
                "room_state": self._serialize_room_state(room),
                "chat_history": list(room.chat),
            }

    # -- authentication / authorization ----------------------------------

    def _find_participant_by_token_locked(
        self,
        room: WatchPartyRoom,
        token: str,
    ) -> Participant | None:
        digest = _token_digest(token)
        match: Participant | None = None
        for participant in room.participants.values():
            if participant.kicked or not participant.token_digest:
                continue
            if hmac.compare_digest(participant.token_digest, digest):
                match = participant
        return match

    def authenticate_token(
        self,
        room_code: str,
        token: str | None,
    ) -> tuple[WatchPartyRoom, Participant]:
        if not isinstance(token, str) or not token:
            raise AuthError("A watch party session token is required")
        with self._lock:
            room = self._require_room_locked(room_code)
            participant = self._find_participant_by_token_locked(room, token)
            if participant is None:
                raise AuthError("Invalid or expired watch party token")
            return room, participant

    def authorize_media_access(
        self,
        room_code: str,
        token: str | None,
        media_id: str,
    ) -> tuple[WatchPartyRoom, Participant]:
        room, participant = self.authenticate_token(room_code, token)
        if not isinstance(media_id, str) or media_id not in room.allowed_media_ids():
            raise ForbiddenError("Media item is not in this watch party queue")
        return room, participant

    # -- WebSocket lifecycle ---------------------------------------------

    def _presence_payload(
        self,
        room: WatchPartyRoom,
        event: str,
        participant: Participant,
    ) -> dict[str, Any]:
        # The web client switches on ``join`` / ``leave`` / ``kick``.
        client_event = {"joined": "join", "left": "leave", "kicked": "kick"}.get(event, event)
        return {
            "type": "presence",
            "event": client_event,
            "action": client_event,
            "participant_id": participant.participant_id,
            "display_name": participant.display_name,
            "role": participant.role,
            "participant": participant.public_dict(),
            "participants": [
                current.public_dict() for current in room.connected_participants()
            ],
            "room_state": self._serialize_room_state(room),
            "participant_count": room.participant_count,
            "connected_count": room.participant_count,
            "max_participants": room.max_participants,
        }

    async def connect_ws(
        self,
        room_code: str,
        token: str,
        websocket: Any,
    ) -> tuple[WatchPartyRoom, Participant]:
        old_websocket: Any = None
        with self._lock:
            room = self._require_room_locked(room_code)
            participant = self._find_participant_by_token_locked(room, token)
            if participant is None:
                raise AuthError("Invalid or expired watch party token")
            if participant.kicked:
                raise AuthError("This participant has been removed from the watch party")
            old_websocket = participant.websocket
            participant.websocket = websocket
            participant.connected = True
            participant.ever_connected = True
            participant.buffering = False
            participant.last_seen_mono = time.monotonic()
            room.last_activity_mono = participant.last_seen_mono
            auth_ok = {
                "type": "auth_ok",
                "room_code": room.room_code,
                "participant_id": participant.participant_id,
                "role": participant.role,
                "display_name": participant.display_name,
                "room_state": self._serialize_room_state(room),
                "chat_history": list(room.chat),
            }
            presence = self._presence_payload(room, "joined", participant)

        if old_websocket is not None and old_websocket is not websocket:
            await self._ws_close(old_websocket, WS_CLOSE_REPLACED, "Connection replaced")

        await self._ws_send(websocket, auth_ok)
        await self._broadcast(
            room,
            presence,
            exclude_participant_id=participant.participant_id,
        )

        resumed = False
        with self._lock:
            if self._get_room_locked(room.room_code) is room:
                resumed = self._maybe_resume_after_buffering_locked(room)
        if resumed:
            await self._broadcast_sync_state(room, reason="buffering")
        return room, participant

    async def disconnect_ws(
        self,
        room_code: str,
        participant_id: str,
        websocket: Any,
    ) -> None:
        with self._lock:
            room = self._get_room_locked(room_code)
            if room is None:
                return
            participant = room.participants.get(participant_id)
            if participant is None:
                return
            # A replaced socket may finish disconnecting after a newer socket
            # connected; only the current socket may clear this participant.
            if participant.websocket is not websocket:
                return
            participant.websocket = None
            participant.connected = False
            participant.buffering = False
            participant.last_seen_mono = time.monotonic()
            room.last_activity_mono = participant.last_seen_mono
            presence = self._presence_payload(room, "left", participant)
            resumed = self._maybe_resume_after_buffering_locked(room)

        await self._broadcast(room, presence)
        if resumed:
            await self._broadcast_sync_state(room, reason="buffering")

    # -- buffering coordination ------------------------------------------

    def _maybe_resume_after_buffering_locked(self, room: WatchPartyRoom) -> bool:
        if not room.paused_for_buffering:
            return False
        connected = room.connected_participants()
        if not connected:
            return False
        if any(participant.buffering for participant in connected):
            return False
        now_wall, now_mono = self._now()
        room.is_playing = True
        room.paused_for_buffering = False
        room.state_updated_mono = now_mono
        room.state_updated_at = now_wall
        room.state_seq += 1
        room.last_activity_mono = now_mono
        return True

    def _set_buffering_locked(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        buffering: bool,
    ) -> bool:
        now_wall, now_mono = self._now()
        participant.buffering = bool(buffering)
        participant.last_seen_mono = now_mono
        room.last_activity_mono = now_mono
        if participant.buffering:
            if room.wait_for_buffering and room.is_playing:
                room.position_sec = room.effective_position(now_mono)
                room.is_playing = False
                room.paused_for_buffering = True
                room.state_updated_mono = now_mono
                room.state_updated_at = now_wall
                room.state_seq += 1
                return True
            return False
        return self._maybe_resume_after_buffering_locked(room)

    async def _broadcast_sync_state(
        self,
        room: WatchPartyRoom,
        *,
        reason: str,
        target: Any | None = None,
    ) -> dict[str, Any]:
        state = self._serialize_room_state(room)
        payload = {
            "type": "sync_state",
            "reason": reason,
            "room_state": state,
            "state_seq": state["state_seq"],
            "is_playing": state["is_playing"],
            "position_sec": state["effective_position_sec"],
        }
        if target is not None:
            await self._ws_send(target, payload)
        else:
            await self._broadcast(room, payload)
        return state

    # -- WS message handling ---------------------------------------------

    def _ws_error_code(self, error: WatchPartyError) -> str:
        if isinstance(error, RateLimitError):
            return "rate_limited"
        if isinstance(error, (AuthError, PinRequiredError)):
            return "unauthorized"
        if isinstance(error, ForbiddenError):
            return "forbidden"
        if isinstance(error, RoomNotFoundError):
            return "room_not_found"
        if isinstance(error, RoomCapacityError):
            return "room_full"
        if isinstance(error, WatchPartyDisabledError):
            return "disabled"
        if isinstance(error, BadRequestError):
            return "bad_request"
        return "error"

    async def handle_ws_message(
        self,
        room_code: str,
        participant_id: str,
        raw_text: str,
        websocket: Any,
    ) -> None:
        if isinstance(raw_text, bytes):
            try:
                text = raw_text.decode("utf-8")
            except UnicodeDecodeError:
                await self._send_error(websocket, "invalid_message", "Message must be valid UTF-8")
                return
        else:
            text = str(raw_text)

        if len(text.encode("utf-8")) > MAX_WS_MESSAGE_BYTES:
            await self._send_error(
                websocket,
                "message_too_large",
                f"Message exceeds {MAX_WS_MESSAGE_BYTES} bytes",
            )
            return

        pending_error: tuple[str, str, float | None] | None = None
        with self._lock:
            room = self._get_room_locked(room_code)
            if room is None:
                pending_error = ("room_not_found", "Watch party room is not active", None)
                participant = None
            else:
                participant = room.participants.get(participant_id)
                if participant is None or participant.kicked:
                    pending_error = ("unauthorized", "Participant is not active", None)
                elif participant.websocket is not None and participant.websocket is not websocket:
                    # Ignore frames from a socket that has already been replaced.
                    return
                else:
                    now_mono = time.monotonic()
                    self._prune_bucket(
                        participant.frame_timestamps,
                        now_mono - _WS_FRAME_RATE_WINDOW_SEC,
                    )
                    if len(participant.frame_timestamps) >= _WS_FRAME_RATE_COUNT:
                        retry = _WS_FRAME_RATE_WINDOW_SEC - (
                            now_mono - participant.frame_timestamps[0]
                        )
                        pending_error = (
                            "rate_limited",
                            "Too many WebSocket messages",
                            max(1.0, retry),
                        )
                    else:
                        participant.frame_timestamps.append(now_mono)
                        participant.last_seen_mono = now_mono
                        room.last_activity_mono = now_mono

        if pending_error is not None:
            code, message, retry_after = pending_error
            await self._send_error(
                websocket,
                code,
                message,
                retry_after_sec=retry_after,
            )
            return
        if room is None or participant is None:
            return

        try:
            try:
                payload = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                await self._send_error(websocket, "invalid_json", "Message must be valid JSON")
                return
            if not isinstance(payload, dict):
                await self._send_error(websocket, "invalid_message", "Message must be a JSON object")
                return
            message_type = payload.get("type")
            if not isinstance(message_type, str) or not message_type:
                await self._send_error(websocket, "invalid_message", "Message type is required")
                return

            if message_type == "ping":
                await self._handle_ping(room, participant, payload, websocket)
            elif message_type == "request_sync":
                await self._handle_request_sync(room, participant, payload, websocket)
            elif message_type == "play":
                await self._handle_play(room, participant, payload, websocket)
            elif message_type == "pause":
                await self._handle_pause(room, participant, payload, websocket)
            elif message_type == "seek":
                await self._handle_seek(room, participant, payload, websocket)
            elif message_type == "rate":
                await self._handle_rate(room, participant, payload, websocket)
            elif message_type == "select_item":
                await self._handle_select_item(room, participant, payload, websocket)
            elif message_type == "queue_update":
                await self._handle_queue_update(room, participant, payload, websocket)
            elif message_type == "update_settings":
                await self._handle_update_settings(room, participant, payload, websocket)
            elif message_type == "close_room":
                await self._handle_close_room(room, participant, payload, websocket)
            elif message_type == "buffering":
                await self._handle_buffering(room, participant, payload, websocket, default=True)
            elif message_type == "ready":
                await self._handle_buffering(
                    room, participant, payload, websocket, default=False, ready_mode=True
                )
            elif message_type == "chat_message":
                await self._handle_chat_message(room, participant, payload, websocket)
            elif message_type == "reaction":
                await self._handle_reaction(room, participant, payload, websocket)
            elif message_type == "kick":
                await self._handle_kick(room, participant, payload, websocket)
            elif message_type == "auth":
                await self._handle_auth(room, participant, payload, websocket)
            elif message_type == "leave":
                await self.disconnect_ws(room.room_code, participant.participant_id, websocket)
            else:
                await self._send_error(
                    websocket,
                    "unknown_type",
                    f"Unsupported message type: {message_type[:64]}",
                )
        except WatchPartyError as error:
            retry_after = (
                error.retry_after_sec if isinstance(error, RateLimitError) else None
            )
            await self._send_error(
                websocket,
                self._ws_error_code(error),
                error.detail,
                retry_after_sec=retry_after,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Unexpected watch party WebSocket message failure",
                extra={"room_code": room_code},
            )
            await self._send_error(
                websocket,
                "internal_error",
                "The message could not be processed",
            )

    def _require_control_locked(self, room: WatchPartyRoom, participant: Participant) -> None:
        if participant.role == "host" or room.control_mode == "everyone":
            return
        raise ForbiddenError("Only the host can control playback in this room")

    def _require_host_locked(self, participant: Participant) -> None:
        if participant.role != "host":
            raise ForbiddenError("Only the host can do that")

    def _sync_position_locked(self, room: WatchPartyRoom, now_mono: float) -> None:
        room.position_sec = room.effective_position(now_mono)
        room.state_updated_mono = now_mono

    async def _handle_ping(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        now_wall = time.time()
        client_time_ms = _coerce_number(
            payload.get("client_time_ms", payload.get("ts", payload.get("client_ts")))
        )
        with self._lock:
            state_seq = room.state_seq
        await self._ws_send(
            websocket,
            {
                "type": "pong",
                # The web client computes its clock offset from these two
                # millisecond fields; the legacy keys stay for other clients.
                "client_time_ms": client_time_ms,
                "server_time_ms": int(now_wall * 1000),
                "server_time": now_wall,
                "state_seq": state_seq,
                "client_ts": client_time_ms,
            },
        )

    async def _handle_request_sync(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        await self._broadcast_sync_state(room, reason="requested", target=websocket)

    async def _handle_play(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        now_wall, now_mono = self._now()
        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._require_control_locked(current, actor)
            if current.active_media_id is None:
                raise BadRequestError("No media is selected")
            if "position_sec" in payload:
                position = self._validate_position(payload.get("position_sec"))
            else:
                position = current.effective_position(now_mono)
            if current.active_duration_sec is not None and current.active_duration_sec > 0:
                position = min(float(current.active_duration_sec), position)
            current.position_sec = position
            current.is_playing = True
            current.paused_for_buffering = False
            current.state_updated_mono = now_mono
            current.state_updated_at = now_wall
            current.state_seq += 1
            current.last_activity_mono = now_mono
        await self._broadcast_sync_state(current, reason="play")

    async def _handle_pause(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        now_wall, now_mono = self._now()
        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._require_control_locked(current, actor)
            self._sync_position_locked(current, now_mono)
            current.is_playing = False
            current.paused_for_buffering = False
            current.state_updated_at = now_wall
            current.state_seq += 1
            current.last_activity_mono = now_mono
        await self._broadcast_sync_state(current, reason="pause")

    async def _handle_seek(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        now_wall, now_mono = self._now()
        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._require_control_locked(current, actor)
            position = self._validate_position(payload.get("position_sec"))
            if current.active_duration_sec is not None and current.active_duration_sec > 0:
                position = min(float(current.active_duration_sec), position)
            current.position_sec = position
            current.state_updated_mono = now_mono
            current.state_updated_at = now_wall
            current.state_seq += 1
            current.last_activity_mono = now_mono
        await self._broadcast_sync_state(current, reason="seek")

    async def _handle_rate(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        now_wall, now_mono = self._now()
        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._require_control_locked(current, actor)
            rate = self._validate_playback_rate(payload.get("rate", payload.get("playback_rate")))
            if current.is_playing:
                self._sync_position_locked(current, now_mono)
            current.playback_rate = rate
            current.state_updated_at = now_wall
            current.state_seq += 1
            current.last_activity_mono = now_mono
        await self._broadcast_sync_state(current, reason="rate")

    async def _handle_select_item(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        now_wall, now_mono = self._now()
        media_id = payload.get("media_id", payload.get("active_media_id"))
        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._require_control_locked(current, actor)
            if not isinstance(media_id, str) or media_id not in current.allowed_media_ids():
                raise ForbiddenError("Media item is not in this watch party queue")
            self._apply_active_media_locked(
                current,
                media_id,
                reset_playback=True,
                now_wall=now_wall,
                now_mono=now_mono,
            )
            current.last_activity_mono = now_mono
        await self._broadcast_sync_state(current, reason="select")

    async def _handle_queue_update(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        with self._lock:
            self._require_host_locked(participant)
            room = self._require_room_locked(room.room_code)
            actor = room.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._require_host_locked(actor)

            now_wall, now_mono = self._now()
            queue_changed = False

            if "queue" in payload:
                new_queue = self._normalize_queue(payload.get("queue"))
                queue_changed = self._apply_queue_locked(
                    room,
                    new_queue,
                    now_wall=now_wall,
                    now_mono=now_mono,
                )
            else:
                working = list(room.queue)
                action = payload.get("action", payload.get("op"))
                raw_ids: Any = payload.get("media_ids")
                if raw_ids is None:
                    if "media_id" in payload:
                        raw_ids = [payload.get("media_id")]
                    elif "add_media_ids" in payload:
                        raw_ids = payload.get("add_media_ids")
                    elif "remove_media_ids" in payload:
                        raw_ids = payload.get("remove_media_ids")
                        action = "remove"
                    else:
                        raw_ids = []

                if isinstance(raw_ids, str):
                    raw_ids = [raw_ids]
                if not isinstance(raw_ids, (list, tuple, set)):
                    raise BadRequestError("media_ids must be a list of media ids")

                if action in (None, "", "add", "append", "set", "replace"):
                    if action in ("set", "replace"):
                        working = []
                    for media_id in self._normalize_queue(raw_ids):
                        if media_id not in working:
                            working.append(media_id)
                    if len(working) > MAX_QUEUE_ITEMS:
                        raise BadRequestError(
                            f"Queue cannot contain more than {MAX_QUEUE_ITEMS} items"
                        )
                elif action in ("remove", "delete"):
                    removal = {str(media_id).strip() for media_id in raw_ids}
                    working = [media_id for media_id in working if media_id not in removal]
                else:
                    raise BadRequestError("Unsupported queue action")

                queue_changed = self._apply_queue_locked(
                    room,
                    working,
                    now_wall=now_wall,
                    now_mono=now_mono,
                )

            if "active_media_id" in payload:
                requested_active = payload.get("active_media_id")
                if isinstance(requested_active, str) and requested_active.strip():
                    resolved_active = self._validate_media_id(requested_active)
                    if resolved_active != room.active_media_id:
                        self._apply_active_media_locked(
                            room,
                            resolved_active,
                            reset_playback=True,
                            now_wall=now_wall,
                            now_mono=now_mono,
                        )
                elif requested_active is None:
                    self._apply_active_media_locked(
                        room,
                        room.queue[0] if room.queue else None,
                        reset_playback=True,
                        now_wall=now_wall,
                        now_mono=now_mono,
                    )

            room.last_activity_mono = now_mono
            state = self._serialize_room_state(room)

        await self._broadcast(
            room,
            {
                "type": "sync_state",
                "reason": "queue_update",
                "room_state": state,
                "state_seq": state["state_seq"],
            },
        )

    async def _handle_update_settings(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        """Host-only ``update_settings`` message from the web client."""

        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._require_host_locked(actor)

            if "control_mode" in payload:
                control_mode = payload.get("control_mode")
                if control_mode not in ("host_only", "everyone"):
                    raise BadRequestError("control_mode must be 'host_only' or 'everyone'")
                if control_mode != current.control_mode:
                    current.control_mode = control_mode
                    current.state_seq += 1
            if "wait_for_buffering" in payload:
                wait_for_buffering = _as_bool(
                    payload.get("wait_for_buffering"), current.wait_for_buffering
                )
                if wait_for_buffering != current.wait_for_buffering:
                    current.wait_for_buffering = wait_for_buffering
                    current.state_seq += 1
            if "title" in payload:
                clean_title = _clean_text(payload.get("title"), field_name="Room title", max_length=120)
                if clean_title != current.title:
                    current.title = clean_title
                    current.state_seq += 1
            current.last_activity_mono = time.monotonic()

        await self._broadcast_sync_state(current, reason="settings")

    async def _handle_close_room(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        """Host-only ``close_room`` message from the web client."""

        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._require_host_locked(actor)
            reason = (_clean_control_chars(payload.get("reason")) or "Room closed by host")[:200]
        await self.close_room(current.room_code, reason=reason)

    async def _handle_auth(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        """Acknowledge the web client's ``auth`` frame without leaking tokens."""

        token = payload.get("token")
        if not isinstance(token, str) or not token:
            raise AuthError("A watch party session token is required")
        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            match = self._find_participant_by_token_locked(current, token)
            if (
                actor is None
                or actor.kicked
                or match is None
                or match.participant_id != actor.participant_id
            ):
                raise AuthError("Invalid or expired watch party token")
            ack = {
                "type": "auth_ok",
                "room_code": current.room_code,
                "participant_id": actor.participant_id,
                "role": actor.role,
                "display_name": actor.display_name,
                "room_state": self._serialize_room_state(current),
                "chat_history": list(current.chat),
            }
        await self._ws_send(websocket, ack)

    async def _handle_buffering(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
        *,
        default: bool,
        ready_mode: bool = False,
    ) -> None:
        # The web client sends {"type": "buffering", "is_buffering": bool}.
        # Older clients use ``buffering``; ``ready`` is accepted for both
        # message types (ready == not buffering).
        if "is_buffering" in payload:
            raw_value: Any = payload.get("is_buffering")
        elif "buffering" in payload:
            raw_value = payload.get("buffering")
        elif "ready" in payload:
            ready_value = payload.get("ready")
            raw_value = (
                not _as_bool(ready_value, not default) if ready_mode else ready_value
            )
        else:
            raw_value = default
        buffering = raw_value if isinstance(raw_value, bool) else _as_bool(raw_value, default)

        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            previous = actor.buffering
            changed = self._set_buffering_locked(current, actor, buffering)
            flag_changed = previous != actor.buffering

        # Broadcast on any buffering change so other clients see the updated
        # participant list, even when playback itself did not pause/resume.
        if changed or flag_changed:
            await self._broadcast_sync_state(current, reason="buffering")

    async def _handle_chat_message(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        limit = self._chat_rate_limit_count()
        window = self._chat_rate_limit_sec()
        now_mono = time.monotonic()
        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._prune_bucket(actor.chat_timestamps, now_mono - window)
            if len(actor.chat_timestamps) >= limit:
                retry = window - (now_mono - actor.chat_timestamps[0])
                raise RateLimitError(
                    "You are sending messages too quickly",
                    retry_after_sec=max(1, int(math.ceil(retry))),
                )

            raw_message = payload.get("text", payload.get("message", ""))
            text = _clean_control_chars(raw_message)
            if not text:
                raise BadRequestError("Chat message cannot be empty")
            if len(text) > MAX_CHAT_LENGTH:
                raise BadRequestError(
                    f"Chat message cannot exceed {MAX_CHAT_LENGTH} characters"
                )

            actor.chat_timestamps.append(now_mono)
            actor.last_seen_mono = now_mono
            current.last_activity_mono = now_mono
            now_wall = time.time()
            created_ms = int(now_wall * 1000)
            message = {
                "id": current.next_chat_id,
                "participant_id": actor.participant_id,
                "display_name": actor.display_name,
                "role": actor.role,
                "kind": "user",
                "text": text,
                "message": text,
                "ts": created_ms,
                "created_at_ms": created_ms,
                "created_at": now_wall,
                "timestamp": now_wall,
            }
            current.next_chat_id += 1
            current.chat.append(message)

        # The web client switches on ``chat_message`` and formats ``ts`` (ms).
        await self._broadcast(
            room,
            {"type": "chat_message", **message, "chat": message},
        )

    async def _handle_reaction(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        emoji = payload.get("emoji", payload.get("reaction"))
        if not isinstance(emoji, str) or emoji not in ALLOWED_REACTIONS:
            raise BadRequestError("Unsupported reaction")
        now_mono = time.monotonic()
        with self._lock:
            current = self._require_room_locked(room.room_code)
            actor = current.participants.get(participant.participant_id)
            if actor is None or actor.kicked:
                raise AuthError("Participant is not active")
            self._prune_bucket(
                actor.reaction_timestamps,
                now_mono - _REACTION_RATE_WINDOW_SEC,
            )
            if len(actor.reaction_timestamps) >= _REACTION_RATE_COUNT:
                retry = _REACTION_RATE_WINDOW_SEC - (
                    now_mono - actor.reaction_timestamps[0]
                )
                raise RateLimitError(
                    "You are sending reactions too quickly",
                    retry_after_sec=max(1, int(math.ceil(retry))),
                )
            actor.reaction_timestamps.append(now_mono)
            actor.last_seen_mono = now_mono
            current.last_activity_mono = now_mono
            display_name = actor.display_name
            actor_id = actor.participant_id
            actor_role = actor.role

        await self._broadcast(
            room,
            {
                "type": "reaction",
                "emoji": emoji,
                "reaction": emoji,
                "participant_id": actor_id,
                "display_name": display_name,
                "role": actor_role,
                "timestamp": time.time(),
            },
        )

    async def _handle_kick(
        self,
        room: WatchPartyRoom,
        participant: Participant,
        payload: dict[str, Any],
        websocket: Any,
    ) -> None:
        target = payload.get("participant_id", payload.get("target_participant_id"))
        if not isinstance(target, str) or not target:
            raise BadRequestError("A participant_id is required")
        await self.kick_participant(
            room.room_code,
            target,
            actor_participant_id=participant.participant_id,
        )

    # -- moderation -------------------------------------------------------

    async def kick_participant(
        self,
        room_code: str,
        target_participant_id: str,
        *,
        actor_participant_id: str | None = None,
        by_host_api: bool = False,
    ) -> dict[str, Any]:
        target_socket: Any = None
        with self._lock:
            room = self._require_room_locked(room_code)
            if not by_host_api:
                if not actor_participant_id:
                    raise ForbiddenError("Only the host can remove participants")
                actor = room.participants.get(actor_participant_id)
                if actor is None or actor.kicked or actor.role != "host":
                    raise ForbiddenError("Only the host can remove participants")

            target = room.participants.get(target_participant_id)
            if target is None:
                raise BadRequestError("Participant not found")
            if target.role == "host":
                raise BadRequestError("Host participants cannot be removed")
            if target.kicked:
                return {
                    "participant_id": target.participant_id,
                    "display_name": target.display_name,
                    "kicked": True,
                    "room_state": self._serialize_room_state(room),
                }

            target_socket = target.websocket
            target.kicked = True
            target.connected = False
            target.buffering = False
            target.websocket = None
            target.token_digest = b""
            target.last_seen_mono = time.monotonic()
            room.last_activity_mono = target.last_seen_mono
            self._prune_kicked_tombstones_locked(room)
            presence = self._presence_payload(room, "kicked", target)
            target_name = target.display_name

        if target_socket is not None:
            await self._ws_send(
                target_socket,
                {"type": "kicked", "reason": "Removed by host"},
            )
            await self._ws_close(target_socket, WS_CLOSE_KICKED, "Removed by host")
        await self._broadcast(room, presence)

        return {
            "participant_id": target_participant_id,
            "display_name": target_name,
            "kicked": True,
            "room_state": self._serialize_room_state(room),
        }

    # -- media lifecycle --------------------------------------------------

    async def handle_media_deleted(self, media_id: str) -> None:
        if not isinstance(media_id, str) or not media_id:
            return
        changed_rooms: list[WatchPartyRoom] = []
        with self._lock:
            now_wall, now_mono = self._now()
            for room in list(self.rooms.values()):
                if media_id not in room.queue and room.active_media_id != media_id:
                    continue
                queue_changed = False
                if media_id in room.queue:
                    room.queue = [item for item in room.queue if item != media_id]
                    queue_changed = True
                if room.active_media_id == media_id:
                    self._apply_active_media_locked(
                        room,
                        room.queue[0] if room.queue else None,
                        reset_playback=True,
                        now_wall=now_wall,
                        now_mono=now_mono,
                    )
                elif queue_changed:
                    room.state_seq += 1
                room.last_activity_mono = now_mono
                changed_rooms.append(room)

        for room in changed_rooms:
            await self._broadcast(
                room,
                {
                    "type": "sync_state",
                    "reason": "queue_update",
                    "media_id": media_id,
                    "room_state": self._serialize_room_state(room),
                    "state_seq": room.state_seq,
                },
            )

    # -- reaper -----------------------------------------------------------

    def start_reaper(self) -> None:
        if self._reaper_task is not None and not self._reaper_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("Watch party reaper not started: no running event loop")
            return
        self._reaper_task = loop.create_task(
            self._reaper_loop(),
            name="watch-party-reaper",
        )

    async def _reaper_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._reaper_interval_sec())
                await self._reap_once()
        except asyncio.CancelledError:
            raise

    async def _reap_once(self) -> list[str]:
        closed: list[str] = []
        with self._lock:
            expired = self._expired_room_codes()
        for room_code, reason in expired:
            if await self.close_room(room_code, reason=reason):
                closed.append(room_code)
        return closed

    async def stop_reaper(self) -> None:
        task = self._reaper_task
        self._reaper_task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        with self._lock:
            room_codes = list(self.rooms.keys())
        if room_codes:
            await asyncio.gather(
                *(
                    self.close_room(code, reason="Server shutting down")
                    for code in room_codes
                ),
                return_exceptions=True,
            )

    async def shutdown(self) -> None:
        """Convenience alias used by application lifespan hooks."""

        await self.stop_reaper()


__all__ = [
    "ALLOWED_REACTIONS",
    "AuthError",
    "BadRequestError",
    "ForbiddenError",
    "InvalidPinError",
    "MAX_CHAT_LENGTH",
    "MAX_PLAYBACK_RATE",
    "MAX_QUEUE_ITEMS",
    "MAX_WS_MESSAGE_BYTES",
    "MIN_PLAYBACK_RATE",
    "Participant",
    "PinRequiredError",
    "ROOM_CODE_RE",
    "RateLimitError",
    "RoomCapacityError",
    "RoomNotFoundError",
    "WS_CLOSE_KICKED",
    "WS_CLOSE_REPLACED",
    "WS_CLOSE_ROOM_CLOSED",
    "WS_CLOSE_UNAUTHORIZED",
    "WatchPartyDisabledError",
    "WatchPartyError",
    "WatchPartyRoom",
    "WatchPartyService",
    "_serialize_media_item",
]
