from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import html
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any, Iterable, NamedTuple, Sequence
from urllib.parse import quote, unquote

from fastapi import Request
from fastapi.responses import Response

from app.context import AppContext

DAV_NS = "DAV:"
ET.register_namespace("D", DAV_NS)

_WWW_AUTHENTICATE = 'Basic realm="Media Vault", charset="UTF-8"'
_MAX_PROPFIND_BYTES = 65536
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_UNCONFIGURED_MESSAGE = (
    "WebDAV is not configured. Set WEBDAV_PASSWORD, WEB_AUTH_TOKEN, "
    "or WEBDAV_ALLOW_ANONYMOUS=true before using WebDAV."
)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


def _auth_failure_headers() -> dict[str, str]:
    return {
        "WWW-Authenticate": _WWW_AUTHENTICATE,
        "DAV": "1",
        "Cache-Control": "no-store",
    }


def _unauthorized(message: str = "Unauthorized") -> Response:
    return Response(
        status_code=401,
        content=message,
        media_type="text/plain; charset=utf-8",
        headers=_auth_failure_headers(),
    )


def verify_webdav_auth(request: Request, ctx: AppContext) -> Response | None:
    """Return a ready-to-send error Response, or ``None`` when access is allowed."""
    auth_cfg = ctx.settings.get_webdav_auth_config()
    if not auth_cfg.get("enabled"):
        return Response(
            status_code=404,
            content="WebDAV is disabled",
            media_type="text/plain; charset=utf-8",
        )

    auth_mode = str(auth_cfg.get("auth_mode") or "")
    if auth_mode == "anonymous":
        return None
    if auth_mode == "unconfigured":
        # Fail closed: never silently expose a vault that has no credentials.
        return _unauthorized(_UNCONFIGURED_MESSAGE)
    if auth_mode not in ("basic_password", "basic_token"):
        return _unauthorized()

    header = request.headers.get("authorization", "") or ""
    pieces = header.split(None, 1)
    if len(pieces) != 2 or pieces[0].lower() != "basic":
        return _unauthorized()

    try:
        decoded_bytes = base64.b64decode(pieces[1].strip(), validate=True)
        decoded = decoded_bytes.decode("utf-8", errors="strict")
    except (binascii.Error, ValueError, UnicodeDecodeError):
        return _unauthorized()

    provided_user, separator, provided_pass = decoded.partition(":")
    if not separator:
        return _unauthorized()

    expected_user = str(auth_cfg.get("username") or "")
    expected_pass = str(auth_cfg.get("password") or "")
    provided_user_b = provided_user.encode("utf-8")
    provided_pass_b = provided_pass.encode("utf-8")

    if expected_user:
        # Evaluate both comparisons unconditionally so a wrong username cannot
        # be distinguished from a wrong password by response timing.
        user_ok = hmac.compare_digest(provided_user_b, expected_user.encode("utf-8"))
        pass_ok = hmac.compare_digest(provided_pass_b, expected_pass.encode("utf-8"))
        valid = bool(user_ok and pass_ok)
    else:
        valid = hmac.compare_digest(provided_pass_b, expected_pass.encode("utf-8"))

    return None if valid else _unauthorized()


# ---------------------------------------------------------------------------
# Strict virtual-path validation
# ---------------------------------------------------------------------------

_FORBIDDEN_RAW_TOKENS = ("%00", "%2e%2e", "%2f", "%5c", "\x00", "\\")


def _folder_path_segments(raw: str) -> tuple[str, ...]:
    """Split a Media Vault folder string into virtual collection segments."""
    parts: list[str] = []
    for piece in str(raw or "").replace("\\", "/").split("/"):
        part = piece.strip()
        if not part or part in (".", ".."):
            continue
        parts.append(part)
    return tuple(parts)


def _extract_raw_subpath(request: Request) -> str | None:
    """Return the still-percent-encoded path below the ``/webdav`` mount."""
    raw = request.scope.get("raw_path")
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw_bytes = raw
    elif isinstance(raw, str):
        raw_bytes = raw.encode("utf-8")
    else:
        return None

    # Some ASGI servers include the query/fragment in raw_path.
    raw_bytes = raw_bytes.split(b"?", 1)[0].split(b"#", 1)[0]

    root_path = request.scope.get("root_path") or ""
    if isinstance(root_path, bytes):
        root_text = root_path.decode("utf-8", errors="ignore")
    else:
        root_text = str(root_path)
    root_text = root_text.rstrip("/")

    prefixes: list[bytes] = []
    if root_text and not root_text.endswith("/webdav"):
        prefixes.append((root_text + "/webdav").encode("utf-8"))
    if root_text:
        prefixes.append(root_text.encode("utf-8"))
    prefixes.append(b"/webdav")

    for prefix in prefixes:
        if raw_bytes == prefix:
            return ""
        if raw_bytes.startswith(prefix + b"/"):
            return raw_bytes[len(prefix) + 1 :].decode("utf-8", errors="strict")

    # Defensive fallback: if raw_path does not match any expected prefix,
    # fall back to validating the routed subpath parameter.
    return None


def _reject_raw_webdav_text(text: str) -> None:
    lowered = text.casefold()
    if any(token in lowered for token in _FORBIDDEN_RAW_TOKENS):
        raise ValueError("Invalid WebDAV path")


def _decoded_webdav_segments(raw_text: str) -> list[str]:
    segments: list[str] = []
    for raw_segment in raw_text.split("/"):
        if not raw_segment:
            continue
        decoded = unquote(raw_segment, errors="strict")
        if decoded in (".", ".."):
            raise ValueError("Invalid WebDAV path segment")
        if "/" in decoded or "\\" in decoded or "\x00" in decoded:
            raise ValueError("Invalid WebDAV path segment")
        if any(ord(char) < 32 or ord(char) == 127 for char in decoded):
            raise ValueError("Invalid WebDAV path segment")
        segments.append(decoded)
    return segments


def parse_webdav_segments(request: Request, subpath: str) -> list[str]:
    """Validate and normalize the virtual WebDAV path into safe segments.

    Virtual segments are never joined with a filesystem path.
    """
    raw_subpath = _extract_raw_subpath(request)
    if raw_subpath is None:
        fallback = str(subpath or "")
        _reject_raw_webdav_text(fallback)
        return _decoded_webdav_segments(fallback)

    _reject_raw_webdav_text(raw_subpath)
    return _decoded_webdav_segments(raw_subpath)


# ---------------------------------------------------------------------------
# Deterministic virtual filenames and folder entries
# ---------------------------------------------------------------------------

_XML_ILLEGAL_POINTS = frozenset(
    [*range(0x00, 0x09), 0x0B, 0x0C, *range(0x0E, 0x20), 0x7F]
)


def _sanitize_webdav_filename(filename: str, media_id: str) -> str:
    """Make a media filename safe for XML/href usage without hiding its meaning."""
    raw = str(filename or "").replace("/", "_").replace("\\", "_")
    cleaned: list[str] = []
    for char in raw:
        codepoint = ord(char)
        if char in ("\r", "\n"):
            continue
        if char == "\t":
            cleaned.append(" ")
        elif codepoint in _XML_ILLEGAL_POINTS:
            continue
        else:
            cleaned.append(char)
    name = "".join(cleaned)
    if not name or name in (".", ".."):
        return f"{media_id}.bin"
    return name


def _append_media_id(name: str, media_id: str) -> str:
    """Insert ``[media_id]`` before the extension (dotfiles keep their name)."""
    stem, dot, extension = name.rpartition(".")
    if not dot or not stem or not extension:
        return f"{name} [{media_id}]"
    return f"{stem} [{media_id}].{extension}"


@dataclass(frozen=True)
class WebDavEntry:
    item: dict[str, Any]
    name: str

    @property
    def webdav_name(self) -> str:
        return self.name

    @property
    def media_id(self) -> str:
        return str(self.item.get("id") or "")

    def __getitem__(self, key: str) -> Any:
        if key in ("name", "webdav_name", "filename"):
            return self.name
        if key == "item":
            return self.item
        if key == "media_id":
            return self.media_id
        raise KeyError(key)


def build_folder_entries(
    items: list[dict[str, Any]], child_dir_names_ci: Iterable[str]
) -> list[WebDavEntry]:
    """Build collision-free WebDAV filenames for one virtual folder.

    File names are deterministic across requests: duplicates are decorated with
    their media id in the sort order of ``(created_at, id)``.
    """
    child_names = {str(name).casefold() for name in child_dir_names_ci}
    groups: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for item in items:
        media_id = str(item.get("id") or "")
        safe_name = _sanitize_webdav_filename(str(item.get("filename") or ""), media_id)
        groups.setdefault(safe_name.casefold(), []).append((safe_name, item))

    decorated = {
        key for key, group in groups.items() if len(group) > 1 or key in child_names
    }
    used: set[str] = set(child_names)
    used.update(key for key in groups if key not in decorated)

    entries: list[WebDavEntry] = []
    for key in sorted(groups):
        group = sorted(
            groups[key],
            key=lambda pair: (
                str(pair[1].get("created_at") or ""),
                str(pair[1].get("id") or ""),
            ),
        )
        if key not in decorated:
            safe_name, item = group[0]
            entries.append(WebDavEntry(item=item, name=safe_name))
            continue

        for safe_name, item in group:
            media_id = str(item.get("id") or "")
            candidate = _append_media_id(safe_name, media_id)
            suffix = 1
            while candidate.casefold() in used:
                candidate = _append_media_id(safe_name, f"{media_id}-{suffix}")
                suffix += 1
            used.add(candidate.casefold())
            entries.append(WebDavEntry(item=item, name=candidate))

    entries.sort(
        key=lambda entry: (
            entry.name.casefold(),
            entry.name,
            entry.media_id,
        )
    )
    return entries


# ---------------------------------------------------------------------------
# Virtual folder tree and node resolution (SQLite metadata only)
# ---------------------------------------------------------------------------


@dataclass
class _VirtualFolder:
    display: str
    segments: tuple[str, ...]
    children: dict[str, "_VirtualFolder"] = field(default_factory=dict)
    item_count: int = 0
    db_folder: str | None = None
    latest_created_at: str | None = None


@dataclass
class WebDavNode:
    segments: tuple[str, ...]
    is_collection: bool
    folder: _VirtualFolder | None = None
    item: dict[str, Any] | None = None
    webdav_name: str | None = None
    entries: tuple[WebDavEntry, ...] = ()

    @property
    def path_segments(self) -> tuple[str, ...]:
        return self.segments


@dataclass
class WebDavTree:
    ctx: AppContext
    root: _VirtualFolder
    _entries_cache: dict[str, tuple[WebDavEntry, ...]] = field(default_factory=dict)

    def folder_at(self, segments: Sequence[str]) -> _VirtualFolder | None:
        node = self.root
        for raw_segment in segments:
            child = node.children.get(str(raw_segment).casefold())
            if child is None:
                return None
            node = child
        return node

    def entries_for(self, folder: _VirtualFolder) -> tuple[WebDavEntry, ...]:
        if not folder.segments:
            return ()
        key = "/".join(folder.segments)
        cached = self._entries_cache.get(key)
        if cached is None:
            db_folder = folder.db_folder or key
            items = self.ctx.db.list_webdav_folder_items(db_folder)
            cached = tuple(build_folder_entries(items, folder.children.keys()))
            self._entries_cache[key] = cached
        return cached


def build_webdav_tree(ctx: AppContext) -> WebDavTree:
    """Build the virtual collection tree from SQLite folder metadata only."""
    root = _VirtualFolder(display="", segments=())

    def add_folder(folder_path: str, item_count: int, latest: Any) -> None:
        parts = _folder_path_segments(folder_path)
        if not parts:
            return
        node = root
        canonical: list[str] = []
        for part in parts:
            key = part.casefold()
            child = node.children.get(key)
            if child is None:
                child = _VirtualFolder(display=part, segments=tuple([*canonical, part]))
                node.children[key] = child
            canonical.append(child.display)
            node = child
        node.item_count += max(0, int(item_count or 0))
        if node.db_folder is None:
            node.db_folder = str(folder_path).strip().strip("/") or "/".join(canonical)
        latest_text = str(latest) if latest else ""
        if latest_text and (
            node.latest_created_at is None or latest_text > node.latest_created_at
        ):
            node.latest_created_at = latest_text

    default_folder = "/".join(
        _folder_path_segments(str(getattr(ctx.settings, "default_folder", "") or "General"))
    )
    if default_folder:
        add_folder(default_folder, 0, None)

    for stat in ctx.db.list_webdav_folder_stats():
        add_folder(
            str(stat.get("folder") or ""),
            int(stat.get("item_count") or 0),
            stat.get("latest_created_at"),
        )

    def propagate(node: _VirtualFolder) -> str | None:
        best = node.latest_created_at
        for child in node.children.values():
            child_latest = propagate(child)
            if child_latest and (best is None or child_latest > best):
                best = child_latest
        node.latest_created_at = best
        return best

    propagate(root)
    return WebDavTree(ctx=ctx, root=root)


def resolve_webdav_node(
    ctx: AppContext,
    segments: Sequence[str],
    tree: WebDavTree | None = None,
) -> WebDavNode | None:
    """Resolve a validated virtual path to a collection or a file node."""
    active_tree = tree if tree is not None else build_webdav_tree(ctx)
    normalized = tuple(str(segment) for segment in segments)

    folder = active_tree.folder_at(normalized)
    if folder is not None:
        return WebDavNode(
            segments=folder.segments,
            is_collection=True,
            folder=folder,
            entries=active_tree.entries_for(folder),
        )

    if not normalized:
        return None

    parent = active_tree.folder_at(normalized[:-1])
    if parent is None:
        return None

    entries = active_tree.entries_for(parent)
    requested = normalized[-1]
    match = next((entry for entry in entries if entry.name == requested), None)
    if match is None:
        folded = requested.casefold()
        match = next(
            (entry for entry in entries if entry.name.casefold() == folded), None
        )
    if match is None:
        return None

    return WebDavNode(
        segments=(*parent.segments, match.name),
        is_collection=False,
        item=match.item,
        webdav_name=match.name,
    )


# ---------------------------------------------------------------------------
# Dates and ETags
# ---------------------------------------------------------------------------


def webdav_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return _EPOCH
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return _EPOCH
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def webdav_last_modified(node: WebDavNode) -> str:
    raw = (
        (node.folder.latest_created_at if node.folder is not None else None)
        if node.is_collection
        else (node.item or {}).get("created_at")
    )
    return format_datetime(webdav_datetime(raw), usegmt=True)


def webdav_creation_date(node: WebDavNode) -> str:
    raw = (
        (node.folder.latest_created_at if node.folder is not None else None)
        if node.is_collection
        else (node.item or {}).get("created_at")
    )
    return webdav_datetime(raw).strftime("%Y-%m-%dT%H:%M:%SZ")


def webdav_etag(node: WebDavNode) -> str:
    """Stable quoted ETag derived from immutable SQLite metadata."""
    if node.is_collection:
        folder = node.folder
        folder_path = "/".join(folder.segments) if folder is not None else ""
        child_count = len(folder.children) if folder is not None else 0
        file_count = folder.item_count if folder is not None else len(node.entries)
        latest = (folder.latest_created_at if folder is not None else None) or ""
        fingerprint = f"{folder_path}:{child_count}:{file_count}:{latest}"
    else:
        item = node.item or {}
        fingerprint = ":".join(
            str(part or "")
            for part in (
                item.get("id"),
                item.get("size_bytes"),
                item.get("created_at"),
                item.get("sha256"),
            )
        )
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return f'"{digest}"'


node_etag = webdav_etag


def etag_matches(header_value: str | None, etag: str) -> bool:
    if not header_value:
        return False
    normalized = etag.strip()
    for candidate in header_value.split(","):
        candidate = candidate.strip()
        if candidate.startswith("W/"):
            candidate = candidate[2:].strip()
        if candidate == "*" or candidate == normalized:
            return True
    return False


# ---------------------------------------------------------------------------
# PROPFIND XML (pure stdlib)
# ---------------------------------------------------------------------------


class PropfindRequest(NamedTuple):
    mode: str
    props: tuple[tuple[str, str], ...]


def _tag_namespace(tag: str) -> str:
    return tag[1:].split("}", 1)[0] if tag.startswith("{") else ""


def _tag_localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _qualified_name(name: tuple[str, str]) -> str:
    namespace, local = name
    return f"{{{namespace}}}{local}" if namespace else local


def _xml_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return "".join(char for char in text if ord(char) in (9, 10, 13) or 0x20 <= ord(char) <= 0xD7FF or 0xE000 <= ord(char) <= 0xFFFD or 0x10000 <= ord(char) <= 0x10FFFF)


def parse_propfind_request(body: bytes) -> PropfindRequest:
    raw = bytes(body or b"")
    if len(raw) > _MAX_PROPFIND_BYTES:
        raise ValueError("PROPFIND request body is too large")
    if not raw.strip():
        return PropfindRequest(mode="allprop", props=())

    upper = raw.upper()
    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
        raise ValueError("PROPFIND DTDs are not allowed")

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise ValueError("Malformed PROPFIND XML") from exc

    if _tag_localname(root.tag) != "propfind" or _tag_namespace(root.tag) != DAV_NS:
        raise ValueError("Expected a DAV:propfind root element")

    children = list(root)
    if any(child.tag == f"{{{DAV_NS}}}propname" for child in children):
        return PropfindRequest(mode="propname", props=())
    prop_element = next(
        (child for child in children if child.tag == f"{{{DAV_NS}}}prop"), None
    )
    if prop_element is not None:
        props: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for child in prop_element:
            name = (_tag_namespace(child.tag), _tag_localname(child.tag))
            if name[1] and name not in seen:
                seen.add(name)
                props.append(name)
        return PropfindRequest(mode="prop", props=tuple(props))
    if any(child.tag == f"{{{DAV_NS}}}allprop" for child in children):
        return PropfindRequest(mode="allprop", props=())
    raise ValueError("Unsupported PROPFIND request body")


def _webdav_href(
    segments: Sequence[str], is_collection: bool, base_href_prefix: str
) -> str:
    prefix = str(base_href_prefix or "/webdav").rstrip("/")
    if prefix and not prefix.startswith("/"):
        prefix = "/" + prefix
    href = prefix + "/" + "/".join(quote(str(segment), safe="") for segment in segments)
    if is_collection and not href.endswith("/"):
        href += "/"
    return href


def _segments_from_href(href: str) -> tuple[tuple[str, ...], bool]:
    from urllib.parse import urlsplit

    path = unquote(urlsplit(str(href)).path)
    is_collection = path.endswith("/")
    segments = tuple(part for part in path.strip("/").split("/") if part)
    if segments and segments[0].casefold() == "webdav":
        segments = segments[1:]
    return segments, is_collection


def _as_webdav_node(resource: Any) -> WebDavNode:
    if isinstance(resource, WebDavNode):
        return resource

    if isinstance(resource, dict):
        segments = resource.get("segments") or resource.get("path_segments") or ()
        is_collection = bool(
            resource.get("is_collection", resource.get("collection", True))
        )
        if not segments and resource.get("href"):
            href_segments, href_is_collection = _segments_from_href(resource["href"])
            segments, is_collection = href_segments, href_is_collection
        return WebDavNode(
            segments=tuple(str(segment) for segment in segments),
            is_collection=bool(resource.get("is_dir", is_collection)),
            folder=resource.get("folder"),
            item=resource.get("item"),
            webdav_name=resource.get("webdav_name"),
            entries=tuple(resource.get("entries") or ()),
        )

    segments = getattr(resource, "segments", None)
    if segments is None:
        segments = getattr(resource, "path_segments", ())
    return WebDavNode(
        segments=tuple(str(segment) for segment in segments),
        is_collection=bool(
            getattr(resource, "is_collection", getattr(resource, "collection", True))
        ),
        folder=getattr(resource, "folder", None),
        item=getattr(resource, "item", None),
        webdav_name=getattr(resource, "webdav_name", None),
        entries=tuple(getattr(resource, "entries", ()) or ()),
    )


def _available_properties(node: WebDavNode) -> dict[tuple[str, str], str | None]:
    name = (
        (node.webdav_name or (node.segments[-1] if node.segments else ""))
        if not node.is_collection
        else (node.segments[-1] if node.segments else "Media Vault")
    )
    props: dict[tuple[str, str], str | None] = {
        (DAV_NS, "displayname"): name,
        (DAV_NS, "resourcetype"): None,
        (DAV_NS, "getlastmodified"): webdav_last_modified(node),
        (DAV_NS, "creationdate"): webdav_creation_date(node),
        (DAV_NS, "getetag"): webdav_etag(node),
    }
    if not node.is_collection:
        item = node.item or {}
        props[(DAV_NS, "getcontentlength")] = str(int(item.get("size_bytes") or 0))
        props[(DAV_NS, "getcontenttype")] = str(
            item.get("mime_type") or "application/octet-stream"
        )
    return props


def _append_property_element(
    parent: ET.Element,
    name: tuple[str, str],
    value: str | None,
    *,
    is_collection: bool,
    include_value: bool,
) -> None:
    element = ET.SubElement(parent, _qualified_name(name))
    if not include_value:
        return
    if name == (DAV_NS, "resourcetype") and is_collection:
        ET.SubElement(element, f"{{{DAV_NS}}}collection")
    elif value is not None:
        element.text = _xml_text(value)


def _append_propstat(
    response: ET.Element,
    names: Sequence[tuple[str, str]],
    available: dict[tuple[str, str], str | None],
    *,
    is_collection: bool,
    status: str,
    include_values: bool = True,
) -> None:
    propstat = ET.SubElement(response, f"{{{DAV_NS}}}propstat")
    prop = ET.SubElement(propstat, f"{{{DAV_NS}}}prop")
    for name in names:
        _append_property_element(
            prop,
            name,
            available.get(name),
            is_collection=is_collection,
            include_value=include_values,
        )
    ET.SubElement(propstat, f"{{{DAV_NS}}}status").text = status


def build_multistatus_xml(
    resources: Iterable[Any],
    prop_mode: str,
    requested_props: Sequence[tuple[str, str]] = (),
    base_href_prefix: str = "/webdav",
) -> bytes:
    root = ET.Element(f"{{{DAV_NS}}}multistatus")
    requested = tuple(requested_props or ())

    for resource in resources:
        node = _as_webdav_node(resource)
        response = ET.SubElement(root, f"{{{DAV_NS}}}response")
        ET.SubElement(response, f"{{{DAV_NS}}}href").text = _xml_text(
            _webdav_href(node.segments, node.is_collection, base_href_prefix)
        )
        available = _available_properties(node)

        if prop_mode == "propname":
            _append_propstat(
                response,
                tuple(available),
                available,
                is_collection=node.is_collection,
                status="HTTP/1.1 200 OK",
                include_values=False,
            )
        elif prop_mode == "prop":
            found = [name for name in requested if name in available]
            missing = [name for name in requested if name not in available]
            if found:
                _append_propstat(
                    response,
                    found,
                    available,
                    is_collection=node.is_collection,
                    status="HTTP/1.1 200 OK",
                )
            if missing:
                _append_propstat(
                    response,
                    missing,
                    available,
                    is_collection=node.is_collection,
                    status="HTTP/1.1 404 Not Found",
                    include_values=False,
                )
            if not found and not missing:
                _append_propstat(
                    response,
                    (),
                    available,
                    is_collection=node.is_collection,
                    status="HTTP/1.1 200 OK",
                )
        else:
            _append_propstat(
                response,
                tuple(available),
                available,
                is_collection=node.is_collection,
                status="HTTP/1.1 200 OK",
            )

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def build_finite_depth_error_xml() -> bytes:
    return (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<D:error xmlns:D="DAV:"><D:propfind-finite-depth/></D:error>'
    )


# ---------------------------------------------------------------------------
# HTML collection index
# ---------------------------------------------------------------------------


def _html_href(
    segments: Sequence[str], is_collection: bool, base_href_prefix: str
) -> str:
    return _webdav_href(segments, is_collection, base_href_prefix)


def _html_escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _format_size(value: Any) -> str:
    try:
        size = int(value or 0)
    except (TypeError, ValueError):
        return "-"
    if size < 1024:
        return f"{size} B"
    units = ("KiB", "MiB", "GiB", "TiB")
    scaled = float(size)
    for unit in units:
        scaled /= 1024.0
        if scaled < 1024.0:
            return f"{scaled:.1f} {unit}"
    return f"{scaled:.1f} PiB"


def render_collection_html(
    node: WebDavNode, *, base_href_prefix: str = "/webdav"
) -> str:
    """Render a minimal, fully escaped, read-only directory index."""
    escaped = _html_escape
    title = "/" + "/".join(node.segments)
    rows: list[str] = []

    if node.segments:
        parent_href = _html_href(node.segments[:-1], True, base_href_prefix)
        rows.append(
            f'<li class="dir"><a href="{escaped(parent_href)}">../</a></li>'
        )

    child_folders = sorted(
        (node.folder.children.values() if node.folder is not None else ()),
        key=lambda folder: (folder.display.casefold(), folder.display),
    )
    for folder in child_folders:
        href = _html_href(folder.segments, True, base_href_prefix)
        rows.append(
            f'<li class="dir"><a href="{escaped(href)}">'
            f'{escaped(folder.display)}/</a></li>'
        )

    for entry in node.entries:
        href = _html_href((*node.segments, entry.name), False, base_href_prefix)
        size = _format_size((entry.item or {}).get("size_bytes"))
        rows.append(
            f'<li class="file"><a href="{escaped(href)}">'
            f"{escaped(entry.name)}</a>"
            f'<span class="size">{escaped(size)}</span></li>'
        )

    listing = "\n".join(rows)
    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{escaped(title)}</title>\n"
        "<style>"
        "body{font:15px/1.5 system-ui,sans-serif;margin:2rem auto;max-width:60rem;"
        "padding:0 1rem;color:#222}header{border-bottom:1px solid #ddd;margin-bottom:1rem}"
        ".hint{color:#666;font-size:.85rem}ul{list-style:none;padding:0}"
        "li{padding:.25rem 0;display:flex;justify-content:space-between}"
        "a{color:#0645ad;text-decoration:none}a:hover{text-decoration:underline}"
        ".size{color:#666;font-variant-numeric:tabular-nums}"
        "</style>\n"
        "</head>\n"
        "<body>\n"
        "<header><h1>"
        f"Index of {escaped(title)}"
        "</h1><p class=\"hint\">Media Vault WebDAV - read-only</p></header>\n"
        f"<ul>\n{listing}\n</ul>\n"
        "</body>\n"
        "</html>\n"
    )


# ---------------------------------------------------------------------------
# M3U playlist
# ---------------------------------------------------------------------------


def _m3u_title(item: dict[str, Any]) -> str:
    raw = str(item.get("title") or item.get("filename") or "Untitled")
    cleaned = "".join(
        " " if ch in ("\r", "\n", "\t") else ch
        for ch in raw
        if ch in ("\r", "\n", "\t") or (ord(ch) >= 32 and ord(ch) != 127)
    ).strip()
    return cleaned or "Untitled"


def _m3u_duration(item: dict[str, Any]) -> int:
    try:
        duration = int(float(item.get("duration_sec") or 0))
    except (TypeError, ValueError):
        duration = 0
    return duration if duration > 0 else -1


def _webdav_base_url(request: Request) -> str:
    # Starlette's request.base_url already includes scope["root_path"].
    base = str(request.base_url).rstrip("/")
    if base.endswith("/webdav"):
        return base
    return f"{base}/webdav"


def build_m3u_playlist(
    ctx: AppContext,
    request: Request,
    *,
    folder: str | None = None,
    media_type: str | None = None,
    tag: str | None = None,
) -> str:
    """Build an M3U whose URLs match the exact WebDAV virtual filenames."""
    tree = build_webdav_tree(ctx)
    items = ctx.db.list_webdav_playlist_items(
        folder=folder, media_type=media_type, tag=tag
    )

    groups: dict[tuple[str, ...], tuple[tuple[str, ...], list[dict[str, Any]]]] = {}
    for item in items:
        raw_folder = str(item.get("folder") or "")
        raw_segments = _folder_path_segments(raw_folder)
        matched = tree.folder_at(raw_segments)
        canonical = matched.segments if matched is not None else raw_segments
        key = tuple(segment.casefold() for segment in canonical)
        if key not in groups:
            groups[key] = (canonical, [])
        groups[key][1].append(item)

    base_url = _webdav_base_url(request)
    lines = ["#EXTM3U"]
    for key in sorted(groups):
        segments, folder_items = groups[key]
        matched = tree.folder_at(segments)
        if matched is not None:
            wanted_ids = {str(it.get("id") or "") for it in folder_items}
            entries = [
                entry
                for entry in tree.entries_for(matched)
                if entry.media_id in wanted_ids
            ]
        else:
            entries = build_folder_entries(folder_items, ())
        for entry in entries:
            href_segments = (*segments, entry.name)
            url = base_url + "/" + "/".join(
                quote(segment, safe="") for segment in href_segments
            )
            lines.append(f"#EXTINF:{_m3u_duration(entry.item)},{_m3u_title(entry.item)}")
            lines.append(url)
    return "\n".join(lines) + "\n"
