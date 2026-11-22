"""Channel / playlist / RSS watch subscriptions.

This module owns feed discovery (safe RSS/Atom parsing plus a yt-dlp fallback),
SSRF hardening for outbound fetches, and the polling state machine that turns
new feed entries into regular ingest jobs.
"""

from __future__ import annotations

import asyncio
import inspect
import ipaddress
import logging
import re
import socket
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urljoin, urlsplit
from xml.parsers import expat

try:
    from re import _parser as _sre_parse  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - fallback for older Python
    import sre_parse as _sre_parse  # type: ignore[no-redef]

import httpx

from app.allocator import sanitize_folder
from app.config import Settings
from app.db import Database, normalize_source_url, normalize_tags, utc_now_iso
from app.ingest import IngestService, expand_ytdlp_playlist, resolve_format_spec

logger = logging.getLogger(__name__)

MAX_XML_ELEMENTS = 50_000
MAX_URL_LENGTH = 2048
MAX_REGEX_LENGTH = 256
MAX_REGEX_TITLE_LENGTH = 512
FEED_USER_AGENT = "MediaVault-Subscriptions/1.0"
FEED_ACCEPT = (
    "application/atom+xml, application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.1"
)
_CREDENTIAL_URL_RE = re.compile(
    r"((?:https?|socks5h?|socks4a?)://)[^@\s/]+@", re.IGNORECASE
)
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_SOURCE_KINDS = frozenset({"auto", "rss", "ytdlp"})
_LOCAL_HOSTNAMES = frozenset(
    {"localhost", "ip6-localhost", "ip6-loopback", "localhost.localdomain"}
)
_LOCAL_SUFFIXES = (".localhost", ".local", ".internal")
_YOUTUBE_HOSTS = frozenset({"youtube.com", "www.youtube.com", "m.youtube.com"})
_YOUTUBE_TAB_SEGMENTS = frozenset({"videos", "shorts", "streams", "featured"})
_MEDIA_PREFERRED_TYPES = ("video/", "audio/", "image/")


def _redact_credentials(text: str) -> str:
    return _CREDENTIAL_URL_RE.sub(r"\1***@", str(text or ""))


class SubscriptionError(Exception):
    """Base error for subscription operations."""


class SubscriptionNotFoundError(SubscriptionError):
    """Raised when a subscription id does not exist."""


class SubscriptionBusyError(SubscriptionError):
    """Raised when a subscription check is already running."""


class SubscriptionValidationError(SubscriptionError):
    """Raised when user supplied subscription input is invalid."""


class SubscriptionFetchError(SubscriptionError):
    """Raised when a feed or playlist could not be fetched or parsed."""


# ---------------------------------------------------------------------------
# URL / regex / SSRF validation
# ---------------------------------------------------------------------------


def validate_subscription_url(url: str) -> str:
    cleaned = str(url or "").strip()
    if not cleaned:
        raise SubscriptionValidationError("Subscription URL is required")
    if len(cleaned) > MAX_URL_LENGTH:
        raise SubscriptionValidationError(
            f"Subscription URL is too long (max {MAX_URL_LENGTH} characters)"
        )
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in cleaned):
        raise SubscriptionValidationError(
            "Subscription URL contains invalid control characters"
        )
    try:
        parts = urlsplit(cleaned)
    except ValueError as exc:
        raise SubscriptionValidationError(f"Invalid subscription URL: {exc}") from exc
    if parts.scheme.lower() not in ("http", "https"):
        raise SubscriptionValidationError("Subscription URL must use http or https")
    try:
        hostname = parts.hostname
    except ValueError as exc:
        raise SubscriptionValidationError(f"Invalid subscription URL: {exc}") from exc
    if not hostname:
        raise SubscriptionValidationError("Subscription URL must include a hostname")
    if parts.username is not None or parts.password is not None:
        raise SubscriptionValidationError(
            "Subscription URL must not include embedded credentials"
        )
    return cleaned


def _contains_nested_repeat_or_branch(nodes: Any) -> bool:
    for op, av in nodes:
        if op in (_sre_parse.MAX_REPEAT, _sre_parse.MIN_REPEAT, _sre_parse.BRANCH, _sre_parse.GROUPREF):
            return True
        if op == _sre_parse.SUBPATTERN:
            if _contains_nested_repeat_or_branch(av[3]):
                return True
        elif op in (_sre_parse.ASSERT, _sre_parse.ASSERT_NOT):
            if _contains_nested_repeat_or_branch(av[1]):
                return True
    return False


def _is_unsafe_regex(pattern: str) -> bool:
    parsed = _sre_parse.parse(pattern)

    def _walk(nodes: Any) -> bool:
        for op, av in nodes:
            if op in (_sre_parse.MAX_REPEAT, _sre_parse.MIN_REPEAT):
                _min_r, max_r, sub = av
                if (
                    max_r == _sre_parse.MAXREPEAT or max_r > 10
                ) and _contains_nested_repeat_or_branch(sub):
                    return True
                if _walk(sub):
                    return True
            elif op == _sre_parse.SUBPATTERN:
                if _walk(av[3]):
                    return True
            elif op == _sre_parse.BRANCH:
                for branch in av[1]:
                    if _walk(branch):
                        return True
            elif op in (_sre_parse.ASSERT, _sre_parse.ASSERT_NOT):
                if _walk(av[1]):
                    return True
        return False

    return _walk(parsed)


def validate_regex_pattern(pattern: str | None, field_name: str = "regex") -> str | None:
    if pattern is None:
        return None
    cleaned = str(pattern).strip()
    if not cleaned:
        return None
    if len(cleaned) > MAX_REGEX_LENGTH:
        raise SubscriptionValidationError(
            f"Invalid {field_name}: pattern is too long "
            f"(max {MAX_REGEX_LENGTH} characters)"
        )
    try:
        if _is_unsafe_regex(cleaned):
            raise SubscriptionValidationError(
                f"Invalid {field_name}: nested quantifiers or repeated alternations "
                "are not allowed (ReDoS protection)"
            )
        re.compile(cleaned, re.IGNORECASE)
    except SubscriptionValidationError:
        raise
    except re.error as exc:
        raise SubscriptionValidationError(f"Invalid {field_name}: {exc}") from exc
    return cleaned


def _is_ip_disallowed(ip_str: str) -> bool:
    raw = str(ip_str or "").strip()
    if not raw:
        return True
    # Strip an IPv6 scope id ("fe80::1%eth0") before parsing.
    raw = raw.split("%", 1)[0]
    try:
        address = ipaddress.ip_address(raw)
    except ValueError:
        return True
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return bool(
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    )


def _is_local_hostname(host: str) -> bool:
    cleaned = host.strip().lower().rstrip(".")
    if not cleaned:
        return True
    if cleaned in _LOCAL_HOSTNAMES:
        return True
    return any(cleaned.endswith(suffix) for suffix in _LOCAL_SUFFIXES)


async def assert_safe_public_url(url: str, *, allow_private: bool = False) -> str:
    validated = validate_subscription_url(url)
    if allow_private:
        return validated

    parts = urlsplit(validated)
    host = (parts.hostname or "").strip().lower().rstrip(".")
    if _is_local_hostname(host):
        raise SubscriptionValidationError(
            "Target host is local or restricted"
        )

    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        if _is_ip_disallowed(host):
            raise SubscriptionValidationError(
                "Target host resolves to a private or restricted IP address"
            )
        return validated

    try:
        port = parts.port or (443 if parts.scheme.lower() == "https" else 80)
    except ValueError as exc:
        raise SubscriptionValidationError(f"Invalid subscription URL: {exc}") from exc

    try:
        infos = await asyncio.to_thread(
            socket.getaddrinfo, host, port, type=socket.SOCK_STREAM
        )
    except (OSError, UnicodeError) as exc:
        raise SubscriptionFetchError(f"Could not resolve host {host}: {exc}") from exc

    addresses = [str(info[4][0]) for info in infos if info and info[4]]
    if not addresses:
        raise SubscriptionFetchError(f"Could not resolve host {host}: no addresses")
    if any(_is_ip_disallowed(addr) for addr in addresses):
        raise SubscriptionValidationError(
            "Target host resolves to a private or restricted IP address"
        )
    return validated


# ---------------------------------------------------------------------------
# Feed fetching
# ---------------------------------------------------------------------------


def _http_proxy_value(proxy: str) -> str | None:
    clean = str(proxy or "").strip()
    if clean.startswith(("http://", "https://")):
        return clean
    return None


async def fetch_feed_bytes(
    url: str,
    *,
    timeout_sec: float = 20.0,
    max_bytes: int = 5 * 1024 * 1024,
    proxy: str = "",
    allow_private: bool = False,
    max_redirects: int = 5,
) -> tuple[bytes, str, str]:
    """Fetch a feed, following redirects manually with SSRF re-validation."""

    current_url = await assert_safe_public_url(url, allow_private=allow_private)
    byte_limit = max(1, int(max_bytes))
    timeout = max(1.0, float(timeout_sec))
    redirects_left = max(0, int(max_redirects))

    try:
        async with httpx.AsyncClient(
            follow_redirects=False,
            timeout=timeout,
            proxy=_http_proxy_value(proxy),
            headers={"User-Agent": FEED_USER_AGENT, "Accept": FEED_ACCEPT},
        ) as client:
            while True:
                async with client.stream("GET", current_url) as response:
                    if response.status_code in _REDIRECT_STATUSES:
                        location = (response.headers.get("location") or "").strip()
                        if not location:
                            raise SubscriptionFetchError(
                                f"HTTP {response.status_code} redirect without Location"
                            )
                        if redirects_left <= 0:
                            raise SubscriptionFetchError(
                                "Too many redirects while fetching feed"
                            )
                        redirects_left -= 1
                        next_url = urljoin(current_url, location)
                        current_url = await assert_safe_public_url(
                            next_url, allow_private=allow_private
                        )
                        continue

                    if response.status_code >= 400:
                        raise SubscriptionFetchError(
                            f"Feed request failed with HTTP {response.status_code}"
                        )
                    if response.status_code >= 300:
                        raise SubscriptionFetchError(
                            f"Unexpected HTTP {response.status_code} while fetching feed"
                        )

                    content_type = (response.headers.get("content-type") or "").strip()
                    declared = (response.headers.get("content-length") or "").strip()
                    if declared:
                        try:
                            if int(declared) > byte_limit:
                                raise SubscriptionFetchError(
                                    f"Feed exceeds maximum size of {byte_limit} bytes"
                                )
                        except ValueError:
                            pass

                    chunks = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        if not chunk:
                            continue
                        chunks.extend(chunk)
                        if len(chunks) > byte_limit:
                            raise SubscriptionFetchError(
                                f"Feed exceeds maximum size of {byte_limit} bytes"
                            )
                    return bytes(chunks), str(response.url or current_url), content_type
    except SubscriptionError:
        raise
    except (httpx.HTTPError, httpx.InvalidURL) as exc:
        raise SubscriptionFetchError(
            _redact_credentials(f"Failed to fetch feed {current_url}: {exc}")
        ) from exc


# ---------------------------------------------------------------------------
# Safe XML parsing (expat + TreeBuilder, no DTD/entities)
# ---------------------------------------------------------------------------


def _expat_name_to_tag(name: str) -> str:
    text = str(name or "")
    if " " in text:
        namespace, local = text.split(" ", 1)
        return f"{{{namespace}}}{local}" if namespace else local
    return text


def _parse_xml_root(raw: bytes) -> ET.Element:
    parser = expat.ParserCreate(namespace_separator=" ")

    def _forbid(*_args: Any) -> None:
        raise SubscriptionFetchError(
            "XML DTD/ENTITY declarations are forbidden in feeds"
        )

    parser.StartDoctypeDeclHandler = _forbid
    parser.EntityDeclHandler = _forbid
    parser.UnparsedEntityDeclHandler = _forbid
    parser.NotationDeclHandler = _forbid
    parser.ExternalEntityRefHandler = lambda *_args: 0

    builder = ET.TreeBuilder()
    element_count = 0

    def _start(name: str, attrs: dict[str, str]) -> None:
        nonlocal element_count
        element_count += 1
        if element_count > MAX_XML_ELEMENTS:
            raise SubscriptionFetchError(
                f"Feed XML contains more than {MAX_XML_ELEMENTS} elements"
            )
        builder.start(
            _expat_name_to_tag(name),
            {_expat_name_to_tag(key): value for key, value in attrs.items()},
        )

    def _end(name: str) -> None:
        builder.end(_expat_name_to_tag(name))

    parser.StartElementHandler = _start
    parser.EndElementHandler = _end
    parser.CharacterDataHandler = builder.data

    try:
        parser.Parse(raw, True)
        root = builder.close()
    except SubscriptionFetchError:
        raise
    except (expat.ExpatError, ValueError, LookupError, UnicodeError, IndexError) as exc:
        raise SubscriptionFetchError(f"Invalid XML feed: {exc}") from exc

    if root is None:
        raise SubscriptionFetchError("Feed XML is empty")
    return root


def _local_name(tag: Any) -> str:
    text = str(tag or "")
    if text.startswith("{"):
        return text.rsplit("}", 1)[-1]
    return text


def _namespace_uri(tag: Any) -> str:
    text = str(tag or "")
    if text.startswith("{") and "}" in text:
        return text[1 : text.index("}")]
    return ""


def _find_children(element: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in list(element) if _local_name(child.tag) == name]


def _first_child(element: ET.Element | None, name: str) -> ET.Element | None:
    if element is None:
        return None
    for child in list(element):
        if _local_name(child.tag) == name:
            return child
    return None


def _element_text(element: ET.Element | None) -> str:
    if element is None:
        return ""
    return "".join(element.itertext()).strip()


def _get_attr(element: ET.Element, name: str) -> str | None:
    direct = element.get(name)
    if direct is not None:
        return direct
    for key, value in element.attrib.items():
        if _local_name(key) == name:
            return value
    return None


def _parse_feed_datetime(raw: str) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    iso_candidate = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        parsed = None
    if parsed is None:
        try:
            parsed = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError):
            return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _extract_published_at(item: ET.Element) -> str | None:
    for name in ("published", "updated", "pubDate"):
        element = _first_child(item, name)
        if element is None:
            continue
        parsed = _parse_feed_datetime(_element_text(element))
        if parsed:
            return parsed
    for child in list(item):
        if _local_name(child.tag).lower() != "date":
            continue
        namespace = _namespace_uri(child.tag).lower()
        if namespace and "purl.org/dc/elements/1.1" not in namespace:
            continue
        parsed = _parse_feed_datetime(_element_text(child))
        if parsed:
            return parsed
    return None


def _resolve_candidate(candidate: str | None, base_url: str | None) -> str | None:
    text = str(candidate or "").strip()
    if not text:
        return None
    if base_url:
        try:
            text = urljoin(base_url, text)
        except ValueError:
            return None
    if text.startswith(("http://", "https://")):
        return text
    return None


def _is_legacy_swf_embed(url: str, content_type: str) -> bool:
    if content_type and "shockwave-flash" in content_type.lower():
        return True
    try:
        parts = urlsplit(url)
    except ValueError:
        return True
    host = (parts.hostname or "").lower()
    is_youtube = (
        host == "youtube.com"
        or host.endswith(".youtube.com")
        or host == "youtu.be"
    )
    if is_youtube:
        segments = [segment for segment in parts.path.split("/") if segment]
        if len(segments) >= 2 and segments[0] == "v":
            return True
    return False


def _pick_enclosure_url(item: ET.Element, base_url: str | None) -> str | None:
    candidates: list[tuple[str, str]] = []
    for enclosure in _find_children(item, "enclosure"):
        resolved = _resolve_candidate(_get_attr(enclosure, "url"), base_url)
        if not resolved:
            continue
        content_type = (_get_attr(enclosure, "type") or "").strip().lower()
        candidates.append((resolved, content_type))
    for url, content_type in candidates:
        if content_type.startswith(_MEDIA_PREFERRED_TYPES):
            return url
    return candidates[0][0] if candidates else None


def _collect_media_contents(item: ET.Element) -> list[ET.Element]:
    contents: list[ET.Element] = []
    for child in list(item):
        local = _local_name(child.tag)
        if local == "content":
            if _get_attr(child, "url"):
                contents.append(child)
        elif local == "group":
            for nested in list(child):
                if _local_name(nested.tag) == "content" and _get_attr(nested, "url"):
                    contents.append(nested)
    return contents


def _pick_media_content_url(item: ET.Element, base_url: str | None) -> str | None:
    for content in _collect_media_contents(item):
        resolved = _resolve_candidate(_get_attr(content, "url"), base_url)
        if not resolved:
            continue
        content_type = (_get_attr(content, "type") or "").strip().lower()
        if _is_legacy_swf_embed(resolved, content_type):
            continue
        return resolved
    return None


def _pick_atom_link_url(item: ET.Element, base_url: str | None) -> str | None:
    resolved: list[tuple[str, str]] = []
    for link in _find_children(item, "link"):
        href = _resolve_candidate(_get_attr(link, "href"), base_url)
        if not href:
            continue
        rel = (_get_attr(link, "rel") or "").strip().lower()
        resolved.append((rel, href))
    for rel, href in resolved:
        if rel == "enclosure":
            return href
    for rel, href in resolved:
        if rel == "alternate":
            return href
    for rel, href in resolved:
        if not rel:
            return href
    return None


def _pick_rss_link_url(item: ET.Element, base_url: str | None) -> str | None:
    link = _first_child(item, "link")
    if link is None:
        return None
    candidate = (_get_attr(link, "href") or "").strip() or _element_text(link)
    return _resolve_candidate(candidate, base_url)


def _parse_feed_entry(
    item: ET.Element, *, base_url: str | None
) -> dict[str, Any] | None:
    video_id = _element_text(_first_child(item, "videoId"))

    if video_id:
        url = f"https://www.youtube.com/watch?v={video_id}"
    else:
        url = (
            _pick_enclosure_url(item, base_url)
            or _pick_media_content_url(item, base_url)
            or _pick_atom_link_url(item, base_url)
            or _pick_rss_link_url(item, base_url)
            or ""
        )
    if not url:
        return None

    title = _element_text(_first_child(item, "title")) or url

    guid = _element_text(_first_child(item, "guid"))
    if not guid:
        guid = _element_text(_first_child(item, "id"))

    normalized_url = normalize_source_url(url)
    if video_id:
        entry_key = normalized_url or f"yt:{video_id}"
    elif guid:
        entry_key = f"guid:{guid.strip()[:512]}"
    else:
        entry_key = normalized_url or url

    return {
        "entry_key": entry_key,
        "title": title,
        "url": url,
        "normalized_url": normalized_url,
        "published_at": _extract_published_at(item),
    }


def parse_feed_xml(
    raw: bytes | bytearray | memoryview | str, *, base_url: str | None = None
) -> dict[str, Any]:
    """Parse RSS 2.0, RSS 1.0 (RDF) or Atom 1.0 without DTD/entity support."""

    data = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
    root = _parse_xml_root(data)
    root_name = _local_name(root.tag)
    lowered = root_name.lower()

    feed_title = ""
    if lowered == "rss":
        channel = _first_child(root, "channel")
        container = channel if channel is not None else root
        feed_title = _element_text(_first_child(container, "title"))
        item_elements = _find_children(container, "item")
    elif lowered == "channel":
        feed_title = _element_text(_first_child(root, "title"))
        item_elements = _find_children(root, "item")
    elif lowered == "rdf":
        channel = _first_child(root, "channel")
        if channel is not None:
            feed_title = _element_text(_first_child(channel, "title"))
        item_elements = _find_children(root, "item")
    elif lowered == "feed":
        feed_title = _element_text(_first_child(root, "title"))
        item_elements = _find_children(root, "entry")
    else:
        raise SubscriptionFetchError(
            f"Unsupported feed format: {root_name or 'unknown'}"
        )

    entries: list[dict[str, Any]] = []
    for item in item_elements:
        parsed = _parse_feed_entry(item, base_url=base_url)
        if parsed is not None:
            entries.append(parsed)
    return {"feed_title": feed_title, "entries": entries}


# ---------------------------------------------------------------------------
# YouTube / feed URL heuristics
# ---------------------------------------------------------------------------


def resolve_youtube_rss_url(url: str) -> str | None:
    text = str(url or "").strip()
    if not text:
        return None
    try:
        parts = urlsplit(text)
    except ValueError:
        return None
    host = (parts.hostname or "").lower()
    if host not in _YOUTUBE_HOSTS:
        return None
    path = parts.path or ""
    if path.startswith("/feeds/videos.xml"):
        return text

    match = re.match(
        r"^/channel/(UC[A-Za-z0-9_-]+)(?:/(?:videos|shorts|streams|featured))?/?$",
        path,
    )
    if match:
        return f"https://www.youtube.com/feeds/videos.xml?channel_id={match.group(1)}"

    if re.match(r"^/playlist/?$", path):
        list_ids = parse_qs(parts.query, keep_blank_values=True).get("list", [])
        list_id = list_ids[0].strip() if list_ids else ""
        if list_id:
            return f"https://www.youtube.com/feeds/videos.xml?playlist_id={list_id}"

    match = re.match(
        r"^/user/([A-Za-z0-9_.-]+)(?:/(?:videos|shorts|streams|featured))?/?$", path
    )
    if match:
        return f"https://www.youtube.com/feeds/videos.xml?user={match.group(1)}"
    return None


def _looks_like_feed_url(url: str) -> bool:
    try:
        parts = urlsplit(str(url or "").strip())
    except ValueError:
        return False
    path = (parts.path or "").lower()
    if not path:
        return False
    if "feeds/videos.xml" in path:
        return True
    if path.endswith((".rss", ".xml", ".atom")):
        return True
    segments = [segment for segment in path.split("/") if segment]
    return any(segment in {"feed", "rss", "atom"} for segment in segments)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


def _derive_subscription_name(url: str) -> str:
    try:
        parts = urlsplit(str(url or "").strip())
    except ValueError:
        return "Subscription"
    host = (parts.hostname or "subscription").lower()
    if host.startswith("www."):
        host = host[4:]
    segments = [
        unquote(segment).strip()
        for segment in (parts.path or "").split("/")
        if segment.strip()
    ]
    if segments:
        tail = segments[-1]
        if tail.lower() in _YOUTUBE_TAB_SEGMENTS and len(segments) > 1:
            tail = segments[-2]
        label = f"{host} / {tail}"
    else:
        label = host
    label = re.sub(r"\s+", " ", label).strip()
    return (label or "Subscription")[:120]


def _coerce_int_field(
    value: Any, field_name: str, *, minimum: int, maximum: int | None = None
) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise SubscriptionValidationError(
            f"Invalid {field_name}: must be an integer"
        ) from None
    if number < minimum:
        raise SubscriptionValidationError(
            f"Invalid {field_name}: must be at least {minimum}"
        )
    if maximum is not None and number > maximum:
        raise SubscriptionValidationError(
            f"Invalid {field_name}: must be at most {maximum}"
        )
    return number


def _setting_int(settings: Any, name: str, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(getattr(settings, name, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _setting_float(
    settings: Any, name: str, default: float, *, minimum: float = 0.0
) -> float:
    try:
        value = float(getattr(settings, name, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


class SubscriptionService:
    def __init__(self, settings: Settings, db: Database, ingest: IngestService) -> None:
        self.settings = settings
        self.db = db
        self.ingest = ingest
        self._sub_locks: dict[str, asyncio.Lock] = {}
        self._poll_sem = asyncio.Semaphore(
            _setting_int(settings, "subscriptions_max_concurrent_polls", 2, minimum=1)
        )
        self._scheduler_task: asyncio.Task[None] | None = None
        self._active_poll_tasks: set[asyncio.Task[Any]] = set()
        self._active_polls: int = 0
        self._stopping: bool = False

    # -- settings helpers ---------------------------------------------------

    def _http_timeout_sec(self) -> float:
        return _setting_float(
            self.settings, "subscriptions_http_timeout_sec", 20.0, minimum=1.0
        )

    def _max_feed_bytes(self) -> int:
        return _setting_int(
            self.settings, "subscriptions_max_feed_bytes", 5 * 1024 * 1024, minimum=1
        )

    def _allow_private_hosts(self) -> bool:
        return bool(getattr(self.settings, "subscriptions_allow_private_hosts", False))

    def _failure_pause_threshold(self) -> int:
        return _setting_int(
            self.settings, "subscriptions_failure_pause_threshold", 5, minimum=0
        )

    def _poll_tick_sec(self) -> float:
        return _setting_float(
            self.settings, "subscriptions_poll_tick_sec", 30.0, minimum=0.01
        )

    def _default_folder(self) -> str:
        return str(getattr(self.settings, "default_folder", "General") or "General")

    # -- CRUD wrappers ------------------------------------------------------

    def create_subscription(
        self,
        payload: Mapping[str, Any] | str | None = None,
        *,
        source_url: str | None = None,
        name: str | None = None,
        source_kind: str | None = None,
        folder: str | None = None,
        tags: str | list[str] | tuple[str, ...] | None = None,
        format_spec: str | None = None,
        forward_to_telegram: bool | None = None,
        interval_minutes: int | None = None,
        max_items_per_check: int | None = None,
        enabled: bool | None = None,
        seed_only: bool | None = None,
        initial_backfill_limit: int | None = None,
        title_include_regex: str | None = None,
        title_exclude_regex: str | None = None,
        next_check_at: str | None = None,
    ) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        if isinstance(payload, str):
            if source_url is None:
                source_url = payload
        elif payload is not None:
            if not isinstance(payload, Mapping):
                raise SubscriptionValidationError("Invalid subscription payload")
            fields.update(dict(payload))

        overrides = {
            "source_url": source_url,
            "name": name,
            "source_kind": source_kind,
            "folder": folder,
            "tags": tags,
            "format_spec": format_spec,
            "forward_to_telegram": forward_to_telegram,
            "interval_minutes": interval_minutes,
            "max_items_per_check": max_items_per_check,
            "enabled": enabled,
            "seed_only": seed_only,
            "initial_backfill_limit": initial_backfill_limit,
            "title_include_regex": title_include_regex,
            "title_exclude_regex": title_exclude_regex,
            "next_check_at": next_check_at,
        }
        for key, value in overrides.items():
            if value is not None:
                fields[key] = value

        clean_url = validate_subscription_url(str(fields.get("source_url") or ""))
        clean_kind = self._validate_source_kind(
            str(fields.get("source_kind") or "auto")
        )
        raw_name = str(fields.get("name") or "").strip()
        clean_name = raw_name[:120] or _derive_subscription_name(clean_url)
        clean_folder = sanitize_folder(fields.get("folder"), self._default_folder())
        clean_tags = normalize_tags(fields.get("tags"))
        clean_format = self._validate_format_spec(fields.get("format_spec"))
        clean_include = validate_regex_pattern(
            fields.get("title_include_regex"), "title_include_regex"
        )
        clean_exclude = validate_regex_pattern(
            fields.get("title_exclude_regex"), "title_exclude_regex"
        )
        clean_interval = _coerce_int_field(
            fields.get("interval_minutes", 60),
            "interval_minutes",
            minimum=5,
            maximum=10080,
        )
        clean_max_items = _coerce_int_field(
            fields.get("max_items_per_check", 10),
            "max_items_per_check",
            minimum=1,
            maximum=100,
        )
        clean_backfill = _coerce_int_field(
            fields.get("initial_backfill_limit", 0),
            "initial_backfill_limit",
            minimum=0,
            maximum=50,
        )
        is_enabled = bool(fields.get("enabled", True))
        clean_next_check = fields.get("next_check_at")

        return self.db.create_subscription(
            name=clean_name,
            source_url=clean_url,
            source_kind=clean_kind,
            folder=clean_folder,
            tags=clean_tags,
            format_spec=clean_format,
            forward_to_telegram=bool(fields.get("forward_to_telegram", True)),
            interval_minutes=clean_interval,
            max_items_per_check=clean_max_items,
            enabled=is_enabled,
            seed_only=bool(fields.get("seed_only", True)),
            initial_backfill_limit=clean_backfill,
            title_include_regex=clean_include,
            title_exclude_regex=clean_exclude,
            next_check_at=clean_next_check if is_enabled else None,
        )

    def update_subscription(
        self,
        subscription_id: str,
        payload: Mapping[str, Any] | None = None,
        *,
        publish: bool = True,
        **fields: Any,
    ) -> dict[str, Any]:
        existing = self.db.get_subscription(subscription_id)
        if existing is None:
            raise SubscriptionNotFoundError(
                f"Subscription not found: {subscription_id}"
            )

        updates: dict[str, Any] = {}
        if payload is not None:
            if not isinstance(payload, Mapping):
                raise SubscriptionValidationError("Invalid subscription payload")
            updates.update(dict(payload))
        updates.update(fields)

        # PATCH bodies may carry explicit nulls for untouched form fields.
        # Drop those for fields where null has no meaningful "clear" action.
        for key in (
            "name",
            "source_url",
            "source_kind",
            "folder",
            "enabled",
            "seed_only",
            "forward_to_telegram",
            "interval_minutes",
            "max_items_per_check",
            "initial_backfill_limit",
        ):
            if key in updates and updates[key] is None:
                updates.pop(key, None)

        if "source_url" in updates:
            updates["source_url"] = validate_subscription_url(
                str(updates.get("source_url") or "")
            )
        if "source_kind" in updates:
            updates["source_kind"] = self._validate_source_kind(
                str(updates.get("source_kind") or "auto")
            )
        if "name" in updates:
            raw_name = str(updates.get("name") or "").strip()
            updates["name"] = (
                raw_name[:120]
                if raw_name
                else _derive_subscription_name(
                    str(updates.get("source_url") or existing.get("source_url") or "")
                )
            )
        if "folder" in updates:
            updates["folder"] = sanitize_folder(
                updates.get("folder"), self._default_folder()
            )
        if "tags" in updates:
            updates["tags"] = normalize_tags(updates.get("tags"))
        if "format_spec" in updates:
            updates["format_spec"] = self._validate_format_spec(
                updates.get("format_spec")
            )
        if "title_include_regex" in updates:
            updates["title_include_regex"] = validate_regex_pattern(
                updates.get("title_include_regex"), "title_include_regex"
            )
        if "title_exclude_regex" in updates:
            updates["title_exclude_regex"] = validate_regex_pattern(
                updates.get("title_exclude_regex"), "title_exclude_regex"
            )
        if "interval_minutes" in updates:
            updates["interval_minutes"] = _coerce_int_field(
                updates.get("interval_minutes"),
                "interval_minutes",
                minimum=5,
                maximum=10080,
            )
        if "max_items_per_check" in updates:
            updates["max_items_per_check"] = _coerce_int_field(
                updates.get("max_items_per_check"),
                "max_items_per_check",
                minimum=1,
                maximum=100,
            )
        if "initial_backfill_limit" in updates:
            updates["initial_backfill_limit"] = _coerce_int_field(
                updates.get("initial_backfill_limit"),
                "initial_backfill_limit",
                minimum=0,
                maximum=50,
            )
        if "forward_to_telegram" in updates:
            updates["forward_to_telegram"] = bool(updates.get("forward_to_telegram"))
        if "seed_only" in updates:
            updates["seed_only"] = bool(updates.get("seed_only"))

        if "enabled" in updates:
            is_enabled = bool(updates.get("enabled"))
            updates["enabled"] = is_enabled
            if is_enabled and (
                not bool(existing.get("enabled"))
                or existing.get("last_status") == "paused_error"
            ):
                updates.setdefault("consecutive_failures", 0)
                updates["last_error"] = None
                updates["last_status"] = "pending"
                updates["next_check_at"] = utc_now_iso()
            elif not is_enabled:
                updates["next_check_at"] = None

        updated = self.db.update_subscription(
            subscription_id, publish=publish, **updates
        )
        if updated is None:
            raise SubscriptionNotFoundError(
                f"Subscription not found: {subscription_id}"
            )
        return updated

    def delete_subscription(self, subscription_id: str) -> dict[str, Any]:
        deleted = self.db.delete_subscription(subscription_id)
        if deleted is None:
            raise SubscriptionNotFoundError(
                f"Subscription not found: {subscription_id}"
            )
        lock = self._sub_locks.get(subscription_id)
        if lock is None or not lock.locked():
            self._sub_locks.pop(subscription_id, None)
        return deleted

    def list_subscriptions(self, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        return self.db.list_subscriptions(enabled_only=enabled_only)

    def get_subscription(self, subscription_id: str) -> dict[str, Any]:
        sub = self.db.get_subscription(subscription_id)
        if sub is None:
            raise SubscriptionNotFoundError(
                f"Subscription not found: {subscription_id}"
            )
        return sub

    def list_entries(
        self,
        subscription_id: str,
        *,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        if self.db.get_subscription(subscription_id) is None:
            raise SubscriptionNotFoundError(
                f"Subscription not found: {subscription_id}"
            )
        safe_offset = max(0, int(offset))
        items, total = self.db.list_subscription_entries(
            subscription_id, status=status, limit=limit, offset=safe_offset
        )
        return {
            "entries": items,
            "total": total,
            "has_more": safe_offset + len(items) < total,
        }

    @staticmethod
    def _validate_source_kind(source_kind: str) -> str:
        kind = str(source_kind or "auto").strip().lower()
        if kind not in _SOURCE_KINDS:
            raise SubscriptionValidationError(
                f"Invalid source_kind: {source_kind!r} "
                f"(expected one of {', '.join(sorted(_SOURCE_KINDS))})"
            )
        return kind

    @staticmethod
    def _validate_format_spec(format_spec: str | None) -> str | None:
        cleaned = str(format_spec or "").strip()
        if not cleaned:
            return None
        try:
            resolve_format_spec(cleaned)
        except ValueError as exc:
            raise SubscriptionValidationError(
                f"Invalid format_spec: {exc}"
            ) from exc
        return cleaned

    # -- check / poll -------------------------------------------------------

    async def check_subscription(
        self, subscription_id: str, *, dry_run: bool = False, trigger: str = "manual"
    ) -> dict[str, Any]:
        if self.db.get_subscription(subscription_id) is None:
            raise SubscriptionNotFoundError(
                f"Subscription not found: {subscription_id}"
            )

        lock = self._sub_locks.setdefault(subscription_id, asyncio.Lock())
        if lock.locked():
            raise SubscriptionBusyError("Subscription check is already in progress")

        async with lock:
            async with self._poll_sem:
                sub = self.db.get_subscription(subscription_id)
                if sub is None:
                    raise SubscriptionNotFoundError(
                        f"Subscription not found: {subscription_id}"
                    )
                self._active_polls += 1
                try:
                    return await self._run_check(
                        sub, dry_run=dry_run, trigger=trigger
                    )
                finally:
                    self._active_polls = max(0, self._active_polls - 1)

    def _is_entry_url_safe(self, url: str) -> bool:
        try:
            validated = validate_subscription_url(url)
        except SubscriptionValidationError:
            return False
        if self._allow_private_hosts():
            return True
        parts = urlsplit(validated)
        host = (parts.hostname or "").strip().lower().rstrip(".")
        if _is_local_hostname(host):
            return False
        try:
            ipaddress.ip_address(host)
        except ValueError:
            return True
        return not _is_ip_disallowed(host)

    async def _run_check(
        self, sub: dict[str, Any], *, dry_run: bool, trigger: str
    ) -> dict[str, Any]:
        sub_id = str(sub.get("id") or "")
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()

        try:
            discovery = await self._discover_entries(sub)
        except (SubscriptionFetchError, SubscriptionValidationError) as exc:
            redacted_msg = _redact_credentials(str(exc))
            if dry_run:
                raise type(exc)(redacted_msg) from exc
            await self._record_discovery_failure(sub, exc, now_dt=now_dt, now_iso=now_iso)
            raise SubscriptionFetchError(redacted_msg) from exc
        except Exception as exc:
            redacted_msg = _redact_credentials(
                f"Subscription discovery failed: {exc}"
            )
            if dry_run:
                raise SubscriptionFetchError(redacted_msg) from exc
            wrapped = SubscriptionFetchError(redacted_msg)
            await self._record_discovery_failure(
                sub, wrapped, now_dt=now_dt, now_iso=now_iso
            )
            raise wrapped from exc

        entries = list(discovery.get("entries") or [])
        include_re = self._compile_filter(sub.get("title_include_regex"))
        exclude_re = self._compile_filter(sub.get("title_exclude_regex"))

        entry_keys = [str(entry.get("entry_key") or "") for entry in entries]
        normalized_urls = [
            str(entry.get("normalized_url") or "")
            for entry in entries
            if entry.get("normalized_url")
        ]
        seen_map = self.db.get_subscription_entries_map(
            sub_id, entry_keys, normalized_urls
        ) or {}

        all_urls: list[str] = []
        for entry in entries:
            if entry.get("url"):
                all_urls.append(str(entry["url"]))
            if entry.get("normalized_url"):
                all_urls.append(str(entry["normalized_url"]))
        vault_map = self.db.find_media_by_source_urls(all_urls) or {}

        is_initial = not bool(sub.get("seeded_at"))
        seed_only = bool(sub.get("seed_only", True))
        backfill_limit = int(sub.get("initial_backfill_limit") or 0)
        max_items = int(sub.get("max_items_per_check") or 10)
        if is_initial and seed_only and backfill_limit <= 0:
            queue_budget = 0
            mark_remaining_as_seen = True
        elif is_initial and backfill_limit > 0:
            queue_budget = min(backfill_limit, max_items)
            mark_remaining_as_seen = True
        else:
            queue_budget = max_items
            mark_remaining_as_seen = False

        folder = sanitize_folder(sub.get("folder"), self._default_folder())
        tags = normalize_tags(sub.get("tags"))
        format_spec = sub.get("format_spec")
        forward_to_telegram = bool(sub.get("forward_to_telegram", True))

        queued_count = 0
        seeded_count = 0
        skipped_seen_count = 0
        skipped_duplicate_count = 0
        skipped_filter_count = 0
        job_ids: list[str] = []
        report_entries: list[dict[str, Any]] = []

        for entry in entries:
            entry_key = str(entry.get("entry_key") or "").strip()
            url = str(entry.get("url") or "").strip()
            normalized = str(entry.get("normalized_url") or "") or (
                normalize_source_url(url) or ""
            )
            title = str(entry.get("title") or "").strip() or url
            published_at = entry.get("published_at")
            if not entry_key or not url:
                continue

            action: str
            job_id: str | None = None
            existing_entry = seen_map.get(entry_key) or (
                seen_map.get(normalized) if normalized else None
            )
            existing_media = vault_map.get(url) or (
                vault_map.get(normalized) if normalized else None
            )

            if existing_entry is not None:
                action = "seen"
                skipped_seen_count += 1
            elif existing_media is not None:
                action = "duplicate"
                skipped_duplicate_count += 1
                if not dry_run:
                    self.db.upsert_subscription_entry(
                        sub_id,
                        entry_key,
                        title=title,
                        url=url,
                        normalized_url=normalized or None,
                        status="seen",
                        media_id=existing_media.get("id"),
                        published_at=published_at,
                        last_attempt_at=now_iso,
                    )
            elif not self._is_entry_url_safe(url):
                action = "filtered"
                skipped_filter_count += 1
                if not dry_run:
                    self.db.upsert_subscription_entry(
                        sub_id,
                        entry_key,
                        title=title,
                        url=url,
                        normalized_url=normalized or None,
                        status="skipped",
                        error="Entry URL targets a local, private, or invalid host",
                        published_at=published_at,
                        last_attempt_at=now_iso,
                    )
            elif (
                include_re is not None
                and not include_re.search(title[:MAX_REGEX_TITLE_LENGTH])
            ) or (
                exclude_re is not None
                and exclude_re.search(title[:MAX_REGEX_TITLE_LENGTH])
            ):
                action = "filtered"
                skipped_filter_count += 1
                if not dry_run:
                    self.db.upsert_subscription_entry(
                        sub_id,
                        entry_key,
                        title=title,
                        url=url,
                        normalized_url=normalized or None,
                        status="skipped",
                        published_at=published_at,
                        last_attempt_at=now_iso,
                    )
            elif queued_count < queue_budget:
                if dry_run:
                    action = "would_queue"
                    queued_count += 1
                else:
                    job_id = self._queue_entry(
                        sub_id=sub_id,
                        entry_key=entry_key,
                        url=url,
                        normalized_url=normalized or None,
                        title=title,
                        published_at=published_at,
                        folder=folder,
                        tags=tags,
                        format_spec=format_spec,
                        forward_to_telegram=forward_to_telegram,
                        now_iso=now_iso,
                    )
                    action = "queued"
                    queued_count += 1
                    job_ids.append(job_id)
            elif mark_remaining_as_seen:
                action = "would_seed" if dry_run else "seeded"
                seeded_count += 1
                if not dry_run:
                    self.db.upsert_subscription_entry(
                        sub_id,
                        entry_key,
                        title=title,
                        url=url,
                        normalized_url=normalized or None,
                        status="seen",
                        published_at=published_at,
                        last_attempt_at=now_iso,
                    )
            else:
                action = "deferred"

            report_entries.append(
                {
                    "entry_key": entry_key,
                    "title": title,
                    "url": url,
                    "published_at": published_at,
                    "action": action,
                    "job_id": job_id,
                }
            )

        updated_sub: dict[str, Any] = sub
        if not dry_run:
            latest_sub = self.db.get_subscription(sub_id)
            if latest_sub is not None:
                interval = max(5, int(latest_sub.get("interval_minutes") or 60))
                last_status = (
                    "seeded"
                    if (is_initial and queued_count == 0 and seeded_count > 0)
                    else "ok"
                )
                next_iso = (
                    (now_dt + timedelta(minutes=interval)).isoformat()
                    if bool(latest_sub.get("enabled"))
                    else None
                )
                updates: dict[str, Any] = {
                    "last_checked_at": now_iso,
                    "next_check_at": next_iso,
                    "last_status": last_status,
                    "last_error": None,
                    "consecutive_failures": 0,
                }
                if is_initial:
                    updates["seeded_at"] = now_iso
                fresh = self.db.update_subscription(sub_id, **updates)
                if fresh is not None:
                    updated_sub = fresh

        logger.debug(
            "Subscription %s check (%s) finished: backend=%s discovered=%d queued=%d "
            "seeded=%d seen=%d duplicate=%d filtered=%d",
            sub_id,
            trigger,
            discovery.get("backend"),
            len(entries),
            queued_count,
            seeded_count,
            skipped_seen_count,
            skipped_duplicate_count,
            skipped_filter_count,
        )

        return {
            "subscription_id": sub_id,
            "dry_run": dry_run,
            "backend": discovery.get("backend"),
            "resolved_url": discovery.get("resolved_url") or sub.get("source_url"),
            "feed_title": discovery.get("feed_title") or "",
            "seeded": bool(is_initial and seeded_count > 0 and queued_count == 0),
            "discovered_count": len(entries),
            "queued_count": queued_count,
            "seeded_count": seeded_count,
            "skipped_seen_count": skipped_seen_count,
            "skipped_duplicate_count": skipped_duplicate_count,
            "skipped_filter_count": skipped_filter_count,
            "job_ids": job_ids,
            "entries": report_entries,
            "subscription": updated_sub,
        }

    @staticmethod
    def _compile_filter(pattern: Any) -> re.Pattern[str] | None:
        cleaned = str(pattern or "").strip()
        if not cleaned or len(cleaned) > MAX_REGEX_LENGTH:
            return None
        try:
            if _is_unsafe_regex(cleaned):
                logger.warning("Ignoring unsafe stored subscription regex %r", cleaned)
                return None
            return re.compile(cleaned, re.IGNORECASE)
        except re.error as exc:
            logger.warning("Ignoring invalid stored subscription regex %r: %s", cleaned, exc)
            return None

    async def _record_discovery_failure(
        self,
        sub: dict[str, Any],
        exc: Exception,
        *,
        now_dt: datetime,
        now_iso: str,
    ) -> None:
        sub_id = str(sub.get("id") or "")
        err_msg = _redact_credentials(str(exc).strip() or exc.__class__.__name__)[:1000]
        latest = self.db.get_subscription(sub_id)
        if latest is None:
            return
        failures = int(latest.get("consecutive_failures") or 0) + 1
        interval = max(5, int(latest.get("interval_minutes") or 60))
        backoff_min = min(
            interval * (2 ** min(failures, 5)), max(interval * 16, 1440)
        )
        threshold = self._failure_pause_threshold()

        try:
            if threshold > 0 and failures >= threshold:
                self.db.update_subscription(
                    sub_id,
                    enabled=False,
                    last_checked_at=now_iso,
                    next_check_at=None,
                    last_status="paused_error",
                    last_error=err_msg,
                    consecutive_failures=failures,
                )
            else:
                next_iso = (
                    (now_dt + timedelta(minutes=backoff_min)).isoformat()
                    if bool(latest.get("enabled"))
                    else None
                )
                self.db.update_subscription(
                    sub_id,
                    last_checked_at=now_iso,
                    next_check_at=next_iso,
                    last_status="error",
                    last_error=err_msg,
                    consecutive_failures=failures,
                )
        except Exception as update_exc:
            logger.warning(
                "Could not record subscription failure for %s: %s", sub_id, update_exc
            )
        logger.warning(
            "Subscription %s discovery failed (attempt %d): %s",
            sub_id,
            failures,
            err_msg,
        )

    def _queue_entry(
        self,
        *,
        sub_id: str,
        entry_key: str,
        url: str,
        normalized_url: str | None,
        title: str,
        published_at: Any,
        folder: str,
        tags: list[str],
        format_spec: Any,
        forward_to_telegram: bool,
        now_iso: str,
    ) -> str:
        params = {
            "subscription_id": sub_id,
            "entry_key": entry_key,
            "url": url,
            "folder": folder,
            "tags": tags,
            "title": title,
            "forward_to_telegram": bool(forward_to_telegram),
            "format_spec": format_spec,
            "source_origin": "subscription",
        }
        job_id = self.db.create_job("subscription", "url", url, params=params)
        self.db.upsert_subscription_entry(
            sub_id,
            entry_key,
            title=title,
            url=url,
            normalized_url=normalized_url,
            status="queued",
            job_id=job_id,
            published_at=published_at,
            last_attempt_at=now_iso,
        )
        self.ingest.submit_job(
            job_id,
            self._run_ingest_job(
                job_id=job_id,
                sub_id=sub_id,
                entry_key=entry_key,
                url=url,
                folder=folder,
                tags=tags,
                title=title,
                format_spec=format_spec,
                forward_to_telegram=forward_to_telegram,
            ),
        )
        return job_id

    @staticmethod
    def _supports_kwarg(fn: Any, name: str) -> bool:
        try:
            sig = inspect.signature(fn)
        except (TypeError, ValueError):
            return False
        for param in sig.parameters.values():
            if param.kind == inspect.Parameter.VAR_KEYWORD or param.name == name:
                return True
        return False

    async def _run_ingest_job(
        self,
        *,
        job_id: str,
        sub_id: str,
        entry_key: str,
        url: str,
        folder: str,
        tags: list[str],
        title: str,
        format_spec: Any,
        forward_to_telegram: bool,
    ) -> None:
        allow_private = self._allow_private_hosts()
        try:
            await assert_safe_public_url(url, allow_private=allow_private)
            call_kwargs: dict[str, Any] = {
                "source_origin": "subscription",
                "folder": folder,
                "tags": tags,
                "title": title,
                "forward_to_telegram": forward_to_telegram,
                "format_spec": format_spec,
            }
            if self._supports_kwarg(self.ingest.process_url_job, "block_private_hosts"):
                call_kwargs["block_private_hosts"] = not allow_private
            media_items = await self.ingest.process_url_job(
                job_id,
                url,
                **call_kwargs,
            )
        except asyncio.CancelledError:
            self._safe_update_entry_status(
                sub_id,
                entry_key,
                status="failed",
                job_id=job_id,
                error="Cancelled",
            )
            raise
        except Exception as exc:
            safe_err = _redact_credentials(str(exc))[:1000]
            try:
                job = self.db.get_job(job_id)
                if job is not None and job.get("status") in ("queued", "downloading", "allocating"):
                    self.db.update_job(job_id, status="failed", message=safe_err[:300])
            except Exception:
                pass
            self._safe_update_entry_status(
                sub_id,
                entry_key,
                status="failed",
                job_id=job_id,
                error=safe_err,
            )
            raise

        items = list(media_items or [])
        if not items:
            self._safe_update_entry_status(
                sub_id,
                entry_key,
                status="skipped",
                job_id=job_id,
                error="Ingest produced no media",
            )
            return

        first = items[0] if isinstance(items[0], dict) else {}
        media_id = first.get("id")
        deduplicated = bool(first.get("deduplicated"))
        self._safe_update_entry_status(
            sub_id,
            entry_key,
            status="ingested",
            media_id=media_id,
            job_id=job_id,
        )
        if not deduplicated:
            try:
                self.db.increment_subscription_ingested(sub_id, 1)
            except Exception as exc:
                logger.warning(
                    "Could not increment ingested count for subscription %s: %s",
                    sub_id,
                    exc,
                )

    def _safe_update_entry_status(
        self,
        sub_id: str,
        entry_key: str,
        *,
        status: str,
        media_id: Any = None,
        job_id: str | None = None,
        error: str | None = None,
    ) -> None:
        try:
            self.db.update_subscription_entry_status(
                sub_id,
                entry_key,
                status=status,
                media_id=media_id,
                job_id=job_id,
                error=error,
                last_attempt_at=utc_now_iso(),
            )
        except Exception as exc:
            logger.warning(
                "Could not update subscription entry %s/%s: %s",
                sub_id,
                entry_key,
                exc,
            )

    # -- discovery ----------------------------------------------------------

    async def _effective_proxy(self) -> str:
        try:
            proxy = await self.ingest.get_effective_proxy()
        except Exception as exc:
            logger.debug("Could not resolve ingest proxy: %s", exc)
            return str(getattr(self.settings, "ytdlp_proxy", "") or "").strip()
        return str(proxy or "").strip()

    async def _fetch_rss(
        self, url: str, *, proxy: str
    ) -> tuple[dict[str, Any], str]:
        raw, final_url, _content_type = await fetch_feed_bytes(
            url,
            timeout_sec=self._http_timeout_sec(),
            max_bytes=self._max_feed_bytes(),
            proxy=proxy,
            allow_private=self._allow_private_hosts(),
        )
        parsed = parse_feed_xml(raw, base_url=final_url)
        return parsed, final_url

    async def _expand_with_ytdlp(
        self, url: str, *, fetch_limit: int, proxy: str
    ) -> dict[str, Any]:
        await assert_safe_public_url(
            url, allow_private=self._allow_private_hosts()
        )
        cookies_file = getattr(self.settings, "ytdlp_cookies_file", None)
        if isinstance(cookies_file, str) and cookies_file.strip():
            cookies_file = Path(cookies_file)
        try:
            result = await expand_ytdlp_playlist(
                url,
                cookies_file=cookies_file,
                proxy=proxy,
                max_items=fetch_limit,
            )
        except SubscriptionError:
            raise
        except Exception as exc:
            raise SubscriptionFetchError(
                _redact_credentials(f"yt-dlp playlist discovery failed: {exc}")
            ) from exc

        entries = self._normalize_ytdlp_entries(result.get("entries") or [])
        return {
            "backend": "ytdlp",
            "resolved_url": str(result.get("webpage_url") or url),
            "feed_title": str(result.get("playlist_title") or ""),
            "entries": self._dedupe_entries(entries),
        }

    @staticmethod
    def _normalize_ytdlp_entries(entries: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for raw_entry in entries:
            if not isinstance(raw_entry, dict):
                continue
            entry_url = str(raw_entry.get("url") or "").strip()
            if not entry_url.startswith(("http://", "https://")):
                continue
            normalized_url = normalize_source_url(entry_url) or entry_url
            title = str(raw_entry.get("title") or "").strip() or entry_url
            normalized.append(
                {
                    "entry_key": normalized_url,
                    "title": title,
                    "url": entry_url,
                    "normalized_url": normalized_url,
                    "published_at": None,
                }
            )
        return normalized

    @staticmethod
    def _dedupe_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for entry in entries:
            keys: list[str] = []
            if entry.get("entry_key"):
                keys.append(f"k:{entry['entry_key']}")
            if entry.get("normalized_url"):
                keys.append(f"u:{entry['normalized_url']}")
            if not keys:
                keys.append(f"r:{entry.get('url') or ''}")
            if any(key in seen for key in keys):
                continue
            seen.update(keys)
            unique.append(entry)
        return unique

    async def _discover_entries(self, sub: dict[str, Any]) -> dict[str, Any]:
        sub_id = str(sub.get("id") or "")
        url = str(sub.get("source_url") or "").strip()
        kind = str(sub.get("source_kind") or "auto").strip().lower()
        max_items = int(sub.get("max_items_per_check") or 10)
        backfill_limit = int(sub.get("initial_backfill_limit") or 0)
        fetch_limit = min(100, max(1, max(max_items, backfill_limit, 25)))
        proxy = await self._effective_proxy()

        if kind == "rss":
            parsed, final_url = await self._fetch_rss(url, proxy=proxy)
            return {
                "backend": "rss",
                "resolved_url": final_url,
                "feed_title": str(parsed.get("feed_title") or ""),
                "entries": self._dedupe_entries(list(parsed.get("entries") or [])),
            }

        if kind == "ytdlp":
            return await self._expand_with_ytdlp(
                url, fetch_limit=fetch_limit, proxy=proxy
            )

        youtube_rss = resolve_youtube_rss_url(url)
        if youtube_rss:
            try:
                parsed, final_url = await self._fetch_rss(youtube_rss, proxy=proxy)
                entries = list(parsed.get("entries") or [])
                if not entries:
                    raise SubscriptionFetchError(
                        "YouTube RSS feed returned no entries"
                    )
                return {
                    "backend": "rss",
                    "resolved_url": final_url,
                    "feed_title": str(parsed.get("feed_title") or ""),
                    "entries": self._dedupe_entries(entries),
                }
            except SubscriptionFetchError as exc:
                logger.info(
                    "YouTube RSS unavailable for subscription %s (%s); "
                    "falling back to yt-dlp",
                    sub_id,
                    exc,
                )
            return await self._expand_with_ytdlp(
                url, fetch_limit=fetch_limit, proxy=proxy
            )

        if _looks_like_feed_url(url):
            try:
                parsed, final_url = await self._fetch_rss(url, proxy=proxy)
                return {
                    "backend": "rss",
                    "resolved_url": final_url,
                    "feed_title": str(parsed.get("feed_title") or ""),
                    "entries": self._dedupe_entries(
                        list(parsed.get("entries") or [])
                    ),
                }
            except SubscriptionFetchError as exc:
                logger.info(
                    "RSS parse/fetch failed for subscription %s (%s); "
                    "falling back to yt-dlp",
                    sub_id,
                    exc,
                )

        return await self._expand_with_ytdlp(url, fetch_limit=fetch_limit, proxy=proxy)

    # -- scheduler ----------------------------------------------------------

    async def poll_due_subscriptions(self) -> list[dict[str, Any]]:
        due = self.db.list_due_subscriptions(utc_now_iso(), limit=50)
        tasks: list[asyncio.Task[Any]] = []
        for sub in due:
            sub_id = str(sub.get("id") or "").strip()
            if not sub_id:
                continue
            lock = self._sub_locks.get(sub_id)
            if lock is not None and lock.locked():
                continue
            task = asyncio.create_task(
                self._poll_due_one(sub_id), name=f"subscription-poll-{sub_id}"
            )
            self._active_poll_tasks.add(task)
            tasks.append(task)

        if not tasks:
            return []

        results = await asyncio.gather(*tasks, return_exceptions=True)
        reports: list[dict[str, Any]] = []
        for result in results:
            if isinstance(result, dict):
                reports.append(result)
            elif isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                logger.warning("Subscription poll task failed: %s", result)
        return reports

    async def _poll_due_one(self, sub_id: str) -> dict[str, Any] | None:
        try:
            return await self.check_subscription(sub_id, trigger="scheduled")
        except asyncio.CancelledError:
            raise
        except SubscriptionBusyError:
            return None
        except (
            SubscriptionNotFoundError,
            SubscriptionFetchError,
            SubscriptionValidationError,
        ) as exc:
            logger.warning(
                "Scheduled subscription check failed for %s: %s", sub_id, exc
            )
            return None
        except Exception as exc:
            logger.exception(
                "Unexpected subscription check failure for %s: %s", sub_id, exc
            )
            return None
        finally:
            current = asyncio.current_task()
            if current is not None:
                self._active_poll_tasks.discard(current)

    async def _scheduler_loop(self) -> None:
        tick = self._poll_tick_sec()
        while not self._stopping:
            try:
                await self.poll_due_subscriptions()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Subscription scheduler tick failed: %s", exc)
            await asyncio.sleep(tick)

    def start_scheduler(self) -> None:
        try:
            self.db.reconcile_subscription_entries_with_jobs()
        except Exception as exc:
            logger.warning(
                "Could not reconcile interrupted subscription entries: %s", exc
            )
        if not bool(getattr(self.settings, "subscriptions_enabled", True)):
            return
        if self._scheduler_task is not None and not self._scheduler_task.done():
            return
        self._stopping = False
        self._scheduler_task = asyncio.create_task(
            self._scheduler_loop(), name="subscriptions-scheduler"
        )

    async def stop_scheduler(self) -> None:
        self._stopping = True
        tasks: list[asyncio.Task[Any]] = []
        if self._scheduler_task is not None:
            tasks.append(self._scheduler_task)
            self._scheduler_task = None
        tasks.extend(list(self._active_poll_tasks))
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._active_poll_tasks.clear()

    def get_health_summary(self) -> dict[str, Any]:
        summary = dict(self.db.get_subscriptions_summary() or {})
        summary.update(
            {
                "scheduler_running": bool(
                    self._scheduler_task is not None
                    and not self._scheduler_task.done()
                ),
                "enabled_in_config": bool(
                    getattr(self.settings, "subscriptions_enabled", True)
                ),
                "active_polls": int(self._active_polls),
                "poll_tick_sec": self._poll_tick_sec(),
            }
        )
        return summary
