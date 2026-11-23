"""Offline, deterministic tests for Channel / Playlist / RSS watch subscriptions.

Everything here runs without network access: HTTP is served by httpx.MockTransport,
DNS lookups are monkeypatched, and yt-dlp / ingest jobs are replaced with fakes.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import socket
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.db import Database, normalize_source_url
from app.main import create_app
from app.subscriptions import (
    SubscriptionFetchError,
    SubscriptionService,
    SubscriptionValidationError,
    assert_safe_public_url,
    fetch_feed_bytes,
    parse_feed_xml,
    resolve_youtube_rss_url,
)
from app import subscriptions as subs_mod
from tests.helpers import STATIC_DIR, _make_settings

AUTH_TOKEN = "subs-test-token"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeIngest:
    """Minimal IngestService stand-in that never touches the network."""

    def __init__(self) -> None:
        self.submitted_job_ids: list[str] = []
        self.process_calls: list[dict] = []
        self.process_result: list[dict] = []
        self.process_error: Exception | None = None
        self.proxy = ""

    async def get_effective_proxy(self) -> str:
        return self.proxy

    def submit_job(self, job_id: str, coro):
        self.submitted_job_ids.append(job_id)
        # The real service runs this coroutine; closing it keeps the test offline
        # while still leaving the DB job in its "queued" state.
        coro.close()
        return None

    async def process_url_job(self, job_id: str, url: str, **kwargs):
        self.process_calls.append({"job_id": job_id, "url": url, **kwargs})
        if self.process_error is not None:
            raise self.process_error
        return list(self.process_result)


def _settings_for(tmp_path: Path, **overrides) -> object:
    settings = _make_settings(tmp_path, storage_mode="drives")
    settings.ytdlp_tls_fragment_proxy = False
    for key, value in overrides.items():
        setattr(settings, key, value)
    settings.ensure_dirs()
    return settings


def _make_service(tmp_path: Path, **overrides):
    settings = _settings_for(tmp_path, **overrides)
    db = Database(settings.db_path)
    ingest = FakeIngest()
    service = SubscriptionService(settings, db, ingest)
    return service, db, ingest


def _make_api_app(tmp_path: Path, *, auth_token: str = AUTH_TOKEN, enabled: bool = False):
    settings = _settings_for(
        tmp_path, web_auth_token=auth_token, subscriptions_enabled=enabled
    )
    app = create_app(settings)
    return app, settings


def _entry(key: str, url: str, title: str | None = None, published_at: str | None = None):
    return {
        "entry_key": key,
        "title": title or key,
        "url": url,
        "normalized_url": normalize_source_url(url) or url,
        "published_at": published_at,
    }


def _install_discovery(service: SubscriptionService, entries: list[dict], backend: str = "rss"):
    async def fake_discover(sub):
        return {
            "backend": backend,
            "resolved_url": sub.get("source_url"),
            "feed_title": "Test Feed",
            "entries": list(entries),
        }

    service._discover_entries = fake_discover
    return fake_discover


def _insert_media(db: Database, media_id: str, source_url: str, *, title: str = "Media"):
    db.insert_media(
        media_id=media_id,
        title=title,
        filename=f"{media_id}.mp4",
        folder="General",
        media_type="video",
        mime_type="video/mp4",
        size_bytes=1024,
        drive_name="drive1",
        file_path=f"General/{media_id}.mp4",
        thumb_filename=None,
        source_origin="web",
        source_kind="url",
        source_url=source_url,
    )


def _install_transport(monkeypatch, handler):
    """Force fetch_feed_bytes() to use a MockTransport AsyncClient."""

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(subs_mod.httpx, "AsyncClient", factory)
    return transport


def _public_dns(monkeypatch, ip: str = "93.184.216.34"):
    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "",
                (ip, int(port or 80)),
            )
        ]

    monkeypatch.setattr(subs_mod.socket, "getaddrinfo", fake_getaddrinfo)


ATOM_YOUTUBE_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns:media="http://search.yahoo.com/mrss/">
  <title>Example Channel</title>
  <entry>
    <title>Video One</title>
    <yt:videoId>vid001</yt:videoId>
    <link rel="alternate" href="https://www.youtube.com/watch?v=vid001"/>
    <published>2025-02-03T04:05:06+00:00</published>
  </entry>
  <entry>
    <title>Video Two</title>
    <yt:videoId>vid002</yt:videoId>
    <link rel="alternate" href="https://www.youtube.com/watch?v=vid002"/>
    <published>2025-02-04T04:05:06+00:00</published>
  </entry>
</feed>
"""


# ---------------------------------------------------------------------------
# 1. Safe XML / feed parsing
# ---------------------------------------------------------------------------


def test_parse_feed_rss2_enclosure_media_content_link_guid_and_pubdate():
    rss = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
      <channel>
        <title>Example Feed</title>
        <item>
          <title>Episode One</title>
          <guid>guid-ep-1</guid>
          <enclosure url="https://cdn.example.com/ep1.mp4" type="video/mp4" length="123"/>
          <pubDate>Wed, 01 Jan 2025 12:00:00 GMT</pubDate>
        </item>
        <item>
          <title>Episode Two</title>
          <media:content url="https://cdn.example.com/ep2.mp3" type="audio/mpeg"/>
        </item>
        <item>
          <title>Episode Three</title>
          <link>https://example.com/ep3</link>
        </item>
      </channel>
    </rss>"""

    parsed = parse_feed_xml(rss)
    assert parsed["feed_title"] == "Example Feed"
    entries = parsed["entries"]
    assert len(entries) == 3

    first = entries[0]
    assert first["url"] == "https://cdn.example.com/ep1.mp4"
    assert first["entry_key"] == "guid:guid-ep-1"
    assert first["published_at"] == "2025-01-01T12:00:00+00:00"

    second = entries[1]
    assert second["url"] == "https://cdn.example.com/ep2.mp3"
    assert second["entry_key"] == normalize_source_url("https://cdn.example.com/ep2.mp3")

    third = entries[2]
    assert third["url"] == "https://example.com/ep3"


def test_parse_feed_atom_youtube_video_id_media_group_and_legacy_flash_skip():
    atom = b"""<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom"
          xmlns:yt="http://www.youtube.com/xml/schemas/2015"
          xmlns:media="http://search.yahoo.com/mrss/">
      <title>YT Channel</title>
      <entry>
        <title>Video One</title>
        <yt:videoId>vid001</yt:videoId>
        <link rel="alternate" href="https://www.youtube.com/watch?v=vid001"/>
        <media:group>
          <media:content url="https://www.youtube.com/v/vid001?version=3" type="application/x-shockwave-flash"/>
          <media:content url="https://i.ytimg.com/vi/vid001/hqdefault.jpg" type="image/jpeg"/>
        </media:group>
        <published>2025-02-03T04:05:06+00:00</published>
      </entry>
      <entry>
        <title>Video Two</title>
        <media:group>
          <media:content url="https://www.youtube.com/v/legacyvid" type="application/x-shockwave-flash"/>
          <media:content url="https://cdn.example.com/video2.mp4" type="video/mp4"/>
        </media:group>
      </entry>
      <entry>
        <title>Legacy Only</title>
        <media:group>
          <media:content url="https://www.youtube.com/v/onlylegacy" type="application/x-shockwave-flash"/>
        </media:group>
      </entry>
    </feed>"""

    parsed = parse_feed_xml(atom)
    assert parsed["feed_title"] == "YT Channel"
    entries = parsed["entries"]
    assert len(entries) == 2

    assert entries[0]["url"] == "https://www.youtube.com/watch?v=vid001"
    assert entries[0]["entry_key"] == normalize_source_url(
        "https://www.youtube.com/watch?v=vid001"
    )
    assert entries[0]["published_at"] == "2025-02-03T04:05:06+00:00"

    # The legacy /v/ Flash embed is skipped in favour of the real media content.
    assert entries[1]["url"] == "https://cdn.example.com/video2.mp4"


def test_parse_feed_rss1_rdf():
    rdf = """<?xml version="1.0" encoding="UTF-8"?>
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns="http://purl.org/rss/1.0/"
             xmlns:dc="http://purl.org/dc/elements/1.1/">
      <channel>
        <title>RDF Feed</title>
      </channel>
      <item>
        <title>RDF Item</title>
        <link>https://example.com/rdf1</link>
        <dc:date>2024-06-07T08:09:10Z</dc:date>
      </item>
    </rdf:RDF>"""

    parsed = parse_feed_xml(rdf)
    assert parsed["feed_title"] == "RDF Feed"
    assert len(parsed["entries"]) == 1
    entry = parsed["entries"][0]
    assert entry["url"] == "https://example.com/rdf1"
    assert entry["published_at"] == "2024-06-07T08:09:10+00:00"


def test_parse_feed_rejects_xxe_doctype_entity_and_element_cap(monkeypatch):
    xxe = """<?xml version="1.0"?>
    <!DOCTYPE rss [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
    <rss version="2.0"><channel><title>&xxe;</title></channel></rss>"""
    with pytest.raises(SubscriptionFetchError):
        parse_feed_xml(xxe)

    doctype_only = """<?xml version="1.0"?>
    <!DOCTYPE rss SYSTEM "http://example.com/evil.dtd">
    <rss version="2.0"><channel><title>x</title></channel></rss>"""
    with pytest.raises(SubscriptionFetchError):
        parse_feed_xml(doctype_only)

    monkeypatch.setattr(subs_mod, "MAX_XML_ELEMENTS", 20)
    items = "".join(f"<item><title>item {i}</title></item>" for i in range(30))
    huge = f'<?xml version="1.0"?><rss version="2.0"><channel><title>x</title>{items}</channel></rss>'
    with pytest.raises(SubscriptionFetchError) as excinfo:
        parse_feed_xml(huge)
    assert "more than 20 elements" in str(excinfo.value)


# ---------------------------------------------------------------------------
# 2. SSRF & HTTP fetch hardening
# ---------------------------------------------------------------------------


def test_assert_safe_public_url_blocks_local_private_and_malformed_targets():
    blocked = [
        "http://localhost/feed",
        "http://127.0.0.1/feed",
        "http://[::1]/feed",
        "http://169.254.169.254/latest/meta-data",
        "http://10.0.0.1/feed",
        "http://[::ffff:127.0.0.1]/feed",
        "http://foo.local/feed",
        "http://user:pass@example.com/feed",
        "file:///etc/passwd",
    ]
    for url in blocked:
        with pytest.raises((SubscriptionValidationError, SubscriptionFetchError)):
            asyncio.run(assert_safe_public_url(url))


def test_assert_safe_public_url_dns_resolution_private_and_public(monkeypatch):
    def private_dns(host, port, *args, **kwargs):
        return [
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "",
                ("10.1.2.3", int(port or 80)),
            )
        ]

    monkeypatch.setattr(subs_mod.socket, "getaddrinfo", private_dns)
    with pytest.raises(SubscriptionValidationError):
        asyncio.run(assert_safe_public_url("http://evil.example.com/feed"))

    def mixed_dns(host, port, *args, **kwargs):
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 80)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.5", 80)),
        ]

    monkeypatch.setattr(subs_mod.socket, "getaddrinfo", mixed_dns)
    with pytest.raises(SubscriptionValidationError):
        asyncio.run(assert_safe_public_url("http://evil.example.com/feed"))

    _public_dns(monkeypatch)
    assert (
        asyncio.run(assert_safe_public_url("https://example.com/feed"))
        == "https://example.com/feed"
    )

    def failing_dns(host, port, *args, **kwargs):
        raise socket.gaierror("no such host")

    monkeypatch.setattr(subs_mod.socket, "getaddrinfo", failing_dns)
    with pytest.raises(SubscriptionFetchError):
        asyncio.run(assert_safe_public_url("http://does-not-exist.example/feed"))


def test_assert_safe_public_url_allow_private_opt_in():
    for url in ("http://localhost/feed", "http://127.0.0.1/feed"):
        assert asyncio.run(assert_safe_public_url(url, allow_private=True)) == url


def test_fetch_feed_bytes_blocks_redirect_to_loopback(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "8.8.8.8":
            return httpx.Response(
                302, headers={"location": "http://127.0.0.1/secret.mp4"}
            )
        return httpx.Response(200, content=b"secret")

    _install_transport(monkeypatch, handler)
    with pytest.raises(SubscriptionValidationError):
        asyncio.run(fetch_feed_bytes("http://8.8.8.8/feed"))


def test_fetch_feed_bytes_enforces_max_redirects(monkeypatch):
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(302, headers={"location": "/next"})

    _install_transport(monkeypatch, handler)
    with pytest.raises(SubscriptionFetchError) as excinfo:
        asyncio.run(fetch_feed_bytes("http://8.8.8.8/feed", max_redirects=2))
    assert "Too many redirects" in str(excinfo.value)
    assert len(calls) == 3


def test_fetch_feed_bytes_enforces_max_bytes_content_length_and_stream(monkeypatch):
    def declared_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"tiny",
            headers={"content-length": "999999", "content-type": "application/rss+xml"},
        )

    _install_transport(monkeypatch, declared_handler)
    with pytest.raises(SubscriptionFetchError) as excinfo:
        asyncio.run(fetch_feed_bytes("http://8.8.8.8/feed", max_bytes=10))
    assert "maximum size" in str(excinfo.value)

    class _OverflowStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"x" * 64
            yield b"x" * 64

    def stream_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_OverflowStream())

    _install_transport(monkeypatch, stream_handler)
    with pytest.raises(SubscriptionFetchError) as excinfo:
        asyncio.run(fetch_feed_bytes("http://8.8.8.8/feed", max_bytes=10))
    assert "maximum size" in str(excinfo.value)


def test_fetch_feed_bytes_success_and_http_error(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/missing":
            return httpx.Response(404)
        return httpx.Response(
            200,
            content=b"<rss version='2.0'><channel><title>t</title></channel></rss>",
            headers={"content-type": "application/rss+xml"},
        )

    _install_transport(monkeypatch, handler)
    data, final_url, content_type = asyncio.run(
        fetch_feed_bytes("http://8.8.8.8/feed")
    )
    assert data.startswith(b"<rss")
    assert final_url == "http://8.8.8.8/feed"
    assert content_type == "application/rss+xml"

    with pytest.raises(SubscriptionFetchError):
        asyncio.run(fetch_feed_bytes("http://8.8.8.8/missing"))


def test_entry_level_ssrf_guard_filters_private_and_metadata_urls(tmp_path: Path):
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml",
        name="Entry Guard",
        seed_only=False,
        max_items_per_check=10,
    )
    _install_discovery(
        service,
        [
            _entry("k-private", "http://127.0.0.1:8080/secret.mp4", "Private"),
            _entry("k-meta", "http://169.254.169.254/latest", "Metadata"),
            _entry("k-public", "https://cdn.example.com/public.mp4", "Public"),
        ],
    )

    result = asyncio.run(service.check_subscription(sub["id"]))

    actions = {e["entry_key"]: e["action"] for e in result["entries"]}
    assert actions["k-private"] == "filtered"
    assert actions["k-meta"] == "filtered"
    assert actions["k-public"] == "queued"
    assert result["skipped_filter_count"] == 2
    assert result["queued_count"] == 1
    assert len(ingest.submitted_job_ids) == 1

    rows, _ = db.list_subscription_entries(sub["id"])
    by_key = {row["entry_key"]: row for row in rows}
    for key in ("k-private", "k-meta"):
        assert by_key[key]["status"] == "skipped"
        assert "local, private, or invalid" in by_key[key]["error"]
    assert by_key["k-public"]["status"] == "queued"


# ---------------------------------------------------------------------------
# 3. YouTube discovery & fallback
# ---------------------------------------------------------------------------


def test_resolve_youtube_rss_url_variants():
    assert (
        resolve_youtube_rss_url("https://www.youtube.com/channel/UCabc123")
        == "https://www.youtube.com/feeds/videos.xml?channel_id=UCabc123"
    )
    assert (
        resolve_youtube_rss_url("https://www.youtube.com/channel/UCabc123/videos")
        == "https://www.youtube.com/feeds/videos.xml?channel_id=UCabc123"
    )
    assert (
        resolve_youtube_rss_url("https://www.youtube.com/playlist?list=PL123abc")
        == "https://www.youtube.com/feeds/videos.xml?playlist_id=PL123abc"
    )
    assert (
        resolve_youtube_rss_url("https://www.youtube.com/user/SomeUser/videos")
        == "https://www.youtube.com/feeds/videos.xml?user=SomeUser"
    )
    existing = "https://www.youtube.com/feeds/videos.xml?channel_id=UCabc123"
    assert resolve_youtube_rss_url(existing) == existing
    assert resolve_youtube_rss_url("https://www.youtube.com/@SomeHandle") is None
    assert resolve_youtube_rss_url("https://www.youtube.com/watch?v=abc") is None
    assert resolve_youtube_rss_url("https://example.com/channel/UCabc") is None


def test_auto_mode_youtube_uses_rss_fast_path(monkeypatch, tmp_path: Path):
    service, _db, _ingest = _make_service(tmp_path)
    fetched: list[str] = []

    async def fake_fetch(url, **kwargs):
        fetched.append(url)
        return (
            ATOM_YOUTUBE_FEED,
            "https://www.youtube.com/feeds/videos.xml?channel_id=UCabc123",
            "application/atom+xml",
        )

    async def unexpected_expand(*args, **kwargs):
        raise AssertionError("yt-dlp must not be used when the RSS feed works")

    monkeypatch.setattr(subs_mod, "fetch_feed_bytes", fake_fetch)
    monkeypatch.setattr(subs_mod, "expand_ytdlp_playlist", unexpected_expand)

    sub = {
        "id": "sub-rss",
        "source_url": "https://www.youtube.com/channel/UCabc123",
        "source_kind": "auto",
        "max_items_per_check": 10,
        "initial_backfill_limit": 0,
    }
    discovery = asyncio.run(service._discover_entries(sub))
    assert discovery["backend"] == "rss"
    assert len(discovery["entries"]) == 2
    assert fetched == [
        "https://www.youtube.com/feeds/videos.xml?channel_id=UCabc123"
    ]


def test_auto_mode_youtube_falls_back_to_ytdlp_on_404_and_empty_feed(
    monkeypatch, tmp_path: Path
):
    service, _db, _ingest = _make_service(tmp_path)
    _public_dns(monkeypatch)
    expand_calls: list[str] = []

    async def fake_expand(url, **kwargs):
        expand_calls.append(url)
        return {
            "playlist_title": "Channel X",
            "webpage_url": url,
            "entries": [
                {"url": "https://www.youtube.com/watch?v=a", "title": "A"},
                {"url": "https://www.youtube.com/watch?v=b", "title": "B"},
            ],
        }

    monkeypatch.setattr(subs_mod, "expand_ytdlp_playlist", fake_expand)

    async def not_found(url, **kwargs):
        raise SubscriptionFetchError("Feed request failed with HTTP 404")

    monkeypatch.setattr(subs_mod, "fetch_feed_bytes", not_found)
    sub = {
        "id": "sub-404",
        "source_url": "https://www.youtube.com/channel/UCmissing",
        "source_kind": "auto",
        "max_items_per_check": 10,
        "initial_backfill_limit": 0,
    }
    discovery = asyncio.run(service._discover_entries(sub))
    assert discovery["backend"] == "ytdlp"
    assert len(discovery["entries"]) == 2
    assert expand_calls == ["https://www.youtube.com/channel/UCmissing"]

    empty_feed = b'<feed xmlns="http://www.w3.org/2005/Atom"><title>Empty</title></feed>'

    async def empty_rss(url, **kwargs):
        return (empty_feed, url, "application/atom+xml")

    monkeypatch.setattr(subs_mod, "fetch_feed_bytes", empty_rss)
    expand_calls.clear()
    discovery = asyncio.run(service._discover_entries(sub))
    assert discovery["backend"] == "ytdlp"
    assert expand_calls == ["https://www.youtube.com/channel/UCmissing"]


def test_auto_mode_youtube_handle_goes_straight_to_ytdlp(monkeypatch, tmp_path: Path):
    service, _db, _ingest = _make_service(tmp_path)
    _public_dns(monkeypatch)
    expand_calls: list[str] = []

    async def fake_expand(url, **kwargs):
        expand_calls.append(url)
        return {
            "playlist_title": "Handle",
            "webpage_url": url,
            "entries": [{"url": "https://www.youtube.com/watch?v=h1", "title": "H1"}],
        }

    async def unexpected_fetch(*args, **kwargs):
        raise AssertionError("RSS must not be fetched for an @handle")

    monkeypatch.setattr(subs_mod, "expand_ytdlp_playlist", fake_expand)
    monkeypatch.setattr(subs_mod, "fetch_feed_bytes", unexpected_fetch)

    sub = {
        "id": "sub-handle",
        "source_url": "https://www.youtube.com/@Creator",
        "source_kind": "auto",
        "max_items_per_check": 10,
        "initial_backfill_limit": 0,
    }
    discovery = asyncio.run(service._discover_entries(sub))
    assert discovery["backend"] == "ytdlp"
    assert expand_calls == ["https://www.youtube.com/@Creator"]


# ---------------------------------------------------------------------------
# 4. First-run flood protection & dry-run
# ---------------------------------------------------------------------------


def test_first_run_seed_only_then_queues_only_new_entry(tmp_path: Path):
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml",
        name="Seed Feed",
        seed_only=True,
        initial_backfill_limit=0,
    )
    initial_entries = [
        _entry("k1", "https://example.com/1.mp4", "One"),
        _entry("k2", "https://example.com/2.mp4", "Two"),
        _entry("k3", "https://example.com/3.mp4", "Three"),
    ]
    _install_discovery(service, initial_entries)

    result = asyncio.run(service.check_subscription(sub["id"]))
    assert result["seeded"] is True
    assert result["queued_count"] == 0
    assert result["seeded_count"] == 3
    assert result["job_ids"] == []
    assert ingest.submitted_job_ids == []

    row = db.get_subscription(sub["id"])
    assert row["seeded_at"] is not None
    assert row["last_status"] == "seeded"
    entries, total = db.list_subscription_entries(sub["id"])
    assert total == 3
    assert {e["status"] for e in entries} == {"seen"}
    assert db.count_jobs() == 0

    # A later check must only queue the genuinely new entry.
    _install_discovery(
        service,
        [*initial_entries, _entry("k4", "https://example.com/4.mp4", "Four")],
    )
    result = asyncio.run(service.check_subscription(sub["id"]))
    assert result["queued_count"] == 1
    assert result["skipped_seen_count"] == 3
    assert len(result["job_ids"]) == 1
    assert ingest.submitted_job_ids == result["job_ids"]
    assert db.count_jobs() == 1

    entries, _ = db.list_subscription_entries(sub["id"])
    by_key = {e["entry_key"]: e for e in entries}
    assert by_key["k4"]["status"] == "queued"
    assert by_key["k1"]["status"] == "seen"


def test_first_run_initial_backfill_limit_queues_top_n_and_seeds_rest(tmp_path: Path):
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml",
        name="Backfill Feed",
        seed_only=True,
        initial_backfill_limit=2,
        max_items_per_check=10,
    )
    entries = [
        _entry("b1", "https://example.com/b1.mp4", "B1"),
        _entry("b2", "https://example.com/b2.mp4", "B2"),
        _entry("b3", "https://example.com/b3.mp4", "B3"),
        _entry("b4", "https://example.com/b4.mp4", "B4"),
    ]
    _install_discovery(service, entries)

    result = asyncio.run(service.check_subscription(sub["id"]))
    assert result["queued_count"] == 2
    assert result["seeded_count"] == 2
    assert len(ingest.submitted_job_ids) == 2
    assert db.count_jobs() == 2

    rows, _ = db.list_subscription_entries(sub["id"])
    statuses = {row["entry_key"]: row["status"] for row in rows}
    assert statuses == {"b1": "queued", "b2": "queued", "b3": "seen", "b4": "seen"}


def test_first_run_seed_only_false_queues_budget_and_defers_rest(tmp_path: Path):
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml",
        name="Deferred Feed",
        seed_only=False,
        max_items_per_check=2,
    )
    entries = [
        _entry("d1", "https://example.com/d1.mp4", "D1"),
        _entry("d2", "https://example.com/d2.mp4", "D2"),
        _entry("d3", "https://example.com/d3.mp4", "D3"),
        _entry("d4", "https://example.com/d4.mp4", "D4"),
    ]
    _install_discovery(service, entries)

    result = asyncio.run(service.check_subscription(sub["id"]))
    actions = {e["entry_key"]: e["action"] for e in result["entries"]}
    assert actions == {
        "d1": "queued",
        "d2": "queued",
        "d3": "deferred",
        "d4": "deferred",
    }
    assert result["queued_count"] == 2
    assert result["seeded_count"] == 0
    assert len(ingest.submitted_job_ids) == 2

    rows, total = db.list_subscription_entries(sub["id"])
    assert total == 2
    assert {row["status"] for row in rows} == {"queued"}


def test_dry_run_reports_actions_without_touching_database(tmp_path: Path):
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml",
        name="Dry Feed",
        seed_only=True,
        initial_backfill_limit=1,
        max_items_per_check=10,
    )

    _insert_media(db, "m-dup", "https://example.com/duplicate.mp4")
    db.upsert_subscription_entry(
        sub["id"],
        "k-seen",
        title="Seen",
        url="https://example.com/seen.mp4",
        status="seen",
    )
    _install_discovery(
        service,
        [
            _entry("k-seen", "https://example.com/seen.mp4", "Seen"),
            _entry("k-dup", "https://example.com/duplicate.mp4", "Duplicate"),
            _entry("k-private", "http://127.0.0.1:8080/secret.mp4", "Private"),
            _entry("k-new-1", "https://example.com/new1.mp4", "New 1"),
            _entry("k-new-2", "https://example.com/new2.mp4", "New 2"),
        ],
    )

    before_count = db.list_subscription_entries(sub["id"])[1]
    result = asyncio.run(service.check_subscription(sub["id"], dry_run=True))
    actions = {e["entry_key"]: e["action"] for e in result["entries"]}

    assert result["dry_run"] is True
    assert actions["k-seen"] == "seen"
    assert actions["k-dup"] == "duplicate"
    assert actions["k-private"] == "filtered"
    assert actions["k-new-1"] == "would_queue"
    assert actions["k-new-2"] == "would_seed"
    assert result["queued_count"] == 1
    assert result["seeded_count"] == 1
    assert result["skipped_seen_count"] == 1
    assert result["skipped_duplicate_count"] == 1
    assert result["skipped_filter_count"] == 1
    assert result["job_ids"] == []
    assert ingest.submitted_job_ids == []

    after_count = db.list_subscription_entries(sub["id"])[1]
    assert after_count == before_count
    row = db.get_subscription(sub["id"])
    assert row["seeded_at"] is None
    assert row["last_checked_at"] is None
    assert row["last_status"] == "pending"
    assert db.count_jobs() == 0

    # A completely clean subscription records nothing at all.
    clean = service.create_subscription(
        source_url="https://example.com/clean.xml",
        name="Clean Dry Feed",
        seed_only=True,
        initial_backfill_limit=0,
    )
    _install_discovery(
        service,
        [_entry("c1", "https://example.com/c1.mp4", "C1")],
    )
    clean_result = asyncio.run(service.check_subscription(clean["id"], dry_run=True))
    assert clean_result["seeded_count"] == 1
    assert db.list_subscription_entries(clean["id"])[1] == 0
    assert db.count_jobs() == 0
    assert db.get_subscription(clean["id"])["seeded_at"] is None


# ---------------------------------------------------------------------------
# 5. Dual deduplication & regex filters
# ---------------------------------------------------------------------------


def test_seen_entry_stays_seen_even_after_media_row_is_deleted(tmp_path: Path):
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml",
        name="Dedup Feed",
        seed_only=False,
    )
    url = "https://example.com/already.mp4"
    _insert_media(db, "m-seen", url)
    _install_discovery(service, [_entry("k-seen", url, "Already")])

    first = asyncio.run(service.check_subscription(sub["id"]))
    assert first["skipped_duplicate_count"] == 1
    assert first["queued_count"] == 0
    assert db.count_jobs() == 0

    row = db.list_subscription_entries(sub["id"])[0][0]
    assert row["status"] == "seen"
    assert row["media_id"] == "m-seen"

    # The vault row disappearing must not make the feed entry eligible again.
    assert db.delete_media("m-seen") is not None
    second = asyncio.run(service.check_subscription(sub["id"]))
    assert second["skipped_seen_count"] == 1
    assert second["queued_count"] == 0
    assert db.count_jobs() == 0

    row = db.list_subscription_entries(sub["id"])[0][0]
    assert row["status"] == "seen"
    assert row["media_id"] == "m-seen"


def test_manual_ingest_url_is_recorded_as_duplicate_and_never_requeued(tmp_path: Path):
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml",
        name="Duplicate Feed",
        seed_only=False,
    )
    url = "https://example.com/manual.mp4"
    _insert_media(db, "m-manual", url)
    _install_discovery(service, [_entry("k-manual", url, "Manual")])

    result = asyncio.run(service.check_subscription(sub["id"]))
    assert result["skipped_duplicate_count"] == 1
    assert result["queued_count"] == 0
    assert ingest.submitted_job_ids == []
    assert db.count_jobs() == 0

    rows, total = db.list_subscription_entries(sub["id"])
    assert total == 1
    assert rows[0]["status"] == "seen"
    assert rows[0]["media_id"] == "m-manual"

    second = asyncio.run(service.check_subscription(sub["id"]))
    assert second["skipped_seen_count"] == 1
    assert second["queued_count"] == 0
    assert db.count_jobs() == 0


def test_title_include_and_exclude_regex_are_case_insensitive(tmp_path: Path):
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml",
        name="Regex Feed",
        seed_only=False,
        title_include_regex="trailer",
        title_exclude_regex="behind",
        max_items_per_check=10,
    )
    _install_discovery(
        service,
        [
            _entry("k-match", "https://example.com/match.mp4", "BIG TRAILER 2026"),
            _entry("k-excluded", "https://example.com/excluded.mp4", "BEHIND the scenes"),
            _entry("k-miss", "https://example.com/miss.mp4", "Random clip"),
        ],
    )

    result = asyncio.run(service.check_subscription(sub["id"]))
    actions = {e["entry_key"]: e["action"] for e in result["entries"]}
    assert actions["k-match"] == "queued"
    assert actions["k-excluded"] == "filtered"
    assert actions["k-miss"] == "filtered"
    assert result["queued_count"] == 1
    assert result["skipped_filter_count"] == 2

    rows, _ = db.list_subscription_entries(sub["id"])
    by_key = {row["entry_key"]: row for row in rows}
    assert by_key["k-match"]["status"] == "queued"
    assert by_key["k-excluded"]["status"] == "skipped"
    assert by_key["k-miss"]["status"] == "skipped"


# ---------------------------------------------------------------------------
# 6. Error backoff, auto-pause, re-enable reset & single-flight lock
# ---------------------------------------------------------------------------


def test_discovery_failures_backoff_then_auto_pause(tmp_path: Path):
    service, db, _ingest = _make_service(
        tmp_path, subscriptions_failure_pause_threshold=3
    )
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml",
        name="Failing Feed",
        interval_minutes=60,
    )

    async def failing_discover(_sub):
        raise SubscriptionFetchError("simulated discovery failure")

    service._discover_entries = failing_discover

    for attempt in (1, 2):
        with pytest.raises(SubscriptionFetchError):
            asyncio.run(service.check_subscription(sub["id"]))
        row = db.get_subscription(sub["id"])
        assert row["consecutive_failures"] == attempt
        assert row["last_status"] == "error"
        assert row["enabled"] is True
        assert row["next_check_at"] is not None
        next_at = datetime.fromisoformat(row["next_check_at"])
        assert next_at > datetime.now(timezone.utc)

    with pytest.raises(SubscriptionFetchError):
        asyncio.run(service.check_subscription(sub["id"]))
    row = db.get_subscription(sub["id"])
    assert row["consecutive_failures"] == 3
    assert row["enabled"] is False
    assert row["last_status"] == "paused_error"
    assert row["next_check_at"] is None
    assert "simulated discovery failure" in row["last_error"]


def test_patch_reenable_resets_failure_state_and_reschedules(tmp_path: Path):
    app, _settings = _make_api_app(tmp_path)
    db = app.state.db
    sub = db.create_subscription(
        name="Paused Feed",
        source_url="https://example.com/feed.xml",
        enabled=True,
    )
    db.update_subscription(
        sub["id"],
        enabled=False,
        consecutive_failures=4,
        last_error="boom",
        last_status="paused_error",
        next_check_at=None,
    )

    with TestClient(app) as client:
        client.headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
        resp = client.patch(f"/api/subscriptions/{sub['id']}", json={"enabled": True})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["enabled"] is True
        assert body["consecutive_failures"] == 0
        assert body["last_error"] is None
        assert body["last_status"] == "pending"
        assert body["next_check_at"] is not None


def test_concurrent_check_returns_409_while_lock_is_held(tmp_path: Path):
    app, _settings = _make_api_app(tmp_path)
    service = app.state.subscriptions
    entered = threading.Event()
    release = threading.Event()

    async def slow_discovery(sub):
        entered.set()
        await asyncio.to_thread(release.wait, 5.0)
        return {
            "backend": "rss",
            "resolved_url": sub.get("source_url"),
            "feed_title": "",
            "entries": [],
        }

    service._discover_entries = slow_discovery

    with TestClient(app) as client:
        client.headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
        created = client.post(
            "/api/subscriptions",
            json={"source_url": "https://example.com/feed.xml", "name": "Lock Feed"},
        )
        assert created.status_code == 201, created.text
        sub_id = created.json()["id"]

        first_result: dict = {}

        def run_first() -> None:
            first_result["response"] = client.post(
                f"/api/subscriptions/{sub_id}/check"
            )

        thread = threading.Thread(target=run_first)
        thread.start()
        try:
            assert entered.wait(5.0), "first check never started discovery"
            second = client.post(f"/api/subscriptions/{sub_id}/check")
            assert second.status_code == 409, second.text
        finally:
            release.set()
            thread.join(5.0)
        assert not thread.is_alive()
        assert first_result["response"].status_code == 200


# ---------------------------------------------------------------------------
# 7. Job completion tracking & startup reconciliation
# ---------------------------------------------------------------------------


def test_run_ingest_job_success_updates_entry_and_count_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _public_dns(monkeypatch)
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml", name="Job Feed"
    )
    entry_key = "guid:job-1"
    url = "https://example.com/job1.mp4"
    db.upsert_subscription_entry(
        sub["id"], entry_key, title="Job 1", url=url, status="queued"
    )
    job_id = db.create_job("subscription", "url", url)

    ingest.process_result = [{"id": "m-job-1"}]

    async def run():
        await service._run_ingest_job(
            job_id=job_id,
            sub_id=sub["id"],
            entry_key=entry_key,
            url=url,
            folder="General",
            tags=[],
            title="Job 1",
            format_spec=None,
            forward_to_telegram=False,
        )

    asyncio.run(run())
    entry = db.list_subscription_entries(sub["id"])[0][0]
    assert entry["status"] == "ingested"
    assert entry["media_id"] == "m-job-1"
    assert db.get_subscription(sub["id"])["items_ingested_count"] == 1

    # A deduplicated ingest must not double-increment the archived counter.
    ingest.process_result = [{"id": "m-job-1", "deduplicated": True}]
    asyncio.run(run())
    assert db.get_subscription(sub["id"])["items_ingested_count"] == 1
    entry = db.list_subscription_entries(sub["id"])[0][0]
    assert entry["status"] == "ingested"


def test_run_ingest_job_failure_marks_entry_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _public_dns(monkeypatch)
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml", name="Fail Feed"
    )
    entry_key = "guid:fail-1"
    url = "https://example.com/fail1.mp4"
    db.upsert_subscription_entry(
        sub["id"], entry_key, title="Fail 1", url=url, status="queued"
    )
    job_id = db.create_job("subscription", "url", url)
    ingest.process_error = RuntimeError("download exploded")

    async def run():
        await service._run_ingest_job(
            job_id=job_id,
            sub_id=sub["id"],
            entry_key=entry_key,
            url=url,
            folder="General",
            tags=[],
            title="Fail 1",
            format_spec=None,
            forward_to_telegram=False,
        )

    with pytest.raises(RuntimeError):
        asyncio.run(run())
    entry = db.list_subscription_entries(sub["id"])[0][0]
    assert entry["status"] == "failed"
    assert "download exploded" in entry["error"]
    assert db.get_subscription(sub["id"])["items_ingested_count"] == 0


def test_reconcile_subscription_entries_with_done_failed_cancelled_and_missing_jobs(
    tmp_path: Path,
):
    _service, db, _ingest = _make_service(tmp_path)
    sub = db.create_subscription(
        name="Reconcile Feed", source_url="https://example.com/feed.xml"
    )

    def add_entry(key: str, job_id: str) -> None:
        db.upsert_subscription_entry(
            sub["id"],
            key,
            title=key,
            url=f"https://example.com/{key}.mp4",
            status="queued",
            job_id=job_id,
        )

    done_job = db.create_job("subscription", "url", "https://example.com/done.mp4")
    db.update_job(done_job, status="done", media_ids_json=json.dumps(["m-done"]))
    failed_job = db.create_job("subscription", "url", "https://example.com/failed.mp4")
    db.update_job(failed_job, status="failed", message="boom")
    cancelled_job = db.create_job(
        "subscription", "url", "https://example.com/cancelled.mp4"
    )
    db.update_job(cancelled_job, status="cancelled")

    add_entry("done", done_job)
    add_entry("failed", failed_job)
    add_entry("cancelled", cancelled_job)
    add_entry("missing", "deadbeefdeadbeef")

    reconciled = db.reconcile_subscription_entries_with_jobs()
    assert reconciled == 4

    rows, _ = db.list_subscription_entries(sub["id"])
    by_key = {row["entry_key"]: row for row in rows}
    assert by_key["done"]["status"] == "ingested"
    assert by_key["done"]["media_id"] == "m-done"
    assert by_key["done"]["error"] is None
    assert by_key["failed"]["status"] == "failed"
    assert by_key["failed"]["error"] == "boom"
    assert by_key["cancelled"]["status"] == "skipped"
    assert by_key["cancelled"]["error"] == "Cancelled"
    assert by_key["missing"]["status"] == "failed"
    assert by_key["missing"]["error"] == "Job failed"


# ---------------------------------------------------------------------------
# 8. Scheduler lifecycle
# ---------------------------------------------------------------------------


def test_scheduler_start_stop_and_disabled_setting(tmp_path: Path):
    service, _db, _ingest = _make_service(
        tmp_path / "enabled", subscriptions_poll_tick_sec=0.01
    )

    async def enabled_scenario():
        service.start_scheduler()
        task = service._scheduler_task
        assert task is not None
        await asyncio.sleep(0.03)
        assert not task.done()
        assert service.get_health_summary()["scheduler_running"] is True
        await service.stop_scheduler()
        assert service._scheduler_task is None
        assert task.done()

    asyncio.run(enabled_scenario())

    disabled, _db2, _ingest2 = _make_service(
        tmp_path / "disabled", subscriptions_enabled=False
    )

    async def disabled_scenario():
        disabled.start_scheduler()
        assert disabled._scheduler_task is None
        assert disabled.get_health_summary()["scheduler_running"] is False

    asyncio.run(disabled_scenario())


def test_poll_due_subscriptions_only_polls_due_enabled_rows(tmp_path: Path):
    service, db, _ingest = _make_service(tmp_path)
    due = db.create_subscription(
        name="Due Feed",
        source_url="https://example.com/due.xml",
        next_check_at="2000-01-01T00:00:00+00:00",
    )
    future = db.create_subscription(
        name="Future Feed",
        source_url="https://example.com/future.xml",
        next_check_at="2999-01-01T00:00:00+00:00",
    )
    disabled = db.create_subscription(
        name="Disabled Feed",
        source_url="https://example.com/disabled.xml",
        enabled=False,
    )
    _install_discovery(service, [])

    async def scenario():
        first = await service.poll_due_subscriptions()
        assert [report["subscription_id"] for report in first] == [due["id"]]
        assert db.get_subscription(future["id"])["last_checked_at"] is None
        assert db.get_subscription(disabled["id"])["last_checked_at"] is None
        second = await service.poll_due_subscriptions()
        assert second == []

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# 9. REST API, Auth, System Status & Telegram /subs
# ---------------------------------------------------------------------------


def test_subscriptions_api_auth_crud_entries_pagination_and_validation(tmp_path: Path):
    app, _settings = _make_api_app(tmp_path)

    with TestClient(app) as client:
        # Unauthenticated requests are rejected before touching the service.
        unauth_calls = [
            client.get("/api/subscriptions"),
            client.post("/api/subscriptions", json={"source_url": "https://example.com/f.xml"}),
            client.get("/api/subscriptions/anything"),
            client.patch("/api/subscriptions/anything", json={"name": "x"}),
            client.delete("/api/subscriptions/anything"),
            client.get("/api/subscriptions/anything/entries"),
            client.post("/api/subscriptions/anything/check"),
        ]
        for response in unauth_calls:
            assert response.status_code == 401, response.text

        client.headers["Authorization"] = f"Bearer {AUTH_TOKEN}"

        created = client.post(
            "/api/subscriptions",
            json={
                "source_url": "https://example.com/feed.xml",
                "name": "API Feed",
                "folder": "Inbox",
                "tags": ["a", "b"],
                "interval_minutes": 60,
                "max_items_per_check": 10,
                "seed_only": True,
                "initial_backfill_limit": 0,
            },
        )
        assert created.status_code == 201, created.text
        sub = created.json()
        sub_id = sub["id"]
        assert sub["name"] == "API Feed"
        assert sub["source_url"] == "https://example.com/feed.xml"
        assert sub["tags"] == ["a", "b"]
        assert sub["enabled"] is True

        listing = client.get("/api/subscriptions")
        assert listing.status_code == 200
        body = listing.json()
        assert [item["id"] for item in body["subscriptions"]] == [sub_id]
        for key in (
            "total",
            "enabled",
            "paused",
            "errored",
            "paused_error",
            "total_entries",
            "total_ingested",
            "next_check_at",
            "scheduler_running",
            "enabled_in_config",
            "active_polls",
            "poll_tick_sec",
        ):
            assert key in body["summary"]

        fetched = client.get(f"/api/subscriptions/{sub_id}")
        assert fetched.status_code == 200
        assert fetched.json()["id"] == sub_id

        patched = client.patch(
            f"/api/subscriptions/{sub_id}",
            json={"name": "Renamed Feed", "folder": "Movies", "max_items_per_check": 5},
        )
        assert patched.status_code == 200, patched.text
        patched_body = patched.json()
        assert patched_body["name"] == "Renamed Feed"
        assert patched_body["folder"] == "Movies"
        assert patched_body["max_items_per_check"] == 5

        # Invalid regex must be rejected on both create and update.
        bad_create = client.post(
            "/api/subscriptions",
            json={
                "source_url": "https://example.com/bad.xml",
                "title_include_regex": "(",
            },
        )
        assert bad_create.status_code == 400
        bad_patch = client.patch(
            f"/api/subscriptions/{sub_id}", json={"title_exclude_regex": "["}
        )
        assert bad_patch.status_code == 400

        # Unknown ids are 404 for every operation.
        assert client.get("/api/subscriptions/unknown-id").status_code == 404
        assert (
            client.patch(
                "/api/subscriptions/unknown-id", json={"name": "x"}
            ).status_code
            == 404
        )
        assert client.delete("/api/subscriptions/unknown-id").status_code == 404
        assert client.get("/api/subscriptions/unknown-id/entries").status_code == 404

        db = app.state.db
        for idx in range(4):
            db.upsert_subscription_entry(
                sub_id,
                f"seen-{idx}",
                title=f"Seen {idx}",
                url=f"https://example.com/seen{idx}.mp4",
                status="seen",
            )
        db.upsert_subscription_entry(
            sub_id,
            "failed-0",
            title="Failed 0",
            url="https://example.com/failed0.mp4",
            status="failed",
            error="boom",
        )

        empty_page = client.get(f"/api/subscriptions/{sub_id}/entries?limit=2")
        assert empty_page.status_code == 200
        page = empty_page.json()
        assert page["total"] == 5
        assert len(page["entries"]) == 2
        assert page["has_more"] is True

        last_page = client.get(
            f"/api/subscriptions/{sub_id}/entries?limit=2&offset=4"
        )
        assert last_page.status_code == 200
        last = last_page.json()
        assert last["total"] == 5
        assert len(last["entries"]) == 1
        assert last["has_more"] is False

        failed_page = client.get(
            f"/api/subscriptions/{sub_id}/entries?status=failed"
        )
        assert failed_page.status_code == 200
        assert failed_page.json()["total"] == 1
        assert failed_page.json()["entries"][0]["status"] == "failed"

        invalid_filter = client.get(
            f"/api/subscriptions/{sub_id}/entries?status=bogus"
        )
        assert invalid_filter.status_code == 400

        deleted = client.delete(f"/api/subscriptions/{sub_id}")
        assert deleted.status_code == 200
        assert deleted.json()["deleted"] is True
        assert client.get(f"/api/subscriptions/{sub_id}").status_code == 404


def test_api_dry_run_query_and_json_body_do_not_mutate(tmp_path: Path):
    app, _settings = _make_api_app(tmp_path)
    service = app.state.subscriptions

    with TestClient(app) as client:
        client.headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
        created = client.post(
            "/api/subscriptions",
            json={
                "source_url": "https://example.com/feed.xml",
                "name": "Dry API Feed",
                "seed_only": True,
                "initial_backfill_limit": 0,
            },
        )
        assert created.status_code == 201
        sub_id = created.json()["id"]

        _install_discovery(
            service,
            [
                _entry("a1", "https://example.com/a1.mp4", "A1"),
                _entry("a2", "https://example.com/a2.mp4", "A2"),
            ],
        )

        query_dry = client.post(f"/api/subscriptions/{sub_id}/check?dry_run=true")
        assert query_dry.status_code == 200, query_dry.text
        body = query_dry.json()
        assert body["dry_run"] is True
        assert body["queued_count"] == 0
        assert body["seeded_count"] == 2
        assert {e["action"] for e in body["entries"]} == {"would_seed"}

        db = app.state.db
        assert db.list_subscription_entries(sub_id)[1] == 0
        assert db.count_jobs() == 0
        assert db.get_subscription(sub_id)["seeded_at"] is None

        body_dry = client.post(
            f"/api/subscriptions/{sub_id}/check", json={"dry_run": True}
        )
        assert body_dry.status_code == 200, body_dry.text
        assert body_dry.json()["dry_run"] is True
        assert db.list_subscription_entries(sub_id)[1] == 0
        assert db.count_jobs() == 0
        assert db.get_subscription(sub_id)["seeded_at"] is None


def test_system_status_and_health_include_subscription_summary(tmp_path: Path):
    app, _settings = _make_api_app(tmp_path)
    db = app.state.db
    db.create_subscription(name="Status Feed", source_url="https://example.com/f.xml")

    with TestClient(app) as client:
        client.headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
        status = client.get("/api/system/status")
        assert status.status_code == 200
        body = status.json()
        assert body["database"]["subscriptions_count"] == 1
        assert body["subscriptions"]["total"] == 1
        assert body["subscriptions"]["enabled"] == 1
        assert body["subscriptions"]["enabled_in_config"] is False
        assert body["subscriptions"]["scheduler_running"] is False

        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["subscriptions_enabled"] is False

    enabled_app, _enabled_settings = _make_api_app(tmp_path / "enabled", enabled=True)
    with TestClient(enabled_app) as client:
        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["subscriptions_enabled"] is True


def test_telegram_subs_command_empty_populated_check_and_unknown(tmp_path: Path):
    settings = _settings_for(
        tmp_path, subscriptions_enabled=False, web_auth_token=""
    )
    app = create_app(settings)
    pool = app.state.tg_pool
    service = app.state.subscriptions

    assert asyncio.run(pool.handle_subs_command("")) == "No subscriptions configured yet."

    original = pool.subscriptions
    pool.subscriptions = None
    try:
        assert (
            asyncio.run(pool.handle_subs_command(""))
            == "Subscriptions service is unavailable."
        )
    finally:
        pool.subscriptions = original

    first = service.create_subscription(
        source_url="https://example.com/one.xml", name="Feed One"
    )
    service.create_subscription(
        source_url="https://example.com/two.xml", name="Feed Two"
    )

    listing = asyncio.run(pool.handle_subs_command(""))
    assert "Subscriptions (2)" in listing
    assert first["id"][:12] in listing
    assert "Feed One" in listing
    assert "Run `/subs check <id>`" in listing

    _install_discovery(service, [])

    checked = asyncio.run(pool.handle_subs_command(f"check {first['id'][:6]}"))
    assert "Checked **Feed One**" in checked
    assert "0 discovered, 0 queued, 0 seeded." in checked

    unknown = asyncio.run(pool.handle_subs_command("check zzzzzz"))
    assert "not found" in unknown

    usage = asyncio.run(pool.handle_subs_command("bogus"))
    assert usage == "Usage: `/subs` or `/subs check <id>`"


# ---------------------------------------------------------------------------
# 10. Frontend static guardrails
# ---------------------------------------------------------------------------


def test_frontend_subscription_and_maintenance_assets_pass_syntax_and_guardrails():
    node = shutil.which("node")
    if not node:
        pytest.skip("node is not installed")

    for rel in (
        "js/features/subscriptions.js",
        "js/features/maintenance.js",
    ):
        path = STATIC_DIR / rel
        assert path.is_file(), rel
        source = path.read_text(encoding="utf-8")
        checked = subprocess.run(
            [node, "--input-type=module", "--check"],
            input=source,
            text=True,
            capture_output=True,
        )
        assert checked.returncode == 0, f"{rel}: {checked.stderr}"
        for forbidden in ("window.alert(", "window.confirm(", "window.prompt("):
            assert forbidden not in source, f"{forbidden} found in {rel}"

    index_source = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    assert len(index_source.splitlines()) < 350
    assert 'id="subscriptionsBtn"' in index_source
    assert 'id="sheetSubscriptionsBtn"' in index_source


# ---------------------------------------------------------------------------
# 11. Wave 3 security & hardening regressions
# ---------------------------------------------------------------------------


def test_run_ingest_job_blocks_dns_private_hostname_and_marks_job_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _public_dns(monkeypatch, ip="169.254.169.254")
    service, db, ingest = _make_service(tmp_path)
    sub = service.create_subscription(
        source_url="https://example.com/feed.xml", name="SSRF Feed"
    )
    entry_key = "guid:ssrf-1"
    url = "https://evil.example.com/media.mp4"
    db.upsert_subscription_entry(
        sub["id"], entry_key, title="SSRF Entry", url=url, status="queued"
    )
    job_id = db.create_job("subscription", "url", url)

    async def run():
        await service._run_ingest_job(
            job_id=job_id,
            sub_id=sub["id"],
            entry_key=entry_key,
            url=url,
            folder="General",
            tags=[],
            title="SSRF Entry",
            format_spec=None,
            forward_to_telegram=False,
        )

    with pytest.raises(subs_mod.SubscriptionValidationError):
        asyncio.run(run())

    assert ingest.process_calls == []
    entry = db.list_subscription_entries(sub["id"])[0][0]
    assert entry["status"] == "failed"
    assert entry["error"]
    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == "failed"


def test_ingest_ddl_redirect_to_private_host_blocked_when_block_private_hosts_true(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    import app.ingest as ingest_mod

    _public_dns(monkeypatch, ip="93.184.216.34")
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        if request.url.host == "public.example.com":
            return httpx.Response(
                302,
                headers={"Location": "http://169.254.169.254/latest/meta-data/"},
                request=request,
            )
        return httpx.Response(200, content=b"secret", request=request)

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(ingest_mod.httpx, "AsyncClient", factory)

    with pytest.raises(subs_mod.SubscriptionValidationError):
        asyncio.run(
            ingest_mod.is_direct_download(
                "https://public.example.com/media", block_private_hosts=True
            )
        )

    with pytest.raises(subs_mod.SubscriptionValidationError):
        asyncio.run(
            ingest_mod._download_ddl(
                "https://public.example.com/video.mp4",
                tmp_path,
                block_private_hosts=True,
            )
        )

    assert all("169.254.169.254" not in u for u in seen_urls)


def test_redos_nested_quantifier_regex_rejected_in_service_and_api(tmp_path: Path):
    for bad in ("^(a+)+b$", "(a|a)*b", r"(foo\1)+"):
        with pytest.raises(subs_mod.SubscriptionValidationError):
            subs_mod.validate_regex_pattern(bad, "title_include_regex")

    assert (
        subs_mod.validate_regex_pattern(r"^Episode\s+\d{1,4}:", "title_include_regex")
        == r"^Episode\s+\d{1,4}:"
    )

    app, _settings = _make_api_app(tmp_path)
    with TestClient(app) as client:
        client.headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
        bad_create = client.post(
            "/api/subscriptions",
            json={
                "source_url": "https://example.com/feed.xml",
                "title_include_regex": "^(a+)+b$",
            },
        )
        assert bad_create.status_code == 400

        created = client.post(
            "/api/subscriptions",
            json={
                "source_url": "https://example.com/feed.xml",
                "title_include_regex": r"^Episode\s+\d+$",
            },
        )
        assert created.status_code == 201
        sub_id = created.json()["id"]

        bad_patch = client.patch(
            f"/api/subscriptions/{sub_id}",
            json={"title_exclude_regex": "(a|a)*b"},
        )
        assert bad_patch.status_code == 400


def test_service_bounds_and_credential_redaction(tmp_path: Path):
    service, db, _ingest = _make_service(tmp_path)

    for bad_kwargs in (
        {"interval_minutes": 4},
        {"interval_minutes": 10081},
        {"max_items_per_check": 0},
        {"max_items_per_check": 101},
        {"initial_backfill_limit": -1},
        {"initial_backfill_limit": 51},
    ):
        with pytest.raises(subs_mod.SubscriptionValidationError):
            service.create_subscription(
                source_url="https://example.com/feed.xml", **bad_kwargs
            )

    sub = service.create_subscription(
        source_url="https://example.com/feed.xml", name="Bounds Feed"
    )
    for bad_patch in (
        {"interval_minutes": 4},
        {"interval_minutes": 10081},
        {"max_items_per_check": 0},
        {"max_items_per_check": 101},
        {"initial_backfill_limit": -1},
        {"initial_backfill_limit": 51},
    ):
        with pytest.raises(subs_mod.SubscriptionValidationError):
            service.update_subscription(sub["id"], **bad_patch)

    redacted = subs_mod._redact_credentials(
        "Proxy error via http://alice:s3cr3t@proxy.example.com:8080/path"
    )
    assert "http://***@proxy.example.com:8080/path" in redacted
    assert "alice" not in redacted
    assert "s3cr3t" not in redacted

    async def failing_discover(_sub, max_items=10):
        raise subs_mod.SubscriptionFetchError(
            "connect failed via http://bob:topsecret@proxy.example.com:8080"
        )

    service._discover_entries = failing_discover  # type: ignore[method-assign]
    with pytest.raises(subs_mod.SubscriptionFetchError) as exc_info:
        asyncio.run(service.check_subscription(sub["id"]))

    assert "topsecret" not in str(exc_info.value)
    assert "http://***@proxy.example.com:8080" in str(exc_info.value)
    stored = db.get_subscription(sub["id"])
    assert stored is not None
    assert "topsecret" not in (stored["last_error"] or "")
    assert "http://***@proxy.example.com:8080" in (stored["last_error"] or "")


def test_queue_existing_entry_and_backfill_api(tmp_path: Path):
    app, _settings = _make_api_app(tmp_path, enabled=False)
    headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}
    subs = app.state.subscriptions
    db = app.state.db

    submitted: list[str] = []
    app.state.ingest.submit_job = lambda job_id, coro: (submitted.append(job_id), coro.close())[0]

    with TestClient(app) as client:
        created = client.post(
            "/api/subscriptions",
            json={
                "source_url": "https://example.com/feed.xml",
                "name": "Seeded Feed",
                "seed_only": True,
                "initial_backfill_limit": 0,
                "max_items_per_check": 2,
            },
            headers=headers,
        ).json()
        sub_id = created["id"]

        # Seed 3 entries via first check
        _install_discovery(
            subs,
            [
                _entry("e1", "https://example.com/v1.mp4", "Video 1"),
                _entry("e2", "https://example.com/v2.mp4", "Video 2"),
                _entry("e3", "https://example.com/v3.mp4", "Video 3"),
            ],
        )
        r_check = client.post(f"/api/subscriptions/{sub_id}/check", headers=headers)
        assert r_check.status_code == 200
        assert r_check.json()["seeded_count"] == 3
        assert r_check.json()["queued_count"] == 0

        # Queue one specific seen entry
        r_one = client.post(
            f"/api/subscriptions/{sub_id}/entries/queue",
            json={"entry_key": "e1"},
            headers=headers,
        )
        assert r_one.status_code == 200
        assert r_one.json()["queued"] is True
        assert len(submitted) == 1

        # Re-queuing already queued entry returns queued=False cleanly
        r_again = client.post(
            f"/api/subscriptions/{sub_id}/entries/queue",
            json={"entry_key": "e1"},
            headers=headers,
        )
        assert r_again.status_code == 200
        assert r_again.json()["queued"] is False
        assert r_again.json()["reason"] == "queued"

        # Backfill remaining seen entries (max_items_per_check=2 -> e2 and e3)
        _insert_media(db, "m-dup", "https://example.com/v3.mp4", title="Video 3")
        r_backfill = client.post(
            f"/api/subscriptions/{sub_id}/backfill",
            json={},
            headers=headers,
        )
        assert r_backfill.status_code == 200
        bf = r_backfill.json()
        assert bf["queued_count"] == 1
        assert bf["skipped_duplicate_count"] == 1
        assert len(submitted) == 2

        # Missing entry returns 404
        r_missing = client.post(
            f"/api/subscriptions/{sub_id}/entries/queue",
            json={"entry_key": "does-not-exist"},
            headers=headers,
        )
        assert r_missing.status_code == 404


