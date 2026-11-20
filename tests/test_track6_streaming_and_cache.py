from __future__ import annotations

import asyncio
import io
import json
import re
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi.testclient import TestClient

from app.backup import restore_backup_file, verify_backup_file
from app.config import TelegramApiConfig
from app.main import create_app
from app.telegram_mtproto import TelegramPool
from tests.helpers import STATIC_DIR, _make_settings


def _insert_local_media(
    db,
    *,
    media_id: str,
    filename: str,
    path: Path,
    payload: bytes,
    folder: str = "General",
    mime_type: str = "application/octet-stream",
) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return db.insert_media(
        media_id=media_id,
        title=Path(filename).stem or media_id,
        filename=filename,
        folder=folder,
        media_type="file",
        mime_type=mime_type,
        size_bytes=len(payload),
        drive_name="drive1",
        file_path=str(path),
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
    )


def _insert_telegram_media(
    db,
    *,
    media_id: str,
    filename: str,
    size_bytes: int,
    chat_id: str = "-100123456",
    msg_id: int = 1001,
    worker_name: str = "bot1",
    folder: str = "General",
    mime_type: str = "application/octet-stream",
    parts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return db.insert_media(
        media_id=media_id,
        title=Path(filename).stem or media_id,
        filename=filename,
        folder=folder,
        media_type="file",
        mime_type=mime_type,
        size_bytes=size_bytes,
        drive_name="telegram",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
        telegram_chat_id=chat_id,
        telegram_topic_id=1,
        telegram_msg_id=msg_id,
        telegram_worker_name=worker_name,
        part_count=len(parts) if parts else 1,
        parts=parts,
    )


class _DictBackedTelegramClient:
    """Deterministic fake Telethon client backed by {msg_id: bytes}."""

    def __init__(self, name: str, messages: dict[int, bytes], *, chunk_size: int = 16384) -> None:
        self.name = name
        self.messages = messages
        self.chunk_size = chunk_size
        self.get_messages_calls: list[int] = []
        self.iter_download_calls: list[tuple[int, int, int]] = []
        self._connected = True

    def is_connected(self) -> bool:
        return self._connected

    async def get_input_entity(self, target: Any) -> Any:
        return target

    async def get_messages(self, peer: Any, ids: int) -> Any:
        self.get_messages_calls.append(int(ids))
        payload = self.messages.get(int(ids))
        if payload is None:
            return None
        return SimpleNamespace(media=SimpleNamespace(msg_id=int(ids), size=len(payload)))

    def iter_download(self, media: Any, *, offset: int = 0, request_size: int = 524288):
        msg_id = int(media.msg_id)
        payload = self.messages[msg_id]
        self.iter_download_calls.append((msg_id, int(offset), int(request_size)))

        async def _gen():
            pos = int(offset)
            step = min(self.chunk_size, max(1, int(request_size)))
            while pos < len(payload):
                end = min(len(payload), pos + step)
                yield payload[pos:end]
                pos = end

        return _gen()

    async def disconnect(self) -> None:
        self._connected = False


async def _run_asgi_stream(
    app,
    *,
    method: str,
    path: str,
    body: bytes = b"",
    headers: dict[str, str] | None = None,
    on_body_chunk=None,
    disconnect_after_chunks: int | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """Drive an ASGI HTTP request directly so response chunks are observed incrementally."""
    raw_headers = [(b"host", b"testserver")]
    for k, v in (headers or {}).items():
        raw_headers.append((k.lower().encode("latin-1"), v.encode("latin-1")))
    if body and "content-length" not in {k.lower() for k in (headers or {})}:
        raw_headers.append((b"content-length", str(len(body)).encode("latin-1")))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.1"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": raw_headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }

    sent_request = False
    disconnect_event = asyncio.Event()
    status_code = 0
    resp_headers: dict[str, str] = {}
    collected = bytearray()
    chunk_count = 0

    async def receive():
        nonlocal sent_request
        if not sent_request:
            sent_request = True
            return {"type": "http.request", "body": body, "more_body": False}
        await disconnect_event.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status_code, resp_headers, chunk_count
        msg_type = message["type"]
        if msg_type == "http.response.start":
            status_code = int(message["status"])
            resp_headers = {
                k.decode("latin-1").lower(): v.decode("latin-1")
                for k, v in message.get("headers", [])
            }
        elif msg_type == "http.response.body":
            chunk = message.get("body", b"")
            if chunk:
                collected.extend(chunk)
                chunk_count += 1
                if on_body_chunk is not None:
                    on_body_chunk(bytes(chunk), bytes(collected), chunk_count)
                if disconnect_after_chunks is not None and chunk_count >= disconnect_after_chunks:
                    disconnect_event.set()
            if not message.get("more_body", False):
                disconnect_event.set()

    await app(scope, receive, send)
    return status_code, resp_headers, bytes(collected)


# ---------------------------------------------------------------------------
# 1. Streaming Bulk ZIP64 over HTTP (GET & POST /api/media/bulk/download)
# ---------------------------------------------------------------------------


def test_streaming_bulk_zip64_mixed_archive_get_and_post(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="both")
    settings.telegram_stream_cache_mb = 1
    settings.telegram_stream_chunk_kb = 64
    app = create_app(settings)
    db = app.state.db
    drive_dir = tmp_path / "drives" / "drive1" / "General"

    local_payload = b"LOCAL-FILE-DATA-" * 1024  # 16 KiB
    tg_single_payload = bytes((i * 17 + 3) % 256 for i in range(72 * 1024))  # > 1 block
    part0 = bytes((i * 29 + 7) % 256 for i in range(50 * 1024))
    part1 = bytes((i * 43 + 11) % 256 for i in range(45 * 1024))
    part2 = bytes((i * 59 + 13) % 256 for i in range(40 * 1024))
    tg_multi_payload = part0 + part1 + part2
    dup_local_payload = b"DUPLICATE-LOCAL-PAYLOAD-" * 60
    dup_tg_payload = b"DUPLICATE-TELEGRAM-PAYLOAD-" * 75
    utf8_filename = "🎬_テスト_résumé_資料.txt"
    utf8_payload = "UTF-8 payload: こんにちは世界 🚀 café\n".encode("utf-8") * 25

    fake_client = _DictBackedTelegramClient(
        "bot1",
        {
            201: tg_single_payload,
            202: part0,
            203: part1,
            204: part2,
            205: dup_tg_payload,
        },
        chunk_size=16384,
    )
    app.state.tg_pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), fake_client)
    ]

    it_local = _insert_local_media(
        db,
        media_id="bulk_local",
        filename="local.bin",
        path=drive_dir / "local.bin",
        payload=local_payload,
    )
    it_tg_single = _insert_telegram_media(
        db,
        media_id="bulk_tg_1",
        filename="tg_single.bin",
        size_bytes=len(tg_single_payload),
        msg_id=201,
    )
    it_tg_multi = _insert_telegram_media(
        db,
        media_id="bulk_tg_mp",
        filename="tg_multi.bin",
        size_bytes=len(tg_multi_payload),
        msg_id=202,
        parts=[
            {
                "part_index": 0,
                "start_byte": 0,
                "end_byte": len(part0) - 1,
                "size_bytes": len(part0),
                "telegram_chat_id": "-100123456",
                "telegram_topic_id": 1,
                "telegram_msg_id": 202,
                "telegram_worker_name": "bot1",
            },
            {
                "part_index": 1,
                "start_byte": len(part0),
                "end_byte": len(part0) + len(part1) - 1,
                "size_bytes": len(part1),
                "telegram_chat_id": "-100123456",
                "telegram_topic_id": 1,
                "telegram_msg_id": 203,
                "telegram_worker_name": "bot1",
            },
            {
                "part_index": 2,
                "start_byte": len(part0) + len(part1),
                "end_byte": len(tg_multi_payload) - 1,
                "size_bytes": len(part2),
                "telegram_chat_id": "-100123456",
                "telegram_topic_id": 1,
                "telegram_msg_id": 204,
                "telegram_worker_name": "bot1",
            },
        ],
    )
    it_dup1 = _insert_local_media(
        db,
        media_id="bulk_dup_a",
        filename="shared.dat",
        path=drive_dir / "shared.dat",
        payload=dup_local_payload,
    )
    it_dup2 = _insert_telegram_media(
        db,
        media_id="bulk_dup_b",
        filename="shared.dat",
        size_bytes=len(dup_tg_payload),
        msg_id=205,
    )
    it_utf8 = _insert_local_media(
        db,
        media_id="bulk_utf8",
        filename=utf8_filename,
        path=drive_dir / "utf8_src.txt",
        payload=utf8_payload,
        mime_type="text/plain",
    )
    it_empty = _insert_local_media(
        db,
        media_id="bulk_empty",
        filename="empty.txt",
        path=drive_dir / "empty.txt",
        payload=b"",
        mime_type="text/plain",
    )

    all_ids = [
        it_local["id"],
        it_tg_single["id"],
        it_tg_multi["id"],
        it_dup1["id"],
        it_dup2["id"],
        it_utf8["id"],
        it_empty["id"],
    ]

    with TestClient(app) as client:
        for method, resp in (
            ("GET", client.get("/api/media/bulk/download", params={"ids": ",".join(all_ids)})),
            ("POST", client.post("/api/media/bulk/download", json={"media_ids": all_ids})),
        ):
            assert resp.status_code == 200, f"{method} failed: {resp.text}"
            assert resp.headers["content-type"].startswith("application/zip")
            assert resp.headers["content-disposition"] == 'attachment; filename="media_bundle.zip"'
            assert resp.headers["cache-control"] == "no-store"
            assert resp.headers["x-accel-buffering"] == "no"
            assert "content-length" not in {k.lower() for k in resp.headers.keys()}

            with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zf:
                assert zf.testzip() is None
                names = zf.namelist()
                assert len(names) == 7
                assert len(set(names)) == 7
                assert "local.bin" in names
                assert "tg_single.bin" in names
                assert "tg_multi.bin" in names
                assert utf8_filename in names
                assert "empty.txt" in names

                dup_names = [n for n in names if n == "shared.dat" or n.startswith("shared_")]
                assert len(dup_names) == 2

                assert zf.read("local.bin") == local_payload
                assert zf.read("tg_single.bin") == tg_single_payload
                assert zf.read("tg_multi.bin") == tg_multi_payload
                assert zf.read(utf8_filename) == utf8_payload
                assert zf.read("empty.txt") == b""
                assert zf.read("shared.dat") == dup_local_payload
                other_dup = [n for n in dup_names if n != "shared.dat"][0]
                assert zf.read(other_dup) == dup_tg_payload


def test_streaming_bulk_zip64_zero_buffer_ordering_and_disconnect_cleanup(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="telegram")
    # Disable block cache so iter_telegram_range streams directly from upstream iter_download
    settings.telegram_stream_cache_mb = 0
    settings.telegram_disk_cache_mb = 0
    app = create_app(settings)
    db = app.state.db

    item1_chunk1 = b"ITEM1-CHUNK-A-" * 512  # 7 KiB
    item1_chunk2 = b"ITEM1-CHUNK-B-" * 512  # 7 KiB
    item1_payload = item1_chunk1 + item1_chunk2
    item2_payload = b"ITEM2-DATA-" * 512

    _insert_telegram_media(
        db,
        media_id="stream_1",
        filename="first.bin",
        size_bytes=len(item1_payload),
        msg_id=301,
    )
    _insert_telegram_media(
        db,
        media_id="stream_2",
        filename="second.bin",
        size_bytes=len(item2_payload),
        msg_id=302,
    )

    class _TrackedAsyncStream:
        def __init__(self, chunks: list[bytes], gate: asyncio.Event | None, on_close):
            self._chunks = chunks
            self._idx = 0
            self._gate = gate
            self._on_close = on_close

        def __aiter__(self):
            return self

        async def __anext__(self) -> bytes:
            if self._idx == 1 and self._gate is not None:
                await self._gate.wait()
            if self._idx >= len(self._chunks):
                raise StopAsyncIteration
            chunk = self._chunks[self._idx]
            self._idx += 1
            return chunk

        async def aclose(self) -> None:
            self._on_close()

    # --- Phase A: Prove item 1 LFH + payload bytes reach HTTP client before item 2 opens ---
    allow_item1_rest = asyncio.Event()
    item1_first_chunk_at_client = asyncio.Event()
    state_a = {
        "item1_opened": 0,
        "item1_closed": 0,
        "item2_opened": 0,
        "item2_opened_before_item1_chunk": False,
    }

    class _OrderedStreamClient:
        async def get_input_entity(self, target: Any) -> Any:
            return target

        async def get_messages(self, peer: Any, ids: int) -> Any:
            size = len(item1_payload) if int(ids) == 301 else len(item2_payload)
            return SimpleNamespace(media=SimpleNamespace(msg_id=int(ids), size=size))

        def iter_download(self, media: Any, *, offset: int = 0, request_size: int = 524288):
            mid = int(media.msg_id)
            if mid == 301:
                state_a["item1_opened"] += 1
                return _TrackedAsyncStream(
                    [item1_chunk1, item1_chunk2],
                    allow_item1_rest,
                    lambda: state_a.__setitem__("item1_closed", state_a["item1_closed"] + 1),
                )
            state_a["item2_opened"] += 1
            if not item1_first_chunk_at_client.is_set():
                state_a["item2_opened_before_item1_chunk"] = True
            return _TrackedAsyncStream(
                [item2_payload],
                None,
                lambda: None,
            )

        async def disconnect(self) -> None:
            return None

    app.state.tg_pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), _OrderedStreamClient())
    ]

    async def _verify_zero_buffer_order():
        def _on_chunk(_chunk: bytes, acc: bytes, _count: int) -> None:
            if b"PK\x03\x04" in acc and item1_chunk1 in acc:
                item1_first_chunk_at_client.set()

        req_body = json.dumps({"media_ids": ["stream_1", "stream_2"]}).encode("utf-8")
        task = asyncio.create_task(
            _run_asgi_stream(
                app,
                method="POST",
                path="/api/media/bulk/download",
                body=req_body,
                headers={"content-type": "application/json"},
                on_body_chunk=_on_chunk,
            )
        )
        await asyncio.wait_for(item1_first_chunk_at_client.wait(), timeout=2.0)
        assert state_a["item1_opened"] == 1
        assert state_a["item2_opened"] == 0
        assert state_a["item2_opened_before_item1_chunk"] is False

        allow_item1_rest.set()
        status, headers, raw_zip = await asyncio.wait_for(task, timeout=2.0)
        assert status == 200
        assert headers["cache-control"] == "no-store"
        assert headers["x-accel-buffering"] == "no"
        assert "content-length" not in headers
        assert state_a["item2_opened"] == 1
        assert state_a["item1_closed"] == 1
        with zipfile.ZipFile(io.BytesIO(raw_zip), "r") as zf:
            assert zf.testzip() is None
            assert zf.read("first.bin") == item1_payload
            assert zf.read("second.bin") == item2_payload

    asyncio.run(_verify_zero_buffer_order())

    # --- Phase B: Mid-stream client disconnect closes item 1 iterator & never opens item 2 ---
    never_unblock = asyncio.Event()
    item1_seen = asyncio.Event()
    state_b = {"item1_aclosed": False, "item2_opened": False}

    class _DisconnectClient:
        async def get_input_entity(self, target: Any) -> Any:
            return target

        async def get_messages(self, peer: Any, ids: int) -> Any:
            size = len(item1_payload) if int(ids) == 301 else len(item2_payload)
            return SimpleNamespace(media=SimpleNamespace(msg_id=int(ids), size=size))

        def iter_download(self, media: Any, *, offset: int = 0, request_size: int = 524288):
            if int(media.msg_id) == 301:
                return _TrackedAsyncStream(
                    [item1_chunk1, item1_chunk2],
                    never_unblock,
                    lambda: state_b.__setitem__("item1_aclosed", True),
                )
            state_b["item2_opened"] = True
            return _TrackedAsyncStream([item2_payload], None, lambda: None)

        async def disconnect(self) -> None:
            return None

    app.state.tg_pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), _DisconnectClient())
    ]

    async def _verify_disconnect_cleanup():
        def _on_chunk(_chunk: bytes, acc: bytes, _count: int) -> None:
            if item1_chunk1 in acc:
                item1_seen.set()

        req_body = json.dumps({"media_ids": ["stream_1", "stream_2"]}).encode("utf-8")
        status, _headers, partial = await asyncio.wait_for(
            _run_asgi_stream(
                app,
                method="POST",
                path="/api/media/bulk/download",
                body=req_body,
                headers={"content-type": "application/json"},
                on_body_chunk=_on_chunk,
                disconnect_after_chunks=2,  # LFH + first chunk of item 1
            ),
            timeout=2.0,
        )
        assert status == 200
        assert partial.startswith(b"PK\x03\x04")
        assert item1_seen.is_set()
        assert state_b["item1_aclosed"] is True
        assert state_b["item2_opened"] is False

    asyncio.run(_verify_disconnect_cleanup())


# ---------------------------------------------------------------------------
# 2. Persisted Two-Tier ZIP Central Directory Cache across Simulated Restart
# ---------------------------------------------------------------------------


def test_zip_central_directory_persisted_cache_restart_invalidation_and_bounds(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="both")
    settings.telegram_stream_cache_mb = 1
    settings.telegram_stream_chunk_kb = 64

    def _build_zip(entries: dict[str, tuple[bytes, int]]) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for name, (data, ctype) in entries.items():
                zf.writestr(name, data, compress_type=ctype)
        return buf.getvalue()

    zip_v1 = _build_zip(
        {
            "big_stored.bin": (b"Z" * 180_000, zipfile.ZIP_STORED),
            "docs/readme.txt": (b"Hello persisted ZIP cache!\n" * 40, zipfile.ZIP_DEFLATED),
            "utf8/資料.txt": ("日本語データ".encode("utf-8"), zipfile.ZIP_DEFLATED),
        }
    )
    zip_v2_probe = _build_zip(
        {
            "big_stored.bin": (b"Z" * 180_000, zipfile.ZIP_STORED),
            "v2/updated.txt": (b"Updated archive payload!", zipfile.ZIP_DEFLATED),
        }
    )
    delta = len(zip_v1) - len(zip_v2_probe)
    zip_v2 = _build_zip(
        {
            "big_stored.bin": (b"Z" * (180_000 + delta), zipfile.ZIP_STORED),
            "v2/updated.txt": (b"Updated archive payload!", zipfile.ZIP_DEFLATED),
        }
    )
    assert len(zip_v2) == len(zip_v1)

    # --- Boot 1: populate L1 + L2 SQLite zip_entries_cache ---
    app1 = create_app(settings)
    _insert_telegram_media(
        app1.state.db,
        media_id="tg_zip_persist",
        filename="archive.zip",
        size_bytes=len(zip_v1),
        msg_id=501,
    )
    client1 = _DictBackedTelegramClient("bot1", {501: zip_v1, 502: zip_v2}, chunk_size=65536)
    app1.state.tg_pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), client1)
    ]

    with TestClient(app1) as http1:
        r1 = http1.get("/api/media/tg_zip_persist/zip/entries")
        assert r1.status_code == 200
        entries_v1 = r1.json()["entries"]
        assert [e["path"] for e in entries_v1] == [
            "big_stored.bin",
            "docs/readme.txt",
            "utf8/資料.txt",
        ]
        assert len(client1.get_messages_calls) >= 1
        assert len(client1.iter_download_calls) >= 1
        assert app1.state.db.has_zip_cache_entry("tg_zip_persist")

    # --- Boot 2: fresh app instance pointing at same SQLite DB, fresh TG client ---
    app2 = create_app(settings)
    client2 = _DictBackedTelegramClient("bot1", {501: zip_v1, 502: zip_v2}, chunk_size=65536)
    app2.state.tg_pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), client2)
    ]

    with TestClient(app2) as http2:
        r2 = http2.get("/api/media/tg_zip_persist/zip/entries")
        assert r2.status_code == 200
        assert r2.json()["entries"] == entries_v1
        assert client2.get_messages_calls == []
        assert client2.iter_download_calls == []

        status = http2.get("/api/system/status").json()
        zc = status["zip_cache"]
        assert zc["l2_hits"] == 1
        assert zc["l1_entries"] >= 1
        assert zc["l2_entries"] >= 1

        # Metadata-only PATCH (tags) must NOT invalidate ZIP directory cache
        patch_tags = http2.patch("/api/media/tg_zip_persist", json={"tags": "#release #v1"})
        assert patch_tags.status_code == 200
        r3 = http2.get("/api/media/tg_zip_persist/zip/entries")
        assert r3.status_code == 200
        assert r3.json()["entries"] == entries_v1
        assert client2.get_messages_calls == []
        assert client2.iter_download_calls == []
        assert http2.get("/api/system/status").json()["zip_cache"]["l1_hits"] >= 1

        # Structural update (changing telegram_msg_id) invalidates via fingerprint
        app2.state.db.update_media(
            "tg_zip_persist",
            telegram_msg_id=502,
        )
        r4 = http2.get("/api/media/tg_zip_persist/zip/entries")
        assert r4.status_code == 200
        assert [e["path"] for e in r4.json()["entries"]] == ["big_stored.bin", "v2/updated.txt"]
        assert len(client2.get_messages_calls) >= 1
        assert len(client2.iter_download_calls) >= 1

        # Local ZIP file move via PATCH folder invalidates stale cache and re-reads cleanly
        local_zip_path = tmp_path / "drives" / "drive1" / "General" / "local_bundle.zip"
        _insert_local_media(
            app2.state.db,
            media_id="local_zip_1",
            filename="local_bundle.zip",
            path=local_zip_path,
            payload=zip_v1,
            mime_type="application/zip",
        )
        assert http2.get("/api/media/local_zip_1/zip/entries").status_code == 200
        assert app2.state.db.has_zip_cache_entry("local_zip_1")
        inv_before = app2.state.zip_cache.stats()["invalidations"]

        moved = http2.patch("/api/media/local_zip_1", json={"folder": "MovedArchives"})
        assert moved.status_code == 200
        assert not app2.state.db.has_zip_cache_entry("local_zip_1")
        assert "local_zip_1" not in app2.state.zip_cache
        assert app2.state.zip_cache.stats()["invalidations"] > inv_before

        reloaded = http2.get("/api/media/local_zip_1/zip/entries")
        assert reloaded.status_code == 200
        assert len(reloaded.json()["entries"]) == 3

        # DELETE removes row from zip_entries_cache
        del_resp = http2.delete("/api/media/tg_zip_persist")
        assert del_resp.status_code == 200
        assert not app2.state.db.has_zip_cache_entry("tg_zip_persist")
        assert "tg_zip_persist" not in app2.state.zip_cache

    # --- Bounded SQLite table + Track #1 backup safety ---
    bounded_settings = _make_settings(tmp_path / "bounded", storage_mode="drives")
    bounded_settings.zip_cache_db_max_rows = 3
    bounded_settings.zip_cache_db_max_mb = 1
    app_b = create_app(bounded_settings)

    with TestClient(app_b) as http_b:
        for i in range(8):
            app_b.state.zip_cache.set_entries(
                f"zip_{i}",
                [{"path": f"file_{i}.txt", "size_bytes": 100}],
                fingerprint=f"fp_{i}",
            )
        assert app_b.state.db.get_db_stats()["zip_cache_entries"] == 3

        # Byte-limit bounding on zip_entries_cache
        for i in range(5):
            app_b.state.db.put_zip_cache_entry(
                f"byte_cap_{i}",
                fingerprint=f"bfp_{i}",
                entry_count=1,
                entries_blob=b"X" * 80,
                max_rows=100,
                max_bytes=200,
            )
        db_cache_stats = app_b.state.db.get_zip_cache_db_stats()
        assert db_cache_stats["total_bytes"] <= 200
        assert db_cache_stats["entries"] <= 2

        # Track #1 backup + verify + restore succeeds with zip_entries_cache populated
        bk_resp = http_b.post("/api/system/backups", json={"note": "with-zip-cache"})
        assert bk_resp.status_code == 201, bk_resp.text
        bk_path = Path(bk_resp.json()["file_path"])
        verified = verify_backup_file(bk_path)
        assert verified["valid"] is True
        assert "zip_entries_cache" in verified["tables"]

        restored_db = tmp_path / "bounded" / "restored.db"
        restored = restore_backup_file(bk_path, restored_db)
        assert restored["restored"] is True


# ---------------------------------------------------------------------------
# 3. Two-Tier RAM+Disk Block Cache, Alignment, Coalescing, Prefetch & Routing
# ---------------------------------------------------------------------------


def test_stream_canonical_alignment_and_l2_persistence_across_restart(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_stream_cache_mb = 1
    settings.telegram_disk_cache_mb = 4
    settings.telegram_stream_chunk_kb = 64
    settings.telegram_prefetch_blocks = 2

    block_size = settings.telegram_stream_chunk_bytes
    assert block_size == 64 * 1024
    payload = bytes((i * 37 + 9) % 256 for i in range(4 * block_size))

    # --- Boot 1: unaligned overlapping probes hit single canonical block ---
    app1 = create_app(settings)
    _insert_telegram_media(
        app1.state.db,
        media_id="align_vid",
        filename="align.mp4",
        size_bytes=len(payload),
        msg_id=601,
        mime_type="video/mp4",
    )
    c1 = _DictBackedTelegramClient("bot1", {601: payload}, chunk_size=32768)
    app1.state.tg_pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), c1)
    ]

    with TestClient(app1) as http1:
        for hdr, s, e in (
            ("bytes=0-100", 0, 100),
            ("bytes=5000-5100", 5000, 5100),
            ("bytes=10000-20000", 10000, 20000),
        ):
            r = http1.get("/api/media/align_vid/stream", headers={"Range": hdr})
            assert r.status_code == 206
            assert r.content == payload[s : e + 1]

        # Only ONE canonical MTProto iter_download(offset=0) for all 3 probes
        assert c1.iter_download_calls == [(601, 0, block_size)]
        blk_files = list(settings.stream_cache_dir.rglob("*.blk"))
        assert len(blk_files) == 1

    # --- Boot 2: fresh app/pool pointing at same stream_cache_dir serves from L2 ---
    app2 = create_app(settings)
    c2 = _DictBackedTelegramClient("bot1", {601: payload}, chunk_size=32768)
    app2.state.tg_pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), c2)
    ]

    with TestClient(app2) as http2:
        assert app2.state.tg_pool._block_cache.stats()["l1"]["entries"] == 0
        r_l2 = http2.get("/api/media/align_vid/stream", headers={"Range": "bytes=1000-1999"})
        assert r_l2.status_code == 206
        assert r_l2.content == payload[1000:2000]
        assert c2.iter_download_calls == []  # 0 MTProto iter_download calls

        bc = app2.state.tg_pool.get_telemetry()["block_cache"]
        assert bc["l2"]["hits"] >= 1
        assert bc["l1"]["entries"] >= 1  # promoted back into L1

        footprint = app2.state.integrity.get_storage_footprint()
        assert footprint["stream_cache_entries"] > 0
        assert footprint["stream_cache_bytes"] > 0


def test_stream_singleflight_coalescing_prefetch_floodwait_and_routing(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_stream_cache_mb = 1
    settings.telegram_disk_cache_mb = 4
    settings.telegram_stream_chunk_kb = 64
    settings.telegram_prefetch_blocks = 2

    block_size = settings.telegram_stream_chunk_bytes
    payload_a = bytes((i * 19 + 5) % 256 for i in range(5 * block_size))
    payload_b = bytes((i * 23 + 1) % 256 for i in range(4 * block_size))

    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool

    gate = asyncio.Event()
    gate.set()
    download_calls: list[tuple[int, int]] = []

    class _CoalesceClient:
        def __init__(self, name: str, msgs: dict[int, bytes]) -> None:
            self.name = name
            self.msgs = msgs
            self.get_msg_calls: list[int] = []

        def is_connected(self) -> bool:
            return True

        async def get_input_entity(self, target: Any) -> Any:
            return target

        async def get_messages(self, peer: Any, ids: int) -> Any:
            self.get_msg_calls.append(int(ids))
            data = self.msgs.get(int(ids))
            if data is None:
                return None
            return SimpleNamespace(media=SimpleNamespace(msg_id=int(ids), size=len(data)))

        def iter_download(self, media: Any, *, offset: int = 0, request_size: int = 524288):
            mid = int(media.msg_id)
            data = self.msgs[mid]
            download_calls.append((mid, int(offset)))

            async def _gen():
                await gate.wait()
                pos = int(offset)
                end = min(len(data), pos + int(request_size))
                yield data[pos:end]

            return _gen()

        async def disconnect(self) -> None:
            return None

    c1 = _CoalesceClient("bot1", {701: payload_a, 702: payload_b})
    c2 = _CoalesceClient("bot2", {701: payload_a, 702: payload_b, 888: b"routed"})
    pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), c1),
        (TelegramApiConfig(name="bot2", api_id=2, api_hash="h", bot_token="t2"), c2),
    ]

    # 1) Stream-aware routing prefers worker with fewer active_uploads
    pool._active_uploads["bot1"] = 2
    pool._active_uploads["bot2"] = 0
    ordered_stream = pool._ordered_clients(purpose="stream")
    assert ordered_stream[0][0].name == "bot2"

    async def _run_scenarios():
        try:
            # Verify prepare_stream routes to bot2 first
            c_routed, m_routed = await pool.prepare_stream("-100123", 888)
            assert c_routed is c2
            assert m_routed.msg_id == 888
            assert c2.get_msg_calls == [888]
            assert c1.get_msg_calls == []

            # 2) Single-flight coalescing under 4 concurrent Range requests for uncached block 0
            client_a, media_a = await pool.prepare_stream("-100123", 701)
            gate.clear()

            async def _read_slice() -> bytes:
                out = bytearray()
                async for ch in pool.iter_telegram_range(
                    client_a, media_a, 0, 1023, total_size=len(payload_a)
                ):
                    out.extend(ch)
                return bytes(out)

            tasks = [asyncio.create_task(_read_slice()) for _ in range(4)]
            deadline = time.monotonic() + 2.0
            while pool.get_telemetry()["block_cache"]["coalesced_waits"] < 3:
                assert time.monotonic() < deadline, "Coalescing did not engage in time"
                await asyncio.sleep(0.005)

            gate.set()
            results = await asyncio.gather(*tasks)
            assert all(res == payload_a[:1024] for res in results)
            assert download_calls == [(701, 0)]
            assert pool.get_telemetry()["block_cache"]["coalesced_waits"] >= 3

            # 3) Sequential read-ahead prefetch populates block 2 ahead of time
            async def _collect_range(client, media, start: int, end: int, total: int) -> bytes:
                out = bytearray()
                async for ch in pool.iter_telegram_range(
                    client, media, start, end, total_size=total
                ):
                    out.extend(ch)
                return bytes(out)

            seq_got = await _collect_range(
                client_a, media_a, 0, block_size + 511, len(payload_a)
            )
            assert seq_got == payload_a[: block_size + 512]

            # Wait for background prefetch tasks (blocks 2 and 3) to finish
            pf_tasks = list(pool._stream_coordinator._prefetch_tasks)
            if pf_tasks:
                await asyncio.gather(*pf_tasks, return_exceptions=True)

            # Block 2 should now be served from cache with zero new download calls
            calls_before_b2 = len(download_calls)
            b2_got = await _collect_range(
                client_a, media_a, 2 * block_size, 2 * block_size + 255, len(payload_a)
            )
            assert b2_got == payload_a[2 * block_size : 2 * block_size + 256]
            assert len(download_calls) == calls_before_b2
            assert pool.get_telemetry()["block_cache"]["prefetch"]["hits"] >= 1

            # 4) FloodWait cooldown skips prefetch cleanly without issuing prefetch RPCs
            client_b, media_b = await pool.prepare_stream("-100123", 702)
            active_worker_name = pool._worker_name_for_client(client_b)
            assert active_worker_name is not None
            pool._record_flood_wait(active_worker_name, 60.0)
            drops_before = pool.get_telemetry()["block_cache"]["prefetch"]["drops"]
            calls_before_fw = len(download_calls)

            fw_got = await _collect_range(
                client_b, media_b, 0, block_size + 127, len(payload_b)
            )
            assert fw_got == payload_b[: block_size + 128]
            # Only foreground blocks 0 and 1 fetched; no prefetch blocks 2 or 3 fetched
            new_calls = download_calls[calls_before_fw:]
            assert new_calls == [(702, 0), (702, block_size)]
            assert pool.get_telemetry()["block_cache"]["prefetch"]["drops"] >= drops_before + 1
        finally:
            await pool.stop()
            app.state.db.close()

    asyncio.run(_run_scenarios())


def test_multipart_warming_abort_and_pool_stop_leak_free(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_stream_cache_mb = 1
    settings.telegram_disk_cache_mb = 4
    settings.telegram_stream_chunk_kb = 64
    settings.telegram_prefetch_blocks = 2

    block_size = settings.telegram_stream_chunk_bytes
    part0 = bytes((i * 11 + 3) % 256 for i in range(2 * block_size))
    part1 = bytes((i * 13 + 7) % 256 for i in range(2 * block_size))
    total_size = len(part0) + len(part1)

    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool
    db = app.state.db

    parts = [
        {
            "part_index": 0,
            "start_byte": 0,
            "end_byte": len(part0) - 1,
            "size_bytes": len(part0),
            "telegram_chat_id": "-100999",
            "telegram_topic_id": 1,
            "telegram_msg_id": 910,
            "telegram_worker_name": "bot1",
        },
        {
            "part_index": 1,
            "start_byte": len(part0),
            "end_byte": total_size - 1,
            "size_bytes": len(part1),
            "telegram_chat_id": "-100999",
            "telegram_topic_id": 1,
            "telegram_msg_id": 911,
            "telegram_worker_name": "bot1",
        },
    ]
    _insert_telegram_media(
        db,
        media_id="mp_warm_1",
        filename="movie.mp4",
        size_bytes=total_size,
        chat_id="-100999",
        msg_id=910,
        parts=parts,
    )

    c1 = _DictBackedTelegramClient("bot1", {910: part0, 911: part1}, chunk_size=32768)
    pool.clients = [(TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), c1)]

    # 1) Stream near end of part 0 via HTTP and confirm part 1 is warmed in _stream_media_cache
    with TestClient(app) as client:
        r = client.get(
            "/api/media/mp_warm_1/stream",
            headers={"Range": f"bytes={len(part0) - 1024}-{len(part0) - 1}"},
        )
        assert r.status_code == 206
        assert r.content == part0[-1024:]

        deadline = time.monotonic() + 2.0
        while ("-100999", 911, "bot1") not in pool._stream_media_cache:
            assert time.monotonic() < deadline, "Part 1 was not warmed in _stream_media_cache"
            time.sleep(0.01)

    # 2) Abort mid-range with pending prefetch/warm tasks, then pool.stop() leaves zero leaked tasks
    app2 = create_app(settings)
    pool2: TelegramPool = app2.state.tg_pool
    gate_prefetch = asyncio.Event()

    class _SlowPrefetchClient(_DictBackedTelegramClient):
        def iter_download(self, media: Any, *, offset: int = 0, request_size: int = 524288):
            msg_id = int(media.msg_id)
            payload = self.messages[msg_id]

            async def _gen():
                if int(offset) > 0:
                    await gate_prefetch.wait()
                pos = int(offset)
                end = min(len(payload), pos + int(request_size))
                yield payload[pos:end]

            return _gen()

    c2 = _SlowPrefetchClient("bot1", {910: part0, 911: part1})
    pool2.clients = [(TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), c2)]

    async def _abort_and_stop():
        try:
            gen = pool2.iter_telegram_multipart_range(parts, 0, total_size - 1)
            first_chunk = await asyncio.wait_for(anext(gen), timeout=2.0)
            assert first_chunk == part0[:block_size]
            await gen.aclose()
        finally:
            await pool2.stop()
            app2.state.db.close()

        assert pool2.get_telemetry()["block_cache"]["prefetch"]["active_tasks"] == 0
        assert pool2.get_telemetry()["block_cache"]["prefetch"]["errors"] == 0
        leaked = [
            t
            for t in asyncio.all_tasks()
            if not t.done()
            and (t.get_name().startswith("stream-prefetch:") or t.get_name().startswith("stream-warm:"))
        ]
        assert leaked == []

    asyncio.run(_abort_and_stop())


# ---------------------------------------------------------------------------
# 4. Ops API (/api/system/cache/purge, /api/system/status, /api/health) & UI
# ---------------------------------------------------------------------------


def test_ops_cache_purge_health_system_status_and_frontend_guardrails(tmp_path: Path):
    settings = _make_settings(tmp_path, auth_token="ops-secret", storage_mode="telegram")
    settings.telegram_archive_chat_id = "-100777888"
    settings.telegram_stream_cache_mb = 1
    settings.telegram_disk_cache_mb = 4
    settings.telegram_stream_chunk_kb = 64

    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool
    db = app.state.db

    block_size = settings.telegram_stream_chunk_bytes
    sample_block = b"B" * block_size
    fake_client = _DictBackedTelegramClient("bot1", {101: sample_block})
    pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), fake_client)
    ]
    pool._worker_meta["bot1"] = {"username": "ops_bot"}
    pool._record_worker_error("bot1", "SENSITIVE_INTERNAL_WORKER_ERROR_PATH")

    def _seed_all_caches() -> None:
        pool._block_cache.put(("-100777888", 101), 0, sample_block)
        pool._stream_media_cache[("-100777888", 101, "bot1")] = (
            time.monotonic() + 300.0,
            SimpleNamespace(msg_id=101),
        )
        app.state.zip_cache.set_entries(
            "zip_ops_1",
            [{"path": "hello.txt", "size_bytes": 5}],
            fingerprint="fp_ops_1",
        )

    _seed_all_caches()
    auth = {"Authorization": "Bearer ops-secret"}

    with TestClient(app) as client:
        # 1) Auth enforced on /api/system/cache/purge
        assert client.post("/api/system/cache/purge", json={"scope": "all"}).status_code == 401

        # 2) /api/health preserves legacy keys and leaks NO filesystem paths or errors
        health_resp = client.get("/api/health")
        assert health_resp.status_code == 200
        health = health_resp.json()
        legacy_keys = {
            "ok",
            "storage_mode",
            "telegram_workers",
            "telegram_workers_online",
            "telegram_workers_in_cooldown",
            "telegram_archive_enabled",
            "block_cache_utilization_pct",
            "last_backup_at",
            "default_folder",
            "auth_required",
            "max_upload_mb",
            "upload_chunk_size_mb",
            "failed_upload_retention_sec",
            "telegram_part_size_mb",
            "ingest_concurrency",
            "ytdlp_cookies_configured",
            "ytdlp_version",
        }
        assert legacy_keys.issubset(health.keys())
        assert "SENSITIVE_INTERNAL_WORKER_ERROR_PATH" not in health_resp.text
        assert str(settings.stream_cache_dir) not in health_resp.text
        assert str(settings.data_dir) not in health_resp.text
        assert str(tmp_path) not in health_resp.text

        # 3) /api/system/status exposes Track #6 telemetry & storage footprint
        status = client.get("/api/system/status", headers=auth).json()
        assert {"l1_entries", "l2_entries", "l2_bytes", "hit_rate_pct"}.issubset(
            status["zip_cache"].keys()
        )
        bc = status["telegram"]["block_cache"]
        assert bc["l1"]["enabled"] is True
        assert bc["l2"]["enabled"] is True
        assert "coalesced_waits" in bc
        assert {"enabled", "blocks_ahead", "hits", "scheduled", "drops"}.issubset(
            bc["prefetch"].keys()
        )
        assert status["storage"]["stream_cache_entries"] == 1
        assert status["storage"]["stream_cache_bytes"] == block_size

        # 4) Scope: stream_ram clears L1 + _stream_media_cache, keeps L2 disk & zip cache
        r_ram = client.post("/api/system/cache/purge", json={"scope": "stream_ram"}, headers=auth)
        assert r_ram.status_code == 200
        assert r_ram.json()["purged"] is True
        st_ram = client.get("/api/system/status", headers=auth).json()
        assert st_ram["telegram"]["block_cache"]["l1"]["entries"] == 0
        assert st_ram["telegram"]["block_cache"]["l1"]["used_bytes"] == 0
        assert st_ram["telegram"]["stream_media_cache_entries"] == 0
        assert st_ram["telegram"]["block_cache"]["l2"]["entries"] == 1
        assert len(list(settings.stream_cache_dir.rglob("*.blk"))) == 1
        assert st_ram["zip_cache"]["l2_entries"] == 1

        # 5) Scope: stream_disk clears L2 .blk files, keeps L1 RAM & zip cache
        pool._block_cache.put(("-100777888", 101), block_size, sample_block)
        r_disk = client.post("/api/system/cache/purge", json={"scope": "stream_disk"}, headers=auth)
        assert r_disk.status_code == 200
        st_disk = client.get("/api/system/status", headers=auth).json()
        assert st_disk["telegram"]["block_cache"]["l2"]["entries"] == 0
        assert st_disk["storage"]["stream_cache_entries"] == 0
        assert st_disk["storage"]["stream_cache_bytes"] == 0
        assert list(settings.stream_cache_dir.rglob("*.blk")) == []
        assert st_disk["telegram"]["block_cache"]["l1"]["entries"] == 1
        assert st_disk["zip_cache"]["l2_entries"] == 1

        # 6) Scope: zip clears L1 + L2 zip_entries_cache, keeps RAM block cache
        r_zip = client.post("/api/system/cache/purge", json={"scope": "zip"}, headers=auth)
        assert r_zip.status_code == 200
        st_zip = client.get("/api/system/status", headers=auth).json()
        assert st_zip["zip_cache"]["l1_entries"] == 0
        assert st_zip["zip_cache"]["l2_entries"] == 0
        assert db.get_db_stats()["zip_cache_entries"] == 0
        assert st_zip["telegram"]["block_cache"]["l1"]["entries"] == 1

        # 7) Scope: all clears everything
        _seed_all_caches()
        r_all = client.post("/api/system/cache/purge", json={"scope": "all"}, headers=auth)
        assert r_all.status_code == 200
        st_all = client.get("/api/system/status", headers=auth).json()
        assert st_all["telegram"]["block_cache"]["l1"]["entries"] == 0
        assert st_all["telegram"]["block_cache"]["l2"]["entries"] == 0
        assert st_all["telegram"]["stream_media_cache_entries"] == 0
        assert st_all["zip_cache"]["l1_entries"] == 0
        assert st_all["zip_cache"]["l2_entries"] == 0
        assert list(settings.stream_cache_dir.rglob("*.blk")) == []
        assert db.get_db_stats()["zip_cache_entries"] == 0

        # 8) Frontend guardrails: maintenance.js hooks, no native dialogs, index.html < 350 lines
        maint_resp = client.get("/static/js/features/maintenance.js")
        assert maint_resp.status_code == 200
        maint_js = maint_resp.text
        for token in (
            "purge-caches",
            "/api/system/cache/purge",
            "L2 Disk Cache",
            "ZIP Directory Cache",
        ):
            assert token in maint_js

        for forbidden in ("window.alert(", "window.confirm(", "window.prompt("):
            assert forbidden not in maint_js
        assert re.search(r"(?<![A-Za-z0-9_])(?:window\.)?(?:alert|confirm|prompt)\s*\(", maint_js) is None

        index_lines = (STATIC_DIR / "index.html").read_text(encoding="utf-8").splitlines()
        assert len(index_lines) < 350
