from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from PIL import Image

from app.config import Settings
from app.main import create_app
from tests.helpers import (
    _make_settings,
    _sample_jpeg_bytes,
    _wait_for_upload_item,
)


def test_telegram_only_storage_deletes_local_and_streams_range(tmp_path: Path, monkeypatch):
    settings = _make_settings(tmp_path, storage_mode="telegram")
    app = create_app(settings)
    jpg = _sample_jpeg_bytes()

    async def fake_mirror(payload: dict) -> dict:
        assert Path(payload["local_path"]).exists()
        return {"chat_id": "-100123456", "topic_id": 55, "message_id": 901, "worker_name": "bot_a"}

    app.state.ingest.mirror_hook = fake_mirror

    async def fake_prepare_stream(chat_id: str, msg_id: int, worker_name: str | None = None):
        assert chat_id == "-100123456"
        assert msg_id == 901
        assert worker_name == "bot_a"
        return object(), object()

    async def fake_iter_range(_client, _media, start: int, end: int):
        yield jpg[start : end + 1]

    deleted_tg: list[tuple[str, int, str | None]] = []

    async def fake_delete(chat_id: str, msg_id: int, worker_name: str | None = None):
        deleted_tg.append((chat_id, msg_id, worker_name))

    monkeypatch.setattr(app.state.tg_pool, "prepare_stream", fake_prepare_stream)
    monkeypatch.setattr(app.state.tg_pool, "iter_telegram_range", fake_iter_range)
    monkeypatch.setattr(app.state.tg_pool, "delete_archive_message", fake_delete)

    with TestClient(app) as client:
        resp = client.post(
            "/api/ingest/upload",
            data={"folder": "Clips"},
            files={"file": ("clip.jpg", jpg, "image/jpeg")},
        )
        item = _wait_for_upload_item(client, resp)
        assert item["drive_name"] == "telegram"
        assert item["file_path"] == ""
        assert item["telegram_topic_id"] == 55
        assert item["telegram_msg_id"] == 901
        assert item["telegram_worker_name"] == "bot_a"
        # Local staged & drive files are gone, while thumbnail remains locally
        assert list((tmp_path / "drives" / "drive1").rglob("*")) == []
        assert client.get(f"/api/media/{item['id']}/thumb").status_code == 200

        # Stream & Range 206 served directly from Telegram helper
        r = client.get(f"/api/media/{item['id']}/stream", headers={"Range": "bytes=10-49"})
        assert r.status_code == 206
        assert r.content == jpg[10:50]

        # Delete removes DB row, thumb, and archive message
        d = client.delete(f"/api/media/{item['id']}")
        assert d.status_code == 200
        assert deleted_tg == [("-100123456", 901, "bot_a")]


def test_parse_folder_from_telegram_text():
    from app.telegram_mtproto import parse_folder_from_text

    assert parse_folder_from_text("#Movies https://example.com/v", "General") == "Movies"
    assert parse_folder_from_text("folder:Work/Refs https://example.com/a", "General") == "Work/Refs"
    assert parse_folder_from_text("Vacation", "General") == "Vacation"
    assert parse_folder_from_text("https://example.com/v", "General") == "General"


def test_prepare_stream_falls_back_across_workers_and_aligns_range(tmp_path: Path):
    import asyncio
    from types import SimpleNamespace
    from app.config import TelegramApiConfig
    from app.telegram_mtproto import TelegramPool

    settings = _make_settings(tmp_path, storage_mode="telegram")
    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool

    calls: list[str] = []

    class FakeClient:
        def __init__(self, name: str, has_msg: bool):
            self.name = name
            self.has_msg = has_msg
            self.seen_offsets: list[int] = []

        async def get_input_entity(self, target):
            return target

        async def get_messages(self, peer, ids: int):
            calls.append(self.name)
            if not self.has_msg:
                return None
            return SimpleNamespace(media=b"media-obj")

        def iter_download(self, media, *, offset: int = 0, request_size: int = 524288):
            self.seen_offsets.append(offset)
            payload = bytes(i % 256 for i in range(10000))

            async def _gen():
                yield payload[offset : offset + 4096]
                yield payload[offset + 4096 : offset + 8192]

            return _gen()

    c1 = FakeClient("bot1", has_msg=False)
    c2 = FakeClient("bot2", has_msg=True)
    pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), c1),
        (TelegramApiConfig(name="bot2", api_id=2, api_hash="h", bot_token="t2"), c2),
    ]

    # Even when round-robin or preferred points to bot1, falls back to bot2
    client, media = asyncio.run(pool.prepare_stream("-100123", 14, worker_name="bot1"))
    assert client is c2
    assert media == b"media-obj"
    assert calls == ["bot1", "bot2"]

    # Range starting at unaligned offset 5000 aligns GetFileRequest down to 4096 and trims cleanly
    async def _collect():
        out = bytearray()
        async for chunk in pool.iter_telegram_range(c2, media, 5000, 5099):
            out.extend(chunk)
        return bytes(out)

    got = asyncio.run(_collect())
    assert c2.seen_offsets == [4096]
    expected = bytes(i % 256 for i in range(5000, 5100))
    assert got == expected


def test_ensure_topic_id_uses_bot_api_and_caches(tmp_path: Path, monkeypatch):
    import asyncio
    from app.config import TelegramApiConfig
    from app.telegram_mtproto import TelegramPool

    settings = _make_settings(tmp_path, storage_mode="telegram")
    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool

    posted: list[tuple[str, dict]] = []

    class FakeResp:
        def json(self):
            return {"ok": True, "result": {"message_thread_id": 88}}

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, json=None):
            posted.append((url, json))
            return FakeResp()

    monkeypatch.setattr("app.telegram_mtproto.httpx.AsyncClient", FakeAsyncClient)

    cfg = TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="123:ABC")
    tid1 = asyncio.run(pool._ensure_topic_id(cfg, "-1009998887776", "Futa"))
    tid2 = asyncio.run(pool._ensure_topic_id(cfg, "-1009998887776", "Futa"))
    assert tid1 == 88
    assert tid2 == 88
    assert len(posted) == 1
    assert posted[0][0] == "https://api.telegram.org/bot123:ABC/createForumTopic"
    assert posted[0][1] == {"chat_id": "-1009998887776", "name": "Futa"}


def test_file_slice_reader_bounds_and_seeking(tmp_path: Path):
    import asyncio
    from telethon import helpers
    from app.telegram_mtproto import FileSliceReader

    raw = bytes((i * 7 + 3) % 256 for i in range(1000))
    fpath = tmp_path / "payload.bin"
    fpath.write_bytes(raw)

    with FileSliceReader(fpath, 200, 350, name="payload.bin.part002of003") as r:
        assert r.readable() is True
        assert r.seekable() is True
        assert r.writable() is False
        assert r.name == "payload.bin.part002of003"
        assert r.tell() == 0

        assert r.read(10) == raw[200:210]
        assert r.tell() == 10

        r.seek(0, io.SEEK_END)
        assert r.tell() == 350
        assert r.read(50) == b""

        r.seek(-20, io.SEEK_END)
        assert r.tell() == 330
        assert r.read(100) == raw[530:550]  # clamped to slice end (200 + 350 = 550)
        assert r.read() == b""

        r.seek(50, io.SEEK_SET)
        buf = bytearray(25)
        n = r.readinto(buf)
        assert n == 25
        assert bytes(buf) == raw[250:275]

        r.seek(-5, io.SEEK_CUR)
        assert r.tell() == 70
        rest = r.read()
        assert len(rest) == 280
        assert rest == raw[270:550]

    # Verify Telethon's _FileStream wraps FileSliceReader cleanly without reading into memory
    async def _check_filestream():
        with FileSliceReader(fpath, 100, 123, name="slice.part") as r:
            async with helpers._FileStream(r) as stream:
                assert stream.file_size == 123
                assert stream.name == "slice.part"
                assert stream.read() == raw[100:223]

    asyncio.run(_check_filestream())


def test_multipart_upload_streaming_and_delete(tmp_path: Path, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from telethon.errors import FloodWaitError
    from app.config import TelegramApiConfig
    from app.telegram_mtproto import TelegramPool

    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_part_size_mb = 1  # 1 MiB per part
    settings.telegram_archive_chat_id = "-100555666777"
    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool
    db = app.state.db

    # Create a 2.5 MiB deterministic payload -> 3 parts (1 MiB, 1 MiB, 0.5 MiB)
    part_bytes = 1024 * 1024
    total_size = int(2.5 * part_bytes)
    payload = bytes((i * 31 + 17) % 256 for i in range(total_size))

    stored_parts: dict[int, bytes] = {}
    upload_workers: dict[int, str] = {}
    deleted_msgs: list[tuple[str, int, str | None]] = []
    next_msg_id = 1000

    class FakeWorkerClient:
        def __init__(self, name: str, fail_first_flood: bool = False):
            self.name = name
            self.fail_first_flood = fail_first_flood
            self.upload_calls = 0

        async def get_input_entity(self, target):
            return target

        async def upload_file(self, reader, *, file_size: int, file_name: str, progress_callback=None):
            self.upload_calls += 1
            if self.fail_first_flood:
                self.fail_first_flood = False
                fw = FloodWaitError(request=None, capture=12)
                fw.seconds = 12
                raise fw
            chunks = []
            while True:
                chunk = reader.read(262144)
                if not chunk:
                    break
                chunks.append(chunk)
            data = b"".join(chunks)
            assert len(data) == file_size
            if progress_callback:
                res = progress_callback(len(data), file_size)
                if asyncio.iscoroutine(res):
                    await res
            return SimpleNamespace(data=data, file_name=file_name, file_size=file_size)

        async def send_file(self, peer, uploaded, *, caption: str = "", reply_to=None, force_document: bool = False, **_):
            nonlocal next_msg_id
            assert force_document is True
            assert reply_to == 1  # General topic
            next_msg_id += 1
            mid = next_msg_id
            stored_parts[mid] = uploaded.data
            upload_workers[mid] = self.name
            return SimpleNamespace(id=mid)

        async def get_messages(self, peer, ids: int):
            if ids not in stored_parts:
                return None
            return SimpleNamespace(media=stored_parts[ids])

        def iter_download(self, media: bytes, *, offset: int = 0, request_size: int = 524288):
            async def _gen():
                pos = offset
                while pos < len(media):
                    yield media[pos : pos + 65536]
                    pos += 65536

            return _gen()

        async def delete_messages(self, peer, ids: list[int]):
            for mid in ids:
                deleted_msgs.append((str(peer), int(mid), self.name))
                stored_parts.pop(int(mid), None)
            return [True]

        async def disconnect(self):
            return None

    w1 = FakeWorkerClient("bot1", fail_first_flood=True)
    w2 = FakeWorkerClient("bot2")
    w3 = FakeWorkerClient("bot3")
    pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), w1),
        (TelegramApiConfig(name="bot2", api_id=2, api_hash="h", bot_token="t2"), w2),
        (TelegramApiConfig(name="bot3", api_id=3, api_hash="h", bot_token="t3"), w3),
    ]

    with TestClient(app) as client:
        resp = client.post(
            "/api/ingest/upload",
            data={"folder": "General"},
            files={"file": ("big_archive.bin", payload, "application/octet-stream")},
        )
        item = _wait_for_upload_item(client, resp)
        media_id = item["id"]
        assert item["drive_name"] == "telegram"
        assert item["part_count"] == 3
        assert len(item["parts"]) == 3

        # Verify DB media_parts metadata
        parts = db.get_media_parts(media_id)
        assert len(parts) == 3
        assert [(p["part_index"], p["start_byte"], p["end_byte"], p["size_bytes"]) for p in parts] == [
            (0, 0, part_bytes - 1, part_bytes),
            (1, part_bytes, 2 * part_bytes - 1, part_bytes),
            (2, 2 * part_bytes, total_size - 1, total_size - 2 * part_bytes),
        ]
        # Verify FloodWait put bot1 in cooldown and parts concatenated match original payload
        assert pool._cooldown_until.get("bot1", 0) > 0
        reconstructed = b"".join(stored_parts[p["telegram_msg_id"]] for p in parts)
        assert reconstructed == payload

        # 1) Range inside Part 1
        r1 = client.get(f"/api/media/{media_id}/stream", headers={"Range": "bytes=100-599"})
        assert r1.status_code == 206
        assert r1.content == payload[100:600]

        # 2) Range inside Part 2
        r2 = client.get(
            f"/api/media/{media_id}/stream",
            headers={"Range": f"bytes={part_bytes + 50}-{part_bytes + 1049}"},
        )
        assert r2.status_code == 206
        assert r2.content == payload[part_bytes + 50 : part_bytes + 1050]

        # 3) Range spanning across boundary between Part 1 and Part 2
        cross_start = part_bytes - 1234
        cross_end = part_bytes + 4321
        r_cross = client.get(
            f"/api/media/{media_id}/stream",
            headers={"Range": f"bytes={cross_start}-{cross_end}"},
        )
        assert r_cross.status_code == 206
        assert r_cross.headers["content-range"] == f"bytes {cross_start}-{cross_end}/{total_size}"
        assert r_cross.content == payload[cross_start : cross_end + 1]

        # 4) Range spanning across all 3 parts (Part 1 -> Part 2 -> Part 3)
        span_start = part_bytes - 500
        span_end = 2 * part_bytes + 500
        r_span = client.get(
            f"/api/media/{media_id}/stream",
            headers={"Range": f"bytes={span_start}-{span_end}"},
        )
        assert r_span.status_code == 206
        assert r_span.content == payload[span_start : span_end + 1]

        # 5) Full 200 OK download
        r_full = client.get(f"/api/media/{media_id}/stream?download=true")
        assert r_full.status_code == 200
        assert r_full.content == payload

        # 6) Delete deletes all 3 part messages in Telegram
        del_resp = client.delete(f"/api/media/{media_id}")
        assert del_resp.status_code == 200
        assert len(deleted_msgs) == 3
        assert {m[1] for m in deleted_msgs} == {p["telegram_msg_id"] for p in parts}
        assert db.get_media_parts(media_id) == []


def test_multipart_stream_prepare_error_returns_404_before_streaming(tmp_path: Path, monkeypatch):
    settings = _make_settings(tmp_path, storage_mode="telegram")
    app = create_app(settings)
    db = app.state.db

    item = db.insert_media(
        media_id="mp_err_001",
        title="Broken Multi",
        filename="broken.bin",
        folder="General",
        media_type="file",
        mime_type="application/octet-stream",
        size_bytes=2000,
        drive_name="telegram",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
        telegram_chat_id="-100111",
        telegram_topic_id=1,
        telegram_msg_id=10,
        part_count=2,
        parts=[
            {
                "part_index": 0,
                "start_byte": 0,
                "end_byte": 999,
                "size_bytes": 1000,
                "telegram_chat_id": "-100111",
                "telegram_topic_id": 1,
                "telegram_msg_id": 10,
                "telegram_worker_name": "bot1",
            },
            {
                "part_index": 1,
                "start_byte": 1000,
                "end_byte": 1999,
                "size_bytes": 1000,
                "telegram_chat_id": "-100111",
                "telegram_topic_id": 1,
                "telegram_msg_id": 11,
                "telegram_worker_name": "bot2",
            },
        ],
    )

    async def missing_prepare_stream(chat_id: str, msg_id: int, worker_name: str | None = None):
        raise FileNotFoundError("Part missing in Telegram")

    monkeypatch.setattr(app.state.tg_pool, "prepare_stream", missing_prepare_stream)

    with TestClient(app) as client:
        res = client.get(f"/api/media/{item['id']}/stream", headers={"Range": "bytes=0-100"})
        assert res.status_code == 404


def test_telegram_dm_zero_disk_copy_ingest_and_fallback(tmp_path: Path):
    import asyncio
    from types import SimpleNamespace
    from telethon.errors import FloodWaitError
    from app.config import TelegramApiConfig
    from app.telegram_mtproto import TelegramPool

    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_archive_chat_id = "-100888999"
    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool
    db = app.state.db

    thumb_jpg = _sample_jpeg_bytes()
    fake_media_obj = SimpleNamespace(tag="tg-remote-video")

    class FakeStatusMsg:
        def __init__(self) -> None:
            self.edits: list[str] = []

        async def edit(self, text: str) -> None:
            self.edits.append(text)

    class FakeDmMessage:
        def __init__(self, *, name: str, size: int, mime_type: str, full_bytes: bytes | None = None, has_thumb: bool = True, photo=None, video=None):
            self.id = 42
            self.file = SimpleNamespace(name=name, ext=Path(name).suffix, size=size, mime_type=mime_type)
            self.media = fake_media_obj
            self.photo = photo
            self.video = video
            self.full_bytes = full_bytes or b""
            self.has_thumb = has_thumb
            self.download_calls: list[tuple[str, Any]] = []

        async def download_media(self, file: str | None = None, thumb: Any = None, progress_callback=None):
            self.download_calls.append((str(file), thumb))
            assert file is not None
            out = Path(file)
            out.parent.mkdir(parents=True, exist_ok=True)
            if thumb == -1:
                if not self.has_thumb:
                    return None
                out.write_bytes(thumb_jpg)
                return str(out)
            out.write_bytes(self.full_bytes)
            if progress_callback:
                res = progress_callback(len(self.full_bytes), len(self.full_bytes))
                if asyncio.iscoroutine(res):
                    await res
            return str(out)

    class FakeEvent:
        def __init__(self, msg: FakeDmMessage) -> None:
            self.message = msg
            self.replies: list[FakeStatusMsg] = []

        async def reply(self, text: str):
            sm = FakeStatusMsg()
            sm.edits.append(text)
            self.replies.append(sm)
            return sm

    class FakeWorker:
        def __init__(self, name: str, *, raise_flood: bool = False) -> None:
            self.name = name
            self.raise_flood = raise_flood
            self.sent_args: list[tuple[Any, bytes | None]] = []

        async def get_input_entity(self, target):
            return target

        async def send_file(self, peer, file_arg, **kwargs):
            if self.raise_flood:
                fw = FloodWaitError(request=None, capture=15)
                fw.seconds = 15
                raise fw
            disk_bytes = None
            if isinstance(file_arg, str) and Path(file_arg).is_file():
                disk_bytes = Path(file_arg).read_bytes()
            self.sent_args.append((file_arg, disk_bytes))
            return SimpleNamespace(id=555 if self.name == "bot1" else 666)

    cfg1 = TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1")
    cfg2 = TelegramApiConfig(name="bot2", api_id=2, api_hash="h", bot_token="t2")

    # 1) Happy path: zero-disk-copy reuses event.message.media, only downloads thumb=-1
    w1 = FakeWorker("bot1")
    pool.clients = [(cfg1, w1)]
    msg_video = FakeDmMessage(name="movie.mp4", size=5_000_000, mime_type="video/mp4", video=object(), has_thumb=True)
    ev1 = FakeEvent(msg_video)
    asyncio.run(pool._handle_telegram_media(cfg1, ev1, "General", tags=["clip", "hd"]))

    assert len(msg_video.download_calls) == 1
    assert msg_video.download_calls[0][1] == -1
    assert len(w1.sent_args) == 1
    assert w1.sent_args[0][0] is fake_media_obj
    assert w1.sent_args[0][1] is None

    items, total = db.list_media()
    assert total == 1
    vid_item = items[0]
    assert vid_item["drive_name"] == "telegram"
    assert vid_item["file_path"] == ""
    assert vid_item["size_bytes"] == 5_000_000
    assert vid_item["telegram_msg_id"] == 555
    assert vid_item["telegram_worker_name"] == "bot1"
    assert vid_item["tags"] == ["clip", "hd"]
    assert vid_item["thumb_filename"] is not None
    assert (settings.thumbs_dir / vid_item["thumb_filename"]).is_file()

    jobs = db.list_jobs(limit=5)
    assert jobs[0]["status"] == "done"
    assert jobs[0]["progress"] == 100.0
    assert json.loads(jobs[0]["media_ids_json"]) == [vid_item["id"]]
    assert list(settings.staging_dir.iterdir()) == []

    # 2) Small image without embedded thumb falls back to downloading image for thumbnail, still reuses media
    w1.sent_args.clear()
    msg_img = FakeDmMessage(
        name="photo.jpg",
        size=len(thumb_jpg),
        mime_type="image/jpeg",
        full_bytes=thumb_jpg,
        has_thumb=False,
        photo=object(),
    )
    ev2 = FakeEvent(msg_img)
    asyncio.run(pool._handle_telegram_media(cfg1, ev2, "General"))
    assert [c[1] for c in msg_img.download_calls] == [-1, None]
    assert w1.sent_args[0][0] is fake_media_obj
    items2, _ = db.list_media()
    img_item = items2[0]
    assert img_item["thumb_filename"] is not None
    assert (settings.thumbs_dir / img_item["thumb_filename"]).is_file()
    assert list(settings.staging_dir.iterdir()) == []

    # 3) Fallback when preferred worker hits FloodWait: downloads full file on demand for next worker
    w1_fail = FakeWorker("bot1", raise_flood=True)
    w2_ok = FakeWorker("bot2")
    pool.clients = [(cfg1, w1_fail), (cfg2, w2_ok)]
    full_payload = b"fallback-payload-" * 100
    msg_fb = FakeDmMessage(
        name="fallback.mp4",
        size=len(full_payload),
        mime_type="video/mp4",
        full_bytes=full_payload,
        has_thumb=True,
        video=object(),
    )
    ev3 = FakeEvent(msg_fb)
    asyncio.run(pool._handle_telegram_media(cfg1, ev3, "General"))

    assert [c[1] for c in msg_fb.download_calls] == [-1, None]
    assert pool._cooldown_until.get("bot1", 0) > 0
    assert len(w2_ok.sent_args) == 1
    assert isinstance(w2_ok.sent_args[0][0], str)
    assert w2_ok.sent_args[0][1] == full_payload

    items3, _ = db.list_media()
    fb_item = items3[0]
    assert fb_item["telegram_msg_id"] == 666
    assert fb_item["telegram_worker_name"] == "bot2"
    assert fb_item["drive_name"] == "telegram"
    assert fb_item["file_path"] == ""
    assert list(settings.staging_dir.iterdir()) == []


def test_telegram_stream_block_cache_hits_lru_eviction_and_delete(tmp_path: Path):
    import asyncio
    from types import SimpleNamespace
    from app.config import TelegramApiConfig
    from app.telegram_mtproto import TelegramPool

    # Verify default & aligned tuning settings
    defaults = Settings(_env_file=None)
    assert defaults.telegram_stream_chunk_kb == 512
    assert defaults.telegram_stream_cache_mb == 64
    assert defaults.telegram_upload_concurrency == 4
    assert defaults.telegram_stream_chunk_bytes == 512 * 1024

    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_stream_chunk_kb = 512
    settings.telegram_stream_cache_mb = 1  # 1 MiB = 2 blocks of 512 KiB
    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool
    db = app.state.db

    block_size = 512 * 1024
    total_size = 3 * block_size  # 1.5 MiB (3 blocks: B0, B1, B2)
    payload = bytes((i * 13 + 5) % 256 for i in range(total_size))

    class FakeStreamClient:
        def __init__(self) -> None:
            self.iter_download_calls: list[tuple[int, int]] = []
            self.deleted_ids: list[int] = []

        async def get_input_entity(self, target):
            return target

        async def get_messages(self, peer, ids: int):
            return SimpleNamespace(media=SimpleNamespace(file_id=ids))

        def iter_download(self, media, *, offset: int = 0, request_size: int = 524288):
            self.iter_download_calls.append((offset, request_size))

            async def _gen():
                pos = offset
                while pos < len(payload):
                    end = min(len(payload), pos + request_size)
                    yield payload[pos:end]
                    pos = end

            return _gen()

        async def delete_messages(self, peer, ids: list[int]):
            self.deleted_ids.extend(ids)
            return [True]

        async def disconnect(self):
            return None

    sc = FakeStreamClient()
    pool.clients = [(TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), sc)]

    item = db.insert_media(
        media_id="cache_vid_01",
        title="Seekable Video",
        filename="seek.mp4",
        folder="General",
        media_type="video",
        mime_type="video/mp4",
        size_bytes=total_size,
        drive_name="telegram",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
        telegram_chat_id="-100123456",
        telegram_topic_id=1,
        telegram_msg_id=9001,
        telegram_worker_name="bot1",
    )

    with TestClient(app) as client:
        # 1) Initial Range request inside Block 0 fetches Block 0 from Telegram
        r1 = client.get(f"/api/media/{item['id']}/stream", headers={"Range": "bytes=0-4095"})
        assert r1.status_code == 206
        assert r1.content == payload[0:4096]
        assert len(sc.iter_download_calls) == 1
        assert sc.iter_download_calls[0] == (0, block_size)

        # 2) Repeated Range request inside Block 0 is served from cache with ZERO iter_download calls
        r2 = client.get(f"/api/media/{item['id']}/stream", headers={"Range": "bytes=10000-20000"})
        assert r2.status_code == 206
        assert r2.content == payload[10000:20001]
        assert len(sc.iter_download_calls) == 1

        # 3) Request inside Block 1 fetches Block 1 (cache now holds B0 and B1 = 1 MiB full)
        r3 = client.get(
            f"/api/media/{item['id']}/stream",
            headers={"Range": f"bytes={block_size}-{block_size + 2047}"},
        )
        assert r3.status_code == 206
        assert r3.content == payload[block_size : block_size + 2048]
        assert len(sc.iter_download_calls) == 2

        # 4) Re-read Block 0 (hit -> promotes B0 to MRU, B1 becomes LRU)
        r4 = client.get(f"/api/media/{item['id']}/stream", headers={"Range": "bytes=500-999"})
        assert r4.status_code == 206
        assert r4.content == payload[500:1000]
        assert len(sc.iter_download_calls) == 2

        # 5) Request Block 2 -> exceeds 1 MiB capacity, evicts LRU (B1); cache now holds [B0, B2]
        r5 = client.get(
            f"/api/media/{item['id']}/stream",
            headers={"Range": f"bytes={2 * block_size}-{2 * block_size + 1023}"},
        )
        assert r5.status_code == 206
        assert r5.content == payload[2 * block_size : 2 * block_size + 1024]
        assert len(sc.iter_download_calls) == 3

        # 6) Block 0 is still cached (0 new RPCs)
        r6 = client.get(f"/api/media/{item['id']}/stream", headers={"Range": "bytes=100-199"})
        assert r6.status_code == 206
        assert r6.content == payload[100:200]
        assert len(sc.iter_download_calls) == 3

        # 7) Block 1 was evicted, so requesting Block 1 invokes iter_download again
        r7 = client.get(
            f"/api/media/{item['id']}/stream",
            headers={"Range": f"bytes={block_size + 10}-{block_size + 109}"},
        )
        assert r7.status_code == 206
        assert r7.content == payload[block_size + 10 : block_size + 110]
        assert len(sc.iter_download_calls) == 4
        assert len(pool._block_cache) == 2

        # 8) Deleting the media item invalidates its cached blocks
        del_res = client.delete(f"/api/media/{item['id']}")
        assert del_res.status_code == 200
        assert sc.deleted_ids == [9001]
        assert len(pool._block_cache) == 0

    # Verify cache_mb=0 disables caching cleanly
    settings.telegram_stream_cache_mb = 0
    sc.iter_download_calls.clear()
    media_obj = SimpleNamespace(file_id=9999)

    async def _direct_read():
        out = bytearray()
        async for chunk in pool.iter_telegram_range(sc, media_obj, 0, 1023):
            out.extend(chunk)
        return bytes(out)

    assert asyncio.run(_direct_read()) == payload[:1024]
    assert asyncio.run(_direct_read()) == payload[:1024]
    assert len(sc.iter_download_calls) == 2


def test_rar_ddl_detection_caption_truncation_and_no_caption_reupload_loop(tmp_path: Path):
    import asyncio
    from types import SimpleNamespace
    from telethon.errors import MediaCaptionTooLongError
    from app.config import TelegramApiConfig
    from app.ingest import is_direct_download
    from app.telegram_mtproto import (
        TELEGRAM_CAPTION_MAX_UTF16,
        TelegramPool,
        _utf16_len,
        build_archive_caption,
    )

    # 1) .rar URLs are treated as direct downloads without needing HEAD
    rar_url = (
        "https://cdn.discordapp.com/attachments/111111111111111111/222222222222222222/"
        "archive_release.rar?ex=673b0000&is=6739ae00&hm=" + ("a" * 1200)
    )
    assert asyncio.run(is_direct_download(rar_url)) is True

    # 2) build_archive_caption keeps normal captions intact and caps oversized ones at <= 1024 UTF-16 units
    short_cap = build_archive_caption(
        filename="archive.rar",
        folder="General",
        size_bytes=2048,
        gallery_link="http://localhost:8080/#media=abc123",
        tags=["rar", "backup"],
        source_url="https://cdn.discordapp.com/attachments/1/2/archive.rar",
    )
    assert "**archive.rar**" in short_cap
    assert "Tags: #rar #backup" in short_cap
    assert "Source: https://cdn.discordapp.com/attachments/1/2/archive.rar" in short_cap

    long_cap = build_archive_caption(
        filename="🎬_" + ("very_long_archive_name_" * 12) + ".rar",
        folder="Discord/Archives",
        size_bytes=50 * 1024 * 1024,
        gallery_link="http://localhost:8080/#media=abc123def456",
        tags=[f"tag{i}" for i in range(40)],
        source_url=rar_url,
    )
    assert _utf16_len(long_cap) <= TELEGRAM_CAPTION_MAX_UTF16
    assert "Gallery: http://localhost:8080/#media=abc123def456" in long_cap
    assert "Source: https://cdn.discordapp.com/" in long_cap

    long_part_cap = build_archive_caption(
        filename="big_archive.rar",
        folder="General",
        size_bytes=3 * 1024 * 1024,
        gallery_link="http://localhost:8080/#media=mp123",
        tags=["archive"],
        source_url=rar_url,
        part_index=0,
        total_parts=3,
        part_size_bytes=1024 * 1024,
    )
    assert _utf16_len(long_part_cap) <= TELEGRAM_CAPTION_MAX_UTF16
    assert "(Part 1/3)" in long_part_cap

    # 3) Single-part mirror_to_archive_topic with long Discord .rar URL succeeds on first worker
    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_archive_chat_id = "-100999888"
    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool

    staged_rar = tmp_path / "archive_release.rar"
    staged_rar.write_bytes(b"Rar!\x1a\x07\x01\x00" + b"x" * 1024)

    class RecordingWorker:
        def __init__(self, name: str, *, fail_caption: bool = False) -> None:
            self.name = name
            self.fail_caption = fail_caption
            self.send_calls: list[str] = []
            self.upload_calls = 0

        async def get_input_entity(self, target):
            return target

        async def upload_file(self, reader, *, file_size: int, file_name: str, progress_callback=None):
            self.upload_calls += 1
            while reader.read(65536):
                pass
            return SimpleNamespace(file_name=file_name, file_size=file_size)

        async def send_file(self, peer, file_arg, *, caption: str = "", **_):
            self.send_calls.append(caption)
            if self.fail_caption and len(caption) > 200:
                raise MediaCaptionTooLongError(request=None)
            if self.fail_caption and not isinstance(file_arg, SimpleNamespace):
                raise MediaCaptionTooLongError(request=None)
            return SimpleNamespace(id=700 + len(self.send_calls))

    w1 = RecordingWorker("bot1")
    w2 = RecordingWorker("bot2")
    pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), w1),
        (TelegramApiConfig(name="bot2", api_id=2, api_hash="h", bot_token="t2"), w2),
    ]

    res = asyncio.run(
        pool.mirror_to_archive_topic(
            {
                "media_id": "rar001",
                "title": "archive_release",
                "filename": staged_rar.name,
                "folder": "General",
                "tags": ["discord"],
                "media_type": "file",
                "size_bytes": staged_rar.stat().st_size,
                "local_path": staged_rar,
                "source_url": rar_url,
            }
        )
    )
    assert res is not None
    assert res["worker_name"] == "bot1"
    assert len(w1.send_calls) == 1
    assert len(w2.send_calls) == 0
    assert _utf16_len(w1.send_calls[0]) <= TELEGRAM_CAPTION_MAX_UTF16

    # 4) If MediaCaptionTooLongError ever occurs on single-part disk send_file, it does NOT retry across remaining workers
    f1 = RecordingWorker("bot1", fail_caption=True)
    f2 = RecordingWorker("bot2", fail_caption=True)
    pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), f1),
        (TelegramApiConfig(name="bot2", api_id=2, api_hash="h", bot_token="t2"), f2),
    ]
    try:
        asyncio.run(
            pool.mirror_to_archive_topic(
                {
                    "media_id": "rar002",
                    "title": "archive_release",
                    "filename": staged_rar.name,
                    "folder": "General",
                    "tags": [],
                    "media_type": "file",
                    "size_bytes": staged_rar.stat().st_size,
                    "local_path": staged_rar,
                    "source_url": rar_url,
                }
            )
        )
        assert False, "Expected MediaCaptionTooLongError"
    except MediaCaptionTooLongError:
        pass
    assert len(f1.send_calls) + len(f2.send_calls) == 1

    # 5) On multi-part upload, if send_file rejects caption, it reuses uploaded_handle with compact caption without re-uploading
    settings.telegram_part_size_mb = 1
    big_rar = tmp_path / "big_release.rar"
    big_rar.write_bytes(b"R" * (1024 * 1024 + 512))

    m1 = RecordingWorker("bot1", fail_caption=True)
    m2 = RecordingWorker("bot2", fail_caption=True)
    pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), m1),
        (TelegramApiConfig(name="bot2", api_id=2, api_hash="h", bot_token="t2"), m2),
    ]
    mp_res = asyncio.run(
        pool.mirror_to_archive_topic(
            {
                "media_id": "rar003",
                "title": "big_release",
                "filename": big_rar.name,
                "folder": "General",
                "tags": [],
                "media_type": "file",
                "size_bytes": big_rar.stat().st_size,
                "local_path": big_rar,
                "source_url": rar_url,
            }
        )
    )
    assert mp_res is not None
    assert mp_res["part_count"] == 2
    assert m1.upload_calls + m2.upload_calls == 2  # uploaded each part only ONCE
    assert any(len(c) <= 200 for c in m1.send_calls + m2.send_calls)


def test_long_comic_image_forces_document_and_photo_dimension_fallback(tmp_path: Path, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from telethon.errors import PhotoInvalidDimensionsError
    from telethon.tl import types as tg_types
    from app.config import TelegramApiConfig
    from app.telegram_mtproto import TelegramPool, _should_force_document_for_image

    # 1) Proactive image check forces lossless document uploads by default for all images,
    #    and still guards long comics / oversized images even when compress_photos=True
    normal_img = tmp_path / "normal.jpg"
    Image.new("RGB", (640, 360), (20, 40, 80)).save(normal_img, "JPEG")
    long_comic = tmp_path / "long_comic.jpeg"
    Image.new("RGB", (800, 25000), (200, 120, 60)).save(long_comic, "JPEG")
    wide_strip = tmp_path / "wide_strip.png"
    Image.new("RGB", (12000, 800), (60, 180, 90)).save(wide_strip, "PNG")
    oversized_sq = tmp_path / "oversized.jpg"
    Image.new("RGB", (5000, 5000), (90, 90, 90)).save(oversized_sq, "JPEG")
    fake_vid = tmp_path / "clip.mp4"
    fake_vid.write_bytes(b"mp4")

    assert _should_force_document_for_image(normal_img, "image") is True
    assert _should_force_document_for_image(normal_img, "image", compress_photos=True) is False
    assert _should_force_document_for_image(long_comic, "image", compress_photos=True) is True
    assert _should_force_document_for_image(wide_strip, "image", compress_photos=True) is True
    assert _should_force_document_for_image(oversized_sq, "image", compress_photos=True) is True
    assert _should_force_document_for_image(fake_vid, "video") is False

    # 2) By default (telegram_compress_photos=False), even normal images send with force_document=True
    #    to prevent Telegram from downscaling/JPEG-recompressing pictures
    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_archive_chat_id = "-100999111"
    app = create_app(settings)
    pool: TelegramPool = app.state.tg_pool

    class PhotoAwareWorker:
        def __init__(self, name: str, *, reject_photo_mode: bool = False) -> None:
            self.name = name
            self.reject_photo_mode = reject_photo_mode
            self.calls: list[dict] = []

        async def get_input_entity(self, target):
            return target

        async def send_file(self, peer, file_arg, **kwargs):
            self.calls.append({"file": file_arg, "kwargs": dict(kwargs)})
            if self.reject_photo_mode and not kwargs.get("force_document"):
                raise PhotoInvalidDimensionsError(request=None)
            return SimpleNamespace(id=900 + len(self.calls))

        async def disconnect(self):
            return None

    w_comic = PhotoAwareWorker("bot1", reject_photo_mode=True)
    pool.clients = [(TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), w_comic)]

    normal_lossless_res = asyncio.run(
        pool.mirror_to_archive_topic(
            {
                "media_id": "norm_lossless",
                "title": "normal_lossless",
                "filename": normal_img.name,
                "folder": "General",
                "tags": [],
                "media_type": "image",
                "size_bytes": normal_img.stat().st_size,
                "local_path": normal_img,
                "source_url": "https://example.com/normal.jpg",
            }
        )
    )
    assert normal_lossless_res is not None
    assert normal_lossless_res["message_id"] == 901
    assert len(w_comic.calls) == 1
    assert w_comic.calls[0]["kwargs"].get("force_document") is True

    comic_res = asyncio.run(
        pool.mirror_to_archive_topic(
            {
                "media_id": "comic1",
                "title": "long_comic",
                "filename": long_comic.name,
                "folder": "General",
                "tags": [],
                "media_type": "image",
                "size_bytes": long_comic.stat().st_size,
                "local_path": long_comic,
                "source_url": "https://example.com/images/701/comic.jpeg?123",
            }
        )
    )
    assert comic_res is not None
    assert comic_res["message_id"] == 902
    assert len(w_comic.calls) == 2
    assert w_comic.calls[1]["kwargs"].get("force_document") is True

    # 3) When telegram_compress_photos=True and an image passes proactive bounds,
    #    PhotoInvalidDimensionsError still triggers same-worker force_document=True retry
    settings.telegram_compress_photos = True
    w_fallback = PhotoAwareWorker("bot1", reject_photo_mode=True)
    w_unused = PhotoAwareWorker("bot2", reject_photo_mode=True)
    pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), w_fallback),
        (TelegramApiConfig(name="bot2", api_id=2, api_hash="h", bot_token="t2"), w_unused),
    ]
    pool._rr_index = 0

    fb_res = asyncio.run(
        pool.mirror_to_archive_topic(
            {
                "media_id": "norm1",
                "title": "normal",
                "filename": normal_img.name,
                "folder": "General",
                "tags": [],
                "media_type": "image",
                "size_bytes": normal_img.stat().st_size,
                "local_path": normal_img,
                "source_url": "https://example.com/normal.jpg",
            }
        )
    )
    assert fb_res is not None
    assert fb_res["worker_name"] == "bot1"
    assert len(w_fallback.calls) == 2
    assert w_fallback.calls[0]["kwargs"].get("force_document") is None
    assert w_fallback.calls[1]["kwargs"].get("force_document") is True
    assert len(w_unused.calls) == 0

    # 4) Streaming a Telegram MessageMediaPhoto uses Telegram's actual recompressed size for Content-Length
    recompressed_bytes = b"recompressed-jpeg-payload"
    tg_photo_media = tg_types.MessageMediaPhoto(
        photo=tg_types.Photo(
            id=1,
            access_hash=2,
            file_reference=b"",
            date=0,
            dc_id=1,
            sizes=[
                tg_types.PhotoSizeProgressive(
                    type="y",
                    w=640,
                    h=360,
                    sizes=[10, len(recompressed_bytes)],
                )
            ],
        )
    )
    app.state.db.insert_media(
        media_id="tgphoto1",
        title="Photo",
        filename="photo.jpg",
        folder="General",
        media_type="image",
        mime_type="image/jpeg",
        size_bytes=50000,  # pre-upload disk size > recompressed Telegram size
        drive_name="telegram",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="url",
        source_url="https://example.com/photo.jpg",
        telegram_chat_id="-100999111",
        telegram_topic_id=1,
        telegram_msg_id=42,
        telegram_worker_name="bot1",
    )

    async def fake_prepare_stream(chat_id: str, msg_id: int, worker_name: str | None = None):
        return object(), tg_photo_media

    async def fake_iter_range(_client, _media, start: int, end: int):
        yield recompressed_bytes[start : end + 1]

    monkeypatch.setattr(pool, "prepare_stream", fake_prepare_stream)
    monkeypatch.setattr(pool, "iter_telegram_range", fake_iter_range)

    with TestClient(app) as client:
        stream_res = client.get("/api/media/tgphoto1/stream")
        assert stream_res.status_code == 200
        assert int(stream_res.headers["content-length"]) == len(recompressed_bytes)
        assert stream_res.content == recompressed_bytes
