from __future__ import annotations

import io
import json
from pathlib import Path

from fastapi.testclient import TestClient
from PIL import Image

from app.config import Settings
from app.main import create_app


def _make_settings(
    tmp_path: Path,
    *,
    auth_token: str = "",
    storage_mode: str = "drives",
) -> Settings:
    drive1 = tmp_path / "drives" / "drive1"
    drive2 = tmp_path / "drives" / "drive2"
    drive1.mkdir(parents=True, exist_ok=True)
    drive2.mkdir(parents=True, exist_ok=True)

    return Settings(
        data_dir=tmp_path / "data",
        staging_dir=tmp_path / "staging",
        storage_mode=storage_mode,
        drives_json=json.dumps([
            {"name": "drive1", "path": str(drive1)},
            {"name": "drive2", "path": str(drive2)},
        ]),
        allocation_strategy="first_fit",
        min_free_buffer_mb=0,
        web_auth_token=auth_token,
        telegram_apis_json="[]",
        ytdlp_cookies_file=tmp_path / "data" / "cookies.txt",
    )


def _sample_jpeg_bytes() -> bytes:
    buf = io.BytesIO()
    img = Image.new("RGB", (640, 360), color=(40, 120, 220))
    img.save(buf, format="JPEG")
    return buf.getvalue()


def test_upload_thumbnail_gallery_stream_and_delete(tmp_path: Path):
    settings = _make_settings(tmp_path)
    app = create_app(settings)

    with TestClient(app) as client:
        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["ok"] is True

        drives = client.get("/api/drives").json()["drives"]
        assert len(drives) == 2
        assert drives[0]["mounted"] is True

        # Upload a photo
        jpg = _sample_jpeg_bytes()
        resp = client.post(
            "/api/ingest/upload",
            files={"file": ("sunset.jpg", jpg, "image/jpeg")},
        )
        assert resp.status_code == 201
        item = resp.json()["item"]
        media_id = item["id"]
        assert item["drive_name"] == "drive1"
        assert item["media_type"] == "image"
        assert Path(item["file_path"]).exists()

        # Gallery listing & search
        listing = client.get("/api/media?media_type=image&q=sunset").json()
        assert listing["total"] == 1
        assert listing["items"][0]["id"] == media_id

        # Thumbnail generated
        thumb_resp = client.get(f"/api/media/{media_id}/thumb")
        assert thumb_resp.status_code == 200
        assert thumb_resp.headers["content-type"].startswith("image/jpeg")

        # Full stream + Range 206 stream
        full_resp = client.get(f"/api/media/{media_id}/stream")
        assert full_resp.status_code == 200
        assert full_resp.content == jpg

        range_resp = client.get(
            f"/api/media/{media_id}/stream",
            headers={"Range": "bytes=0-99"},
        )
        assert range_resp.status_code == 206
        assert range_resp.headers["content-range"] == f"bytes 0-99/{len(jpg)}"
        assert range_resp.content == jpg[:100]

        # Delete cleans up file + thumb + DB entry
        del_resp = client.delete(f"/api/media/{media_id}")
        assert del_resp.status_code == 200
        assert not Path(item["file_path"]).exists()
        assert client.get(f"/api/media/{media_id}").status_code == 404


def test_web_auth_token_enforced(tmp_path: Path):
    settings = _make_settings(tmp_path, auth_token="secret-123")
    app = create_app(settings)

    with TestClient(app) as client:
        assert client.get("/api/media").status_code == 401
        assert client.get("/api/media", headers={"Authorization": "Bearer secret-123"}).status_code == 200
        assert client.get("/api/media?token=secret-123").status_code == 200


def test_ui_served_and_url_validation(tmp_path: Path):
    settings = _make_settings(tmp_path)
    app = create_app(settings)

    with TestClient(app) as client:
        home = client.get("/")
        assert home.status_code == 200
        assert "Media Vault" in home.text

        bad = client.post("/api/ingest/url", json={"url": "ftp://example.com/file.mp4"})
        assert bad.status_code == 400


def test_allocator_skips_full_drive_and_supports_most_free(tmp_path: Path, monkeypatch):
    from collections import namedtuple
    from app.allocator import DriveAllocator

    DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])
    settings = _make_settings(tmp_path)
    drive1 = str(tmp_path / "drives" / "drive1")
    drive2 = str(tmp_path / "drives" / "drive2")

    def fake_disk_usage(path):
        s = str(path)
        if s.startswith(drive1):
            return DiskUsage(1000, 950, 50)
        return DiskUsage(1000, 200, 800)

    monkeypatch.setattr("app.allocator.shutil.disk_usage", fake_disk_usage)

    allocator = DriveAllocator(settings)
    staged = tmp_path / "staging" / "big.bin"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(b"x" * 100)

    import asyncio
    drive_name, final_path, _ = asyncio.run(allocator.allocate_and_move(staged))
    assert drive_name == "drive2"
    assert final_path.exists()


def test_folder_routing_filtering_and_mirror_hook(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="both")
    app = create_app(settings)

    mirrored: list[dict] = []

    async def fake_mirror(payload: dict) -> dict:
        mirrored.append(payload)
        return {"chat_id": "-100999", "topic_id": 42, "message_id": 777}

    app.state.ingest.mirror_hook = fake_mirror

    with TestClient(app) as client:
        jpg = _sample_jpeg_bytes()
        resp = client.post(
            "/api/ingest/upload",
            data={"folder": "../Vacation/2026", "forward_to_telegram": "true"},
            files={"file": ("beach.jpg", jpg, "image/jpeg")},
        )
        assert resp.status_code == 201
        item = resp.json()["item"]
        assert item["folder"] == "Vacation/2026"
        assert item["telegram_topic_id"] == 42
        assert item["telegram_msg_id"] == 777
        assert Path(item["file_path"]).parent.name == "2026"
        assert len(mirrored) == 1
        assert mirrored[0]["folder"] == "Vacation/2026"

        folders = client.get("/api/folders").json()["folders"]
        assert "Vacation/2026" in folders

        filtered = client.get("/api/media?folder=Vacation/2026").json()
        assert filtered["total"] == 1
        assert filtered["items"][0]["id"] == item["id"]


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
        assert resp.status_code == 201
        item = resp.json()["item"]
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


def test_ytdlp_cmd_resolves_venv_executable(monkeypatch):
    import sys
    from app.ingest import _ytdlp_cmd

    monkeypatch.setenv("PATH", "")
    cmd = _ytdlp_cmd()
    expected = Path(sys.executable).parent / "yt-dlp"
    if expected.is_file():
        assert cmd == [str(expected)]
    else:
        assert cmd == [sys.executable, "-m", "yt_dlp"]


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


def test_ytdlp_cookies_endpoints_and_args(tmp_path: Path, monkeypatch):
    import asyncio
    from app.ingest import _download_ytdlp, _format_ytdlp_error

    settings = _make_settings(tmp_path)
    app = create_app(settings)

    with TestClient(app) as client:
        assert client.get("/api/health").json()["ytdlp_cookies_configured"] is False

        bad = client.post("/api/ytdlp/cookies", json={"cookies_text": "not valid cookies"})
        assert bad.status_code == 400

        sample_cookies = "# Netscape HTTP Cookie File\n.youtube.com\tTRUE\t/\tTRUE\t0\tVISITOR_INFO1_LIVE\tabc\n"
        ok = client.post("/api/ytdlp/cookies", json={"cookies_text": sample_cookies})
        assert ok.status_code == 200
        assert client.get("/api/health").json()["ytdlp_cookies_configured"] is True

        captured_args: list[tuple[str, ...]] = []

        async def fake_streaming(*args: str, on_line=None):
            captured_args.append(args)
            (tmp_path / "out.mp4").write_bytes(b"mp4")
            return 0, []

        monkeypatch.setattr("app.ingest._run_subprocess_streaming", fake_streaming)
        files = asyncio.run(
            _download_ytdlp(
                "https://www.youtube.com/watch?v=jNQXAC9IVRw",
                tmp_path,
                cookies_file=settings.ytdlp_cookies_file,
                proxy="socks5://127.0.0.1:1080",
            )
        )
        assert len(files) == 1
        assert "--cookies" in captured_args[0]
        assert str(settings.ytdlp_cookies_file) in captured_args[0]
        assert "--proxy" in captured_args[0]
        assert "socks5://127.0.0.1:1080" in captured_args[0]

        cleared = client.delete("/api/ytdlp/cookies")
        assert cleared.status_code == 200
        assert client.get("/api/health").json()["ytdlp_cookies_configured"] is False

    msg = _format_ytdlp_error(
        "ERROR: [youtube] jNQXAC9IVRw: Sign in to confirm you’re not a bot. Use --cookies"
    )
    assert "YouTube bot check" in msg
    assert "cookies.txt" in msg


def test_ytdlp_update_endpoint_and_version_in_health(tmp_path: Path, monkeypatch):
    settings = _make_settings(tmp_path)
    app = create_app(settings)

    async def fake_update_ytdlp():
        return {"ok": True, "before": "2026.10.26", "after": "2026.11.16", "updated": True, "output": "ok"}

    monkeypatch.setattr("app.main.update_ytdlp", fake_update_ytdlp)

    with TestClient(app) as client:
        h = client.get("/api/health").json()
        assert "ytdlp_version" in h

        r = client.post("/api/ytdlp/update")
        assert r.status_code == 200
        assert r.json()["updated"] is True
        assert r.json()["after"] == "2026.11.16"


def test_format_spec_resolution_and_formats_endpoint(tmp_path: Path, monkeypatch):
    from app.ingest import resolve_format_spec

    assert resolve_format_spec(None) == (["-f", "bv*+ba/b", "--merge-output-format", "mp4"], False)
    assert resolve_format_spec("720p") == (
        ["-f", "bv*[height<=720]+ba/b[height<=720]/b", "--merge-output-format", "mp4"],
        False,
    )
    assert resolve_format_spec("audio") == (["-f", "ba/b", "-x", "--audio-format", "mp3"], True)
    assert resolve_format_spec("audio:140") == (["-f", "140"], True)
    assert resolve_format_spec("299+bestaudio/299") == (
        ["-f", "299+bestaudio/299", "--merge-output-format", "mp4"],
        False,
    )

    try:
        resolve_format_spec("bad format; rm -rf")
        assert False, "Expected ValueError"
    except ValueError:
        pass

    settings = _make_settings(tmp_path)
    app = create_app(settings)

    async def fake_subproc(*args: str):
        payload = {
            "title": "Sample Video",
            "duration": 90,
            "webpage_url": "https://example.com/watch?v=abc",
            "formats": [
                {"format_id": "sb0", "ext": "mhtml", "vcodec": "none", "acodec": "none"},
                {"format_id": "140", "ext": "m4a", "vcodec": "none", "acodec": "mp4a.40.2", "filesize": 1200},
                {"format_id": "299", "ext": "mp4", "vcodec": "avc1", "acodec": "none", "height": 1080, "fps": 60, "filesize": 50000},
            ],
        }
        return 0, json.dumps(payload), ""

    monkeypatch.setattr("app.ingest._run_subprocess", fake_subproc)

    with TestClient(app) as client:
        res = client.post("/api/ytdlp/formats", json={"url": "https://example.com/watch?v=abc"})
        assert res.status_code == 200
        body = res.json()
        assert body["title"] == "Sample Video"
        assert [f["id"] for f in body["formats"]] == ["299", "140"]
        assert body["formats"][0]["select_spec"] == "299+bestaudio/299"
        assert body["formats"][1]["select_spec"] == "audio:140"

        bad = client.post("/api/ingest/url", json={"url": "https://example.com/v", "format_spec": "bad;spec"})
        assert bad.status_code == 400


def test_job_progress_and_group_topic_routing(tmp_path: Path, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from app.telegram_mtproto import TelegramPool, parse_format_from_text

    settings = _make_settings(tmp_path)
    app = create_app(settings)
    db = app.state.db
    pool: TelegramPool = app.state.tg_pool

    # Verify job progress updates from streaming yt-dlp output
    async def fake_streaming(*args: str, on_line=None):
        if on_line:
            await on_line("[download]  40.0% of 10.00MiB at 2.00MiB/s ETA 00:03")
        # Find --paths directory in args and create output file
        paths_idx = list(args).index("--paths")
        out_dir = Path(args[paths_idx + 1])
        (out_dir / "video.mp4").write_bytes(b"fake-mp4-data")
        return 0, []

    monkeypatch.setattr("app.ingest._run_subprocess_streaming", fake_streaming)
    job_id = db.create_job("telegram", "url", "https://example.com/video")
    items = asyncio.run(
        app.state.ingest.process_url_job(
            job_id,
            "https://example.com/video",
            source_origin="telegram",
            folder="Clips",
            format_spec="720p",
        )
    )
    assert len(items) == 1
    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == "done"
    assert job["progress"] == 100.0

    # Verify group deduplication claims a message only once across multiple workers
    assert asyncio.run(pool._claim_group_message(-1009998887776, 501)) is True
    assert asyncio.run(pool._claim_group_message(-1009998887776, 501)) is False

    # Verify topic-based folder lookup & explicit #Folder override
    settings.telegram_archive_chat_id = "-1009998887776"
    db.save_topic_id("-1009998887776", "Futa", 77)
    assert db.get_folder_by_topic_id("-1009998887776", 77) == "futa"

    ev_in_topic = SimpleNamespace(
        chat_id=-1009998887776,
        message=SimpleNamespace(reply_to=SimpleNamespace(forum_topic=True, reply_to_top_id=77, reply_to_msg_id=77)),
    )
    assert asyncio.run(pool._resolve_group_folder(None, ev_in_topic, "https://example.com/v", "General")) == "futa"
    assert asyncio.run(pool._resolve_group_folder(None, ev_in_topic, "#Anime https://example.com/v", "General")) == "Anime"
    assert parse_format_from_text("#Anime format:720p https://example.com/v") == "720p"


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
        assert resp.status_code == 201
        item = resp.json()["item"]
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


def test_folder_upload_packs_zip_and_streams_valid_archive(tmp_path: Path):
    import zipfile

    settings = _make_settings(tmp_path, storage_mode="drives")
    app = create_app(settings)

    file_a = b"Hello from file A!\n" * 50
    file_b = b"\x89PNG\r\n\x1a\n" + bytes(range(256)) * 10
    file_c = b"nested config = true\n"

    with TestClient(app) as client:
        resp = client.post(
            "/api/ingest/upload-folder",
            data={
                "archive_name": "ProjectAssets",
                "folder": "Backups",
                "relative_paths": [
                    "ProjectAssets/readme.txt",
                    "ProjectAssets/images/sprite.png",
                    "../evil_prefix/sub/settings.cfg",
                ],
            },
            files=[
                ("files", ("readme.txt", file_a, "text/plain")),
                ("files", ("sprite.png", file_b, "image/png")),
                ("files", ("settings.cfg", file_c, "text/plain")),
            ],
        )
        assert resp.status_code == 201
        item = resp.json()["item"]
        assert item["filename"] == "ProjectAssets.zip"
        assert item["title"] == "ProjectAssets"
        assert item["folder"] == "Backups"

        dl = client.get(f"/api/media/{item['id']}/stream?download=true")
        assert dl.status_code == 200
        zf = zipfile.ZipFile(io.BytesIO(dl.content), "r")
        names = set(zf.namelist())
        assert names == {
            "ProjectAssets/readme.txt",
            "ProjectAssets/images/sprite.png",
            "evil_prefix/sub/settings.cfg",
        }
        assert zf.read("ProjectAssets/readme.txt") == file_a
        assert zf.read("ProjectAssets/images/sprite.png") == file_b
        assert zf.read("evil_prefix/sub/settings.cfg") == file_c


def test_db_migration_and_ui_multipart_markers(tmp_path: Path):
    import sqlite3
    from app.db import Database

    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE media_items (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                filename TEXT NOT NULL,
                media_type TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                drive_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                thumb_filename TEXT,
                source_origin TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_url TEXT,
                telegram_chat_id TEXT,
                telegram_topic_id INTEGER,
                telegram_msg_id INTEGER,
                telegram_worker_name TEXT,
                created_at TEXT NOT NULL
            );
            INSERT INTO media_items (
                id, title, filename, media_type, mime_type, size_bytes,
                drive_name, file_path, thumb_filename, source_origin,
                source_kind, source_url, telegram_chat_id, telegram_topic_id,
                telegram_msg_id, telegram_worker_name, created_at
            ) VALUES (
                'legacy1', 'Legacy', 'legacy.mp4', 'video', 'video/mp4', 4096,
                'telegram', '', NULL, 'web', 'upload', NULL,
                '-100777', 1, 888, 'bot1', '2026-01-01T00:00:00Z'
            );
            """
        )

    db = Database(db_path)
    legacy_item = db.get_media("legacy1")
    assert legacy_item is not None
    assert legacy_item["part_count"] == 1
    assert len(legacy_item["parts"]) == 1
    assert legacy_item["parts"][0]["telegram_msg_id"] == 888
    assert legacy_item["parts"][0]["start_byte"] == 0
    assert legacy_item["parts"][0]["end_byte"] == 4095

    from app.config import Settings

    default_settings = Settings(_env_file=None)
    assert default_settings.max_upload_mb == 20480
    assert default_settings.telegram_part_size_mb == 1900
    assert default_settings.telegram_part_size_bytes == 1900 * 1024 * 1024

    settings = _make_settings(tmp_path)
    app = create_app(settings)
    with TestClient(app) as client:
        html = client.get("/").text
        assert "folderUploadInput" in html
        assert "Unified Stream" in html
        health = client.get("/api/health").json()
        assert health["max_upload_mb"] == settings.max_upload_mb
        assert health["telegram_part_size_mb"] == settings.telegram_part_size_mb


def test_hashtags_ingest_filter_patch_and_telegram_caption(tmp_path: Path, monkeypatch):
    from app.db import normalize_tags
    from app.telegram_mtproto import parse_tags_from_text

    assert normalize_tags("#soap, #HD soap  wrestling!") == ["soap", "hd", "wrestling"]
    assert parse_tags_from_text("https://x.com/v folder:lesbian #soap #hd") == ["soap", "hd"]
    assert parse_tags_from_text("https://x.com/v tag:soap,hd") == ["soap", "hd"]
    assert parse_tags_from_text("https://x.com/v #lesbian") == []

    settings = _make_settings(tmp_path)
    settings.storage_mode = "both"
    app = create_app(settings)
    captured_payloads = []

    async def fake_mirror(payload):
        captured_payloads.append(payload)
        return {
            "chat_id": "-100999",
            "topic_id": 11,
            "message_id": 1001,
            "worker_name": "bot1",
            "part_count": 1,
            "parts": [
                {
                    "part_index": 0,
                    "start_byte": 0,
                    "end_byte": payload["size_bytes"] - 1,
                    "size_bytes": payload["size_bytes"],
                    "telegram_chat_id": "-100999",
                    "telegram_topic_id": 11,
                    "telegram_msg_id": 1001,
                    "telegram_worker_name": "bot1",
                }
            ],
        }

    app.state.ingest.mirror_hook = fake_mirror

    with TestClient(app) as client:
        res1 = client.post(
            "/api/ingest/upload",
            files={"file": ("clip1.bin", b"a" * 128, "application/octet-stream")},
            data={"folder": "lesbian", "tags": "#soap, HD"},
        )
        assert res1.status_code == 201
        item1 = res1.json()["item"]
        assert item1["folder"] == "lesbian"
        assert item1["tags"] == ["soap", "hd"]
        assert captured_payloads[0]["tags"] == ["soap", "hd"]

        res2 = client.post(
            "/api/ingest/upload",
            files={"file": ("clip2.bin", b"b" * 128, "application/octet-stream")},
            data={"folder": "lesbian", "tags": "#soapy"},
        )
        assert res2.status_code == 201

        tags_res = client.get("/api/tags").json()
        assert tags_res["tags"] == ["hd", "soap", "soapy"]

        filtered = client.get("/api/media?tag=soap").json()
        assert filtered["total"] == 1
        assert filtered["items"][0]["id"] == item1["id"]

        patched = client.patch(f"/api/media/{item1['id']}/tags", json={"tags": "#soap #archive"}).json()
        assert patched["tags"] == ["soap", "archive"]

        html = client.get("/").text
        assert "tagsInput" in html
        assert "tagFilter" in html
        assert "modalEditTagsBtn" in html


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
