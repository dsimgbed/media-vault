from __future__ import annotations

import io
import json
import time
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


def _wait_for_upload_item(client: TestClient, resp, *, timeout: float = 5.0) -> dict:
    assert resp.status_code == 202, resp.text
    body = resp.json()
    job_id = body["job_id"]
    deadline = time.monotonic() + timeout
    job = None
    while time.monotonic() < deadline:
        job_resp = client.get(f"/api/jobs/{job_id}")
        assert job_resp.status_code == 200
        job = job_resp.json()
        if job["status"] in ("done", "failed"):
            break
        time.sleep(0.01)
    assert job is not None
    assert job["status"] == "done", f"Job {job_id} did not finish cleanly: {job}"
    media_ids = json.loads(job["media_ids_json"])
    assert len(media_ids) >= 1
    media_resp = client.get(f"/api/media/{media_ids[0]}")
    assert media_resp.status_code == 200
    return media_resp.json()


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
        item = _wait_for_upload_item(client, resp)
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
        assert thumb_resp.headers["content-type"].startswith("image/webp")
        assert thumb_resp.headers["cache-control"] == "public, max-age=31536000, immutable"

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
        item = _wait_for_upload_item(client, resp)
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
        item = _wait_for_upload_item(client, resp)
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
    assert default_settings.ingest_concurrency == 3
    assert default_settings.ytdlp_concurrent_fragments == 4

    settings = _make_settings(tmp_path)
    app = create_app(settings)
    with TestClient(app) as client:
        html = client.get("/").text
        assert "folderUploadInput" in html
        assert "Unified Stream" in html
        health = client.get("/api/health").json()
        assert health["max_upload_mb"] == settings.max_upload_mb
        assert health["telegram_part_size_mb"] == settings.telegram_part_size_mb
        assert health["ingest_concurrency"] == settings.ingest_concurrency


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
        item1 = _wait_for_upload_item(client, res1)
        assert item1["folder"] == "lesbian"
        assert item1["tags"] == ["soap", "hd"]
        assert captured_payloads[0]["tags"] == ["soap", "hd"]

        res2 = client.post(
            "/api/ingest/upload",
            files={"file": ("clip2.bin", b"b" * 128, "application/octet-stream")},
            data={"folder": "lesbian", "tags": "#soapy"},
        )
        _wait_for_upload_item(client, res2)

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


def test_decoupled_202_upload_and_folder_upload_with_background_mirror_and_413(tmp_path: Path):
    import asyncio
    import threading

    settings = _make_settings(tmp_path, storage_mode="telegram")
    app = create_app(settings)

    mirror_started = threading.Event()
    mirror_release = threading.Event()
    mirror_calls: list[str] = []

    async def blocking_mirror(payload: dict) -> dict:
        mirror_calls.append(payload["filename"])
        mirror_started.set()
        while not mirror_release.is_set():
            await asyncio.sleep(0.01)
        cb = payload.get("progress_cb")
        if cb:
            await cb(75.0, "Uploading to Telegram: 75%")
        return {
            "chat_id": "-100123",
            "topic_id": 1,
            "message_id": 100 + len(mirror_calls),
            "worker_name": "bot1",
        }

    app.state.ingest.mirror_hook = blocking_mirror
    jpg = _sample_jpeg_bytes()

    with TestClient(app) as client:
        # 1) Single file upload returns 202 immediately while mirror_hook is still blocked
        resp = client.post(
            "/api/ingest/upload",
            data={"folder": "General"},
            files={"file": ("async_photo.jpg", jpg, "image/jpeg")},
        )
        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "queued"
        job_id = body["job_id"]

        assert mirror_started.wait(timeout=2.0)
        mid_job = client.get(f"/api/jobs/{job_id}").json()
        assert mid_job["status"] == "allocating"

        # Release mirror_hook -> job transitions to done
        mirror_release.set()
        item = _wait_for_upload_item(client, resp)
        assert item["filename"] == "async_photo.jpg"
        assert item["telegram_msg_id"] == 101

        # 2) Folder upload also returns 202 immediately while mirror_hook is blocked
        mirror_started.clear()
        mirror_release.clear()
        folder_resp = client.post(
            "/api/ingest/upload-folder",
            data={"archive_name": "AsyncBundle", "relative_paths": ["AsyncBundle/a.bin"]},
            files=[("files", ("a.bin", b"hello-bundle" * 10, "application/octet-stream"))],
        )
        assert folder_resp.status_code == 202
        f_body = folder_resp.json()
        assert f_body["status"] == "queued"
        f_job_id = f_body["job_id"]

        assert mirror_started.wait(timeout=2.0)
        f_mid_job = client.get(f"/api/jobs/{f_job_id}").json()
        assert f_mid_job["status"] == "allocating"

        mirror_release.set()
        f_item = _wait_for_upload_item(client, folder_resp)
        assert f_item["filename"] == "AsyncBundle.zip"
        assert f_item["telegram_msg_id"] == 102

        # 3) 413 payload-too-large fails synchronously and cleans staging_dir
        settings.max_upload_mb = 0
        too_big = client.post(
            "/api/ingest/upload",
            files={"file": ("too_big.bin", b"12345", "application/octet-stream")},
        )
        assert too_big.status_code == 413
        assert list(settings.staging_dir.iterdir()) == []

        too_big_folder = client.post(
            "/api/ingest/upload-folder",
            data={"archive_name": "TooBigFolder", "relative_paths": ["TooBigFolder/x.bin"]},
            files=[("files", ("x.bin", b"12345", "application/octet-stream"))],
        )
        assert too_big_folder.status_code == 413
        assert list(settings.staging_dir.iterdir()) == []


def test_bounded_concurrency_url_job_queue(tmp_path: Path, monkeypatch):
    import asyncio
    import threading

    settings = _make_settings(tmp_path, storage_mode="drives")
    settings.ingest_concurrency = 2
    app = create_app(settings)
    db = app.state.db

    active_lock = threading.Lock()
    active_count = 0
    max_seen_active = 0
    two_running = threading.Event()
    release_gate = threading.Event()

    async def fake_is_ddl(url: str) -> bool:
        return False

    async def fake_ytdlp(
        url: str,
        job_dir: Path,
        *,
        cookies_file=None,
        proxy: str = "",
        format_spec=None,
        concurrent_fragments: int = 4,
        on_progress=None,
    ):
        nonlocal active_count, max_seen_active
        with active_lock:
            active_count += 1
            max_seen_active = max(max_seen_active, active_count)
            if active_count == 2:
                two_running.set()
        try:
            while not release_gate.is_set():
                await asyncio.sleep(0.01)
            out_file = job_dir / f"{url.rsplit('/', 1)[-1]}.mp4"
            out_file.write_bytes(b"fake-video-bytes")
            return [out_file]
        finally:
            with active_lock:
                active_count -= 1

    monkeypatch.setattr("app.ingest.is_direct_download", fake_is_ddl)
    monkeypatch.setattr("app.ingest._download_ytdlp", fake_ytdlp)

    with TestClient(app) as client:
        job_ids = []
        for idx in range(4):
            res = client.post("/api/ingest/url", json={"url": f"https://example.com/vid_{idx}"})
            assert res.status_code == 202
            job_ids.append(res.json()["job_id"])

        assert two_running.wait(timeout=2.0)
        # Allow a moment for jobs to settle at the semaphore
        deadline = time.monotonic() + 2.0
        statuses = []
        while time.monotonic() < deadline:
            statuses = [client.get(f"/api/jobs/{jid}").json()["status"] for jid in job_ids]
            if statuses.count("downloading") == 2 and statuses.count("queued") == 2:
                break
            time.sleep(0.01)

        assert statuses.count("downloading") == 2
        assert statuses.count("queued") == 2
        assert max_seen_active <= 2

        release_gate.set()

        finish_deadline = time.monotonic() + 5.0
        while time.monotonic() < finish_deadline:
            statuses = [client.get(f"/api/jobs/{jid}").json()["status"] for jid in job_ids]
            if all(s == "done" for s in statuses):
                break
            time.sleep(0.01)

        assert all(s == "done" for s in statuses)
        assert max_seen_active == 2
        assert len(db.list_media()[0]) == 4


def test_startup_crash_recovery_marks_stuck_jobs_and_cleans_staging(tmp_path: Path):
    from app.db import Database

    settings = _make_settings(tmp_path, storage_mode="drives")
    settings.ensure_dirs()
    db = Database(settings.db_path)

    j_queued = db.create_job("web", "url", "https://example.com/1")
    j_downloading = db.create_job("web", "url", "https://example.com/2")
    db.update_job(j_downloading, status="downloading", progress=30.0, message="Downloading...")
    j_allocating = db.create_job("web", "upload", "file.bin")
    db.update_job(j_allocating, status="allocating", progress=75.0, message="Allocating...")
    j_done = db.create_job("web", "upload", "ok.bin")
    db.update_job(j_done, status="done", progress=100.0, message="Done")

    orphan_dir = settings.staging_dir / "orphan_tmp"
    orphan_dir.mkdir(parents=True, exist_ok=True)
    (orphan_dir / "partial.part").write_bytes(b"partial")
    orphan_file = settings.staging_dir / "stray.tmp"
    orphan_file.write_bytes(b"stray")

    app = create_app(settings)
    with TestClient(app) as client:
        for jid in (j_queued, j_downloading, j_allocating):
            job = client.get(f"/api/jobs/{jid}").json()
            assert job["status"] == "failed"
            assert job["message"] == "Interrupted by server restart"

        done_job = client.get(f"/api/jobs/{j_done}").json()
        assert done_job["status"] == "done"
        assert done_job["message"] == "Done"
        assert list(settings.staging_dir.iterdir()) == []


def test_subprocess_streaming_chunk_splitting_and_ytdlp_flags(tmp_path: Path, monkeypatch):
    import asyncio
    from app.ingest import _download_ytdlp, _run_subprocess_streaming

    class FakeStdout:
        def __init__(self, chunks: list[bytes]) -> None:
            self._chunks = list(chunks)

        async def read(self, n: int) -> bytes:
            if self._chunks:
                return self._chunks.pop(0)
            return b""

    class FakeProc:
        def __init__(self, chunks: list[bytes]) -> None:
            self.stdout = FakeStdout(chunks)
            self.returncode = 0

        async def wait(self) -> int:
            return self.returncode

    # Mix \r, \n, and \r\n across chunk boundaries + >80 lines total to verify deque cap
    chunks = [
        b"line_000\r",
        b"\nline_001\rline_002\npa",
        b"rtial_003\r\n",
    ]
    for i in range(4, 100):
        chunks.append(f"line_{i:03d}\n".encode())
    chunks.append(b"final_trailing_no_newline")

    async def fake_exec(*args, **kwargs):
        return FakeProc(chunks)

    monkeypatch.setattr("app.ingest.asyncio.create_subprocess_exec", fake_exec)

    seen_lines: list[str] = []

    async def on_line(line: str) -> None:
        seen_lines.append(line)

    rc, tail = asyncio.run(_run_subprocess_streaming("dummy-cmd", on_line=on_line))
    assert rc == 0
    assert seen_lines[0] == "line_000"
    assert seen_lines[1] == "line_001"
    assert seen_lines[2] == "line_002"
    assert seen_lines[3] == "partial_003"
    assert seen_lines[-1] == "final_trailing_no_newline"
    assert len(seen_lines) == 101
    assert len(tail) == 80
    assert tail == seen_lines[-80:]

    # Verify _download_ytdlp passes --concurrent-fragments and faststart postprocessor-args for video only
    recorded_args: list[tuple[str, ...]] = []

    async def fake_streaming(*args: str, on_line=None):
        recorded_args.append(args)
        paths_idx = list(args).index("--paths")
        out_dir = Path(args[paths_idx + 1])
        (out_dir / "out.mp4").write_bytes(b"data")
        return 0, []

    monkeypatch.setattr("app.ingest._run_subprocess_streaming", fake_streaming)

    vid_dir = tmp_path / "vid"
    vid_dir.mkdir()
    asyncio.run(
        _download_ytdlp(
            "https://example.com/watch?v=123",
            vid_dir,
            format_spec="720p",
            concurrent_fragments=6,
        )
    )
    vid_args = list(recorded_args[0])
    cf_idx = vid_args.index("--concurrent-fragments")
    assert vid_args[cf_idx + 1] == "6"
    pp_idx = vid_args.index("--postprocessor-args")
    assert vid_args[pp_idx + 1] == "Merger+ffmpeg:-movflags +faststart"

    aud_dir = tmp_path / "aud"
    aud_dir.mkdir()
    asyncio.run(
        _download_ytdlp(
            "https://example.com/watch?v=123",
            aud_dir,
            format_spec="audio",
            concurrent_fragments=3,
        )
    )
    aud_args = list(recorded_args[1])
    cf_idx_a = aud_args.index("--concurrent-fragments")
    assert aud_args[cf_idx_a + 1] == "3"
    assert "Merger+ffmpeg:-movflags +faststart" not in aud_args


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


def test_persistent_sqlite_connection_pragmas_close_and_reconnect(tmp_path: Path):
    from app.db import Database

    db_path = tmp_path / "persistent.db"
    db = Database(db_path)

    with db._connect() as conn1:
        conn_id_1 = id(conn1)
        jm = conn1.execute("PRAGMA journal_mode;").fetchone()[0]
        sync = conn1.execute("PRAGMA synchronous;").fetchone()[0]
        temp_store = conn1.execute("PRAGMA temp_store;").fetchone()[0]
        fk = conn1.execute("PRAGMA foreign_keys;").fetchone()[0]

    assert str(jm).lower() == "wal"
    assert int(sync) == 1  # NORMAL
    assert int(temp_store) == 2  # MEMORY
    assert int(fk) == 1  # ON

    with db._connect() as conn2:
        assert id(conn2) == conn_id_1

    db.close()
    assert db._conn is None

    # Lazy reconnect after close()
    jid = db.create_job("web", "url", "https://example.com/after-close")
    assert db.get_job(jid) is not None
    assert db._conn is not None
    db.close()

    # App lifespan calls db.close() cleanly
    settings = _make_settings(tmp_path)
    app = create_app(settings)
    with TestClient(app) as client:
        assert client.get("/api/health").status_code == 200
        assert app.state.db._conn is not None
    assert app.state.db._conn is None


def test_normalized_media_tags_table_and_legacy_backfill(tmp_path: Path):
    import sqlite3
    from app.db import Database

    db_path = tmp_path / "tags_legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE media_items (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                filename TEXT NOT NULL,
                folder TEXT NOT NULL DEFAULT 'General',
                tags TEXT NOT NULL DEFAULT '',
                media_type TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                drive_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                thumb_filename TEXT,
                source_origin TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_url TEXT,
                created_at TEXT NOT NULL
            );
            INSERT INTO media_items (
                id, title, filename, folder, tags, media_type, mime_type, size_bytes,
                drive_name, file_path, thumb_filename, source_origin, source_kind, source_url, created_at
            ) VALUES (
                'leg_tag_1', 'Old Item', 'old.jpg', 'Archive', 'soap,hd', 'image', 'image/jpeg', 123,
                'drive1', '', NULL, 'web', 'upload', NULL, '2026-01-01T00:00:00Z'
            );
            """
        )

    db = Database(db_path)
    try:
        # Backfilled on startup
        with db._connect() as conn:
            rows = conn.execute(
                "SELECT tag FROM media_tags WHERE media_id = ? ORDER BY tag ASC",
                ("leg_tag_1",),
            ).fetchall()
        assert [r["tag"] for r in rows] == ["hd", "soap"]
        assert db.list_tags() == ["hd", "soap"]

        # Insert populates media_tags
        db.insert_media(
            media_id="new_tag_2",
            title="New Item",
            filename="new.jpg",
            folder="Archive",
            media_type="image",
            mime_type="image/jpeg",
            size_bytes=456,
            drive_name="drive1",
            file_path="",
            thumb_filename=None,
            source_origin="web",
            source_kind="upload",
            source_url=None,
            tags="#soap, #soapy, #4k",
        )
        assert db.list_tags() == ["4k", "hd", "soap", "soapy"]

        # Tag filtering uses exact normalized tags (soap != soapy)
        items_soap, total_soap = db.list_media(tag="soap")
        assert total_soap == 2
        assert {it["id"] for it in items_soap} == {"leg_tag_1", "new_tag_2"}

        items_multi, total_multi = db.list_media(tag="soap,4k")
        assert total_multi == 1
        assert items_multi[0]["id"] == "new_tag_2"

        # Update tags replaces media_tags rows
        db.update_media_tags("new_tag_2", "#cinema")
        with db._connect() as conn:
            new_rows = conn.execute(
                "SELECT tag FROM media_tags WHERE media_id = ? ORDER BY tag ASC",
                ("new_tag_2",),
            ).fetchall()
        assert [r["tag"] for r in new_rows] == ["cinema"]
        assert db.list_tags() == ["cinema", "hd", "soap"]

        # Delete removes media_tags rows
        db.delete_media("leg_tag_1")
        with db._connect() as conn:
            rem = conn.execute(
                "SELECT tag FROM media_tags WHERE media_id = ?",
                ("leg_tag_1",),
            ).fetchall()
        assert rem == []
        assert db.list_tags() == ["cinema"]
    finally:
        db.close()


def test_fts5_search_prefix_multitoken_sync_and_legacy_backfill(tmp_path: Path):
    import sqlite3
    from app.db import Database

    db_path = tmp_path / "fts_legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE media_items (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                filename TEXT NOT NULL,
                folder TEXT NOT NULL DEFAULT 'General',
                tags TEXT NOT NULL DEFAULT '',
                media_type TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                drive_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                thumb_filename TEXT,
                source_origin TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_url TEXT,
                created_at TEXT NOT NULL
            );
            INSERT INTO media_items (
                id, title, filename, folder, tags, media_type, mime_type, size_bytes,
                drive_name, file_path, thumb_filename, source_origin, source_kind, source_url, created_at
            ) VALUES (
                'leg_fts_1', 'Golden Horizon', 'sunset.jpg', 'Vacation/2026', 'beach,golden',
                'image', 'image/jpeg', 500, 'drive1', '', NULL, 'web', 'upload',
                'https://cdn.example.org/photos/sunset.jpg', '2026-01-01T00:00:00Z'
            );
            """
        )

    db = Database(db_path)
    try:
        # Legacy row backfilled into media_fts
        res_prefix, total_prefix = db.list_media(q="sun")
        assert total_prefix == 1
        assert res_prefix[0]["id"] == "leg_fts_1"

        # Insert second item
        db.insert_media(
            media_id="fts_2",
            title="Mountain Hike",
            filename="alpine-trail_4k.mp4",
            folder="Trips/Alps",
            media_type="video",
            mime_type="video/mp4",
            size_bytes=9000,
            drive_name="drive1",
            file_path="",
            thumb_filename=None,
            source_origin="web",
            source_kind="ytdlp",
            source_url="https://video.example.net/watch?v=alps99",
            tags="#nature, #hike",
        )

        # Multi-token FTS prefix search across title/folder/url/tags
        res_multi, total_multi = db.list_media(q="mount alps")
        assert total_multi == 1
        assert res_multi[0]["id"] == "fts_2"

        res_url, total_url = db.list_media(q="alps99")
        assert total_url == 1
        assert res_url[0]["id"] == "fts_2"

        # Mid-word substring fallback via LIKE
        res_mid, total_mid = db.list_media(q="unset")
        assert total_mid == 1
        assert res_mid[0]["id"] == "leg_fts_1"

        # Update tags updates FTS index
        db.update_media_tags("fts_2", "#glacier")
        assert db.list_media(q="glaci")[1] == 1
        assert db.list_media(q="nature")[1] == 0

        # Delete removes from FTS index
        db.delete_media("fts_2")
        assert db.list_media(q="glaci")[1] == 0
        with db._connect() as conn:
            fts_left = conn.execute(
                "SELECT media_id FROM media_fts WHERE media_id = ?",
                ("fts_2",),
            ).fetchall()
        assert fts_left == []
    finally:
        db.close()


def test_webp_thumbnails_video_fallback_and_legacy_jpg_compat(tmp_path: Path, monkeypatch):
    import asyncio
    from app.thumbnails import generate_thumbnail

    settings = _make_settings(tmp_path)
    app = create_app(settings)
    db = app.state.db

    # 1) Image upload generates .webp thumbnail and serves immutable Cache-Control
    with TestClient(app) as client:
        jpg = _sample_jpeg_bytes()
        resp = client.post(
            "/api/ingest/upload",
            files={"file": ("beach_webp.jpg", jpg, "image/jpeg")},
        )
        item = _wait_for_upload_item(client, resp)
        assert item["thumb_filename"] == f"{item['id']}.webp"
        thumb_file = settings.thumbs_dir / item["thumb_filename"]
        assert thumb_file.is_file()
        with Image.open(thumb_file) as im:
            assert im.format == "WEBP"

        thumb_resp = client.get(f"/api/media/{item['id']}/thumb")
        assert thumb_resp.status_code == 200
        assert thumb_resp.headers["content-type"] == "image/webp"
        assert thumb_resp.headers["cache-control"] == "public, max-age=31536000, immutable"

        # 2) Legacy .jpg thumbnail on disk still served as image/jpeg with immutable Cache-Control
        legacy_id = "legacy_jpg_thumb"
        legacy_path = settings.thumbs_dir / f"{legacy_id}.jpg"
        legacy_path.write_bytes(jpg)
        db.insert_media(
            media_id=legacy_id,
            title="Legacy Thumb",
            filename="legacy.jpg",
            folder="General",
            media_type="image",
            mime_type="image/jpeg",
            size_bytes=len(jpg),
            drive_name="drive1",
            file_path="",
            thumb_filename=f"{legacy_id}.jpg",
            source_origin="web",
            source_kind="upload",
            source_url=None,
        )
        leg_resp = client.get(f"/api/media/{legacy_id}/thumb")
        assert leg_resp.status_code == 200
        assert leg_resp.headers["content-type"] == "image/jpeg"
        assert leg_resp.headers["cache-control"] == "public, max-age=31536000, immutable"

        # 3) Fallback lookup when thumb_filename points to missing .webp but legacy .jpg exists on disk
        fallback_id = "legacy_fallback_thumb"
        (settings.thumbs_dir / f"{fallback_id}.jpg").write_bytes(jpg)
        db.insert_media(
            media_id=fallback_id,
            title="Fallback Thumb",
            filename="fb.jpg",
            folder="General",
            media_type="image",
            mime_type="image/jpeg",
            size_bytes=len(jpg),
            drive_name="drive1",
            file_path="",
            thumb_filename=f"{fallback_id}.webp",
            source_origin="web",
            source_kind="upload",
            source_url=None,
        )
        fb_resp = client.get(f"/api/media/{fallback_id}/thumb")
        assert fb_resp.status_code == 200
        assert fb_resp.headers["content-type"] == "image/jpeg"
        assert fb_resp.headers["cache-control"] == "public, max-age=31536000, immutable"

    # 4) Video thumbnail falls back to PNG + Pillow WebP when ffmpeg fails libwebp
    calls: list[tuple[str, ...]] = []

    class FakeProc:
        def __init__(self, rc: int) -> None:
            self.returncode = rc

        async def communicate(self):
            return b"", b""

    async def fake_exec(*args, **kwargs):
        calls.append(tuple(str(a) for a in args))
        out_arg = str(args[-1])
        if out_arg.endswith(".webp"):
            return FakeProc(1)  # simulate ffmpeg without libwebp
        # PNG extraction succeeds
        Image.new("RGB", (320, 180), color=(20, 160, 90)).save(out_arg, format="PNG")
        return FakeProc(0)

    monkeypatch.setattr("app.thumbnails.shutil.which", lambda _: "/usr/bin/ffmpeg")
    monkeypatch.setattr("app.thumbnails.asyncio.create_subprocess_exec", fake_exec)

    vid_thumb = asyncio.run(
        generate_thumbnail(tmp_path / "dummy.mp4", "video", settings.thumbs_dir, "vid_fallback_1")
    )
    assert vid_thumb == "vid_fallback_1.webp"
    assert (settings.thumbs_dir / "vid_fallback_1.webp").is_file()
    with Image.open(settings.thumbs_dir / "vid_fallback_1.webp") as im:
        assert im.format == "WEBP"
    assert any(a.endswith(".tmp.png") for call in calls for a in call)


def test_sse_events_endpoint_and_frontend_polling_removed(tmp_path: Path):
    import threading

    settings = _make_settings(tmp_path, auth_token="sse-secret")
    app = create_app(settings)
    db = app.state.db

    with TestClient(app) as client:
        # Auth enforced on /api/events
        assert client.get("/api/events").status_code == 401

        # UI HTML uses EventSource on /api/events and does NOT re-fetch gallery on active.length > 0
        html = client.get("/").text
        assert 'new EventSource(withToken("/api/events"))' in html
        assert "if (newlyFinished)" in html
        assert "newlyFinished || active.length > 0" not in html

        # Trigger live job + media events shortly after SSE stream connects
        def _emit_events():
            time.sleep(0.15)
            jid = db.create_job("web", "url", "https://example.com/sse-test")
            db.update_job(jid, status="downloading", progress=45.0, message="Downloading...")
            db.insert_media(
                media_id="sse_media_1",
                title="SSE Item",
                filename="sse.bin",
                folder="General",
                media_type="file",
                mime_type="application/octet-stream",
                size_bytes=64,
                drive_name="drive1",
                file_path="",
                thumb_filename=None,
                source_origin="web",
                source_kind="upload",
                source_url=None,
            )
            db.update_job(jid, status="done", progress=100.0, message="Done")

        emitter = threading.Thread(target=_emit_events, daemon=True)
        emitter.start()

        received: list[dict] = []
        with client.stream("GET", "/api/events?token=sse-secret") as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")
            assert resp.headers["cache-control"] == "no-cache"
            for line in resp.iter_lines():
                if line.startswith("data: "):
                    ev = json.loads(line[6:])
                    received.append(ev)
                    if ev.get("type") == "job_update" and ev.get("status") == "done":
                        break

        emitter.join(timeout=2.0)

        assert received[0]["type"] == "connected"
        assert received[0]["ok"] is True
        assert any(e.get("type") == "job_update" and e.get("status") == "downloading" for e in received)
        assert any(e.get("type") == "media_update" and e.get("media_id") == "sse_media_1" for e in received)
        assert any(e.get("type") == "job_update" and e.get("status") == "done" for e in received)


def _make_sample_mp4(dst: Path) -> bool:
    import shutil
    import subprocess

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return False
    res = subprocess.run(
        [
            ffmpeg,
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=1.5:size=320x180:rate=24",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=1.5",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-shortest",
            str(dst),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return res.returncode == 0 and dst.is_file() and dst.stat().st_size > 0


def test_rich_metadata_video_preview_fallback_and_telegram_zerocopy(tmp_path: Path, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from app.thumbnails import extract_media_metadata, generate_video_preview

    settings = _make_settings(tmp_path)
    app = create_app(settings)

    with TestClient(app) as client:
        # 1) Image upload extracts width/height
        jpg = _sample_jpeg_bytes()
        img_resp = client.post(
            "/api/ingest/upload",
            files={"file": ("photo_meta.jpg", jpg, "image/jpeg")},
        )
        img_item = _wait_for_upload_item(client, img_resp)
        assert img_item["width"] == 640
        assert img_item["height"] == 360

        # 2) Real MP4 video upload extracts metadata and generates animated WebP preview
        sample_mp4 = tmp_path / "sample.mp4"
        if _make_sample_mp4(sample_mp4):
            vid_bytes = sample_mp4.read_bytes()
            vid_resp = client.post(
                "/api/ingest/upload",
                files={"file": ("clip_meta.mp4", vid_bytes, "video/mp4")},
            )
            vid_item = _wait_for_upload_item(client, vid_resp, timeout=15.0)
            vid_id = vid_item["id"]
            assert vid_item["width"] == 320
            assert vid_item["height"] == 180
            assert vid_item["duration_sec"] and 1.0 <= vid_item["duration_sec"] <= 2.5
            assert vid_item["video_codec"] == "h264"
            assert vid_item["audio_codec"] == "aac"
            assert vid_item["fps"] and abs(vid_item["fps"] - 24.0) < 0.5
            assert vid_item["preview_filename"] == f"{vid_id}_preview.webp"

            prev_path = settings.thumbs_dir / vid_item["preview_filename"]
            assert prev_path.is_file()

            prev_resp = client.get(f"/api/media/{vid_id}/preview")
            assert prev_resp.status_code == 200
            assert prev_resp.headers["content-type"] == "image/webp"
            assert prev_resp.headers["cache-control"] == "public, max-age=31536000, immutable"
            with Image.open(io.BytesIO(prev_resp.content)) as im:
                assert im.format == "WEBP"

            # Deleting video unlinks both thumb and animated preview
            del_resp = client.delete(f"/api/media/{vid_id}")
            assert del_resp.status_code == 200
            assert not prev_path.exists()
            assert client.get(f"/api/media/{vid_id}/preview").status_code == 404

    # 3) Synthetic invalid file degrades gracefully in extract_media_metadata
    bad_mp4 = tmp_path / "broken.mp4"
    bad_mp4.write_bytes(b"not-a-real-video")
    assert asyncio.run(extract_media_metadata(bad_mp4, "video")) == {}

    # 4) generate_video_preview falls back to Pillow animated WebP when ffmpeg libwebp fails
    class FakeProc:
        def __init__(self, rc: int) -> None:
            self.returncode = rc

        async def communicate(self):
            return b"", b""

    async def fake_exec(*args, **kwargs):
        out_arg = str(args[-1])
        if out_arg.endswith(".webp"):
            return FakeProc(1)
        # Produce 3 PNG frames matching the pattern
        parent = Path(out_arg).parent
        for idx in (1, 2, 3):
            frame_file = parent / f"fb_preview_pframe_{idx:02d}.png"
            Image.new("RGB", (160, 90), color=(40 * idx, 100, 200)).save(frame_file, format="PNG")
        return FakeProc(0)

    monkeypatch.setattr("app.thumbnails.shutil.which", lambda _: "/usr/bin/ffmpeg")
    monkeypatch.setattr("app.thumbnails.asyncio.create_subprocess_exec", fake_exec)

    dummy_mp4 = tmp_path / "dummy.mp4"
    dummy_mp4.write_bytes(b"fake")
    preview_name = asyncio.run(
        generate_video_preview(dummy_mp4, settings.thumbs_dir, "fb_preview", duration_sec=4.0)
    )
    assert preview_name == "fb_preview_preview.webp"
    fb_prev_path = settings.thumbs_dir / preview_name
    assert fb_prev_path.is_file()
    with Image.open(fb_prev_path) as im:
        assert im.format == "WEBP"
        assert getattr(im, "n_frames", 1) == 3

    # 5) Zero-copy Telegram ingestion extracts duration_sec, width, height from event.message.file
    from app.config import TelegramApiConfig

    settings_tg = _make_settings(tmp_path / "tg_zc", storage_mode="telegram")
    settings_tg.telegram_archive_chat_id = "-100111222"
    app_tg = create_app(settings_tg)
    pool = app_tg.state.tg_pool

    async def fake_mirror(_payload: dict):
        return {"chat_id": "-100111222", "topic_id": 5, "message_id": 900, "worker_name": "bot1"}

    monkeypatch.setattr(pool, "mirror_to_archive_topic", fake_mirror)

    class FakeStatusMsg:
        async def edit(self, _text: str):
            return None

    async def fake_reply(_text: str):
        return FakeStatusMsg()

    async def fake_dl_media(**_kwargs):
        return None

    fake_cfg = TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1")
    fake_event = SimpleNamespace(
        chat_id=-100111222,
        reply=fake_reply,
        message=SimpleNamespace(
            id=888,
            message="ZeroCopy Video #action",
            reply_to=None,
            media=object(),
            download_media=fake_dl_media,
            file=SimpleNamespace(
                name="action_clip.mp4",
                mime_type="video/mp4",
                size=1024 * 1024,
                duration=95.5,
                width=1920,
                height=1080,
            ),
        ),
    )
    asyncio.run(pool._handle_telegram_media(fake_cfg, fake_event, "Action", tags=["action"]))
    listed, total = app_tg.state.db.list_media()
    assert total == 1
    assert listed[0]["duration_sec"] == 95.5
    assert listed[0]["width"] == 1920
    assert listed[0]["height"] == 1080


def test_zip_archive_explorer_local_and_tail_backed_telegram(tmp_path: Path, monkeypatch):
    import zipfile

    settings = _make_settings(tmp_path, storage_mode="both")
    app = create_app(settings)

    # Build a ZIP containing directory entries, stored text, deflated text, and an image
    zip_buf = io.BytesIO()
    stored_payload = b"StoredPayload-" + (b"0123456789ABCDEF" * 20)
    deflated_payload = b"DeflatedLine\n" * 150
    jpg_bytes = _sample_jpeg_bytes()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        zf.writestr("docs/", b"")
        zf.writestr("docs/readme.txt", stored_payload, compress_type=zipfile.ZIP_STORED)
        zf.writestr("logs/app.log", deflated_payload, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("images/cover.jpg", jpg_bytes, compress_type=zipfile.ZIP_STORED)
    zip_bytes = zip_buf.getvalue()

    with TestClient(app) as client:
        # Non-zip rejects /zip/entries with 400
        img_resp = client.post(
            "/api/ingest/upload",
            files={"file": ("not_a_zip.jpg", jpg_bytes, "image/jpeg")},
        )
        img_item = _wait_for_upload_item(client, img_resp)
        assert client.get(f"/api/media/{img_item['id']}/zip/entries").status_code == 400
        assert client.get("/api/media/does_not_exist/zip/entries").status_code == 404

        # Upload ZIP archive locally
        z_resp = client.post(
            "/api/ingest/upload",
            files={"file": ("bundle.zip", zip_bytes, "application/zip")},
        )
        z_item = _wait_for_upload_item(client, z_resp)
        zip_id = z_item["id"]

        entries_resp = client.get(f"/api/media/{zip_id}/zip/entries")
        assert entries_resp.status_code == 200
        data = entries_resp.json()
        assert data["total_entries"] == 3
        paths = [e["path"] for e in data["entries"]]
        assert "docs/" not in paths
        assert paths == ["docs/readme.txt", "logs/app.log", "images/cover.jpg"]
        assert zip_id in app.state.zip_cache

        # Stream full ZIP_STORED entry + download disposition
        r_stored = client.get(
            f"/api/media/{zip_id}/zip/stream",
            params={"path": "docs/readme.txt", "download": "true"},
        )
        assert r_stored.status_code == 200
        assert r_stored.content == stored_payload
        assert 'attachment; filename="readme.txt"' in r_stored.headers["content-disposition"]

        # Range 206 on ZIP_STORED entry
        r_stored_range = client.get(
            f"/api/media/{zip_id}/zip/stream",
            params={"path": "docs/readme.txt"},
            headers={"Range": "bytes=14-45"},
        )
        assert r_stored_range.status_code == 206
        assert r_stored_range.headers["content-range"] == f"bytes 14-45/{len(stored_payload)}"
        assert r_stored_range.content == stored_payload[14:46]

        # Full stream + Range 206 on ZIP_DEFLATED entry
        r_def = client.get(f"/api/media/{zip_id}/zip/stream", params={"path": "logs/app.log"})
        assert r_def.status_code == 200
        assert r_def.content == deflated_payload

        r_def_range = client.get(
            f"/api/media/{zip_id}/zip/stream",
            params={"path": "logs/app.log"},
            headers={"Range": "bytes=20-79"},
        )
        assert r_def_range.status_code == 206
        assert r_def_range.headers["content-range"] == f"bytes 20-79/{len(deflated_payload)}"
        assert r_def_range.content == deflated_payload[20:80]

        # Missing entry returns 404
        assert (
            client.get(f"/api/media/{zip_id}/zip/stream", params={"path": "missing.txt"}).status_code
            == 404
        )

        # Delete invalidates zip_cache
        assert client.delete(f"/api/media/{zip_id}").status_code == 200
        assert zip_id not in app.state.zip_cache

    # Telegram-backed large ZIP (>200 KB) verifies tail-only Central Directory fetch
    tg_settings = _make_settings(tmp_path / "tg_zip", storage_mode="telegram")
    tg_app = create_app(tg_settings)

    big_zip_buf = io.BytesIO()
    with zipfile.ZipFile(big_zip_buf, "w") as zf:
        zf.writestr("big.bin", b"A" * 220_000, compress_type=zipfile.ZIP_STORED)
        zf.writestr("nested/hello.txt", b"Hello from Telegram ZIP!", compress_type=zipfile.ZIP_DEFLATED)
    big_zip_bytes = big_zip_buf.getvalue()

    tg_app.state.db.insert_media(
        media_id="tg_zip_1",
        title="Telegram Zip",
        filename="tg_bundle.zip",
        folder="Archives",
        media_type="file",
        mime_type="application/zip",
        size_bytes=len(big_zip_bytes),
        drive_name="telegram",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
        telegram_chat_id="-100888",
        telegram_topic_id=12,
        telegram_msg_id=505,
        telegram_worker_name="bot1",
    )

    fetched_ranges: list[tuple[int, int]] = []

    async def fake_prepare_stream(chat_id: str, msg_id: int, worker_name: str | None = None):
        return object(), object()

    async def fake_iter_range(_client, _media, start: int, end: int):
        fetched_ranges.append((start, end))
        yield big_zip_bytes[start : end + 1]

    monkeypatch.setattr(tg_app.state.tg_pool, "prepare_stream", fake_prepare_stream)
    monkeypatch.setattr(tg_app.state.tg_pool, "iter_telegram_range", fake_iter_range)

    with TestClient(tg_app) as tg_client:
        res_entries = tg_client.get("/api/media/tg_zip_1/zip/entries")
        assert res_entries.status_code == 200
        assert [e["path"] for e in res_entries.json()["entries"]] == ["big.bin", "nested/hello.txt"]
        # Tail read only fetched the last 131072 bytes, never offset 0
        assert len(fetched_ranges) == 1
        assert fetched_ranges[0][0] == len(big_zip_bytes) - 131072
        assert fetched_ranges[0][1] == len(big_zip_bytes) - 1

        res_hello = tg_client.get(
            "/api/media/tg_zip_1/zip/stream",
            params={"path": "nested/hello.txt"},
        )
        assert res_hello.status_code == 200
        assert res_hello.content == b"Hello from Telegram ZIP!"


def test_patch_move_rename_local_and_telegram_topic_migration(tmp_path: Path, monkeypatch):
    from types import SimpleNamespace
    from app.config import TelegramApiConfig

    # 1) Local drive move & rename updates file path on disk, DB folder/title, and FTS
    settings = _make_settings(tmp_path)
    app = create_app(settings)

    with TestClient(app) as client:
        jpg = _sample_jpeg_bytes()
        resp = client.post(
            "/api/ingest/upload",
            data={"folder": "General"},
            files={"file": ("move_me.jpg", jpg, "image/jpeg")},
        )
        item = _wait_for_upload_item(client, resp)
        old_path = Path(item["file_path"])
        assert old_path.is_file()

        patch_resp = client.patch(
            f"/api/media/{item['id']}",
            json={"title": "Renamed Sunset", "folder": "Travel/Spain"},
        )
        assert patch_resp.status_code == 200
        updated = patch_resp.json()
        new_path = Path(updated["file_path"])
        assert updated["title"] == "Renamed Sunset"
        assert updated["folder"] == "Travel/Spain"
        assert new_path.is_file()
        assert not old_path.exists()
        assert new_path.parent == tmp_path / "drives" / "drive1" / "Travel" / "Spain"

        # FTS search finds new title and folder filter finds new folder
        assert client.get("/api/media?q=Renamed").json()["total"] == 1
        assert client.get("/api/media?folder=Travel/Spain").json()["total"] == 1
        assert client.get("/api/media?folder=General").json()["total"] == 0

    # 2) Multi-part Telegram item folder change migrates messages to new topic and deletes old messages
    tg_settings = _make_settings(tmp_path / "tg_move", storage_mode="telegram")
    tg_settings.telegram_archive_chat_id = "-100777"
    tg_app = create_app(tg_settings)
    pool = tg_app.state.tg_pool
    db = tg_app.state.db

    part0 = b"PART_ZERO_" * 50
    part1 = b"PART_ONE__" * 50
    total_size = len(part0) + len(part1)
    stored_msgs: dict[int, bytes] = {101: part0, 102: part1}
    sent_topics: list[tuple[int, int]] = []
    deleted_msgs: list[int] = []
    next_id = 200

    class FakeMigrateWorker:
        def __init__(self, name: str) -> None:
            self.name = name

        async def get_input_entity(self, target):
            return target

        async def get_messages(self, peer, ids: int):
            if ids not in stored_msgs:
                return None
            return SimpleNamespace(media=stored_msgs[ids])

        def iter_download(self, media: bytes, *, offset: int = 0, request_size: int = 524288):
            async def _gen():
                yield media[offset:]

            return _gen()

        async def upload_file(self, reader, *, file_size: int, file_name: str, progress_callback=None):
            buf = bytearray()
            while True:
                c = reader.read(65536)
                if not c:
                    break
                buf.extend(c)
            return SimpleNamespace(data=bytes(buf), file_name=file_name, file_size=file_size)

        async def send_file(self, peer, uploaded, *, caption: str = "", reply_to=None, **_):
            nonlocal next_id
            next_id += 1
            raw = uploaded if isinstance(uploaded, (bytes, bytearray)) else uploaded.data
            stored_msgs[next_id] = bytes(raw)
            sent_topics.append((next_id, int(reply_to)))
            return SimpleNamespace(id=next_id)

        async def delete_messages(self, peer, ids: list[int]):
            for mid in ids:
                deleted_msgs.append(int(mid))
                stored_msgs.pop(int(mid), None)
            return [True]

        async def disconnect(self):
            return None

    worker = FakeMigrateWorker("bot1")
    pool.clients = [(TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), worker)]
    db.save_topic_id("-100777", "General", 1)
    db.save_topic_id("-100777", "Movies", 42)

    db.insert_media(
        media_id="tg_mp_move",
        title="Original Movie",
        filename="movie.bin",
        folder="General",
        media_type="file",
        mime_type="application/octet-stream",
        size_bytes=total_size,
        drive_name="telegram",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
        telegram_chat_id="-100777",
        telegram_topic_id=1,
        telegram_msg_id=101,
        telegram_worker_name="bot1",
        part_count=2,
        parts=[
            {
                "part_index": 0,
                "start_byte": 0,
                "end_byte": len(part0) - 1,
                "size_bytes": len(part0),
                "telegram_chat_id": "-100777",
                "telegram_topic_id": 1,
                "telegram_msg_id": 101,
                "telegram_worker_name": "bot1",
            },
            {
                "part_index": 1,
                "start_byte": len(part0),
                "end_byte": total_size - 1,
                "size_bytes": len(part1),
                "telegram_chat_id": "-100777",
                "telegram_topic_id": 1,
                "telegram_msg_id": 102,
                "telegram_worker_name": "bot1",
            },
        ],
    )

    with TestClient(tg_app) as tg_client:
        res = tg_client.patch(
            "/api/media/tg_mp_move",
            json={"title": "Migrated Movie", "folder": "Movies"},
        )
        assert res.status_code == 200
        moved = res.json()
        assert moved["title"] == "Migrated Movie"
        assert moved["folder"] == "Movies"
        assert moved["telegram_topic_id"] == 42
        assert moved["telegram_msg_id"] == 201
        assert [p["telegram_msg_id"] for p in moved["parts"]] == [201, 202]
        assert all(tid == 42 for _, tid in sent_topics)
        assert set(deleted_msgs) == {101, 102}

        # Streaming still returns intact payload from new topic messages
        streamed = tg_client.get("/api/media/tg_mp_move/stream")
        assert streamed.status_code == 200
        assert streamed.content == part0 + part1


def test_pagination_bulk_actions_and_2a_frontend_hooks(tmp_path: Path):
    import zipfile

    settings = _make_settings(tmp_path)
    app = create_app(settings)

    with TestClient(app) as client:
        # UI HTML exposes all 2.A hooks
        html = client.get("/").text
        for hook in (
            "badge-tr",
            "mouseenter",
            "renderZipExplorer",
            "modalEditMetaBtn",
            "editMetaModal",
            "selectModeBtn",
            "bulkToolbar",
            "loadMoreWrap",
            "IntersectionObserver",
        ):
            assert hook in html

        jpg = _sample_jpeg_bytes()
        created: list[dict] = []
        for idx in range(5):
            # Use same filename for first two to verify bulk ZIP duplicate filename handling
            fname = "dup_photo.jpg" if idx < 2 else f"photo_{idx}.jpg"
            r = client.post(
                "/api/ingest/upload",
                data={"folder": "General", "tags": "#init"},
                files={"file": (fname, jpg, "image/jpeg")},
            )
            created.append(_wait_for_upload_item(client, r))

        # Pagination limit & offset
        p1 = client.get("/api/media?limit=2&offset=0").json()
        p2 = client.get("/api/media?limit=2&offset=2").json()
        p3 = client.get("/api/media?limit=2&offset=4").json()
        assert p1["total"] == p2["total"] == p3["total"] == 5
        assert len(p1["items"]) == 2
        assert len(p2["items"]) == 2
        assert len(p3["items"]) == 1
        all_paged_ids = [x["id"] for page in (p1, p2, p3) for x in page["items"]]
        assert len(set(all_paged_ids)) == 5

        target_ids = [created[0]["id"], created[1]["id"], created[2]["id"]]

        # Bulk Move
        mv_resp = client.post(
            "/api/media/bulk/move",
            json={"media_ids": target_ids, "folder": "BulkFolder"},
        )
        assert mv_resp.status_code == 200
        assert mv_resp.json()["moved"] == 3
        for it in mv_resp.json()["items"]:
            assert it["folder"] == "BulkFolder"
            assert Path(it["file_path"]).is_file()
            assert Path(it["file_path"]).parent.name == "BulkFolder"

        # Bulk Tags: add, remove, replace
        add_resp = client.post(
            "/api/media/bulk/tags",
            json={"media_ids": target_ids, "tags": "#vip, #4k", "mode": "add"},
        )
        assert add_resp.status_code == 200
        for it in add_resp.json()["items"]:
            assert set(it["tags"]) == {"init", "vip", "4k"}

        rem_resp = client.post(
            "/api/media/bulk/tags",
            json={"media_ids": target_ids, "tags": "#init", "mode": "remove"},
        )
        assert rem_resp.status_code == 200
        for it in rem_resp.json()["items"]:
            assert set(it["tags"]) == {"vip", "4k"}

        rep_resp = client.post(
            "/api/media/bulk/tags",
            json={"media_ids": target_ids, "tags": "#final", "mode": "replace"},
        )
        assert rep_resp.status_code == 200
        for it in rep_resp.json()["items"]:
            assert it["tags"] == ["final"]

        # Bulk Download (GET & POST) produces valid media_bundle.zip with deduped filenames
        dl_get = client.get("/api/media/bulk/download", params={"ids": ",".join(target_ids)})
        assert dl_get.status_code == 200
        assert dl_get.headers["content-type"].startswith("application/zip")
        assert 'filename="media_bundle.zip"' in dl_get.headers["content-disposition"]
        with zipfile.ZipFile(io.BytesIO(dl_get.content), "r") as zf:
            names = zf.namelist()
            assert len(names) == 3
            assert len(set(names)) == 3
            for name in names:
                assert zf.read(name) == jpg

        dl_post = client.post("/api/media/bulk/download", json={"media_ids": target_ids})
        assert dl_post.status_code == 200
        with zipfile.ZipFile(io.BytesIO(dl_post.content), "r") as zf:
            assert len(zf.namelist()) == 3

        # Bulk Delete cleans files, thumbnails, and DB rows while keeping untouched items
        paths_before_del = [Path(client.get(f"/api/media/{mid}").json()["file_path"]) for mid in target_ids]
        del_resp = client.post("/api/media/bulk/delete", json={"media_ids": target_ids})
        assert del_resp.status_code == 200
        assert del_resp.json()["deleted"] == 3
        for p in paths_before_del:
            assert not p.exists()
        assert client.get("/api/media").json()["total"] == 2



