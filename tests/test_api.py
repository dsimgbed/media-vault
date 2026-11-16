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





