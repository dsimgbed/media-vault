from __future__ import annotations

import io
import shutil
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient
from PIL import Image

from app.main import create_app
from tests.helpers import (
    _insert_tagged_media,
    _make_sample_mp4,
    _make_settings,
    _sample_jpeg_bytes,
    _ui_source,
    _wait_for_upload_item,
)


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
        assert tags_res["tags"] == [
            {"name": "hd", "count": 1},
            {"name": "soap", "count": 1},
            {"name": "soapy", "count": 1},
        ]

        filtered = client.get("/api/media?tag=soap").json()
        assert filtered["total"] == 1
        assert filtered["items"][0]["id"] == item1["id"]

        patched = client.patch(f"/api/media/{item1['id']}/tags", json={"tags": "#soap #archive"}).json()
        assert patched["tags"] == ["soap", "archive"]

        html = _ui_source(client)
        assert "tagsInput" in html
        assert "tagFilter" in html
        assert "modalEditTagsBtn" in html


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
                assert getattr(im, "n_frames", 1) == 10

            # Deleting video unlinks both thumb and animated preview
            del_resp = client.delete(f"/api/media/{vid_id}")
            assert del_resp.status_code == 200
            assert not prev_path.exists()
            assert client.get(f"/api/media/{vid_id}/preview").status_code == 404

    # 2b) Multi-segment video (red -> green -> blue over 12s) produces 10 screenshots spanning full duration
    from app.thumbnails import PREVIEW_FRAME_COUNT, _compute_preview_timestamps

    assert _compute_preview_timestamps(100.0) == [5.0, 15.0, 25.0, 35.0, 45.0, 55.0, 65.0, 75.0, 85.0, 95.0]
    assert _compute_preview_timestamps(None) == []
    assert _compute_preview_timestamps(0.0) == []

    ffmpeg_bin = shutil.which("ffmpeg")
    if ffmpeg_bin:
        multi_mp4 = tmp_path / "multi_segment.mp4"
        proc = subprocess.run(
            [
                ffmpeg_bin,
                "-y",
                "-f", "lavfi", "-i", "color=c=red:size=320x180:duration=4:rate=10",
                "-f", "lavfi", "-i", "color=c=green:size=320x180:duration=4:rate=10",
                "-f", "lavfi", "-i", "color=c=blue:size=320x180:duration=4:rate=10",
                "-filter_complex", "[0:v][1:v][2:v]concat=n=3:v=1:a=0[bg];testsrc2=size=64x36:duration=12:rate=10[pip];[bg][pip]overlay=10:10[v]",
                "-map", "[v]",
                "-c:v", "libx264",
                "-pix_fmt", "yuv420p",
                str(multi_mp4),
            ],
            capture_output=True,
            check=False,
        )
        if proc.returncode == 0 and multi_mp4.is_file():
            multi_prev = asyncio.run(
                generate_video_preview(multi_mp4, settings.thumbs_dir, "multi_seg", duration_sec=12.0)
            )
            assert multi_prev == "multi_seg_preview.webp"
            multi_path = settings.thumbs_dir / multi_prev
            assert multi_path.is_file()
            with Image.open(multi_path) as im:
                assert im.format == "WEBP"
                assert getattr(im, "n_frames", 1) == PREVIEW_FRAME_COUNT
                # Sample center pixel at frame 0 (red ~0.6s), frame 4 (green ~5.4s), frame 9 (blue ~11.4s)
                im.seek(0)
                r0, g0, b0 = im.convert("RGB").getpixel((160, 90))
                im.seek(4)
                r4, g4, b4 = im.convert("RGB").getpixel((160, 90))
                im.seek(9)
                r9, g9, b9 = im.convert("RGB").getpixel((160, 90))
                assert r0 > 180 and g0 < 60 and b0 < 60
                assert g4 > 100 and r4 < 60 and b4 < 60
                assert b9 > 180 and r9 < 60 and g9 < 60
            multi_path.unlink(missing_ok=True)

    # 3) Synthetic invalid file degrades gracefully in extract_media_metadata
    bad_mp4 = tmp_path / "broken.mp4"
    bad_mp4.write_bytes(b"not-a-real-video")
    assert asyncio.run(extract_media_metadata(bad_mp4, "video")) == {}

    # 4) generate_video_preview samples timestamps via ffmpeg seeks and falls back when duration is unknown
    class FakeProc:
        def __init__(self, rc: int) -> None:
            self.returncode = rc

        async def communicate(self):
            return b"", b""

    seek_timestamps: list[str] = []

    async def fake_exec_seeks(*args, **kwargs):
        args_list = [str(a) for a in args]
        out_arg = args_list[-1]
        if "-ss" in args_list:
            ss_idx = args_list.index("-ss")
            seek_timestamps.append(args_list[ss_idx + 1])
            idx = len(seek_timestamps)
            Image.new("RGB", (160, 90), color=(20 * idx, 100, 200)).save(out_arg, format="PNG")
            return FakeProc(0)
        return FakeProc(1)

    monkeypatch.setattr("app.thumbnails.shutil.which", lambda _: "/usr/bin/ffmpeg")
    monkeypatch.setattr("app.thumbnails.asyncio.create_subprocess_exec", fake_exec_seeks)

    dummy_mp4 = tmp_path / "dummy.mp4"
    dummy_mp4.write_bytes(b"fake")
    preview_name = asyncio.run(
        generate_video_preview(dummy_mp4, settings.thumbs_dir, "seek_preview", duration_sec=20.0)
    )
    assert preview_name == "seek_preview_preview.webp"
    assert len(seek_timestamps) == 10
    assert sorted(float(t) for t in seek_timestamps) == [1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 13.0, 15.0, 17.0, 19.0]
    seek_prev_path = settings.thumbs_dir / preview_name
    assert seek_prev_path.is_file()
    with Image.open(seek_prev_path) as im:
        assert im.format == "WEBP"
        assert getattr(im, "n_frames", 1) == 10

    # Fallback single-pass PNG extraction when duration cannot be probed
    async def fake_meta(_src, _mtype):
        return {}

    async def fake_exec_fallback(*args, **kwargs):
        out_arg = str(args[-1])
        parent = Path(out_arg).parent
        for idx in (1, 2, 3):
            frame_file = parent / f"fb_preview_pframe_{idx:02d}.png"
            Image.new("RGB", (160, 90), color=(40 * idx, 100, 200)).save(frame_file, format="PNG")
        return FakeProc(0)

    monkeypatch.setattr("app.thumbnails.extract_media_metadata", fake_meta)
    monkeypatch.setattr("app.thumbnails.asyncio.create_subprocess_exec", fake_exec_fallback)

    fb_preview_name = asyncio.run(
        generate_video_preview(dummy_mp4, settings.thumbs_dir, "fb_preview", duration_sec=None)
    )
    assert fb_preview_name == "fb_preview_preview.webp"
    fb_prev_path = settings.thumbs_dir / fb_preview_name
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
        html = _ui_source(client)
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
                data={"folder": "General", "tags": "#init", "force": "true"},
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


def test_tag_counts_frequency_sorted_query_limit_and_auth(tmp_path: Path):
    settings = _make_settings(tmp_path, auth_token="tag-secret")
    app = create_app(settings)
    db = app.state.db

    _insert_tagged_media(db, "m1", "#soap #hd a_b")
    _insert_tagged_media(db, "m2", "#soap #4k axb")
    _insert_tagged_media(db, "m3", "#soap #hd #soapy")

    # Backwards-compatible name list is unchanged (alphabetical names)
    assert db.list_tags() == ["4k", "a_b", "axb", "hd", "soap", "soapy"]

    # Frequency-sorted counts, ties broken alphabetically
    assert db.list_tag_counts() == [
        {"name": "soap", "count": 3},
        {"name": "hd", "count": 2},
        {"name": "4k", "count": 1},
        {"name": "a_b", "count": 1},
        {"name": "axb", "count": 1},
        {"name": "soapy", "count": 1},
    ]
    # q is normalized like tags (#, case) and matched as a substring
    assert [t["name"] for t in db.list_tag_counts(q="#SOA")] == ["soap", "soapy"]
    assert [t["name"] for t in db.list_tag_counts(q="oap")] == ["soap", "soapy"]
    # LIKE wildcards in q are escaped: "_" only matches a literal underscore
    assert [t["name"] for t in db.list_tag_counts(q="a_b")] == ["a_b"]
    # q that normalizes to nothing means "no filter"
    assert len(db.list_tag_counts(q="%%")) == 6
    assert db.list_tag_counts(limit=2) == [
        {"name": "soap", "count": 3},
        {"name": "hd", "count": 2},
    ]

    with TestClient(app) as client:
        assert client.get("/api/tags").status_code == 401
        auth = {"Authorization": "Bearer tag-secret"}
        body = client.get("/api/tags", headers=auth).json()
        assert body["tags"][0] == {"name": "soap", "count": 3}
        assert [t["name"] for t in body["tags"]] == ["soap", "hd", "4k", "a_b", "axb", "soapy"]

        q_body = client.get("/api/tags?q=soa&limit=1&token=tag-secret").json()
        assert q_body["tags"] == [{"name": "soap", "count": 3}]
        assert client.get("/api/tags?limit=0", headers=auth).status_code == 422

        # Counts track edits and deletes
        client.patch("/api/media/m3/tags", headers=auth, json={"tags": ["hd"]})
        db.delete_media("m2")
        after = client.get("/api/tags", headers=auth).json()["tags"]
        assert after == [
            {"name": "hd", "count": 2},
            {"name": "a_b", "count": 1},
            {"name": "soap", "count": 1},
        ]


def test_folder_case_insensitive_filter_and_deduplicated_listing(tmp_path: Path):
    settings = _make_settings(tmp_path)
    app = create_app(settings)
    db = app.state.db

    for mid, folder in (
        ("f1", "Futa"),
        ("f2", "Futa"),
        ("l1", "Lesbian"),
        ("l2", "lesbian"),
    ):
        db.insert_media(
            media_id=mid,
            title=mid,
            filename=f"{mid}.jpg",
            folder=folder,
            media_type="image",
            mime_type="image/jpeg",
            size_bytes=123,
            drive_name="telegram",
            file_path="",
            thumb_filename=None,
            source_origin="web",
            source_kind="upload",
            source_url=None,
        )

    db.save_topic_id("-100111222", "Futa", 15)
    db.save_topic_id("-100111222", "Anime", 22)

    with TestClient(app) as client:
        folders = client.get("/api/folders").json()["folders"]
        # Deduplicated case-insensitively, preferring cased media_items folder names
        assert folders == ["General", "anime", "Futa", "Lesbian"]

        # Lowercase query matches cased items and vice versa
        futa_lower = client.get("/api/media?folder=futa").json()
        assert {it["id"] for it in futa_lower["items"]} == {"f1", "f2"}

        lesbian_cased = client.get("/api/media?folder=Lesbian").json()
        assert {it["id"] for it in lesbian_cased["items"]} == {"l1", "l2"}
