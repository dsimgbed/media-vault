from __future__ import annotations

import io
import json
import time
from pathlib import Path

from fastapi.testclient import TestClient
from PIL import Image

from app.config import Settings

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


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


def _wait_for_job(client: TestClient, job_id: str, *, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    job = None
    while time.monotonic() < deadline:
        resp = client.get(f"/api/jobs/{job_id}")
        assert resp.status_code == 200
        job = resp.json()
        if job["status"] in ("done", "failed", "cancelled"):
            break
        time.sleep(0.01)
    assert job is not None
    job["media_ids"] = json.loads(job.get("media_ids_json") or "[]")
    return job


def _static_asset_paths(*suffixes: str) -> list[Path]:
    exts = suffixes or (".js", ".css")
    return sorted(p for p in STATIC_DIR.rglob("*") if p.is_file() and p.suffix in exts)


def _ui_source(client: TestClient) -> str:
    """Return the full shipped UI source: the `/` shell plus every JS/CSS asset,
    each fetched over HTTP through the `/static` mount (so serving is verified too)."""
    index = client.get("/")
    assert index.status_code == 200
    chunks = [index.text]
    for path in _static_asset_paths(".js", ".css"):
        rel = path.relative_to(STATIC_DIR).as_posix()
        res = client.get(f"/static/{rel}")
        assert res.status_code == 200, rel
        chunks.append(f"\n/* ---- {rel} ---- */\n{res.text}")
    return "\n".join(chunks)


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


def _insert_tagged_media(db, media_id: str, tags: str) -> None:
    db.insert_media(
        media_id=media_id,
        title=f"Item {media_id}",
        filename=f"{media_id}.jpg",
        folder="Archive",
        media_type="image",
        mime_type="image/jpeg",
        size_bytes=100,
        drive_name="drive1",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
        tags=tags,
    )
