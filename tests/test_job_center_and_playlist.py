from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app
from tests.helpers import STATIC_DIR, _make_settings, _static_asset_paths, _wait_for_job


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_retained_failed_staged_upload_and_retry_chunked_and_legacy(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="telegram")
    app = create_app(settings)
    db = app.state.db

    should_fail = True
    msg_seq = 0

    async def flakey_mirror(payload: dict) -> dict:
        nonlocal msg_seq
        if should_fail:
            raise RuntimeError("Simulated Telegram upload failure")
        msg_seq += 1
        return {
            "chat_id": "-100999",
            "topic_id": 5,
            "message_id": 500 + msg_seq,
            "worker_name": "bot1",
        }

    app.state.ingest.mirror_hook = flakey_mirror

    chunked_bytes = b"chunked-retry-payload-" * 200
    legacy_bytes = b"legacy-retry-payload-" * 200

    with TestClient(app) as client:
        # 1) Chunked upload fails on Telegram mirror -> staged file retained
        s_res = client.post(
            "/api/uploads/sessions",
            json={
                "filename": "chunked_retry.bin",
                "folder": "RetryFolder",
                "total_size": len(chunked_bytes),
            },
        )
        assert s_res.status_code == 201
        sid = s_res.json()["session"]["id"]
        assert client.put(f"/api/uploads/sessions/{sid}/chunks/0", content=chunked_bytes).status_code == 200
        comp = client.post(
            f"/api/uploads/sessions/{sid}/complete",
            json={"sha256": _sha256_hex(chunked_bytes)},
        )
        assert comp.status_code == 202
        chunked_job_id = comp.json()["job_id"]

        failed_chunked = _wait_for_job(client, chunked_job_id)
        assert failed_chunked["status"] == "failed"
        assert failed_chunked["retryable"] is True
        assert "staged_path" not in failed_chunked
        assert failed_chunked["staged_expires_at"]

        chunked_db_row = db.get_job(chunked_job_id)
        assert chunked_db_row is not None
        assert chunked_db_row["staged_path"]
        chunked_staged_file = Path(chunked_db_row["staged_path"])
        assert chunked_staged_file.is_file()
        assert chunked_staged_file.read_bytes() == chunked_bytes

        # 2) Legacy /api/ingest/upload fails on Telegram mirror -> staged file also retained
        leg_res = client.post(
            "/api/ingest/upload",
            data={"folder": "RetryFolder"},
            files={"file": ("legacy_retry.bin", legacy_bytes, "application/octet-stream")},
        )
        assert leg_res.status_code == 202
        legacy_job_id = leg_res.json()["job_id"]

        failed_legacy = _wait_for_job(client, legacy_job_id)
        assert failed_legacy["status"] == "failed"
        assert failed_legacy["retryable"] is True
        assert "staged_path" not in failed_legacy
        assert failed_legacy["staged_expires_at"]

        legacy_db_row = db.get_job(legacy_job_id)
        assert legacy_db_row is not None
        assert legacy_db_row["staged_path"]
        legacy_staged_file = Path(legacy_db_row["staged_path"])
        assert legacy_staged_file.is_file()
        assert legacy_staged_file.read_bytes() == legacy_bytes

        # 3) Restore Telegram mirror and retry both jobs -> both succeed and delete staged files
        should_fail = False

        r_c = client.post(f"/api/jobs/{chunked_job_id}/retry")
        assert r_c.status_code == 202, r_c.text
        done_c = _wait_for_job(client, chunked_job_id)
        assert done_c["status"] == "done", done_c
        assert len(done_c["media_ids"]) == 1
        assert not chunked_staged_file.exists()
        assert not chunked_staged_file.parent.exists()
        c_media = db.get_media(done_c["media_ids"][0])
        assert c_media is not None
        assert c_media["sha256"] == _sha256_hex(chunked_bytes)
        assert c_media["telegram_msg_id"] == 501

        r_l = client.post(f"/api/jobs/{legacy_job_id}/retry")
        assert r_l.status_code == 202, r_l.text
        done_l = _wait_for_job(client, legacy_job_id)
        assert done_l["status"] == "done", done_l
        assert len(done_l["media_ids"]) == 1
        assert not legacy_staged_file.exists()
        assert not legacy_staged_file.parent.exists()
        l_media = db.get_media(done_l["media_ids"][0])
        assert l_media is not None
        assert l_media["sha256"] == _sha256_hex(legacy_bytes)
        assert l_media["telegram_msg_id"] == 502


def test_failed_upload_ttl_expiry_gc_and_lru_retention_budget(tmp_path: Path):
    # A) Failed upload TTL expiry via retry (409 + cleanup) and run_gc()
    settings = _make_settings(tmp_path / "ttl", storage_mode="telegram")
    app = create_app(settings)
    db = app.state.db

    async def failing_mirror(_payload: dict) -> dict:
        raise RuntimeError("Telegram unavailable")

    app.state.ingest.mirror_hook = failing_mirror

    with TestClient(app) as client:
        r1 = client.post(
            "/api/ingest/upload",
            files={"file": ("exp1.bin", b"expire-me-1" * 50, "application/octet-stream")},
        )
        r2 = client.post(
            "/api/ingest/upload",
            files={"file": ("exp2.bin", b"expire-me-2" * 50, "application/octet-stream")},
        )
        j1_id = r1.json()["job_id"]
        j2_id = r2.json()["job_id"]
        assert _wait_for_job(client, j1_id)["status"] == "failed"
        assert _wait_for_job(client, j2_id)["status"] == "failed"

        p1 = Path(db.get_job(j1_id)["staged_path"])
        p2 = Path(db.get_job(j2_id)["staged_path"])
        assert p1.is_file() and p2.is_file()

        past_iso = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        with db._connect() as conn:
            conn.execute(
                "UPDATE ingest_jobs SET staged_expires_at = ? WHERE id IN (?, ?)",
                (past_iso, j1_id, j2_id),
            )

        j1_view = client.get(f"/api/jobs/{j1_id}").json()
        assert j1_view["retryable"] is False

        retry_expired = client.post(f"/api/jobs/{j1_id}/retry")
        assert retry_expired.status_code == 409
        assert not p1.exists()
        assert not p1.parent.exists()

        # j2 is cleaned by run_gc()
        gc_res = asyncio.run(app.state.uploads.run_gc())
        assert gc_res["expired_failed_uploads"] >= 1
        assert not p2.exists()
        assert not p2.parent.exists()
        assert db.get_job(j2_id)["staged_path"] is None

    # B) LRU eviction under failed_upload_max_retained_mb=1 (1 MiB budget)
    lru_settings = _make_settings(tmp_path / "lru", storage_mode="telegram")
    lru_settings.failed_upload_max_retained_mb = 1
    lru_app = create_app(lru_settings)
    lru_db = lru_app.state.db
    lru_app.state.ingest.mirror_hook = failing_mirror

    chunk_payload = b"Z" * 450_000  # 3 * 450,000 = 1,350,000 > 1,048,576 (1 MiB)
    with TestClient(lru_app) as lru_client:
        job_ids: list[str] = []
        staged_paths: list[Path] = []
        for idx in range(3):
            resp = lru_client.post(
                "/api/ingest/upload",
                files={"file": (f"lru_{idx}.bin", chunk_payload, "application/octet-stream")},
            )
            assert resp.status_code == 202
            jid = resp.json()["job_id"]
            assert _wait_for_job(lru_client, jid)["status"] == "failed"
            job_ids.append(jid)
            row = lru_db.get_job(jid)
            staged_paths.append(Path(row["staged_path"]) if row and row.get("staged_path") else Path())

        # Oldest (index 0) must have been evicted; newest (index 1, 2) must be kept
        j0 = lru_client.get(f"/api/jobs/{job_ids[0]}").json()
        j1 = lru_client.get(f"/api/jobs/{job_ids[1]}").json()
        j2 = lru_client.get(f"/api/jobs/{job_ids[2]}").json()
        assert j0["retryable"] is False
        assert not staged_paths[0].exists()
        assert j1["retryable"] is True
        assert staged_paths[1].is_file()
        assert j2["retryable"] is True
        assert staged_paths[2].is_file()


def test_smart_job_center_apis_sse_counts_and_health_summary(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="drives")
    app = create_app(settings)
    db = app.state.db

    with TestClient(app) as client:
        # Health & System Status fields for Track #2
        health = client.get("/api/health").json()
        assert health["upload_chunk_size_mb"] == settings.upload_chunk_size_mb
        assert health["failed_upload_retention_sec"] == settings.failed_upload_retention_sec

        sys_status = client.get("/api/system/status").json()
        assert "uploads" in sys_status
        assert sys_status["uploads"]["chunk_size_bytes"] == settings.upload_chunk_size_bytes
        assert sys_status["uploads"]["failed_upload_retention_sec"] == settings.failed_upload_retention_sec

        # Create jobs across done, failed (1 retryable URL job + 1 non-retryable upload job), cancelled, active
        done_ids: list[str] = []
        for i in range(3):
            jid = db.create_job("web", "url", f"https://example.com/done_{i}")
            db.update_job(jid, status="done", progress=100.0, message="Done")
            done_ids.append(jid)

        cancelled_id = db.create_job("web", "url", "https://example.com/cancelled_1")
        db.update_job(cancelled_id, status="cancelled", message="Cancelled by user")

        # Retained failed upload job (retryable)
        retained_dir = settings.staging_dir / "up_jc_retained"
        retained_dir.mkdir(parents=True, exist_ok=True)
        retained_file = retained_dir / "kept.bin"
        retained_file.write_bytes(b"kept-bytes")
        future_iso = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        failed_retryable_id = db.create_job(
            "web",
            "upload",
            "kept.bin",
            params={"staged_path": str(retained_file.resolve()), "folder": "Inbox"},
        )
        db.update_job(
            failed_retryable_id,
            status="failed",
            message="Temporary failure",
            staged_path=str(retained_file.resolve()),
            staged_expires_at=future_iso,
            staged_size_bytes=retained_file.stat().st_size,
        )

        # Non-retryable failed upload job (no staged file)
        failed_nonretryable_id = db.create_job("web", "upload", "lost.bin")
        db.update_job(failed_nonretryable_id, status="failed", message="Staging lost")

        # Active job (must survive clear-completed)
        active_id = db.create_job("web", "url", "https://example.com/active_1")
        db.update_job(active_id, status="downloading", progress=25.0, message="Downloading...")

        # 1) Pagination & status filter on GET /api/jobs
        page1 = client.get("/api/jobs?status=done&limit=2&offset=0")
        assert page1.status_code == 200
        p1 = page1.json()
        assert len(p1["jobs"]) == 2
        assert p1["total"] == 3
        assert p1["has_more"] is True
        assert p1["counts"] == {
            "all": 7,
            "active": 1,
            "done": 3,
            "failed": 2,
            "cancelled": 1,
        }

        page2 = client.get("/api/jobs?status=done&limit=2&offset=2")
        assert page2.status_code == 200
        p2 = page2.json()
        assert len(p2["jobs"]) == 1
        assert p2["total"] == 3
        assert p2["has_more"] is False
        assert {j["id"] for j in p1["jobs"] + p2["jobs"]} == set(done_ids)

        # Invalid status filter -> 400
        assert client.get("/api/jobs?status=invalid").status_code == 400

        # 2) SSE /api/events includes counts on connected & live job_update
        connected_ev = threading.Event()

        def _emit_live_job():
            assert connected_ev.wait(timeout=2.0)
            live_id = db.create_job("web", "url", "https://example.com/sse_counts")
            db.update_job(live_id, status="done", progress=100.0, message="SSE done")

        emitter = threading.Thread(target=_emit_live_job, daemon=True)
        emitter.start()

        sse_events: list[dict] = []
        with client.stream("GET", "/api/events") as resp:
            assert resp.status_code == 200
            for line in resp.iter_lines():
                if line.startswith("data: "):
                    ev = json.loads(line[6:])
                    sse_events.append(ev)
                    if ev.get("type") == "connected":
                        connected_ev.set()
                    if ev.get("type") == "job_update" and ev.get("status") == "done":
                        break
        emitter.join(timeout=2.0)

        assert sse_events[0]["type"] == "connected"
        assert sse_events[0]["counts"]["done"] == 3
        live_done_ev = next(
            e for e in sse_events if e.get("type") == "job_update" and e.get("status") == "done"
        )
        assert live_done_ev["counts"]["done"] == 4

        # 3) POST /api/jobs/retry-failed retries retryable and skips non-retryable
        rf_res = client.post("/api/jobs/retry-failed", json={"force": False})
        assert rf_res.status_code == 202, rf_res.text
        rf_body = rf_res.json()
        assert rf_body["retried_count"] == 1
        assert rf_body["retried_ids"] == [failed_retryable_id]
        assert rf_body["skipped_count"] == 1
        assert rf_body["skipped"][0]["job_id"] == failed_nonretryable_id
        assert _wait_for_job(client, failed_retryable_id)["status"] == "done"

        # 4) POST /api/jobs/clear-completed deletes done & cancelled, never active
        clr_res = client.post("/api/jobs/clear-completed", json={"statuses": ["done", "cancelled"]})
        assert clr_res.status_code == 200
        assert clr_res.json()["deleted"] == 6  # 3 original done + 1 SSE done + 1 retried done + 1 cancelled
        assert db.get_job(active_id) is not None
        assert db.get_job(failed_nonretryable_id) is not None

        # 5) Explicit clear of failed jobs also deletes retained staging files on disk
        r_dir2 = settings.staging_dir / "up_jc_clear_failed"
        r_dir2.mkdir(parents=True, exist_ok=True)
        r_file2 = r_dir2 / "clearme.bin"
        r_file2.write_bytes(b"delete-me-on-clear")
        failed_with_file = db.create_job("web", "upload", "clearme.bin")
        db.update_job(
            failed_with_file,
            status="failed",
            message="Failed",
            staged_path=str(r_file2.resolve()),
            staged_expires_at=future_iso,
            staged_size_bytes=r_file2.stat().st_size,
        )
        assert r_file2.is_file()

        clr_failed = client.post("/api/jobs/clear-completed", json={"statuses": ["failed"]})
        assert clr_failed.status_code == 200
        assert clr_failed.json()["deleted"] == 2
        assert clr_failed.json()["cleaned_staging"] == 1
        assert not r_file2.exists()
        assert not r_dir2.exists()
        assert db.get_job(active_id) is not None


def test_playlist_preview_cherry_pick_dedup_enrichment(tmp_path: Path, monkeypatch):
    settings = _make_settings(tmp_path, storage_mode="drives")
    app = create_app(settings)
    db = app.state.db

    # Seed existing media item with tracking params on YouTube URL in "Courses"
    existing = db.insert_media(
        media_id="pl_existing_1",
        title="Lecture 1",
        filename="lecture_1.mp4",
        folder="Courses",
        media_type="video",
        mime_type="video/mp4",
        size_bytes=12345,
        drive_name="drive1",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="ytdlp",
        source_url="https://www.youtube.com/watch?v=vidOne111&si=tracking99",
    )

    async def fake_expand_playlist(url: str, **_kwargs):
        return {
            "is_playlist": True,
            "playlist_title": "Systems Engineering 101",
            "webpage_url": url,
            "entries": [
                {
                    "url": "https://youtu.be/vidOne111?t=30",
                    "title": "Lecture 1",
                    "id": "vidOne111",
                    "duration": 600,
                },
                {
                    "url": "https://www.youtube.com/watch?v=vidTwo222",
                    "title": "Lecture 2",
                    "id": "vidTwo222",
                    "duration": 720,
                },
                {
                    "url": "https://www.youtube.com/watch?v=vidThree333",
                    "title": "Lecture 3",
                    "id": "vidThree333",
                    "duration": 840,
                },
            ],
        }

    monkeypatch.setattr("app.routers.system.expand_ytdlp_playlist", fake_expand_playlist)

    with TestClient(app) as client:
        res = client.post(
            "/api/ytdlp/playlist",
            json={"url": "https://youtube.com/playlist?list=PL_COURSES", "max_items": 50},
        )
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["is_playlist"] is True
        assert body["duplicate_count"] == 1
        assert body["suggested_folder"] == "Systems Engineering 101"
        assert len(body["entries"]) == 3

        e0, e1, e2 = body["entries"]
        assert e0["normalized_url"] == "https://youtube.com/watch?v=vidOne111"
        assert e0["already_in_vault"] is True
        assert e0["existing_media_id"] == existing["id"]
        assert e0["existing_folder"] == "Courses"

        for new_entry, vid in ((e1, "vidTwo222"), (e2, "vidThree333")):
            assert new_entry["normalized_url"] == f"https://youtube.com/watch?v={vid}"
            assert new_entry["already_in_vault"] is False
            assert new_entry["existing_media_id"] is None
            assert new_entry["existing_folder"] is None


def test_track2_frontend_guardrails_static_modules_and_no_native_dialogs(tmp_path: Path):
    settings = _make_settings(tmp_path)
    app = create_app(settings)

    index_file = STATIC_DIR / "index.html"
    assert index_file.is_file()
    index_text = index_file.read_text(encoding="utf-8")
    assert len(index_text.splitlines()) < 350
    for elem_id in ("inspectPlaylistBtn", "cancelFolderUploadBtn", "uploadQueue"):
        assert f'id="{elem_id}"' in index_text

    # Zero window.alert/confirm/prompt across all static/js/**/*.js
    js_files = _static_asset_paths(".js")
    assert len(js_files) > 0
    for js_path in js_files:
        content = js_path.read_text(encoding="utf-8")
        for forbidden in ("window.alert(", "window.confirm(", "window.prompt("):
            assert forbidden not in content, f"{forbidden} found in {js_path}"

    # Track #2 static modules served with 200 and pass `node --input-type=module --check`
    track2_modules = (
        "/static/js/crypto/sha256.js",
        "/static/js/features/upload-queue.js",
        "/static/js/features/playlist-picker.js",
    )
    with TestClient(app) as client:
        for mod_url in track2_modules:
            res = client.get(mod_url)
            assert res.status_code == 200, mod_url
            assert "javascript" in res.headers.get("content-type", "")
            check = subprocess.run(
                ["node", "--input-type=module", "--check"],
                input=res.text,
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert check.returncode == 0, f"Syntax check failed for {mod_url}:\n{check.stderr}"


def test_track2_hardening_staged_path_redaction_recovery_ttl_and_batch_urls(tmp_path: Path):
    settings = _make_settings(tmp_path / "hardening", storage_mode="telegram")
    app = create_app(settings)
    db = app.state.db

    # 1) Create a staged upload job with staged_path in both column & params_json
    job_dir = settings.staging_dir / "up_hardening"
    job_dir.mkdir(parents=True, exist_ok=True)
    staged_file = job_dir / "secret_clip.bin"
    staged_file.write_bytes(b"hardening-bytes")

    job_id = db.create_job(
        "web",
        "upload",
        "secret_clip.bin",
        params={"folder": "General", "staged_path": str(staged_file)},
    )
    db.update_job(
        job_id,
        status="allocating",
        progress=60.0,
        message="Uploading to Telegram...",
        staged_path=str(staged_file),
        staged_size_bytes=staged_file.stat().st_size,
    )

    # 2) Startup crash recovery must mark it failed AND assign a bounded staged_expires_at
    app.state.ingest.recover_on_startup()
    recovered = db.get_job(job_id)
    assert recovered is not None
    assert recovered["status"] == "failed"
    assert recovered["staged_expires_at"] is not None
    assert staged_file.is_file()

    # 3) Public job API must redact staged_path from both top-level AND params_json
    with TestClient(app) as client:
        res = client.get(f"/api/jobs/{job_id}")
        assert res.status_code == 200
        body = res.json()
        assert "staged_path" not in body
        assert "staged_path" not in (body.get("params_json") or "")
        assert str(staged_file) not in json.dumps(body)

        # 4) Atomic claim_job_for_retry prevents claim_staged_job_for_cleanup from deleting active job
        claimed_retry = db.claim_job_for_retry(job_id, progress=50.0, message="Retrying")
        assert claimed_retry is not None
        assert claimed_retry["status"] == "queued"
        assert claimed_retry["staged_expires_at"] is None
        assert db.claim_staged_job_for_cleanup(job_id) is None

        # 5) CheckDuplicateRequest rejects > 500 URLs (422), while DB batches > 500 URLs safely
        too_many = client.post(
            "/api/ingest/check-duplicate",
            json={"urls": [f"https://example.com/v/{i}" for i in range(501)]},
        )
        assert too_many.status_code == 422

        db.insert_media(
            media_id="batch-media-599",
            title="Batch Item",
            filename="batch.mp4",
            media_type="video",
            mime_type="video/mp4",
            size_bytes=123,
            drive_name="drive1",
            file_path="",
            thumb_filename=None,
            sha256=_sha256_hex(b"batch-item"),
            folder="General",
            source_url="https://example.com/v/599",
            source_origin="web",
            source_kind="ytdlp",
        )
        found = db.find_media_by_source_urls(
            [f"https://example.com/v/{i}" for i in range(600)]
        )
        assert "https://example.com/v/599" in found

