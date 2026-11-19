from __future__ import annotations

import asyncio
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from telethon.errors import FloodWaitError

from app.config import TelegramApiConfig
from app.main import create_app
from tests.helpers import _make_settings, _sample_jpeg_bytes, _wait_for_upload_item


def _wait_for_maintenance_run(client: TestClient, run_id: str, *, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    run = None
    while time.monotonic() < deadline:
        resp = client.get(f"/api/system/integrity/runs/{run_id}")
        assert resp.status_code == 200, resp.text
        run = resp.json()
        if run["status"] in ("done", "failed"):
            return run
        time.sleep(0.01)
    raise AssertionError(f"Maintenance run {run_id} did not finish within {timeout}s: {run}")


def _set_old_mtime(path: Path, age_sec: float = 600.0) -> None:
    old_ts = time.time() - age_sec
    if path.is_dir():
        for child in path.rglob("*"):
            os.utime(child, (old_ts, old_ts))
    os.utime(path, (old_ts, old_ts))


def test_integrity_audit_detects_local_issues_and_respects_active_staging(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="drives")
    app = create_app(settings)
    jpg = _sample_jpeg_bytes()

    with TestClient(app) as client:
        # 1. Upload real JPEG so local file + thumb + metadata exist
        resp = client.post(
            "/api/ingest/upload",
            data={"folder": "Audit"},
            files={"file": ("photo.jpg", jpg, "image/jpeg")},
        )
        item = _wait_for_upload_item(client, resp)
        media_id = item["id"]
        thumb_path = settings.thumbs_dir / item["thumb_filename"]
        assert thumb_path.is_file()

        # Delete thumbnail file on disk
        thumb_path.unlink()

        # Clear width/height in SQLite
        with app.state.db._connect() as conn:
            conn.execute(
                "UPDATE media_items SET width = NULL, height = NULL WHERE id = ?",
                (media_id,),
            )

        # Create orphan thumb in data/thumbs/stale_orphan.webp
        orphan_thumb = settings.thumbs_dir / "stale_orphan.webp"
        orphan_thumb.write_bytes(b"stale-thumb-bytes")

        # Create active registered staging dir (even backdated) AND old unregistered stale staging dir
        active_dir = settings.staging_dir / "active_ingest_dir"
        active_dir.mkdir(parents=True, exist_ok=True)
        (active_dir / "part.tmp").write_bytes(b"active")
        _set_old_mtime(active_dir, age_sec=600.0)
        app.state.ingest.register_staging_dir(active_dir)

        stale_dir = settings.staging_dir / "stale_orphan_dir"
        stale_dir.mkdir(parents=True, exist_ok=True)
        (stale_dir / "leftover.tmp").write_bytes(b"stale-staging")
        _set_old_mtime(stale_dir, age_sec=600.0)

        # Insert local-only media item pointing to a missing file on disk
        missing_file_path = tmp_path / "drives" / "drive1" / "Archive" / "gone.bin"
        app.state.db.insert_media(
            media_id="missing_local_1",
            title="Missing Local",
            filename="gone.bin",
            folder="Archive",
            media_type="file",
            mime_type="application/octet-stream",
            size_bytes=512,
            drive_name="drive1",
            file_path=str(missing_file_path),
            thumb_filename=None,
            source_origin="web",
            source_kind="upload",
            source_url=None,
        )

        # Run integrity audit
        audit_resp = client.post(
            "/api/system/integrity/audit",
            json={"verify_telegram": False, "staging_grace_sec": 300},
        )
        assert audit_resp.status_code == 200, audit_resp.text
        audit = audit_resp.json()
        assert audit["status"] == "done"

        summary = audit["summary"]
        assert summary["healthy"] is False
        assert summary["missing_thumbs"] == 1
        assert summary["missing_metadata"] == 1
        assert summary["orphaned_thumbs"] == 1
        assert summary["orphaned_staging"] == 1
        assert summary["missing_local_files"] == 1
        assert summary["missing_local_unrecoverable"] == 1

        details = audit["details"]
        assert [x["media_id"] for x in details["missing_thumbs"]] == [media_id]
        assert [x["media_id"] for x in details["missing_metadata"]] == [media_id]
        assert [x["name"] for x in details["orphaned_thumbs"]] == ["stale_orphan.webp"]
        assert [x["name"] for x in details["orphaned_staging"]] == ["stale_orphan_dir"]
        assert all(x["name"] != "active_ingest_dir" for x in details["orphaned_staging"])
        assert [x["media_id"] for x in details["missing_local_files"]] == ["missing_local_1"]

        # Verify status and runs endpoints reflect the completed audit
        status_resp = client.get("/api/system/integrity/status")
        assert status_resp.status_code == 200
        assert status_resp.json()["latest_audit"]["id"] == audit["id"]

        runs_resp = client.get("/api/system/integrity/runs?kind=audit")
        assert runs_resp.status_code == 200
        assert any(r["id"] == audit["id"] for r in runs_resp.json()["runs"])


def test_integrity_repair_backfill_and_opt_in_purge(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="drives")
    app = create_app(settings)
    jpg = _sample_jpeg_bytes()

    with TestClient(app) as client:
        resp = client.post(
            "/api/ingest/upload",
            data={"folder": "Repair"},
            files={"file": ("repair.jpg", jpg, "image/jpeg")},
        )
        item = _wait_for_upload_item(client, resp)
        media_id = item["id"]
        thumb_path = settings.thumbs_dir / item["thumb_filename"]
        thumb_path.unlink()

        with app.state.db._connect() as conn:
            conn.execute(
                "UPDATE media_items SET width = NULL, height = NULL WHERE id = ?",
                (media_id,),
            )

        orphan_thumb = settings.thumbs_dir / "stale_orphan.webp"
        orphan_thumb.write_bytes(b"orphan-bytes")

        active_dir = settings.staging_dir / "active_dir"
        active_dir.mkdir(parents=True, exist_ok=True)
        (active_dir / "busy.bin").write_bytes(b"123")
        _set_old_mtime(active_dir, age_sec=600.0)
        app.state.ingest.register_staging_dir(active_dir)

        stale_dir = settings.staging_dir / "stale_dir"
        stale_dir.mkdir(parents=True, exist_ok=True)
        (stale_dir / "old.bin").write_bytes(b"456")
        _set_old_mtime(stale_dir, age_sec=600.0)

        # Step 1: Backfill thumbs & metadata WITHOUT purging orphans
        rep1_resp = client.post(
            "/api/system/integrity/repair",
            json={
                "backfill_thumbs": True,
                "backfill_metadata": True,
                "purge_orphan_thumbs": False,
                "purge_orphan_staging": False,
                "staging_grace_sec": 300,
            },
        )
        assert rep1_resp.status_code == 202, rep1_resp.text
        rep1_done = _wait_for_maintenance_run(client, rep1_resp.json()["id"])
        assert rep1_done["status"] == "done"
        assert rep1_done["summary"]["thumbs_repaired"] == 1
        assert rep1_done["summary"]["metadata_repaired"] == 1
        assert rep1_done["summary"]["orphan_thumbs_purged"] == 0
        assert rep1_done["summary"]["orphan_staging_purged"] == 0

        # Thumbnail regenerated on disk and width/height restored in DB
        assert (settings.thumbs_dir / f"{media_id}.webp").is_file()
        refreshed = client.get(f"/api/media/{media_id}").json()
        assert refreshed["width"] == 640
        assert refreshed["height"] == 360

        # Orphans MUST still exist because purge flags were False
        assert orphan_thumb.is_file()
        assert stale_dir.is_dir()
        assert active_dir.is_dir()

        # Step 2: Run repair again with opt-in purge enabled
        rep2_resp = client.post(
            "/api/system/integrity/repair",
            json={
                "backfill_thumbs": False,
                "backfill_previews": False,
                "backfill_metadata": False,
                "purge_orphan_thumbs": True,
                "purge_orphan_staging": True,
                "staging_grace_sec": 300,
            },
        )
        assert rep2_resp.status_code == 202, rep2_resp.text
        rep2_done = _wait_for_maintenance_run(client, rep2_resp.json()["id"])
        assert rep2_done["status"] == "done"
        assert rep2_done["summary"]["orphan_thumbs_purged"] == 1
        assert rep2_done["summary"]["orphan_staging_purged"] == 1
        assert rep2_done["summary"]["freed_bytes"] > 0

        assert not orphan_thumb.exists()
        assert not stale_dir.exists()
        assert active_dir.is_dir()
        assert (active_dir / "busy.bin").is_file()


def test_integrity_audit_telegram_verification_and_floodwait(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_archive_chat_id = "-100555666"
    settings.integrity_telegram_batch_delay_sec = 0.0
    app = create_app(settings)

    class FakeWorkerClient:
        def __init__(self, name: str):
            self.name = name
            self.raise_flood = False
            self.calls: list[list[int]] = []

        def is_connected(self) -> bool:
            return True

        async def disconnect(self) -> None:
            pass

        async def get_input_entity(self, target):
            return target

        async def get_messages(self, peer, ids):
            batch = list(ids)
            self.calls.append(batch)
            if self.raise_flood:
                raise FloodWaitError(None, 30)
            out = []
            for mid in batch:
                if mid == 101:
                    out.append(SimpleNamespace(id=101, media=object(), empty=False))
                else:
                    out.append(None)
            return out

    w1 = FakeWorkerClient("bot1")
    w2 = FakeWorkerClient("bot2")
    app.state.tg_pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h1", bot_token="t1"), w1),
        (TelegramApiConfig(name="bot2", api_id=2, api_hash="h2", bot_token="t2"), w2),
    ]

    with TestClient(app) as client:
        app.state.db.insert_media(
            media_id="tg_present",
            title="Present",
            filename="present.bin",
            folder="Archive",
            media_type="file",
            mime_type="application/octet-stream",
            size_bytes=100,
            drive_name="telegram",
            file_path="",
            thumb_filename=None,
            source_origin="web",
            source_kind="upload",
            source_url=None,
            telegram_chat_id="-100555666",
            telegram_msg_id=101,
            telegram_worker_name="bot1",
        )
        app.state.db.insert_media(
            media_id="tg_deleted",
            title="Deleted",
            filename="deleted.bin",
            folder="Archive",
            media_type="file",
            mime_type="application/octet-stream",
            size_bytes=100,
            drive_name="telegram",
            file_path="",
            thumb_filename=None,
            source_origin="web",
            source_kind="upload",
            source_url=None,
            telegram_chat_id="-100555666",
            telegram_msg_id=102,
            telegram_worker_name="bot1",
        )

        # 1. Batch get_messages detects 1 present & 1 deleted message
        resp1 = client.post("/api/system/integrity/audit", json={"verify_telegram": True})
        assert resp1.status_code == 200, resp1.text
        s1 = resp1.json()["summary"]
        assert s1["telegram_verified"] is True
        assert s1["telegram_checked_messages"] == 2
        assert s1["broken_telegram_refs"] == 1
        assert s1["telegram_unchecked_rate_limited"] == 0
        assert resp1.json()["details"]["broken_telegram_refs"][0]["media_id"] == "tg_deleted"

        # 2. Worker 1 raises FloodWaitError -> worker 2 handles the batch seamlessly
        app.state.tg_pool._cooldown_until.clear()
        app.state.tg_pool._rr_index = 0
        w1.calls.clear()
        w2.calls.clear()
        w1.raise_flood = True
        w2.raise_flood = False

        resp2 = client.post("/api/system/integrity/audit", json={"verify_telegram": True})
        assert resp2.status_code == 200, resp2.text
        s2 = resp2.json()["summary"]
        assert s2["telegram_checked_messages"] == 2
        assert s2["broken_telegram_refs"] == 1
        assert s2["telegram_unchecked_rate_limited"] == 0
        assert len(w2.calls) == 1
        assert app.state.tg_pool._flood_wait_counts.get("bot1") == 1

        # 3. ALL workers raise FloodWaitError across multi-batch scan -> stops cleanly without hammering
        settings.integrity_telegram_batch_size = 1
        app.state.tg_pool._cooldown_until.clear()
        w1.calls.clear()
        w2.calls.clear()
        w1.raise_flood = True
        w2.raise_flood = True

        resp3 = client.post("/api/system/integrity/audit", json={"verify_telegram": True})
        assert resp3.status_code == 200, resp3.text
        s3 = resp3.json()["summary"]
        assert s3["telegram_unchecked_rate_limited"] == 2
        assert s3["telegram_checked_messages"] == 0
        # Batch 2 must NOT be attempted once all workers entered FloodWait cooldown on batch 1
        assert len(w1.calls) == 1
        assert len(w2.calls) == 1


def test_integrity_repair_telegram_download_and_size_cap(tmp_path: Path, monkeypatch):
    settings = _make_settings(tmp_path, storage_mode="telegram")
    settings.telegram_archive_chat_id = "-100777888"
    app = create_app(settings)
    jpg = _sample_jpeg_bytes()

    class OnlineFakeClient:
        def is_connected(self) -> bool:
            return True

        async def disconnect(self) -> None:
            pass

    app.state.tg_pool.clients = [
        (
            TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"),
            OnlineFakeClient(),
        )
    ]

    prepared_msg_ids: list[int] = []

    async def fake_prepare_stream(chat_id: str, msg_id: int, worker_name: str | None = None):
        prepared_msg_ids.append(msg_id)
        return app.state.tg_pool.clients[0][1], {"msg_id": msg_id}

    async def fake_iter_range(_client, media, start: int, end: int):
        assert media["msg_id"] == 501
        yield jpg[start : end + 1]

    monkeypatch.setattr(app.state.tg_pool, "prepare_stream", fake_prepare_stream)
    monkeypatch.setattr(app.state.tg_pool, "iter_telegram_range", fake_iter_range)

    with TestClient(app) as client:
        app.state.db.insert_media(
            media_id="tg_ok",
            title="TG Ok",
            filename="tg_ok.jpg",
            folder="Telegram",
            media_type="image",
            mime_type="image/jpeg",
            size_bytes=len(jpg),
            drive_name="telegram",
            file_path="",
            thumb_filename=None,
            source_origin="web",
            source_kind="upload",
            source_url=None,
            telegram_chat_id="-100777888",
            telegram_msg_id=501,
            telegram_worker_name="bot1",
        )
        app.state.db.insert_media(
            media_id="tg_huge",
            title="TG Huge",
            filename="tg_huge.jpg",
            folder="Telegram",
            media_type="image",
            mime_type="image/jpeg",
            size_bytes=10 * 1024 * 1024,  # 10 MiB > 1 MiB cap
            drive_name="telegram",
            file_path="",
            thumb_filename=None,
            source_origin="web",
            source_kind="upload",
            source_url=None,
            telegram_chat_id="-100777888",
            telegram_msg_id=502,
            telegram_worker_name="bot1",
        )

        rep_resp = client.post(
            "/api/system/integrity/repair",
            json={"max_telegram_item_mb": 1},
        )
        assert rep_resp.status_code == 202, rep_resp.text
        done = _wait_for_maintenance_run(client, rep_resp.json()["id"])
        assert done["status"] == "done"

        summary = done["summary"]
        assert summary["thumbs_repaired"] == 1
        assert summary["metadata_repaired"] == 1
        assert summary["telegram_downloads"] == 1
        assert summary["skipped"] == 1
        assert summary["failed"] == 0

        # Size-capped item was skipped without calling prepare_stream
        assert prepared_msg_ids == [501]
        assert done["details"]["skipped_items"] == [
            {"media_id": "tg_huge", "reason": "skipped_size_limit"}
        ]

        # Thumbnail generated and width/height updated in DB
        assert (settings.thumbs_dir / "tg_ok.webp").is_file()
        updated_ok = client.get("/api/media/tg_ok").json()
        assert updated_ok["thumb_filename"] == "tg_ok.webp"
        assert updated_ok["width"] == 640
        assert updated_ok["height"] == 360

        # Temp repair staging dir cleaned up and unregistered
        assert list(settings.staging_dir.iterdir()) == []
        assert app.state.ingest.get_active_staging_dirs() == set()


def test_integrity_single_flight_concurrency_guard_and_auth(tmp_path: Path, monkeypatch):
    # 1. Auth enforcement across all /api/system/integrity/* endpoints
    auth_settings = _make_settings(tmp_path / "auth", auth_token="vault-tok")
    auth_app = create_app(auth_settings)
    with TestClient(auth_app) as auth_client:
        assert auth_client.get("/api/system/integrity/status").status_code == 401
        assert auth_client.get("/api/system/integrity/runs").status_code == 401
        assert auth_client.get("/api/system/integrity/runs/nonexistent").status_code == 401
        assert auth_client.post("/api/system/integrity/audit", json={}).status_code == 401
        assert auth_client.post("/api/system/integrity/repair", json={}).status_code == 401

        assert (
            auth_client.get(
                "/api/system/integrity/status",
                headers={"Authorization": "Bearer vault-tok"},
            ).status_code
            == 200
        )

    # 2. Single-flight concurrency guard (409 Conflict)
    settings = _make_settings(tmp_path / "single_flight", storage_mode="drives")
    app = create_app(settings)
    jpg = _sample_jpeg_bytes()

    entered = threading.Event()
    release = threading.Event()
    import app.integrity as integrity_mod

    orig_gen_thumb = integrity_mod.generate_thumbnail

    async def slow_generate_thumbnail(*args, **kwargs):
        entered.set()
        while not release.is_set():
            await asyncio.sleep(0.01)
        return await orig_gen_thumb(*args, **kwargs)

    with TestClient(app) as client:
        resp = client.post(
            "/api/ingest/upload",
            data={"folder": "LockTest"},
            files={"file": ("lock.jpg", jpg, "image/jpeg")},
        )
        item = _wait_for_upload_item(client, resp)
        (settings.thumbs_dir / item["thumb_filename"]).unlink()

        monkeypatch.setattr(integrity_mod, "generate_thumbnail", slow_generate_thumbnail)

        try:
            rep_resp = client.post("/api/system/integrity/repair", json={})
            assert rep_resp.status_code == 202, rep_resp.text
            run_id = rep_resp.json()["id"]
            assert entered.wait(timeout=3.0), "Repair task did not reach generate_thumbnail"

            # Concurrent repair and concurrent audit must both return 409 Conflict
            conflict_repair = client.post("/api/system/integrity/repair", json={})
            assert conflict_repair.status_code == 409

            conflict_audit = client.post("/api/system/integrity/audit", json={})
            assert conflict_audit.status_code == 409
        finally:
            release.set()

        done = _wait_for_maintenance_run(client, run_id)
        assert done["status"] == "done"
        assert done["summary"]["thumbs_repaired"] == 1


def test_startup_recovers_interrupted_maintenance_runs_and_stream_hardening(tmp_path: Path, monkeypatch):
    import time as time_mod
    from app.db import Database

    # 1. Startup recovers stale running maintenance_runs left by an unclean crash
    settings = _make_settings(tmp_path / "crash_recovery", storage_mode="telegram")
    pre_db = Database(settings.db_path)
    stale_run = pre_db.create_maintenance_run("repair", status="running", message="In progress before crash")
    pre_db.close()

    app = create_app(settings)
    recovered = app.state.db.get_maintenance_run(stale_run["id"])
    assert recovered is not None
    assert recovered["status"] == "failed"
    assert "restart" in (recovered["message"] or "").lower()

    # 2. download_media_to_path closes chunk_iter cleanly when max_bytes is exceeded
    closed_flag = {"closed": False}

    class FakeClient:
        def is_connected(self) -> bool:
            return True

        async def get_input_entity(self, target):
            return target

        async def get_messages(self, peer, ids: int):
            raise FloodWaitError(request=None, capture=12)

    pool = app.state.tg_pool
    pool.clients = [
        (TelegramApiConfig(name="bot1", api_id=1, api_hash="h", bot_token="t1"), FakeClient())
    ]

    async def fake_prepare_stream(chat_id: str, msg_id: int, worker_name: str | None = None):
        return pool.clients[0][1], object()

    async def oversize_gen(_client, _media, start: int, end: int):
        try:
            yield b"0123456789"
            yield b"0123456789"
        finally:
            closed_flag["closed"] = True

    monkeypatch.setattr(pool, "prepare_stream", fake_prepare_stream)
    monkeypatch.setattr(pool, "iter_telegram_range", oversize_gen)

    dl_target = tmp_path / "capped.bin"
    with pytest.raises(ValueError, match="exceeds repair download limit"):
        asyncio.run(
            pool.download_media_to_path(
                {"telegram_chat_id": "-1001", "telegram_msg_id": 10, "size_bytes": 0},
                dl_target,
                max_bytes=16,
            )
        )
    assert closed_flag["closed"] is True
    assert not dl_target.exists()

    # 3. Real prepare_stream propagates FloodWaitError and skips already-cooled-down workers
    monkeypatch.undo()
    with pytest.raises(FloodWaitError) as fw_exc:
        asyncio.run(pool.prepare_stream("-1001", 10))
    assert fw_exc.value.seconds == 12
    assert pool._cooldown_until.get("bot1", 0.0) > time_mod.monotonic()

    # Subsequent call while all workers are in cooldown immediately raises FloodWaitError without calling Telegram
    with pytest.raises(FloodWaitError):
        asyncio.run(pool.prepare_stream("-1001", 10))

