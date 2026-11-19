from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app
from tests.helpers import STATIC_DIR, _make_settings, _wait_for_job


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_chunked_upload_out_of_order_idempotent_and_assembly(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="drives")
    app = create_app(settings)

    chunk_size = 65536
    # 2 full chunks + 1 partial chunk = 3 chunks total (143,417 bytes)
    payload = (b"CHUNKED-UPLOAD-PAYLOAD-0123456789ABCDEF-" * 3600)[: (chunk_size * 2 + 12345)]
    expected_sha = _sha256_hex(payload)
    c0 = payload[0:chunk_size]
    c1 = payload[chunk_size : chunk_size * 2]
    c2 = payload[chunk_size * 2 :]

    with TestClient(app) as client:
        init_res = client.post(
            "/api/uploads/sessions",
            json={
                "filename": "assembled_video.bin",
                "title": "Assembled Binary",
                "folder": "Archives",
                "tags": ["chunked", "e2e"],
                "total_size": len(payload),
                "chunk_size": chunk_size,
            },
        )
        assert init_res.status_code == 201, init_res.text
        init_body = init_res.json()
        assert init_body["duplicate"] is False
        session = init_body["session"]
        sid = session["id"]
        assert session["total_chunks"] == 3
        assert session["chunk_size"] == chunk_size
        assert session["received_chunks"] == []
        assert session["missing_chunks"] == [0, 1, 2]
        assert session["received_bytes"] == 0

        # Upload out of order: chunk 2, then chunk 0
        r2 = client.put(
            f"/api/uploads/sessions/{sid}/chunks/2",
            content=c2,
            headers={"X-Chunk-SHA256": _sha256_hex(c2)},
        )
        assert r2.status_code == 200, r2.text
        assert r2.json()["received_bytes"] == len(c2)
        assert r2.json()["complete_ready"] is False

        r0 = client.put(
            f"/api/uploads/sessions/{sid}/chunks/0",
            content=c0,
            headers={"X-Chunk-SHA256": _sha256_hex(c0)},
        )
        assert r0.status_code == 200, r0.text
        assert r0.json()["received_bytes"] == len(c0) + len(c2)

        # Idempotent re-PUT of chunk 0 must return 200 and not double-count received_bytes
        r0_dup = client.put(
            f"/api/uploads/sessions/{sid}/chunks/0",
            content=c0,
            headers={"X-Chunk-SHA256": _sha256_hex(c0)},
        )
        assert r0_dup.status_code == 200, r0_dup.text
        assert r0_dup.json()["received_bytes"] == len(c0) + len(c2)
        assert r0_dup.json()["received_count"] == 2

        # Mid-upload status check
        status_res = client.get(f"/api/uploads/sessions/{sid}")
        assert status_res.status_code == 200
        status_body = status_res.json()
        assert status_body["received_chunks"] == [0, 2]
        assert status_body["missing_chunks"] == [1]
        assert status_body["received_bytes"] == len(c0) + len(c2)
        assert status_body["total_chunks"] == 3

        # Attempting to complete while chunk 1 is missing -> 409 with missing chunk info
        premature = client.post(f"/api/uploads/sessions/{sid}/complete")
        assert premature.status_code == 409
        premature_body = premature.json()
        assert premature_body["missing_chunks"] == [1]
        assert premature_body["missing_count"] == 1

        # Upload remaining chunk 1
        r1 = client.put(
            f"/api/uploads/sessions/{sid}/chunks/1",
            content=c1,
            headers={"X-Chunk-SHA256": _sha256_hex(c1)},
        )
        assert r1.status_code == 200, r1.text
        assert r1.json()["received_bytes"] == len(payload)
        assert r1.json()["complete_ready"] is True

        # Complete upload -> 202 with job_id
        comp1 = client.post(
            f"/api/uploads/sessions/{sid}/complete",
            json={"sha256": expected_sha},
        )
        assert comp1.status_code == 202, comp1.text
        job_id = comp1.json()["job_id"]
        assert job_id

        # Idempotent repeat call to /complete returns same job_id and creates no duplicate job
        comp2 = client.post(
            f"/api/uploads/sessions/{sid}/complete",
            json={"sha256": expected_sha},
        )
        assert comp2.status_code == 202, comp2.text
        assert comp2.json()["job_id"] == job_id

        job = _wait_for_job(client, job_id)
        assert job["status"] == "done", job
        assert len(job["media_ids"]) == 1
        media_id = job["media_ids"][0]

        media = app.state.db.get_media(media_id)
        assert media is not None
        assert media["sha256"] == expected_sha
        assert media["size_bytes"] == len(payload)
        assert media["folder"] == "Archives"
        assert set(media["tags"]) == {"chunked", "e2e"}

        stored_path = Path(media["file_path"])
        assert stored_path.is_file()
        assert stored_path.read_bytes() == payload
        assert _sha256_hex(stored_path.read_bytes()) == expected_sha

        # Only 1 job was created for this upload
        assert app.state.db.count_jobs() == 1


def test_chunk_and_complete_integrity_verification(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="drives")
    app = create_app(settings)

    payload = b"integrity-check-data-" * 100
    actual_sha = _sha256_hex(payload)
    wrong_sha = "f" * 64

    with TestClient(app) as client:
        # Session 1: no expected_sha256 at init, verify chunk SHA rejection & complete SHA mismatch recovery
        s1_res = client.post(
            "/api/uploads/sessions",
            json={"filename": "integrity.bin", "total_size": len(payload)},
        )
        assert s1_res.status_code == 201
        sid1 = s1_res.json()["session"]["id"]

        # Wrong X-Chunk-SHA256 header -> 400 and chunk NOT marked received
        bad_chunk = client.put(
            f"/api/uploads/sessions/{sid1}/chunks/0",
            content=payload,
            headers={"X-Chunk-SHA256": wrong_sha},
        )
        assert bad_chunk.status_code == 400
        mid = client.get(f"/api/uploads/sessions/{sid1}").json()
        assert mid["received_chunks"] == []
        assert mid["received_bytes"] == 0

        # Correct X-Chunk-SHA256 header -> 200
        good_chunk = client.put(
            f"/api/uploads/sessions/{sid1}/chunks/0",
            content=payload,
            headers={"X-Chunk-SHA256": actual_sha},
        )
        assert good_chunk.status_code == 200
        assert good_chunk.json()["complete_ready"] is True

        # Wrong sha256 on /complete -> 400 and session remains 'uploading' (recoverable)
        bad_complete = client.post(
            f"/api/uploads/sessions/{sid1}/complete",
            json={"sha256": wrong_sha},
        )
        assert bad_complete.status_code == 400
        still_uploading = client.get(f"/api/uploads/sessions/{sid1}").json()
        assert still_uploading["status"] == "uploading"

        # Retry /complete with correct sha256 -> succeeds
        good_complete = client.post(
            f"/api/uploads/sessions/{sid1}/complete",
            json={"sha256": actual_sha},
        )
        assert good_complete.status_code == 202
        done_job = _wait_for_job(client, good_complete.json()["job_id"])
        assert done_job["status"] == "done"

        # Session 2: expected_sha256 mismatch detected at /complete (with force=True to bypass dedup)
        s2_res = client.post(
            "/api/uploads/sessions",
            json={
                "filename": "init_mismatch.bin",
                "total_size": len(payload),
                "sha256": wrong_sha,
                "force": True,
            },
        )
        assert s2_res.status_code == 201
        sid2 = s2_res.json()["session"]["id"]
        assert (
            client.put(f"/api/uploads/sessions/{sid2}/chunks/0", content=payload).status_code
            == 200
        )
        mismatch_comp = client.post(f"/api/uploads/sessions/{sid2}/complete")
        assert mismatch_comp.status_code == 400
        s2_status = client.get(f"/api/uploads/sessions/{sid2}").json()
        assert s2_status["status"] == "uploading"


def test_preflight_dedup_sessions_and_check_duplicate(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="drives")
    app = create_app(settings)

    file_bytes = b"preflight-dedup-content-" * 128
    file_sha = _sha256_hex(file_bytes)
    source_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ&si=track99"

    with TestClient(app) as client:
        # 1) Upload once via chunked session and wait for ingest done
        s1 = client.post(
            "/api/uploads/sessions",
            json={"filename": "original.bin", "total_size": len(file_bytes), "folder": "Media"},
        ).json()["session"]
        assert client.put(f"/api/uploads/sessions/{s1['id']}/chunks/0", content=file_bytes).status_code == 200
        comp = client.post(f"/api/uploads/sessions/{s1['id']}/complete", json={"sha256": file_sha})
        assert comp.status_code == 202
        j1 = _wait_for_job(client, comp.json()["job_id"])
        assert j1["status"] == "done"
        existing_id = j1["media_ids"][0]

        # Seed a URL-backed media item for batch URL duplicate check
        app.state.db.insert_media(
            media_id="url_dup_1",
            title="Vault Video",
            filename="vault_video.mp4",
            folder="Videos",
            media_type="video",
            mime_type="video/mp4",
            size_bytes=9999,
            drive_name="drive1",
            file_path="",
            thumb_filename=None,
            source_origin="web",
            source_kind="ytdlp",
            source_url=source_url,
        )

        # Ensure staging_dir is empty before duplicate session init
        assert list(settings.staging_dir.iterdir()) == []

        # 2) Session init with matching sha256 and force=False -> 200, duplicate=True, zero staging files
        dup_init = client.post(
            "/api/uploads/sessions",
            json={
                "filename": "duplicate_copy.bin",
                "total_size": len(file_bytes),
                "sha256": file_sha,
                "force": False,
            },
        )
        assert dup_init.status_code == 200
        dup_body = dup_init.json()
        assert dup_body["duplicate"] is True
        assert dup_body["existing_item"]["id"] == existing_id
        assert dup_body["session"] is None
        assert list(settings.staging_dir.iterdir()) == []

        # 3) Session init with matching sha256 and force=True -> 201 real session
        force_init = client.post(
            "/api/uploads/sessions",
            json={
                "filename": "forced_copy.bin",
                "total_size": len(file_bytes),
                "sha256": file_sha,
                "force": True,
            },
        )
        assert force_init.status_code == 201
        force_body = force_init.json()
        assert force_body["duplicate"] is False
        assert force_body["session"]["id"]
        assert len(list(settings.staging_dir.iterdir())) == 1

        # 4) /api/ingest/check-duplicate: exact sha256 match
        chk_sha = client.post("/api/ingest/check-duplicate", json={"sha256": file_sha})
        assert chk_sha.status_code == 200
        assert chk_sha.json()["duplicate"] is True
        assert chk_sha.json()["match_type"] == "sha256"
        assert chk_sha.json()["item"]["id"] == existing_id

        # 5) /api/ingest/check-duplicate: size_bytes + filename candidate check
        chk_size_hit = client.post(
            "/api/ingest/check-duplicate",
            json={"size_bytes": len(file_bytes), "filename": "anything.bin"},
        )
        assert chk_size_hit.status_code == 200
        assert chk_size_hit.json()["size_match_count"] >= 1
        assert chk_size_hit.json()["needs_hash_verification"] is True

        chk_size_miss = client.post(
            "/api/ingest/check-duplicate",
            json={"size_bytes": len(file_bytes) + 777, "filename": "anything.bin"},
        )
        assert chk_size_miss.status_code == 200
        assert chk_size_miss.json()["size_match_count"] == 0
        assert chk_size_miss.json()["needs_hash_verification"] is False

        # 6) /api/ingest/check-duplicate: batch urls check
        variant_hit = "https://youtu.be/dQw4w9WgXcQ?t=12"
        url_miss = "https://example.com/not-in-vault"
        chk_urls = client.post(
            "/api/ingest/check-duplicate",
            json={"urls": [variant_hit, url_miss]},
        )
        assert chk_urls.status_code == 200
        url_matches = chk_urls.json()["url_matches"]
        assert url_matches[variant_hit]["duplicate"] is True
        assert url_matches[variant_hit]["item"]["id"] == "url_dup_1"
        assert url_matches[url_miss]["duplicate"] is False
        assert url_matches[url_miss]["item"] is None


def test_chunked_upload_edge_cases_security_quotas_ttl_and_auth(tmp_path: Path):
    # A) 0-byte upload, cancellation, oversized file, oversized chunk, filename sanitization
    settings = _make_settings(tmp_path / "a", storage_mode="drives")
    settings.max_upload_mb = 1
    app = create_app(settings)

    with TestClient(app) as client:
        # 1) 0-byte file upload: init -> complete immediately without chunk PUTs
        zero_res = client.post(
            "/api/uploads/sessions",
            json={"filename": "empty.bin", "total_size": 0},
        )
        assert zero_res.status_code == 201
        zero_sess = zero_res.json()["session"]
        assert zero_sess["total_chunks"] == 0
        assert zero_sess["missing_chunks"] == []
        zero_comp = client.post(
            f"/api/uploads/sessions/{zero_sess['id']}/complete",
            json={"sha256": _sha256_hex(b"")},
        )
        assert zero_comp.status_code == 202
        zero_job = _wait_for_job(client, zero_comp.json()["job_id"])
        assert zero_job["status"] == "done"
        zero_media = app.state.db.get_media(zero_job["media_ids"][0])
        assert zero_media is not None
        assert zero_media["size_bytes"] == 0
        assert zero_media["sha256"] == _sha256_hex(b"")

        # 2) Session cancellation deletes staging dir and rejects subsequent chunk PUT
        c_res = client.post(
            "/api/uploads/sessions",
            json={"filename": "to_cancel.bin", "total_size": 1024},
        )
        assert c_res.status_code == 201
        c_sid = c_res.json()["session"]["id"]
        c_dir = settings.staging_dir / f"up_{c_sid}"
        assert c_dir.is_dir()

        del_res = client.delete(f"/api/uploads/sessions/{c_sid}")
        assert del_res.status_code == 200
        assert del_res.json()["deleted"] is True
        assert not c_dir.exists()

        put_after_del = client.put(f"/api/uploads/sessions/{c_sid}/chunks/0", content=b"x" * 1024)
        assert put_after_del.status_code in (404, 409, 410)

        # 3) Size limit (413): total_size > max_upload_mb
        too_large = client.post(
            "/api/uploads/sessions",
            json={"filename": "huge.bin", "total_size": (2 * 1024 * 1024)},
        )
        assert too_large.status_code == 413

        # 4) Oversized chunk body rejected
        small_sess = client.post(
            "/api/uploads/sessions",
            json={"filename": "small.bin", "total_size": 100},
        ).json()["session"]
        oversized_chunk = client.put(
            f"/api/uploads/sessions/{small_sess['id']}/chunks/0",
            content=b"A" * 256,
        )
        # Note: app/uploads.py raises UploadError(400, ...) on oversized chunk body
        assert oversized_chunk.status_code in (400, 413)
        assert "exceeds expected size" in oversized_chunk.json()["detail"].lower()

        # 5) Path-traversal / unsafe filename sanitization
        trav1 = client.post(
            "/api/uploads/sessions",
            json={"filename": "../../etc/passwd", "total_size": 10},
        )
        assert trav1.status_code == 201
        t1_sess = trav1.json()["session"]
        assert t1_sess["filename"] == "passwd"
        t1_db = app.state.db.get_upload_session(t1_sess["id"])
        assert t1_db is not None
        assert Path(t1_db["staging_file"]).parent == (settings.staging_dir / f"up_{t1_sess['id']}").resolve()

        trav2 = client.post(
            "/api/uploads/sessions",
            json={"filename": "..\\..\\evil.mp4", "total_size": 10},
        )
        assert trav2.status_code == 201
        assert trav2.json()["session"]["filename"] == "evil.mp4"

        for bad_name in ("..", ".", "../..", "/"):
            bad_res = client.post(
                "/api/uploads/sessions",
                json={"filename": bad_name, "total_size": 10},
            )
            assert bad_res.status_code == 400, f"Expected 400 for {bad_name!r}, got {bad_res.status_code}"

        empty_name = client.post(
            "/api/uploads/sessions",
            json={"filename": "", "total_size": 10},
        )
        assert empty_name.status_code in (400, 422)

    # B) Staging quota (507)
    q_settings = _make_settings(tmp_path / "quota", storage_mode="drives")
    q_settings.upload_max_staging_mb = 1  # 1 MB staging quota
    q_app = create_app(q_settings)
    with TestClient(q_app) as q_client:
        r_ok = q_client.post(
            "/api/uploads/sessions",
            json={"filename": "part1.bin", "total_size": 700_000},
        )
        assert r_ok.status_code == 201
        r_over = q_client.post(
            "/api/uploads/sessions",
            json={"filename": "part2.bin", "total_size": 500_000},
        )
        assert r_over.status_code == 507

    # C) Concurrent session cap (429) & TTL expiry (410) & GC cleanup
    c_settings = _make_settings(tmp_path / "cap", storage_mode="drives")
    c_settings.upload_max_concurrent_sessions = 2
    c_app = create_app(c_settings)
    with TestClient(c_app) as c_client:
        s_a = c_client.post("/api/uploads/sessions", json={"filename": "a.bin", "total_size": 64})
        s_b = c_client.post("/api/uploads/sessions", json={"filename": "b.bin", "total_size": 64})
        assert s_a.status_code == 201
        assert s_b.status_code == 201
        s_c = c_client.post("/api/uploads/sessions", json={"filename": "c.bin", "total_size": 64})
        assert s_c.status_code == 429

        sid_a = s_a.json()["session"]["id"]
        sid_b = s_b.json()["session"]["id"]
        dir_a = c_settings.staging_dir / f"up_{sid_a}"
        dir_b = c_settings.staging_dir / f"up_{sid_b}"
        assert dir_a.is_dir() and dir_b.is_dir()

        past_iso = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        c_app.state.db.update_upload_session(sid_a, expires_at=past_iso)
        expired_put = c_client.put(f"/api/uploads/sessions/{sid_a}/chunks/0", content=b"x" * 64)
        assert expired_put.status_code == 410
        assert not dir_a.exists()
        assert c_app.state.db.get_upload_session(sid_a)["status"] == "expired"

        # Expire sid_b and verify run_gc() cleans staging dir and marks expired
        c_app.state.db.update_upload_session(sid_b, expires_at=past_iso)
        gc_res = asyncio.run(c_app.state.uploads.run_gc())
        assert gc_res["expired_sessions"] >= 1
        assert not dir_b.exists()
        assert c_app.state.db.get_upload_session(sid_b)["status"] == "expired"

    # D) Auth enforcement on upload session endpoints
    auth_settings = _make_settings(tmp_path / "auth", auth_token="upload-secret", storage_mode="drives")
    auth_app = create_app(auth_settings)
    with TestClient(auth_app) as a_client:
        assert (
            a_client.post("/api/uploads/sessions", json={"filename": "x.bin", "total_size": 10}).status_code
            == 401
        )
        assert a_client.get("/api/uploads/sessions").status_code == 401

        headers = {"Authorization": "Bearer upload-secret"}
        created = a_client.post(
            "/api/uploads/sessions",
            json={"filename": "x.bin", "total_size": 10},
            headers=headers,
        )
        assert created.status_code == 201
        sid = created.json()["session"]["id"]

        assert a_client.get(f"/api/uploads/sessions/{sid}").status_code == 401
        assert a_client.put(f"/api/uploads/sessions/{sid}/chunks/0", content=b"1234567890").status_code == 401
        assert a_client.post(f"/api/uploads/sessions/{sid}/complete").status_code == 401
        assert a_client.delete(f"/api/uploads/sessions/{sid}").status_code == 401


def test_startup_recovery_and_integrity_protection_for_sessions_and_retained_uploads(tmp_path: Path):
    from app.db import Database
    from app.ingest import IngestService
    from app.allocator import DriveAllocator
    from app.uploads import UploadSessionService

    settings = _make_settings(tmp_path, storage_mode="drives")
    settings.ensure_dirs()
    db = Database(settings.db_path)
    allocator = DriveAllocator(settings)
    ingest = IngestService(settings, db, allocator)
    uploads = UploadSessionService(settings, db, ingest)
    ingest.upload_service = uploads

    future_iso = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()
    past_iso = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()

    # 1) Active unexpired upload session
    active_dir = settings.staging_dir / "up_active_sess"
    active_dir.mkdir(parents=True, exist_ok=True)
    active_file = active_dir / "active.bin"
    active_file.write_bytes(b"in-progress-bytes")
    db.create_upload_session(
        session_id="active_sess",
        filename="active.bin",
        total_size=1024,
        chunk_size=65536,
        total_chunks=1,
        staging_dir=str(active_dir.resolve()),
        staging_file=str(active_file.resolve()),
        expires_at=future_iso,
        status="uploading",
    )

    # 2) Expired upload session
    expired_dir = settings.staging_dir / "up_expired_sess"
    expired_dir.mkdir(parents=True, exist_ok=True)
    expired_file = expired_dir / "expired.bin"
    expired_file.write_bytes(b"stale-bytes")
    db.create_upload_session(
        session_id="expired_sess",
        filename="expired.bin",
        total_size=1024,
        chunk_size=65536,
        total_chunks=1,
        staging_dir=str(expired_dir.resolve()),
        staging_file=str(expired_file.resolve()),
        expires_at=past_iso,
        status="uploading",
    )

    # 3) Retained unexpired failed upload job
    retained_dir = settings.staging_dir / "up_retained_job"
    retained_dir.mkdir(parents=True, exist_ok=True)
    retained_file = retained_dir / "failed.bin"
    retained_file.write_bytes(b"retained-failed-bytes")
    retained_job_id = db.create_job("web", "upload", "failed.bin")
    db.update_job(
        retained_job_id,
        status="failed",
        message="Telegram down",
        staged_path=str(retained_file.resolve()),
        staged_expires_at=future_iso,
        staged_size_bytes=retained_file.stat().st_size,
    )

    # 4) Unreferenced orphan staging directory
    orphan_dir = settings.staging_dir / "orphan_garbage"
    orphan_dir.mkdir(parents=True, exist_ok=True)
    (orphan_dir / "junk.part").write_bytes(b"junk")

    db.close()

    # Boot app -> recover_on_startup() runs
    app = create_app(settings)
    with TestClient(app) as client:
        # Active session & retained failed job survive; expired session & orphan dir are deleted
        assert active_dir.is_dir() and active_file.is_file()
        assert retained_dir.is_dir() and retained_file.is_file()
        assert not expired_dir.exists()
        assert not orphan_dir.exists()

        assert app.state.db.get_upload_session("active_sess")["status"] == "uploading"
        assert app.state.db.get_upload_session("expired_sess")["status"] == "expired"

        # Now place a fresh orphan dir inside staging_dir and run IntegrityService audit with grace=0
        stray_dir = settings.staging_dir / "stray_audit_orphan"
        stray_dir.mkdir(parents=True, exist_ok=True)
        (stray_dir / "stray.tmp").write_bytes(b"stray")

        audit_res = client.post(
            "/api/system/integrity/audit",
            json={"verify_telegram": False, "staging_grace_sec": 0},
        )
        assert audit_res.status_code == 200, audit_res.text
        audit = audit_res.json()
        orphan_names = [o["name"] for o in audit["details"]["orphaned_staging"]]
        assert "stray_audit_orphan" in orphan_names
        assert "up_active_sess" not in orphan_names
        assert "up_retained_job" not in orphan_names


def test_pure_js_streaming_sha256_via_node():
    sha_js_src = (STATIC_DIR / "js" / "crypto" / "sha256.js").read_text(encoding="utf-8")
    one_mb = b"MediaVault-SHA256-Pattern-0123456789!" * (1024 * 1024 // 37 + 1)
    one_mb = one_mb[: 1024 * 1024]
    expected_1mb = _sha256_hex(one_mb)

    boundary_55 = b"a" * 55
    boundary_56 = b"b" * 56
    boundary_63 = b"c" * 63
    boundary_64 = b"d" * 64
    boundary_65 = b"e" * 65
    boundary_128 = b"f" * 128

    expected_vectors = {
        "empty": _sha256_hex(b""),
        "abc": _sha256_hex(b"abc"),
        "b55": _sha256_hex(boundary_55),
        "b56": _sha256_hex(boundary_56),
        "b63": _sha256_hex(boundary_63),
        "b64": _sha256_hex(boundary_64),
        "b65": _sha256_hex(boundary_65),
        "b128": _sha256_hex(boundary_128),
        "one_mb": expected_1mb,
    }

    node_harness = """
// Strip globalThis.crypto to simulate plain HTTP LAN/Tailscale origin (no crypto.subtle)
Object.defineProperty(globalThis, "crypto", {
  value: undefined,
  configurable: true,
  writable: true,
});

(async () => {
  const enc = new TextEncoder();

  // 1) NIST & boundary vectors via IncrementalSha256 + sha256Bytes
  const emptyHex = new IncrementalSha256().hex();
  const abcHex = new IncrementalSha256().update(enc.encode("abc")).hex();
  const abcFallback = await sha256Bytes(enc.encode("abc"));

  // Multi-chunk boundary splits (e.g. 1-byte / 7-byte / 55/56/63/64/65/128 splits)
  function splitHash(bytes, splits) {
    const h = new IncrementalSha256();
    let offset = 0;
    for (const sz of splits) {
      if (offset >= bytes.length) break;
      h.update(bytes.subarray(offset, Math.min(bytes.length, offset + sz)));
      // Calling hex() mid-stream must not corrupt internal state
      void h.hex();
      offset += sz;
    }
    if (offset < bytes.length) {
      h.update(bytes.subarray(offset));
    }
    return h.hex();
  }

  const b55 = await sha256Bytes(new Uint8Array(55).fill(97));
  const b56 = splitHash(new Uint8Array(56).fill(98), [1, 7, 13, 20, 15]);
  const b63 = splitHash(new Uint8Array(63).fill(99), [31, 32]);
  const b64 = splitHash(new Uint8Array(64).fill(100), [64]);
  const b65 = splitHash(new Uint8Array(65).fill(101), [55, 1, 9]);
  const b128 = splitHash(new Uint8Array(128).fill(102), [56, 7, 1, 64]);

  // 2) 1MB repeating pattern across irregular chunks
  const pat = enc.encode("MediaVault-SHA256-Pattern-0123456789!");
  const oneMb = new Uint8Array(1024 * 1024);
  for (let i = 0; i < oneMb.length; i++) {
    oneMb[i] = pat[i % pat.length];
  }
  const h1mb = new IncrementalSha256();
  const step = 65537; // intentionally unaligned to 64-byte SHA blocks
  for (let i = 0; i < oneMb.length; i += step) {
    h1mb.update(oneMb.subarray(i, Math.min(oneMb.length, i + step)));
  }
  const oneMbHex = h1mb.hex();
  const oneMbDirect = await sha256Bytes(oneMb);
  if (oneMbHex !== oneMbDirect) {
    throw new Error("Incremental vs sha256Bytes mismatch on 1MB");
  }
  if (abcHex !== abcFallback) {
    throw new Error("Incremental vs sha256Bytes mismatch on abc");
  }

  console.log(JSON.stringify({
    empty: emptyHex,
    abc: abcHex,
    b55,
    b56,
    b63,
    b64,
    b65,
    b128,
    one_mb: oneMbHex,
  }));
})();
"""

    proc = subprocess.run(
        ["node", "--input-type=module", "-"],
        input=f"{sha_js_src}\n{node_harness}",
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, f"Node script failed:\nSTDOUT: {proc.stdout}\nSTDERR: {proc.stderr}"
    actual = json.loads(proc.stdout.strip())
    assert actual == expected_vectors
