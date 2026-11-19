from __future__ import annotations

import gzip
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.backup import restore_backup_file, verify_backup_file
from app.config import TelegramApiConfig
from app.db import Database
from app.main import create_app
from tests.helpers import _insert_tagged_media, _make_settings, _sample_jpeg_bytes, _wait_for_upload_item


def test_backup_api_auth_create_list_download_delete_and_traversal(tmp_path: Path):
    settings = _make_settings(tmp_path, auth_token="secret-tok", storage_mode="drives")
    app = create_app(settings)
    jpg = _sample_jpeg_bytes()

    with TestClient(app) as client:
        # Unauthenticated requests must be rejected with 401
        assert client.get("/api/system/backups").status_code == 401
        assert client.post("/api/system/backups", json={}).status_code == 401
        assert client.get("/api/system/backups/missing/download").status_code == 401
        assert client.delete("/api/system/backups/missing").status_code == 401

        client.headers["Authorization"] = "Bearer secret-tok"

        # Populate DB with an uploaded media item
        upload_resp = client.post(
            "/api/ingest/upload",
            data={"folder": "BackupsTest"},
            files={"file": ("sample.jpg", jpg, "image/jpeg")},
        )
        _wait_for_upload_item(client, upload_resp)

        # Create backup via POST /api/system/backups
        create_resp = client.post("/api/system/backups", json={"note": "initial snap"})
        assert create_resp.status_code == 201, create_resp.text
        created = create_resp.json()
        backup_id = created["id"]
        backup_file = Path(created["file_path"])
        assert backup_file.is_file()
        assert backup_file.name.endswith(".db.gz")
        assert backup_file.parent == settings.backups_dir
        assert len(created["sha256"]) == 64
        assert created["size_bytes"] == backup_file.stat().st_size
        assert created["size_bytes"] > 0
        assert created["uncompressed_bytes"] > 0
        assert created["exists_locally"] is True
        assert created["in_db"] is True
        assert created["note"] == "initial snap"

        # List backups via GET /api/system/backups
        list_resp = client.get("/api/system/backups")
        assert list_resp.status_code == 200
        list_body = list_resp.json()
        assert len(list_body["backups"]) == 1
        assert list_body["backups"][0]["id"] == backup_id
        assert list_body["backups"][0]["exists_locally"] is True
        assert list_body["config"]["backups_dir"] == str(settings.backups_dir)

        # Download gzipped SQLite DB and verify via verify_backup_file()
        dl_resp = client.get(f"/api/system/backups/{backup_id}/download")
        assert dl_resp.status_code == 200
        assert "application/gzip" in dl_resp.headers.get("content-type", "")
        dl_path = tmp_path / "downloaded.db.gz"
        dl_path.write_bytes(dl_resp.content)
        verified = verify_backup_file(dl_path)
        assert verified["valid"] is True
        assert verified["media_count"] == 1
        assert verified["sha256"] == created["sha256"]
        assert verified["uncompressed_bytes"] == created["uncompressed_bytes"]

        # Path traversal attempts on download/delete must return 404 and never leak files outside backups_dir
        for bad_id in ("..%2Fmedia.db", "../media.db", "..%5Cmedia.db", "..%2F..%2Fetc%2Fpasswd", "missing_id"):
            r_dl = client.get(f"/api/system/backups/{bad_id}/download")
            assert r_dl.status_code == 404, f"Expected 404 for download {bad_id}, got {r_dl.status_code}"
            r_del = client.delete(f"/api/system/backups/{bad_id}")
            assert r_del.status_code == 404, f"Expected 404 for delete {bad_id}, got {r_del.status_code}"

        # Direct service-level path traversal checks (including "." and "..")
        for bad_service_id in ("../media.db", ".", ".."):
            with pytest.raises(FileNotFoundError):
                app.state.backup.resolve_backup_file(bad_service_id)
            assert app.state.backup.delete_backup(bad_service_id) is None
        assert settings.db_path.is_file()

        # Delete backup cleanly removes disk file & DB record
        del_resp = client.delete(f"/api/system/backups/{backup_id}")
        assert del_resp.status_code == 200
        assert del_resp.json()["deleted"] is True
        assert not backup_file.exists()
        assert client.get("/api/system/backups").json()["backups"] == []


def test_backup_rotating_local_retention(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="drives")
    settings.backup_retention_count = 2
    app = create_app(settings)

    with TestClient(app) as client:
        _insert_tagged_media(app.state.db, "m1", "tag1")

        created_ids: list[str] = []
        created_paths: list[Path] = []
        base_ts = 1_700_000_000.0
        for idx in range(4):
            # Backdate existing local backups in creation order so mtime ordering is deterministic
            for prev_idx, prev_file in enumerate(created_paths):
                if prev_file.exists():
                    os.utime(prev_file, (base_ts + prev_idx, base_ts + prev_idx))
            resp = client.post("/api/system/backups", json={"note": f"snap-{idx}"})
            assert resp.status_code == 201, resp.text
            body = resp.json()
            created_ids.append(body["id"])
            created_paths.append(Path(body["file_path"]))

        remaining_files = list(settings.backups_dir.glob("*.db.gz"))
        assert len(remaining_files) == 2

        listed = client.get("/api/system/backups").json()["backups"]
        listed_ids = {b["id"] for b in listed}
        assert listed_ids == {created_ids[-1], created_ids[-2]}
        assert all(b["exists_locally"] is True for b in listed)


def test_backup_telegram_upload_and_failure_resilience(tmp_path: Path, monkeypatch):
    settings = _make_settings(tmp_path, storage_mode="drives")
    settings.telegram_archive_chat_id = "-100999888777"
    app = create_app(settings)

    created_topics: list[tuple[str, dict]] = []
    sent_calls: list[dict] = []

    class FakeResp:
        def json(self):
            return {"ok": True, "result": {"message_thread_id": 4242}}

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, json=None):
            created_topics.append((url, json))
            return FakeResp()

    monkeypatch.setattr("app.telegram_mtproto.httpx.AsyncClient", FakeAsyncClient)

    class FakeTgClient:
        def __init__(self, should_fail: bool = False):
            self.should_fail = should_fail

        def is_connected(self) -> bool:
            return True

        async def disconnect(self) -> None:
            pass

        async def get_input_entity(self, target):
            return target

        async def send_file(self, peer, file_arg, **kwargs):
            if self.should_fail:
                raise RuntimeError("Simulated Telegram upload failure")
            sent_calls.append({"peer": peer, "file": file_arg, **kwargs})
            return SimpleNamespace(id=7001)

    fake_cfg = TelegramApiConfig(name="bk_worker", api_id=123, api_hash="hash", bot_token="123:ABC")
    fake_client = FakeTgClient(should_fail=False)
    app.state.tg_pool.clients = [(fake_cfg, fake_client)]

    with TestClient(app) as client:
        _insert_tagged_media(app.state.db, "tg_bk_item", "backup")

        resp = client.post(
            "/api/system/backups",
            json={"upload_to_telegram": True, "note": "offsite"},
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()
        assert data["exists_locally"] is True
        assert Path(data["file_path"]).is_file()
        assert data["telegram_chat_id"] == "-100999888777"
        assert data["telegram_topic_id"] == 4242
        assert data["telegram_msg_id"] == 7001
        assert data["telegram_worker_name"] == "bk_worker"
        assert "telegram_error" not in data

        assert len(created_topics) == 1
        assert created_topics[0][1]["name"] == "_VaultBackups"
        assert len(sent_calls) == 1
        assert sent_calls[0]["reply_to"] == 4242
        assert sent_calls[0]["force_document"] is True

        # Failure resilience: when Telegram upload raises, local backup still succeeds and surfaces telegram_error
        fake_client.should_fail = True
        fail_resp = client.post(
            "/api/system/backups",
            json={"upload_to_telegram": True, "note": "resilient"},
        )
        assert fail_resp.status_code == 201, fail_resp.text
        fail_data = fail_resp.json()
        assert fail_data["exists_locally"] is True
        assert Path(fail_data["file_path"]).is_file()
        assert fail_data["telegram_msg_id"] is None
        assert "Simulated Telegram upload failure" in fail_data.get("telegram_error", "")
        assert app.state.db.get_backup_record(fail_data["id"]) is not None


def test_safe_restore_wal_cleanup_pre_restore_snapshot_corrupt_and_cli(tmp_path: Path):
    db_path = tmp_path / "vault" / "media.db"
    db = Database(db_path)

    # Insert item A and create compressed SQLite backup
    _insert_tagged_media(db, "item_a", "alpha")
    raw_snap = tmp_path / "snap.db"
    db.backup_to(raw_snap)
    backup_gz = tmp_path / "vault_backup_snap.db.gz"
    with raw_snap.open("rb") as src, gzip.open(backup_gz, "wb") as dst:
        dst.write(src.read())

    # Insert item B in WAL mode and capture uncheckpointed -wal/-shm bytes before closing
    _insert_tagged_media(db, "item_b", "beta")
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")
    assert wal_path.is_file() and wal_path.stat().st_size > 0
    saved_wal = wal_path.read_bytes()
    saved_shm = shm_path.read_bytes() if shm_path.is_file() else b""
    db.close()

    # Re-create stale -wal/-shm sidecars (simulating crash before checkpoint)
    wal_path.write_bytes(saved_wal)
    shm_path.write_bytes(saved_shm)

    result = restore_backup_file(backup_gz, db_path)
    assert result["restored"] is True
    assert result["pre_restore_backup"] is not None
    pre_bak = Path(result["pre_restore_backup"])
    assert pre_bak.is_file()
    # Stale WAL frames must be cleared (read-only post-restore check may leave an empty 0-byte WAL file)
    assert (not wal_path.exists()) or wal_path.stat().st_size == 0

    with sqlite3.connect(db_path) as conn:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        assert str(integrity).lower() == "ok"
        ids = {r[0] for r in conn.execute("SELECT id FROM media_items").fetchall()}
    assert ids == {"item_a"}

    # Corrupt gzip and non-SQLite rejections without touching existing DB
    bad_gz = tmp_path / "corrupt.db.gz"
    bad_gz.write_bytes(b"not-a-valid-gzip-payload")
    with pytest.raises(ValueError):
        verify_backup_file(bad_gz)
    with pytest.raises(ValueError):
        restore_backup_file(bad_gz, db_path)

    non_sqlite_gz = tmp_path / "non_sqlite.db.gz"
    with gzip.open(non_sqlite_gz, "wb") as gz:
        gz.write(b"plain text is definitely not a sqlite database")
    with pytest.raises(ValueError):
        verify_backup_file(non_sqlite_gz)
    with pytest.raises(ValueError):
        restore_backup_file(non_sqlite_gz, db_path)

    missing_tables_db = tmp_path / "missing_tables.db"
    with sqlite3.connect(missing_tables_db) as conn:
        conn.execute("CREATE TABLE unrelated (id TEXT PRIMARY KEY)")
    with pytest.raises(ValueError):
        verify_backup_file(missing_tables_db)

    # Confirm existing DB is still intact with item_a
    with sqlite3.connect(db_path) as conn:
        ids_after_bad = {r[0] for r in conn.execute("SELECT id FROM media_items").fetchall()}
    assert ids_after_bad == {"item_a"}

    # Mutate DB with item C, then test scripts/restore_vault.py CLI
    db2 = Database(db_path)
    _insert_tagged_media(db2, "item_c", "gamma")
    db2.close()

    repo_root = Path(__file__).resolve().parent.parent
    script_path = repo_root / "scripts" / "restore_vault.py"

    # --verify-only must exit 0 and leave DB untouched (item_c still present)
    res_verify = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--backup",
            str(backup_gz),
            "--db-path",
            str(db_path),
            "--verify-only",
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_verify.returncode == 0, res_verify.stderr
    assert "verify-only" in res_verify.stdout
    with sqlite3.connect(db_path) as conn:
        ids_verify = {r[0] for r in conn.execute("SELECT id FROM media_items").fetchall()}
    assert ids_verify == {"item_a", "item_c"}

    # --yes must exit 0 and restore DB to item_a only
    res_restore = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--backup",
            str(backup_gz),
            "--db-path",
            str(db_path),
            "--yes",
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_restore.returncode == 0, res_restore.stderr
    assert "RESTORED" in res_restore.stdout
    with sqlite3.connect(db_path) as conn:
        ids_restored = {r[0] for r in conn.execute("SELECT id FROM media_items").fetchall()}
    assert ids_restored == {"item_a"}


def test_legacy_db_migration_creates_backup_and_maintenance_tables(tmp_path: Path):
    db_path = tmp_path / "pre_track1_legacy.db"
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
            CREATE TABLE ingest_jobs (
                id TEXT PRIMARY KEY,
                source_origin TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                media_ids_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE folder_topics (
                chat_id TEXT NOT NULL,
                folder_name TEXT NOT NULL,
                topic_id INTEGER NOT NULL,
                PRIMARY KEY (chat_id, folder_name)
            );
            INSERT INTO media_items (
                id, title, filename, folder, tags, media_type, mime_type, size_bytes,
                drive_name, file_path, thumb_filename, source_origin, source_kind, source_url, created_at
            ) VALUES (
                'pre_row_1', 'Legacy Shot', 'shot.jpg', 'Old', 'retro', 'image', 'image/jpeg', 2048,
                'drive1', '', NULL, 'web', 'upload', NULL, '2026-01-01T00:00:00Z'
            );
            """
        )

    db = Database(db_path)
    try:
        with db._connect() as conn:
            tables = {
                r["name"]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "vault_backups" in tables
        assert "maintenance_runs" in tables

        existing = db.get_media("pre_row_1")
        assert existing is not None
        assert existing["title"] == "Legacy Shot"
        assert existing["tags"] == ["retro"]

        # Verify new Track 1 DB operations work on migrated DB
        bk = db.insert_backup_record(
            {
                "id": "bk_migrated",
                "filename": "vault_backup_20260101T000000Z_bk_migrated.db.gz",
                "file_path": str(tmp_path / "bk.db.gz"),
                "size_bytes": 128,
                "uncompressed_bytes": 512,
                "sha256": "a" * 64,
            }
        )
        assert bk["id"] == "bk_migrated"

        run = db.create_maintenance_run("audit", status="done", message="ok")
        assert run["id"]
    finally:
        db.close()

    # Re-opening must remain idempotent and preserve existing rows
    db_reopen = Database(db_path)
    try:
        assert db_reopen.get_media("pre_row_1") is not None
        assert db_reopen.get_backup_record("bk_migrated") is not None
        assert db_reopen.get_latest_maintenance_run("audit") is not None
    finally:
        db_reopen.close()


def test_restore_over_corrupted_db_and_delete_by_filename_and_symlink_rejection(tmp_path: Path):
    # 1. Restoring over a corrupted live DB falls back to raw file copy + WAL/SHM preservation
    db_path = tmp_path / "corrupt_target" / "media.db"
    db = Database(db_path)
    _insert_tagged_media(db, "good_item", "safe")
    raw_snap = tmp_path / "good_snap.db"
    db.backup_to(raw_snap)
    db.close()

    backup_gz = tmp_path / "good_snap.db.gz"
    with raw_snap.open("rb") as src, gzip.open(backup_gz, "wb") as dst:
        dst.write(src.read())

    # Corrupt live DB and write a fake WAL sidecar
    corrupt_bytes = b"CORRUPTED_NOT_SQLITE_HEADER_BYTES_1234567890"
    db_path.write_bytes(corrupt_bytes)
    Path(f"{db_path}-wal").write_bytes(b"corrupt-wal-bytes")

    res = restore_backup_file(backup_gz, db_path)
    assert res["restored"] is True
    assert "warning" in res
    pre_bak = Path(res["pre_restore_backup"])
    assert pre_bak.read_bytes() == corrupt_bytes
    assert Path(f"{pre_bak}-wal").read_bytes() == b"corrupt-wal-bytes"

    with sqlite3.connect(db_path) as conn:
        assert str(conn.execute("PRAGMA integrity_check").fetchone()[0]).lower() == "ok"
        ids = {r[0] for r in conn.execute("SELECT id FROM media_items").fetchall()}
    assert ids == {"good_item"}

    # 2. Deleting backup by filename cleans DB row; symlinks and hidden files are rejected
    settings = _make_settings(tmp_path / "api_hardening")
    app = create_app(settings)
    with TestClient(app) as client:
        created = client.post("/api/system/backups", json={"upload_to_telegram": False}).json()
        bid = created["id"]
        fname = created["filename"]
        assert app.state.db.get_backup_record(bid) is not None

        # Delete via filename instead of ID -> removes both file and DB row
        del_resp = client.delete(f"/api/system/backups/{fname}")
        assert del_resp.status_code == 200
        assert not Path(created["file_path"]).exists()
        assert app.state.db.get_backup_record(bid) is None

        # Symlink & hidden files inside backups_dir must not be listed or downloadable
        outside = tmp_path / "outside_secret.db.gz"
        outside.write_bytes(b"secret")
        sym = settings.backups_dir / "linked.db.gz"
        sym.symlink_to(outside)
        hidden = settings.backups_dir / ".tmp_secret.db.gz"
        hidden.write_bytes(b"hidden")
        non_backup = settings.backups_dir / "notes.txt"
        non_backup.write_text("not a backup", encoding="utf-8")

        listed_names = {b["filename"] for b in client.get("/api/system/backups").json()["backups"]}
        assert "linked.db.gz" not in listed_names
        assert ".tmp_secret.db.gz" not in listed_names
        assert "notes.txt" not in listed_names

        assert client.get("/api/system/backups/linked.db.gz/download").status_code == 404
        assert client.get("/api/system/backups/.tmp_secret.db.gz/download").status_code == 404
        assert client.get("/api/system/backups/notes.txt/download").status_code == 404

