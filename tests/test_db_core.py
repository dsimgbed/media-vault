from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from tests.helpers import _make_settings, _ui_source


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
        html = _ui_source(client)
        assert "folderUploadInput" in html
        assert "Unified Stream" in html
        health = client.get("/api/health").json()
        assert health["max_upload_mb"] == settings.max_upload_mb
        assert health["telegram_part_size_mb"] == settings.telegram_part_size_mb
        assert health["ingest_concurrency"] == settings.ingest_concurrency


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
