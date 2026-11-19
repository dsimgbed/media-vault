from __future__ import annotations

from pathlib import Path

from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from app.config import TelegramApiConfig
from app.context import AppContext
from app.main import create_app
from tests.helpers import _make_settings


def test_worker_telemetry_block_cache_health_and_system_status(tmp_path: Path):
    settings = _make_settings(tmp_path, auth_token="sys-secret", storage_mode="drives")
    settings.telegram_archive_chat_id = "-100444555"
    settings.telegram_stream_cache_mb = 1
    app = create_app(settings)

    class FakeWorkerClient:
        def __init__(self, connected: bool):
            self._connected = connected

        def is_connected(self) -> bool:
            return self._connected

        async def disconnect(self) -> None:
            pass

    pool = app.state.tg_pool
    c_online = FakeWorkerClient(connected=True)
    c_cooldown = FakeWorkerClient(connected=False)
    pool.clients = [
        (TelegramApiConfig(name="worker_online", api_id=101, api_hash="h1", bot_token="t1"), c_online),
        (TelegramApiConfig(name="worker_cooldown", api_id=102, api_hash="h2", bot_token="t2"), c_cooldown),
    ]
    pool._worker_meta["worker_online"] = {"username": "bot_one"}
    pool._worker_meta["worker_cooldown"] = {"username": "bot_two"}
    pool._active_uploads["worker_online"] = 2
    pool._active_streams["worker_online"] = 3
    pool._record_flood_wait("worker_cooldown", 45)
    pool._record_worker_error("worker_cooldown", "SENSITIVE_INTERNAL_ERR_DO_NOT_LEAK")

    # Populate block cache with hits, misses, and evictions
    cache = pool._block_cache
    cache.configure(max_bytes=100, block_size=4096)
    cache.put(("ns", 1), 0, b"a" * 60)
    assert cache.get_span(("ns", 1), 0, 10) == b"a" * 11  # hit
    assert cache.get_span(("ns", 2), 0, 10) is None       # miss
    cache.put(("ns", 1), 60, b"b" * 60)                   # evicts offset 0 entry

    with TestClient(app) as client:
        auth = {"Authorization": "Bearer sys-secret"}

        # Create a backup so last_backup_at & backup path exist
        bk_resp = client.post("/api/system/backups", json={"note": "telemetry"}, headers=auth)
        assert bk_resp.status_code == 201, bk_resp.text
        bk_data = bk_resp.json()

        # 1. Unauthenticated /api/health must work, retain all legacy fields + summary counters,
        # and MUST NOT leak worker error strings or backup file paths
        health_resp = client.get("/api/health")
        assert health_resp.status_code == 200
        health = health_resp.json()

        legacy_keys = {
            "ok",
            "storage_mode",
            "telegram_workers",
            "telegram_archive_enabled",
            "default_folder",
            "auth_required",
            "max_upload_mb",
            "telegram_part_size_mb",
            "ingest_concurrency",
            "ytdlp_cookies_configured",
            "ytdlp_version",
        }
        summary_keys = {
            "telegram_workers_online",
            "telegram_workers_in_cooldown",
            "block_cache_utilization_pct",
            "last_backup_at",
        }
        assert legacy_keys.issubset(health.keys())
        assert summary_keys.issubset(health.keys())

        assert health["ok"] is True
        assert health["storage_mode"] == "drives"
        assert health["auth_required"] is True
        assert health["telegram_workers"] == 2
        assert health["telegram_workers_online"] == 1
        assert health["telegram_workers_in_cooldown"] == 1
        assert health["telegram_archive_enabled"] is True
        assert health["block_cache_utilization_pct"] == 60.0
        assert health["last_backup_at"] == bk_data["created_at"]

        assert "SENSITIVE_INTERNAL_ERR_DO_NOT_LEAK" not in health_resp.text
        assert str(settings.backups_dir) not in health_resp.text
        assert str(settings.db_path) not in health_resp.text
        assert "workers" not in health

        # 2. /api/system/status enforces 401 without auth token
        assert client.get("/api/system/status").status_code == 401

        # 3. Authenticated /api/system/status returns full telemetry & subsystem summaries
        status_resp = client.get("/api/system/status", headers=auth)
        assert status_resp.status_code == 200
        status = status_resp.json()

        assert status["ok"] is True
        assert status["storage_mode"] == "drives"

        tg = status["telegram"]
        assert tg["worker_count"] == 2
        assert tg["online_count"] == 1
        assert tg["cooldown_count"] == 1
        assert len(tg["workers"]) == 2

        by_name = {w["name"]: w for w in tg["workers"]}
        w_on = by_name["worker_online"]
        assert set(w_on.keys()) >= {
            "name",
            "online",
            "active_uploads",
            "active_streams",
            "in_cooldown",
            "cooldown_remaining_sec",
            "flood_wait_count",
            "last_error",
        }
        assert w_on["online"] is True
        assert w_on["active_uploads"] == 2
        assert w_on["active_streams"] == 3
        assert w_on["in_cooldown"] is False
        assert w_on["cooldown_remaining_sec"] == 0.0
        assert w_on["flood_wait_count"] == 0
        assert w_on["last_error"] is None

        w_cd = by_name["worker_cooldown"]
        assert w_cd["online"] is False
        assert w_cd["in_cooldown"] is True
        assert w_cd["cooldown_remaining_sec"] > 0.0
        assert w_cd["flood_wait_count"] == 1
        assert w_cd["last_error"] == "SENSITIVE_INTERNAL_ERR_DO_NOT_LEAK"

        bc = tg["block_cache"]
        assert set(bc.keys()) >= {
            "used_bytes",
            "max_bytes",
            "utilization_pct",
            "hits",
            "misses",
            "evictions",
            "hit_rate_pct",
        }
        assert bc["used_bytes"] == 60
        assert bc["max_bytes"] == 100
        assert bc["utilization_pct"] == 60.0
        assert bc["hits"] == 1
        assert bc["misses"] == 1
        assert bc["evictions"] == 1
        assert bc["hit_rate_pct"] == 50.0

        assert "db_path" in status["database"]
        assert status["database"]["backups_count"] == 1
        assert "thumbs_count" in status["storage"]
        assert status["backups"]["count"] == 1
        assert status["backups"]["latest"]["id"] == bk_data["id"]
        assert "busy" in status["integrity"]
        assert status["ingest"]["concurrency"] == settings.ingest_concurrency


def test_app_context_backwards_compat_and_openapi_routing(tmp_path: Path):
    settings = _make_settings(tmp_path, storage_mode="drives")
    app = create_app(settings)

    # Verify app.state.ctx mirrors all legacy app.state attributes
    ctx = app.state.ctx
    assert isinstance(ctx, AppContext)
    assert ctx.settings is app.state.settings
    assert ctx.db is app.state.db
    assert ctx.allocator is app.state.allocator
    assert ctx.ingest is app.state.ingest
    assert ctx.tg_pool is app.state.tg_pool
    assert ctx.zip_cache is app.state.zip_cache
    assert ctx.backup is app.state.backup
    assert ctx.integrity is app.state.integrity

    with TestClient(app) as client:
        openapi_resp = client.get("/openapi.json")
        assert openapi_resp.status_code == 200
        paths = openapi_resp.json()["paths"]

        expected_routes = {
            "/api/health": {"get"},
            "/api/system/status": {"get"},
            "/api/drives": {"get"},
            "/api/folders": {"get"},
            "/api/tags": {"get"},
            "/api/ytdlp/update": {"post"},
            "/api/ytdlp/formats": {"post"},
            "/api/ytdlp/playlist": {"post"},
            "/api/ytdlp/cookies": {"post", "delete"},
            "/api/system/backups": {"get", "post"},
            "/api/system/backups/{backup_id}/download": {"get"},
            "/api/system/backups/{backup_id}": {"delete"},
            "/api/system/integrity/status": {"get"},
            "/api/system/integrity/runs": {"get"},
            "/api/system/integrity/runs/{run_id}": {"get"},
            "/api/system/integrity/audit": {"post"},
            "/api/system/integrity/repair": {"post"},
            "/api/jobs": {"get"},
            "/api/jobs/{job_id}": {"get"},
            "/api/jobs/{job_id}/cancel": {"post"},
            "/api/jobs/{job_id}/retry": {"post"},
            "/api/events": {"get"},
            "/api/ingest/check-duplicate": {"post"},
            "/api/ingest/url": {"post"},
            "/api/ingest/upload": {"post"},
            "/api/ingest/upload-folder": {"post"},
            "/api/media": {"get"},
            "/api/media/bulk/delete": {"post"},
            "/api/media/bulk/move": {"post"},
            "/api/media/bulk/tags": {"post"},
            "/api/media/bulk/download": {"get", "post"},
            "/api/media/{media_id}": {"get", "patch", "delete"},
            "/api/media/{media_id}/tags": {"patch"},
            "/api/media/{media_id}/thumb": {"get"},
            "/api/media/{media_id}/preview": {"get"},
            "/api/media/{media_id}/stream": {"get"},
            "/api/media/{media_id}/zip/entries": {"get"},
            "/api/media/{media_id}/zip/stream": {"get"},
        }
        for route_path, methods in expected_routes.items():
            assert route_path in paths, f"Missing OpenAPI route: {route_path}"
            actual_methods = {m.lower() for m in paths[route_path].keys()}
            assert methods.issubset(actual_methods), (
                f"Route {route_path} missing methods {methods - actual_methods}"
            )

        # Verify bulk routes are registered ahead of /api/media/{media_id}
        def _iter_api_routes(routes):
            for r in routes:
                orig = getattr(r, "original_router", None)
                if orig is not None:
                    yield from _iter_api_routes(orig.routes)
                    continue
                nested = getattr(r, "routes", None)
                if nested is not None and not isinstance(r, APIRoute):
                    yield from _iter_api_routes(nested)
                elif isinstance(r, APIRoute):
                    yield r

        route_paths = [r.path for r in _iter_api_routes(app.router.routes)]
        first_media_id_idx = route_paths.index("/api/media/{media_id}")
        for bulk_path in (
            "/api/media/bulk/delete",
            "/api/media/bulk/move",
            "/api/media/bulk/tags",
            "/api/media/bulk/download",
        ):
            assert route_paths.index(bulk_path) < first_media_id_idx, (
                f"{bulk_path} must precede /api/media/{{media_id}}"
            )

        # Runtime check that bulk endpoints dispatch properly instead of matching {media_id}
        bulk_del = client.post("/api/media/bulk/delete", json={"media_ids": ["missing"]})
        assert bulk_del.status_code == 200
        assert bulk_del.json() == {"deleted": 0, "ids": []}

        bulk_move = client.post("/api/media/bulk/move", json={"media_ids": ["missing"], "folder": "Archive"})
        assert bulk_move.status_code == 200
        assert bulk_move.json()["moved"] == 0

        bulk_tags = client.post("/api/media/bulk/tags", json={"media_ids": ["missing"], "tags": "tag1"})
        assert bulk_tags.status_code == 200
        assert bulk_tags.json()["updated"] == 0

        bulk_dl = client.get("/api/media/bulk/download", params={"ids": "missing"})
        assert bulk_dl.status_code == 404
        assert "Media item not found" not in bulk_dl.text

        bulk_dl_post = client.post("/api/media/bulk/download", json={"media_ids": ["missing"]})
        assert bulk_dl_post.status_code == 404
        assert "Media item not found" not in bulk_dl_post.text
