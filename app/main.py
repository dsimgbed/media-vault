from __future__ import annotations

import logging
import mimetypes
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

from app.allocator import DriveAllocator
from app.backup import BackupService
from app.config import Settings
from app.context import STATIC_DIR, AppContext
from app.db import Database
from app.ingest import IngestService, update_ytdlp
from app.integrity import IntegrityService
from app.routers import ingest as ingest_router
from app.routers import jobs as jobs_router
from app.routers import maintenance as maintenance_router
from app.routers import media as media_router
from app.routers import pwa as pwa_router
from app.routers import system as system_router
from app.routers import zip_explorer as zip_explorer_router
from app.telegram_mtproto import TelegramPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

mimetypes.add_type("text/javascript", ".js")
mimetypes.add_type("text/javascript", ".mjs")
mimetypes.add_type("application/manifest+json", ".webmanifest")
mimetypes.add_type("image/svg+xml", ".svg")


class RevalidatingStaticFiles(StaticFiles):
    def file_response(self, *args: Any, **kwargs: Any) -> Response:
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "no-cache"
        return response


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or Settings()
    cfg.ensure_dirs()

    db = Database(cfg.db_path)
    allocator = DriveAllocator(cfg)
    ingest = IngestService(cfg, db, allocator)
    tg_pool = TelegramPool(cfg, db, allocator, ingest)
    backup = BackupService(cfg, db, tg_pool)
    integrity = IntegrityService(cfg, db, ingest, tg_pool)
    zip_cache: dict[str, list[dict[str, Any]]] = {}
    ctx = AppContext(
        settings=cfg,
        db=db,
        allocator=allocator,
        ingest=ingest,
        tg_pool=tg_pool,
        zip_cache=zip_cache,
        backup=backup,
        integrity=integrity,
    )
    ingest.recover_on_startup()
    db.recover_interrupted_maintenance_runs()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        ingest.recover_on_startup()
        db.recover_interrupted_maintenance_runs()
        await tg_pool.start()
        backup.start_scheduler()
        try:
            yield
        finally:
            await backup.stop_scheduler()
            await integrity.shutdown()
            await ingest.shutdown()
            await tg_pool.stop()
            db.close()

    app = FastAPI(title="Media Ingest & Gallery", lifespan=lifespan)
    app.state.ctx = ctx
    app.state.settings = cfg
    app.state.db = db
    app.state.allocator = allocator
    app.state.ingest = ingest
    app.state.tg_pool = tg_pool
    app.state.zip_cache = zip_cache
    app.state.backup = backup
    app.state.integrity = integrity

    app.include_router(system_router.router)
    app.include_router(maintenance_router.router)
    app.include_router(jobs_router.router)
    app.include_router(ingest_router.router)
    app.include_router(media_router.router)
    app.include_router(zip_explorer_router.router)

    if STATIC_DIR.exists():
        app.mount("/static", RevalidatingStaticFiles(directory=str(STATIC_DIR)), name="static")

    app.include_router(pwa_router.router)

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    s = app.state.settings
    uvicorn.run(app, host=s.host, port=s.port)
