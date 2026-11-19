from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.allocator import DriveAllocator
from app.config import Settings
from app.db import Database
from app.ingest import IngestService
from app.telegram_mtproto import TelegramPool

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


@dataclass
class AppContext:
    settings: Settings
    db: Database
    allocator: DriveAllocator
    ingest: IngestService
    tg_pool: TelegramPool
    zip_cache: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    backup: Any = None
    integrity: Any = None
