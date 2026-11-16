from __future__ import annotations

import asyncio
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

from app.config import DriveConfig, Settings


@dataclass
class DriveStatus:
    name: str
    path: str
    mounted: bool
    total_bytes: int = 0
    used_bytes: int = 0
    free_bytes: int = 0


def fmt_size(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} PB"


def sanitize_folder(folder: str | None, default: str = "General") -> str:
    if not folder:
        return default
    parts = []
    for raw in folder.replace("\\", "/").split("/"):
        part = raw.strip().lstrip("#")
        if not part or part in (".", ".."):
            continue
        cleaned = "".join(ch for ch in part if ch.isalnum() or ch in (" ", "-", "_", "."))
        cleaned = cleaned.strip(" .")
        if cleaned:
            parts.append(cleaned[:64])
    return "/".join(parts) if parts else default


class DriveAllocator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = asyncio.Lock()

    def get_drive_statuses(self) -> list[DriveStatus]:
        statuses: list[DriveStatus] = []
        for drive in self.settings.get_drives():
            path = Path(drive.path)
            # Check mount point or parent if subfolder isn't created yet
            probe = path if path.exists() else path.parent
            if not probe.exists():
                statuses.append(DriveStatus(name=drive.name, path=drive.path, mounted=False))
                continue
            try:
                usage = shutil.disk_usage(probe)
                statuses.append(
                    DriveStatus(
                        name=drive.name,
                        path=drive.path,
                        mounted=True,
                        total_bytes=usage.total,
                        used_bytes=usage.used,
                        free_bytes=usage.free,
                    )
                )
            except OSError:
                statuses.append(DriveStatus(name=drive.name, path=drive.path, mounted=False))
        return statuses

    def _pick_drive(self, file_size: int) -> tuple[DriveConfig, int] | None:
        buffer_bytes = self.settings.min_free_buffer_bytes
        candidates: list[tuple[DriveConfig, int]] = []

        for drive in self.settings.get_drives():
            path = Path(drive.path)
            probe = path if path.exists() else path.parent
            if not probe.exists():
                continue
            try:
                free = shutil.disk_usage(probe).free
            except OSError:
                continue
            if free >= file_size + buffer_bytes:
                candidates.append((drive, free))

        if not candidates:
            return None

        if self.settings.allocation_strategy == "most_free":
            return max(candidates, key=lambda item: item[1])
        return candidates[0]

    async def allocate_and_move(
        self, local_path: Path, folder: str | None = None
    ) -> tuple[str, Path, int]:
        """Pick a drive with sufficient space and move the staged file into its folder."""
        size = local_path.stat().st_size
        safe_folder = sanitize_folder(folder, self.settings.default_folder)

        async with self._lock:
            choice = self._pick_drive(size)
            if not choice:
                needed = fmt_size(size + self.settings.min_free_buffer_bytes)
                raise RuntimeError(f"No mounted drive has at least {needed} free.")

            drive, free_before = choice
            dest_dir = Path(drive.path) / safe_folder
            dest_dir.mkdir(parents=True, exist_ok=True)

            target = dest_dir / local_path.name
            if target.exists():
                target = dest_dir / f"{local_path.stem}_{uuid.uuid4().hex[:6]}{local_path.suffix}"

            await asyncio.to_thread(shutil.move, str(local_path), str(target))
            free_after = max(0, free_before - size)
            return drive.name, target, free_after
