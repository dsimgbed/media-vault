from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from telethon.errors import FloodWaitError

from app.config import Settings
from app.db import Database
from app.ingest import IngestService
from app.telegram_mtproto import TelegramPool
from app.thumbnails import extract_media_metadata, generate_thumbnail, generate_video_preview

logger = logging.getLogger(__name__)

_THUMB_FALLBACK_EXTS = (".webp", ".jpg", ".jpeg", ".png")
_REPAIR_DETAIL_LIMIT = 200


def _is_positive_number(val: Any) -> bool:
    if val is None or isinstance(val, bool):
        return False
    try:
        return float(val) > 0.0
    except (TypeError, ValueError):
        return False


def _safe_basename(raw_name: Any) -> str | None:
    if not raw_name:
        return None
    name = Path(str(raw_name).strip()).name
    if not name or name in (".", ".."):
        return None
    return name


def _safe_child_file(base_dir: Path, raw_name: Any) -> Path | None:
    name = _safe_basename(raw_name)
    if not name:
        return None
    candidate = base_dir / name
    try:
        if candidate.is_symlink() or not candidate.is_file():
            return None
        if not candidate.resolve().is_relative_to(base_dir.resolve()):
            return None
    except OSError:
        return None
    return candidate


def _find_existing_thumb(thumbs_dir: Path, item: dict[str, Any]) -> Path | None:
    if not thumbs_dir.exists():
        return None
    candidates: list[str] = []
    if item.get("thumb_filename"):
        name = _safe_basename(item["thumb_filename"])
        if name:
            candidates.append(name)
    media_id = _safe_basename(item.get("id"))
    if media_id:
        for ext in _THUMB_FALLBACK_EXTS:
            fname = f"{media_id}{ext}"
            if fname not in candidates:
                candidates.append(fname)
    for fname in candidates:
        found = _safe_child_file(thumbs_dir, fname)
        if found is not None:
            return found
    return None


def _find_existing_preview(thumbs_dir: Path, item: dict[str, Any]) -> Path | None:
    if not thumbs_dir.exists():
        return None
    candidates: list[str] = []
    if item.get("preview_filename"):
        name = _safe_basename(item["preview_filename"])
        if name:
            candidates.append(name)
    media_id = _safe_basename(item.get("id"))
    if media_id:
        fallback = f"{media_id}_preview.webp"
        if fallback not in candidates:
            candidates.append(fallback)
    for fname in candidates:
        found = _safe_child_file(thumbs_dir, fname)
        if found is not None:
            return found
    return None


def _missing_metadata_fields(item: dict[str, Any]) -> list[str]:
    media_type = str(item.get("media_type") or "").lower()
    if media_type == "image":
        required = ("width", "height")
    elif media_type == "video":
        required = ("duration_sec", "width", "height")
    elif media_type == "audio":
        required = ("duration_sec",)
    else:
        return []
    return [field for field in required if not _is_positive_number(item.get(field))]


def _collect_referenced_thumb_names(items: list[dict[str, Any]]) -> set[str]:
    referenced: set[str] = set()
    for item in items:
        for col in ("thumb_filename", "preview_filename"):
            name = _safe_basename(item.get(col))
            if name:
                referenced.add(name)
        media_id = _safe_basename(item.get("id"))
        if media_id:
            for ext in _THUMB_FALLBACK_EXTS:
                referenced.add(f"{media_id}{ext}")
            referenced.add(f"{media_id}_preview.webp")
    return referenced


def _is_active_temp_thumb(name: str, mtime: float, now: float, grace_sec: int) -> bool:
    is_tmp = name.endswith(".tmp.png") or ("_pframe_" in name and name.endswith(".png"))
    if not is_tmp:
        return False
    return mtime > (now - max(60, grace_sec))


def _is_path_active(entry_resolved: Path, active_dirs: set[Path]) -> bool:
    for active in active_dirs:
        if entry_resolved == active:
            return True
        if active.is_relative_to(entry_resolved) or entry_resolved.is_relative_to(active):
            return True
    return False


def _measure_entry(entry: Path) -> tuple[bool, int, float]:
    st = entry.lstat()
    latest_mtime = float(st.st_mtime)
    is_dir = entry.is_dir() and not entry.is_symlink()
    if not is_dir:
        size = int(st.st_size) if (entry.is_file() and not entry.is_symlink()) else 0
        return False, size, latest_mtime

    total_bytes = 0
    stack: list[Path] = [entry]
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for child in it:
                    try:
                        cst = child.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    if cst.st_mtime > latest_mtime:
                        latest_mtime = float(cst.st_mtime)
                    try:
                        if child.is_symlink():
                            continue
                        if child.is_dir(follow_symlinks=False):
                            stack.append(Path(child.path))
                        elif child.is_file(follow_symlinks=False):
                            total_bytes += int(cst.st_size)
                    except OSError:
                        continue
        except OSError:
            continue
    return True, total_bytes, latest_mtime


def _has_telegram_fallback(item: dict[str, Any]) -> bool:
    parts = item.get("parts") or []
    if any(p.get("telegram_chat_id") and p.get("telegram_msg_id") for p in parts):
        return True
    return bool(item.get("telegram_chat_id") and item.get("telegram_msg_id"))


def _iter_item_telegram_refs(item: dict[str, Any]) -> list[tuple[str, int, int]]:
    refs: list[tuple[str, int, int]] = []
    parts = item.get("parts") or []
    if parts:
        for p in parts:
            chat_id = p.get("telegram_chat_id")
            msg_id = p.get("telegram_msg_id")
            if chat_id and msg_id:
                try:
                    refs.append((str(chat_id), int(msg_id), int(p.get("part_index") or 0)))
                except (TypeError, ValueError):
                    continue
        return refs
    chat_id = item.get("telegram_chat_id")
    msg_id = item.get("telegram_msg_id")
    if chat_id and msg_id:
        try:
            refs.append((str(chat_id), int(msg_id), 0))
        except (TypeError, ValueError):
            pass
    return refs


class IntegrityService:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        ingest: IngestService,
        tg_pool: TelegramPool,
    ) -> None:
        self.settings = settings
        self.db = db
        self.ingest = ingest
        self.tg_pool = tg_pool
        self._lock = asyncio.Lock()
        self._active_task: asyncio.Task[Any] | None = None
        self._active_run_id: str | None = None
        self._shutting_down = False

    def is_busy(self) -> bool:
        return bool(
            self._lock.locked()
            or (self._active_task is not None and not self._active_task.done())
        )

    async def shutdown(self) -> None:
        self._shutting_down = True
        run_id = self._active_run_id
        task = self._active_task
        try:
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            if run_id:
                run = self.db.get_maintenance_run(run_id)
                if run and run.get("kind") == "repair" and run.get("status") == "running":
                    self.db.update_maintenance_run(
                        run_id,
                        status="failed",
                        message="Interrupted by server shutdown",
                    )
        finally:
            self._active_task = None
            if self._active_run_id == run_id:
                self._active_run_id = None
            self._shutting_down = False

    def get_status(self) -> dict[str, Any]:
        return {
            "busy": self.is_busy(),
            "active_run_id": self._active_run_id,
            "active_run": (
                self.db.get_maintenance_run(self._active_run_id)
                if self._active_run_id
                else None
            ),
            "latest_audit": self.db.get_latest_maintenance_run("audit"),
            "latest_repair": self.db.get_latest_maintenance_run("repair"),
        }

    def get_storage_footprint(self) -> dict[str, Any]:
        thumbs_count = 0
        thumbs_bytes = 0
        thumbs_dir = self.settings.thumbs_dir
        if thumbs_dir.exists():
            try:
                for entry in thumbs_dir.iterdir():
                    try:
                        if entry.is_symlink() or not entry.is_file():
                            continue
                        thumbs_count += 1
                        thumbs_bytes += int(entry.stat().st_size)
                    except OSError:
                        continue
            except OSError:
                pass

        staging_entries = 0
        staging_bytes = 0
        staging_dir = self.settings.staging_dir
        if staging_dir.exists():
            try:
                for entry in staging_dir.iterdir():
                    try:
                        staging_entries += 1
                        _, entry_bytes, _ = _measure_entry(entry)
                        staging_bytes += entry_bytes
                    except OSError:
                        continue
            except OSError:
                pass

        backups_count = 0
        backups_bytes = 0
        backups_dir = self.settings.backups_dir
        if backups_dir.exists():
            try:
                for entry in backups_dir.iterdir():
                    try:
                        if entry.is_symlink() or not entry.is_file():
                            continue
                        backups_count += 1
                        backups_bytes += int(entry.stat().st_size)
                    except OSError:
                        continue
            except OSError:
                pass

        stream_cache_entries = 0
        stream_cache_bytes = 0
        if (
            hasattr(self.tg_pool, "_block_cache")
            and hasattr(self.tg_pool._block_cache, "get_disk_footprint")
            and self.settings.telegram_disk_cache_bytes > 0
        ):
            entries, bytes_ = self.tg_pool._block_cache.get_disk_footprint()
            stream_cache_entries = int(entries)
            stream_cache_bytes = int(bytes_)
        else:
            stream_cache_dir = self.settings.stream_cache_dir
            if stream_cache_dir.exists():
                try:
                    for root, _dirs, files in os.walk(stream_cache_dir):
                        for fname in files:
                            if not fname.endswith(".blk") or fname.startswith(".tmp_"):
                                continue
                            fpath = Path(root) / fname
                            try:
                                if fpath.is_symlink() or not fpath.is_file():
                                    continue
                                stream_cache_entries += 1
                                stream_cache_bytes += int(fpath.stat().st_size)
                            except OSError:
                                continue
                except OSError:
                    pass

        return {
            "thumbs_count": thumbs_count,
            "thumbs_bytes": thumbs_bytes,
            "staging_entries": staging_entries,
            "staging_bytes": staging_bytes,
            "backups_count": backups_count,
            "backups_bytes": backups_bytes,
            "stream_cache_entries": stream_cache_entries,
            "stream_cache_bytes": stream_cache_bytes,
        }

    def _scan_orphaned_thumbs(
        self,
        items: list[dict[str, Any]],
        grace_sec: int,
    ) -> tuple[list[dict[str, Any]], int]:
        thumbs_dir = self.settings.thumbs_dir
        if not thumbs_dir.exists():
            return [], 0
        try:
            thumbs_root = thumbs_dir.resolve()
        except OSError:
            return [], 0

        referenced_names = _collect_referenced_thumb_names(items)
        now = time.time()
        orphans: list[dict[str, Any]] = []
        total_bytes = 0

        try:
            entries = list(thumbs_dir.iterdir())
        except OSError:
            return [], 0

        for entry in entries:
            try:
                if entry.is_symlink() or not entry.is_file():
                    continue
                if not entry.resolve().is_relative_to(thumbs_root):
                    continue
                if entry.name in referenced_names:
                    continue
                st = entry.stat()
                if _is_active_temp_thumb(entry.name, float(st.st_mtime), now, grace_sec):
                    continue
                size_bytes = int(st.st_size)
                orphans.append(
                    {
                        "name": entry.name,
                        "size_bytes": size_bytes,
                        "mtime": round(float(st.st_mtime), 3),
                    }
                )
                total_bytes += size_bytes
            except OSError:
                continue

        return orphans, total_bytes

    def _scan_orphaned_staging(
        self,
        grace_sec: int,
    ) -> tuple[list[dict[str, Any]], int]:
        staging_dir = self.settings.staging_dir
        if not staging_dir.exists():
            return [], 0
        try:
            staging_root = staging_dir.resolve()
        except OSError:
            return [], 0

        active_dirs = {p.resolve() for p in self.ingest.get_active_staging_dirs()}
        now = time.time()
        orphans: list[dict[str, Any]] = []
        total_bytes = 0

        try:
            entries = list(staging_dir.iterdir())
        except OSError:
            return [], 0

        for entry in entries:
            try:
                if entry.is_symlink():
                    continue
                entry_resolved = entry.resolve()
                if not entry_resolved.is_relative_to(staging_root):
                    continue
                if _is_path_active(entry_resolved, active_dirs):
                    continue
                is_dir, size_bytes, latest_mtime = _measure_entry(entry)
                age_sec = max(0.0, now - latest_mtime)
                if age_sec < grace_sec:
                    continue
                orphans.append(
                    {
                        "name": entry.name,
                        "is_dir": is_dir,
                        "size_bytes": size_bytes,
                        "age_sec": round(age_sec, 1),
                    }
                )
                total_bytes += size_bytes
            except OSError:
                continue

        return orphans, total_bytes

    def _scan_local_audit(
        self,
        items: list[dict[str, Any]],
        effective_grace_sec: int,
    ) -> dict[str, Any]:
        thumbs_dir = self.settings.thumbs_dir
        missing_thumbs: list[dict[str, Any]] = []
        missing_previews: list[dict[str, Any]] = []
        missing_metadata: list[dict[str, Any]] = []
        missing_local_files: list[dict[str, Any]] = []

        for item in items:
            media_id = item["id"]
            title = item.get("title") or item.get("filename") or ""
            mtype = str(item.get("media_type") or "")

            if mtype in ("image", "video") and _find_existing_thumb(thumbs_dir, item) is None:
                missing_thumbs.append(
                    {
                        "media_id": media_id,
                        "title": title,
                        "media_type": mtype,
                        "thumb_filename": item.get("thumb_filename"),
                    }
                )

            if mtype == "video" and _find_existing_preview(thumbs_dir, item) is None:
                missing_previews.append(
                    {
                        "media_id": media_id,
                        "title": title,
                        "media_type": mtype,
                        "preview_filename": item.get("preview_filename"),
                    }
                )

            missing_fields = _missing_metadata_fields(item)
            if missing_fields:
                missing_metadata.append(
                    {
                        "media_id": media_id,
                        "title": title,
                        "media_type": mtype,
                        "missing_fields": missing_fields,
                    }
                )

            raw_path = str(item.get("file_path") or "").strip()
            if raw_path and not Path(raw_path).is_file():
                missing_local_files.append(
                    {
                        "media_id": media_id,
                        "title": title,
                        "drive_name": item.get("drive_name") or "",
                        "file_path": raw_path,
                        "has_telegram_fallback": _has_telegram_fallback(item),
                    }
                )

        orphaned_thumbs, orphan_thumbs_bytes = self._scan_orphaned_thumbs(items, effective_grace_sec)
        orphaned_staging, orphan_staging_bytes = self._scan_orphaned_staging(effective_grace_sec)

        return {
            "missing_thumbs": missing_thumbs,
            "missing_previews": missing_previews,
            "missing_metadata": missing_metadata,
            "missing_local_files": missing_local_files,
            "orphaned_thumbs": orphaned_thumbs,
            "orphan_thumbs_bytes": orphan_thumbs_bytes,
            "orphaned_staging": orphaned_staging,
            "orphan_staging_bytes": orphan_staging_bytes,
        }

    async def run_audit(
        self,
        *,
        verify_telegram: bool = False,
        staging_grace_sec: int | None = None,
        issue_sample_limit: int = 200,
    ) -> dict[str, Any]:
        if self.is_busy():
            raise RuntimeError("Maintenance task already in progress")

        async with self._lock:
            effective_grace = max(
                0,
                int(
                    staging_grace_sec
                    if staging_grace_sec is not None
                    else self.settings.integrity_staging_grace_sec
                ),
            )
            limit = max(1, int(issue_sample_limit))
            run = self.db.create_maintenance_run(
                "audit",
                status="running",
                message="Scanning local media assets...",
                options={
                    "verify_telegram": bool(verify_telegram),
                    "staging_grace_sec": effective_grace,
                    "issue_sample_limit": limit,
                },
            )
            run_id = run["id"]
            self._active_run_id = run_id

            try:
                items = await asyncio.to_thread(self.db.list_all_media_for_audit)
                local = await asyncio.to_thread(self._scan_local_audit, items, effective_grace)

                missing_thumbs = local["missing_thumbs"]
                missing_previews = local["missing_previews"]
                missing_metadata = local["missing_metadata"]
                missing_local_files = local["missing_local_files"]
                orphaned_thumbs = local["orphaned_thumbs"]
                orphan_thumbs_bytes = local["orphan_thumbs_bytes"]
                orphaned_staging = local["orphaned_staging"]
                orphan_staging_bytes = local["orphan_staging_bytes"]

                broken_telegram_refs: list[dict[str, Any]] = []
                tg_checked = 0
                tg_unchecked = 0
                tg_errors: list[str] = []

                if verify_telegram:
                    self.db.update_maintenance_run(
                        run_id,
                        progress=60.0,
                        message="Verifying Telegram archive references...",
                    )
                    ptr_map: dict[tuple[str, int], list[dict[str, Any]]] = {}
                    chat_msg_ids: dict[str, set[int]] = {}
                    for item in items:
                        title = item.get("title") or item.get("filename") or ""
                        for chat_id, msg_id, part_idx in _iter_item_telegram_refs(item):
                            ptr_map.setdefault((chat_id, msg_id), []).append(
                                {
                                    "media_id": item["id"],
                                    "title": title,
                                    "part_index": part_idx,
                                }
                            )
                            chat_msg_ids.setdefault(chat_id, set()).add(msg_id)

                    for chat_id, msg_id_set in chat_msg_ids.items():
                        res = await self.tg_pool.verify_archive_messages(
                            chat_id,
                            sorted(msg_id_set),
                            batch_size=self.settings.integrity_telegram_batch_size,
                            delay_sec=self.settings.integrity_telegram_batch_delay_sec,
                        )
                        present = res.get("present") or []
                        missing = res.get("missing") or []
                        unchecked = res.get("unchecked_rate_limited") or []
                        errs = res.get("errors") or []

                        tg_checked += len(present) + len(missing)
                        tg_unchecked += len(unchecked)
                        tg_errors.extend(str(e) for e in errs)

                        for mid in missing:
                            for ref in ptr_map.get((chat_id, int(mid)), []):
                                broken_telegram_refs.append(
                                    {
                                        "media_id": ref["media_id"],
                                        "title": ref["title"],
                                        "chat_id": chat_id,
                                        "msg_id": int(mid),
                                        "part_index": ref["part_index"],
                                    }
                                )

                summary = {
                    "total_media": len(items),
                    "healthy": (
                        len(missing_thumbs) == 0
                        and len(missing_previews) == 0
                        and len(missing_metadata) == 0
                        and len(missing_local_files) == 0
                        and len(orphaned_thumbs) == 0
                        and len(orphaned_staging) == 0
                        and len(broken_telegram_refs) == 0
                    ),
                    "missing_thumbs": len(missing_thumbs),
                    "missing_previews": len(missing_previews),
                    "missing_metadata": len(missing_metadata),
                    "missing_local_files": len(missing_local_files),
                    "missing_local_unrecoverable": sum(
                        1 for x in missing_local_files if not x["has_telegram_fallback"]
                    ),
                    "orphaned_thumbs": len(orphaned_thumbs),
                    "orphaned_thumbs_bytes": orphan_thumbs_bytes,
                    "orphaned_staging": len(orphaned_staging),
                    "orphaned_staging_bytes": orphan_staging_bytes,
                    "telegram_verified": bool(verify_telegram),
                    "telegram_checked_messages": tg_checked,
                    "broken_telegram_refs": len(broken_telegram_refs),
                    "telegram_unchecked_rate_limited": tg_unchecked,
                }

                raw_lists = {
                    "missing_thumbs": missing_thumbs,
                    "missing_previews": missing_previews,
                    "missing_metadata": missing_metadata,
                    "missing_local_files": missing_local_files,
                    "orphaned_thumbs": orphaned_thumbs,
                    "orphaned_staging": orphaned_staging,
                    "broken_telegram_refs": broken_telegram_refs,
                }
                details: dict[str, Any] = {k: v[:limit] for k, v in raw_lists.items()}
                details["truncated"] = {k: len(v) > limit for k, v in raw_lists.items()}
                details["telegram_errors"] = tg_errors[:limit]

                updated = self.db.update_maintenance_run(
                    run_id,
                    status="done",
                    progress=100.0,
                    message="Audit complete",
                    summary=summary,
                    details=details,
                )
                assert updated is not None
                return updated
            except asyncio.CancelledError:
                self.db.update_maintenance_run(run_id, status="failed", message="Cancelled")
                raise
            except Exception as exc:
                self.db.update_maintenance_run(
                    run_id,
                    status="failed",
                    message=str(exc)[:500],
                )
                raise
            finally:
                self._active_run_id = None

    def start_repair(
        self,
        *,
        backfill_thumbs: bool = True,
        backfill_previews: bool = True,
        backfill_metadata: bool = True,
        purge_orphan_thumbs: bool = False,
        purge_orphan_staging: bool = False,
        staging_grace_sec: int | None = None,
        max_telegram_downloads: int = 25,
        max_telegram_item_mb: int = 250,
        media_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        if self.is_busy():
            raise RuntimeError("Maintenance task already in progress")

        effective_grace = max(
            0,
            int(
                staging_grace_sec
                if staging_grace_sec is not None
                else self.settings.integrity_staging_grace_sec
            ),
        )
        effective_max_tg_dl = max(0, int(max_telegram_downloads))
        effective_max_tg_mb = max(1, int(max_telegram_item_mb))
        target_ids = (
            [m.strip() for m in media_ids if m and m.strip()]
            if media_ids is not None
            else None
        )

        run = self.db.create_maintenance_run(
            "repair",
            status="running",
            message="Starting repair...",
            options={
                "backfill_thumbs": bool(backfill_thumbs),
                "backfill_previews": bool(backfill_previews),
                "backfill_metadata": bool(backfill_metadata),
                "purge_orphan_thumbs": bool(purge_orphan_thumbs),
                "purge_orphan_staging": bool(purge_orphan_staging),
                "staging_grace_sec": effective_grace,
                "max_telegram_downloads": effective_max_tg_dl,
                "max_telegram_item_mb": effective_max_tg_mb,
                "media_ids": target_ids,
            },
        )
        run_id = run["id"]
        self._active_run_id = run_id
        self._active_task = asyncio.create_task(
            self._execute_repair(
                run_id,
                backfill_thumbs=bool(backfill_thumbs),
                backfill_previews=bool(backfill_previews),
                backfill_metadata=bool(backfill_metadata),
                purge_orphan_thumbs=bool(purge_orphan_thumbs),
                purge_orphan_staging=bool(purge_orphan_staging),
                effective_grace_sec=effective_grace,
                max_telegram_downloads=effective_max_tg_dl,
                max_telegram_item_mb=effective_max_tg_mb,
                media_ids=target_ids,
            )
        )
        return run

    async def _execute_repair(
        self,
        run_id: str,
        *,
        backfill_thumbs: bool,
        backfill_previews: bool,
        backfill_metadata: bool,
        purge_orphan_thumbs: bool,
        purge_orphan_staging: bool,
        effective_grace_sec: int,
        max_telegram_downloads: int,
        max_telegram_item_mb: int,
        media_ids: list[str] | None,
    ) -> None:
        try:
            async with self._lock:
                thumbs_dir = self.settings.thumbs_dir
                staging_dir = self.settings.staging_dir
                thumbs_dir.mkdir(parents=True, exist_ok=True)
                staging_dir.mkdir(parents=True, exist_ok=True)

                max_telegram_item_bytes = max_telegram_item_mb * 1024 * 1024

                thumbs_repaired = 0
                previews_repaired = 0
                metadata_repaired = 0
                orphan_thumbs_purged = 0
                orphan_staging_purged = 0
                freed_bytes = 0
                tg_downloads_used = 0
                skipped = 0
                failed = 0

                repaired_items: list[dict[str, Any]] = []
                skipped_items: list[dict[str, Any]] = []
                errors: list[dict[str, Any]] = []

                all_items = await asyncio.to_thread(self.db.list_all_media_for_audit)
                if media_ids is not None:
                    id_set = set(media_ids)
                    candidate_pool = [it for it in all_items if it["id"] in id_set]
                else:
                    candidate_pool = all_items

                work_queue: list[dict[str, Any]] = []
                for item in candidate_pool:
                    mtype = str(item.get("media_type") or "")
                    existing_thumb = (
                        _find_existing_thumb(thumbs_dir, item)
                        if (backfill_thumbs and mtype in ("image", "video"))
                        else None
                    )
                    need_thumb_gen = bool(
                        backfill_thumbs
                        and mtype in ("image", "video")
                        and existing_thumb is None
                    )
                    need_thumb_sync = bool(
                        backfill_thumbs
                        and mtype in ("image", "video")
                        and existing_thumb is not None
                        and _safe_basename(item.get("thumb_filename")) != existing_thumb.name
                    )

                    existing_preview = (
                        _find_existing_preview(thumbs_dir, item)
                        if (backfill_previews and mtype == "video")
                        else None
                    )
                    need_preview_gen = bool(
                        backfill_previews
                        and mtype == "video"
                        and existing_preview is None
                    )
                    need_preview_sync = bool(
                        backfill_previews
                        and mtype == "video"
                        and existing_preview is not None
                        and _safe_basename(item.get("preview_filename")) != existing_preview.name
                    )

                    missing_meta = _missing_metadata_fields(item) if backfill_metadata else []
                    need_meta = bool(missing_meta)

                    if (
                        need_thumb_gen
                        or need_thumb_sync
                        or need_preview_gen
                        or need_preview_sync
                        or need_meta
                    ):
                        work_queue.append(
                            {
                                "item": item,
                                "mtype": mtype,
                                "existing_thumb": existing_thumb,
                                "need_thumb_gen": need_thumb_gen,
                                "need_thumb_sync": need_thumb_sync,
                                "existing_preview": existing_preview,
                                "need_preview_gen": need_preview_gen,
                                "need_preview_sync": need_preview_sync,
                                "missing_meta": missing_meta,
                                "need_meta": need_meta,
                            }
                        )

                total_work = len(work_queue)
                tg_aborted_reason: str | None = None

                for idx, entry in enumerate(work_queue):
                    item = entry["item"]
                    media_id = item["id"]
                    mtype = entry["mtype"]
                    existing_thumb: Path | None = entry["existing_thumb"]
                    existing_preview: Path | None = entry["existing_preview"]
                    need_thumb_gen: bool = entry["need_thumb_gen"]
                    need_thumb_sync: bool = entry["need_thumb_sync"]
                    need_preview_gen: bool = entry["need_preview_gen"]
                    need_preview_sync: bool = entry["need_preview_sync"]
                    missing_meta: list[str] = entry["missing_meta"]
                    need_meta: bool = entry["need_meta"]

                    pct = 5.0 + (80.0 * (idx / max(1, total_work)))
                    self.db.update_maintenance_run(
                        run_id,
                        progress=pct,
                        message=f"Repairing {idx + 1}/{total_work}: {item.get('filename') or media_id}",
                    )

                    repaired_actions: list[str] = []
                    sync_kwargs: dict[str, Any] = {}
                    if need_thumb_sync and existing_thumb is not None:
                        sync_kwargs["thumb_filename"] = existing_thumb.name
                        thumbs_repaired += 1
                        repaired_actions.append("thumb_synced")
                    if need_preview_sync and existing_preview is not None:
                        sync_kwargs["preview_filename"] = existing_preview.name
                        previews_repaired += 1
                        repaired_actions.append("preview_synced")

                    if sync_kwargs:
                        self.db.update_media_assets(media_id, **sync_kwargs)

                    if not (need_thumb_gen or need_preview_gen or need_meta):
                        if repaired_actions:
                            repaired_items.append(
                                {"media_id": media_id, "actions": repaired_actions}
                            )
                        continue

                    raw_path = str(item.get("file_path") or "").strip()
                    local_path = Path(raw_path) if raw_path else None
                    has_local = bool(local_path and local_path.is_file())

                    async def _backfill_from_path(src_path: Path) -> None:
                        nonlocal thumbs_repaired, previews_repaired, metadata_repaired, failed
                        asset_kwargs: dict[str, Any] = {}
                        meta: dict[str, Any] = {}

                        if need_meta:
                            meta = await extract_media_metadata(src_path, mtype)
                            for k in (
                                "duration_sec",
                                "width",
                                "height",
                                "video_codec",
                                "audio_codec",
                                "fps",
                            ):
                                if meta.get(k) is not None:
                                    asset_kwargs[k] = meta[k]

                        dur_for_preview = meta.get("duration_sec") or item.get("duration_sec")
                        if need_thumb_gen:
                            new_thumb = await generate_thumbnail(
                                src_path, mtype, thumbs_dir, media_id
                            )
                            if new_thumb:
                                asset_kwargs["thumb_filename"] = new_thumb

                        if need_preview_gen:
                            new_preview = await generate_video_preview(
                                src_path,
                                thumbs_dir,
                                media_id,
                                duration_sec=dur_for_preview,
                            )
                            if new_preview:
                                asset_kwargs["preview_filename"] = new_preview

                        if asset_kwargs:
                            self.db.update_media_assets(media_id, **asset_kwargs)

                        did_any = False
                        if "thumb_filename" in asset_kwargs:
                            thumbs_repaired += 1
                            repaired_actions.append("thumb_generated")
                            did_any = True
                        if "preview_filename" in asset_kwargs:
                            previews_repaired += 1
                            repaired_actions.append("preview_generated")
                            did_any = True
                        if need_meta and any(_is_positive_number(meta.get(f)) for f in missing_meta):
                            metadata_repaired += 1
                            repaired_actions.append("metadata_extracted")
                            did_any = True

                        if repaired_actions:
                            repaired_items.append(
                                {"media_id": media_id, "actions": repaired_actions}
                            )
                        if not did_any and not repaired_actions:
                            failed += 1
                            errors.append(
                                {
                                    "media_id": media_id,
                                    "error": "Unable to generate requested assets from source file",
                                }
                            )

                    if has_local:
                        assert local_path is not None
                        try:
                            await _backfill_from_path(local_path)
                        except Exception as exc:
                            failed += 1
                            errors.append({"media_id": media_id, "error": str(exc)[:300]})
                        continue

                    if not _has_telegram_fallback(item):
                        if repaired_actions:
                            repaired_items.append(
                                {"media_id": media_id, "actions": repaired_actions}
                            )
                        skipped += 1
                        skipped_items.append(
                            {
                                "media_id": media_id,
                                "reason": "no_local_or_telegram_source",
                            }
                        )
                        continue

                    if tg_aborted_reason is not None:
                        if repaired_actions:
                            repaired_items.append(
                                {"media_id": media_id, "actions": repaired_actions}
                            )
                        skipped += 1
                        skipped_items.append(
                            {"media_id": media_id, "reason": tg_aborted_reason}
                        )
                        continue

                    if tg_downloads_used >= max_telegram_downloads:
                        if repaired_actions:
                            repaired_items.append(
                                {"media_id": media_id, "actions": repaired_actions}
                            )
                        skipped += 1
                        skipped_items.append(
                            {"media_id": media_id, "reason": "skipped_download_cap"}
                        )
                        continue

                    item_size = int(item.get("size_bytes") or 0)
                    if item_size > max_telegram_item_bytes:
                        if repaired_actions:
                            repaired_items.append(
                                {"media_id": media_id, "actions": repaired_actions}
                            )
                        skipped += 1
                        skipped_items.append(
                            {"media_id": media_id, "reason": "skipped_size_limit"}
                        )
                        continue

                    telem = self.tg_pool.get_telemetry()
                    worker_count = int(telem.get("worker_count") or 0)
                    online_count = int(telem.get("online_count") or 0)
                    workers_list = list(telem.get("workers") or [])
                    if workers_list:
                        online_available = sum(
                            1 for w in workers_list if w.get("online") and not w.get("in_cooldown")
                        )
                    else:
                        online_available = max(0, online_count - int(telem.get("cooldown_count") or 0))

                    if worker_count == 0 or online_count == 0:
                        tg_aborted_reason = "skipped_no_workers"
                    elif online_available == 0:
                        tg_aborted_reason = "skipped_rate_limited"

                    if tg_aborted_reason is not None:
                        if repaired_actions:
                            repaired_items.append(
                                {"media_id": media_id, "actions": repaired_actions}
                            )
                        skipped += 1
                        skipped_items.append(
                            {"media_id": media_id, "reason": tg_aborted_reason}
                        )
                        continue

                    if tg_downloads_used > 0:
                        await asyncio.sleep(0.1)

                    tmp_dir = Path(tempfile.mkdtemp(dir=staging_dir, prefix="repair_"))
                    self.ingest.register_staging_dir(tmp_dir)
                    try:
                        safe_name = _safe_basename(item.get("filename")) or f"{media_id}.bin"
                        dl_path = tmp_dir / safe_name
                        await self.tg_pool.download_media_to_path(
                            item,
                            dl_path,
                            max_bytes=max_telegram_item_bytes,
                        )
                        tg_downloads_used += 1
                        await _backfill_from_path(dl_path)
                    except FloodWaitError as fw:
                        skipped += 1
                        fw_sec = getattr(fw, "seconds", 0)
                        skipped_items.append(
                            {
                                "media_id": media_id,
                                "reason": "skipped_rate_limited",
                                "detail": f"FloodWait ({fw_sec}s)",
                            }
                        )
                        after_telem = self.tg_pool.get_telemetry()
                        wc = int(after_telem.get("worker_count") or 0)
                        oc = int(after_telem.get("online_count") or 0)
                        after_workers = list(after_telem.get("workers") or [])
                        if after_workers:
                            after_avail = sum(
                                1 for w in after_workers if w.get("online") and not w.get("in_cooldown")
                            )
                        else:
                            after_avail = max(0, oc - int(after_telem.get("cooldown_count") or 0))
                        if wc == 0 or oc == 0:
                            tg_aborted_reason = "skipped_no_workers"
                        elif after_avail == 0:
                            tg_aborted_reason = "skipped_rate_limited"
                    except ValueError as exc:
                        msg = str(exc)
                        if "exceeds repair download limit" in msg.lower():
                            skipped += 1
                            skipped_items.append(
                                {
                                    "media_id": media_id,
                                    "reason": "skipped_size_limit",
                                    "detail": msg[:300],
                                }
                            )
                        else:
                            failed += 1
                            errors.append({"media_id": media_id, "error": msg[:300]})
                    except Exception as exc:
                        failed += 1
                        errors.append({"media_id": media_id, "error": str(exc)[:300]})
                    finally:
                        shutil.rmtree(tmp_dir, ignore_errors=True)
                        self.ingest.unregister_staging_dir(tmp_dir)

                if purge_orphan_thumbs:
                    self.db.update_maintenance_run(
                        run_id,
                        progress=90.0,
                        message="Purging orphaned thumbnails...",
                    )
                    latest_items = await asyncio.to_thread(self.db.list_all_media_for_audit)
                    orphan_thumbs, _ = await asyncio.to_thread(
                        self._scan_orphaned_thumbs, latest_items, effective_grace_sec
                    )
                    thumbs_root = thumbs_dir.resolve()
                    for ot in orphan_thumbs:
                        target = thumbs_dir / ot["name"]
                        try:
                            if target.is_symlink() or not target.is_file():
                                continue
                            if not target.resolve().is_relative_to(thumbs_root):
                                continue
                            target.unlink(missing_ok=True)
                            orphan_thumbs_purged += 1
                            freed_bytes += int(ot["size_bytes"])
                        except OSError as exc:
                            errors.append(
                                {"orphan_thumb": ot["name"], "error": str(exc)[:300]}
                            )

                if purge_orphan_staging:
                    self.db.update_maintenance_run(
                        run_id,
                        progress=95.0,
                        message="Purging orphaned staging entries...",
                    )
                    orphan_staging, _ = await asyncio.to_thread(
                        self._scan_orphaned_staging, effective_grace_sec
                    )
                    staging_root = staging_dir.resolve()
                    active_dirs = {p.resolve() for p in self.ingest.get_active_staging_dirs()}
                    for os_entry in orphan_staging:
                        target = staging_dir / os_entry["name"]
                        try:
                            if target.is_symlink() or not target.exists():
                                continue
                            target_resolved = target.resolve()
                            if not target_resolved.is_relative_to(staging_root):
                                continue
                            if _is_path_active(target_resolved, active_dirs):
                                continue
                            if target.is_dir():
                                shutil.rmtree(target)
                            else:
                                target.unlink(missing_ok=True)
                            orphan_staging_purged += 1
                            freed_bytes += int(os_entry["size_bytes"])
                        except OSError as exc:
                            errors.append(
                                {"orphan_staging": os_entry["name"], "error": str(exc)[:300]}
                            )

                summary = {
                    "thumbs_repaired": thumbs_repaired,
                    "previews_repaired": previews_repaired,
                    "metadata_repaired": metadata_repaired,
                    "orphan_thumbs_purged": orphan_thumbs_purged,
                    "orphan_staging_purged": orphan_staging_purged,
                    "freed_bytes": freed_bytes,
                    "telegram_downloads": tg_downloads_used,
                    "skipped": skipped,
                    "failed": failed,
                }
                details = {
                    "repaired_items": repaired_items[:_REPAIR_DETAIL_LIMIT],
                    "skipped_items": skipped_items[:_REPAIR_DETAIL_LIMIT],
                    "errors": errors[:_REPAIR_DETAIL_LIMIT],
                }
                self.db.update_maintenance_run(
                    run_id,
                    status="done",
                    progress=100.0,
                    message="Repair complete",
                    summary=summary,
                    details=details,
                )
        except asyncio.CancelledError:
            msg = "Interrupted by server shutdown" if self._shutting_down else "Cancelled"
            self.db.update_maintenance_run(run_id, status="failed", message=msg)
            raise
        except Exception as exc:
            logger.exception("Repair run %s failed: %s", run_id, exc)
            self.db.update_maintenance_run(
                run_id,
                status="failed",
                message=str(exc)[:500],
            )
        finally:
            if self._active_run_id == run_id:
                self._active_run_id = None
            self._active_task = None
