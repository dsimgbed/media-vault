from __future__ import annotations

import asyncio
import gzip
import hashlib
import logging
import os
import re
import shutil
import sqlite3
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from app.config import Settings
from app.db import Database
from app.telegram_mtproto import TelegramPool

logger = logging.getLogger(__name__)

_CHUNK_BYTES = 1024 * 1024  # 1 MiB
_GZIP_MAGIC = b"\x1f\x8b"
_CORE_TABLES = {"media_items", "ingest_jobs", "folder_topics"}
_BACKUP_FILENAME_RE = re.compile(
    r"^vault_backup_(\d{8}T\d{6}Z)_([A-Za-z0-9_-]+)\.db\.gz$"
)


def _sqlite_ro_uri(db_path: Path) -> str:
    resolved = Path(db_path).resolve()
    return f"file:{quote(resolved.as_posix(), safe='/')}?mode=ro"


def _is_gzip_file(path: Path) -> bool:
    if path.suffix.lower() == ".gz":
        return True
    try:
        with path.open("rb") as fh:
            return fh.read(2) == _GZIP_MAGIC
    except OSError:
        return False


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _check_sqlite_integrity(db_path: Path, *, read_only: bool = True) -> list[str]:
    conn: sqlite3.Connection | None = None
    try:
        if read_only:
            conn = sqlite3.connect(_sqlite_ro_uri(db_path), uri=True)
        else:
            conn = sqlite3.connect(db_path)
        rows = conn.execute("PRAGMA integrity_check").fetchall()
    except sqlite3.Error as exc:
        raise ValueError(f"Invalid or corrupted SQLite database: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()

    messages = [str(r[0]) for r in rows]
    if len(messages) != 1 or messages[0].lower() != "ok":
        raise ValueError(f"SQLite integrity_check failed: {messages}")
    return messages


def _decompress_gzip_to_file(src_path: Path, dst_path: Path) -> int:
    total = 0
    try:
        with gzip.open(src_path, "rb") as gz_in, dst_path.open("wb") as out:
            while True:
                chunk = gz_in.read(_CHUNK_BYTES)
                if not chunk:
                    break
                out.write(chunk)
                total += len(chunk)
    except (gzip.BadGzipFile, OSError, EOFError) as exc:
        raise ValueError(f"Invalid or corrupted gzip backup: {exc}") from exc
    return total


def verify_backup_file(backup_path: Path) -> dict[str, Any]:
    backup_path = Path(backup_path)
    if not backup_path.exists() or not backup_path.is_file():
        raise FileNotFoundError(f"Backup file not found: {backup_path}")

    size_bytes = backup_path.stat().st_size
    if size_bytes <= 0:
        raise ValueError(f"Backup file is empty: {backup_path}")

    sha256 = _sha256_file(backup_path)
    is_gz = _is_gzip_file(backup_path)

    tmp_db_path: Path | None = None
    conn: sqlite3.Connection | None = None
    try:
        if is_gz:
            with tempfile.NamedTemporaryFile(
                prefix="vault_verify_", suffix=".db", delete=False
            ) as tmp_fh:
                tmp_db_path = Path(tmp_fh.name)
            uncompressed_bytes = _decompress_gzip_to_file(backup_path, tmp_db_path)
            if uncompressed_bytes <= 0:
                raise ValueError(f"Decompressed backup is empty: {backup_path}")
            sqlite_path = tmp_db_path
        else:
            uncompressed_bytes = size_bytes
            sqlite_path = backup_path

        try:
            conn = sqlite3.connect(_sqlite_ro_uri(sqlite_path), uri=True)
            rows = conn.execute("PRAGMA integrity_check").fetchall()
            messages = [str(r[0]) for r in rows]
            if len(messages) != 1 or messages[0].lower() != "ok":
                raise ValueError(f"SQLite integrity_check failed: {messages}")

            table_rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            sorted_tables = sorted(str(r[0]) for r in table_rows)
            missing = _CORE_TABLES - set(sorted_tables)
            if missing:
                raise ValueError(
                    f"Backup missing required core tables: {', '.join(sorted(missing))}"
                )

            media_count = int(
                conn.execute("SELECT COUNT(*) FROM media_items").fetchone()[0]
            )
            jobs_count = int(
                conn.execute("SELECT COUNT(*) FROM ingest_jobs").fetchone()[0]
            )
        except sqlite3.Error as exc:
            raise ValueError(f"Invalid or corrupted SQLite database: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()
        if tmp_db_path is not None:
            tmp_db_path.unlink(missing_ok=True)

    return {
        "valid": True,
        "backup_path": str(backup_path),
        "size_bytes": size_bytes,
        "uncompressed_bytes": uncompressed_bytes,
        "sha256": sha256,
        "media_count": media_count,
        "jobs_count": jobs_count,
        "tables": sorted_tables,
    }


def restore_backup_file(
    backup_path: Path, db_path: Path, *, verify_only: bool = False
) -> dict[str, Any]:
    backup_path = Path(backup_path)
    db_path = Path(db_path)

    info = verify_backup_file(backup_path)
    if verify_only:
        return {**info, "restored": False, "verify_only": True}

    db_path.parent.mkdir(parents=True, exist_ok=True)

    pre_restore_path: Path | None = None
    pre_restore_warning: str | None = None
    if db_path.exists():
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        candidate = db_path.parent / f"{db_path.name}.pre-restore-{ts}.bak"
        if candidate.exists():
            candidate = (
                db_path.parent
                / f"{db_path.name}.pre-restore-{ts}-{uuid.uuid4().hex[:6]}.bak"
            )
        pre_restore_path = candidate
        src_conn: sqlite3.Connection | None = None
        dst_conn: sqlite3.Connection | None = None
        try:
            src_conn = sqlite3.connect(db_path)
            dst_conn = sqlite3.connect(pre_restore_path)
            src_conn.backup(dst_conn)
        except sqlite3.Error as exc:
            logger.warning(
                "Pre-restore SQLite backup of %s failed (%s); falling back to raw file copy.",
                db_path,
                exc,
            )
            pre_restore_warning = (
                f"Existing database could not be opened cleanly via SQLite ({exc}); "
                f"copied raw database file(s) to {pre_restore_path.name}."
            )
            if dst_conn is not None:
                dst_conn.close()
                dst_conn = None
            pre_restore_path.unlink(missing_ok=True)
            shutil.copy2(db_path, pre_restore_path)
            for suffix in ("-wal", "-shm"):
                sidecar = Path(f"{db_path}{suffix}")
                if sidecar.is_file():
                    shutil.copy2(sidecar, Path(f"{pre_restore_path}{suffix}"))
            if src_conn is not None:
                src_conn.close()
                src_conn = None
        finally:
            if dst_conn is not None:
                dst_conn.close()
            if src_conn is not None:
                src_conn.close()

    tmp_restored: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=db_path.parent,
            prefix=f".{db_path.name}.restore.",
            suffix=".tmp",
            delete=False,
        ) as tmp_fh:
            tmp_restored = Path(tmp_fh.name)

        if _is_gzip_file(backup_path):
            _decompress_gzip_to_file(backup_path, tmp_restored)
        else:
            shutil.copyfile(backup_path, tmp_restored)

        _check_sqlite_integrity(tmp_restored, read_only=True)

        # Remove stale WAL/SHM sidecar files before replacing DB so SQLite never
        # replays old WAL frames onto the restored snapshot.
        Path(f"{db_path}-wal").unlink(missing_ok=True)
        Path(f"{db_path}-shm").unlink(missing_ok=True)

        os.replace(tmp_restored, db_path)
        tmp_restored = None
    finally:
        if tmp_restored is not None:
            tmp_restored.unlink(missing_ok=True)

    _check_sqlite_integrity(db_path, read_only=True)

    result: dict[str, Any] = {
        **info,
        "restored": True,
        "verify_only": False,
        "db_path": str(db_path),
        "pre_restore_backup": str(pre_restore_path) if pre_restore_path else None,
    }
    if pre_restore_warning:
        result["warning"] = pre_restore_warning
    return result


def _extract_backup_meta_from_filename(filename: str) -> tuple[str, str | None]:
    match = _BACKUP_FILENAME_RE.match(filename)
    if match:
        ts_raw, bid = match.group(1), match.group(2)
        try:
            dt = datetime.strptime(ts_raw, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
            return bid, dt.isoformat()
        except ValueError:
            return bid, None
    stem = filename[:-6] if filename.endswith(".db.gz") else Path(filename).stem
    return stem, None


class BackupService:
    def __init__(
        self, settings: Settings, db: Database, tg_pool: TelegramPool
    ) -> None:
        self.settings = settings
        self.db = db
        self.tg_pool = tg_pool
        self._lock = asyncio.Lock()
        self._scheduler_task: asyncio.Task[None] | None = None

    async def create_backup(
        self,
        *,
        trigger: str = "manual",
        upload_to_telegram: bool | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            self.settings.backups_dir.mkdir(parents=True, exist_ok=True)
            backup_id = uuid.uuid4().hex[:12]
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            filename = f"vault_backup_{ts}_{backup_id}.db.gz"
            final_path = self.settings.backups_dir / filename

            def _run_sync_backup() -> tuple[int, int, str]:
                tmp_db = self.settings.backups_dir / f".tmp_{backup_id}.db"
                tmp_gz = self.settings.backups_dir / f".tmp_{backup_id}.db.gz"
                try:
                    self.db.backup_to(tmp_db)

                    conn = sqlite3.connect(tmp_db)
                    try:
                        rows = conn.execute("PRAGMA integrity_check").fetchall()
                    finally:
                        conn.close()
                    messages = [str(r[0]) for r in rows]
                    if len(messages) != 1 or messages[0].lower() != "ok":
                        raise RuntimeError(
                            f"Backup SQLite integrity_check failed: {messages}"
                        )

                    uncompressed_bytes = tmp_db.stat().st_size

                    with tmp_db.open("rb") as src, gzip.open(
                        tmp_gz, "wb", compresslevel=6
                    ) as gz_out:
                        while True:
                            chunk = src.read(_CHUNK_BYTES)
                            if not chunk:
                                break
                            gz_out.write(chunk)

                    sha256_hex = _sha256_file(tmp_gz)
                    size_bytes = tmp_gz.stat().st_size
                    os.replace(tmp_gz, final_path)
                    return size_bytes, uncompressed_bytes, sha256_hex
                finally:
                    tmp_db.unlink(missing_ok=True)
                    tmp_gz.unlink(missing_ok=True)

            size_bytes, uncompressed_bytes, sha256 = await asyncio.to_thread(
                _run_sync_backup
            )

            should_upload = (
                self.settings.backup_telegram_enabled
                if upload_to_telegram is None
                else bool(upload_to_telegram)
            )

            telegram_chat_id: str | None = None
            telegram_topic_id: int | None = None
            telegram_msg_id: int | None = None
            telegram_worker_name: str | None = None
            telegram_error: str | None = None
            telegram_skipped: str | None = None

            if should_upload:
                caption_lines = [
                    f"Vault Backup ({trigger})",
                    f"File: {filename}",
                    f"Size: {size_bytes} bytes (uncompressed {uncompressed_bytes} bytes)",
                    f"SHA-256: {sha256[:16]}...",
                ]
                if note:
                    caption_lines.append(f"Note: {note}")
                caption = "\n".join(caption_lines)

                try:
                    tg_res = await self.tg_pool.upload_backup_file(
                        final_path,
                        topic_name=self.settings.backup_telegram_topic,
                        caption=caption,
                    )
                    if tg_res is None:
                        telegram_skipped = (
                            "Telegram archive not configured or no workers online"
                        )
                    else:
                        telegram_chat_id = (
                            str(tg_res["chat_id"])
                            if tg_res.get("chat_id") is not None
                            else None
                        )
                        telegram_topic_id = tg_res.get("topic_id")
                        telegram_msg_id = tg_res.get("message_id")
                        telegram_worker_name = tg_res.get("worker_name")
                except Exception as exc:
                    logger.warning(
                        "Failed uploading backup %s to Telegram: %s", filename, exc
                    )
                    telegram_error = str(exc)

            record = self.db.insert_backup_record(
                {
                    "id": backup_id,
                    "filename": filename,
                    "file_path": str(final_path),
                    "size_bytes": size_bytes,
                    "uncompressed_bytes": uncompressed_bytes,
                    "sha256": sha256,
                    "trigger": trigger,
                    "note": note,
                    "telegram_chat_id": telegram_chat_id,
                    "telegram_topic_id": telegram_topic_id,
                    "telegram_msg_id": telegram_msg_id,
                    "telegram_worker_name": telegram_worker_name,
                }
            )

            pruned = self.prune_local_backups(
                keep=max(1, int(self.settings.backup_retention_count)),
                protect_ids={backup_id},
            )

            self.db.publish(
                {
                    "type": "backup_update",
                    "action": "created",
                    "backup_id": backup_id,
                }
            )

            result: dict[str, Any] = {
                **record,
                "exists_locally": final_path.is_file(),
                "in_db": True,
                "pruned_count": pruned,
            }
            if telegram_error is not None:
                result["telegram_error"] = telegram_error
            if telegram_skipped is not None:
                result["telegram_skipped"] = telegram_skipped
            return result

    def _iter_local_backup_files(self) -> list[Path]:
        if not self.settings.backups_dir.is_dir():
            return []
        files: list[Path] = []
        for entry in self.settings.backups_dir.iterdir():
            if entry.name.startswith(".") or entry.is_symlink():
                continue
            if entry.is_file() and entry.name.endswith(".db.gz"):
                files.append(entry)
        return files

    def prune_local_backups(
        self, *, keep: int | None = None, protect_ids: set[str] | None = None
    ) -> int:
        keep_count = max(
            1,
            int(
                self.settings.backup_retention_count
                if keep is None
                else keep
            ),
        )
        protected = protect_ids or set()
        files = self._iter_local_backup_files()
        if len(files) <= keep_count:
            return 0

        files.sort(key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True)

        db_records = self.db.list_backup_records(limit=1000)
        by_filename: dict[str, str] = {
            str(r["filename"]): str(r["id"]) for r in db_records
        }
        by_path: dict[str, str] = {
            str(r["file_path"]): str(r["id"]) for r in db_records
        }

        protected_files: list[Path] = []
        unprotected_files: list[tuple[Path, str]] = []
        for path in files:
            extracted_id, _ = _extract_backup_meta_from_filename(path.name)
            db_id = (
                by_path.get(str(path))
                or by_filename.get(path.name)
                or extracted_id
            )
            if (
                extracted_id in protected
                or db_id in protected
                or path.name in protected
            ):
                protected_files.append(path)
            else:
                unprotected_files.append((path, db_id))

        keep_unprotected = max(0, keep_count - len(protected_files))
        pruned = 0
        for old_path, db_id in unprotected_files[keep_unprotected:]:
            try:
                old_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("Failed to prune backup file %s: %s", old_path, exc)
                continue

            pruned += 1
            if db_id:
                self.db.delete_backup_record(db_id)

        return pruned

    def _disk_record_for_path(self, path: Path) -> dict[str, Any]:
        st = path.stat()
        disk_id, parsed_ts = _extract_backup_meta_from_filename(path.name)
        created_at = parsed_ts or datetime.fromtimestamp(
            st.st_mtime, tz=timezone.utc
        ).isoformat()
        return {
            "id": disk_id,
            "filename": path.name,
            "file_path": str(path),
            "size_bytes": int(st.st_size),
            "uncompressed_bytes": 0,
            "sha256": None,
            "trigger": "disk",
            "note": None,
            "telegram_chat_id": None,
            "telegram_topic_id": None,
            "telegram_msg_id": None,
            "telegram_worker_name": None,
            "created_at": created_at,
            "exists_locally": True,
            "in_db": False,
        }

    def list_backups(self, limit: int = 50) -> list[dict[str, Any]]:
        db_records = self.db.list_backup_records(limit=max(limit, 100))
        items: list[dict[str, Any]] = []
        seen_filenames: set[str] = set()
        seen_resolved: set[Path] = set()

        for rec in db_records:
            rec_path = Path(str(rec["file_path"]))
            fallback_path = self.settings.backups_dir / str(rec["filename"])
            exists_locally = rec_path.is_file() or fallback_path.is_file()
            items.append(
                {
                    "id": rec["id"],
                    "filename": rec["filename"],
                    "file_path": rec["file_path"],
                    "size_bytes": rec["size_bytes"],
                    "uncompressed_bytes": rec.get("uncompressed_bytes", 0),
                    "sha256": rec.get("sha256"),
                    "trigger": rec.get("trigger", "manual"),
                    "note": rec.get("note"),
                    "telegram_chat_id": rec.get("telegram_chat_id"),
                    "telegram_topic_id": rec.get("telegram_topic_id"),
                    "telegram_msg_id": rec.get("telegram_msg_id"),
                    "telegram_worker_name": rec.get("telegram_worker_name"),
                    "created_at": rec["created_at"],
                    "exists_locally": exists_locally,
                    "in_db": True,
                }
            )
            seen_filenames.add(str(rec["filename"]))
            if rec_path.exists():
                seen_resolved.add(rec_path.resolve())
            if fallback_path.exists():
                seen_resolved.add(fallback_path.resolve())

        for disk_path in self._iter_local_backup_files():
            if disk_path.name in seen_filenames:
                continue
            try:
                if disk_path.resolve() in seen_resolved:
                    continue
            except OSError:
                continue
            items.append(self._disk_record_for_path(disk_path))

        def _sort_key(item: dict[str, Any]) -> tuple[float, str]:
            raw_ts = str(item.get("created_at") or "")
            try:
                dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                return (dt.timestamp(), str(item.get("filename") or ""))
            except ValueError:
                return (0.0, raw_ts)

        items.sort(key=_sort_key, reverse=True)
        return items[: max(0, int(limit))]

    def _validate_path_in_backups_dir(self, candidate: Path) -> Path | None:
        if not self.settings.backups_dir.exists():
            return None
        if candidate.is_symlink():
            return None
        root = self.settings.backups_dir.resolve()
        try:
            resolved = candidate.resolve()
            resolved.relative_to(root)
        except (OSError, ValueError):
            return None
        if resolved == root:
            return None
        if not candidate.exists() or not resolved.is_file():
            return None
        return resolved

    def resolve_backup_file(self, backup_id: str) -> tuple[dict[str, Any], Path]:
        if not backup_id or not str(backup_id).strip():
            raise FileNotFoundError("Backup ID is empty")

        rec = self.db.get_backup_record(backup_id)
        if rec is not None:
            for candidate in (
                Path(str(rec["file_path"])),
                self.settings.backups_dir / str(rec["filename"]),
            ):
                resolved = self._validate_path_in_backups_dir(candidate)
                if resolved is not None:
                    meta = {
                        **rec,
                        "exists_locally": True,
                        "in_db": True,
                    }
                    return meta, resolved
            raise FileNotFoundError(
                f"Backup file not found locally for backup_id={backup_id}"
            )

        for disk_path in self._iter_local_backup_files():
            extracted_id, _ = _extract_backup_meta_from_filename(disk_path.name)
            stem_no_ext = disk_path.name[:-6] if disk_path.name.endswith(".db.gz") else disk_path.stem
            if backup_id in (disk_path.name, extracted_id, stem_no_ext, disk_path.stem):
                resolved = self._validate_path_in_backups_dir(disk_path)
                if resolved is not None:
                    return self._disk_record_for_path(resolved), resolved

        if (
            "/" not in backup_id
            and "\\" not in backup_id
            and not backup_id.startswith(".")
            and (backup_id.endswith(".db.gz") or backup_id.endswith(".db"))
        ):
            candidate = self.settings.backups_dir / backup_id
            resolved = self._validate_path_in_backups_dir(candidate)
            if resolved is not None:
                return self._disk_record_for_path(resolved), resolved

        raise FileNotFoundError(f"Backup not found: {backup_id}")

    def delete_backup(self, backup_id: str) -> dict[str, Any] | None:
        if not backup_id or not str(backup_id).strip():
            return None

        rec = self.db.get_backup_record(backup_id)
        local_path: Path | None = None
        disk_meta: dict[str, Any] | None = None

        if rec is not None:
            for candidate in (
                Path(str(rec["file_path"])),
                self.settings.backups_dir / str(rec["filename"]),
            ):
                resolved = self._validate_path_in_backups_dir(candidate)
                if resolved is not None:
                    local_path = resolved
                    break
        else:
            try:
                disk_meta, local_path = self.resolve_backup_file(backup_id)
            except FileNotFoundError:
                local_path = None
            if disk_meta is not None:
                extracted_id = str(disk_meta.get("id") or "")
                if extracted_id:
                    rec = self.db.get_backup_record(extracted_id)
            if rec is None and local_path is not None:
                resolved_str = str(local_path)
                for row in self.db.list_backup_records(limit=1000):
                    if row.get("filename") == local_path.name or row.get("file_path") == resolved_str:
                        rec = row
                        break

        if rec is None and local_path is None:
            return None

        if local_path is not None:
            local_path.unlink(missing_ok=True)

        deleted_rec: dict[str, Any] | None = None
        if rec is not None:
            deleted_rec = self.db.delete_backup_record(str(rec["id"]))

        resolved_id = str((deleted_rec or rec or disk_meta or {}).get("id") or backup_id)
        self.db.publish(
            {
                "type": "backup_update",
                "action": "deleted",
                "backup_id": resolved_id,
            }
        )

        base = deleted_rec or rec or disk_meta or {"id": resolved_id}
        return {
            **base,
            "exists_locally": False,
            "in_db": False,
        }

    def get_config_summary(self) -> dict[str, Any]:
        return {
            "backups_dir": str(self.settings.backups_dir),
            "retention_count": self.settings.backup_retention_count,
            "interval_hours": self.settings.backup_interval_hours,
            "telegram_enabled": self.settings.backup_telegram_enabled,
            "telegram_topic": self.settings.backup_telegram_topic,
            "scheduler_running": bool(
                self._scheduler_task and not self._scheduler_task.done()
            ),
        }

    async def _scheduler_loop(self) -> None:
        interval_sec = float(self.settings.backup_interval_hours) * 3600.0
        while True:
            await asyncio.sleep(interval_sec)
            try:
                await self.create_backup(trigger="scheduled")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Scheduled backup failed: %s", exc)

    def start_scheduler(self) -> None:
        if float(self.settings.backup_interval_hours) <= 0:
            return
        if self._scheduler_task is not None and not self._scheduler_task.done():
            return
        self._scheduler_task = asyncio.create_task(
            self._scheduler_loop(), name="backup-scheduler"
        )

    async def stop_scheduler(self) -> None:
        task = self._scheduler_task
        self._scheduler_task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
