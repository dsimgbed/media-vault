from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import re
import shutil
import threading
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.config import Settings
from app.db import Database, utc_now_iso
from app.ingest import compute_file_sha256

logger = logging.getLogger(__name__)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ACTIVE_SESSION_STATUSES = ("uploading", "completing")
_ACTIVE_JOB_STATUSES = ("queued", "downloading", "allocating")
_TERMINAL_RETAINABLE_JOB_STATUSES = ("failed", "cancelled")


class UploadError(Exception):
    def __init__(
        self,
        status_code: int,
        detail: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = int(status_code)
        self.detail = detail
        self.extra = extra


def _sanitize_upload_filename(raw: str) -> str:
    if not isinstance(raw, str):
        raise UploadError(400, "Invalid filename")
    cleaned = "".join(ch for ch in raw if ord(ch) >= 32 and ord(ch) != 127).strip()
    if not cleaned:
        raise UploadError(400, "Invalid filename")

    parts = [seg for seg in cleaned.replace("\\", "/").split("/") if seg]
    if not parts:
        raise UploadError(400, "Invalid filename")
    name = parts[-1].replace("/", "").replace("\\", "").strip()
    if not name or name in (".", ".."):
        raise UploadError(400, "Invalid filename")

    if len(name) > 200:
        p = Path(name)
        suffix = p.suffix[:32]
        stem = p.stem
        max_stem = max(1, 200 - len(suffix))
        name = f"{stem[:max_stem]}{suffix}"[:200]
        if not name or name in (".", ".."):
            raise UploadError(400, "Invalid filename")
    return name


def _parse_iso_utc(raw: str | None) -> datetime | None:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = f"{s[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _pwrite_all(path: Path, offset: int, data: bytes) -> None:
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        view = memoryview(data)
        written = 0
        total = len(data)
        while written < total:
            n = os.pwrite(fd, view[written:], offset + written)
            if n <= 0:
                raise OSError("os.pwrite wrote 0 bytes")
            written += n
    finally:
        os.close(fd)


class UploadSessionService:
    def __init__(self, settings: Settings, db: Database, ingest: Any) -> None:
        self.settings = settings
        self.db = db
        self.ingest = ingest
        self._locks: dict[str, asyncio.Lock] = {}
        self._init_lock = asyncio.Lock()
        self._retention_lock = threading.RLock()
        self._gc_task: asyncio.Task[Any] | None = None

    def _get_lock(self, session_id: str) -> asyncio.Lock:
        lock = self._locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[session_id] = lock
        return lock

    def _release_lock_if_idle(self, session_id: str) -> None:
        lock = self._locks.get(session_id)
        if lock is not None and not lock.locked():
            self._locks.pop(session_id, None)

    def _new_expires_at(self) -> str:
        ttl = max(60, int(self.settings.upload_session_ttl_sec))
        return (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat()

    @staticmethod
    def _is_expired(session: dict[str, Any], now: datetime | None = None) -> bool:
        ref = now or datetime.now(timezone.utc)
        exp = _parse_iso_utc(session.get("expires_at"))
        if exp is not None:
            return exp <= ref
        raw_exp = str(session.get("expires_at") or "")
        return bool(raw_exp and raw_exp <= ref.isoformat())

    @staticmethod
    def _to_public_session(session: dict[str, Any]) -> dict[str, Any]:
        received_chunks = list(session.get("received_chunks") or [])
        return {
            "id": session["id"],
            "filename": session["filename"],
            "title": session.get("title"),
            "folder": session.get("folder"),
            "tags": list(session.get("tags") or []),
            "forward_to_telegram": bool(session.get("forward_to_telegram", True)),
            "force": bool(session.get("force", False)),
            "total_size": int(session["total_size"]),
            "chunk_size": int(session["chunk_size"]),
            "total_chunks": int(session["total_chunks"]),
            "received_chunks": received_chunks,
            "missing_chunks": list(session.get("missing_chunks") or []),
            "received_count": len(received_chunks),
            "received_bytes": int(session.get("received_bytes") or 0),
            "expected_sha256": session.get("expected_sha256"),
            "fingerprint": session.get("fingerprint"),
            "status": session["status"],
            "job_id": session.get("job_id"),
            "created_at": session["created_at"],
            "updated_at": session["updated_at"],
            "expires_at": session["expires_at"],
        }

    def resolve_inside_staging(self, path: Path | str) -> Path | None:
        try:
            staging_root = self.settings.staging_dir.resolve()
            candidate = Path(path)
            if candidate.is_symlink():
                return None
            cur = candidate
            while cur != cur.parent:
                if cur.is_symlink():
                    return None
                try:
                    if cur.resolve() == staging_root:
                        break
                except OSError:
                    return None
                cur = cur.parent
            resolved = candidate.resolve()
            if resolved == staging_root:
                return None
            if not resolved.is_relative_to(staging_root):
                return None
            return resolved
        except (OSError, ValueError):
            return None

    def _register_staging_dir(self, path: Path | str) -> None:
        if self.ingest is not None and hasattr(self.ingest, "register_staging_dir"):
            self.ingest.register_staging_dir(path)

    def _unregister_staging_dir(self, path: Path | str) -> None:
        if self.ingest is not None and hasattr(self.ingest, "unregister_staging_dir"):
            self.ingest.unregister_staging_dir(path)

    def _safe_delete_staging_path(self, target: Path | str | None) -> None:
        if not target:
            return
        resolved = self.resolve_inside_staging(target)
        if resolved is None:
            return
        if resolved.is_dir():
            shutil.rmtree(resolved, ignore_errors=True)
            self._unregister_staging_dir(resolved)
        else:
            resolved.unlink(missing_ok=True)

    def _cleanup_session_files(self, session: dict[str, Any]) -> None:
        staging_dir = session.get("staging_dir")
        if staging_dir:
            self._safe_delete_staging_path(staging_dir)

    def _cleanup_job_staged_files(self, job: dict[str, Any]) -> None:
        staged_raw = job.get("staged_path")
        if not staged_raw:
            return
        staged_file = Path(str(staged_raw))
        parent_resolved = self.resolve_inside_staging(staged_file.parent)
        if parent_resolved is not None:
            shutil.rmtree(parent_resolved, ignore_errors=True)
            self._unregister_staging_dir(parent_resolved)
            return
        self._safe_delete_staging_path(staged_file)

    @staticmethod
    def _validate_sha256(raw: str | None) -> str | None:
        if raw is None:
            return None
        clean = str(raw).strip().lower()
        if not clean:
            return None
        if not _SHA256_RE.fullmatch(clean):
            raise UploadError(400, "Invalid SHA-256 checksum (expected 64 hex characters)")
        return clean

    async def init_session(self, payload: Any) -> dict[str, Any]:
        safe_filename = _sanitize_upload_filename(payload.filename)
        total_size = int(payload.total_size)
        if total_size < 0:
            raise UploadError(400, "total_size must be non-negative")
        if total_size > self.settings.max_upload_bytes:
            raise UploadError(413, f"File exceeds {self.settings.max_upload_mb} MB limit.")

        expected_sha256 = self._validate_sha256(getattr(payload, "sha256", None))

        if expected_sha256 and not bool(getattr(payload, "force", False)):
            existing = self.db.find_media_by_sha256(expected_sha256)
            if existing:
                return {
                    "duplicate": True,
                    "existing_item": existing,
                    "session": None,
                }

        async with self._init_lock:
            max_sessions = max(1, int(self.settings.upload_max_concurrent_sessions))
            if self.db.count_active_upload_sessions() >= max(1, max_sessions - 5):
                await self.cleanup_expired_sessions()

            if self.db.count_active_upload_sessions() >= max_sessions:
                raise UploadError(
                    429,
                    "Too many concurrent upload sessions; please wait or cancel stale uploads.",
                )

            max_staging_bytes = self.settings.upload_max_staging_bytes
            if max_staging_bytes > 0:
                reserved = self.db.sum_active_upload_reserved_bytes()
                if reserved + total_size > max_staging_bytes:
                    await self.cleanup_expired_sessions()
                    await self.cleanup_expired_failed_uploads()
                    reserved = self.db.sum_active_upload_reserved_bytes()
                    if reserved + total_size > max_staging_bytes:
                        raise UploadError(507, "Staging quota exceeded.")

            self.settings.staging_dir.mkdir(parents=True, exist_ok=True)
            free_bytes = shutil.disk_usage(self.settings.staging_dir).free
            if free_bytes < total_size + self.settings.min_free_buffer_bytes:
                await self.cleanup_expired_sessions()
                await self.cleanup_expired_failed_uploads()
                free_bytes = shutil.disk_usage(self.settings.staging_dir).free
                if free_bytes < total_size + self.settings.min_free_buffer_bytes:
                    raise UploadError(507, "Insufficient disk space in staging directory.")

            req_chunk = getattr(payload, "chunk_size", None)
            if req_chunk is None:
                chunk_size = self.settings.upload_chunk_size_bytes
            else:
                chunk_size = max(
                    65536,
                    min(self.settings.upload_max_chunk_size_bytes, int(req_chunk)),
                )

            total_chunks = 0 if total_size == 0 else math.ceil(total_size / chunk_size)
            session_id = uuid.uuid4().hex[:16]
            session_dir = self.settings.staging_dir / f"up_{session_id}"
            session_dir.mkdir(parents=True, exist_ok=True)
            staging_file = session_dir / safe_filename

            try:
                staging_file.touch(exist_ok=True)
                self._register_staging_dir(session_dir)
                expires_at = self._new_expires_at()
                session = self.db.create_upload_session(
                    session_id=session_id,
                    filename=safe_filename,
                    title=getattr(payload, "title", None),
                    folder=getattr(payload, "folder", None),
                    tags=getattr(payload, "tags", None),
                    forward_to_telegram=bool(getattr(payload, "forward_to_telegram", True)),
                    force=bool(getattr(payload, "force", False)),
                    total_size=total_size,
                    chunk_size=chunk_size,
                    total_chunks=total_chunks,
                    expected_sha256=expected_sha256,
                    fingerprint=getattr(payload, "fingerprint", None),
                    staging_dir=str(session_dir.resolve()),
                    staging_file=str(staging_file.resolve()),
                    expires_at=expires_at,
                    status="uploading",
                )
            except Exception:
                shutil.rmtree(session_dir, ignore_errors=True)
                self._unregister_staging_dir(session_dir)
                raise

        return {
            "duplicate": False,
            "existing_item": None,
            "session": self._to_public_session(session),
        }

    async def get_session_status(self, session_id: str) -> dict[str, Any]:
        session = self.db.get_upload_session(session_id)
        if not session:
            raise UploadError(404, "Upload session not found")
        if session["status"] == "uploading" and self._is_expired(session):
            async with self._get_lock(session_id):
                fresh = self.db.get_upload_session(session_id)
                if fresh and fresh["status"] == "uploading" and self._is_expired(fresh):
                    self._cleanup_session_files(fresh)
                    updated = self.db.update_upload_session(session_id, status="expired")
                    if updated:
                        fresh = updated
                if fresh:
                    session = fresh
        return self._to_public_session(session)

    def list_sessions(self, status: str | None = None) -> list[dict[str, Any]]:
        return [self._to_public_session(s) for s in self.db.list_upload_sessions(status=status)]

    async def receive_chunk(
        self,
        session_id: str,
        chunk_index: int,
        body_iter: AsyncIterator[bytes] | bytes,
        *,
        expected_chunk_sha256: str | None = None,
    ) -> dict[str, Any]:
        session = self.db.get_upload_session(session_id)
        if not session:
            raise UploadError(404, "Upload session not found")
        if session["status"] == "expired":
            raise UploadError(410, "Upload session expired")
        if session["status"] != "uploading":
            raise UploadError(409, f"Upload session is {session['status']}")
        if self._is_expired(session):
            async with self._get_lock(session_id):
                fresh = self.db.get_upload_session(session_id)
                if fresh and fresh["status"] == "uploading" and self._is_expired(fresh):
                    self._cleanup_session_files(fresh)
                    self.db.update_upload_session(session_id, status="expired")
            raise UploadError(410, "Upload session expired")

        total_chunks = int(session["total_chunks"])
        total_size = int(session["total_size"])
        chunk_size = int(session["chunk_size"])
        if chunk_index < 0 or chunk_index >= total_chunks:
            raise UploadError(
                400,
                f"Invalid chunk index {chunk_index}; expected 0 <= index < {total_chunks}",
            )

        clean_chunk_sha = self._validate_sha256(expected_chunk_sha256)
        offset = chunk_index * chunk_size
        expected_len = min(chunk_size, total_size - offset)

        if isinstance(body_iter, (bytes, bytearray, memoryview)):
            if len(body_iter) > expected_len:
                raise UploadError(
                    413,
                    f"Chunk {chunk_index} exceeds expected size ({len(body_iter)} > {expected_len})",
                )
            data = bytes(body_iter)
        else:
            buf = bytearray()
            async for piece in body_iter:
                if not piece:
                    continue
                if len(buf) + len(piece) > expected_len:
                    raise UploadError(
                        413,
                        f"Chunk {chunk_index} exceeds expected size (> {expected_len})",
                    )
                buf.extend(piece)
            data = bytes(buf)

        if len(data) != expected_len:
            raise UploadError(
                400,
                f"Chunk {chunk_index} size mismatch (got {len(data)}, expected {expected_len})",
            )

        if clean_chunk_sha:
            actual_sha = hashlib.sha256(data).hexdigest()
            if actual_sha != clean_chunk_sha:
                raise UploadError(400, f"Chunk {chunk_index} SHA-256 mismatch")

        async with self._get_lock(session_id):
            fresh = self.db.get_upload_session(session_id)
            if not fresh:
                raise UploadError(404, "Upload session not found")
            if fresh["status"] == "expired":
                raise UploadError(410, "Upload session expired")
            if fresh["status"] != "uploading":
                raise UploadError(409, f"Upload session is {fresh['status']}")
            if self._is_expired(fresh):
                self._cleanup_session_files(fresh)
                self.db.update_upload_session(session_id, status="expired")
                raise UploadError(410, "Upload session expired")

            resolved_file = self.resolve_inside_staging(fresh["staging_file"])
            if resolved_file is None:
                raise UploadError(500, "Invalid upload staging file path")
            resolved_dir = self.resolve_inside_staging(fresh["staging_dir"])
            if resolved_dir is None or not resolved_dir.is_dir():
                raise UploadError(410, "Upload staging directory is unavailable")

            await asyncio.to_thread(_pwrite_all, resolved_file, offset, data)
            updated = self.db.mark_upload_chunk_received(
                session_id,
                chunk_index,
                len(data),
                self._new_expires_at(),
            )
            if not updated:
                raise UploadError(404, "Upload session not found")

        received_count = len(updated["received_chunks"])
        return {
            "session_id": session_id,
            "chunk_index": chunk_index,
            "chunk_size": len(data),
            "received_count": received_count,
            "total_chunks": int(updated["total_chunks"]),
            "received_bytes": int(updated["received_bytes"]),
            "complete_ready": received_count == int(updated["total_chunks"]),
        }

    async def complete_session(
        self,
        session_id: str,
        *,
        sha256: str | None = None,
    ) -> dict[str, Any]:
        clean_arg_sha = self._validate_sha256(sha256)
        if not self.db.get_upload_session(session_id):
            raise UploadError(404, "Upload session not found")

        terminal_reached = False
        try:
            async with self._get_lock(session_id):
                session = self.db.get_upload_session(session_id)
                if not session:
                    raise UploadError(404, "Upload session not found")

                if session["status"] == "completed" and session.get("job_id"):
                    terminal_reached = True
                    return {
                        "session_id": session_id,
                        "job_id": session["job_id"],
                        "status": "queued",
                    }
                if session["status"] == "expired":
                    terminal_reached = True
                    raise UploadError(410, "Upload session expired")
                if session["status"] != "uploading":
                    raise UploadError(409, f"Upload session is {session['status']}")
                if self._is_expired(session):
                    self._cleanup_session_files(session)
                    self.db.update_upload_session(session_id, status="expired")
                    terminal_reached = True
                    raise UploadError(410, "Upload session expired")

                missing = list(session.get("missing_chunks") or [])
                if missing:
                    raise UploadError(
                        409,
                        f"Upload incomplete; missing {len(missing)} chunk(s)",
                        extra={
                            "missing_chunks": missing[:50],
                            "missing_count": len(missing),
                        },
                    )

                expected_sha = session.get("expected_sha256")
                if clean_arg_sha and expected_sha and clean_arg_sha != expected_sha:
                    raise UploadError(
                        400,
                        "Provided SHA-256 does not match session expected_sha256",
                    )
                effective_sha256 = clean_arg_sha or expected_sha

                self.db.update_upload_session(session_id, status="completing")
                try:
                    resolved_file = self.resolve_inside_staging(session["staging_file"])
                    if resolved_file is None or not resolved_file.is_file():
                        self.db.update_upload_session(session_id, status="uploading")
                        raise UploadError(500, "Staged upload file is missing")

                    expected_size = int(session["total_size"])
                    actual_size = resolved_file.stat().st_size
                    if actual_size != expected_size:
                        self.db.update_upload_session(session_id, status="uploading")
                        raise UploadError(
                            400,
                            f"Staged file size mismatch (got {actual_size}, expected {expected_size})",
                        )

                    if effective_sha256:
                        actual_sha = (
                            await asyncio.to_thread(compute_file_sha256, resolved_file)
                        ).lower()
                        if actual_sha != effective_sha256:
                            self.db.update_upload_session(session_id, status="uploading")
                            raise UploadError(400, "SHA-256 checksum mismatch")

                    filename = session["filename"]
                    resolved_title = session.get("title") or Path(filename).stem
                    tags = list(session.get("tags") or [])
                    folder = session.get("folder")
                    forward_to_telegram = bool(session.get("forward_to_telegram", True))
                    force = bool(session.get("force", False))

                    job_params: dict[str, Any] = {
                        "folder": folder,
                        "tags": tags,
                        "title": resolved_title,
                        "forward_to_telegram": forward_to_telegram,
                        "force": force,
                        "source_origin": "web",
                        "staged_path": str(resolved_file),
                        "upload_session_id": session_id,
                    }
                    if effective_sha256:
                        job_params["precomputed_sha256"] = effective_sha256

                    job_id = self.db.create_job("web", "upload", filename, params=job_params)
                    self.db.update_job(
                        job_id,
                        status="queued",
                        progress=50.0,
                        message=f"Staged {filename}, queued for processing...",
                        staged_path=str(resolved_file),
                        staged_size_bytes=expected_size,
                    )
                    self.db.update_upload_session(
                        session_id,
                        status="completed",
                        job_id=job_id,
                        expected_sha256=effective_sha256,
                    )
                    terminal_reached = True
                except UploadError:
                    raise
                except Exception as exc:
                    self.db.update_upload_session(session_id, status="uploading")
                    raise UploadError(500, f"Failed to verify staged upload: {exc}") from exc

                if self.ingest is not None:
                    self.ingest.submit_job(
                        job_id,
                        self.ingest.process_staged_upload_job(
                            job_id,
                            resolved_file,
                            source_origin="web",
                            folder=folder,
                            tags=tags,
                            title=resolved_title,
                            forward_to_telegram=forward_to_telegram,
                            force=force,
                            precomputed_sha256=effective_sha256,
                        ),
                    )
        finally:
            if terminal_reached:
                self._release_lock_if_idle(session_id)

        return {
            "session_id": session_id,
            "job_id": job_id,
            "status": "queued",
        }

    async def cancel_session(self, session_id: str) -> dict[str, Any]:
        if not self.db.get_upload_session(session_id):
            raise UploadError(404, "Upload session not found")
        try:
            async with self._get_lock(session_id):
                session = self.db.get_upload_session(session_id)
                if not session:
                    raise UploadError(404, "Upload session not found")
                if session["status"] == "completed":
                    raise UploadError(409, "Upload session already completed")
                if session["status"] in ("cancelled", "expired"):
                    return self._to_public_session(session)

                self._cleanup_session_files(session)
                updated = self.db.update_upload_session(session_id, status="cancelled")
                assert updated is not None
                return self._to_public_session(updated)
        finally:
            self._release_lock_if_idle(session_id)

    async def cleanup_expired_sessions(self) -> int:
        now = datetime.now(timezone.utc)
        expired_list = self.db.list_expired_upload_sessions(now.isoformat())
        cleaned = 0
        for item in expired_list:
            sid = item["id"]
            async with self._get_lock(sid):
                fresh = self.db.get_upload_session(sid)
                if (
                    fresh
                    and fresh["status"] == "uploading"
                    and self._is_expired(fresh, now=now)
                ):
                    self._cleanup_session_files(fresh)
                    self.db.update_upload_session(sid, status="expired")
                    cleaned += 1
            self._release_lock_if_idle(sid)
        return cleaned

    def _cleanup_expired_failed_uploads_sync(self) -> int:
        with self._retention_lock:
            now = datetime.now(timezone.utc)
            now_iso = now.isoformat()
            jobs = self.db.list_retained_staged_jobs(include_expired=True)
            cleaned = 0
            for job in jobs:
                status = job.get("status")
                if status in _ACTIVE_JOB_STATUSES:
                    continue
                exp_dt = _parse_iso_utc(job.get("staged_expires_at"))
                is_expired = (
                    status == "done"
                    or (
                        exp_dt <= now
                        if exp_dt is not None
                        else bool(
                            job.get("staged_expires_at")
                            and str(job["staged_expires_at"]) <= now_iso
                        )
                    )
                )
                if not is_expired:
                    continue
                claimed = self.db.claim_staged_job_for_cleanup(
                    job["id"],
                    allowed_statuses=("failed", "cancelled", "done"),
                    require_expired_before=now_iso,
                )
                if claimed is None:
                    continue
                self._cleanup_job_staged_files(claimed)
                cleaned += 1
            return cleaned

    async def cleanup_expired_failed_uploads(self) -> int:
        return await asyncio.to_thread(self._cleanup_expired_failed_uploads_sync)

    async def run_gc(self) -> dict[str, int]:
        expired_sessions = await self.cleanup_expired_sessions()
        expired_failed = await self.cleanup_expired_failed_uploads()
        return {
            "expired_sessions": expired_sessions,
            "expired_failed_uploads": expired_failed,
        }

    def enforce_failed_upload_retention_budget(self, incoming_bytes: int) -> bool:
        with self._retention_lock:
            self._cleanup_expired_failed_uploads_sync()

            if int(self.settings.failed_upload_retention_sec) <= 0:
                return False
            budget = self.settings.failed_upload_max_retained_bytes
            if budget <= 0:
                return False
            incoming = max(0, int(incoming_bytes))
            if incoming > budget:
                return False

            retained = self.db.list_retained_staged_jobs(include_expired=True)
            evictable: list[tuple[dict[str, Any], int]] = []
            current_total = 0
            for job in retained:
                if job.get("status") not in _TERMINAL_RETAINABLE_JOB_STATUSES:
                    continue
                sz = int(job.get("staged_size_bytes") or 0)
                if sz <= 0 and job.get("staged_path"):
                    try:
                        p = Path(str(job["staged_path"]))
                        if p.is_file():
                            sz = p.stat().st_size
                    except OSError:
                        sz = 0
                evictable.append((job, sz))
                current_total += sz

            while current_total + incoming > budget and evictable:
                victim, victim_size = evictable.pop(0)
                claimed = self.db.claim_staged_job_for_cleanup(
                    victim["id"],
                    allowed_statuses=_TERMINAL_RETAINABLE_JOB_STATUSES,
                )
                if claimed is None:
                    continue
                self._cleanup_job_staged_files(claimed)
                current_total = max(0, current_total - victim_size)

            return (current_total + incoming) <= budget

    async def _gc_loop(self) -> None:
        interval = max(10, int(self.settings.staging_gc_interval_sec))
        while True:
            await asyncio.sleep(interval)
            try:
                await self.run_gc()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Staging GC pass failed: %s", exc)

    def start_gc(self) -> None:
        if int(self.settings.staging_gc_interval_sec) <= 0:
            return
        if self._gc_task is not None and not self._gc_task.done():
            return
        self._gc_task = asyncio.create_task(self._gc_loop(), name="staging-gc")

    async def stop_gc(self) -> None:
        task = self._gc_task
        self._gc_task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def get_protected_staging_dirs(self) -> set[Path]:
        protected: set[Path] = set()
        now = datetime.now(timezone.utc)
        session_scan_limit = max(500, int(self.settings.upload_max_concurrent_sessions) * 2)

        for status in _ACTIVE_SESSION_STATUSES:
            for session in self.db.list_upload_sessions(status=status, limit=session_scan_limit):
                resolved = self.resolve_inside_staging(session.get("staging_dir", ""))
                if resolved is not None:
                    protected.add(resolved)

        for job in self.db.list_retained_staged_jobs(include_expired=True):
            staged_raw = job.get("staged_path")
            if not staged_raw:
                continue
            status = job.get("status")
            keep = False
            if status in _ACTIVE_JOB_STATUSES:
                keep = True
            elif status in _TERMINAL_RETAINABLE_JOB_STATUSES:
                exp_dt = _parse_iso_utc(job.get("staged_expires_at"))
                if exp_dt is not None:
                    keep = exp_dt > now
                elif not job.get("staged_expires_at"):
                    keep = True
            if keep:
                resolved = self.resolve_inside_staging(Path(str(staged_raw)).parent)
                if resolved is not None:
                    protected.add(resolved)

        return protected

    def get_status_summary(self) -> dict[str, Any]:
        retained = [
            j
            for j in self.db.list_retained_staged_jobs(include_expired=False)
            if j.get("status") in _TERMINAL_RETAINABLE_JOB_STATUSES
        ]
        retained_bytes = sum(max(0, int(j.get("staged_size_bytes") or 0)) for j in retained)
        try:
            staging_free = (
                shutil.disk_usage(self.settings.staging_dir).free
                if self.settings.staging_dir.exists()
                else 0
            )
        except OSError:
            staging_free = 0

        return {
            "active_sessions": self.db.count_active_upload_sessions(),
            "reserved_bytes": self.db.sum_active_upload_reserved_bytes(),
            "max_concurrent_sessions": int(self.settings.upload_max_concurrent_sessions),
            "max_staging_bytes": self.settings.upload_max_staging_bytes,
            "session_ttl_sec": int(self.settings.upload_session_ttl_sec),
            "chunk_size_bytes": self.settings.upload_chunk_size_bytes,
            "max_chunk_size_bytes": self.settings.upload_max_chunk_size_bytes,
            "retained_failed_jobs": len(retained),
            "retained_failed_bytes": retained_bytes,
            "failed_upload_retention_sec": int(self.settings.failed_upload_retention_sec),
            "failed_upload_max_retained_bytes": self.settings.failed_upload_max_retained_bytes,
            "staging_free_bytes": staging_free,
            "gc_running": bool(self._gc_task and not self._gc_task.done()),
        }
