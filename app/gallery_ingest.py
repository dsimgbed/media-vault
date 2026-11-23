from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import app.ingest as ingest_mod
from app.allocator import fmt_size, sanitize_folder
from app.config import Settings
from app.db import Database, normalize_tags
from app.ingest import IMAGE_EXTS, IngestService, ProgressHook

logger = logging.getLogger(__name__)


class GalleryError(RuntimeError):
    """Base error for gallery-dl gallery ingestion."""


class GalleryUnavailableError(GalleryError):
    """gallery-dl is disabled or its binary is not installed (HTTP 503)."""


class GalleryQuotaError(GalleryError):
    """Gallery exceeds the configured size cap or staging free-space buffer."""


_gallery_dl_update_lock = asyncio.Lock()

# Version probes spawn ``gallery-dl --version``; cache per binary path so
# ``/api/health`` does not fork a process on every request.
_gallery_dl_version_cache: dict[str, str | None] = {}

_GALLERY_MAX_TOTAL_MB_DEFAULT = 4096

_IGNORED_DOWNLOAD_NAMES = {
    ".ds_store",
    "thumbs.db",
    "desktop.ini",
}
_IGNORED_DOWNLOAD_SUFFIXES = (
    ".part",
    ".ytdl",
    ".tmp",
    ".temp",
    ".crdownload",
    ".aria2",
    ".download",
)

_UNSAFE_TITLE_CHARS_RE = re.compile(r"[^\w\-. ]+", re.UNICODE)
_WHITESPACE_RE = re.compile(r"\s+")
_NATURAL_SPLIT_RE = re.compile(r"(\d+)")


def find_gallery_dl_cmd() -> list[str] | None:
    """Locate the gallery-dl executable (venv console script first)."""
    venv_gallery_dl = Path(sys.executable).parent / "gallery-dl"
    if venv_gallery_dl.is_file():
        return [str(venv_gallery_dl)]
    path_gallery_dl = shutil.which("gallery-dl")
    if path_gallery_dl:
        return [path_gallery_dl]
    return None


def _clear_gallery_dl_version_cache() -> None:
    _gallery_dl_version_cache.clear()


def get_gallery_dl_version_sync() -> str | None:
    """Return the installed gallery-dl version without blocking health checks."""
    try:
        return importlib.metadata.version("gallery-dl")
    except importlib.metadata.PackageNotFoundError:
        pass
    except Exception:
        pass

    cmd = find_gallery_dl_cmd()
    if cmd is None:
        return None

    cache_key = str(cmd[0])
    if cache_key in _gallery_dl_version_cache:
        return _gallery_dl_version_cache[cache_key]

    version: str | None = None
    try:
        result = subprocess.run(
            [*cmd, "--version"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if getattr(result, "returncode", 0) == 0:
            stdout = (getattr(result, "stdout", "") or "").strip()
            if stdout:
                version = stdout.splitlines()[0].strip() or None
    except (OSError, subprocess.SubprocessError, ValueError):
        version = None
    except Exception:
        version = None

    _gallery_dl_version_cache[cache_key] = version
    return version


def get_gallery_dl_status(settings: Settings | None = None) -> dict[str, Any]:
    """Describe gallery-dl availability and configured limits."""
    cmd = find_gallery_dl_cmd()
    enabled = bool(getattr(settings, "gallery_dl_enabled", True)) if settings else True
    return {
        "enabled": enabled,
        "installed": cmd is not None,
        "binary": cmd[0] if cmd else None,
        "version": get_gallery_dl_version_sync() if cmd is not None else None,
        "default_max_items": int(getattr(settings, "gallery_dl_default_max_items", 200))
        if settings
        else 200,
        "max_items_cap": int(getattr(settings, "gallery_dl_max_items_cap", 500))
        if settings
        else 500,
        "max_total_mb": int(getattr(settings, "gallery_dl_max_total_mb", 4096))
        if settings
        else 4096,
        "install_hint": "pip install gallery-dl" if cmd is None else None,
    }


async def update_gallery_dl() -> dict[str, Any]:
    """Upgrade gallery-dl in the current Python environment."""
    async with _gallery_dl_update_lock:
        cmd = find_gallery_dl_cmd()
        if cmd is None:
            raise GalleryUnavailableError(
                "gallery-dl is not installed. Install it with: pip install gallery-dl"
            )

        before = get_gallery_dl_version_sync()
        code, out, err = await ingest_mod._run_subprocess(
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "gallery-dl",
        )
        if code != 0:
            detail = (err or out).strip()[-500:]
            raise RuntimeError(f"Failed to update gallery-dl: {detail}")

        _clear_gallery_dl_version_cache()
        after = get_gallery_dl_version_sync()
        return {
            "ok": True,
            "before": before,
            "after": after,
            "updated": before != after,
            "output": out.strip()[-400:],
        }


def validate_gallery_url(url: str) -> str:
    """Validate and normalize a gallery-dl URL."""
    raw = str(url or "").strip()
    if not raw:
        raise ValueError("URL is required")
    if len(raw) > 4096:
        raise ValueError("URL must be at most 4096 characters")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in raw):
        raise ValueError("URL must not contain control characters")
    if any(ch.isspace() for ch in raw):
        raise ValueError("URL must not contain whitespace")
    if not raw.startswith(("http://", "https://")):
        raise ValueError("URL must start with http:// or https://")
    try:
        parts = urlsplit(raw)
    except ValueError as exc:
        raise ValueError("URL must start with http:// or https://") from exc
    if not parts.netloc:
        raise ValueError("URL must start with http:// or https://")
    return raw


def natural_sort_key(path: Path | str) -> list[tuple[int, Any]]:
    """Return a mixed text/numeric key so page_2 sorts before page_10."""
    if isinstance(path, Path):
        text = path.as_posix()
    else:
        text = str(path).replace("\\", "/")
    parts = _NATURAL_SPLIT_RE.split(text.lower())
    return [
        (0, int(token)) if token.isdigit() else (1, token)
        for token in parts
    ]


def _is_ignored_download_file(path: Path) -> bool:
    name = path.name.lower()
    if name in _IGNORED_DOWNLOAD_NAMES:
        return True
    # gallery-dl and download tools write hidden temporary/state files next to
    # completed media (for example ``.DS_Store`` or ``.<name>.part``).
    if name.startswith("."):
        return True
    return name.endswith(_IGNORED_DOWNLOAD_SUFFIXES)


def _safe_archive_suffix(path: Path) -> str:
    """Return a lowercase, path-traversal-free extension for an archive entry."""
    raw = path.suffix.lower()
    cleaned = "".join(ch for ch in raw if ch.isalnum() or ch in "._-")
    cleaned = cleaned.strip(".")
    if not cleaned:
        return ""
    extension = cleaned.split(".")[-1][:16]
    extension = "".join(ch for ch in extension if ch.isalnum())
    return f".{extension}" if extension else ""


def collect_safe_downloaded_files(
    download_dir: Path,
    *,
    max_total_bytes: int = 0,
) -> list[Path]:
    """Collect downloaded media safely, rejecting symlinks and size overruns."""
    root = Path(download_dir)
    if root.is_symlink():
        raise GalleryError("Refusing symlink inside gallery download directory")
    if not root.exists():
        return []

    root_resolved = root.resolve()
    collected: list[Path] = []
    total_bytes = 0

    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        base = Path(dirpath)
        for dirname in dirnames:
            if (base / dirname).is_symlink():
                raise GalleryError("Refusing symlink inside gallery download directory")
        for filename in filenames:
            candidate = base / filename
            if candidate.is_symlink():
                raise GalleryError("Refusing symlink inside gallery download directory")
            if _is_ignored_download_file(candidate):
                continue
            try:
                resolved = candidate.resolve()
            except (OSError, RuntimeError):
                continue
            if not resolved.is_relative_to(root_resolved):
                raise GalleryError("Refusing symlink inside gallery download directory")
            try:
                if not resolved.is_file():
                    continue
                size = int(resolved.stat().st_size)
            except OSError:
                continue

            total_bytes += size
            if max_total_bytes > 0 and total_bytes > max_total_bytes:
                raise GalleryQuotaError(
                    "Gallery download exceeds the "
                    f"{fmt_size(max_total_bytes)} size limit"
                )
            collected.append(resolved)

    collected.sort(
        key=lambda p: natural_sort_key(p.relative_to(root_resolved).as_posix())
    )
    return collected


def pack_gallery_archive(
    files: list[Path],
    out_path: Path,
    *,
    archive_mode: str = "cbz",
) -> tuple[Path, Path | None]:
    """Pack naturally sorted media files into a stored (uncompressed) CBZ/ZIP."""
    del archive_mode  # Both CBZ and ZIP use the same stored archive layout.
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    pad = max(3, len(str(len(files))))
    cover_image_path: Path | None = None

    with zipfile.ZipFile(
        out,
        "w",
        compression=zipfile.ZIP_STORED,
        allowZip64=True,
    ) as archive:
        for idx, path in enumerate(files, start=1):
            source = Path(path)
            arcname = f"{idx:0{pad}d}{_safe_archive_suffix(source)}"
            archive.write(source, arcname=arcname)
            if cover_image_path is None and source.suffix.lower() in IMAGE_EXTS:
                cover_image_path = source

    return out, cover_image_path


def build_gallery_dl_cmd(
    cmd: list[str],
    url: str,
    download_dir: Path,
    *,
    max_items: int,
    cookies_file: Path | None = None,
    proxy: str = "",
) -> list[str]:
    """Build a gallery-dl invocation, keeping ``--`` before untrusted URL data."""
    args: list[str] = [
        *cmd,
        "--dest",
        str(download_dir),
        "--range",
        f"1-{max(1, int(max_items))}",
        "--no-mtime",
    ]
    if cookies_file is not None:
        try:
            if cookies_file.is_file() and cookies_file.stat().st_size > 0:
                args.extend(["--cookies", str(cookies_file)])
        except OSError:
            pass
    proxy_value = (proxy or "").strip()
    if (
        proxy_value
        and not any(ch in proxy_value for ch in ("\r", "\n", "\x00", " ", "\t"))
        and proxy_value.startswith(("http://", "https://", "socks4://", "socks5://", "socks5h://"))
    ):
        args.extend(["--proxy", proxy_value])
    args.extend(["--", url])
    return args


def _sanitize_title_stem(raw: str, *, fallback: str = "gallery") -> str:
    """Turn a user/URL supplied title into a safe single filename stem."""
    text = "".join(
        ch for ch in str(raw or "") if ord(ch) >= 32 and ord(ch) != 127
    ).strip()
    text = text.replace("\\", "/")
    base = Path(text).name if text else ""
    stem = Path(base).stem if base not in ("", ".", "..") else ""
    stem = _UNSAFE_TITLE_CHARS_RE.sub("_", stem)
    stem = _WHITESPACE_RE.sub(" ", stem).strip(" ._-")
    if not stem or stem in (".", ".."):
        stem = fallback
    stem = stem[:200].strip(" ._-")
    return stem or fallback


def _gallery_fallback_title(clean_url: str) -> str:
    short_id = hashlib.sha256(clean_url.encode("utf-8", "ignore")).hexdigest()[:8]
    return f"gallery_{short_id}"


def _infer_gallery_title(
    files: list[Path],
    download_dir: Path,
    fallback: str,
) -> str:
    """Use the deepest common gallery-dl subdirectory, or a stable fallback."""
    try:
        root_resolved = Path(download_dir).resolve()
    except OSError:
        return fallback

    relative_parents: list[Path] = []
    for file_path in files:
        try:
            relative = Path(file_path).resolve().relative_to(root_resolved)
        except (OSError, ValueError):
            continue
        relative_parents.append(relative.parent)

    if not relative_parents:
        return fallback

    try:
        common = Path(
            os.path.commonpath([parent.as_posix() for parent in relative_parents])
        )
    except ValueError:
        return fallback

    if not common.as_posix() or common.as_posix() == ".":
        return fallback

    return _sanitize_title_stem(common.name, fallback=fallback)


class GalleryIngestService:
    """Run gallery-dl downloads and archive their output through IngestService."""

    def __init__(self, settings: Settings, db: Database, ingest: IngestService) -> None:
        self.settings = settings
        self.db = db
        self.ingest = ingest

    def get_status(self) -> dict[str, Any]:
        return get_gallery_dl_status(self.settings)

    def _staging_buffer_bytes(self) -> int:
        raw_mb = getattr(self.settings, "torrent_min_free_buffer_mb", None)
        if raw_mb is None:
            raw_mb = getattr(self.settings, "min_free_buffer_mb", 500)
        return max(0, int(raw_mb or 0)) * 1024 * 1024

    def _max_total_bytes(self) -> int:
        configured = getattr(self.settings, "gallery_dl_max_total_bytes", None)
        if configured is not None:
            return max(0, int(configured))
        max_total_mb = getattr(
            self.settings,
            "gallery_dl_max_total_mb",
            _GALLERY_MAX_TOTAL_MB_DEFAULT,
        )
        return max(0, int(max_total_mb or 0)) * 1024 * 1024

    def _ensure_staging_free(self, buffer_bytes: int) -> None:
        staging = Path(self.settings.staging_dir)
        try:
            staging.mkdir(parents=True, exist_ok=True)
            free_bytes = int(shutil.disk_usage(staging).free)
        except OSError as exc:
            raise GalleryError(f"Unable to check staging disk space: {exc}") from exc

        if free_bytes < buffer_bytes:
            raise GalleryQuotaError(
                "Insufficient staging disk space: "
                f"{fmt_size(free_bytes)} free, "
                f"{fmt_size(buffer_bytes)} buffer required."
            )

    @staticmethod
    def _staging_free_bytes(staging: Path) -> int:
        try:
            return int(shutil.disk_usage(staging).free)
        except (OSError, AttributeError, TypeError):
            return 0

    @staticmethod
    def _terminate_proc(proc: asyncio.subprocess.Process | None) -> None:
        if proc is None or getattr(proc, "returncode", None) is not None:
            return
        try:
            proc.terminate()
        except (ProcessLookupError, AttributeError, OSError):
            pass

    async def ingest_gallery_job(
        self,
        job_id: str,
        url: str,
        *,
        source_origin: str = "web",
        folder: str | None = None,
        tags: str | list[str] | None = None,
        title: str | None = None,
        archive_mode: str = "items",
        max_items: int | None = None,
        forward_to_telegram: bool = True,
        force: bool = False,
        on_progress: ProgressHook | None = None,
    ) -> list[dict[str, Any]]:
        job_dir: Path | None = None
        slot_acquired = False
        try:
            if job_id in self.ingest._cancelled_jobs:
                raise asyncio.CancelledError()

            if not bool(getattr(self.settings, "gallery_dl_enabled", True)):
                raise GalleryUnavailableError(
                    "gallery-dl is disabled. Set GALLERY_DL_ENABLED=true to enable it."
                )
            cmd = find_gallery_dl_cmd()
            if cmd is None:
                raise GalleryUnavailableError(
                    "gallery-dl is not installed. Install it with: pip install gallery-dl"
                )

            clean_url = validate_gallery_url(url)
            mode = (archive_mode or "items").strip().lower()
            if mode not in ("items", "cbz", "zip"):
                raise ValueError("archive_mode must be one of: items, cbz, zip")

            raw_default = getattr(self.settings, "gallery_dl_default_max_items", 200)
            default_max = int(raw_default) if raw_default is not None else 200
            raw_cap = getattr(self.settings, "gallery_dl_max_items_cap", 500)
            cap = max(1, int(raw_cap) if raw_cap is not None else 500)
            bounded_max = max(1, min(cap, int(max_items or default_max)))
            max_total_bytes = self._max_total_bytes()
            buffer_bytes = self._staging_buffer_bytes()

            staging = Path(self.settings.staging_dir)
            await asyncio.to_thread(self._ensure_staging_free, buffer_bytes)

            sem = self.ingest.semaphore
            slot_acquired = False
            self.ingest._active_slots = getattr(self.ingest, "_active_slots", 0) + 1
            slot_acquired = True
            async with sem:
                if job_id in self.ingest._cancelled_jobs:
                    raise asyncio.CancelledError()

                job_dir = Path(
                    tempfile.mkdtemp(prefix="gallery_", dir=self.settings.staging_dir)
                )
                download_dir = job_dir / "dl"
                download_dir.mkdir(parents=True, exist_ok=True)
                self.ingest.register_staging_dir(job_dir)

                self.db.update_job(
                    job_id,
                    status="downloading",
                    progress=1.0,
                    message="Starting gallery-dl...",
                )
                effective_proxy = await self.ingest.get_effective_proxy()

                cookies_file = getattr(self.settings, "ytdlp_cookies_file", None)
                if isinstance(cookies_file, str):
                    cookies_file = Path(cookies_file)
                iso_cookies: Path | None = None
                if cookies_file and cookies_file.is_file() and cookies_file.stat().st_size > 0:
                    iso_cookies = job_dir / "_cookies.txt"
                    try:
                        shutil.copyfile(cookies_file, iso_cookies)
                    except OSError:
                        iso_cookies = None
                args = build_gallery_dl_cmd(
                    cmd,
                    clean_url,
                    download_dir,
                    max_items=bounded_max,
                    cookies_file=iso_cookies,
                    proxy=effective_proxy,
                )

                proc_ref: dict[str, asyncio.subprocess.Process] = {}
                quota_exc: list[GalleryQuotaError | None] = [None]
                downloaded_count = 0
                last_report = 0.0
                last_disk_check = 0.0

                def _on_proc(proc: asyncio.subprocess.Process) -> None:
                    proc_ref["proc"] = proc
                    self.ingest.register_proc(job_id, proc)

                def _measure_download_dir() -> int:
                    running_bytes = 0
                    for dp, _, fns in os.walk(download_dir, followlinks=False):
                        for fn in fns:
                            fp = Path(dp) / fn
                            if fp.is_symlink():
                                continue
                            try:
                                running_bytes += fp.stat().st_size
                            except OSError:
                                continue
                    return running_bytes

                async def _check_gallery_limits() -> None:
                    free_bytes = await asyncio.to_thread(self._staging_free_bytes, staging)
                    if free_bytes < buffer_bytes:
                        raise GalleryQuotaError(
                            "Staging disk space dropped below the configured "
                            f"{fmt_size(buffer_bytes)} buffer."
                        )
                    if max_total_bytes > 0:
                        running_bytes = await asyncio.to_thread(_measure_download_dir)
                        if running_bytes > max_total_bytes:
                            raise GalleryQuotaError(
                                "Gallery download exceeds the "
                                f"{fmt_size(max_total_bytes)} size limit"
                            )

                async def _quota_watchdog() -> None:
                    try:
                        while True:
                            await asyncio.sleep(3.0)
                            if job_id in self.ingest._cancelled_jobs:
                                self._terminate_proc(proc_ref.get("proc"))
                                return
                            try:
                                await _check_gallery_limits()
                            except GalleryQuotaError as exc:
                                quota_exc[0] = exc
                                self._terminate_proc(proc_ref.get("proc"))
                                return
                    except asyncio.CancelledError:
                        return

                async def handle_line(line: str) -> None:
                    nonlocal downloaded_count, last_report, last_disk_check
                    if job_id in self.ingest._cancelled_jobs:
                        self._terminate_proc(proc_ref.get("proc"))
                        raise asyncio.CancelledError()
                    if quota_exc[0] is not None:
                        raise quota_exc[0]
                    now = time.monotonic()

                    if now - last_disk_check >= 5.0:
                        last_disk_check = now
                        try:
                            await _check_gallery_limits()
                        except GalleryQuotaError as exc:
                            quota_exc[0] = exc
                            self._terminate_proc(proc_ref.get("proc"))
                            raise

                    text = line.strip()
                    # gallery-dl warnings/errors are prefixed with ``[site]``.
                    if not text or text.startswith("["):
                        return
                    downloaded_count += 1
                    if now - last_report >= 0.7 or downloaded_count == 1:
                        last_report = now
                        progress = min(
                            65.0,
                            1.0 + (downloaded_count / float(bounded_max)) * 64.0,
                        )
                        message = (
                            f"[gallery-dl] Downloaded {downloaded_count} item(s)..."
                        )
                        self.db.update_job(
                            job_id,
                            status="downloading",
                            progress=progress,
                            message=message,
                        )
                        if on_progress:
                            await on_progress(progress, message)

                streaming_kwargs: dict[str, Any] = {"on_line": handle_line}
                if ingest_mod._accepts_kwarg(
                    ingest_mod._run_subprocess_streaming, "on_proc"
                ):
                    streaming_kwargs["on_proc"] = _on_proc

                watchdog_task = asyncio.create_task(_quota_watchdog())
                try:
                    code, tail = await ingest_mod._run_subprocess_streaming(
                        *args, **streaming_kwargs
                    )
                finally:
                    watchdog_task.cancel()
                    try:
                        await watchdog_task
                    except asyncio.CancelledError:
                        pass
                    proc = proc_ref.get("proc")
                    if proc is not None:
                        try:
                            self.ingest.unregister_proc(job_id, proc)
                        except Exception:
                            logger.debug(
                                "Failed to unregister gallery-dl proc for job %s",
                                job_id,
                                exc_info=True,
                            )

                if quota_exc[0] is not None:
                    raise quota_exc[0]

                if code != 0:
                    detail = " | ".join(tail[-5:])[-400:] if tail else "unknown error"
                    raise GalleryError(f"gallery-dl failed: {detail}")
                if job_id in self.ingest._cancelled_jobs:
                    raise asyncio.CancelledError()

                await asyncio.to_thread(self._ensure_staging_free, buffer_bytes)

                files = await asyncio.to_thread(
                    collect_safe_downloaded_files,
                    download_dir,
                    max_total_bytes=max_total_bytes,
                )
                if not files:
                    raise GalleryError(
                        "gallery-dl finished but downloaded no files."
                    )

                safe_folder = sanitize_folder(folder, self.settings.default_folder)
                normalized_tags = normalize_tags(tags)
                fallback_title = _gallery_fallback_title(clean_url)
                if title and str(title).strip():
                    gallery_title = _sanitize_title_stem(
                        str(title), fallback=fallback_title
                    )
                else:
                    gallery_title = _infer_gallery_title(
                        files, download_dir, fallback_title
                    )

                items: list[dict[str, Any]] = []

                if mode in ("cbz", "zip"):
                    archive_title = gallery_title or fallback_title
                    archive_path = job_dir / f"{archive_title}.{mode}"
                    packing_message = (
                        f"[gallery-dl] Packing {len(files)} file(s) "
                        f"into .{mode}..."
                    )
                    self.db.update_job(
                        job_id,
                        status="allocating",
                        progress=70.0,
                        message=packing_message,
                    )
                    if on_progress:
                        await on_progress(70.0, packing_message)

                    await asyncio.to_thread(
                        pack_gallery_archive,
                        files,
                        archive_path,
                        archive_mode=mode,
                    )
                    # Free the raw pages before the archive is uploaded/mirrored.
                    shutil.rmtree(download_dir, ignore_errors=True)

                    async def report_archive(
                        pct: float, message: str
                    ) -> None:
                        overall = 70.0 + (
                            max(0.0, min(100.0, float(pct))) / 100.0
                        ) * 29.0
                        self.db.update_job(
                            job_id,
                            status="allocating",
                            progress=overall,
                            message=message,
                        )
                        if on_progress:
                            await on_progress(overall, message)

                    item = await self.ingest.finalize_staged_file(
                        archive_path,
                        folder=safe_folder,
                        tags=normalized_tags,
                        source_origin=source_origin,
                        source_kind="gallery",
                        source_url=clean_url,
                        title=archive_title,
                        forward_to_telegram=forward_to_telegram,
                        progress_cb=report_archive,
                        force=force,
                    )
                    items.append(item)
                else:
                    total = len(files)
                    for idx, file_path in enumerate(files, start=1):
                        if job_id in self.ingest._cancelled_jobs:
                            raise asyncio.CancelledError()

                        segment_start = 65.0 + (
                            ((idx - 1) / max(1, total)) * 34.0
                        )
                        segment_end = 65.0 + (
                            (idx / max(1, total)) * 34.0
                        )
                        item_message = (
                            f"[gallery-dl] Archiving {idx}/{total}: "
                            f"{file_path.name}"
                        )
                        self.db.update_job(
                            job_id,
                            status="allocating",
                            progress=min(99.0, segment_start),
                            message=item_message,
                        )
                        if on_progress:
                            await on_progress(
                                min(99.0, segment_start), item_message
                            )

                        async def report_item(
                            pct: float,
                            message: str,
                            *,
                            _start: float = segment_start,
                            _end: float = segment_end,
                        ) -> None:
                            span = max(0.0, _end - _start)
                            overall = _start + (
                                max(0.0, min(100.0, float(pct))) / 100.0
                            ) * span
                            overall = max(0.0, min(99.0, overall))
                            self.db.update_job(
                                job_id,
                                status="allocating",
                                progress=overall,
                                message=message,
                            )
                            if on_progress:
                                await on_progress(overall, message)

                        file_source_url = f"{clean_url}#item={idx}"
                        item = await self.ingest.finalize_staged_file(
                            file_path,
                            folder=safe_folder,
                            tags=normalized_tags,
                            source_origin=source_origin,
                            source_kind="gallery",
                            source_url=file_source_url,
                            title=file_path.stem,
                            forward_to_telegram=forward_to_telegram,
                            progress_cb=report_item,
                            force=force,
                        )
                        file_path.unlink(missing_ok=True)
                        items.append(item)

                if items and all(i.get("deduplicated") for i in items):
                    done_message = (
                        f"Duplicate gallery skipped ({len(items)} item(s) already archived)"
                    )
                else:
                    done_message = (
                        f"Saved {len(items)} gallery item(s) to {safe_folder}"
                    )
                self.db.update_job(
                    job_id,
                    status="done",
                    progress=100.0,
                    message=done_message,
                    media_ids_json=json.dumps([item["id"] for item in items]),
                )
                if on_progress:
                    await on_progress(100.0, done_message)
                return items

        except asyncio.CancelledError:
            job_tasks = getattr(self.ingest, "_job_tasks", {})
            if job_tasks.get(job_id) in (None, asyncio.current_task()):
                job = self.db.get_job(job_id)
                if not job or job.get("status") != "cancelled":
                    self.db.update_job(
                        job_id,
                        status="cancelled",
                        message="Cancelled by user",
                    )
            raise
        except Exception as exc:
            job_tasks = getattr(self.ingest, "_job_tasks", {})
            if job_tasks.get(job_id) not in (None, asyncio.current_task()):
                raise
            if job_id in self.ingest._cancelled_jobs:
                self.db.update_job(
                    job_id,
                    status="cancelled",
                    message="Cancelled by user",
                )
                raise asyncio.CancelledError() from exc
            self.db.update_job(
                job_id,
                status="failed",
                message=str(exc),
            )
            raise
        finally:
            if slot_acquired:
                self.ingest._active_slots = max(
                    0, getattr(self.ingest, "_active_slots", 0) - 1
                )
            if job_dir is not None:
                shutil.rmtree(job_dir, ignore_errors=True)
                try:
                    self.ingest.unregister_staging_dir(job_dir)
                except Exception:
                    logger.debug(
                        "Failed to unregister gallery staging dir %s",
                        job_dir,
                        exc_info=True,
                    )
            job_tasks = getattr(self.ingest, "_job_tasks", {})
            if job_tasks.get(job_id) in (None, asyncio.current_task()):
                active_procs = getattr(self.ingest, "_active_procs", None)
                if isinstance(active_procs, dict):
                    active_procs.pop(job_id, None)
