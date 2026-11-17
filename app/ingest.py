from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
import mimetypes
import re
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import unquote, urlparse

import httpx

from app.allocator import DriveAllocator, fmt_size, sanitize_folder
from app.config import Settings
from app.db import Database, normalize_tags
from app.thumbnails import extract_media_metadata, generate_thumbnail, generate_video_preview

logger = logging.getLogger(__name__)

DDL_EXTENSIONS = {
    ".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v",
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".bmp",
    ".mp3", ".flac", ".wav", ".m4a", ".ogg",
    ".zip", ".rar", ".tar", ".gz", ".bz2", ".xz", ".zst", ".7z", ".iso", ".pdf",
}

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".bmp", ".avif"}
VIDEO_EXTS = {".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v"}
AUDIO_EXTS = {".mp3", ".flac", ".wav", ".m4a", ".ogg", ".opus"}

MirrorHook = Callable[[dict[str, Any]], Awaitable[dict[str, Any] | None]]
ProgressHook = Callable[[float, str], Awaitable[None]]


def classify_media(path: Path) -> tuple[str, str]:
    ext = path.suffix.lower()
    mime, _ = mimetypes.guess_type(path.name)
    if ext in IMAGE_EXTS or (mime and mime.startswith("image/")):
        return "image", mime or "image/jpeg"
    if ext in VIDEO_EXTS or (mime and mime.startswith("video/")):
        return "video", mime or "video/mp4"
    if ext in AUDIO_EXTS or (mime and mime.startswith("audio/")):
        return "audio", mime or "audio/mpeg"
    return "file", mime or "application/octet-stream"


async def is_direct_download(url: str) -> bool:
    ext = Path(urlparse(url).path).suffix.lower()
    if ext in DDL_EXTENSIONS:
        return True
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
            resp = await client.head(url)
            ctype = resp.headers.get("content-type", "").lower()
            if ctype and not ctype.startswith(("text/html", "application/xhtml")):
                return True
    except httpx.HTTPError:
        pass
    return False


_FORMAT_SAFE_RE = re.compile(r"^[A-Za-z0-9_+/\[\]<=.,:-]{1,96}$")
_PCT_RE = re.compile(r"(\d{1,3}(?:\.\d+)?)%")
_YTDLP_DL_RE = re.compile(r"\[download\]\s+(\d{1,3}(?:\.\d+)?)%(?:\s+of\s+~?\s*(\S+))?(?:\s+at\s+(\S+))?(?:\s+ETA\s+(\S+))?")
_ARIA_PCT_RE = re.compile(r"\((\d{1,3})%\).*?(?:DL:(\S+))?")


async def _run_subprocess(*args: str) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stdout.decode(errors="ignore"), stderr.decode(errors="ignore")


async def _run_subprocess_streaming(
    *args: str,
    on_line: Callable[[str], Awaitable[None]] | None = None,
) -> tuple[int, list[str]]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    tail: deque[str] = deque(maxlen=80)
    assert proc.stdout is not None
    pending = b""
    while True:
        chunk = await proc.stdout.read(16384)
        if not chunk:
            break
        pending += chunk.replace(b"\r", b"\n")
        parts = pending.split(b"\n")
        pending = parts.pop()
        for raw_line in parts:
            line = raw_line.decode(errors="ignore").strip()
            if line:
                tail.append(line)
                if on_line:
                    await on_line(line)
    if pending:
        line = pending.decode(errors="ignore").strip()
        if line:
            tail.append(line)
            if on_line:
                await on_line(line)
    rc = await proc.wait()
    return rc, list(tail)


async def _download_ddl(
    url: str,
    job_dir: Path,
    *,
    on_progress: ProgressHook | None = None,
) -> list[Path]:
    last_report = 0.0

    async def report(pct: float, msg: str) -> None:
        nonlocal last_report
        now = time.monotonic()
        if on_progress and (now - last_report >= 0.7 or pct >= 99.9):
            last_report = now
            await on_progress(pct, msg)

    aria2c = shutil.which("aria2c")
    if aria2c:
        async def handle_aria_line(line: str) -> None:
            m = _ARIA_PCT_RE.search(line)
            if m:
                pct = float(m.group(1))
                speed = f" ({m.group(2)})" if m.group(2) else ""
                await report(pct, f"Downloading{speed}: {pct:.0f}%")

        code, tail = await _run_subprocess_streaming(
            aria2c,
            "--dir",
            str(job_dir),
            "--max-connection-per-server=4",
            "--content-disposition-default-utf8=true",
            "--auto-file-renaming=true",
            "--summary-interval=1",
            url,
            on_line=handle_aria_line,
        )
        if code != 0:
            raise RuntimeError(f"aria2c failed: {' | '.join(tail[-5:])[-300:]}")
    else:
        async with httpx.AsyncClient(follow_redirects=True, timeout=None) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length") or 0)
                filename = ""
                cd = resp.headers.get("content-disposition", "")
                if "filename=" in cd:
                    filename = cd.split("filename=")[-1].strip('"; ')
                if not filename:
                    filename = Path(unquote(urlparse(str(resp.url)).path)).name or f"download_{uuid.uuid4().hex[:8]}"
                target = job_dir / Path(filename).name
                received = 0
                with target.open("wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
                        received += len(chunk)
                        if total > 0:
                            pct = (received / total) * 100.0
                            await report(pct, f"Downloading: {pct:.0f}% ({fmt_size(received)} / {fmt_size(total)})")
                        else:
                            await report(25.0, f"Downloading: {fmt_size(received)}")

    files = [p for p in job_dir.iterdir() if p.is_file() and not p.name.endswith(".aria2")]
    if not files:
        raise RuntimeError("Direct download produced no file.")
    return files


_ytdlp_update_lock = asyncio.Lock()


def _ytdlp_cmd() -> list[str]:
    venv_ytdlp = Path(sys.executable).parent / "yt-dlp"
    if venv_ytdlp.is_file():
        return [str(venv_ytdlp)]
    path_ytdlp = shutil.which("yt-dlp")
    if path_ytdlp:
        return [path_ytdlp]
    return [sys.executable, "-m", "yt_dlp"]


def _ytdlp_base_args(cookies_file: Path | None = None, proxy: str = "") -> list[str]:
    args: list[str] = [*_ytdlp_cmd(), "--no-playlist"]
    venv_deno = Path(sys.executable).parent / "deno"
    if venv_deno.is_file():
        args.extend(["--js-runtimes", f"deno:{venv_deno}"])
    if cookies_file and cookies_file.is_file() and cookies_file.stat().st_size > 0:
        args.extend(["--cookies", str(cookies_file)])
    if proxy.strip():
        args.extend(["--proxy", proxy.strip()])
    return args


def resolve_format_spec(format_spec: str | None) -> tuple[list[str], bool]:
    spec = (format_spec or "best").strip()
    if not spec or spec == "best":
        return ["-f", "bv*+ba/b", "--merge-output-format", "mp4"], False
    if spec == "1080p":
        return ["-f", "bv*[height<=1080]+ba/b[height<=1080]/b", "--merge-output-format", "mp4"], False
    if spec == "720p":
        return ["-f", "bv*[height<=720]+ba/b[height<=720]/b", "--merge-output-format", "mp4"], False
    if spec == "480p":
        return ["-f", "bv*[height<=480]+ba/b[height<=480]/b", "--merge-output-format", "mp4"], False
    if spec == "audio":
        return ["-f", "ba/b", "-x", "--audio-format", "mp3"], True
    if spec.startswith("audio:"):
        fmt_id = spec[6:].strip()
        if not _FORMAT_SAFE_RE.match(fmt_id):
            raise ValueError("Invalid audio format ID")
        return ["-f", fmt_id], True
    if not _FORMAT_SAFE_RE.match(spec):
        raise ValueError("Invalid format specification")
    return ["-f", spec, "--merge-output-format", "mp4"], False


async def list_ytdlp_formats(
    url: str,
    *,
    cookies_file: Path | None = None,
    proxy: str = "",
) -> dict[str, Any]:
    args = [
        *_ytdlp_base_args(cookies_file=cookies_file, proxy=proxy),
        "-J",
        url,
    ]
    code, out, err = await _run_subprocess(*args)
    if code != 0:
        raise RuntimeError(_format_ytdlp_error(err or out))
    try:
        info = json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse yt-dlp metadata: {exc}")

    entries = info.get("entries")
    if isinstance(entries, list) and entries:
        info = entries[0] or info

    options: list[dict[str, Any]] = []
    for fmt in info.get("formats") or []:
        fid = str(fmt.get("format_id") or "").strip()
        if not fid:
            continue
        ext = str(fmt.get("ext") or "")
        vcodec = str(fmt.get("vcodec") or "none")
        acodec = str(fmt.get("acodec") or "none")
        if ext == "mhtml" or (vcodec == "none" and acodec == "none"):
            continue

        has_v = vcodec != "none"
        has_a = acodec != "none"
        if has_v and has_a:
            kind = "video+audio"
            select_spec = fid
        elif has_v:
            kind = "video"
            select_spec = f"{fid}+bestaudio/{fid}"
        else:
            kind = "audio"
            select_spec = f"audio:{fid}"

        options.append({
            "id": fid,
            "select_spec": select_spec,
            "kind": kind,
            "ext": ext,
            "resolution": fmt.get("resolution") or (f"{fmt.get('height')}p" if fmt.get("height") else "audio"),
            "height": fmt.get("height") or 0,
            "fps": fmt.get("fps"),
            "vcodec": vcodec if has_v else "",
            "acodec": acodec if has_a else "",
            "size_bytes": fmt.get("filesize") or fmt.get("filesize_approx") or 0,
            "note": fmt.get("format_note") or "",
        })

    # Show highest-res video+audio / video first, then audio-only
    kind_rank = {"video+audio": 0, "video": 1, "audio": 2}
    options.sort(
        key=lambda o: (kind_rank.get(o["kind"], 3), -(o["height"] or 0), -(o["fps"] or 0), -(o["size_bytes"] or 0))
    )
    return {
        "title": info.get("title") or url,
        "duration": info.get("duration"),
        "webpage_url": info.get("webpage_url") or url,
        "formats": options,
    }


async def get_ytdlp_version() -> str:
    try:
        code, out, _ = await _run_subprocess(*_ytdlp_cmd(), "--version")
        if code == 0 and out.strip():
            return out.strip().splitlines()[-1].strip()
    except Exception:
        pass
    try:
        from importlib.metadata import version

        return version("yt-dlp")
    except Exception:
        return "unknown"


async def update_ytdlp() -> dict[str, Any]:
    async with _ytdlp_update_lock:
        before = await get_ytdlp_version()
        code, out, err = await _run_subprocess(
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "yt-dlp[default]",
        )
        if code != 0:
            detail = (err or out).strip()[-500:]
            raise RuntimeError(f"Failed to update yt-dlp: {detail}")
        after = await get_ytdlp_version()
        return {
            "ok": True,
            "before": before,
            "after": after,
            "updated": before != after,
            "output": out.strip()[-400:],
        }


def _format_ytdlp_error(stderr: str) -> str:
    err_lines = [line.strip() for line in stderr.splitlines() if line.strip().startswith("ERROR:")]
    detail = err_lines[-1] if err_lines else stderr.strip()[-400:]
    lower = detail.lower()
    if "not a bot" in lower or ("cookies" in lower and "youtube" in lower):
        return (
            "yt-dlp blocked by YouTube bot check on this server IP. "
            "Upload a Netscape cookies.txt via the Web UI (YouTube Cookies) "
            "or set YTDLP_COOKIES_FILE / YTDLP_PROXY in .env."
        )
    return f"yt-dlp failed: {detail[:400]}"


async def _download_ytdlp(
    url: str,
    job_dir: Path,
    *,
    cookies_file: Path | None = None,
    proxy: str = "",
    format_spec: str | None = None,
    concurrent_fragments: int = 4,
    on_progress: ProgressHook | None = None,
) -> list[Path]:
    fmt_args, is_audio = resolve_format_spec(format_spec)
    args: list[str] = [
        *_ytdlp_base_args(cookies_file=cookies_file, proxy=proxy),
        "--newline",
        "--progress",
        "--concurrent-fragments",
        str(max(1, int(concurrent_fragments))),
        "--paths",
        str(job_dir),
        "-o",
        "%(title).100s [%(id)s].%(ext)s",
        *fmt_args,
    ]
    if not is_audio:
        args.extend(["--postprocessor-args", "Merger+ffmpeg:-movflags +faststart"])
    args.append(url)

    last_report = 0.0

    async def handle_line(line: str) -> None:
        nonlocal last_report
        if not on_progress:
            return
        m = _YTDLP_DL_RE.search(line)
        if m:
            pct = float(m.group(1))
            now = time.monotonic()
            if now - last_report >= 0.7 or pct >= 99.9:
                last_report = now
                parts = [f"Downloading {pct:.0f}%"]
                if m.group(2):
                    parts.append(f"of {m.group(2)}")
                if m.group(3):
                    parts.append(f"at {m.group(3)}")
                if m.group(4):
                    parts.append(f"ETA {m.group(4)}")
                await on_progress(pct, " ".join(parts))
        elif "[Merger]" in line or "[ExtractAudio]" in line:
            await on_progress(98.0, "Post-processing media...")

    code, tail = await _run_subprocess_streaming(*args, on_line=handle_line)
    if code != 0:
        raise RuntimeError(_format_ytdlp_error("\n".join(tail)))

    files = [p for p in job_dir.iterdir() if p.is_file()]
    if not files:
        raise RuntimeError("yt-dlp finished but produced no file.")
    return files


class IngestService:
    def __init__(self, settings: Settings, db: Database, allocator: DriveAllocator) -> None:
        self.settings = settings
        self.db = db
        self.allocator = allocator
        self.mirror_hook: MirrorHook | None = None
        self._configured_concurrency = max(1, int(settings.ingest_concurrency))
        self._semaphore = asyncio.Semaphore(self._configured_concurrency)
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._active_slots = 0

    @property
    def semaphore(self) -> asyncio.Semaphore:
        target = max(1, int(self.settings.ingest_concurrency))
        if target != self._configured_concurrency and not self._background_tasks and self._active_slots == 0:
            self._configured_concurrency = target
            self._semaphore = asyncio.Semaphore(target)
        return self._semaphore

    def recover_on_startup(self) -> dict[str, int]:
        recovered_jobs = self.db.recover_interrupted_jobs()
        cleaned_staging = 0
        staging = self.settings.staging_dir
        if staging.exists():
            for entry in staging.iterdir():
                if entry.is_dir() and not entry.is_symlink():
                    shutil.rmtree(entry, ignore_errors=True)
                else:
                    entry.unlink(missing_ok=True)
                cleaned_staging += 1
        staging.mkdir(parents=True, exist_ok=True)
        return {
            "recovered_jobs": recovered_jobs,
            "cleaned_staging_entries": cleaned_staging,
        }

    def submit_job(self, job_id: str, coro: Awaitable[Any]) -> asyncio.Task[Any]:
        async def _runner() -> Any:
            try:
                return await coro
            except asyncio.CancelledError:
                job = self.db.get_job(job_id)
                if job and job.get("status") not in ("done", "failed"):
                    self.db.update_job(job_id, status="failed", message="Job cancelled")
                raise
            except Exception as exc:
                logger.warning("Background ingest job %s failed: %s", job_id, exc)
                job = self.db.get_job(job_id)
                if job and job.get("status") not in ("done", "failed"):
                    self.db.update_job(job_id, status="failed", message=str(exc))
                return None

        task = asyncio.create_task(_runner())
        self._background_tasks.add(task)

        def _on_done(t: asyncio.Task[Any]) -> None:
            self._background_tasks.discard(t)
            if hasattr(coro, "close"):
                try:
                    coro.close()  # type: ignore[attr-defined]
                except RuntimeError:
                    pass

        task.add_done_callback(_on_done)
        return task

    async def wait_for_jobs(self, timeout: float | None = None) -> None:
        if not self._background_tasks:
            return
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        while self._background_tasks:
            pending = list(self._background_tasks)
            if not pending:
                break
            remaining = (deadline - time.monotonic()) if deadline is not None else None
            if remaining is not None and remaining <= 0:
                raise asyncio.TimeoutError("Timed out waiting for ingest jobs")
            _, still_pending = await asyncio.wait(pending, timeout=remaining)
            if still_pending and deadline is not None and time.monotonic() >= deadline:
                raise asyncio.TimeoutError("Timed out waiting for ingest jobs")

    async def shutdown(self) -> None:
        tasks = list(self._background_tasks)
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()

    def record_media(
        self,
        *,
        media_id: str,
        title: str,
        filename: str,
        folder: str,
        tags: str | list[str] | None,
        media_type: str,
        mime_type: str,
        size_bytes: int,
        drive_name: str,
        file_path: str,
        thumb_filename: str | None,
        source_origin: str,
        source_kind: str,
        source_url: str | None,
        mirror_info: dict[str, Any] | None = None,
        duration_sec: float | None = None,
        width: int | None = None,
        height: int | None = None,
        video_codec: str | None = None,
        audio_codec: str | None = None,
        fps: float | None = None,
        preview_filename: str | None = None,
    ) -> dict[str, Any]:
        return self.db.insert_media(
            media_id=media_id,
            title=title,
            filename=filename,
            folder=folder,
            tags=tags,
            media_type=media_type,
            mime_type=mime_type,
            size_bytes=size_bytes,
            drive_name=drive_name,
            file_path=file_path,
            thumb_filename=thumb_filename,
            source_origin=source_origin,
            source_kind=source_kind,
            source_url=source_url,
            telegram_chat_id=mirror_info.get("chat_id") if mirror_info else None,
            telegram_topic_id=mirror_info.get("topic_id") if mirror_info else None,
            telegram_msg_id=mirror_info.get("message_id") if mirror_info else None,
            telegram_worker_name=mirror_info.get("worker_name") if mirror_info else None,
            part_count=int(mirror_info.get("part_count", 1)) if mirror_info else 1,
            parts=mirror_info.get("parts") if mirror_info else None,
            duration_sec=duration_sec,
            width=width,
            height=height,
            video_codec=video_codec,
            audio_codec=audio_codec,
            fps=fps,
            preview_filename=preview_filename,
        )

    async def finalize_staged_file(
        self,
        local_path: Path,
        *,
        folder: str | None = None,
        tags: str | list[str] | None = None,
        source_origin: str,
        source_kind: str,
        source_url: str | None = None,
        title: str | None = None,
        forward_to_telegram: bool = True,
        tg_source: dict[str, Any] | None = None,
        progress_cb: ProgressHook | None = None,
    ) -> dict[str, Any]:
        media_id = uuid.uuid4().hex[:12]
        safe_folder = sanitize_folder(folder, self.settings.default_folder)
        normalized_tags = normalize_tags(tags)
        media_type, mime_type = classify_media(local_path)
        size_bytes = local_path.stat().st_size
        resolved_title = title or local_path.stem

        thumb_filename = await generate_thumbnail(
            local_path, media_type, self.settings.thumbs_dir, media_id
        )
        meta = await extract_media_metadata(local_path, media_type)
        preview_filename: str | None = None
        if media_type == "video":
            preview_filename = await generate_video_preview(
                local_path,
                self.settings.thumbs_dir,
                media_id,
                duration_sec=meta.get("duration_sec"),
            )

        mode = self.settings.storage_mode
        should_mirror = mode == "telegram" or (mode == "both" and forward_to_telegram)

        mirror_info: dict[str, Any] | None = None
        if should_mirror and self.mirror_hook is not None:
            payload = {
                "media_id": media_id,
                "title": resolved_title,
                "filename": local_path.name,
                "folder": safe_folder,
                "tags": normalized_tags,
                "media_type": media_type,
                "size_bytes": size_bytes,
                "local_path": local_path,
                "source_url": source_url,
                "tg_source": tg_source,
                "progress_cb": progress_cb,
            }
            if mode == "telegram":
                mirror_info = await self.mirror_hook(payload)
            else:
                try:
                    mirror_info = await self.mirror_hook(payload)
                except Exception as exc:
                    logger.warning("Telegram topic mirror skipped for %s: %s", local_path.name, exc)

        if mode == "telegram":
            if not mirror_info or not mirror_info.get("message_id"):
                raise RuntimeError(
                    "Telegram storage mode requires TELEGRAM_APIS_JSON and TELEGRAM_ARCHIVE_CHAT_ID to be configured and reachable."
                )
            final_name = local_path.name
            local_path.unlink(missing_ok=True)
            drive_name = "telegram"
            file_path_str = ""
        else:
            drive_name, final_path, _ = await self.allocator.allocate_and_move(
                local_path, folder=safe_folder
            )
            final_name = final_path.name
            file_path_str = str(final_path)

        return self.record_media(
            media_id=media_id,
            title=resolved_title,
            filename=final_name,
            folder=safe_folder,
            tags=normalized_tags,
            media_type=media_type,
            mime_type=mime_type,
            size_bytes=size_bytes,
            drive_name=drive_name,
            file_path=file_path_str,
            thumb_filename=thumb_filename,
            source_origin=source_origin,
            source_kind=source_kind,
            source_url=source_url,
            mirror_info=mirror_info,
            duration_sec=meta.get("duration_sec"),
            width=meta.get("width"),
            height=meta.get("height"),
            video_codec=meta.get("video_codec"),
            audio_codec=meta.get("audio_codec"),
            fps=meta.get("fps"),
            preview_filename=preview_filename,
        )

    async def process_url_job(
        self,
        job_id: str,
        url: str,
        *,
        source_origin: str,
        folder: str | None = None,
        tags: str | list[str] | None = None,
        title: str | None = None,
        forward_to_telegram: bool = True,
        format_spec: str | None = None,
        on_progress: ProgressHook | None = None,
    ) -> list[dict[str, Any]]:
        safe_folder = sanitize_folder(folder, self.settings.default_folder)
        normalized_tags = normalize_tags(tags)
        uses_tg = self.settings.storage_mode == "telegram" or (
            self.settings.storage_mode == "both" and forward_to_telegram
        )
        dl_weight = 70.0 if uses_tg else 95.0

        async def report_dl(pct: float, msg: str) -> None:
            overall = max(1.0, min(dl_weight, (pct / 100.0) * dl_weight))
            self.db.update_job(job_id, status="downloading", progress=overall, message=msg)
            if on_progress:
                await on_progress(overall, msg)

        async def report_ul(pct: float, msg: str) -> None:
            overall = dl_weight + ((pct / 100.0) * (99.0 - dl_weight))
            self.db.update_job(job_id, status="allocating", progress=overall, message=msg)
            if on_progress:
                await on_progress(overall, msg)

        sem = self.semaphore
        self._active_slots += 1
        try:
            async with sem:
                job_dir = Path(tempfile.mkdtemp(dir=self.settings.staging_dir))
                try:
                    ddl = await is_direct_download(url)
                    kind = "ddl" if ddl else "ytdlp"
                    self.db.update_job(
                        job_id,
                        status="downloading",
                        progress=2.0,
                        message=f"Downloading via {kind} → {safe_folder}...",
                    )

                    files = await (
                        _download_ddl(url, job_dir, on_progress=report_dl)
                        if ddl
                        else _download_ytdlp(
                            url,
                            job_dir,
                            cookies_file=self.settings.ytdlp_cookies_file,
                            proxy=self.settings.ytdlp_proxy,
                            format_spec=format_spec,
                            concurrent_fragments=self.settings.ytdlp_concurrent_fragments,
                            on_progress=report_dl,
                        )
                    )

                    self.db.update_job(
                        job_id,
                        status="allocating",
                        progress=dl_weight,
                        message=f"Saving to {safe_folder}...",
                    )
                    items: list[dict[str, Any]] = []
                    for f in files:
                        item = await self.finalize_staged_file(
                            f,
                            folder=safe_folder,
                            tags=normalized_tags,
                            source_origin=source_origin,
                            source_kind=kind,
                            source_url=url,
                            title=title or f.stem,
                            forward_to_telegram=forward_to_telegram,
                            progress_cb=report_ul,
                        )
                        items.append(item)

                    self.db.update_job(
                        job_id,
                        status="done",
                        progress=100.0,
                        message=f"Saved {len(items)} file(s) to {safe_folder}",
                        media_ids_json=json.dumps([i["id"] for i in items]),
                    )
                    return items
                finally:
                    shutil.rmtree(job_dir, ignore_errors=True)
        except Exception as exc:
            self.db.update_job(job_id, status="failed", message=str(exc))
            raise
        finally:
            self._active_slots = max(0, self._active_slots - 1)

    async def process_staged_upload_job(
        self,
        job_id: str,
        staged_file: Path,
        *,
        source_origin: str,
        folder: str | None = None,
        tags: str | list[str] | None = None,
        title: str | None = None,
        forward_to_telegram: bool = True,
        tg_source: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        safe_folder = sanitize_folder(folder, self.settings.default_folder)

        async def report_ul(pct: float, msg: str) -> None:
            overall = 50.0 + (pct / 100.0) * 49.0
            self.db.update_job(job_id, status="allocating", progress=overall, message=msg)

        sem = self.semaphore
        self._active_slots += 1
        try:
            async with sem:
                self.db.update_job(
                    job_id,
                    status="allocating",
                    progress=50.0,
                    message=f"Saving to {safe_folder}...",
                )
                item = await self.finalize_staged_file(
                    staged_file,
                    folder=safe_folder,
                    tags=tags,
                    source_origin=source_origin,
                    source_kind="upload",
                    source_url=None,
                    title=title or staged_file.stem,
                    forward_to_telegram=forward_to_telegram,
                    tg_source=tg_source,
                    progress_cb=report_ul,
                )
                self.db.update_job(
                    job_id,
                    status="done",
                    progress=100.0,
                    message=f"Saved to {item['drive_name']}/{safe_folder}",
                    media_ids_json=json.dumps([item["id"]]),
                )
                return item
        except Exception as exc:
            self.db.update_job(job_id, status="failed", message=str(exc))
            raise
        finally:
            self._active_slots = max(0, self._active_slots - 1)
            shutil.rmtree(staged_file.parent, ignore_errors=True)
