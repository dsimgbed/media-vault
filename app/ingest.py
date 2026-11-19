from __future__ import annotations

import asyncio
from collections import deque
import hashlib
import inspect
import json
import logging
import mimetypes
import re
import shutil
import sys
import tempfile
import threading
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
ProcHook = Callable[[asyncio.subprocess.Process], None]


class HashWriter:
    def __init__(self) -> None:
        self._hasher = hashlib.sha256()
        self.size = 0

    def update(self, data: bytes) -> None:
        self._hasher.update(data)
        self.size += len(data)

    def hexdigest(self) -> str:
        return self._hasher.hexdigest()


def compute_file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


async def async_compute_file_sha256(path: Path) -> str:
    return await asyncio.to_thread(compute_file_sha256, path)


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
    on_proc: ProcHook | None = None,
) -> tuple[int, list[str]]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    if on_proc:
        on_proc(proc)
    tail: deque[str] = deque(maxlen=80)
    assert proc.stdout is not None
    pending = b""
    try:
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
    except BaseException:
        if proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    await proc.wait()
                except Exception:
                    pass
        raise


def _accepts_kwarg(fn: Callable[..., Any], name: str) -> bool:
    try:
        sig = inspect.signature(fn)
        return name in sig.parameters or any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )
    except (TypeError, ValueError):
        return False


async def _invoke_streaming(
    *args: str,
    on_line: Callable[[str], Awaitable[None]] | None = None,
    on_proc: ProcHook | None = None,
) -> tuple[int, list[str]]:
    kwargs: dict[str, Any] = {"on_line": on_line}
    if on_proc is not None and _accepts_kwarg(_run_subprocess_streaming, "on_proc"):
        kwargs["on_proc"] = on_proc
    return await _run_subprocess_streaming(*args, **kwargs)


async def _download_ddl(
    url: str,
    job_dir: Path,
    *,
    on_progress: ProgressHook | None = None,
    on_proc: ProcHook | None = None,
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

        code, tail = await _invoke_streaming(
            aria2c,
            "--dir",
            str(job_dir),
            "--max-connection-per-server=4",
            "--content-disposition-default-utf8=true",
            "--auto-file-renaming=true",
            "--summary-interval=1",
            url,
            on_line=handle_aria_line,
            on_proc=on_proc,
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


def _ytdlp_base_args(
    cookies_file: Path | None = None,
    proxy: str = "",
    *,
    no_playlist: bool = True,
) -> list[str]:
    args: list[str] = [*_ytdlp_cmd(), "--no-playlist" if no_playlist else "--yes-playlist"]
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


async def expand_ytdlp_playlist(
    url: str,
    *,
    cookies_file: Path | None = None,
    proxy: str = "",
    max_items: int = 100,
) -> dict[str, Any]:
    limit = max(1, int(max_items))
    args = [
        *_ytdlp_base_args(cookies_file=cookies_file, proxy=proxy, no_playlist=False),
        "--flat-playlist",
        "--playlist-end",
        str(limit),
        "-J",
        url,
    ]
    code, out, err = await _run_subprocess(*args)
    if code != 0:
        raise RuntimeError(_format_ytdlp_error(err or out))
    try:
        info = json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse yt-dlp playlist metadata: {exc}")

    raw_entries = info.get("entries")
    if info.get("_type") in ("playlist", "multi_video") or isinstance(raw_entries, list):
        entries: list[dict[str, Any]] = []
        parent_web = str(info.get("webpage_url") or url).lower()
        for entry in (raw_entries or []):
            if not isinstance(entry, dict):
                continue
            cand_url = str(entry.get("url") or "").strip()
            cand_web = str(entry.get("webpage_url") or "").strip()
            if cand_url.startswith(("http://", "https://")):
                entry_url = cand_url
            elif cand_web.startswith(("http://", "https://")):
                entry_url = cand_web
            else:
                entry_id = str(entry.get("id") or cand_url or "").strip()
                ie_key = str(
                    entry.get("ie_key") or entry.get("extractor_key") or info.get("extractor_key") or ""
                ).lower()
                if entry_id and (
                    "youtube" in ie_key or "youtube.com" in parent_web or "youtu.be" in parent_web
                ):
                    entry_url = f"https://www.youtube.com/watch?v={entry_id}"
                else:
                    entry_url = ""
            if not entry_url.startswith(("http://", "https://")):
                continue
            entries.append(
                {
                    "url": entry_url,
                    "title": entry.get("title"),
                    "id": entry.get("id"),
                    "duration": entry.get("duration"),
                }
            )
            if len(entries) >= limit:
                break
        return {
            "is_playlist": len(entries) > 1 or info.get("_type") in ("playlist", "multi_video"),
            "playlist_title": info.get("title") or info.get("playlist_title") or "",
            "entries": entries,
            "webpage_url": info.get("webpage_url") or url,
        }

    return {
        "is_playlist": False,
        "playlist_title": None,
        "entries": [
            {
                "url": info.get("webpage_url") or url,
                "title": info.get("title"),
                "id": info.get("id"),
                "duration": info.get("duration"),
            }
        ],
        "webpage_url": info.get("webpage_url") or url,
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
    on_proc: ProcHook | None = None,
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

    code, tail = await _invoke_streaming(*args, on_line=handle_line, on_proc=on_proc)
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
        self._job_tasks: dict[str, asyncio.Task[Any]] = {}
        self._active_procs: dict[str, set[asyncio.subprocess.Process]] = {}
        self._cancelled_jobs: set[str] = set()
        self._active_slots = 0
        self._staging_lock = threading.Lock()
        self._active_staging_dirs: set[Path] = set()

    def register_staging_dir(self, path: Path | str) -> None:
        resolved = Path(path).resolve()
        with self._staging_lock:
            self._active_staging_dirs.add(resolved)

    def unregister_staging_dir(self, path: Path | str) -> None:
        resolved = Path(path).resolve()
        with self._staging_lock:
            self._active_staging_dirs.discard(resolved)

    def get_active_staging_dirs(self) -> set[Path]:
        with self._staging_lock:
            return set(self._active_staging_dirs)

    def has_active_jobs(self) -> bool:
        with self._staging_lock:
            return bool(self._job_tasks or self._active_staging_dirs)

    @property
    def semaphore(self) -> asyncio.Semaphore:
        target = max(1, int(self.settings.ingest_concurrency))
        if target != self._configured_concurrency and not self._background_tasks and self._active_slots == 0:
            self._configured_concurrency = target
            self._semaphore = asyncio.Semaphore(target)
        return self._semaphore

    def register_proc(self, job_id: str, proc: asyncio.subprocess.Process) -> None:
        self._active_procs.setdefault(job_id, set()).add(proc)

    def unregister_proc(self, job_id: str, proc: asyncio.subprocess.Process) -> None:
        procs = self._active_procs.get(job_id)
        if procs is not None:
            procs.discard(proc)
            if not procs:
                self._active_procs.pop(job_id, None)

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
                if job and job.get("status") not in ("done", "failed", "cancelled"):
                    self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
                raise
            except Exception as exc:
                if job_id in self._cancelled_jobs:
                    self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
                    return None
                logger.warning("Background ingest job %s failed: %s", job_id, exc)
                job = self.db.get_job(job_id)
                if job and job.get("status") not in ("done", "failed", "cancelled"):
                    self.db.update_job(job_id, status="failed", message=str(exc))
                return None

        task = asyncio.create_task(_runner())
        self._background_tasks.add(task)
        self._job_tasks[job_id] = task

        def _on_done(t: asyncio.Task[Any]) -> None:
            self._background_tasks.discard(t)
            if self._job_tasks.get(job_id) is t:
                self._job_tasks.pop(job_id, None)
                self._active_procs.pop(job_id, None)
            if hasattr(coro, "close"):
                try:
                    coro.close()  # type: ignore[attr-defined]
                except RuntimeError:
                    pass

        task.add_done_callback(_on_done)
        return task

    async def cancel_job(self, job_id: str) -> dict[str, Any]:
        job = self.db.get_job(job_id)
        if not job:
            raise KeyError("Job not found")
        if job["status"] not in ("queued", "downloading", "allocating"):
            raise ValueError(f"Job is already {job['status']}")

        self._cancelled_jobs.add(job_id)
        for proc in list(self._active_procs.get(job_id, set())):
            if proc.returncode is None:
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass

        task = self._job_tasks.get(job_id)
        if task and not task.done():
            task.cancel()

        self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
        updated = self.db.get_job(job_id)
        assert updated is not None
        return updated

    async def retry_job(self, job_id: str, *, force: bool = False) -> dict[str, Any]:
        job = self.db.get_job(job_id)
        if not job:
            raise KeyError("Job not found")
        if job["status"] not in ("failed", "cancelled"):
            raise ValueError("Only failed or cancelled jobs can be retried")

        try:
            params = json.loads(job.get("params_json") or "{}")
            if not isinstance(params, dict):
                params = {}
        except json.JSONDecodeError:
            params = {}

        is_url_job = (
            job["source_kind"] == "url"
            or bool(params.get("url"))
            or str(job.get("source_ref") or "").startswith(("http://", "https://"))
        )
        if is_url_job:
            url = str(params.get("url") or job["source_ref"])
            self._cancelled_jobs.discard(job_id)
            self.db.update_job(
                job_id,
                status="queued",
                progress=0.0,
                message="Queued (retry)",
                media_ids_json="[]",
            )
            effective_force = bool(force or params.get("force", False))
            self.submit_job(
                job_id,
                self.process_url_job(
                    job_id,
                    url,
                    source_origin=str(params.get("source_origin") or job["source_origin"]),
                    folder=params.get("folder"),
                    tags=params.get("tags"),
                    title=params.get("title"),
                    forward_to_telegram=bool(params.get("forward_to_telegram", True)),
                    format_spec=params.get("format_spec"),
                    force=effective_force,
                ),
            )
            updated = self.db.get_job(job_id)
            assert updated is not None
            return updated

        staged_raw = params.get("staged_path")
        staged_path = Path(staged_raw) if staged_raw else None
        if staged_path and staged_path.is_file():
            self._cancelled_jobs.discard(job_id)
            self.db.update_job(
                job_id,
                status="queued",
                progress=50.0,
                message="Queued (retry)",
                media_ids_json="[]",
            )
            effective_force = bool(force or params.get("force", False))
            self.submit_job(
                job_id,
                self.process_staged_upload_job(
                    job_id,
                    staged_path,
                    source_origin=str(params.get("source_origin") or job["source_origin"]),
                    folder=params.get("folder"),
                    tags=params.get("tags"),
                    title=params.get("title"),
                    forward_to_telegram=bool(params.get("forward_to_telegram", True)),
                    force=effective_force,
                ),
            )
            updated = self.db.get_job(job_id)
            assert updated is not None
            return updated

        raise ValueError("Upload jobs cannot be retried after staging cleanup; please re-upload the file.")

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
        for procs in list(self._active_procs.values()):
            for proc in list(procs):
                if proc.returncode is None:
                    try:
                        proc.terminate()
                    except ProcessLookupError:
                        pass
        tasks = list(self._background_tasks)
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()
        self._job_tasks.clear()
        self._active_procs.clear()

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
        sha256: str | None = None,
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
            sha256=sha256,
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
        force: bool = False,
        precomputed_sha256: str | None = None,
    ) -> dict[str, Any]:
        sha256 = (precomputed_sha256 or await async_compute_file_sha256(local_path)).lower()
        if not force:
            existing = self.db.find_media_by_sha256(sha256)
            if not existing and source_url:
                existing = self.db.find_media_by_source_url(source_url)
            if existing:
                local_path.unlink(missing_ok=True)
                return dict(existing, deduplicated=True)

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
            sha256=sha256,
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
        force: bool = False,
    ) -> list[dict[str, Any]]:
        if job_id in self._cancelled_jobs:
            self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
            raise asyncio.CancelledError()

        if not force:
            existing = self.db.find_media_by_source_url(url)
            if existing:
                self.db.update_job(
                    job_id,
                    status="done",
                    progress=100.0,
                    message=f"Duplicate URL skipped (already in {existing['folder']}: {existing['filename']})",
                    media_ids_json=json.dumps([existing["id"]]),
                )
                return [dict(existing, deduplicated=True)]

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

        def _on_proc(proc: asyncio.subprocess.Process) -> None:
            self.register_proc(job_id, proc)

        sem = self.semaphore
        self._active_slots += 1
        try:
            async with sem:
                if job_id in self._cancelled_jobs:
                    raise asyncio.CancelledError()
                job_dir = Path(tempfile.mkdtemp(dir=self.settings.staging_dir))
                self.register_staging_dir(job_dir)
                try:
                    ddl = await is_direct_download(url)
                    kind = "ddl" if ddl else "ytdlp"
                    self.db.update_job(
                        job_id,
                        status="downloading",
                        progress=2.0,
                        message=f"Downloading via {kind} → {safe_folder}...",
                    )

                    if ddl:
                        ddl_kwargs: dict[str, Any] = {"on_progress": report_dl}
                        if _accepts_kwarg(_download_ddl, "on_proc"):
                            ddl_kwargs["on_proc"] = _on_proc
                        files = await _download_ddl(url, job_dir, **ddl_kwargs)
                    else:
                        yt_kwargs: dict[str, Any] = {
                            "cookies_file": self.settings.ytdlp_cookies_file,
                            "proxy": self.settings.ytdlp_proxy,
                            "format_spec": format_spec,
                            "concurrent_fragments": self.settings.ytdlp_concurrent_fragments,
                            "on_progress": report_dl,
                        }
                        if _accepts_kwarg(_download_ytdlp, "on_proc"):
                            yt_kwargs["on_proc"] = _on_proc
                        files = await _download_ytdlp(url, job_dir, **yt_kwargs)

                    if job_id in self._cancelled_jobs:
                        raise asyncio.CancelledError()

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
                            force=force,
                        )
                        items.append(item)

                    if items and all(i.get("deduplicated") for i in items):
                        first = items[0]
                        done_msg = f"Duplicate skipped (already in {first['folder']}: {first['filename']})"
                    else:
                        done_msg = f"Saved {len(items)} file(s) to {safe_folder}"

                    self.db.update_job(
                        job_id,
                        status="done",
                        progress=100.0,
                        message=done_msg,
                        media_ids_json=json.dumps([i["id"] for i in items]),
                    )
                    return items
                finally:
                    shutil.rmtree(job_dir, ignore_errors=True)
                    self.unregister_staging_dir(job_dir)
        except asyncio.CancelledError:
            job = self.db.get_job(job_id)
            if job and job.get("status") != "cancelled":
                self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
            raise
        except Exception as exc:
            if job_id in self._cancelled_jobs:
                self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
                raise asyncio.CancelledError()
            self.db.update_job(job_id, status="failed", message=str(exc))
            raise
        finally:
            self._active_slots = max(0, self._active_slots - 1)
            self._active_procs.pop(job_id, None)

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
        force: bool = False,
        precomputed_sha256: str | None = None,
    ) -> dict[str, Any]:
        job_dir = staged_file.parent
        self.register_staging_dir(job_dir)
        if job_id in self._cancelled_jobs:
            self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
            shutil.rmtree(job_dir, ignore_errors=True)
            self.unregister_staging_dir(job_dir)
            raise asyncio.CancelledError()

        safe_folder = sanitize_folder(folder, self.settings.default_folder)

        async def report_ul(pct: float, msg: str) -> None:
            overall = 50.0 + (pct / 100.0) * 49.0
            self.db.update_job(job_id, status="allocating", progress=overall, message=msg)

        sem = self.semaphore
        self._active_slots += 1
        try:
            async with sem:
                if job_id in self._cancelled_jobs:
                    raise asyncio.CancelledError()
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
                    force=force,
                    precomputed_sha256=precomputed_sha256,
                )
                if item.get("deduplicated"):
                    done_msg = f"Duplicate skipped (already in {item['folder']}: {item['filename']})"
                else:
                    done_msg = f"Saved to {item['drive_name']}/{safe_folder}"
                self.db.update_job(
                    job_id,
                    status="done",
                    progress=100.0,
                    message=done_msg,
                    media_ids_json=json.dumps([item["id"]]),
                )
                return item
        except asyncio.CancelledError:
            job = self.db.get_job(job_id)
            if job and job.get("status") != "cancelled":
                self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
            raise
        except Exception as exc:
            if job_id in self._cancelled_jobs:
                self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
                raise asyncio.CancelledError()
            self.db.update_job(job_id, status="failed", message=str(exc))
            raise
        finally:
            self._active_slots = max(0, self._active_slots - 1)
            shutil.rmtree(job_dir, ignore_errors=True)
            self.unregister_staging_dir(job_dir)
