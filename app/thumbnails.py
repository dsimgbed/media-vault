from __future__ import annotations

import asyncio
import io
import json
import math
import re
import shutil
import zipfile
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps


def _parse_fps(raw: Any) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s in ("0/0", "0", "N/A"):
        return None
    try:
        if "/" in s:
            num_s, den_s = s.split("/", 1)
            num = float(num_s)
            den = float(den_s)
            if den <= 0 or num <= 0:
                return None
            val = num / den
        else:
            val = float(s)
        if val <= 0 or val > 1000:
            return None
        return round(val, 3)
    except (ValueError, ZeroDivisionError):
        return None


def _parse_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        val = float(raw)
        if val > 0:
            return val
    except (TypeError, ValueError):
        pass
    return None


def _parse_int(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        val = int(raw)
        if val > 0:
            return val
    except (TypeError, ValueError):
        pass
    return None


def _extract_image_dims(src: Path) -> dict[str, Any]:
    try:
        with Image.open(src) as img:
            img = ImageOps.exif_transpose(img)
            w, h = img.size
            if w > 0 and h > 0:
                return {"width": int(w), "height": int(h)}
    except Exception:
        pass
    return {}


async def extract_media_metadata(src: Path, media_type: str) -> dict[str, Any]:
    if not src.is_file():
        return {}

    if media_type == "image":
        return await asyncio.to_thread(_extract_image_dims, src)

    if media_type not in ("video", "audio"):
        return {}

    ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        return {}

    try:
        proc = await asyncio.create_subprocess_exec(
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration:stream=codec_type,codec_name,width,height,r_frame_rate,avg_frame_rate,duration",
            "-of",
            "json",
            str(src),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
    except Exception:
        return {}

    if proc.returncode != 0 or not stdout:
        return {}

    try:
        data = json.loads(stdout.decode(errors="ignore"))
    except Exception:
        return {}

    meta: dict[str, Any] = {}
    fmt = data.get("format") or {}
    duration_sec = _parse_float(fmt.get("duration"))

    streams = data.get("streams") or []
    for st in streams:
        stype = st.get("codec_type")
        if stype == "video":
            if "video_codec" not in meta and st.get("codec_name"):
                meta["video_codec"] = str(st["codec_name"])
            if "width" not in meta:
                w = _parse_int(st.get("width"))
                h = _parse_int(st.get("height"))
                if w and h:
                    meta["width"] = w
                    meta["height"] = h
            if "fps" not in meta:
                fps = _parse_fps(st.get("avg_frame_rate")) or _parse_fps(st.get("r_frame_rate"))
                if fps is not None:
                    meta["fps"] = fps
            if duration_sec is None:
                duration_sec = _parse_float(st.get("duration"))
        elif stype == "audio":
            if "audio_codec" not in meta and st.get("codec_name"):
                meta["audio_codec"] = str(st["codec_name"])
            if duration_sec is None:
                duration_sec = _parse_float(st.get("duration"))

    if duration_sec is not None:
        meta["duration_sec"] = round(duration_sec, 3)

    return meta


def _make_image_thumb(src: Path, dst: Path) -> bool:
    try:
        with Image.open(src) as img:
            img = ImageOps.exif_transpose(img)
            if img.mode not in ("RGB", "RGBA"):
                img = img.convert("RGB")
            img.thumbnail((480, 480))
            dst.parent.mkdir(parents=True, exist_ok=True)
            img.save(dst, format="WEBP", quality=80, method=4)
        return True
    except Exception:
        return False


PREVIEW_MIN_FRAMES = 2
PREVIEW_MAX_FRAMES = 20
PREVIEW_DEFAULT_FRAMES = 10
PREVIEW_FRAME_COUNT = PREVIEW_DEFAULT_FRAMES
PREVIEW_FRAME_DURATION_MS = 600
PREVIEW_MAX_WIDTH = 320
PREVIEW_WEBP_QUALITY = 50


def compute_preview_frame_count(duration_sec: float | None) -> int:
    dur = _parse_float(duration_sec)
    if dur is None:
        return PREVIEW_DEFAULT_FRAMES
    scaled = int(round(0.6 * math.sqrt(dur)))
    return max(PREVIEW_MIN_FRAMES, min(PREVIEW_MAX_FRAMES, scaled))


def compute_preview_frame_duration_ms(frame_count: int) -> int:
    count = max(1, int(frame_count))
    return min(750, max(400, int(round(6000 / count))))


def _compute_preview_timestamps(
    duration_sec: float | None,
    count: int | None = None,
) -> list[float]:
    dur = _parse_float(duration_sec)
    if dur is None:
        return []
    target_count = compute_preview_frame_count(dur) if count is None else int(count)
    if target_count <= 0:
        return []
    safe_end = max(0.01, dur - min(0.05, dur * 0.02))
    return [
        round(min(max(0.0, ((i + 0.5) / target_count) * dur), safe_end), 3)
        for i in range(target_count)
    ]


def _save_animated_webp_from_pngs(
    frame_paths: list[Path],
    dst: Path,
    *,
    duration_ms: int = PREVIEW_FRAME_DURATION_MS,
    quality: int = PREVIEW_WEBP_QUALITY,
    max_dim: int = PREVIEW_MAX_WIDTH,
) -> bool:
    frames: list[Image.Image] = []
    try:
        for p in frame_paths:
            if not p.is_file() or p.stat().st_size == 0:
                continue
            with Image.open(p) as im:
                im = ImageOps.exif_transpose(im)
                if im.mode != "RGB":
                    im = im.convert("RGB")
                im.thumbnail((max_dim, max_dim))
                frames.append(im.copy())
        if not frames:
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp_dst = dst.with_name(f".{dst.name}.tmp")
        try:
            frames[0].save(
                tmp_dst,
                format="WEBP",
                save_all=True,
                append_images=frames[1:],
                loop=0,
                duration=duration_ms,
                quality=quality,
                method=4,
                minimize_size=True,
            )
            if tmp_dst.is_file() and tmp_dst.stat().st_size > 0:
                tmp_dst.replace(dst)
                return dst.is_file() and dst.stat().st_size > 0
        finally:
            tmp_dst.unlink(missing_ok=True)
        return False
    except Exception:
        return False
    finally:
        for fr in frames:
            try:
                fr.close()
            except Exception:
                pass


async def _make_video_thumb(src: Path, dst: Path) -> bool:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return False

    dst.parent.mkdir(parents=True, exist_ok=True)
    for seek in ("00:00:01", "00:00:00"):
        proc = await asyncio.create_subprocess_exec(
            ffmpeg,
            "-y",
            "-ss",
            seek,
            "-i",
            str(src),
            "-frames:v",
            "1",
            "-vf",
            "scale='min(480,iw)':-2",
            "-c:v",
            "libwebp",
            "-quality",
            "80",
            str(dst),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.communicate()
        if proc.returncode == 0 and dst.exists() and dst.stat().st_size > 0:
            return True
        dst.unlink(missing_ok=True)

        tmp_png = dst.with_suffix(".tmp.png")
        try:
            proc_png = await asyncio.create_subprocess_exec(
                ffmpeg,
                "-y",
                "-ss",
                seek,
                "-i",
                str(src),
                "-frames:v",
                "1",
                "-vf",
                "scale='min(480,iw)':-2",
                str(tmp_png),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc_png.communicate()
            if proc_png.returncode == 0 and tmp_png.exists() and tmp_png.stat().st_size > 0:
                ok = await asyncio.to_thread(_make_image_thumb, tmp_png, dst)
                if ok and dst.exists() and dst.stat().st_size > 0:
                    return True
        finally:
            tmp_png.unlink(missing_ok=True)
    return False


async def _extract_preview_frame_png(
    ffmpeg: str,
    src: Path,
    timestamp_sec: float,
    dst_png: Path,
) -> Path | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            ffmpeg,
            "-y",
            "-ss",
            f"{max(0.0, float(timestamp_sec)):.3f}",
            "-i",
            str(src),
            "-frames:v",
            "1",
            "-vf",
            f"scale='min({PREVIEW_MAX_WIDTH},iw)':-2:flags=fast_bilinear",
            "-an",
            "-sn",
            "-dn",
            str(dst_png),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.communicate()
        if proc.returncode == 0 and dst_png.is_file() and dst_png.stat().st_size > 0:
            return dst_png
    except Exception:
        pass
    dst_png.unlink(missing_ok=True)
    return None


async def generate_video_preview(
    src: Path,
    thumbs_dir: Path,
    media_id: str,
    duration_sec: float | None = None,
    frame_count: int | None = None,
) -> str | None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg or not src.is_file():
        return None

    preview_name = f"{media_id}_preview.webp"
    dst = thumbs_dir / preview_name
    thumbs_dir.mkdir(parents=True, exist_ok=True)

    effective_duration = _parse_float(duration_sec)
    if effective_duration is None:
        meta = await extract_media_metadata(src, "video")
        effective_duration = _parse_float(meta.get("duration_sec"))

    target_count = (
        max(1, int(frame_count))
        if frame_count is not None
        else compute_preview_frame_count(effective_duration)
    )
    timestamps = _compute_preview_timestamps(effective_duration, count=target_count)
    frame_paths: list[Path] = []

    try:
        if timestamps:
            sem = asyncio.Semaphore(4)

            async def _grab(idx: int, ts: float) -> Path | None:
                async with sem:
                    out_png = thumbs_dir / f"{media_id}_pframe_{idx:02d}.png"
                    return await _extract_preview_frame_png(ffmpeg, src, ts, out_png)

            results = await asyncio.gather(
                *(_grab(idx, ts) for idx, ts in enumerate(timestamps))
            )
            frame_paths = [p for p in results if p is not None]

        if not frame_paths:
            # Fallback when container duration is unavailable or keyframe seeking fails
            for vf_expr in (
                f"fps=1,scale='min({PREVIEW_MAX_WIDTH},iw)':-2:flags=fast_bilinear",
                f"fps=5,scale='min({PREVIEW_MAX_WIDTH},iw)':-2:flags=fast_bilinear",
            ):
                for leftover in thumbs_dir.glob(f"{media_id}_pframe_*.png"):
                    leftover.unlink(missing_ok=True)
                frame_pattern = thumbs_dir / f"{media_id}_pframe_%02d.png"
                try:
                    proc_png = await asyncio.create_subprocess_exec(
                        ffmpeg,
                        "-y",
                        "-i",
                        str(src),
                        "-vf",
                        vf_expr,
                        "-frames:v",
                        str(target_count),
                        "-an",
                        "-sn",
                        "-dn",
                        str(frame_pattern),
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await proc_png.communicate()
                    candidates = sorted(thumbs_dir.glob(f"{media_id}_pframe_*.png"))
                    if proc_png.returncode == 0 and candidates:
                        frame_paths = candidates
                        break
                except Exception:
                    pass

        if frame_paths:
            frame_duration_ms = compute_preview_frame_duration_ms(len(frame_paths))
            ok = await asyncio.to_thread(
                _save_animated_webp_from_pngs,
                frame_paths,
                dst,
                duration_ms=frame_duration_ms,
            )
            if ok and dst.is_file() and dst.stat().st_size > 0:
                return preview_name
    except Exception:
        pass
    finally:
        for p in thumbs_dir.glob(f"{media_id}_pframe_*.png"):
            p.unlink(missing_ok=True)

    dst.unlink(missing_ok=True)
    return None


def _natural_key(text: str):
    return [
        (0, int(t)) if (t.isdigit() and len(t) <= 18) else (1, t.lower())
        for t in re.split(r"(\d+)", text)
    ]


def _is_safe_cbz_entry_name(name: str) -> bool:
    if not name or name.startswith("__MACOSX/") or name.startswith(("/", "\\")) or "\\" in name:
        return False
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in name):
        return False
    segments = name.split("/")
    for seg in segments:
        if not seg or seg in (".", ".."):
            return False
        if len(seg) >= 2 and seg[1] == ":":
            return False
    return True


def _make_cbz_thumb(src: Path, dst: Path) -> bool:
    allowed = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
    max_pixels = 64_000_000
    try:
        with zipfile.ZipFile(src, "r") as zf:
            candidates = []
            for info in zf.infolist():
                if info.is_dir():
                    continue
                name = info.filename
                if not _is_safe_cbz_entry_name(name):
                    continue
                if Path(name).suffix.lower() not in allowed:
                    continue
                if info.file_size > 32 * 1024 * 1024:
                    continue
                candidates.append(info)
            if not candidates:
                return False
            candidates.sort(key=lambda i: _natural_key(i.filename))
            for info in candidates:
                try:
                    with zf.open(info) as member:
                        raw = member.read(32 * 1024 * 1024 + 1)
                except Exception:
                    continue
                if len(raw) > 32 * 1024 * 1024:
                    continue
                try:
                    with Image.open(io.BytesIO(raw)) as img:
                        w, h = img.size
                        if w <= 0 or h <= 0 or (w * h) > max_pixels:
                            continue
                        img = ImageOps.exif_transpose(img)
                        if img.mode not in ("RGB", "RGBA"):
                            img = img.convert("RGB")
                        img.thumbnail((480, 480))
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        img.save(dst, format="WEBP", quality=80, method=4)
                    if dst.is_file() and dst.stat().st_size > 0:
                        return True
                except Exception:
                    continue
        return False
    except Exception:
        return False


async def generate_thumbnail(*args: Any, **kwargs: Any) -> Any:
    # Support both legacy signature generate_thumbnail(src, media_type, thumbs_dir, media_id)
    # and new signature generate_thumbnail(src, dst, media_type).
    src = kwargs.get("src", args[0] if len(args) > 0 else None)
    src = Path(src) if not isinstance(src, Path) else src

    new_style = False
    if "dst" in kwargs:
        new_style = True
    elif len(args) >= 2 and isinstance(args[1], (str, Path)) and not str(args[1]).lower() in ("image", "video", "audio", "file"):
        # second arg looks like a path, not a media_type
        if Path(str(args[1])).suffix or True:
            # heuristic: if second arg is Path or contains separator/dot path
            if isinstance(args[1], Path) or ("/" in str(args[1]) or "\\" in str(args[1])):
                new_style = True
    # also if third positional provided as media_type string in new style
    if len(args) >= 3 and isinstance(args[2], str) and args[2].lower() in ("image", "video", "audio", "file"):
        # could be new style (src, dst, media_type)
        if len(args) == 3:
            new_style = True

    if new_style:
        dst = kwargs.get("dst", args[1] if len(args) > 1 else None)
        dst = Path(dst) if not isinstance(dst, Path) else dst
        media_type = kwargs.get("media_type", args[2] if len(args) > 2 else "file")
        media_type = str(media_type)
        if src.suffix.lower() == ".cbz":
            ok = await asyncio.to_thread(_make_cbz_thumb, src, dst)
            return bool(ok)
        # fallback for image/video when dst provided
        if media_type == "image":
            ok = await asyncio.to_thread(_make_image_thumb, src, dst)
            return bool(ok)
        if media_type == "video":
            ok = await _make_video_thumb(src, dst)
            return bool(ok)
        return False

    # legacy
    media_type = kwargs.get("media_type", args[1] if len(args) > 1 else "file")
    thumbs_dir = kwargs.get("thumbs_dir", args[2] if len(args) > 2 else None)
    media_id = kwargs.get("media_id", args[3] if len(args) > 3 else None)
    media_type = str(media_type)
    thumb_name = f"{media_id}.webp"
    dst = Path(thumbs_dir) / thumb_name if thumbs_dir is not None else None

    if media_type == "image":
        ok = await asyncio.to_thread(_make_image_thumb, src, dst)
        return thumb_name if ok else None
    if media_type == "video":
        ok = await _make_video_thumb(src, dst)
        return thumb_name if ok else None
    if src.suffix.lower() == ".cbz":
        ok = await asyncio.to_thread(_make_cbz_thumb, src, dst)
        return thumb_name if ok else None
    return None
