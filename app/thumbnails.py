from __future__ import annotations

import asyncio
import json
import shutil
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


def _save_animated_webp_from_pngs(frame_paths: list[Path], dst: Path) -> bool:
    frames: list[Image.Image] = []
    try:
        for p in frame_paths:
            if not p.is_file() or p.stat().st_size == 0:
                continue
            with Image.open(p) as im:
                im = ImageOps.exif_transpose(im)
                if im.mode not in ("RGB", "RGBA"):
                    im = im.convert("RGB")
                im.thumbnail((320, 320))
                frames.append(im.copy())
        if not frames:
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        frames[0].save(
            dst,
            format="WEBP",
            save_all=True,
            append_images=frames[1:],
            loop=0,
            duration=180,
            quality=60,
        )
        return dst.is_file() and dst.stat().st_size > 0
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


async def generate_video_preview(
    src: Path,
    thumbs_dir: Path,
    media_id: str,
    duration_sec: float | None = None,
) -> str | None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg or not src.is_file():
        return None

    preview_name = f"{media_id}_preview.webp"
    dst = thumbs_dir / preview_name
    thumbs_dir.mkdir(parents=True, exist_ok=True)
    seek = "00:00:01" if (duration_sec is not None and duration_sec > 4.0) else "00:00:00"

    for codec in ("libwebp_anim", "libwebp"):
        try:
            proc = await asyncio.create_subprocess_exec(
                ffmpeg,
                "-y",
                "-ss",
                seek,
                "-t",
                "3",
                "-i",
                str(src),
                "-vf",
                "fps=6,scale='min(320,iw)':-2:flags=fast_bilinear",
                "-c:v",
                codec,
                "-loop",
                "0",
                "-an",
                "-quality",
                "60",
                str(dst),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.communicate()
            if proc.returncode == 0 and dst.is_file() and dst.stat().st_size > 0:
                return preview_name
        except Exception:
            pass
        dst.unlink(missing_ok=True)

    # Fallback: extract PNG frames and assemble animated WebP with Pillow
    frame_pattern = thumbs_dir / f"{media_id}_pframe_%02d.png"
    try:
        proc_png = await asyncio.create_subprocess_exec(
            ffmpeg,
            "-y",
            "-ss",
            seek,
            "-t",
            "3",
            "-i",
            str(src),
            "-vf",
            "fps=4,scale='min(320,iw)':-2:flags=fast_bilinear",
            "-frames:v",
            "12",
            str(frame_pattern),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc_png.communicate()
        frame_paths = sorted(thumbs_dir.glob(f"{media_id}_pframe_*.png"))
        if proc_png.returncode == 0 and frame_paths:
            ok = await asyncio.to_thread(_save_animated_webp_from_pngs, frame_paths, dst)
            if ok and dst.is_file() and dst.stat().st_size > 0:
                return preview_name
    except Exception:
        pass
    finally:
        for p in thumbs_dir.glob(f"{media_id}_pframe_*.png"):
            p.unlink(missing_ok=True)

    dst.unlink(missing_ok=True)
    return None


async def generate_thumbnail(src: Path, media_type: str, thumbs_dir: Path, media_id: str) -> str | None:
    thumb_name = f"{media_id}.webp"
    dst = thumbs_dir / thumb_name

    if media_type == "image":
        ok = await asyncio.to_thread(_make_image_thumb, src, dst)
        return thumb_name if ok else None

    if media_type == "video":
        ok = await _make_video_thumb(src, dst)
        return thumb_name if ok else None

    return None
