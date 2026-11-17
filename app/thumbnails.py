from __future__ import annotations

import asyncio
import shutil
from pathlib import Path

from PIL import Image, ImageOps


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
