from __future__ import annotations

import io
import json
import logging
import os
import re
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import ValidationError

from app.allocator import sanitize_folder
from app.context import STATIC_DIR, AppContext
from app.ingest import resolve_format_spec
from app.routers.deps import get_ctx, require_auth
from app.schemas import QuickCaptureRequest

logger = logging.getLogger(__name__)

router = APIRouter()

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EXTENSION_DIR = PROJECT_ROOT / "clients" / "browser-extension"

_URL_RE = re.compile(r"https?://[^\s<>\"'`]+")
_TRUE_VALUES = {"true", "1", "yes", "on"}
_FALSE_VALUES = {"false", "0", "no", "off"}
_ALLOWED_SOURCES = {"quick", "shortcut", "ios", "android", "bookmarklet", "extension"}

_CAPTURE_SECURITY_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "img-src 'self' data: blob:; connect-src 'self'; base-uri 'none'; "
        "form-action 'self'; frame-ancestors 'none'"
    ),
}


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    token = str(value).strip().lower()
    if not token:
        return default
    if token in _TRUE_VALUES:
        return True
    if token in _FALSE_VALUES:
        return False
    return default


def _form_str(form: Any, key: str) -> str | None:
    value = form.get(key)
    if value is None or not isinstance(value, str):
        return None
    return value


async def _parse_capture_payload(request: Request) -> QuickCaptureRequest:
    content_type = (request.headers.get("content-type") or "").lower()
    is_form = content_type.startswith(
        ("application/x-www-form-urlencoded", "multipart/form-data")
    )

    if is_form:
        try:
            form = await request.form()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid form payload")
        data: dict[str, Any] = {
            "url": _form_str(form, "url") or "",
            "text": _form_str(form, "text"),
            "folder": _form_str(form, "folder"),
            "tags": _form_str(form, "tags"),
            "title": _form_str(form, "title"),
            "format_spec": _form_str(form, "format_spec"),
            "forward_to_telegram": _coerce_bool(form.get("forward_to_telegram"), True),
            "force": _coerce_bool(form.get("force"), False),
            "source": _form_str(form, "source") or "quick",
        }
    else:
        try:
            raw = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON payload")
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail="Invalid JSON payload")
        data = dict(raw)

    try:
        return QuickCaptureRequest(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors())


def _extract_url(url: str, text: str | None) -> str:
    target = (url or "").strip()
    if not target and text:
        match = _URL_RE.search(text)
        if match:
            target = match.group(0).rstrip(".,;!?")
    return target


def _display_label(title: str | None, url: str) -> str:
    label = title or urlsplit(url).netloc or url
    if len(label) > 80:
        label = label[:77] + "..."
    return label


@router.get("/capture", response_class=HTMLResponse)
async def capture_page():
    capture_file = STATIC_DIR / "capture.html"
    if not capture_file.is_file():
        return HTMLResponse(
            "<h1>Capture page missing</h1>",
            status_code=404,
            headers=_CAPTURE_SECURITY_HEADERS,
        )
    return HTMLResponse(
        capture_file.read_text(encoding="utf-8"),
        headers=_CAPTURE_SECURITY_HEADERS,
    )


@router.post("/api/capture/quick", dependencies=[Depends(require_auth)])
async def capture_quick(
    request: Request,
    ctx: AppContext = Depends(get_ctx),
):
    cfg = ctx.settings
    payload = await _parse_capture_payload(request)

    target_url = _extract_url(payload.url, payload.text)
    if not target_url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
    try:
        parsed = urlsplit(target_url)
    except ValueError:
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
    if not parsed.netloc:
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")

    resolved_format = (payload.format_spec or "best").strip() or "best"
    try:
        resolve_format_spec(resolved_format)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    resolved_folder = sanitize_folder(payload.folder, cfg.default_folder)
    clean_title = (payload.title or "").strip() or None

    safe_source = (payload.source or "").strip().lower()
    if safe_source not in _ALLOWED_SOURCES:
        safe_source = "quick"

    base_url = (cfg.public_base_url or "http://localhost:8080").rstrip("/")

    existing = ctx.db.find_media_by_source_url(target_url)
    if existing is not None and not payload.force:
        existing_folder = existing.get("folder") or resolved_folder
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "status": "duplicate",
                "duplicate": True,
                "job_id": None,
                "url": target_url,
                "folder": existing_folder,
                "format_spec": resolved_format,
                "summary": (
                    f"Already in Vault ({existing_folder}): "
                    f"{existing.get('title') or target_url}"
                ),
                "gallery_url": f"{base_url}/#media={existing['id']}",
                "media_id": existing["id"],
            },
        )

    job_params = {
        "url": target_url,
        "folder": resolved_folder,
        "tags": payload.tags,
        "title": clean_title,
        "forward_to_telegram": payload.forward_to_telegram,
        "format_spec": resolved_format,
        "force": payload.force,
        "source_origin": "web",
        "capture_source": safe_source,
    }
    job_id = ctx.db.create_job("web", "url", target_url, params=job_params)
    ctx.ingest.submit_job(
        job_id,
        ctx.ingest.process_url_job(
            job_id,
            target_url,
            source_origin="web",
            folder=resolved_folder,
            tags=payload.tags,
            title=clean_title,
            forward_to_telegram=payload.forward_to_telegram,
            format_spec=resolved_format,
            force=payload.force,
        ),
    )

    return JSONResponse(
        status_code=202,
        content={
            "ok": True,
            "status": "queued",
            "duplicate": False,
            "job_id": job_id,
            "url": target_url,
            "folder": resolved_folder,
            "format_spec": resolved_format,
            "summary": f"Queued for {resolved_folder}: {_display_label(clean_title, target_url)}",
            "gallery_url": f"{base_url}/?mode=uploader",
            "media_id": None,
        },
    )


@router.get("/api/capture/config", dependencies=[Depends(require_auth)])
async def capture_config(ctx: AppContext = Depends(get_ctx)):
    cfg = ctx.settings
    manifest_path = EXTENSION_DIR / "manifest.json"
    ext_version: str | None = None
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            version = manifest.get("version")
            if isinstance(version, str) and version.strip():
                ext_version = version.strip()
        except (OSError, ValueError):
            logger.warning("Unable to read browser extension manifest at %s", manifest_path)

    return {
        "default_folder": cfg.default_folder,
        "public_base_url": cfg.public_base_url.rstrip("/"),
        "auth_required": bool(cfg.web_auth_token.strip()),
        "extension_available": manifest_path.is_file(),
        "extension_version": ext_version,
        "cors_enabled": bool(cfg.get_capture_cors_origins() or cfg.capture_cors_allow_extensions),
        "cors_allow_extensions": bool(cfg.capture_cors_allow_extensions),
        "cors_origins_count": len(cfg.get_capture_cors_origins()),
        "webdav": cfg.get_webdav_status_summary(),
    }


@router.get("/api/system/extension.zip", dependencies=[Depends(require_auth)])
async def download_extension_zip():
    if not EXTENSION_DIR.is_dir() or not (EXTENSION_DIR / "manifest.json").is_file():
        raise HTTPException(status_code=404, detail="Browser extension files not found")

    ext_root = EXTENSION_DIR.resolve()
    entries: list[tuple[str, Path]] = []
    for root, dirnames, filenames in os.walk(EXTENSION_DIR, followlinks=False):
        root_path = Path(root)
        # Prune hidden directories, __pycache__, and any symlinked directories.
        dirnames[:] = [
            d
            for d in dirnames
            if not d.startswith(".")
            and d != "__pycache__"
            and not (root_path / d).is_symlink()
        ]
        for fname in filenames:
            if fname.startswith(".") or fname == ".DS_Store":
                continue
            path = root_path / fname
            if path.suffix in (".pyc", ".pyo") or path.is_symlink():
                continue
            try:
                resolved = path.resolve()
            except OSError:
                continue
            if not resolved.is_relative_to(ext_root) or not resolved.is_file():
                continue
            rel = path.relative_to(EXTENSION_DIR)
            if any(part.startswith(".") or part == "__pycache__" for part in rel.parts):
                continue
            entries.append((rel.as_posix(), resolved))

    entries.sort(key=lambda item: item[0])

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel_posix, path in entries:
            info = zipfile.ZipInfo(rel_posix, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, path.read_bytes())

    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={
            "Content-Disposition": 'attachment; filename="media-vault-extension.zip"',
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )
