from __future__ import annotations

from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from app.context import STATIC_DIR
from app.routers.deps import read_cached_html

router = APIRouter()


@router.get("/sw.js")
async def service_worker():
    sw_file = STATIC_DIR / "sw.js"
    if not sw_file.is_file():
        raise HTTPException(status_code=404, detail="Service worker not found")
    return FileResponse(
        sw_file,
        media_type="text/javascript",
        headers={
            "Cache-Control": "no-cache",
            "Service-Worker-Allowed": "/",
        },
    )


@router.get("/manifest.webmanifest")
async def web_manifest():
    manifest_file = STATIC_DIR / "manifest.webmanifest"
    if not manifest_file.is_file():
        raise HTTPException(status_code=404, detail="Manifest not found")
    return FileResponse(
        manifest_file,
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-cache"},
    )


@router.get("/manifest.json")
async def web_manifest_json():
    manifest_file = STATIC_DIR / "manifest.json"
    if not manifest_file.is_file():
        manifest_file = STATIC_DIR / "manifest.webmanifest"
    if not manifest_file.is_file():
        raise HTTPException(status_code=404, detail="Manifest not found")
    return FileResponse(
        manifest_file,
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-cache"},
    )


@router.get("/favicon.ico")
async def favicon():
    icon_file = STATIC_DIR / "icons" / "icon-192.png"
    if not icon_file.is_file():
        raise HTTPException(status_code=404, detail="Favicon not found")
    return FileResponse(
        icon_file,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=86400"},
    )


def _share_target_redirect(request: Request) -> RedirectResponse:
    # Fallback only reached when the PWA service worker did not intercept an
    # Android share. Unauthenticated, so it must not read the request body
    # or ingest anything — only redirect into the uploader mode.
    if request.method == "POST":
        return RedirectResponse(url="/?mode=uploader&share=fallback", status_code=303)
    params: list[tuple[str, str]] = [("mode", "uploader")]
    shared_pairs = [
        (key, val)
        for key in ("url", "text", "title")
        if (val := request.query_params.get(key)) is not None and val != ""
    ]
    if shared_pairs:
        params.append(("shared", "1"))
        params.extend(shared_pairs)
    return RedirectResponse(url=f"/?{urlencode(params)}", status_code=303)


@router.get("/share-target")
async def share_target_fallback_get(request: Request):
    return _share_target_redirect(request)


@router.post("/share-target")
async def share_target_fallback_post(request: Request):
    return _share_target_redirect(request)


@router.get("/", response_class=HTMLResponse)
async def index():
    html = await read_cached_html(STATIC_DIR / "index.html")
    if html is None:
        return HTMLResponse(
            "<h1>Static UI missing</h1>",
            status_code=404,
            headers={"Cache-Control": "no-cache"},
        )
    return HTMLResponse(
        html,
        headers={"Cache-Control": "no-cache"},
    )
