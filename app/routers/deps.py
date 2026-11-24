from __future__ import annotations

import asyncio
from pathlib import Path
import stat
import threading

from fastapi import Depends, HTTPException, Query, Request

from app.context import AppContext

_HTML_CACHE_LOCK = threading.Lock()
_HTML_CACHE: dict[Path, tuple[int, str]] = {}


def clear_html_cache() -> None:
    with _HTML_CACHE_LOCK:
        _HTML_CACHE.clear()


def _read_cached_html_sync(path: Path) -> str | None:
    resolved = path.resolve()
    try:
        st = resolved.stat()
    except OSError:
        with _HTML_CACHE_LOCK:
            _HTML_CACHE.pop(resolved, None)
        return None
    if not stat.S_ISREG(st.st_mode):
        return None
    mtime_ns = st.st_mtime_ns
    with _HTML_CACHE_LOCK:
        cached = _HTML_CACHE.get(resolved)
        if cached is not None and cached[0] == mtime_ns:
            return cached[1]
    try:
        content = resolved.read_text(encoding="utf-8")
    except OSError:
        return None
    with _HTML_CACHE_LOCK:
        _HTML_CACHE[resolved] = (mtime_ns, content)
    return content


async def read_cached_html(path: Path) -> str | None:
    return await asyncio.to_thread(_read_cached_html_sync, path)


def get_ctx(request: Request) -> AppContext:
    return request.app.state.ctx


def require_auth(
    request: Request,
    token: str | None = Query(default=None),
    ctx: AppContext = Depends(get_ctx),
) -> None:
    expected = ctx.settings.web_auth_token.strip()
    if not expected:
        return
    auth_header = request.headers.get("authorization", "")
    bearer = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else ""
    if bearer != expected and (token or "") != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
