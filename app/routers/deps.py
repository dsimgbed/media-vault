from __future__ import annotations

from fastapi import Depends, HTTPException, Query, Request

from app.context import AppContext


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
