from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from app.context import AppContext
from app.routers.deps import get_ctx, require_auth
from app.schemas import UploadSessionCompleteRequest, UploadSessionInitRequest
from app.uploads import UploadError

router = APIRouter()


def _upload_error_response(err: UploadError) -> JSONResponse:
    content: dict[str, Any] = {"detail": err.detail}
    if err.extra:
        content.update(err.extra)
    return JSONResponse(status_code=err.status_code, content=content)


def _require_uploads(ctx: AppContext) -> Any:
    if ctx.uploads is None:
        raise HTTPException(status_code=503, detail="Upload service unavailable")
    return ctx.uploads


@router.post("/api/uploads/sessions", dependencies=[Depends(require_auth)])
async def create_upload_session(
    payload: UploadSessionInitRequest,
    ctx: AppContext = Depends(get_ctx),
):
    uploads = _require_uploads(ctx)
    try:
        res = await uploads.init_session(payload)
    except UploadError as err:
        return _upload_error_response(err)
    return JSONResponse(status_code=200 if res["duplicate"] else 201, content=res)


@router.get("/api/uploads/sessions", dependencies=[Depends(require_auth)])
async def list_upload_sessions(
    status: str | None = Query(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    uploads = _require_uploads(ctx)
    return {"sessions": uploads.list_sessions(status=status)}


@router.get("/api/uploads/sessions/{session_id}", dependencies=[Depends(require_auth)])
async def get_upload_session(
    session_id: str,
    ctx: AppContext = Depends(get_ctx),
):
    uploads = _require_uploads(ctx)
    try:
        return await uploads.get_session_status(session_id)
    except UploadError as err:
        return _upload_error_response(err)


@router.put("/api/uploads/sessions/{session_id}/chunks/{chunk_index}", dependencies=[Depends(require_auth)])
async def upload_session_chunk(
    session_id: str,
    chunk_index: int,
    request: Request,
    x_chunk_sha256: str | None = Header(default=None, alias="X-Chunk-SHA256"),
    ctx: AppContext = Depends(get_ctx),
):
    uploads = _require_uploads(ctx)
    try:
        return await uploads.receive_chunk(
            session_id,
            chunk_index,
            request.stream(),
            expected_chunk_sha256=x_chunk_sha256,
        )
    except UploadError as err:
        return _upload_error_response(err)


@router.post(
    "/api/uploads/sessions/{session_id}/complete",
    status_code=202,
    dependencies=[Depends(require_auth)],
)
async def complete_upload_session(
    session_id: str,
    payload: UploadSessionCompleteRequest | None = Body(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    uploads = _require_uploads(ctx)
    try:
        return await uploads.complete_session(
            session_id,
            sha256=payload.sha256 if payload else None,
        )
    except UploadError as err:
        return _upload_error_response(err)


@router.delete("/api/uploads/sessions/{session_id}", dependencies=[Depends(require_auth)])
async def cancel_upload_session(
    session_id: str,
    ctx: AppContext = Depends(get_ctx),
):
    uploads = _require_uploads(ctx)
    try:
        res = await uploads.cancel_session(session_id)
    except UploadError as err:
        return _upload_error_response(err)
    return {"session_id": session_id, "status": res["status"], "deleted": True}
