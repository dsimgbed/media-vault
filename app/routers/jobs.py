from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.context import AppContext
from app.routers.deps import get_ctx, require_auth
from app.schemas import RetryJobRequest

router = APIRouter()


@router.get("/api/jobs", dependencies=[Depends(require_auth)])
async def list_jobs(
    limit: int = Query(default=25, ge=1, le=100),
    ctx: AppContext = Depends(get_ctx),
):
    return {"jobs": ctx.db.list_jobs(limit=limit)}


@router.get("/api/jobs/{job_id}", dependencies=[Depends(require_auth)])
async def get_job(job_id: str, ctx: AppContext = Depends(get_ctx)):
    job = ctx.db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/api/jobs/{job_id}/cancel", dependencies=[Depends(require_auth)])
async def cancel_job(job_id: str, ctx: AppContext = Depends(get_ctx)):
    try:
        return await ctx.ingest.cancel_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/api/jobs/{job_id}/retry", status_code=202, dependencies=[Depends(require_auth)])
async def retry_job(
    job_id: str,
    force: bool = Query(default=False),
    payload: RetryJobRequest | None = Body(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    effective_force = bool(force or (payload.force if payload else False))
    try:
        return await ctx.ingest.retry_job(job_id, force=effective_force)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/api/events", dependencies=[Depends(require_auth)])
async def stream_events(request: Request, ctx: AppContext = Depends(get_ctx)):
    async def event_stream():
        sub = ctx.db.subscribe()
        _, queue = sub
        try:
            yield ": connected\n\n"
            init_ev = {
                "type": "connected",
                "ok": True,
                "jobs": ctx.db.list_jobs(limit=8),
            }
            yield f"data: {json.dumps(init_ev)}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                if not isinstance(event, dict) or event.get("type") == "_shutdown":
                    break
                payload = dict(event)
                if payload.get("type") == "job_update" and "jobs" not in payload:
                    payload["jobs"] = ctx.db.list_jobs(limit=8)
                yield f"data: {json.dumps(payload)}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            ctx.db.unsubscribe(sub)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
