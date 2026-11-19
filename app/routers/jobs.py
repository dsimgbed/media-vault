from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.context import AppContext
from app.routers.deps import get_ctx, require_auth
from app.schemas import ClearJobsRequest, RetryFailedJobsRequest, RetryJobRequest

router = APIRouter()

_ALLOWED_JOB_STATUSES = {
    "all",
    "active",
    "queued",
    "downloading",
    "allocating",
    "done",
    "failed",
    "cancelled",
}


@router.get("/api/jobs", dependencies=[Depends(require_auth)])
async def list_jobs(
    limit: int = Query(default=25, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    if status is not None and status not in _ALLOWED_JOB_STATUSES:
        raise HTTPException(status_code=400, detail=f"Invalid job status filter: {status}")

    raw_jobs = ctx.db.list_jobs(limit=limit, offset=offset, status=status)
    jobs = [ctx.ingest.hydrate_job(j) for j in raw_jobs]
    total = ctx.db.count_jobs(status=status)
    counts = ctx.db.get_job_counts()
    return {
        "jobs": jobs,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + len(jobs)) < total,
        "counts": counts,
    }


@router.post("/api/jobs/clear-completed", dependencies=[Depends(require_auth)])
async def clear_completed_jobs(
    payload: ClearJobsRequest | None = Body(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    statuses = list(payload.statuses) if payload else ["done", "cancelled"]
    deleted_rows = ctx.db.clear_jobs(statuses)
    cleaned_staging = 0
    for row in deleted_rows:
        if row.get("staged_path") and ctx.ingest.cleanup_job_staged_files(row):
            cleaned_staging += 1
    return {
        "deleted": len(deleted_rows),
        "cleaned_staging": cleaned_staging,
    }


@router.post("/api/jobs/retry-failed", status_code=202, dependencies=[Depends(require_auth)])
async def retry_failed_jobs(
    payload: RetryFailedJobsRequest | None = Body(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    req = payload or RetryFailedJobsRequest()
    scan_limit = max(req.limit * 4, 200)
    if req.include_cancelled:
        candidates = sorted(
            ctx.db.list_jobs(limit=scan_limit, status="failed")
            + ctx.db.list_jobs(limit=scan_limit, status="cancelled"),
            key=lambda j: str(j.get("created_at") or ""),
            reverse=True,
        )
    else:
        candidates = ctx.db.list_jobs(limit=scan_limit, status="failed")

    retried_ids: list[str] = []
    skipped: list[dict[str, str]] = []
    for job in candidates:
        if len(retried_ids) >= req.limit:
            break
        jid = job["id"]
        try:
            await ctx.ingest.retry_job(jid, force=req.force)
            retried_ids.append(jid)
        except (KeyError, ValueError) as exc:
            skipped.append({"job_id": jid, "reason": str(exc)})

    return {
        "retried_ids": retried_ids,
        "retried_count": len(retried_ids),
        "skipped": skipped,
        "skipped_count": len(skipped),
    }


@router.get("/api/jobs/{job_id}", dependencies=[Depends(require_auth)])
async def get_job(job_id: str, ctx: AppContext = Depends(get_ctx)):
    job = ctx.db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return ctx.ingest.hydrate_job(job)


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
                "jobs": [
                    ctx.ingest.hydrate_job(j) for j in ctx.db.list_jobs_for_strip(limit=20)
                ],
                "counts": ctx.db.get_job_counts(),
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
                if payload.get("type") == "job_update":
                    if "jobs" not in payload:
                        payload["jobs"] = [
                            ctx.ingest.hydrate_job(j)
                            for j in ctx.db.list_jobs_for_strip(limit=20)
                        ]
                    if "counts" not in payload:
                        payload["counts"] = ctx.db.get_job_counts()
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
