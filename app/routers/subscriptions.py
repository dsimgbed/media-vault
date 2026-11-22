from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from app.context import AppContext
from app.routers.deps import get_ctx, require_auth
from app.schemas import (
    SubscriptionCheckRequest,
    SubscriptionCreateRequest,
    SubscriptionUpdateRequest,
)
from app.subscriptions import (
    SubscriptionBusyError,
    SubscriptionFetchError,
    SubscriptionNotFoundError,
    SubscriptionValidationError,
)

router = APIRouter()

_ALLOWED_ENTRY_STATUSES = {"all", "seen", "queued", "ingested", "skipped", "failed"}


def _require_subscriptions(ctx: AppContext) -> Any:
    if ctx.subscriptions is None:
        raise HTTPException(status_code=503, detail="Subscription service unavailable")
    return ctx.subscriptions


@router.get("/api/subscriptions", dependencies=[Depends(require_auth)])
async def list_subscriptions(ctx: AppContext = Depends(get_ctx)):
    subs = _require_subscriptions(ctx)
    return {
        "subscriptions": subs.list_subscriptions(),
        "summary": subs.get_health_summary(),
    }


@router.post("/api/subscriptions", status_code=201, dependencies=[Depends(require_auth)])
async def create_subscription(
    payload: SubscriptionCreateRequest,
    ctx: AppContext = Depends(get_ctx),
):
    subs = _require_subscriptions(ctx)
    try:
        return subs.create_subscription(payload.model_dump())
    except SubscriptionValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/api/subscriptions/{sub_id}", dependencies=[Depends(require_auth)])
async def get_subscription(sub_id: str, ctx: AppContext = Depends(get_ctx)):
    subs = _require_subscriptions(ctx)
    try:
        return subs.get_subscription(sub_id)
    except SubscriptionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.patch("/api/subscriptions/{sub_id}", dependencies=[Depends(require_auth)])
async def update_subscription(
    sub_id: str,
    payload: SubscriptionUpdateRequest,
    ctx: AppContext = Depends(get_ctx),
):
    subs = _require_subscriptions(ctx)
    try:
        return subs.update_subscription(sub_id, payload.model_dump(exclude_unset=True))
    except SubscriptionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SubscriptionValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete("/api/subscriptions/{sub_id}", dependencies=[Depends(require_auth)])
async def delete_subscription(sub_id: str, ctx: AppContext = Depends(get_ctx)):
    subs = _require_subscriptions(ctx)
    try:
        deleted = subs.delete_subscription(sub_id)
    except SubscriptionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"deleted": True, "id": sub_id, "subscription": deleted}


@router.post("/api/subscriptions/{sub_id}/check", dependencies=[Depends(require_auth)])
async def check_subscription(
    sub_id: str,
    dry_run: bool = Query(default=False),
    payload: SubscriptionCheckRequest | None = Body(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    subs = _require_subscriptions(ctx)
    effective_dry_run = bool(dry_run or (payload.dry_run if payload else False))
    try:
        return await subs.check_subscription(
            sub_id,
            dry_run=effective_dry_run,
            trigger="manual",
        )
    except SubscriptionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SubscriptionBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except SubscriptionValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except SubscriptionFetchError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/api/subscriptions/{sub_id}/entries", dependencies=[Depends(require_auth)])
async def list_subscription_entries(
    sub_id: str,
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    ctx: AppContext = Depends(get_ctx),
):
    if status is not None and status not in _ALLOWED_ENTRY_STATUSES:
        raise HTTPException(status_code=400, detail=f"Invalid subscription entry status filter: {status}")
    subs = _require_subscriptions(ctx)
    try:
        return subs.list_entries(sub_id, status=status, limit=limit, offset=offset)
    except SubscriptionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
