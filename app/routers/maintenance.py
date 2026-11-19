from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import FileResponse

from app.context import AppContext
from app.routers.deps import get_ctx, require_auth
from app.schemas import BackupCreateRequest, IntegrityAuditRequest, IntegrityRepairRequest

router = APIRouter()


@router.get("/api/system/backups", dependencies=[Depends(require_auth)])
async def list_backups(
    limit: int = Query(default=50, ge=1, le=200),
    ctx: AppContext = Depends(get_ctx),
):
    return {
        "backups": ctx.backup.list_backups(limit=limit),
        "config": ctx.backup.get_config_summary(),
    }


@router.post("/api/system/backups", status_code=201, dependencies=[Depends(require_auth)])
async def create_backup(
    payload: BackupCreateRequest | None = Body(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    req = payload or BackupCreateRequest()
    try:
        return await ctx.backup.create_backup(
            trigger="manual",
            upload_to_telegram=req.upload_to_telegram,
            note=req.note,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Backup failed: {exc}")


@router.get("/api/system/backups/{backup_id}/download", dependencies=[Depends(require_auth)])
async def download_backup(
    backup_id: str,
    ctx: AppContext = Depends(get_ctx),
):
    try:
        meta, path = ctx.backup.resolve_backup_file(backup_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Backup file not found")
    safe_name = path.name
    return FileResponse(
        path,
        media_type="application/gzip",
        filename=safe_name,
        headers={
            "Cache-Control": "no-store",
            "X-Backup-Id": str(meta.get("id") or backup_id),
        },
    )


@router.delete("/api/system/backups/{backup_id}", dependencies=[Depends(require_auth)])
async def delete_backup(
    backup_id: str,
    ctx: AppContext = Depends(get_ctx),
):
    removed = ctx.backup.delete_backup(backup_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Backup not found")
    return {"deleted": True, "id": removed.get("id") or backup_id, "backup": removed}


@router.get("/api/system/integrity/status", dependencies=[Depends(require_auth)])
async def integrity_status(ctx: AppContext = Depends(get_ctx)):
    return ctx.integrity.get_status()


@router.get("/api/system/integrity/runs", dependencies=[Depends(require_auth)])
async def list_integrity_runs(
    kind: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    ctx: AppContext = Depends(get_ctx),
):
    if kind is not None and kind not in ("audit", "repair"):
        raise HTTPException(status_code=400, detail="kind must be 'audit' or 'repair'")
    return {"runs": ctx.db.list_maintenance_runs(kind=kind, limit=limit)}


@router.get("/api/system/integrity/runs/{run_id}", dependencies=[Depends(require_auth)])
async def get_integrity_run(
    run_id: str,
    ctx: AppContext = Depends(get_ctx),
):
    run = ctx.db.get_maintenance_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Maintenance run not found")
    return run


@router.post("/api/system/integrity/audit", dependencies=[Depends(require_auth)])
async def run_integrity_audit(
    payload: IntegrityAuditRequest | None = Body(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    req = payload or IntegrityAuditRequest()
    try:
        return await ctx.integrity.run_audit(
            verify_telegram=req.verify_telegram,
            staging_grace_sec=req.staging_grace_sec,
            issue_sample_limit=req.issue_sample_limit,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Integrity audit failed: {exc}")


@router.post("/api/system/integrity/repair", status_code=202, dependencies=[Depends(require_auth)])
async def run_integrity_repair(
    payload: IntegrityRepairRequest | None = Body(default=None),
    ctx: AppContext = Depends(get_ctx),
):
    req = payload or IntegrityRepairRequest()
    try:
        return ctx.integrity.start_repair(
            backfill_thumbs=req.backfill_thumbs,
            backfill_previews=req.backfill_previews,
            backfill_metadata=req.backfill_metadata,
            purge_orphan_thumbs=req.purge_orphan_thumbs,
            purge_orphan_staging=req.purge_orphan_staging,
            staging_grace_sec=req.staging_grace_sec,
            max_telegram_downloads=req.max_telegram_downloads,
            max_telegram_item_mb=req.max_telegram_item_mb,
            media_ids=req.media_ids,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Integrity repair failed to start: {exc}")
