# ADR: Track #1 — Vault Resilience, Self-Healing Integrity & Backend Modularization

- **Status**: Implemented & Verified
- **Date**: 2026-11-19
- **Scope**: Media Vault (`v0.5.2` → Track #1)

---

## 1. Context & Architectural Goals

Media Vault is a self-hosted Telegram-backed media vault and gallery with a FastAPI backend, SQLite metadata catalog (`WAL` mode + FTS5 + normalized `media_tags`), Telethon MTProto worker pool, and a zero-build vanilla ES-module PWA frontend.

Prior to Track #1, three structural and operational risks constrained long-term reliability:

1. **Monolithic entry points**: `app/main.py` (~1,680 LOC) combined FastAPI bootstrapping, Pydantic schemas, media streaming/ZIP helpers, and all HTTP routes; `tests/test_api.py` (~3,645 LOC, 46 tests) bundled all domains into a single test file.
2. **Metadata durability gap**: While media payloads live durably in Telegram archive forum topics, the SQLite catalog (`media.db`) had no automated online backup, rotating local retention, off-box Telegram backup mirror, or WAL-safe restore workflow.
3. **Silent drift & limited observability**: Missing thumbnails/previews/metadata, orphaned thumbnail or staging artifacts, and Telegram worker FloodWait/error/cache states could accumulate without an in-app audit, self-healing repair workflow, or per-worker telemetry view.

Track #1 addresses all three while preserving **100% API backwards compatibility**, **backwards-compatible `.env` defaults**, **idempotent SQLite migrations**, and the **zero-build ES-module frontend** (including `< 350` lines in `static/index.html` and zero native `alert`/`confirm`/`prompt` calls).

---

## 2. Implemented Architecture

### 2.1 Backend & Test Suite Modularization
- **`app/context.py`**: Introduced `AppContext` dataclass holding `settings`, `db`, `allocator`, `ingest`, `tg_pool`, `zip_cache`, `backup`, and `integrity`, plus `STATIC_DIR`.
- **`app/schemas.py`**: Centralized all Pydantic request models, including new Track #1 models (`BackupCreateRequest`, `IntegrityAuditRequest`, `IntegrityRepairRequest`).
- **`app/media_ops.py`**: Extracted streaming, HTTP Range parsing, local/Telegram resolution, ZIP central-directory inspection, and delete helpers.
- **`app/routers/`**: Split routes into domain-focused `APIRouter` modules:
  - `deps.py` (`get_ctx`, `require_auth`, `require_auth_query_or_header`)
  - `system.py` (`/api/health`, `/api/system/status`, `/api/events`, cookies & yt-dlp endpoints)
  - `maintenance.py` (`/api/system/backups*`, `/api/system/integrity/*`)
  - `jobs.py`, `ingest.py`, `media.py`, `zip_explorer.py`, `pwa.py`
- **`app/main.py`**: Slimmed to ~119 LOC app factory (`create_app`) + lifespan manager, retaining all legacy `app.state.*` attributes (`settings`, `db`, `allocator`, `ingest`, `tg_pool`, `zip_cache`, plus new `ctx`, `backup`, `integrity`).
- **`tests/`**: Split `tests/test_api.py` into `tests/helpers.py` plus domain modules (`test_media_and_gallery.py`, `test_ingest_and_jobs.py`, `test_telegram_pool.py`, `test_ui_and_pwa.py`, `test_db_core.py`) and added 3 Track #1 suites (`test_backup_restore.py`, `test_integrity_maintenance.py`, `test_worker_telemetry_and_routing.py`).

### 2.2 Automated SQLite Backup & Restore Pipeline
- **`app/db.py`**: Added idempotent `vault_backups` and `maintenance_runs` tables, `Database.backup_to(dest_path)` using a dedicated SQLite source connection (`src.backup(dst, pages=256, sleep=0.005)`) outside `self._lock` so concurrent reads/writes are never blocked, `Database.get_db_stats()`, and `Database.recover_interrupted_maintenance_runs()`.
- **`app/backup.py`**: Implemented `BackupService` (serialized via `asyncio.Lock`, atomic temp `.db` → gzip `.db.gz` → SHA-256 → `os.replace`, rotating local retention with newest-backup protection, optional background scheduler when `BACKUP_INTERVAL_HOURS > 0`, optional Telegram `_VaultBackups` forum topic mirror whose failure never fails the local backup), plus standalone `verify_backup_file()` and `restore_backup_file()`.
- **`scripts/restore_vault.py`**: Standalone WAL-safe CLI restore utility supporting `--backup`, `--db-path`, `--verify-only`, and `--yes`. Takes a pre-restore safety snapshot (falling back to raw file + `-wal`/`-shm` copy if the live DB is corrupted), verifies backup integrity + core tables, clears stale `-wal`/`-shm` sidecars, and atomically replaces `media.db`.

### 2.3 Self-Healing Integrity, Audit & Worker Telemetry
- **`app/integrity.py`**: Implemented `IntegrityService` with single-flight execution guard (`409 Conflict` on concurrent audit/repair), race-safe orphan thumbnail/staging detection (grace period + active staging directory registry across web uploads, Telegram ingest, and active repair temp dirs), batched FloodWait-resilient Telegram archive verification, and bounded/capped background repair (`max_telegram_downloads`, `max_telegram_item_mb`, opt-in orphan purges).
- **`app/telegram_mtproto.py`**: Added thread-safe hit/miss/eviction counters and `stats()` to `TelegramBlockCache`, per-worker telemetry (`online`, `in_cooldown`, `cooldown_remaining_sec`, `active_uploads`, `active_streams`, `flood_wait_count`, `last_error`, `last_error_at`) via `TelegramPool.get_telemetry()`, `upload_backup_file()`, `verify_archive_messages()`, and `download_media_to_path()`.
- **Web UI (`static/js/features/maintenance.js`, `static/js/features/system.js`, `static/index.html`, `static/css/`)**: Upgraded `#systemSheet` into a 4-tab System & Maintenance modal (`Settings` | `Health` | `Backups` | `Integrity`) accessible from the System button (now visible on desktop & mobile) and clickable `#tgStatus` pill, using custom modal dialogs (`confirmDialog` / `toast`) and live SSE refreshes (`maintenance_update`, `backup_update`).

---

## 3. Design & Deviation Ledger

| # | Reference Plan Item | What Was Actually Built | Why Followed or Departed (Codebase Evidence) | Known Risks / Follow-ups |
|---|---|---|---|---|
| 1 | Decompose `app/main.py` into FastAPI `APIRouter` modules + shared context while preserving all endpoints & `app.state.*` | Created `app/context.py` (`AppContext`), `app/schemas.py`, `app/media_ops.py`, and 8 domain routers under `app/routers/`. Reduced `app/main.py` from ~1,680 LOC to 119 LOC while preserving `app.state.{settings,db,allocator,ingest,tg_pool,zip_cache}` and dynamic `app.main.update_ytdlp` lookup. | **Followed + targeted compatibility adaptation**: `tests/test_ingest_and_jobs.py` monkeypatches `app.main.update_ytdlp` directly, and multiple tests access `app.state.db`, `app.state.ingest`, `app.state.tg_pool`. Late attribute lookup in `app/routers/system.py` and mirrored `app.state` fields kept 100% of existing tests green without rewriting test hooks. | Legacy `app.state.*` attributes duplicate fields on `app.state.ctx`; keep both synced until tests migrate exclusively to `app.state.ctx`. |
| 2 | Split monolithic `tests/test_api.py` (~3,645 LOC, 46 tests) by domain with zero test loss | Deleted `tests/test_api.py` and split all 46 tests verbatim into `tests/helpers.py` + 5 domain modules (`test_media_and_gallery.py`, `test_ingest_and_jobs.py`, `test_telegram_pool.py`, `test_ui_and_pwa.py`, `test_db_core.py`), plus 3 new Track #1 test files (14 new tests, 60 total). | **Followed strictly**: AST count verified 46/46 original tests preserved verbatim before adding Track #1 suites. Shared fixtures/helpers in `tests/helpers.py` eliminated duplication. | None. |
| 3 | Automated SQLite online backup (`sqlite3.Connection.backup`), `.db.gz`, rotating retention, optional scheduled backups, optional Telegram `_VaultBackups` topic upload | Built `Database.backup_to()` using a dedicated SQLite source connection outside `self._lock`, `BackupService` in `app/backup.py`, `vault_backups` DB table, `GET/POST/DELETE /api/system/backups` + streaming download, and `TelegramPool.upload_backup_file()`. Safe defaults: `BACKUP_INTERVAL_HOURS=0` (opt-in schedule), `BACKUP_TELEGRAM_ENABLED=false` (opt-in upload), `BACKUP_RETENTION_COUNT=10`. | **Followed + concurrency & security refinements**: Running `conn.backup()` on the shared locked connection would stall gallery/ingest requests on large DBs, so `backup_to()` opens a dedicated WAL reader connection (`pages=256, sleep=0.005`). Also hardened backup resolution/listing against symlinks, hidden temp files, and non-backup filenames, and ensured deleting by filename also cleans the `vault_backups` DB row. | Local backup pruning removes old local `.db.gz` files and DB rows, but does not auto-delete historical backup messages from Telegram `_VaultBackups` (intentionally preserves off-box history). |
| 4 | Safe WAL-aware CLI restore (`scripts/restore_vault.py`) with `--verify-only`, pre-restore snapshot, and `-wal`/`-shm` cleanup | Built `verify_backup_file()`, `restore_backup_file()`, and executable CLI `scripts/restore_vault.py` (`--backup`, `--db-path`, `--verify-only`, `--yes`). | **Followed + disaster-recovery hardening**: Adversarial review revealed that running `sqlite3.backup()` on an already-corrupted live `media.db` would abort the restore. Added fallback to raw `shutil.copy2` of `media.db` + `-wal`/`-shm` sidecars with a warning so disaster recovery always succeeds. | CLI restore must be run while the Media Vault server is stopped so no live process holds open WAL handles; documented prominently in CLI prompt and `README.md`. |
| 5 | Self-healing integrity audit & repair (`app/integrity.py`) with race-safe orphan checks, batched Telegram verification, and bounded repair | Built `IntegrityService` + `maintenance_runs` table + `/api/system/integrity/{status,runs,audit,repair}` endpoints, active staging directory registry in `IngestService` (wired across web uploads, Telegram ingest, and repair temp dirs), single-flight lock (`409 Conflict`), and startup crash recovery for stale `running` runs. | **Followed + rate-limit/safety hardening**: Batched `get_messages` verification respects `INTEGRITY_TELEGRAM_BATCH_SIZE` and `INTEGRITY_TELEGRAM_BATCH_DELAY_SEC`, fails over across workers on `FloodWaitError`, and halts cleanly if all workers enter cooldown. Repair defaults to non-destructive backfill only (`purge_orphan_thumbs=False`, `purge_orphan_staging=False`) with download count and per-item MB caps. | Multi-part Telegram items without local files still require streaming all parts up to `max_telegram_item_mb` to regenerate thumbnails/metadata; very large videos exceeding the cap are skipped cleanly and surfaced in `skipped_items`. |
| 6 | Per-worker Telegram pool telemetry & block-cache utilization without cluttering the main header | Enriched `/api/health` with lightweight non-sensitive summary counts (`telegram_workers_online`, `telegram_workers_in_cooldown`, `backups_count`, `maintenance_busy`) and created authenticated `GET /api/system/status` for full worker cards, block-cache hit/miss/eviction stats, DB/WAL stats, and storage footprint. | **Deliberate security refinement**: `/api/health` is intentionally unauthenticated (bootstraps the lock screen on first load), so exposing detailed worker errors, DB paths, and cache/storage telemetry there would leak operational data when `WEB_AUTH_TOKEN` is enabled. Full telemetry therefore lives behind `require_auth` on `GET /api/system/status`. | None; lock screen continues to bootstrap cleanly while authenticated operators get full telemetry. |
| 7 | Web UI System Health, Backups & Integrity panel with zero build step and strict HTML/dialog guardrails | Upgraded `#systemSheet` into a 4-tab modal (`Settings`, `Health`, `Backups`, `Integrity`) in `static/js/features/maintenance.js`, made `#tgStatus` clickable to open the `Health` tab, and un-hid `#mobileSettingsBtn` on desktop as the unified `System` drawer trigger. | **Followed strictly**: `static/index.html` is 338 lines (strictly below the `< 350` LOC test assertion in `test_ui_and_pwa.py`), zero `window.alert/confirm/prompt` calls (all destructive actions use `confirmDialog`), and all 19 ES modules pass `node --input-type=module --check`. | `renderIntegrityView` re-renders its tab container on live repair progress polls; repair form inputs are disabled while a run is active to prevent editing mid-poll. |

---

## 4. Adversarial Review & Hardening Summary

An independent read-only adversarial code review identified 12 concrete edge cases across backend and frontend, all of which were patched and verified:

1. **Corrupted-DB restore fallback (`app/backup.py`)**: `restore_backup_file` now catches `sqlite3.Error` during pre-restore snapshot and falls back to raw `shutil.copy2` of `media.db` + `-wal`/`-shm` sidecars.
2. **FloodWait propagation & worker availability (`app/telegram_mtproto.py`, `app/integrity.py`)**: `prepare_stream` skips cooled-down workers and propagates `FloodWaitError` directly; `_execute_repair` computes online non-cooldown worker availability accurately.
3. **Async stream iterator cleanup (`app/telegram_mtproto.py`, `app/routers/zip_explorer.py`)**: Both `download_media_to_path` and `_iter_deflated` now call `await iter.aclose()` in `finally` blocks, and multipart repair downloads enforce `max_bytes` upfront.
4. **Staging registry completeness (`app/telegram_mtproto.py`, `app/routers/ingest.py`)**: `_handle_telegram_media` now registers/unregisters `job_dir`, and web upload handlers catch `asyncio.CancelledError` to clean up and unregister `job_dir` on mid-upload disconnect.
5. **RFC 5987 backup download header (`app/routers/maintenance.py`)**: Removed raw `Content-Disposition` override so Starlette `FileResponse(filename=...)` escapes filenames cleanly.
6. **Delete-by-filename DB cleanup (`app/backup.py`)**: `delete_backup` now removes matching `vault_backups` DB rows even when called with a backup filename instead of UUID.
7. **Symlink & hidden-file rejection (`app/backup.py`)**: `_iter_local_backup_files`, `_validate_path_in_backups_dir`, and `resolve_backup_file` reject symlinks, dotfiles, and non-backup extensions.
8. **Maintenance run crash recovery (`app/db.py`, `app/main.py`)**: Added `Database.recover_interrupted_maintenance_runs()` and invoked it on startup so crashed `running` runs transition cleanly to `failed`.
9. **Stale-token UI lock state (`static/js/features/maintenance.js`, `static/js/features/system.js`)**: Tracked `authRejected`, cleared cached tab payloads on `auth:required`/`auth:unlocked`, and kept `#systemSheet` open when unlocking from the Settings tab.
10. **OpenAPI operationId cleanliness (`app/routers/pwa.py`)**: Split `/share-target` into explicit `@router.get` and `@router.post` handlers, eliminating the FastAPI duplicate `operationId` warning.
11. **Non-blocking SQLite backup (`app/db.py`)**: `Database.backup_to` runs on a dedicated SQLite reader connection outside `self._lock`.
12. **Maintenance tab poll/reload polish (`static/js/features/maintenance.js`)**: Disabled repair inputs while busy and guarded queued `runSerializedLoad` retries against closed sheets or switched tabs.
