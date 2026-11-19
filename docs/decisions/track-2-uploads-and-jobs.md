# ADR: Track #2 — Resumable Chunked Uploads, Pre-Flight Deduplication & Smart Job Center

- **Status**: Implemented & Verified
- **Date**: 2026-11-19
- **Scope**: Media Vault (`v0.6.0` → Track #2)

---

## 1. Context & Architectural Goals

Prior to Track #2, Media Vault's ingest and upload pipeline had four practical friction points on real-world LAN, Tailscale, and mobile connections:

1. **Fragile single-shot uploads**: All web uploads streamed through single-request `multipart/form-data` (`POST /api/ingest/upload` and `/api/ingest/upload-folder`). Any Wi-Fi drop, mobile background suspend, or proxy reset discarded the entire transfer with no pause, resume, or per-file queue management.
2. **Wasted bandwidth on duplicate uploads & all-or-nothing playlists**: Files were only SHA-256 deduplicated server-side *after* uploading 100% of the payload. Moreover, `window.crypto.subtle` is unavailable on plain HTTP LAN/Tailscale origins (`http://<ip>:8080`), preventing naive WebCrypto pre-flight hashing. Meanwhile, playlist URLs could only be ingested wholesale via `expand_playlist`, with no preview, vault-duplicate visibility, or cherry-picking.
3. **Non-retryable failed uploads**: `process_staged_upload_job()` deleted the staged temp directory in its `finally` block regardless of outcome, so a transient Telegram mirror outage or FloodWait permanently destroyed the staged file and forced users to re-upload from scratch.
4. **Flat job strip**: `GET /api/jobs` returned an unfiltered fixed slice without status counts, pagination, bulk clear/retry actions, or direct links to finished media items.

Track #2 resolves all four while maintaining **100% backwards compatibility** for `/api/ingest/upload`, `/api/ingest/upload-folder`, Telegram bot/DM ingest, Android PWA Share Target, zero-build ES modules, `< 350` lines in `static/index.html`, and zero native browser dialogs (`alert`/`confirm`/`prompt`).

---

## 2. Implemented Architecture

### 2.1 Resumable Chunked Upload Protocol & Staging Lifecycle
- **`app/config.py`**: Added tunable settings with safe defaults: `UPLOAD_CHUNK_SIZE_MB=8`, `UPLOAD_MAX_CHUNK_SIZE_MB=32`, `UPLOAD_SESSION_TTL_SEC=86400`, `UPLOAD_MAX_STAGING_MB=0` (unlimited unless configured), `UPLOAD_MAX_CONCURRENT_SESSIONS=50`, `FAILED_UPLOAD_RETENTION_SEC=3600`, `FAILED_UPLOAD_MAX_RETAINED_MB=4096`, and `STAGING_GC_INTERVAL_SEC=300`.
- **`app/db.py`**: Added idempotent schema migrations for:
  - `upload_sessions` table + `idx_upload_sessions_status_expires` index (`received_chunks_json`, `received_bytes`, `expected_sha256`, `fingerprint`, `staging_dir`, `staging_file`, `expires_at`, `status`, `job_id`).
  - `ingest_jobs.{staged_path, staged_expires_at, staged_size_bytes}` columns + `idx_ingest_jobs_status_created` and `idx_ingest_jobs_staged_expires` indexes.
  - `idx_media_size_bytes` index on `media_items(size_bytes)` for fast large-file size-candidate pre-flight deduplication.
- **`app/uploads.py` & `app/routers/uploads.py`**:
  - Implemented `UploadSessionService` with auth-protected routes: `POST /api/uploads/sessions`, `GET /api/uploads/sessions`, `GET /api/uploads/sessions/{id}`, `PUT /api/uploads/sessions/{id}/chunks/{chunk_index}`, `POST /api/uploads/sessions/{id}/complete`, and `DELETE /api/uploads/sessions/{id}`.
  - Direct-offset `os.pwrite()` assembly into `staging_dir/up_<session_id>/<safe_filename>` (no temp chunk-file concatenation step, no double disk space usage, supports out-of-order and parallel chunks).
  - Pre-flight SHA-256 short-circuit on session init (`200 OK`, `duplicate: true`, zero staging files written unless `force=true`).
  - Streaming chunk body size enforcement (returns `413` immediately on overflow without buffering unbounded payloads) + optional `X-Chunk-SHA256` per-chunk verification + async final SHA-256 verification on `/complete` (rollback to `'uploading'` on mismatch so corrupted chunks can be retried).
  - Bounded failed-upload retention (`enforce_failed_upload_retention_budget` with oldest-first LRU eviction + periodic `run_gc()` background task + startup crash recovery that preserves active unexpired upload sessions and unexpired retained failed uploads while sweeping unreferenced orphans).
  - Integrated `get_protected_staging_dirs()` into `IngestService.get_active_staging_dirs()` so Track #1's `IntegrityService` never flags active chunked uploads or unexpired retained failed uploads as orphan staging directories.

### 2.2 Plain-HTTP Streaming SHA-256, Upload Queue & Playlist Cherry-Picker
- **`static/js/crypto/sha256.js`**: Zero-dependency FIPS 180-4 `IncrementalSha256` using `Uint32Array` schedules and `BigInt` 64-bit length tracking (`> 4 GB` safe), plus `sha256Bytes()` and `hashFileStreaming()` (2 MB slices yielding to the event loop with `AbortSignal` and progress callbacks, WebCrypto fast-path on HTTPS/localhost `<= 32 MB`, seamless pure-JS fallback on plain HTTP LAN/Tailscale origins).
- **`static/js/features/upload-queue.js` & `static/js/features/uploader.js`**:
  - Multi-file upload queue with two-tier pre-flight deduplication (`<= 128 MB` inline hash; `> 128 MB` instant `size_bytes` candidate check → optional streaming hash with "Skip Check" bypass).
  - Parallel chunk uploader (`CHUNK_CONCURRENCY = 2`, 1 active file at a time to protect home upload bandwidth), intra-chunk XHR progress, EWMA speed/ETA, Pause/Resume/Cancel/Retry, exponential backoff on transient failures, offline/online auto-pause & auto-resume, and `localStorage` (`vault_upload_sessions_v1`) interrupted-upload recovery banner.
  - Preserves Android PWA Share Target (`pwa:shared-files`), drag-and-drop, and folder-to-ZIP upload (now with Abortable XHR cancel button `#cancelFolderUploadBtn`).
- **`static/js/features/playlist-picker.js` & `POST /api/ytdlp/playlist`**:
  - Enriched `POST /api/ytdlp/playlist` with single-query (batched) vault duplicate flags (`normalized_url`, `already_in_vault`, `existing_media_id`, `existing_folder`, `duplicate_count`, `suggested_folder`).
  - Dynamic `<dialog id="playlistModal">` (keeping `static/index.html` at 340 lines) with live search filter, Select All / Select New Only / Deselect All, destination folder/tags/format/force overrides, and direct submission to `POST /api/ingest/urls`.

### 2.3 Retryable Failed Uploads & Smart Job Center
- **`app/ingest.py` & `app/routers/jobs.py`**:
  - Both chunked uploads and legacy `/api/ingest/upload` / `/api/ingest/upload-folder` retain their staged file on failure within `FAILED_UPLOAD_RETENTION_SEC` and `FAILED_UPLOAD_MAX_RETAINED_MB`, enabling one-click `POST /api/jobs/{id}/retry` without re-uploading.
  - Added paginated/filtered `GET /api/jobs` (`status`, `limit`, `offset`, `total`, `has_more`, `counts`), `POST /api/jobs/clear-completed` (never deletes active jobs; cleans staging if `failed` is cleared), `POST /api/jobs/retry-failed` (`retried_count`, `skipped_count`), and `counts` inside SSE `/api/events` payloads.
  - `hydrate_job()` adds `media_ids` and `retryable` while redacting internal `staged_path` from both top-level fields and `params_json`.
- **`static/js/features/jobs.js` & `static/js/features/viewer.js`**:
  - Upgraded `#jobsStrip` into a compact live Smart Job Center with status count pills, Retry All Failed, Clear Completed, expandable paginated History drawer with status filter tabs + Load More, and clickable finished-job links dispatching `media:open` into `viewer.js`.

---

## 3. Design & Deviation Ledger

| # | Reference Plan / Requirement | What Was Actually Built | Why Followed or Departed (Codebase Evidence) | Known Risks / Follow-ups |
|---|---|---|---|---|
| 1 | Direct-offset `os.pwrite` resumable chunked upload protocol (`POST /sessions`, `PUT /chunks/{idx}`, `POST /complete`, `DELETE /sessions/{id}`) without double disk usage | Implemented full `UploadSessionService` (`app/uploads.py`) and `app/routers/uploads.py` writing directly into `staging_dir/up_<id>/<safe_filename>` via `os.pwrite` under per-session `asyncio.Lock` while reading HTTP chunk bodies outside the lock, with `X-Chunk-SHA256`, idempotent chunk re-PUT & idempotent `/complete`, 0-byte support, and `413` streaming overflow abort. | **Followed + concurrency refinement**: Reading the request body outside the session lock prevents slow WAN/mobile clients from stalling concurrent chunks (`CHUNK_CONCURRENCY = 2`) or cancellation, while taking the lock only for validation, `os.pwrite`, and SQLite metadata update keeps state transitions atomic. | `os.pwrite` on sparse files allocates extents on demand on Linux filesystems; `init_session` still checks full `total_size + min_free_buffer_bytes` upfront under `_init_lock` to guard against late ENOSPC. |
| 2 | Plain-HTTP pre-flight deduplication without requiring HTTPS (`window.crypto.subtle` unavailable on LAN/Tailscale HTTP) | Built zero-dependency FIPS 180-4 `IncrementalSha256` (`static/js/crypto/sha256.js`) using `BigInt` 64-bit length counters (`> 4 GB` safe) + two-tier client dedup (`<= 128 MB` inline hash; `> 128 MB` `size_bytes` candidate check via `idx_media_size_bytes` → optional streaming hash with "Skip Check"). | **Followed + UX refinement**: Hashing a multi-GB file synchronously in pure JS on a mobile browser before uploading would feel sluggish when the file is obviously new. Checking `size_bytes` first eliminates hashing entirely for unique `> 128 MB` files while still catching exact duplicates on plain HTTP origins. | Pure-JS SHA-256 runs on the main thread in 2 MB slices yielding via `setTimeout(0)` so UI stays responsive; for multi-GB duplicate candidates on slow mobile CPUs, users can click "Skip Check" to upload immediately. |
| 3 | Bounded TTL retention for failed staged uploads so `POST /api/jobs/{id}/retry` works without re-uploading | Extended `ingest_jobs` with `staged_path`, `staged_expires_at`, `staged_size_bytes`; retained failed staged files for **both** chunked uploads and legacy `/api/ingest/upload` + `/api/ingest/upload-folder` under `FAILED_UPLOAD_RETENTION_SEC` + `FAILED_UPLOAD_MAX_RETAINED_MB` (oldest-first LRU eviction) with atomic DB claim (`claim_staged_job_for_cleanup`) coordinated with `retry_job`. | **Refined beyond baseline**: Applying the exact same bounded retention to legacy `/api/ingest/upload` and `/api/ingest/upload-folder` ensures consistent retryability regardless of upload path, and atomic DB claiming eliminates GC-vs-retry TOCTOU races. | Multi-file legacy folder ZIPs stay as a single staged `.zip` until retried or expired; retrying a cancelled upload job is intentionally not retained (cancel cleans staging immediately to honor explicit user deletion). |
| 4 | Interactive Playlist Preview / Cherry-Pick modal powered by `/api/ytdlp/playlist` with vault duplicate visibility | Enriched `POST /api/ytdlp/playlist` with batched `find_media_by_source_urls()` duplicate detection (`already_in_vault`, `existing_media_id`, `existing_folder`, `duplicate_count`, `suggested_folder`) and built dynamic `<dialog id="playlistModal">` in `static/js/features/playlist-picker.js` (keeping `static/index.html` at 340 lines). | **Followed + HTML budget & SQLite safety**: Rendering `<dialog id="playlistModal">` dynamically in JS kept `static/index.html` strictly below the `< 350` line guardrail (340 lines), and chunking SQLite `IN (...)` queries into batches of 200 prevents `too many SQL variables` crashes on large playlists. | Playlist preview still depends on `yt-dlp --flat-playlist`; sites without flat-playlist extraction fall back gracefully with a clear error message. |
| 5 | Smart Job Center with live active jobs, expandable paginated history drawer, status filters, Clear Completed, Retry All Failed, and clickable jump-to-media links | Upgraded `GET /api/jobs` (status filters, `offset`/`limit`, `total`, `has_more`, `counts`), added `POST /api/jobs/clear-completed` & `POST /api/jobs/retry-failed`, included `counts` in SSE `/api/events`, and upgraded `static/js/features/jobs.js` + `viewer.js` (`media:open`). | **Followed + SSE efficiency refinement**: Existing tests assert `new EventSource(withToken("/api/events"))` and `if (newlyFinished)` behavior; preserved all legacy hooks while restricting open-drawer history reloads to structural transitions (`done`/`failed`/`cancelled`/`cleared`/`queued`) so 0.7s download progress ticks do not reset pagination or spam `/api/jobs`. | Live progress cards in `upload-queue.js` and `jobs.js` still use container innerHTML updates on state ticks; throttling renders via `requestAnimationFrame` keeps updates smooth, though future incremental DOM patching could further reduce mobile touch-target churn. |
| 6 | Preserve Track #1 `IntegrityService` orphan detection without false positives on active uploads or retained failed jobs | Wired `UploadSessionService.get_protected_staging_dirs()` into `IngestService.get_active_staging_dirs()` and startup crash recovery (`recover_on_startup()`). | **Followed + leak prevention**: Startup crash recovery marks interrupted staged upload jobs `failed` and assigns a bounded `staged_expires_at` (rather than leaving `NULL`), ensuring interrupted jobs are both retryable within TTL and guaranteed to expire cleanly on disk. | None; verified by both unit tests and `IntegrityService` audit tests. |

---

## 4. Adversarial Review & Hardening Summary

An independent read-only adversarial audit (`gemini-4-hydrogen`, `high` thinking) found **0 Critical**, **4 High**, **5 Medium**, and **7 Low** items; all High, key Medium, and Low items were patched and covered by regression tests:

1. **GC / LRU Eviction vs. `retry_job` TOCTOU race (`app/db.py`, `app/uploads.py`, `app/ingest.py`)**: Added atomic `Database.claim_staged_job_for_cleanup()` (clears `staged_*` only when `status` is still terminal and expired, returning `None` if already retried), cleared `staged_expires_at = NULL` atomically inside `claim_job_for_retry()`, and coordinated `retry_job`, `_cleanup_expired_failed_uploads_sync`, and `enforce_failed_upload_retention_budget` with a shared `threading.RLock`.
2. **Internal `staged_path` leakage in `params_json` (`app/ingest.py`)**: Updated `hydrate_job()` to strip `"staged_path"` from serialized `params_json` in addition to the top-level column, eliminating filesystem path exposure across REST and SSE responses.
3. **Concurrent `init_session` quota/concurrency/disk race (`app/uploads.py`)**: Added `self._init_lock = asyncio.Lock()` around capacity, staging quota, free-disk checks, and DB session creation.
4. **Infinite disk leak on crash-recovered staged upload jobs (`app/db.py`, `app/ingest.py`)**: `recover_interrupted_jobs()` now populates `staged_expires_at = now + failed_upload_retention_sec` whenever a crash-recovered job has a non-empty `staged_path`.
5. **Cancel-then-retry task race on URL jobs (`app/ingest.py`)**: Guarded `CancelledError` / `Exception` status writes and `_active_procs.pop()` with `self._job_tasks.get(job_id) is asyncio.current_task()`.
6. **SQLite `IN (...)` variable limit & unbounded batch URLs (`app/schemas.py`, `app/db.py`)**: Bounded `CheckDuplicateRequest.urls` to `max_length=500` and chunked `find_media_by_source_urls()` into 200-item SQL batches.
7. **SSE history pagination resets, duplicate media refresh, active-count truncation & stuck buttons (`static/js/features/jobs.js`)**: Used `counts.active` for active badge totals, returned `newlyFinished` to avoid duplicate `media:changed` triggers, restricted open-drawer reloads to structural job transitions, and added `finally` blocks to re-enable action buttons.
8. **Upload resume metadata matching & expired retry progress reset (`static/js/features/upload-queue.js`)**: Persisted and verified `folder`, `force`, `tags`, and `forwardToTelegram` before resuming saved sessions, and reset progress counters in `retryItem()` when `!item.sessionId`.
9. **Protocol & defensive polish (`app/uploads.py`, `app/routers/ingest.py`, `app/routers/jobs.py`, `static/js/features/uploader.js`)**: Returned HTTP `413` on oversized chunk payloads, wrapped `complete_session` DB writes inside the rollback-safe `try...except`, pruned idle per-session locks, scaled `get_protected_staging_dirs()` scan limit with `upload_max_concurrent_sessions`, sanitized legacy upload filenames against `.`/`..`, widened `retry-failed` scan window so expired failures don't starve older retryable jobs, and escaped yt-dlp format fields in the Formats modal.
