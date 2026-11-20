# ADR: Track #6 — Zero-Buffer Streaming & Hot-Cache Overhaul

- **Status**: Implemented & Verified
- **Date**: 2026-11-19
- **Scope**: Media Vault (`v0.7.0` → Track #6)

---

## 1. Context & Architectural Goals

Prior to Track #6, Media Vault's download, archive exploration, and Telegram MTProto streaming paths had four scaling and latency bottlenecks:

1. **Buffered bulk ZIP downloads**: `build_bulk_download_response()` materialized the entire selected bundle into a `tempfile.SpooledTemporaryFile(max_size=16MB)` before sending byte `0` to the browser — blocking time-to-first-byte (TTFB) on multi-GB Telegram archives, consuming server temp disk space, and leaving no clean early abort on client disconnect.
2. **Unbounded, volatile ZIP directory cache**: `ctx.zip_cache` / `app.state.zip_cache` was an unbounded in-memory Python `dict` lost on every server restart (forcing repeated MTProto tail fetches for ZIP explorer views) with no LRU bound and incomplete invalidation on file/location mutations.
3. **Unaligned cache fragmentation, stampedes, and cold sequential playback**: `TelegramPool.iter_telegram_range()` aligned requests only to `4096`-byte boundaries rather than a canonical fixed block grid, causing overlapping browser Range probes (`0-100`, `5000-5100`, etc.) to miss cache; concurrent callers on the same uncached block triggered duplicate MTProto `GetFileRequest` RPCs; there was no sequential read-ahead prefetch or multipart next-part warming; and all hot blocks were lost on restart.
4. **Ops visibility & cache lifecycle**: Operators had no L2 disk cache footprint reporting, no prefetch/coalescing or ZIP cache telemetry in `/api/system/status`, and no safe way to purge stream/ZIP caches from the UI or API.

Track #6 resolves all four while preserving **100% backwards compatibility** with all existing HTTP routes, `.env` defaults, `AppContext`/`app.state` contracts, Track #1 SQLite backup/integrity guarantees, `/api/health` path/error non-leakage, `< 350` lines in `static/index.html`, and zero native browser dialogs.

---

## 2. Implemented Architecture

### 2.1 True Zero-Buffer Async Streaming Bulk ZIP64
- **`app/streaming_zip.py`** (pure stdlib `struct` + `zlib`):
  - Implements APPNOTE 6.3.x `ZIP_STORED` streaming with GP flags `0x0808` (bit 3 data descriptors + bit 11 UTF-8 filenames), 16-byte ZIP32 / 24-byte ZIP64 `PK\x07\x08` data descriptors, strict APPNOTE 4.5.3 ZIP64 extra field ordering (`uncompressed`, `compressed`, `local_header_offset`), and automatic ZIP64 EOCD (`PK\x06\x06`) + Locator (`PK\x06\x07`) when entry sizes/offsets reach `0xFFFFFFFF` or entry counts reach `0xFFFF`.
  - Sanitizes archive entry paths (`sanitize_zip_arcname` / `deduplicate_arcname`), stripping control chars, `NUL`, `.`/`..`, and Windows drive prefixes (`C:\`, `C:`) while preserving UTF-8 and deterministically deduplicating colliding basenames.
  - Opens each entry lazily via `stream_factory` and guarantees `aclose()`/`close()` cleanup on upstream errors, `GeneratorExit`, or `asyncio.CancelledError`.
- **`app/media_ops.py` & `app/routers/media.py`**:
  - Replaced `SpooledTemporaryFile` in `build_bulk_download_response()` with `iter_streaming_zip64` wrapped in `StreamingResponse(..., media_type="application/zip")` with `Cache-Control: no-store`, `X-Accel-Buffering: no`, and chunked transfer encoding (no `Content-Length`).
  - Added thread-safe `_aiter_file_range()` offloading file open/seek/read/close via `asyncio.to_thread` serialized under a per-stream `threading.Lock` so client cancellation never races with an in-flight `fh.read()`.

### 2.2 Two-Tier Bounded + SQLite-Persisted ZIP Central Directory Cache
- **`app/db.py`**:
  - Added idempotent `zip_entries_cache` table + `idx_zip_entries_cache_accessed` index in `Database._init_schema()`.
  - Added transactional invalidation inside `delete_media()` and structural `update_media()` (`file_path`, `telegram_chat_id`, `telegram_msg_id`, `part_count`, `parts`) while preserving cached entries on metadata-only edits (tags, title, etc.).
  - Added bounded CRUD (`get_zip_cache_entry`, `put_zip_cache_entry`, `delete_zip_cache_entry`, `clear_zip_cache`, `get_zip_cache_db_stats`) enforcing both `ZIP_CACHE_DB_MAX_ROWS` (default `500`) and `ZIP_CACHE_DB_MAX_MB` (default `16`) via oldest-first LRU eviction, plus `zip_cache_entries` in `get_db_stats()`.
- **`app/zip_cache.py` & `app/main.py`**:
  - Implemented `ZipDirectoryCache(MutableMapping)` combining a bounded L1 RAM `OrderedDict` (`ZIP_CACHE_MAX_ITEMS=256`) with zlib-compressed L2 SQLite persistence, deterministic `compute_zip_fingerprint(item)` verification, self-healing on corrupt blobs, and drop-in compatibility for `ctx.zip_cache` / `app.state.zip_cache`.

### 2.3 Two-Tier Hot-Block Cache, Canonical Grid, Coalescing & Prefetch
- **`app/stream_cache.py`**:
  - **`TwoTierBlockCache`** (aliased as `TelegramBlockCache` for full backwards compatibility):
    - Preserves all legacy L1 RAM LRU methods and top-level `stats()` fields alongside nested `l1` and `l2` telemetry.
    - Optional L2 persistent disk tier (`TELEGRAM_DISK_CACHE_MB=0` default, opt-in) stored as sharded `.blk` files under `DATA_DIR/stream_cache/<2hex>/<24hex_ns>_<16hex_off>_<8hex_blksize>.blk` — keeping Track #1 SQLite backups lean and immune to disk cache bloat.
    - Atomic `.tmp_<uuid>.blk` + `os.replace()` writes, ephemeral `("media", id)` RAM-only guard, symlink-safe crash reconciliation, block-size change auto-invalidation, and non-blocking lock discipline (`_disk_lock` never wraps disk I/O and never nests with `_lock`).
  - **`StreamFetchCoordinator`**:
    - Single-flight coalescing (`fetch_block`) with cancellation-safe waiter refcounting and multi-event-loop (`asyncio.run`) resilience.
    - Bounded sequential read-ahead prefetch (`TELEGRAM_PREFETCH_BLOCKS=2`, `TELEGRAM_PREFETCH_MAX_CONCURRENCY=4`) firing only on multi-block requests or contiguous sequential strides, respecting EOF, worker cooldowns, and namespace cancellation on seek/abort.
- **`app/telegram_mtproto.py`**:
  - Switched cached `iter_telegram_range()` to canonical fixed-grid `(offset // block_size) * block_size` fetching while preserving `4096`-byte aligned pass-through when caching is disabled.
  - Added stream-aware `_ordered_clients(purpose="stream")` weighting active uploads (`active_streams + active_uploads * 2`), multipart next-part `prepare_stream()` background warming in `iter_telegram_multipart_range()`, `purge_stream_caches(clear_ram, clear_disk)`, and clean background task shutdown in `stop()`.

### 2.4 Ops Telemetry, Cache Purge API & Health UI
- **`app/integrity.py`**: Extended `IntegrityService.get_storage_footprint()` with `stream_cache_entries` and `stream_cache_bytes`.
- **`app/routers/system.py`**: Kept `/api/health` completely unchanged (zero path/error leakage) and added `zip_cache` stats to `/api/system/status`.
- **`app/routers/maintenance.py` & `app/schemas.py`**: Added auth-protected `POST /api/system/cache/purge` (`CachePurgeRequest` scopes: `"all"`, `"stream"`, `"stream_ram"`, `"stream_disk"`, `"zip"`).
- **`static/js/features/maintenance.js`**: Extended the System → Health view with L2 Disk Cache, Coalescing & Prefetch, ZIP Directory Cache, Stream Cache footprint row, and a double-submit-guarded `Purge Caches` button using `confirmDialog` + `toast` (keeping `static/index.html` untouched at `< 350` lines).

---

## 3. Design & Deviation Ledger

| # | Reference Plan / Requirement | What Was Actually Built | Why Followed or Departed (Codebase Evidence) | Known Risks / Follow-ups |
|---|---|---|---|---|
| 1 | Replace `SpooledTemporaryFile` bulk ZIP download with zero-buffer async streaming ZIP64 (`GET`/`POST /api/media/bulk/download`) | Built pure-stdlib `app/streaming_zip.py` (`ZIP_STORED` + data descriptors + ZIP64) wired into `build_bulk_download_response()` with chunked transfer encoding (`no Content-Length`, `Cache-Control: no-store`, `X-Accel-Buffering: no`) and serialized `asyncio.to_thread` local file reads. | **Followed + framing safety**: Omitting `Content-Length` is deliberate because Telegram multipart sizes come from DB metadata and local/upstream streams can fail mid-transfer; emitting byte `0` immediately eliminates TTFB stalls and avoids HTTP framing corruption. | Mid-stream upstream failures (e.g. deleted Telegram message after HTTP 200 starts) terminate the chunked stream cleanly without corrupting server memory/disk, though browsers will see an interrupted download rather than a JSON error. |
| 2 | Persist ZIP Central Directory cache in SQLite without bloating Track #1 backups | Added bounded `zip_entries_cache` table in `app/db.py` (`ZIP_CACHE_DB_MAX_ROWS=500`, `ZIP_CACHE_DB_MAX_MB=16`) with zlib-compressed JSON payloads, `compute_zip_fingerprint()` staleness checks, and transactional invalidation on `delete_media` & structural `update_media`. | **Followed**: Bounded zlib-compressed entries keep worst-case DB overhead strictly capped (`<= 16 MB` default) so Track #1's SQLite backup/restore stays fast and lean while preserving instant ZIP explorer views across restarts. | Very large archives whose compressed directory alone exceeds `zip_cache_db_max_bytes` remain cached in L1 RAM only (never wiping other L2 rows). |
| 3 | Two-tier RAM L1 + optional persistent Disk L2 hot-block cache (`TELEGRAM_DISK_CACHE_MB=0` default) | Built `TwoTierBlockCache` (aliased as `TelegramBlockCache`) using sharded filesystem `.blk` files under `DATA_DIR/stream_cache/<2hex>/` with block-size suffix in filename, atomic `.tmp_<uuid>.blk` + `os.replace()`, and non-blocking `_disk_lock`. | **Followed + opt-in safety**: Defaulting `TELEGRAM_DISK_CACHE_MB=0` protects small container/root disks and preserves 100% compatibility with existing `test_telegram_pool.py` L1 eviction tests; sharded files avoid SQLite backup bloat and orphan-staging false positives in `IntegrityService`. | L2 disk cache stores raw media blocks on the host filesystem under `DATA_DIR/stream_cache`; operators on shared hosts should rely on standard filesystem permissions (same as `DATA_DIR/thumbs`). |
| 4 | Canonical fixed-grid blocking (`(offset // block_size) * block_size`), single-flight coalescing & bounded sequential prefetch | Integrated `StreamFetchCoordinator` + canonical grid blocking into `TelegramPool.iter_telegram_range()`, keeping `4096`-byte aligned pass-through when caching is disabled. Updated existing `test_prepare_stream_falls_back_across_workers_and_aligns_range` to verify `[0]` when cached and `[4096]` when uncached. | **Followed + existing test update**: Legacy `test_telegram_pool.py:143` asserted `seen_offsets == [4096]` under default cache-enabled settings because pre-Track #6 aligned cached fetches to `4096` bytes; canonical grid alignment intentionally maps offset `5000` to block `0` when caching is enabled while retaining `4096` alignment in pass-through mode. | Prefetch heuristic intentionally skips single-block probes to avoid unnecessary Telegram RPCs on quick metadata peeks; first sequential read triggers read-ahead once it spans >1 block or enters a contiguous next block. |
| 5 | Ops telemetry, `POST /api/system/cache/purge`, storage footprint, and minimal Health tab UI | Extended `/api/system/status` (`zip_cache`, `block_cache.{l1,l2,coalesced_waits,prefetch}`), `IntegrityService.get_storage_footprint()` (`stream_cache_entries/bytes`), added auth-protected `POST /api/system/cache/purge`, and added Health UI readouts + `Purge Caches` action in `static/js/features/maintenance.js`. | **Followed**: Kept `/api/health` untouched (zero leaked paths/errors) and `static/index.html` untouched (`< 350` lines). | Health UI triggers `scope: "all"` purge for simplicity; granular scopes (`stream_ram`, `stream_disk`, `zip`) are available via the REST API. |

---

## 4. Adversarial Review & Hardening Summary

An independent read-only adversarial audit (`gemini-4-hydrogen`, `high` thinking) found **0 Critical**, **3 High**, **3 Medium**, and **2 Low** items. All 8 findings were remediated and covered by regression tests:

1. **Cancelled prefetch / dying task coalescing race (`app/stream_cache.py`)**: Added `_InflightEntry.cancelled`, popped `_inflight` keys *before* `task.cancel()` in `cancel_prefetch_for_namespace()`, `cancel_all_background()`, `stop()`, and `fetch_block()`'s `finally`, skipped cancelling tasks on lookup, and retried in `fetch_block()` if `asyncio.shield()` observes external cancellation on an uncancelled caller.
2. **Silent stream truncation on `TELEGRAM_STREAM_CHUNK_KB` increase (`app/stream_cache.py`, `app/telegram_mtproto.py`)**: Encoded `{block_size:08x}` in L2 `.blk` filenames, cleared L1 and reconciled/purged mismatched L2 blocks when `block_size` changes, and added a short-cached-block re-fetch guard in `iter_telegram_range()` when `total_size` indicates remaining bytes.
3. **`_disk_lock` held across blocking disk I/O (`app/stream_cache.py`)**: Refactored `configure()`, `_reconcile_disk_cache()`, `_put_l2_sync()`, `_read_l2_span_sync()`, `_read_l2_exact_sync()`, `invalidate()`, and `purge()` so `_disk_lock` only guards in-memory metadata and never wraps `read_bytes()`, atomic writes, `os.walk()`, `unlink()`, or `os.utime()`.
4. **Concurrent `fh.read()` vs `fh.close()` on stream cancellation (`app/media_ops.py`)**: Serialized `_aiter_file_range()` seek/read/close under a per-stream `threading.Lock` and offloaded `fh.close()` off the event loop.
5. **Purge button double-submit guard (`static/js/features/maintenance.js`)**: Added `purgingCaches` in-flight guard and disabled `[data-sys-action="purge-caches"]` during confirmation and purge execution.
6. **Windows drive-letter prefix sanitization (`app/streaming_zip.py`)**: Stripped leading `[A-Za-z]:+` prefixes in `_clean_arcname_segments()` and `deduplicate_arcname()`.
7. **Symlink skipping in L2 reconciliation & purge (`app/stream_cache.py`)**: Added `followlinks=False` plus symlink file/directory checks in `_reconcile_disk_cache()` and `purge()`.
8. **`configure(max_prefetch_concurrency=...)` clearing `_shutting_down` (`app/stream_cache.py`)**: Resized `_prefetch_sem` directly on the active loop without resetting `_sem_loop` / `_shutting_down`.
