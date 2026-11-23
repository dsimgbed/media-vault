# Design & Deviation Ledger — Personal Cloud Torrent/Magnet (`aria2c`) & `gallery-dl` Leecher

**Status:** Implemented & Verified  
**Scope:** Media Vault (`v0.11.0+`) — Personal Cloud Torrent/Magnet Leecher, Optional `gallery-dl` Gallery Archiver, Telegram Bot Integration, Web Torrent File Picker & Gallery Controls.

---

## 1. Architecture & Data Flow

### 1.1 Torrent / Magnet Leecher (`app/torrent_ingest.py`)
1. **Inspection Phase (`POST /api/ingest/torrent/inspect`)**
   - **`.torrent` files**: Decoded in-process via a pure-Python bounded bencode decoder (`bdecode` / `parse_torrent_bytes`) offloaded to `asyncio.to_thread`. Computes canonical v1 `infohash` (`SHA-1` over exact top-level `info` dict raw byte span) and lists files with 1-based `aria2c --select-file` indices, natural sorting, and smart `selected_default` flags (auto-deselecting `.nfo`, `.txt`, `.url`, `.sfv`, sample clips, and BEP-47 `.pad` files).
   - **`magnet:?` links**: Validated strictly (rejects non-magnet schemes, whitespace/control chars, missing `xt=urn:btih:`/`btmh:`, oversized URIs). Runs bounded `aria2c --bt-metadata-only=true --bt-save-metadata=true` with a temporary registered staging dir and registered subprocess handle (`inspect_<id>`), bounded by `TORRENT_METADATA_TIMEOUT_SEC` (default `45s`).
   - **Inspect Cache**: LRU-bounded TTL cache (`max=32`, `ttl=900s`) returning an `inspect_id` plus staging disk availability (`staging_free_bytes`, `staging_max_total_bytes`, `staging_min_free_buffer_bytes`, `exceeds_quota`, `insufficient_disk`).
2. **Leech & Archive Phase (`POST /api/ingest/torrent` or `magnet:?` auto-route in `POST /api/ingest/url`)**
   - Enforces a dedicated concurrency semaphore (`TORRENT_MAX_CONCURRENT`, default `2`) separate from yt-dlp/DDL workers.
   - Executes `aria2c` with strict leech-only invariants:
     - `--seed-time=0`, `--seed-ratio=0.0`, `--bt-seed-unverified=false`
     - `--file-allocation=none`, `--follow-metalink=false`, `--bt-remove-unselected-file=true`
     - `--allow-overwrite=true`, `--auto-file-renaming=false`
     - `--select-file=<sorted unique 1-based CSV>` whenever `selected_indices` is provided
     - `--` argument terminator immediately before the untrusted source path/magnet
   - Runs a stdout-independent background disk/quota watchdog every 3s (in addition to throttled `on_line` checks) that monitors `shutil.disk_usage` and staged directory size via `asyncio.to_thread` and terminates `aria2c` immediately on quota or free-space buffer breach.
   - Streams live progress (`downloading` → `allocating` → `done`) into `jobs` / SSE (`on_progress`).
   - Archives each collected safe non-symlink file through `IngestService.finalize_staged_file(...)` with `source_kind="torrent"`, unlinking each staged file immediately after Telegram/local archival, then wipes the temporary staging directory in `finally`.

### 1.2 Optional `gallery-dl` Archiver (`app/gallery_ingest.py`)
1. **Zero Required Dependencies**: `gallery-dl` is detected dynamically via `shutil.which("gallery-dl")` or `sys.executable -m gallery_dl` (cached with 15s TTL). Status is exposed via `/api/health`, `/api/system/status`, and `/api/leech/status`. When absent or disabled (`GALLERY_DL_ENABLED=false`), endpoints return clean HTTP `503` without breaking app startup or existing features. Optional `POST /api/gallery-dl/update` supports installing/upgrading `gallery-dl` via `pip` with a serialized lock.
2. **Bounded Execution & Archive Modes (`POST /api/ingest/gallery`)**:
   - Reuses `cfg.ytdlp_cookies_file` and `IngestService.get_effective_proxy()` (validated against scheme/control-char injection), enforcing `--range 1-<bounded_max>`, `--no-mtime`, and `--` before the URL.
   - Enforces staging disk buffer and total size quota both pre-flight and continuously via a 3s background watchdog.
   - Rejects symlinks and path-traversal escapes inside staging.
   - Supports three `archive_mode` values:
     - `items`: archives each naturally-sorted downloaded media file individually (`source_kind="gallery"`).
     - `cbz` / `zip`: packs naturally-sorted files into an uncompressed `ZIP_STORED` archive (`001.jpg`, `002.png`, ...) with automatic first-image WebP cover thumbnail extraction (`_make_cbz_thumb` in `app/thumbnails.py`). `.cbz` is recognized by `isZipItem()` in `static/js/utils.js` so archives open directly in the built-in ZIP Explorer.

### 1.3 Telegram Bot & Frontend Integration
- **Telegram Bot (`app/telegram_mtproto.py`)**:
  - `magnet:?` links sent via DM or archive group auto-dispatch to `TorrentIngestService` with live progress edits.
  - `.torrent` document uploads in DM are streamed via bounded download (`_download_bounded_torrent_bytes`, aborting above `MAX_TORRENT_FILE_BYTES` with a 30s timeout), parsed off the event loop, and queued for leeching.
  - `/help` documents both magnet links and `.torrent` file uploads.
- **Web Frontend (`static/js/features/torrent-picker.js`, `uploader.js`, `system.js`, `state.js`, `utils.js`, `static/index.html`, `static/css/components.css`)**:
  - Dropping/selecting a `.torrent` file or pasting a single `magnet:?` URL opens the **Torrent File Picker Modal** with file search, quick filters (All / Media / Clear), live selected-size vs disk-free warning, folder/tags/subfolder/force controls, and `AbortController` cancellation for magnet peer metadata fetches.
  - Gallery (`gallery-dl`) toggle + archive mode selector (`items` / `cbz` / `zip`) wired into the URL form with graceful disabled hint when `gallery-dl` is not installed.

---

## 2. Design Decisions & Security Invariants

| Area | Invariant / Decision | Rationale |
|---|---|---|
| **Bencode Parser** | Pure Python, no `libtorrent` / C-ext dependency; enforces `MAX_TORRENT_FILE_BYTES=10 MiB`, `MAX_BENCODE_DEPTH=32`, `MAX_BENCODE_ITEMS=100,000`, integer token cap (`≤21` chars), length-prefix cap (`≤10` chars), strict canonical integer rules (no `-0` / leading zeros), duplicate dict key rejection, and top-level `depth == 0` `info` span tracking. | Prevents C-extension build failures, recursion bombs, quadratic `int()` DoS, key-smuggling/infohash spoofing, and trailing-garbage attacks. |
| **Canonical Job Statuses** | Strict reuse of `'queued' \| 'downloading' \| 'allocating' \| 'done' \| 'failed' \| 'cancelled'` (never introducing `'uploading'`). | Keeps SQLite schema, SSE job cards, cancel/retry endpoints, and `static/js/features/jobs.js` 100% consistent. |
| **Leech-Only BitTorrent Policy** | Hardcoded `--seed-time=0`, `--seed-ratio=0.0`, `--bt-seed-unverified=false`, `--bt-remove-unselected-file=true`, `--follow-metalink=false`, `--`. | Prevents accidental seeding on residential/VPS IPs, disables metalink SSRF/redirection, and prevents option injection via malicious magnet/filenames. |
| **Staging Disk Protection** | Pre-flight `assert_staging_disk_space` + stdout-independent 3s background watchdog + per-file immediate unlink after archival + `finally` `rmtree`. | Prevents VPS disk exhaustion even if `aria2c` or `gallery-dl` stalls stdout while writing to disk. |
| **Retry Persistence** | `jobs.params` stores JSON-serializable parameters (`magnet`, `selected_indices`, `folder`, `tags`, `use_torrent_subfolder`, `forward_to_telegram`, `force`) — never raw `.torrent` binary blobs. | Keeps SQLite lean and JSON-clean; magnet jobs (including inspected-magnet `selected_indices`) are retryable after restart. |
| **Frontend `index.html` Guardrail** | Kept `static/index.html` at 349 lines (strictly `< 350` lines enforced by `tests/test_ui_and_pwa.py`) by creating the `<dialog id="torrentModal">` dynamically in `torrent-picker.js`. | Preserves existing UI/PWA architectural tests without bloating static HTML. |

---

## 3. Deviations & Trade-Offs Ledger

| # | Item | Original / Ideal Plan | Actual Implementation & Rationale | Impact / Mitigation |
|---|---|---|---|---|
| **D1** | **Retry of `.torrent`-only jobs without magnet** | Retry any failed torrent job via `POST /api/jobs/{id}/retry`. | Jobs originating from a `.torrent` upload (with no `magnet` URI) do not persist the raw `.torrent` bytes in SQLite `jobs.params`; retry succeeds for magnet-backed jobs (including inspected magnets, preserving `selected_indices`), while raw `.torrent`-only retries report that the original `.torrent` must be re-uploaded. | Avoids storing multi-MB binary blobs in SQLite `jobs` rows; users can re-drop the `.torrent` file (instant local inspect). |
| **D2** | **Inspect Cache Volatility** | Multi-worker shared inspect cache. | In-memory `OrderedDict` LRU+TTL (`32` entries, `900s` TTL) on `TorrentIngestService`. | Media Vault is a single-process FastAPI + SQLite app; expired `inspect_id` returns a clean HTTP `404` prompting re-inspection. |
| **D3** | **`gallery-dl` Optional Installation** | Pre-installed `gallery-dl` binary in container/venv. | `gallery-dl` is kept strictly optional (zero new required pip deps); all tests run and pass without `gallery-dl` installed, with mocked subprocess coverage and live `503` verification. | Keeps base install lightweight and zero-regression; users can install on-demand via `pip install gallery-dl` or `POST /api/gallery-dl/update`. |
| **D4** | **Selective File Verification on Magnet Retry** | Re-parse `.torrent` file to verify relative paths on magnet-only retry. | When `torrent_bytes is None` (direct magnet or retried magnet job), `selected_indices` is still passed to `aria2c` via `--select-file=...`, while post-download collection archives all safe staged payload files produced by `aria2c` (excluding `.aria2` and root `_source.torrent`). | `--bt-remove-unselected-file=true` + `--select-file=` ensures `aria2c` only delivers the chosen indices. |
| **D5** | **Delegate Model Fallback (Operational)** | Initial Wave 1 delegation targeted `opencode/big-pickle` / free tier. | Hit provider free-usage limit on first attempt; pivoted cleanly to `opencode/beam-free` and `opencode/flint-40b-free` with Lead Architect review and independent adversarial review (`beam-free max`). | Zero impact on code quality; every workstream underwent line-by-line Lead Architect review + WS-I audit. |

---

## 4. Adversarial Security & Concurrency Audit (WS-I) — Findings & Dispositions

An independent read-only adversarial audit (WS-I) reviewed the full diff across security, DoS, concurrency, and resource-leak vectors. All Concrete Critical, High, and Medium findings were resolved and covered by regression tests:

| ID | Severity | Finding | Resolution |
|---|---|---|---|
| **C1** | Critical | `app/ingest.py` `_download_ddl` invoked `aria2c` without `--` terminator or `--follow-metalink=false`. | Added `--follow-metalink=false`, `--seed-time=0`, `--seed-ratio=0.0`, and `--` immediately before `url` in `_download_ddl`. Regression test added. |
| **H1** | High | Synchronous bencode parsing, `shutil.disk_usage`, and staging `rglob` ran on the `asyncio` event loop. | Offloaded `parse_torrent_bytes`, `inspect_torrent_bytes`, `assert_staging_disk_space`, `shutil.disk_usage`, `_staging_dir_size_bytes`, `_ensure_staging_free`, and staging collection to `asyncio.to_thread` in `app/routers/leech.py`, `app/torrent_ingest.py`, `app/gallery_ingest.py`, and `app/telegram_mtproto.py`. |
| **H2** | High | Telegram `.torrent` handler relied on client-reported `file.size` before `download_media(file=bytes)`. | Added `_download_bounded_torrent_bytes` in `app/telegram_mtproto.py` streaming via `client.iter_download` with hard `MAX_TORRENT_FILE_BYTES` abort and 30s timeout (with mock-safe fallback). |
| **H3** | High | Bencode decoder allowed duplicate dictionary keys (risk of `info` key smuggling / infohash mismatch). | Rejected duplicate keys in `_bdecode_value` with `BencodeError("Duplicate dict key in bencode")`. Regression test added. |
| **H4** | High | Unbounded bencode integer tokens / byte-string length prefixes could trigger `int()` CPU DoS. | Capped bencode integer token length (`≤21` chars) and length prefix (`≤10` chars) prior to `int()`. Regression test added. |
| **H5** | High | `inspect_magnet` subprocess was not registered with `IngestService`, risking orphaned `aria2c` processes on timeout or shutdown. | Registered `inspect_magnet` `aria2c` subprocess under `inspect_<uuid>` via `register_proc` / `unregister_proc` and explicitly terminated on `TimeoutError`. |
| **M1** | Medium | Mid-download disk/quota checks only ran on `aria2c`/`gallery-dl` stdout lines; a silent/stalled writer could bypass quota. | Added stdout-independent 3s `_quota_watchdog` `asyncio.Task` to both `TorrentIngestService.process_torrent_job` and `GalleryIngestService.ingest_gallery_job`, terminating the subprocess on breach and surfacing the `QuotaError`. |
| **M2** | Medium | `_staging_dir_size_bytes` and collection skipped all `*.torrent` files, allowing nested `.torrent` payloads to evade quota accounting. | Scoped exclusion strictly to root `job_dir/_source.torrent` and `.aria2` control files; nested `.torrent` files are counted toward quota. Regression test added. |
| **M3** | Medium | `process_torrent_job` dropped `selected_indices` when `torrent_bytes is None` (e.g., on inspected-magnet job retry). | Preserved validated positive `selected_indices` into `build_aria2c_leech_cmd` whenever provided, even for magnet-only retries. Regression test added. |
| **M4** | Medium | CBZ cover extraction in `app/thumbnails.py` lacked entry-name sanitization, explicit pixel cap, and natural-key digit cap. | Added `_is_safe_cbz_entry_name`, capped `_natural_key` digit chunks to 18 chars, enforced a `64_000_000` pixel cap, and used `with zf.open(info)` context manager. Regression test added. |
| **M5** | Medium | Telegram archive reply formatting could break Markdown code spans when filenames contained backticks. | Sanitized backticks in `_format_telegram_archive_reply`. |
