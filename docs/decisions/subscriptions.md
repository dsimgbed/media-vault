# ADR: Channel / Playlist / RSS Watch Subscriptions

- **Status**: Accepted
- **Date**: 2026-11-22
- **Scope**: Media Vault (`v0.9.1+` — Auto-Archiver Subscriptions)

## 1. Status & Context

Media Vault is a self-hosted media archive (yt-dlp based ingest). Watch Subscriptions
allow users to point the vault at a YouTube channel, playlist, or RSS/Atom
feed so the vault polls for new media and archives it automatically.

Design constraints that shaped the ADR:

- **Zero new runtime pip dependencies.** Discovery reuses the already-required `httpx`
  and `yt-dlp` stack plus Python stdlib (`xml.parsers.expat`, `xml.etree.ElementTree`,
  `sqlite3`, `asyncio`).
- **Stdlib XML safety.** Feeds are parsed with expat handlers + `ET.TreeBuilder`, with DTD /
  ENTITY / NOTATION declarations forbidden and an element-count cap
  (`MAX_XML_ELEMENTS = 50_000`).
- **SQLite WAL migrations.** Schema evolves additively via `CREATE TABLE IF NOT EXISTS`
  plus `PRAGMA table_info` diffing with guarded `ALTER TABLE ... ADD COLUMN` blocks.
- **Small frontend.** The system sheet lives in a single `static/index.html` (< 350 lines)
  and feature JS under `static/js/features/`.
- **Zero native browser dialogs.** Confirmation flows use in-app sheets, never
  `window.confirm`/custom modal prompts.

## 2. Architecture & Data Flow

### 2.1 SQLite schema

- `subscriptions` table: id, name, source_url, normalized_source_url, source_kind
  (`auto` | `rss` | `ytdlp`), folder, tags, format_spec, forward_to_telegram,
  interval_minutes, max_items_per_check, enabled, seed_only, initial_backfill_limit,
  archive_existing, seeded_at, title_include_regex, title_exclude_regex, last_checked_at,
  next_check_at, last_status, last_error, consecutive_failures, items_ingested_count,
  created_at, updated_at.
- `subscription_entries` ledger: PK `(subscription_id, entry_key)`, with url,
  normalized_url, status (`seen` | `queued` | `ingested` | `skipped` | `failed`), media_id,
  job_id, error, published_at, first_seen_at, last_attempt_at; FK to `subscriptions(id)`
  with `ON DELETE CASCADE`.
- Indexes: `idx_subscriptions_due (enabled, next_check_at)`, `idx_subscriptions_created`,
  `idx_sub_entries_sub_seen (subscription_id, first_seen_at DESC)`, `idx_sub_entries_job`,
  `idx_sub_entries_norm_url (subscription_id, normalized_url)`.
- Migrations are idempotent (`IF NOT EXISTS` + column-presence guards).

### 2.2 Hybrid discovery pipeline

`SubscriptionService._discover_entries(sub, full_catalog=False)`:

1. `source_kind == "rss"` → fetch via `fetch_feed_bytes` + `parse_feed_xml` (backend `rss`).
2. `source_kind == "ytdlp"` → `expand_ytdlp_playlist` with `max_items` (backend `ytdlp`).
3. `source_kind == "auto"`:
   - YouTube watch URL (when `full_catalog=False`) → `resolve_youtube_rss_url` maps channel
     (`/channel/UC…`, `/user/…`, `/playlist?list=…`) to a YouTube Atom feed URL; fast-path
     RSS fetch with fallback to yt-dlp on `SubscriptionFetchError`. When `full_catalog=True`,
     YouTube Atom RSS (capped at 15 items) is bypassed so `expand_ytdlp_playlist` scans the
     full channel/playlist (including nested channel tabs) up to
     `SUBSCRIPTIONS_FULL_CATALOG_MAX_ITEMS`.
   - Feed-looking URL (`feeds/`, `.rss`, `.xml`, `.atom`) → RSS fast path with yt-dlp fallback.
   - Otherwise → yt-dlp playlist expansion.

Entry keys: YouTube video ids become `https://www.youtube.com/watch?v=<id>` (normalized);
other entries key off `guid:` or normalized URL.

### 2.3 First-run flood protection & full-catalog archiving

- Defaults: `seed_only = true`, `initial_backfill_limit = 0`, `archive_existing = false`,
  `max_items_per_check = 10`.
- First check (`seeded_at` unset):
  - `archive_existing = true` → forces `full_catalog=True` and `queue_existing=True` so the
    entire discovered catalog (up to `SUBSCRIPTIONS_FULL_CATALOG_MAX_ITEMS`) is queued;
    `archive_existing` is cleared once the initial run finishes so future polls stay incremental.
  - `seed_only` + no backfill limit → queue budget 0, all entries marked `seen`
    (`action = "seeded"`, no downloads).
  - `initial_backfill_limit > 0` → queue budget `min(backfill_limit, max_items)`, remaining
    entries marked seen.
- Later checks: queue up to `max_items_per_check`, defer the rest.
- Retroactive full archive: `POST /api/subscriptions/{id}/check?full_catalog=true&queue_existing=true`
  (also exposed via the UI **"Archive Existing"** button and Telegram `/subs archive <id>`)
  discovers up to `SUBSCRIPTIONS_FULL_CATALOG_MAX_ITEMS` items, skips `ingested`/`queued`/`skipped`/`failed`
  entries and vault duplicates, and queues both previously `seen` rows and newly discovered backlog.
- `POST /api/subscriptions/{id}/backfill` supports `all: true` (up to 5,000 seen entries).
- `POST /api/subscriptions/{id}/check?dry_run=true` returns `would_queue` / `would_seed`
  without touching state.

### 2.4 Dual deduplication

Two independent layers prevent re-downloading:

1. `subscription_entries` ledger (any existing row for `entry_key` or `normalized_url` short-circuits repeats).
2. Vault check via `Database.find_media_by_source_urls` (normalized + raw `source_url`
   lookups against `media_items`) → `action = "duplicate"`.

Because the ledger persists seen/queued/ingested entries independently of `media_items`,
deleted vault media is not re-fetched.

### 2.5 Job lifecycle

- Queuing: `ingest_jobs` row with `source_origin="subscription"`, `source_kind="url"`,
  params `{subscription_id, entry_key, url, folder, tags, title, forward_to_telegram,
  format_spec, source_origin}`; ledger entry set to `queued` with `job_id`.
- Ingest execution: `SubscriptionService._run_ingest_job` → `IngestService.process_url_job`
  with `source_origin="subscription"`; outcome recorded back via `_safe_update_entry_status`
  (`ingested` + `media_id`, `skipped` when no media produced, `failed` on error);
  `items_ingested_count` incremented on fresh ingest.
- Startup reconciliation: `Database.reconcile_subscription_entries_with_jobs()` joins
  `subscription_entries` (status `queued`) to `ingest_jobs` and resolves stale queued
  entries against job status (`done` → `ingested`, `cancelled` → `skipped`, `failed` →
  `failed`) — invoked from both startup and `SubscriptionService.start_scheduler()`.

### 2.6 Scheduler loop & concurrency

- Scheduler task ticks every `subscriptions_poll_tick_sec` (default 30s), polls due subs
  (`enabled = 1 AND (next_check_at IS NULL OR <= now)`, ordered by next check, cap 50/tick).
- Per-subscription single-flight lock (`asyncio.Lock`); concurrent polls gated by a bounded
  semaphore (`subscriptions_max_concurrent_polls`, default 2, min 1) and an active-poll counter.
- Single-flight conflict → `SubscriptionBusyError` → HTTP 409 from the check endpoint.
- Failure handling: exponential backoff
  (`interval * 2 ** min(consecutive_failures, 5)`, capped at `max(interval*16, 1440)` minutes)
  with next-check scheduling; after `subscriptions_failure_pause_threshold` consecutive
  failures the subscription auto-pauses (`enabled=False`, `last_status="paused_error"`).
- Re-enabling clears failure state and re-arms (`next_check_at = now`).

## 3. Security & Abuse Controls

### 3.1 SSRF protection

- `validate_subscription_url`: http/https scheme only, no embedded credentials, hostname
  required, control-character and length (`MAX_URL_LENGTH = 2048`) caps.
- `assert_safe_public_url`: rejects local hostnames (`localhost`, `.localhost`, `.local`,
  `.internal`, IPv6 localhost variants); literal-IP fast path through `_is_ip_disallowed`
  (private/loopback/link-local/multicast/reserved/unspecified, incl. IPv4-mapped IPv6);
  otherwise resolves via `socket.getaddrinfo` in executor and validates **all** returned
  addresses before returning.
- `fetch_feed_bytes`: manual redirect loop (`follow_redirects=False`, ≤5 hops) re-running
  SSRF validation per hop; 5 MiB feed byte limit (declared `Content-Length` + streamed
  body); non-2xx → error; feed UA `MediaVault-Subscriptions/1.0` + feed Accept header.

### 3.2 Feed entry URL & ingest-path safety

- Before an entry is queued, `_is_entry_url_safe` re-runs URL validation plus local-IP /
  hostname checks; unsafe targets are recorded as `skipped` with error
  `Entry URL targets a local, private, or invalid host`.
- Before download execution, `SubscriptionService._run_ingest_job` calls
  `assert_safe_public_url` (full DNS resolution check unless
  `subscriptions_allow_private_hosts=true`), marking both the ledger entry and `ingest_jobs`
  row `failed` on violation.
- In `app/ingest.py`, subscription-originated jobs (`source_origin="subscription"`,
  including Job Center retries) enable `block_private_hosts=True` unless
  `subscriptions_allow_private_hosts=true`: `is_direct_download` and `_download_ddl` bypass
  `aria2c` and follow redirects manually (`follow_redirects=False`, ≤5 hops), re-running
  `assert_safe_public_url` on every redirect hop.

### 3.3 Safe XML parsing

- expat with `namespace_separator=" "`, handlers for `StartDoctypeDecl`, `EntityDecl`,
  `UnparsedEntityDecl`, `NotationDecl` (all raise) and `ExternalEntityRefHandler` disabled.
- `ET.TreeBuilder` closes the tree; parser errors mapped to `SubscriptionFetchError`.
- Element count capped at `MAX_XML_ELEMENTS = 50_000`; feed byte cap `subscriptions_max_feed_bytes`
  (default 5 MiB).

### 3.4 Input & error hardening

- Regex filters (`title_include_regex` / `title_exclude_regex`): `MAX_REGEX_LENGTH = 256`,
  stdlib `re._parser` AST screening (`_is_unsafe_regex`) rejecting unbounded nested
  quantifiers, repeated alternations, and repeated backreferences, `re.compile` validation,
  and title truncation (`MAX_REGEX_TITLE_LENGTH = 512`) during matching.
- Service-layer bounds match Pydantic schemas (`interval_minutes` 5..10080,
  `max_items_per_check` 1..100, `initial_backfill_limit` 0..50).
- `_redact_credentials` scrubs `scheme://user:pass@host` credentials from feed fetch,
  yt-dlp, discovery, and ingest error strings before persistence or API responses.
- Discovery write-back (`_run_check`, `_record_discovery_failure`) re-reads the latest
  subscription row from SQLite before persisting schedule/status updates so concurrent
  `PATCH` edits (pause, interval change, delete) are not clobbered.

### 3.5 API & frontend

- All `/api/subscriptions*` endpoints require bearer auth (`require_auth` dependency);
  check errors mapped to 404/400/409/502.
- Frontend renders via `escapeHtml` and `renderSafeUrl`, which only emits clickable
  `http(s)://` links (`rel="noopener noreferrer"`, `target="_blank"`); non-http(s) values are
  rendered as inert text.

## 4. UI, Observability & Telegram Integration

- The Subscriptions UI lives in the `systemSheet` as a `"subscriptions"` tab, wired through
  `static/js/features/maintenance.js` (tab registry, input/change/submit/click handlers,
  retry) and rendered by `static/js/features/subscriptions.js` (list, form, card, preview,
  entries drawer, check flow).
- Live updates: DB publishes `subscription_update` events (created/updated/deleted); the
  jobs SSE stream (`/api/events`) re-emits them to `subscriptions:update`; maintenance listens
  and reloads the subscriptions tab (or health tab) when the sheet is open.
- Telemetry: `GET /api/system/status` includes `ctx.subscriptions.get_health_summary()`;
  `GET /api/health` includes `subscriptions_enabled`.
- Telegram: `/subs` (list, incl. per-subscription status / interval / archived count) and
  `/subs check <id>` trigger `SubscriptionService.check_subscription(trigger="telegram")`
  and report discovered/queued/seeded counts.

## 5. Configuration Reference

| Variable | Default | Meaning |
| --- | --- | --- |
| `SUBSCRIPTIONS_ENABLED` | `true` | Master switch for the background poller. |
| `SUBSCRIPTIONS_POLL_TICK_SEC` | `30` | Scheduler wake-up interval. |
| `SUBSCRIPTIONS_MAX_CONCURRENT_POLLS` | `2` | Bounded concurrent feed polls. |
| `SUBSCRIPTIONS_HTTP_TIMEOUT_SEC` | `20.0` | Feed / playlist fetch timeout. |
| `SUBSCRIPTIONS_MAX_FEED_BYTES` | `5242880` (5 MiB) | Max accepted feed payload. |
| `SUBSCRIPTIONS_FAILURE_PAUSE_THRESHOLD` | `5` | Consecutive failures before auto-pause. |
| `SUBSCRIPTIONS_ALLOW_PRIVATE_HOSTS` | `false` | Opt-out of SSRF host restrictions. |

## 6. Trade-offs & Future Work

- **DNS-rebinding TOCTOU**: validation resolves via `getaddrinfo` and the HTTP client
  connects afterwards; resolution results are checked, but resolution/connect are not atomic.
  Future: pinned-IP transport or custom resolver.
- **Failed entries are not auto-retried on every poll**: once an entry fails download, its
  ledger row stays `failed` so permanently broken, deleted, or members-only videos do not
  trigger infinite retry storms on every scheduled check. Failed jobs remain manually
  retryable from the Job Center (with subscription SSRF enforcement preserved).
- **No conditional caching**: no ETag / `Last-Modified` handling yet; every poll refetches.
- **YouTube handle/channel-ID caching**: channel → feed-ID mapping is recomputed per poll;
  persistent channel-ID cache is future work.
- **ReDoS posture**: regex filters are length-capped (`MAX_REGEX_LENGTH = 256`), AST-screened
  against nested quantifiers / repeated alternations / repeated backreferences, and matched
  against titles sliced to 512 chars; future hardening could adopt a linear-time engine
  (`re2`) if optional dependencies are ever allowed.
- **Backoff & pause semantics**: backoff capped at `max(interval*16, 1440)` min; pause
  threshold defaults to 5 consecutive failures (configurable).
- **Fetch limits**: discovery fetch limit = `min(100, max(max_items, backfill_limit, 25))`.
