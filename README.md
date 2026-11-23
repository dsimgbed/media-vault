# Media Vault — Unified WebAPI, Gallery & 2 GB Telegram MTProto Ingest

A self-hosted server that gives you **both**:
1. **A Web API + Visual Media Gallery** — thumbnails, seekable HTML5 video streaming via HTTP `Range` (including streaming directly from Telegram via MTProto), drag-and-drop uploads up to **2 GB**, live download/upload progress bars, `yt-dlp` format selector & live format picker, in-app `yt-dlp` updater, and a YouTube `cookies.txt` manager.
2. **Telegram MTProto Bot Pool** — connects directly via your `api_id` and `api_hash` from `my.telegram.org` using `Telethon` (supporting **up to 2 GB** file transfers natively in Python, no C++ Local Bot API server needed), supporting 1–4+ worker bots sharing one gallery. Send links or media either in bot DM **or directly inside your shared Forum Supergroup** (`#FolderName`, `folder:FolderName`, `format:720p`, or inside any existing Topic).

## Features

- **Storage Modes (`STORAGE_MODE`)**:
  - `telegram` *(default)*: Stage locally to generate thumbnails, upload into the matching **Forum Topic** in your private Telegram supergroup, and immediately delete the staged local copy. Media streams on demand from Telegram with 4 KB-aligned `Range` seeking.
  - `drives`: Save to mounted local/cloud drives (`rclone mount`, etc.) using `first_fit` or `most_free` allocation.
  - `both`: Mirror to Telegram Topics **and** keep a copy on your drives.
- **URL Ingestion (`yt-dlp` + `aria2c` / `httpx`)**:
  - Automatic detection of direct download links (DDLs) vs `yt-dlp` video pages.
  - Format presets (`Best MP4`, `1080p MP4`, `720p MP4`, `480p MP4`, `Audio MP3`) plus a **Formats** button to inspect and pick any exact stream.
  - In-UI **Cookies** modal (Netscape `cookies.txt`) and **`yt-dlp` Update** button.
  - Real-time download + Telegram upload progress bars in the Web UI and live status edits in Telegram.
- **Telegram Group + Topic Routing**:
  - Each folder maps 1:1 to a **Forum Topic** in `TELEGRAM_ARCHIVE_CHAT_ID` (auto-created via Bot API).
  - Posting a video/DDL link directly in the archive supergroup (with `#Folder`, `folder:Name`, `format:720p`, or inside an existing Topic) deduplicates across workers, runs the full pipeline, and indexes into the Web Gallery.
- **Vault Resilience & Maintenance**:
  - **Online SQLite Backups**: Generates verified, gzip-compressed `.db.gz` snapshots via `POST /api/system/backups` (or on a schedule via `BACKUP_INTERVAL_HOURS`) with local rotation (`BACKUP_RETENTION_COUNT`) and optional upload to the `_VaultBackups` Forum Topic (`BACKUP_TELEGRAM_ENABLED=true`). Download or delete snapshots via `/api/system/backups/{backup_id}/download` and `DELETE /api/system/backups/{backup_id}`.
  - **Self-Healing Integrity Audit & Repair**: `POST /api/system/integrity/audit` checks media integrity (missing thumbnails, animated previews, metadata, orphaned staging/thumbnail files, and optional Telegram message verification). `POST /api/system/integrity/repair` backfills missing assets from local drives or bounded Telegram fetches and optionally purges stale orphans, with run history tracked under `/api/system/integrity/runs`.
  - **System Health & Maintenance Sheet**: Displays live per-worker Telegram pool telemetry (online/cooldown state, requests, FloodWait counts) and MTProto streaming block-cache metrics alongside backup and integrity controls (backed by `/api/system/status` and `/api/health`).
- **Resumable Chunked Uploads, Pre-Flight Dedup & Smart Job Center**:
  - **Resumable Chunked Upload Queue (`/api/uploads/sessions`)**: Splits files into 8 MB chunks (`UPLOAD_CHUNK_SIZE_MB`) assembled directly via `os.pwrite()`, with Pause/Resume/Cancel, speed & ETA, offline/online auto-resume, `localStorage` session recovery across refreshes, and periodic/startup GC.
  - **Pre-Flight Deduplication on HTTP & HTTPS**: Zero-dependency pure-JS streaming SHA-256 (`static/js/crypto/sha256.js`) + two-tier size-candidate verification catches duplicates before uploading—even on plain HTTP LAN or Tailscale origins where `crypto.subtle` is disabled.
  - **Playlist Preview / Cherry-Picker (`POST /api/ytdlp/playlist`)**: Inspects playlists before queuing, highlights items already in your vault, and lets you filter, select, and assign folder/tags/format.
  - **Retryable Failed Uploads & Smart Job Center**: Retains failed staged uploads within `FAILED_UPLOAD_RETENTION_SEC` / `FAILED_UPLOAD_MAX_RETAINED_MB` for one-click retry without re-uploading, plus status counts, filter tabs, paginated history, `Clear Completed`, `Retry All Failed`, and clickable links straight to finished media.
- **Zero-Buffer Streaming & Hot-Cache Overhaul**:
  - **Zero-Buffer Bulk ZIP64 Streaming (`GET/POST /api/media/bulk/download`)**: Pure-stdlib APPNOTE 6.3.x `ZIP_STORED` async streaming (`app/streaming_zip.py`) with automatic ZIP64 support and immediate chunked TTFB (no `SpooledTemporaryFile` buffering or client-disconnect leaks).
  - **Persisted Two-Tier ZIP Directory Cache**: Bounded L1 RAM + zlib-compressed SQLite (`zip_entries_cache`, `ZIP_CACHE_DB_MAX_ROWS=500`, `ZIP_CACHE_DB_MAX_MB=16`) with fingerprint verification and transactional invalidation so ZIP explorer survives restarts without bloating backups.
  - **Two-Tier Hot-Block Cache, Coalescing & Sequential Prefetch**: Canonical fixed-grid blocking with RAM L1 + optional opt-in L2 sharded `.blk` disk cache (`TELEGRAM_DISK_CACHE_MB`), single-flight deduplication, bounded read-ahead prefetch (`TELEGRAM_PREFETCH_BLOCKS`), multipart next-part warming, stream-aware worker routing, and `POST /api/system/cache/purge` + Health UI controls.
- **One-Tap Capture Ecosystem**: Quick-capture popup & bookmarklet (`GET /capture`), a zero-build Manifest V3 companion browser extension (`GET /api/system/extension.zip`), and Android/iOS share recipes via `POST /api/capture/quick`, with controlled CORS (`CAPTURE_CORS_ORIGINS`, `CAPTURE_CORS_ALLOW_EXTENSIONS`).
- **Channel / Playlist / RSS Watch Subscriptions (Auto-Archiver)**: Automated polling scheduler (`/api/subscriptions`) supporting YouTube channels/playlists (Atom RSS fast-path with automatic `yt-dlp` fallback), standard RSS/Atom/RDF feeds via hardened stdlib `expat` XML parsing, first-run flood protection (`seed_only`, `initial_backfill_limit`, `dry_run`), one-click `Download Seen` backfill & per-entry `Download`/`Retry`, dual deduplication ledger (`subscription_entries` + `media_items`), ReDoS-screened title regex filters, exponential backoff + auto-pause, System Sheet Subscriptions UI, and Telegram `/subs` commands.
- **Built-in TLS ClientHello Fragmenting Proxy (`YTDLP_TLS_FRAGMENT_PROXY`)**: Lightweight loopback HTTP/CONNECT proxy (`app/tls_proxy.py`) that fragments initial TLS ClientHello packets across TCP segments when `YTDLP_PROXY` is unset, plus `yt-dlp[default,curl-cffi]` impersonation support.
- **Read-Only WebDAV & External Players (`/webdav/`, `/webdav.m3u`)**: Mount the whole vault as a read-only WebDAV drive or open an auto-generated M3U playlist in VLC, Infuse, Kodi, MPV, and other players, with optional Basic Auth or anonymous LAN access and fail-closed defaults.
- **Cloud Torrent / Magnet (`aria2c`) & Optional `gallery-dl` Leecher**: Pure-Python bounded bencode `.torrent` & `magnet:?` inspector with selective file cherry-picking, strict leech-only `aria2c` invariants (`--seed-time=0`, `--seed-ratio=0.0`), stdout-independent disk-quota watchdogs, live SSE progress, Telegram DM/group `.torrent` + `magnet:?` auto-leeching, and optional `gallery-dl` archiving (`items` or naturally-sorted `cbz`/`zip` with WebP cover thumbnails & built-in ZIP Explorer `.cbz` support).

## Quick Start

### 1. Install system tools & Python packages
```bash
sudo apt update && sudo apt install -y python3 python3-venv ffmpeg aria2 curl unzip
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

*(Optional, recommended for YouTube EJS challenge solving)* Install [Deno](https://deno.com/) (`>= 2.3`) either on `PATH` or into `.venv/bin/deno`:
```bash
curl -fsSL https://deno.land/install.sh | sh
```

### 2. Configure `.env`
Copy [`.env.example`](.env.example) to `.env`:
```bash
cp .env.example .env
```

Edit `.env` with your settings:
```env
HOST=0.0.0.0
PORT=8080
PUBLIC_BASE_URL=http://your-tailscale-or-domain:8080
STORAGE_MODE=telegram

# Optional if STORAGE_MODE is "drives" or "both"
DRIVES_JSON=[{"name": "local", "path": "/home/ubuntu/media-vault"}]

# 1 to 4+ Telegram API/bot workers from my.telegram.org + @BotFather
TELEGRAM_APIS_JSON=[{"name": "acc1", "api_id": 111111, "api_hash": "hash1", "bot_token": "bot_token_1"}]
ALLOWED_TELEGRAM_USER_IDS=123456789
TELEGRAM_ARCHIVE_CHAT_ID=-1001234567890
```

### 3. Setting up the Shared Telegram Supergroup (Topics = Folders)
1. Create a **Private Group** in Telegram → **Edit Group → Topics → Enable** (converts it into a Forum Supergroup).
2. Add **your Telegram account(s)** and **all bot(s)** from `TELEGRAM_APIS_JSON` to that group, and promote all bots to **Administrators** (*Post/Edit/Delete Messages* and *Manage Topics*).
3. Grab the supergroup's ID (forward a message from it to `@userinfobot` or `@RawDataBot` — e.g. `-1001234567890`) and set `TELEGRAM_ARCHIVE_CHAT_ID` in `.env`.

### 4. Run the server
```bash
.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8080
```
Open `http://localhost:8080` (or your Tailscale IP) on your phone or laptop.

### 5. Run Tests
```bash
.venv/bin/pytest -v
```

### 6. Notes for `rclone mount` & Reverse Proxies
- Mount cloud drives with `--vfs-cache-mode writes` if using `STORAGE_MODE=drives` or `both`.
- If putting Nginx/Caddy in front of this server for 2 GB web uploads, set `client_max_body_size 2048M;` (Nginx) or leave Caddy's default.

### 7. Verifying & Restoring a Database Backup (CLI)
> **Important:** Stop the Media Vault server before restoring so active connections do not hold SQLite locks or replay stale WAL state.

1. **Verify a backup without modifying the database:**
   ```bash
   .venv/bin/python scripts/restore_vault.py --backup ./data/backups/vault_backup_<timestamp>_<id>.db.gz --verify-only
   ```
2. **Restore the database** (automatically creates a `.pre-restore-<timestamp>.bak` copy of any existing DB, removes stale `-wal`/`-shm` sidecars, and verifies `PRAGMA integrity_check`; pass `--db-path <path>` to override `DATA_DIR/media.db`, and `--yes` to skip the interactive prompt):
   ```bash
   .venv/bin/python scripts/restore_vault.py --backup ./data/backups/vault_backup_<timestamp>_<id>.db.gz --yes
   ```
3. **Restart the server** once the restore reports `Status : RESTORED`.

### 8. One-Tap Capture Ecosystem (Bookmarklet, Browser Extension, Android & iOS)

1. **Quick-Capture Popup & Bookmarklet (`GET /capture`)**: Works across strict-CSP and HTTPS→HTTP LAN/Tailscale setups via a popup window (`window.open`), reusing same-origin `localStorage` auth token so `WEB_AUTH_TOKEN` is never leaked in bookmarklet URLs. Drag-and-drop Bookmarklet generator in **Capture Tools** (System Settings → Capture tab) with optional auto-submit (`auto=1`), auto-close (`autoclose=1`), folder remembrance, duplicate check, and File/PDF/Photo upload.
2. **Companion Browser Extension (`clients/browser-extension/`, Manifest V3, Zero-Build)**: Downloadable directly from the Vault (`GET /api/system/extension.zip` or Capture Tools → Download Extension). Chrome/Edge/Brave & Firefox MV3 compatible; supports URL/page capture, visible-tab screenshot/photo capture, local PDF/file upload, right-click context menus with dynamic `Send to Folder...` submenu and remembered folder choice, plus **opt-in YouTube Cookie Sync** (`optional_permissions: ["cookies"]` strictly scoped to YouTube/Google domains, posting Netscape `cookies.txt` to `POST /api/ytdlp/cookies`).
3. **Android & iOS Capture (`POST /api/capture/quick`)**: Android PWA Share Sheet (`/share-target`) + Android HTTP Shortcuts / Tasker recipe. Apple/iOS Shortcuts recipe calling `POST /api/capture/quick` (JSON or form-encoded) returning concise `summary`, `gallery_url`, `job_id`, and `duplicate` status for instant notifications.
4. **Controlled CORS Configuration**: Document `CAPTURE_CORS_ORIGINS` and `CAPTURE_CORS_ALLOW_EXTENSIONS` (off by default, wildcards rejected, `allow_credentials=False`).

### 9. Watch Subscriptions (Auto-Archiver)

1. **System Sheet → Subscriptions**: Create, edit, pause/resume, dry-run (`?dry_run=true`), live-check, **Archive Existing** (`?full_catalog=true&queue_existing=true`), or inspect the per-entry ledger (`seen`, `queued`, `ingested`, `skipped`, `failed`) for any YouTube channel/playlist or RSS/Atom/RDF feed.
2. **First-Run Flood Protection & Full Archiving**: New subscriptions default to `seed_only=true` (`initial_backfill_limit=0`) so existing catalogs are seeded without triggering a download storm; optional `initial_backfill_limit` queues only the newest N items, or enable **Download all existing videos on first run** (`archive_existing=true`) to scan and archive the full channel/playlist (up to `SUBSCRIPTIONS_FULL_CATALOG_MAX_ITEMS`, default `1000`).
3. **Telegram Commands**: Run `/subs` to view watched feeds, `/subs check <id>` to trigger an incremental check, or `/subs archive <id>` to scan and queue all existing items from Telegram.
4. **Security & SSRF**: Outbound feed fetches, redirect hops, and subscription-triggered downloads block private/loopback/link-local/metadata IPs unless `SUBSCRIPTIONS_ALLOW_PRIVATE_HOSTS=true`; XML parsing forbids DTD/ENTITY/NOTATION declarations; title regexes are AST-screened against ReDoS. See [`docs/decisions/subscriptions.md`](docs/decisions/subscriptions.md) for architecture details.

### 10. External Players & Read-Only WebDAV (/webdav/)

Media Vault exposes your library as a **read-only** WebDAV share so external players and file managers can browse and stream media in place without copying or modifying anything. A generated M3U playlist is also available for one-tap playback.

**Endpoints**
- `GET /webdav/` — WebDAV root (browse folders and play media).
- `GET /webdav.m3u` — M3U playlist of all playable media.
- `GET /webdav.m3u?folder=Movies` — Playlist limited to a specific folder.

**Supported clients**
- **VLC** (desktop & mobile): open network stream and point it at the WebDAV URL or the M3U playlist.
- **Infuse** (Apple TV / iOS / macOS): add a WebDAV share with the server URL and credentials.
- **Kodi**: add a "Video → Network location" WebDAV source.
- **MPV**: play the M3U playlist directly (`mpv "<base>/webdav.m3u"`).
- **Nova Video Player** (Android / Android TV): add a WebDAV network source.
- **Finder / Explorer / file managers**: mount the WebDAV URL as a read-only network drive (macOS ⌘K "Connect to Server", Windows "Map a network drive").
- **rclone**: configure a `webdav` remote and mount it read-only (see below).

**.env settings**

```env
WEBDAV_ENABLED=true                 # master switch (default: true)
WEBDAV_USERNAME=vault               # username presented to clients (default: vault)
WEBDAV_PASSWORD=<strong-password>   # dedicated WebDAV password -> Basic Auth
WEB_AUTH_TOKEN=<your-token>         # alternative: reuse the vault token for WebDAV Basic Auth
WEBDAV_ALLOW_ANONYMOUS=true         # allow unauthenticated access (LAN only!)
```

The share is **fail-closed by default**: with `WEBDAV_ENABLED=true` but none of `WEBDAV_PASSWORD`, `WEB_AUTH_TOKEN`, or `WEBDAV_ALLOW_ANONYMOUS=true` set, the endpoint refuses requests until you configure an auth method. For safety, only use `WEBDAV_ALLOW_ANONYMOUS=true` on a trusted LAN, never on a public host. Passwords and tokens are never exposed by the status endpoints (`/api/health`, `/api/system/status`, `/api/capture/config`).

**Quick examples**

VLC / MPV — open the playlist (add `user:password@` to the URL or use the player's auth prompt when Basic Auth is enabled):

```bash
vlc "http://your-host:8080/webdav.m3u"
mpv "http://your-host:8080/webdav.m3u?folder=Movies"
```

Infuse / Kodi — in the app, choose "Add Network Share / Source", protocol **WebDAV**, address `http://your-host:8080/webdav/`, username `vault`, and the configured WebDAV password (or leave the password empty for anonymous LAN access).

rclone — configure and mount read-only:

```bash
rclone config create mediavault webdav url "http://your-host:8080/webdav/" vendor other user "vault" pass "<PASSWORD>"
rclone mount mediavault: ~/MediaVault --read-only --vfs-cache-mode full
```

The **Capture Tools → External Players & WebDAV Mount (Read-Only)** card in the System sheet shows the live status, copyable endpoints, and ready-to-paste client snippets. Your real WebDAV password is never shown or embedded; the vault token only appears in copied recipes when the existing "Include my active auth token in recipes" opt-in is enabled.


