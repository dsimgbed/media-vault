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
