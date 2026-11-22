# Media Vault Companion

A lightweight Manifest V3 browser extension for sending pages, media URLs, screenshots,
photos, PDFs, and other files straight to your self-hosted **Media Vault** — with remembered
folders, tags, and optional signed-in YouTube cookie sync.

## Install

### Chrome, Edge, or Brave

1. Open `chrome://extensions` (or `edge://extensions` / `brave://extensions`).
2. Enable **Developer mode** (top-right).
3. Click **Load unpacked** and select this `clients/browser-extension` folder.
4. Pin **Media Vault Companion** to the toolbar.

### Firefox

1. Open `about:debugging#/runtime/this-firefox`.
2. Click **Load Temporary Add-on…**.
3. Select `manifest.json` in this folder.

Temporary add-ons are removed when Firefox restarts; reload the manifest after a restart.
For a permanent install, the folder can be packaged and signed through
[addons.mozilla.org](https://addons.mozilla.org/developers/).

## First-time setup

1. Click the extension icon, then the gear button (settings open automatically on first run).
2. Enter your Media Vault server URL, for example `http://localhost:8080` or your
   Tailscale/LAN address such as `http://100.x.y.z:8080`.
3. If the vault uses `WEB_AUTH_TOKEN`, paste it into the token field and use **Show** to verify.
4. Keep **Remember last chosen folder** enabled if you want the folder selection to persist.
5. Click **Save & Test Connection**. The extension asks for permission for that server origin,
   calls `/api/health`, saves the settings, and loads your folders and tags.

The status pill in the header shows **Connected**, **Offline**, or **Setup**.

## Capturing

The capture panel has three tabs, and the shared **Destination** controls (folder, tags,
Telegram forwarding) apply to all of them.

- **URL / Page** — pre-fills the current tab's URL and title, checks for duplicates as you type,
  and lets you pick `best`, `1080p`, `720p`, `480p`, or audio-only. If the item already exists,
  the banner shows its folder and offers **Force re-ingest**.
- **Screenshot / Photo** — captures the visible page (including print preview or a photo opened
  in the browser) as a PNG, shows a thumbnail, and uploads it to the chosen folder.
- **File / PDF** — accepts any file type, including PDFs, print-to-PDF exports, photos, videos,
  and documents, then uploads it to the chosen folder.

Choose a folder by typing, picking from the datalist, or clicking a recent-folder chip. The
last folder is remembered across popup sessions and is also pushed to the background service
worker (`MV_REFRESH_MENUS`), so the right-click context menus offer the same destinations.

The **Recent Captures** list shows the last five submissions and refreshes queued/active jobs
against the vault's job status when the popup opens.

## Right-click context menus

The background service worker adds context-menu entries for the current page, links, images,
and selected text. They use the same remembered folder and tags, so captures started from the
page context land in the same place as captures started from the popup.

## YouTube cookie sync (opt-in)

Some videos require a signed-in YouTube session. The extension can copy only YouTube/Google
cookies into the vault's yt-dlp cookie file:

1. Sign in to YouTube in the same browser.
2. Open settings and click **Sync YouTube Cookies to Vault**.
3. Approve the browser's cookie and YouTube/Google host-permission prompts.

The card shows whether the vault already has cookies (`ytdlp_cookies_configured`), whether the
extension has access, and the number and domains of cookies that were synced. Use
**Revoke Cookie Access** to remove the extension's cookie permission at any time.

The extension requests no required host permissions; server and cookie access are granted only
when you explicitly approve them.

## Permissions

| Permission | Why it is used |
| --- | --- |
| `activeTab` | Read the current tab's URL/title and capture the visible page after a click. |
| `contextMenus` | Add right-click capture actions. |
| `storage` | Remember the server URL, token, folder, format, and recent captures. |
| `notifications` | Report background capture results. |
| `cookies` (optional) | Copy YouTube/Google cookies after explicit approval. |
| Host permissions (optional) | Talk to your vault server and read YouTube/Google cookies after approval. |
