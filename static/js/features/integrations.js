import { state } from "/static/js/state.js";
import { apiFetch } from "/static/js/api.js";
import { $, escapeHtml } from "/static/js/utils.js";
import { confirmDialog, toast } from "/static/js/ui/dialogs.js";

const PREFS_KEY = "mv_capture_prefs_v1";
const TOKEN_PLACEHOLDER = "<YOUR_WEB_AUTH_TOKEN>";
const EXTENSION_FILENAME = "media-vault-extension.zip";
const RECIPE_FILENAME = "media-vault-capture-recipe.json";

let configCache = null;
let configLoading = false;
let configError = "";
let configAuthFailed = false;
let configTokenAttempted = null;

const prefs = readPrefs();

function readPrefs() {
  let saved = {};
  try {
    saved = JSON.parse(localStorage.getItem(PREFS_KEY) || "{}") || {};
  } catch (_) {
    saved = {};
  }
  return {
    // Only fall back to the vault default when nothing has been saved yet,
    // so an intentionally cleared folder stays cleared.
    folder:
      saved.folder === undefined
        ? String(state.defaultFolder || "General")
        : String(saved.folder || ""),
    autoArchive: Boolean(saved.autoArchive),
    autoClose: saved.autoClose === undefined ? true : Boolean(saved.autoClose),
    includeToken: Boolean(saved.includeToken),
  };
}

function savePrefs() {
  try {
    localStorage.setItem(PREFS_KEY, JSON.stringify(prefs));
  } catch (_) {
    /* localStorage may be unavailable (private mode / quota) */
  }
}

function isLocked() {
  return Boolean(state.authRequired && !state.token);
}

function origin() {
  const detected = window.location.origin;
  if (detected && detected !== "null") return detected;
  return `${window.location.protocol}//${window.location.host}`;
}

function resolveView(e) {
  return e?.currentTarget || $("systemMaintenanceView");
}

function extensionVersion() {
  const cfg = configCache || {};
  const raw = cfg.extension_version || cfg.ext_version || cfg.version || "";
  return String(raw || "").trim();
}

function recipeToken() {
  return prefs.includeToken && state.token ? state.token : TOKEN_PLACEHOLDER;
}

function captureFolder() {
  return (prefs.folder || "").trim();
}

/**
 * Read the WebDAV status summary exposed by /api/capture/config.
 * While locked or still loading, fall back to the unauthenticated
 * /api/health values mirrored onto `state`. The summary never contains
 * passwords or tokens.
 */
function webdavInfo() {
  const raw = configCache?.webdav || {};
  const authMode = String(raw.auth_mode || "").trim();
  const hasSummary = Boolean(authMode);
  return {
    enabled: hasSummary ? Boolean(raw.enabled) : Boolean(state.webdavEnabled),
    configured: hasSummary ? Boolean(raw.configured) : Boolean(state.webdavConfigured),
    authRequired: hasSummary
      ? Boolean(raw.auth_required)
      : Boolean(state.webdavAuthRequired),
    authMode: hasSummary ? authMode : "",
    username: hasSummary ? String(raw.username || "vault") : "vault",
    path: hasSummary ? String(raw.path || "/webdav/") : "/webdav/",
    playlistPath: hasSummary
      ? String(raw.playlist_path || "/webdav.m3u")
      : "/webdav.m3u",
    readOnly: hasSummary ? Boolean(raw.read_only) : true,
  };
}

function webdavStatusPill(info) {
  if (info.authMode) {
    switch (info.authMode) {
      case "disabled":
        return `<span class="pill">Disabled</span>`;
      case "basic_password":
        return `<span class="pill ok">Basic Auth (Dedicated)</span>`;
      case "basic_token":
        return `<span class="pill ok">Basic Auth (Vault Token)</span>`;
      case "anonymous":
        return `<span class="pill warn">Anonymous (LAN)</span>`;
      case "unconfigured":
        return `<span class="pill warn">Setup Needed</span>`;
      default:
        break;
    }
  }
  const label = info.enabled
    ? info.configured
      ? "Enabled"
      : "Setup Needed"
    : "Disabled";
  const cls = !info.enabled ? "" : info.configured ? " ok" : " warn";
  return `<span class="pill${cls}">${label}</span>`;
}

/** Password shown in client recipes — never a real secret. */
function webdavPasswordPlaceholder(info) {
  if (info.authMode === "anonymous") return "(none — anonymous)";
  if (info.authMode === "basic_token") return recipeToken();
  return "<WEBDAV_PASSWORD>";
}

function webdavPlaylistCommand(info, base) {
  const url = `${base}${info.playlistPath}`;
  if (info.authRequired) {
    return `vlc http://vault:<WEBDAV_PASSWORD>@${url.replace(/^https?:\/\//, "")}`;
  }
  return `vlc "${url}"`;
}

function webdavRcloneSnippet(info, base) {
  return [
    `rclone config create mediavault webdav url "${base}${info.path}" vendor other user "${info.username}" pass "<PASSWORD>"`,
    `rclone mount mediavault: ~/MediaVault --read-only --vfs-cache-mode full`,
  ].join("\n");
}

/**
 * Build a compact bookmarklet that opens the in-app capture popup.
 *
 * The bookmarklet deliberately never contains the auth token: the capture
 * page itself handles the vault unlock, so a shared bookmark stays safe.
 */
function buildBookmarklet() {
  const folder = captureFolder();
  const folderPart = folder ? `&folder=${encodeURIComponent(folder)}` : "";
  const autoPart = prefs.autoArchive ? "&auto=1" : "";
  const closePart = prefs.autoClose ? "&autoclose=1" : "";
  const target = JSON.stringify(`${origin()}/capture`);
  const extra = JSON.stringify(`${folderPart}${autoPart}${closePart}`);
  return (
    "javascript:(function(){var q=\"?url=\"+encodeURIComponent(location.href)" +
    "+\"&title=\"+encodeURIComponent(document.title)" +
    `+${extra};` +
    `window.open(${target}+q,"mv_capture","width=480,height=660,resizable=yes,scrollbars=yes");})();`
  );
}

function buildAndroidJson() {
  const folder = captureFolder() || "General";
  return `{"url": "https://example.com/video.mp4", "folder": ${JSON.stringify(folder)}, "format_spec": "best", "source": "android"}`;
}

function buildAndroidCurl() {
  const body = buildAndroidJson().replace(/'/g, "'\\''");
  return [
    `curl -X POST '${origin()}/api/capture/quick' \\`,
    `  -H "Authorization: Bearer ${recipeToken()}" \\`,
    `  -H "Content-Type: application/json" \\`,
    `  -d '${body}'`,
  ].join("\n");
}

function buildIosBody() {
  const folder = captureFolder() || "General";
  return `{"url": "Shortcut Input", "folder": ${JSON.stringify(folder)}, "format_spec": "best", "source": "shortcut"}`;
}

function captureTextFor(key) {
  const base = origin();
  switch (key) {
    case "bookmarklet":
      return buildBookmarklet();
    case "server-url":
      return base;
    case "android-endpoint":
    case "ios-endpoint":
      return `${base}/api/capture/quick`;
    case "android-json":
      return buildAndroidJson();
    case "android-curl":
      return buildAndroidCurl();
    case "ios-auth":
      return `Authorization: Bearer ${recipeToken()}`;
    case "ios-body":
      return buildIosBody();
    case "webdav-url": {
      const info = webdavInfo();
      return `${base}${info.path}`;
    }
    case "webdav-m3u": {
      const info = webdavInfo();
      return `${base}${info.playlistPath}`;
    }
    case "webdav-user":
      return webdavInfo().username;
    case "webdav-rclone": {
      const info = webdavInfo();
      return webdavRcloneSnippet(info, base);
    }
    case "webdav-vlc": {
      const info = webdavInfo();
      return webdavPlaylistCommand(info, base);
    }
    default:
      return null;
  }
}

function captureLabelFor(key) {
  switch (key) {
    case "bookmarklet":
      return "Bookmarklet code";
    case "server-url":
      return "Server URL";
    case "android-endpoint":
    case "ios-endpoint":
      return "Endpoint URL";
    case "android-json":
      return "Android JSON payload";
    case "android-curl":
      return "curl command";
    case "ios-auth":
      return "Authorization header";
    case "ios-body":
      return "Sample JSON body";
    case "webdav-url":
      return "WebDAV URL";
    case "webdav-m3u":
      return "M3U Playlist URL";
    case "webdav-user":
      return "WebDAV username";
    case "webdav-rclone":
      return "rclone setup snippet";
    case "webdav-vlc":
      return "VLC / MPV playlist command";
    default:
      return "Value";
  }
}

/**
 * Clipboard helper with a textarea fallback for plain-HTTP LAN/Tailscale
 * origins where navigator.clipboard is missing or rejects.
 */
async function copyText(text, label) {
  const value = String(text ?? "");
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(value);
      toast(`${label} copied.`, { type: "success" });
      return true;
    }
    throw new Error("clipboard API unavailable");
  } catch (_) {
    try {
      const ta = document.createElement("textarea");
      ta.value = value;
      ta.setAttribute("readonly", "");
      ta.style.position = "fixed";
      ta.style.top = "-1000px";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      ta.setSelectionRange(0, value.length);
      const ok = document.execCommand("copy");
      ta.remove();
      if (!ok) throw new Error("copy command rejected");
      toast(`${label} copied.`, { type: "success" });
      return true;
    } catch (err) {
      toast(`Could not copy ${label.toLowerCase()}.`, { type: "error" });
      return false;
    }
  }
}

function downloadBlob(content, filename, type) {
  const blob = content instanceof Blob ? content : new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  // Revoke on the next tick so Safari/Firefox can start the download.
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function testCapturePopup() {
  const url = `${origin()}/capture?url=https://example.com&title=Test+Capture`;
  const popup = window.open(
    url,
    "mv_capture_test",
    "width=480,height=660,resizable=yes,scrollbars=yes"
  );
  if (!popup) {
    toast("Popup blocked — allow popups for this site and try again.", { type: "error" });
    return;
  }
  toast("Capture popup opened.", { type: "info" });
}

async function downloadExtension(view) {
  if (isLocked()) {
    toast("Unlock vault to download the extension.", { type: "error" });
    return;
  }
  const btn = view?.querySelector("[data-capture-download-ext]");
  const prevLabel = btn?.textContent || "Download Extension (.zip)";
  if (btn) {
    btn.disabled = true;
    btn.textContent = "Preparing…";
  }
  try {
    // Send the token as an Authorization header, never in the query string.
    const headers = state.token ? { Authorization: `Bearer ${state.token}` } : {};
    const res = await fetch("/api/system/extension.zip", { headers });
    if (res.status === 401) {
      throw new Error("Unauthorized — unlock the vault and retry.");
    }
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    const blob = await res.blob();
    downloadBlob(blob, EXTENSION_FILENAME, "application/zip");
    toast("Extension download started.", { type: "success" });
  } catch (err) {
    toast(`Extension download failed: ${err.message}`, { type: "error" });
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = prevLabel;
    }
  }
}

function downloadRecipeJson() {
  const payload = {
    name: "Media Vault Quick Capture",
    description: "HTTP Shortcuts / Tasker quick-capture recipe",
    method: "POST",
    url: `${origin()}/api/capture/quick`,
    headers: {
      Authorization: `Bearer ${recipeToken()}`,
      "Content-Type": "application/json",
    },
    body: JSON.parse(buildAndroidJson()),
  };
  downloadBlob(
    JSON.stringify(payload, null, 2),
    RECIPE_FILENAME,
    "application/json"
  );
  toast("Capture recipe downloaded.", { type: "success" });
}

function updateBookmarkletDom(view) {
  if (!view) return;
  const code = buildBookmarklet();
  const anchor = view.querySelector("#captureBookmarklet");
  if (anchor) anchor.setAttribute("href", code);
  const codeEl = view.querySelector("#captureBookmarkletCode");
  if (codeEl) codeEl.textContent = code;
}

function renderCaptureView(view) {
  if (!view) return;
  const scroller = view.closest(".system-sheet-body");
  const scrollTop = scroller?.scrollTop || 0;

  const base = origin();
  const locked = isLocked();
  const version = extensionVersion();
  const folder = prefs.folder || "";
  const bookmarklet = buildBookmarklet();
  const androidJson = buildAndroidJson();
  const androidCurl = buildAndroidCurl();
  const iosBody = buildIosBody();
  const iosAuth = `Authorization: Bearer ${recipeToken()}`;

  const webdav = webdavInfo();
  const webdavUrl = `${base}${webdav.path}`;
  const webdavM3u = `${base}${webdav.playlistPath}`;
  const webdavPassword = webdavPasswordPlaceholder(webdav);
  const webdavStatus = webdavStatusPill(webdav);
  const webdavNeedsSetup = webdav.enabled && (!webdav.configured || webdav.authMode === "unconfigured");
  const webdavSetupHint = webdavNeedsSetup
    ? `<div class="sys-hint-box">
         WebDAV is fail-closed until you set <code>WEBDAV_PASSWORD</code>,
         <code>WEB_AUTH_TOKEN</code>, or <code>WEBDAV_ALLOW_ANONYMOUS=true</code> in <code>.env</code>.
       </div>`
    : "";

  const versionLabel = locked
    ? "Unlock to view version"
    : version
      ? `Extension v${version}`
      : "Extension version unavailable";

  const tokenPill = prefs.includeToken
    ? `<span class="pill warn">Token visible in recipes</span>`
    : `<span class="pill ok">Token masked</span>`;

  const lockedHint = locked
    ? `<div class="capture-locked-hint">
        <span>Unlock vault to download the extension zip.</span>
        <button type="button" class="secondary btn-xs" data-capture-unlock>Open Settings</button>
      </div>`
    : "";

  const tokenWarning =
    prefs.includeToken && state.token
      ? `<div class="sys-hint-box capture-warn">
          Your active auth token is now visible in these recipes. Clear the opt-in when finished.
        </div>`
      : "";

  const configNote =
    !locked && configError
      ? `<div class="small-muted">Extension config unavailable (${escapeHtml(configError)}).</div>`
      : "";

  view.innerHTML = `
    <div class="capture-tools-stack">
      <div class="sys-hint-box capture-security-row">
        <div class="capture-security-copy">
          <strong>Recipes &amp; tokens</strong>
          <span class="small-muted">
            Bookmarklets never embed your auth token. Shortcut recipes use
            <code>${escapeHtml(TOKEN_PLACEHOLDER)}</code> until you opt in.
          </span>
        </div>
        <label class="checkbox-label capture-opt-row">
          <input id="captureIncludeToken" type="checkbox" ${prefs.includeToken ? "checked" : ""} />
          Include my active auth token in copied recipe
        </label>
        ${tokenPill}
      </div>
      ${tokenWarning}

      <div class="sys-card capture-tools-card">
        <div class="sys-card-head">
          <strong>One-Tap Bookmarklet</strong>
          <span class="pill">Desktop &amp; Mobile Browsers</span>
        </div>
        <p class="small-muted sys-card-copy">
          Drag the star button to your bookmarks bar, then click it on any page to
          send that URL and title into Media Vault. Live origin:
          <code>${escapeHtml(base)}</code>
        </p>
        <div class="capture-opt-grid">
          <label class="capture-field">
            <span>Default folder (optional)</span>
            <input id="captureFolderInput" type="text" list="folderList"
              value="${escapeHtml(folder)}" placeholder="General" autocomplete="off" />
          </label>
          <label class="checkbox-label capture-opt-row">
            <input id="captureAutoArchive" type="checkbox" ${prefs.autoArchive ? "checked" : ""} />
            Auto-archive immediately on click (<code>auto=1</code>)
          </label>
          <label class="checkbox-label capture-opt-row">
            <input id="captureAutoClose" type="checkbox" ${prefs.autoClose ? "checked" : ""} />
            Auto-close popup when done (<code>autoclose=1</code>)
          </label>
        </div>
        <div class="sys-actions-row capture-copy-row">
          <a id="captureBookmarklet" class="bookmarklet-drag-btn"
            href="${escapeHtml(bookmarklet)}" draggable="true"
            title="Drag me to your bookmarks bar">★ Archive to Media Vault</a>
          <button type="button" class="secondary btn-sm" data-capture-copy="bookmarklet">Copy Bookmarklet Code</button>
          <button type="button" class="secondary btn-sm" data-capture-test>Test Capture Popup</button>
        </div>
        <pre class="capture-code-block"><code id="captureBookmarkletCode">${escapeHtml(bookmarklet)}</code></pre>
      </div>

      <div class="sys-card capture-tools-card">
        <div class="sys-card-head">
          <strong>Companion Browser Extension</strong>
          <span class="pill">${escapeHtml(versionLabel)}</span>
        </div>
        <p class="small-muted sys-card-copy">
          Chrome, Edge, Brave, and Firefox (MV3). Capture the current tab or upload
          visible page screenshots, photos, and PDFs without leaving the page.
        </p>
        <ul class="capture-feature-list">
          <li>Folder selection with remembrance</li>
          <li>Right-click context menus</li>
          <li>Visible page screenshot / photo &amp; PDF upload</li>
          <li>Opt-in YouTube cookie sync</li>
        </ul>
        <div class="sys-actions-row capture-copy-row">
          <button type="button" class="btn-sm" data-capture-download-ext
            ${locked ? 'disabled title="Unlock vault to download"' : ""}>Download Extension (.zip)</button>
          <button type="button" class="secondary btn-sm" data-capture-copy="server-url">Copy Server URL</button>
        </div>
        ${lockedHint}
        ${configNote}
        <ol class="capture-steps">
          <li>Open <code>chrome://extensions</code>, enable <strong>Developer mode</strong>, then choose <strong>Load unpacked</strong> and select the unzipped folder.</li>
          <li>Firefox: open <code>about:debugging#/runtime/this-firefox</code>, choose <strong>Load Temporary Add-on</strong>, and select the extension manifest.</li>
          <li>Pin the extension. Your server URL and last folder are remembered between captures.</li>
        </ol>
        <div class="sys-hint-box">
          <strong>Firefox / custom CORS:</strong>
          set <code>CAPTURE_CORS_ALLOW_EXTENSIONS=true</code> on the server so
          extension origins can post captures.
        </div>
      </div>

      <div class="sys-card capture-tools-card">
        <div class="sys-card-head">
          <strong>Android Capture</strong>
          <span class="pill">PWA Share Sheet · HTTP Shortcuts</span>
        </div>
        <p class="small-muted sys-card-copy">
          Install Media Vault as an app, then use the Android share sheet to send
          links, photos, videos, or files directly to the vault from any app.
        </p>
        <div class="capture-subhead">
          <strong>HTTP Shortcuts / Tasker quick capture</strong>
          <span class="small-muted">POST <code>${escapeHtml(base)}/api/capture/quick</code></span>
        </div>
        <div class="capture-code-head">
          <span class="small-muted">JSON payload</span>
          <button type="button" class="secondary btn-xs" data-capture-copy="android-json">Copy JSON</button>
        </div>
        <pre class="capture-code-block"><code>${escapeHtml(androidJson)}</code></pre>
        <div class="capture-code-head">
          <span class="small-muted">curl</span>
          <div class="sys-actions-row">
            <button type="button" class="secondary btn-xs" data-capture-copy="android-curl">Copy curl</button>
            <button type="button" class="secondary btn-xs" data-capture-download-json>Download .json Recipe</button>
          </div>
        </div>
        <pre class="capture-code-block"><code>${escapeHtml(androidCurl)}</code></pre>
        <p class="small-muted sys-card-copy">
          In HTTP Shortcuts, create a shortcut with method <code>POST</code>, add the
          Authorization and Content-Type headers, and paste the JSON body. Tasker can
          use the same payload with its HTTP Request action.
        </p>
      </div>

      <div class="sys-card capture-tools-card">
        <div class="sys-card-head">
          <strong>iOS / Apple Shortcuts</strong>
          <span class="pill">Share Sheet</span>
        </div>
        <p class="small-muted sys-card-copy">
          Build a one-tap Shortcut that appears in the iOS share sheet and posts the
          shared URL to the quick-capture endpoint.
        </p>
        <div class="capture-copy-list">
          <div class="capture-copy-row">
            <div class="capture-copy-label">
              <span>Endpoint URL</span>
              <code>${escapeHtml(base)}/api/capture/quick</code>
            </div>
            <button type="button" class="secondary btn-xs" data-capture-copy="ios-endpoint">Copy</button>
          </div>
          <div class="capture-copy-row">
            <div class="capture-copy-label">
              <span>Authorization Header</span>
              <code>${escapeHtml(iosAuth)}</code>
            </div>
            <button type="button" class="secondary btn-xs" data-capture-copy="ios-auth">Copy</button>
          </div>
          <div class="capture-copy-row">
            <div class="capture-copy-label">
              <span>Sample JSON Body</span>
              <code>${escapeHtml(iosBody)}</code>
            </div>
            <button type="button" class="secondary btn-xs" data-capture-copy="ios-body">Copy</button>
          </div>
        </div>
        <ol class="capture-steps">
          <li>Enable <strong>Show in Share Sheet</strong> and accept URLs, Safari web pages, and Text.</li>
          <li>Add <strong>Get Contents of URL</strong> → Method <code>POST</code>, headers <code>Authorization</code> and <code>Content-Type: application/json</code>, request body JSON above.</li>
          <li>Add <strong>Get Dictionary Value</strong> → key <code>summary</code>.</li>
          <li>Add <strong>Show Notification</strong> → text: Dictionary Value.</li>
        </ol>
        <p class="small-muted sys-card-copy">
          The header stays masked as <code>${escapeHtml(TOKEN_PLACEHOLDER)}</code> unless you
          enable "Include my active auth token in recipes" above.
        </p>
      </div>

      <div class="sys-card capture-tools-card">
        <div class="sys-card-head">
          <strong>External Players &amp; WebDAV Mount (Read-Only)</strong>
          ${webdavStatus}
        </div>
        <p class="small-muted sys-card-copy">
          Mount your vault as a read-only network drive or point a media player at the
          WebDAV endpoint to stream and browse your library in place. Nothing can be
          written back to the vault.
        </p>
        ${webdavSetupHint}
        <div class="capture-copy-list">
          <div class="capture-copy-row">
            <div class="capture-copy-label">
              <span>WebDAV URL</span>
              <code>${escapeHtml(webdavUrl)}</code>
            </div>
            <button type="button" class="secondary btn-xs" data-capture-copy="webdav-url">Copy</button>
          </div>
          <div class="capture-copy-row">
            <div class="capture-copy-label">
              <span>M3U Playlist URL</span>
              <code>${escapeHtml(webdavM3u)}</code>
            </div>
            <button type="button" class="secondary btn-xs" data-capture-copy="webdav-m3u">Copy</button>
          </div>
          <div class="capture-copy-row">
            <div class="capture-copy-label">
              <span>Username</span>
              <code>${escapeHtml(webdav.username)}</code>
            </div>
            <button type="button" class="secondary btn-xs" data-capture-copy="webdav-user">Copy</button>
          </div>
        </div>

        <div class="capture-subhead">
          <strong>VLC / MPV / Kodi / Infuse</strong>
          <span class="small-muted">Quick setup</span>
        </div>
        <ul class="capture-feature-list">
          <li>Protocol: <code>WebDAV (HTTP/HTTPS)</code></li>
          <li>Address / Path: <code>${escapeHtml(webdavUrl)}</code></li>
          <li>Username: <code>${escapeHtml(webdav.username)}</code></li>
          <li>Password: <code>${escapeHtml(webdavPassword)}</code></li>
        </ul>

        <div class="capture-code-head">
          <span class="small-muted">rclone config + read-only mount</span>
          <button type="button" class="secondary btn-xs" data-capture-copy="webdav-rclone">Copy rclone snippet</button>
        </div>
        <pre class="capture-code-block"><code>${escapeHtml(webdavRcloneSnippet(webdav, base))}</code></pre>

        <div class="capture-code-head">
          <span class="small-muted">MPV / VLC playlist command</span>
          <button type="button" class="secondary btn-xs" data-capture-copy="webdav-vlc">Copy command</button>
        </div>
        <pre class="capture-code-block"><code>${escapeHtml(webdavPlaylistCommand(webdav, base))}</code></pre>
        ${
          webdav.authRequired
            ? `<div class="sys-hint-box">
                 This server requires authentication. Replace <code>&lt;WEBDAV_PASSWORD&gt;</code>
                 with your WebDAV password (or use the vault token when "Vault Token" auth is active).
               </div>`
            : ""
        }
        <p class="small-muted sys-card-copy">
          Tip: append <code>?folder=Movies</code> to the playlist URL to stream a specific folder,
          e.g. <code>${escapeHtml(webdavM3u)}?folder=Movies</code>.
        </p>
      </div>
    </div>
  `;

  if (scroller) scroller.scrollTop = scrollTop;
}

function isCaptureVisible(view) {
  return Boolean(
    view &&
      !view.hidden &&
      $("systemSheet")?.open &&
      view.querySelector(".capture-tools-stack")
  );
}

async function fetchCaptureConfig(view) {
  if (configLoading || isLocked()) return;
  if (configAuthFailed && configTokenAttempted === state.token) return;
  configLoading = true;
  configTokenAttempted = state.token;
  try {
    configCache = await apiFetch("/api/capture/config");
    configError = "";
    configAuthFailed = false;
  } catch (err) {
    configError = err.message || "unavailable";
    configAuthFailed = /unauthori[sz]ed/i.test(configError);
  } finally {
    configLoading = false;
    if (isCaptureVisible(view)) renderCaptureView(view);
  }
}

export function loadCaptureTab(view) {
  const target = view || $("systemMaintenanceView");
  if (!target) return;
  renderCaptureView(target);
  const authBlocked = configAuthFailed && configTokenAttempted === state.token;
  if (!isLocked() && !configCache && !configLoading && !authBlocked) {
    fetchCaptureConfig(target);
  }
}

export function handleCaptureInput(e) {
  const t = e.target;
  if (!t || t.id !== "captureFolderInput") return;
  prefs.folder = t.value;
  savePrefs();
  updateBookmarkletDom(resolveView(e));
}

export async function handleCaptureChange(e) {
  const t = e.target;
  if (!t) return;
  const view = resolveView(e);

  if (t.id === "captureAutoArchive") {
    prefs.autoArchive = Boolean(t.checked);
    savePrefs();
    updateBookmarkletDom(view);
    return;
  }

  if (t.id === "captureAutoClose") {
    prefs.autoClose = Boolean(t.checked);
    savePrefs();
    updateBookmarkletDom(view);
    return;
  }

  if (t.id !== "captureIncludeToken") return;

  if (!t.checked) {
    prefs.includeToken = false;
    savePrefs();
    renderCaptureView(view);
    return;
  }

  if (!state.token) {
    t.checked = false;
    prefs.includeToken = false;
    savePrefs();
    toast("No active auth token — unlock the vault first.", { type: "error" });
    renderCaptureView(view);
    return;
  }

  const ok = await confirmDialog({
    title: "Include Auth Token in Recipes",
    message:
      "Copied and downloaded recipes will include your active WEB_AUTH_TOKEN. Only do this on a trusted device, and never share the recipe publicly. Continue?",
    confirmLabel: "Include Token",
    danger: true,
  });

  if (!ok) {
    t.checked = false;
    prefs.includeToken = false;
    savePrefs();
    renderCaptureView(view);
    return;
  }

  prefs.includeToken = true;
  savePrefs();
  toast("Auth token will be included in copied recipes. Keep them private.", {
    type: "info",
  });
  renderCaptureView(view);
}

export function handleCaptureClick(e) {
  const view = resolveView(e);

  const bookmarklet = e.target.closest("#captureBookmarklet");
  if (bookmarklet) {
    e.preventDefault();
    toast("Drag this button to your bookmarks bar, or click Copy Code.", {
      type: "info",
    });
    return;
  }

  const copyBtn = e.target.closest("[data-capture-copy]");
  if (copyBtn) {
    const key = copyBtn.dataset.captureCopy;
    const text = captureTextFor(key);
    if (text != null) copyText(text, captureLabelFor(key));
    return;
  }

  if (e.target.closest("[data-capture-test]")) {
    testCapturePopup();
    return;
  }

  if (e.target.closest("[data-capture-download-ext]")) {
    downloadExtension(view);
    return;
  }

  if (e.target.closest("[data-capture-download-json]")) {
    downloadRecipeJson();
    return;
  }

  if (e.target.closest("[data-capture-unlock]")) {
    document.querySelector('[data-sys-tab="settings"]')?.click();
  }
}
