/*
 * WS3 module bindings. These are loaded as ES modules at startup (dynamic import keeps
 * `node --check popup.js` happy from a CommonJS context while the popup itself runs as
 * `<script type="module">`). The module paths and export names below are the frozen
 * contract shared with the background service worker.
 */
let DEFAULT_SETTINGS = {};
let normalizeServerUrl = (value) => String(value || "").trim();
let getSettings = async () => ({});
let saveSettings = async () => ({});
let rememberFolderChoice = async () => ({});
let addRecentJob = async () => ({});
let updateRecentJob = async () => ({});

let buildOriginPattern = (serverUrl) => `${String(serverUrl || "").replace(/\/+$/, "")}/*`;
let hasServerPermission = async () => false;
let requestServerPermission = async () => false;
let checkHealth = async () => ({});
let fetchFolders = async () => ({ folders: [] });
let fetchTags = async () => ({ tags: [] });
let checkDuplicate = async () => ({ duplicate: false });
let quickCapture = async () => ({});
let uploadBlob = async () => ({});
let getJobStatus = async () => ({});
let uploadCookies = async () => ({});

let YOUTUBE_COOKIE_ORIGINS = [];
let ALLOWED_COOKIE_DOMAINS = [];
let formatNetscapeCookies = () => "";
let hasCookiePermission = async () => false;
let requestCookiePermission = async () => false;
let revokeCookiePermission = async () => ({});
let collectYouTubeCookies = async () => [];

const ext = globalThis.browser ?? globalThis.chrome;
const $ = (id) => document.getElementById(id);

async function loadModules() {
  const storage = await import("./lib/storage.js");
  ({
    DEFAULT_SETTINGS,
    normalizeServerUrl,
    getSettings,
    saveSettings,
    rememberFolderChoice,
    addRecentJob,
    updateRecentJob,
  } = storage);

  const api = await import("./lib/api.js");
  ({
    buildOriginPattern,
    hasServerPermission,
    requestServerPermission,
    checkHealth,
    fetchFolders,
    fetchTags,
    checkDuplicate,
    quickCapture,
    uploadBlob,
    getJobStatus,
    uploadCookies,
  } = api);

  const cookies = await import("./lib/cookies.js");
  ({
    YOUTUBE_COOKIE_ORIGINS = [],
    ALLOWED_COOKIE_DOMAINS = [],
    formatNetscapeCookies,
    hasCookiePermission,
    requestCookiePermission,
    revokeCookiePermission,
    collectYouTubeCookies,
  } = cookies);
}

const MAX_RECENT = 5;
const TERMINAL_STATUSES = new Set(["done", "failed", "cancelled"]);
const ACTIVE_STATUSES = new Set(["queued", "pending", "downloading", "processing", "uploading", "running"]);
const VALID_FORMATS = new Set(["best", "1080p", "720p", "480p", "audio"]);

let settings = {};
let health = null;
let activeTab = null;
let folders = [];
let tags = [];
let duplicateInfo = null;
let screenshotBlob = null;
let screenshotDataUrl = "";
let selectedFile = null;
let dupTimer = 0;
let statusTimer = 0;

/* ------------------------------------------------------------------ */
/* Boot                                                               */
/* ------------------------------------------------------------------ */

async function init() {
  await loadModules();
  wireEvents();
  await loadSettings();
  hydrateForm();
  await initActiveTab();
  renderRecent();
  await refreshConnection({ quiet: true });
  if (!settings.serverUrl) {
    setSettingsOpen(true);
    showStatus("Add your Media Vault server URL to get started.", "warn", true);
  } else {
    await refreshRecentStatuses();
  }
  await updateCookieStatus();
  try {
    const version = ext?.runtime?.getManifest?.().version;
    if (version) $("footerVersion").textContent = `v${version}`;
  } catch {
    /* ignore */
  }
}

async function loadSettings() {
  try {
    const stored = await getSettings();
    settings = { ...DEFAULT_SETTINGS, ...(stored || {}) };
  } catch (err) {
    settings = { ...DEFAULT_SETTINGS };
    showStatus(`Could not load saved settings: ${humanError(err)}`, "err", true);
  }
}

function hydrateForm() {
  $("serverUrlInput").value = settings.serverUrl || "";
  $("authTokenInput").value = settings.authToken || "";
  $("rememberFolderCheck").checked = settings.rememberFolder !== false;
  $("folderInput").value = settings.lastFolder || "";
  $("formatSelect").value = VALID_FORMATS.has(settings.lastFormat) ? settings.lastFormat : "best";
  updateOpenVault();
  updateServerLabel();
}

async function initActiveTab() {
  if (!ext?.tabs?.query) return;
  try {
    const tabs = await ext.tabs.query({ active: true, currentWindow: true });
    activeTab = Array.isArray(tabs) ? tabs[0] : null;
  } catch {
    activeTab = null;
  }
  if (!activeTab) return;

  const url = String(activeTab.url || "");
  if (!isHttpUrl(url)) return;

  $("urlInput").value = url;
  if (activeTab.title) {
    $("urlTitle").value = String(activeTab.title);
    $("shotTitle").value = `${activeTab.title} — screenshot`;
  }
  scheduleDuplicateCheck();
}

/* ------------------------------------------------------------------ */
/* Connections & capabilities                                          */
/* ------------------------------------------------------------------ */

async function refreshConnection({ quiet = false } = {}) {
  if (!settings.serverUrl) {
    health = null;
    setConn("setup");
    applyCapabilities();
    return null;
  }

  setConn("checking");
  try {
    const data = await checkHealth(settings);
    health = data || {};
    const ok = health.ok !== false;
    setConn(ok ? "connected" : "offline");
    applyCapabilities();
    if (ok) {
      await Promise.allSettled([refreshFolders(), refreshTags()]);
    }
    if (!quiet) {
      showStatus(
        ok ? "Connected to Media Vault." : "Server responded but reported a problem.",
        ok ? "ok" : "warn",
      );
    }
    await updateCookieStatus();
    return health;
  } catch (err) {
    health = null;
    setConn("offline");
    applyCapabilities();
    await updateCookieStatus();
    if (!quiet) showStatus(`Offline: ${humanError(err)}`, "err", true);
    return null;
  }
}

function setConn(state) {
  const pill = $("connPill");
  const table = {
    setup: ["Setup", "pill-setup"],
    checking: ["Checking…", "pill-checking"],
    connected: ["Connected", "pill-ok"],
    offline: ["Offline", "pill-err"],
  };
  const [text, cls] = table[state] || table.setup;
  pill.textContent = text;
  pill.className = `pill ${cls}`;
}

function applyCapabilities() {
  const telegramAvailable =
    health?.storage_mode === "both" && Boolean(health?.telegram_archive_enabled);
  $("telegramRow").hidden = !telegramAvailable;

  if (!$("folderInput").value.trim() && typeof health?.default_folder === "string") {
    const fallback = health.default_folder.trim();
    if (fallback) $("folderInput").value = fallback;
  }
}

function updateOpenVault() {
  const base = safeNormalize(settings.serverUrl);
  const link = $("openVaultBtn");
  if (base && isHttpUrl(base)) {
    link.href = base;
    link.classList.remove("is-disabled");
    link.removeAttribute("aria-disabled");
  } else {
    link.href = "#";
    link.classList.add("is-disabled");
    link.setAttribute("aria-disabled", "true");
  }
}

function updateServerLabel() {
  const base = safeNormalize(settings.serverUrl);
  if (!base) {
    $("serverLabel").textContent = "Companion";
    $("serverLabel").title = "";
    return;
  }
  try {
    const host = new URL(base).host;
    $("serverLabel").textContent = host;
    $("serverLabel").title = base;
  } catch {
    $("serverLabel").textContent = "Companion";
  }
}

/* ------------------------------------------------------------------ */
/* Folders & tags                                                     */
/* ------------------------------------------------------------------ */

async function refreshFolders() {
  if (!settings.serverUrl) return;
  try {
    const data = await fetchFolders(settings);
    folders = normalizeFolderList(data);
    populateFolderOptions();
  } catch {
    /* non-fatal */
  }
}

async function refreshTags() {
  if (!settings.serverUrl) return;
  try {
    const data = await fetchTags(settings);
    tags = normalizeTagList(data);
    populateTagOptions();
  } catch {
    /* non-fatal */
  }
}

function normalizeFolderList(data) {
  const raw = Array.isArray(data) ? data : Array.isArray(data?.folders) ? data.folders : [];
  return uniqueStrings(
    raw.map((entry) => {
      if (typeof entry === "string") return entry;
      if (entry && typeof entry === "object") return entry.name ?? entry.folder ?? entry.path ?? "";
      return "";
    }),
  );
}

function normalizeTagList(data) {
  const raw = Array.isArray(data) ? data : Array.isArray(data?.tags) ? data.tags : [];
  return uniqueStrings(
    raw.map((entry) => {
      if (typeof entry === "string") return entry;
      if (entry && typeof entry === "object") return entry.tag ?? entry.name ?? "";
      return "";
    }),
  );
}

function populateFolderOptions() {
  const datalist = $("extFolderList");
  datalist.textContent = "";
  for (const folder of folders) {
    const option = document.createElement("option");
    option.value = folder;
    datalist.appendChild(option);
  }

  const chips = $("folderChips");
  chips.textContent = "";
  const current = $("folderInput").value.trim();
  for (const folder of folders.slice(0, 6)) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "chip";
    btn.textContent = folder;
    btn.dataset.folder = folder;
    if (folder === current) btn.classList.add("is-active");
    chips.appendChild(btn);
  }
}

function populateTagOptions() {
  const datalist = $("extTagList");
  datalist.textContent = "";
  for (const tag of tags) {
    const option = document.createElement("option");
    option.value = tag;
    datalist.appendChild(option);
  }

  const chips = $("tagChips");
  chips.textContent = "";
  const selected = new Set(parseTags($("tagsInput").value).map((tag) => tag.toLowerCase()));
  for (const tag of tags.slice(0, 8)) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "chip";
    btn.textContent = tag;
    btn.dataset.tag = tag;
    if (selected.has(tag.toLowerCase())) btn.classList.add("is-active");
    chips.appendChild(btn);
  }
}

function markActiveFolderChip() {
  const current = $("folderInput").value.trim();
  for (const chip of $("folderChips").querySelectorAll("[data-folder]")) {
    chip.classList.toggle("is-active", chip.dataset.folder === current);
  }
}

function markActiveTagChips() {
  const selected = new Set(parseTags($("tagsInput").value).map((tag) => tag.toLowerCase()));
  for (const chip of $("tagChips").querySelectorAll("[data-tag]")) {
    chip.classList.toggle("is-active", selected.has(String(chip.dataset.tag).toLowerCase()));
  }
}

/* ------------------------------------------------------------------ */
/* Duplicate detection                                                */
/* ------------------------------------------------------------------ */

function scheduleDuplicateCheck() {
  clearTimeout(dupTimer);
  dupTimer = setTimeout(() => {
    void runDuplicateCheck($("urlInput").value.trim());
  }, 500);
}

async function runDuplicateCheck(rawUrl) {
  if (!settings.serverUrl || !isHttpUrl(rawUrl)) {
    hideDuplicate();
    return;
  }
  try {
    const data = await checkDuplicate(settings, rawUrl);
    if ($("urlInput").value.trim() !== rawUrl) return;
    if (data?.duplicate) {
      const item = data.item && typeof data.item === "object" ? data.item : {};
      const folder = typeof item.folder === "string" ? item.folder.trim() : "";
      duplicateInfo = { item, folder };
      $("dupDetail").textContent = folder ? `(${folder})` : "";
      $("dupBanner").hidden = false;
    } else {
      hideDuplicate();
    }
  } catch {
    hideDuplicate();
  }
}

function hideDuplicate() {
  duplicateInfo = null;
  $("forceReingest").checked = false;
  $("dupBanner").hidden = true;
  $("dupDetail").textContent = "";
}

/* ------------------------------------------------------------------ */
/* Capture: URL                                                       */
/* ------------------------------------------------------------------ */

async function handleUrlCapture() {
  const url = $("urlInput").value.trim();
  if (!isHttpUrl(url)) {
    showStatus("Enter a valid URL starting with http:// or https://.", "err", true);
    $("urlInput").focus();
    return;
  }

  let destination;
  try {
    destination = collectDestination();
  } catch (err) {
    showStatus(humanError(err), "err", true);
    $("folderInput").focus();
    return;
  }

  const title = $("urlTitle").value.trim();
  const formatSpec = VALID_FORMATS.has($("formatSelect").value) ? $("formatSelect").value : "best";
  const force = Boolean(duplicateInfo) && $("forceReingest").checked;

  await withBusy($("urlCaptureBtn"), async () => {
    const result = await quickCapture(settings, {
      url,
      title: title || undefined,
      folder: destination.folder,
      tags: destination.tags,
      format_spec: formatSpec,
      force,
      forward_to_telegram: destination.forward_to_telegram,
    });

    const jobId = jobIdFrom(result);
    await recordRecent({
      id: jobId || `local-${Date.now()}`,
      kind: "url",
      title: title || url,
      url,
      folder: destination.folder,
      status: "queued",
      createdAt: Date.now(),
    });
    await syncFolderMemory({ quiet: true });
    hideDuplicate();
    showStatus(
      jobId ? `Queued in ${destination.folder}. Job ${shortId(jobId)}.` : `Sent to ${destination.folder}.`,
      "ok",
    );
  });
}

/* ------------------------------------------------------------------ */
/* Capture: screenshot                                                */
/* ------------------------------------------------------------------ */

async function captureVisiblePage() {
  if (!ext?.tabs?.captureVisibleTab) {
    throw new Error("Screenshot capture is not available in this browser.");
  }
  const dataUrl = await ext.tabs.captureVisibleTab(null, { format: "png" });
  if (!dataUrl || !String(dataUrl).startsWith("data:image/")) {
    throw new Error("The browser did not return a screenshot for this page.");
  }

  const response = await fetch(dataUrl);
  screenshotBlob = await response.blob();
  screenshotDataUrl = dataUrl;

  $("shotPreview").src = dataUrl;
  $("shotPreviewWrap").hidden = false;
  $("shotUploadBtn").disabled = false;
  if (!$("shotTitle").value.trim()) {
    $("shotTitle").value = activeTab?.title
      ? `${activeTab.title} — screenshot`
      : `Screenshot ${new Date().toLocaleString()}`;
  }
  showStatus("Screenshot captured. Review it, then send it to the vault.", "ok");
}

async function handleScreenshotUpload() {
  if (!screenshotBlob) {
    showStatus("Capture the visible page first.", "warn");
    return;
  }

  let destination;
  try {
    destination = collectDestination();
  } catch (err) {
    showStatus(humanError(err), "err", true);
    $("folderInput").focus();
    return;
  }

  const title = $("shotTitle").value.trim() || "Screenshot";
  const filename = `screenshot-${timestampSlug()}.png`;

  await withBusy($("shotUploadBtn"), async () => {
    const result = await uploadBlob(settings, {
      blob: screenshotBlob,
      filename,
      title,
      folder: destination.folder,
      tags: destination.tags,
      forward_to_telegram: destination.forward_to_telegram,
    });
    const jobId = jobIdFrom(result);
    await recordRecent({
      id: jobId || `local-${Date.now()}`,
      kind: "screenshot",
      title,
      folder: destination.folder,
      status: "queued",
      createdAt: Date.now(),
    });
    await syncFolderMemory({ quiet: true });
    showStatus(
      jobId ? `Screenshot queued in ${destination.folder}.` : `Screenshot sent to ${destination.folder}.`,
      "ok",
    );
  });
}

/* ------------------------------------------------------------------ */
/* Capture: file / PDF                                                */
/* ------------------------------------------------------------------ */

function handleFileChange() {
  selectedFile = $("fileInput").files?.[0] || null;
  $("fileUploadBtn").disabled = !selectedFile;

  if (!selectedFile) {
    $("fileMeta").hidden = true;
    $("fileMeta").textContent = "";
    $("fileTitle").value = "";
    return;
  }

  const meta = [selectedFile.name, formatBytes(selectedFile.size)];
  if (selectedFile.type) meta.push(selectedFile.type);
  $("fileMeta").textContent = meta.join(" • ");
  $("fileMeta").hidden = false;

  if (!$("fileTitle").value.trim()) {
    $("fileTitle").value = stripExtension(selectedFile.name);
  }
}

async function handleFileUpload() {
  if (!selectedFile) {
    showStatus("Choose a file to upload first.", "warn");
    return;
  }

  let destination;
  try {
    destination = collectDestination();
  } catch (err) {
    showStatus(humanError(err), "err", true);
    $("folderInput").focus();
    return;
  }

  const title = $("fileTitle").value.trim() || stripExtension(selectedFile.name);
  const file = selectedFile;

  await withBusy($("fileUploadBtn"), async () => {
    const result = await uploadBlob(settings, {
      blob: file,
      filename: file.name,
      title,
      folder: destination.folder,
      tags: destination.tags,
      forward_to_telegram: destination.forward_to_telegram,
    });
    const jobId = jobIdFrom(result);
    await recordRecent({
      id: jobId || `local-${Date.now()}`,
      kind: "file",
      title,
      folder: destination.folder,
      status: "queued",
      createdAt: Date.now(),
    });
    await syncFolderMemory({ quiet: true });
    showStatus(
      jobId ? `Uploaded ${file.name} to ${destination.folder}.` : `Sent ${file.name} to ${destination.folder}.`,
      "ok",
    );
  });
}

function collectDestination() {
  let folder = $("folderInput").value.trim();
  if (!folder && typeof health?.default_folder === "string") {
    folder = health.default_folder.trim();
  }
  if (!folder) throw new Error("Choose a destination folder first.");
  if (!$("folderInput").value.trim()) $("folderInput").value = folder;

  return {
    folder,
    tags: parseTags($("tagsInput").value),
    forward_to_telegram: !$("telegramRow").hidden && $("telegramForward").checked,
  };
}

/* ------------------------------------------------------------------ */
/* Settings                                                           */
/* ------------------------------------------------------------------ */

async function handleSaveSettings(event) {
  event.preventDefault();

  const normalized = safeNormalize($("serverUrlInput").value);
  if (!isHttpUrl(normalized)) {
    showStatus("Enter a valid server URL starting with http:// or https://.", "err", true);
    $("serverUrlInput").focus();
    return;
  }

  const authToken = $("authTokenInput").value.trim();
  const rememberFolder = $("rememberFolderCheck").checked;
  const candidate = { ...settings, serverUrl: normalized, authToken, rememberFolder };

  await withBusy($("saveSettingsBtn"), async () => {
    const requested = await requestServerPermission(normalized);
    if (!isGranted(requested) && !(await hasServerPermissionSafe(normalized))) {
      throw new Error(
        "Host permission for this server was denied. The extension needs it to reach your vault.",
      );
    }

    const nextHealth = await checkHealth(candidate);
    await saveSettings({
      serverUrl: normalized,
      authToken,
      rememberFolder,
    });

    settings = { ...settings, serverUrl: normalized, authToken, rememberFolder };
    health = nextHealth || health;
    $("serverUrlInput").value = normalized;
    setConn(health?.ok === false ? "offline" : "connected");
    applyCapabilities();
    updateOpenVault();
    updateServerLabel();

    await Promise.allSettled([refreshFolders(), refreshTags()]);
    if (settings.rememberFolder) {
      await syncFolderMemory({ quiet: true });
    } else {
      notifyMenus();
    }
    await updateCookieStatus();

    showStatus(
      health?.ytdlp_cookies_configured
        ? "Connected. YouTube cookies are already loaded in the vault."
        : "Connected. Tip: sync YouTube cookies below for sign-in-required videos.",
      "ok",
    );
  });
}

async function hasServerPermissionSafe(serverUrl) {
  try {
    if (await hasServerPermission(serverUrl)) return true;
  } catch {
    /* fall through to a direct permissions check */
  }
  try {
    const pattern = buildOriginPattern(serverUrl);
    return Boolean(await ext.permissions.contains({ origins: [pattern] }));
  } catch {
    return false;
  }
}

function isGranted(value) {
  if (value === undefined || value === null) return true;
  if (typeof value === "boolean") return value;
  if (typeof value === "object" && "granted" in value) return Boolean(value.granted);
  return Boolean(value);
}

async function syncFolderMemory({ quiet = false } = {}) {
  if (!settings.rememberFolder) return;
  const folder = $("folderInput").value.trim();
  if (!folder) return;

  try {
    await rememberFolderChoice(folder);
  } catch (err) {
    if (!quiet) showStatus(`Could not remember folder: ${humanError(err)}`, "warn");
  }
  settings.lastFolder = folder;
  markActiveFolderChip();
  notifyMenus();
}

function notifyMenus() {
  try {
    const pending = ext.runtime.sendMessage({ type: "MV_REFRESH_MENUS" });
    if (pending && typeof pending.catch === "function") pending.catch(() => {});
  } catch {
    /* background may be waking up; harmless */
  }
}

/* ------------------------------------------------------------------ */
/* YouTube cookie sync                                                */
/* ------------------------------------------------------------------ */

async function updateCookieStatus() {
  const granted = await cookiePermissionGranted();

  const pill = $("cookiePermPill");
  pill.textContent = granted ? "Granted" : "Not granted";
  pill.className = `pill ${granted ? "pill-granted" : "pill-muted"}`;

  $("revokeCookiesBtn").hidden = !granted;
  $("cookieExtStatus").textContent = granted
    ? "YouTube / Google access granted"
    : "No YouTube / Google access";

  const scopes = Array.isArray(YOUTUBE_COOKIE_ORIGINS) ? YOUTUBE_COOKIE_ORIGINS : [];
  $("cookieExtStatus").title = scopes.join("\n");

  if (!health) {
    $("cookieVaultStatus").textContent = "Unknown";
  } else {
    $("cookieVaultStatus").textContent = health.ytdlp_cookies_configured
      ? "Cookies loaded"
      : "No cookies yet";
  }
}

async function cookiePermissionGranted() {
  try {
    return Boolean(await hasCookiePermission());
  } catch {
    return false;
  }
}

async function handleSyncCookies() {
  if (!settings.serverUrl) {
    showStatus("Save your server URL before syncing cookies.", "warn", true);
    setSettingsOpen(true);
    return;
  }

  await withBusy($("syncCookiesBtn"), async () => {
    const scopes = Array.isArray(YOUTUBE_COOKIE_ORIGINS) ? YOUTUBE_COOKIE_ORIGINS : [];
    const requestResult = await requestCookiePermission(scopes);
    const granted = await cookiePermissionGranted();

    if (!granted) {
      throw new Error(
        requestResult === false
          ? "Cookie permission was denied. You can retry any time."
          : "YouTube cookie access is still not granted.",
      );
    }

    const collected = await collectYouTubeCookies(scopes.length ? scopes : undefined);
    const cookies = Array.isArray(collected) ? collected : [];
    if (!cookies.length) {
      throw new Error(
        "No YouTube cookies found. Sign in to youtube.com in this browser, then try again.",
      );
    }

    const formatted = formatNetscapeCookies(cookies);
    const cookiesText = typeof formatted === "string" ? formatted : formatted?.text;
    const syncedCount = typeof formatted === "object" && formatted ? Number(formatted.count || 0) : 0;
    const domains =
      typeof formatted === "object" && Array.isArray(formatted?.domains)
        ? formatted.domains
        : summarizeCookieDomains(filterAllowedCookies(cookies));
    if (!cookiesText || !String(cookiesText).trim() || syncedCount === 0) {
      throw new Error(
        "No valid YouTube/Google cookies found to export. Sign in to youtube.com in this browser, then try again.",
      );
    }

    const response = await uploadCookies(settings, cookiesText);
    health = { ...(health || {}), ytdlp_cookies_configured: response?.ytdlp_cookies_configured !== false };
    await updateCookieStatus();

    $("cookieDetail").textContent = `Synced ${syncedCount} cookie${
      syncedCount === 1 ? "" : "s"
    }${domains.length ? ` • ${domains.join(", ")}` : ""}`;
    $("cookieDetail").hidden = false;

    showStatus(
      `Synced ${syncedCount} YouTube cookie${syncedCount === 1 ? "" : "s"} to Media Vault.`,
      "ok",
    );
  });
}

async function handleRevokeCookies() {
  await withBusy($("revokeCookiesBtn"), async () => {
    const scopes = Array.isArray(YOUTUBE_COOKIE_ORIGINS) ? YOUTUBE_COOKIE_ORIGINS : [];
    await revokeCookiePermission(scopes);
    $("cookieDetail").hidden = true;
    await updateCookieStatus();
    showStatus("Cookie access revoked.", "ok");
  });
}

function filterAllowedCookies(cookies) {
  const allowed = new Set(
    (Array.isArray(ALLOWED_COOKIE_DOMAINS) ? ALLOWED_COOKIE_DOMAINS : [])
      .map((domain) => String(domain).toLowerCase().replace(/^\./, "").trim())
      .filter(Boolean),
  );
  if (!allowed.size) return [];

  return cookies.filter((cookie) => {
    const raw = cookie?.domain ?? cookie?.host ?? "";
    const domain = String(raw).toLowerCase().replace(/^\./, "");
    if (!domain) return false;
    for (const allowedDomain of allowed) {
      if (domain === allowedDomain || domain.endsWith(`.${allowedDomain}`)) return true;
    }
    return false;
  });
}

function summarizeCookieDomains(cookies) {
  const domains = new Set();
  for (const cookie of cookies) {
    const raw = String(cookie?.domain ?? cookie?.host ?? "").replace(/^\./, "").trim();
    if (!raw) continue;
    domains.add(raw);
    if (domains.size >= 4) break;
  }
  return [...domains];
}

/* ------------------------------------------------------------------ */
/* Recent captures                                                    */
/* ------------------------------------------------------------------ */

async function recordRecent(entry) {
  const normalized = {
    jobId: entry.jobId ?? entry.id ?? "",
    id: entry.id ?? entry.jobId ?? "",
    kind: entry.kind || "url",
    title: entry.title || "",
    url: entry.url || "",
    folder: entry.folder || "",
    status: entry.status || "queued",
    createdAt:
      typeof entry.createdAt === "number"
        ? new Date(entry.createdAt).toISOString()
        : entry.createdAt || new Date().toISOString(),
  };
  try {
    await addRecentJob(normalized);
  } catch (err) {
    console.warn("Media Vault: could not persist recent job", err);
  }

  const existing = Array.isArray(settings.recentJobs) ? settings.recentJobs : [];
  const id = normalized.jobId || normalized.id;
  settings.recentJobs = [normalized, ...existing.filter((job) => (job.jobId || job.id) !== id)].slice(
    0,
    8,
  );
  renderRecent();
}

let renderToken = 0;

function renderRecent() {
  const token = ++renderToken;
  const list = $("recentList");
  list.textContent = "";
  const jobs = (Array.isArray(settings.recentJobs) ? settings.recentJobs : []).slice(0, MAX_RECENT);
  $("recentEmpty").hidden = jobs.length > 0;

  for (const job of jobs) {
    const item = document.createElement("li");
    item.className = "recent-item";

    const main = document.createElement("div");
    main.className = "recent-main";

    const title = document.createElement("span");
    title.className = "recent-title";
    title.textContent = String(job.title || job.url || "Capture");
    title.title = title.textContent;
    main.appendChild(title);

    const metaBits = [];
    if (job.folder) metaBits.push(String(job.folder));
    if (job.kind) metaBits.push(kindLabel(job.kind));
    const when = timeAgo(job.createdAt || job.created_at);
    if (when) metaBits.push(when);

    if (metaBits.length) {
      const meta = document.createElement("span");
      meta.className = "recent-meta";
      meta.textContent = metaBits.join(" • ");
      meta.title = meta.textContent;
      main.appendChild(meta);
    }

    const badge = document.createElement("span");
    const cls = statusClass(job.status);
    badge.className = `badge badge-${cls}`;
    badge.textContent = statusLabel(job.status);

    item.append(main, badge);
    list.appendChild(item);
  }

  // Guard against a stale render completing after a newer one (no-op placeholder).
  if (token !== renderToken) return;
}

async function refreshRecentStatuses({ announce = false } = {}) {
  if (!settings.serverUrl) {
    if (announce) showStatus("Save your server URL first.", "warn");
    return;
  }

  const jobs = (Array.isArray(settings.recentJobs) ? settings.recentJobs : []).slice(0, MAX_RECENT);
  let changed = false;

  for (const job of jobs) {
    const id = job.id || job.jobId;
    if (!id || TERMINAL_STATUSES.has(job.status) || String(id).startsWith("local-")) continue;
    try {
      const data = await getJobStatus(settings, id);
      const status = data?.status || data?.job?.status;
      if (status && status !== job.status) {
        job.status = status;
        changed = true;
        await saveRecentStatus(id, { status });
      }
      const remoteTitle = data?.title || data?.job?.title;
      if (remoteTitle && remoteTitle !== job.title) {
        job.title = remoteTitle;
        changed = true;
      }
    } catch {
      /* keep the last known status */
    }
  }

  if (changed) renderRecent();
  if (announce) showStatus(changed ? "Recent captures updated." : "Recent captures are up to date.", "ok");
}

async function saveRecentStatus(id, patch) {
  try {
    await updateRecentJob(id, patch);
  } catch {
    try {
      await updateRecentJob({ id, ...patch });
    } catch {
      /* recent list is best-effort */
    }
  }
}

/* ------------------------------------------------------------------ */
/* UI helpers                                                         */
/* ------------------------------------------------------------------ */

function wireEvents() {
  $("settingsToggle").addEventListener("click", () => {
    setSettingsOpen($("settingsPanel").hidden);
  });
  $("closeSettingsBtn").addEventListener("click", () => setSettingsOpen(false));
  $("settingsForm").addEventListener("submit", handleSaveSettings);

  $("toggleTokenBtn").addEventListener("click", () => {
    const input = $("authTokenInput");
    const show = input.type === "password";
    input.type = show ? "text" : "password";
    $("toggleTokenBtn").textContent = show ? "Hide" : "Show";
    $("toggleTokenBtn").setAttribute("aria-pressed", String(show));
  });

  $("syncCookiesBtn").addEventListener("click", handleSyncCookies);
  $("revokeCookiesBtn").addEventListener("click", handleRevokeCookies);

  for (const tab of document.querySelectorAll(".tab")) {
    tab.addEventListener("click", () => switchTab(tab.dataset.tab));
  }

  $("urlInput").addEventListener("input", scheduleDuplicateCheck);
  $("urlInput").addEventListener("change", scheduleDuplicateCheck);
  $("urlCaptureBtn").addEventListener("click", handleUrlCapture);

  $("captureShotBtn").addEventListener("click", () => {
    void withBusy($("captureShotBtn"), captureVisiblePage);
  });
  $("shotUploadBtn").addEventListener("click", handleScreenshotUpload);

  $("fileInput").addEventListener("change", handleFileChange);
  $("fileUploadBtn").addEventListener("click", handleFileUpload);

  $("folderInput").addEventListener("change", () => {
    markActiveFolderChip();
    void syncFolderMemory();
  });
  $("folderChips").addEventListener("click", (event) => {
    const chip = event.target.closest("button[data-folder]");
    if (!chip) return;
    $("folderInput").value = chip.dataset.folder || "";
    markActiveFolderChip();
    void syncFolderMemory();
  });

  $("tagsInput").addEventListener("input", markActiveTagChips);
  $("tagChips").addEventListener("click", (event) => {
    const chip = event.target.closest("button[data-tag]");
    if (!chip) return;
    const tag = chip.dataset.tag || "";
    const current = parseTags($("tagsInput").value);
    if (!current.some((entry) => entry.toLowerCase() === tag.toLowerCase())) {
      current.push(tag);
      $("tagsInput").value = current.join(", ");
    }
    markActiveTagChips();
  });

  $("formatSelect").addEventListener("change", () => {
    const value = $("formatSelect").value;
    if (!VALID_FORMATS.has(value)) return;
    settings.lastFormat = value;
    saveSettings({ lastFormat: value }).catch(() => {});
  });

  $("refreshRecentBtn").addEventListener("click", () => {
    void withBusy($("refreshRecentBtn"), () => refreshRecentStatuses({ announce: true }));
  });

  $("openVaultBtn").addEventListener("click", (event) => {
    if ($("openVaultBtn").classList.contains("is-disabled")) event.preventDefault();
  });
}

function switchTab(name) {
  for (const tab of document.querySelectorAll(".tab")) {
    const isActive = tab.dataset.tab === name;
    tab.classList.toggle("is-active", isActive);
    tab.setAttribute("aria-selected", String(isActive));
  }
  const panels = { url: "tabUrl", shot: "tabShot", file: "tabFile" };
  for (const [key, id] of Object.entries(panels)) {
    $(id).hidden = key !== name;
  }
}

function setSettingsOpen(open) {
  $("settingsPanel").hidden = !open;
  $("settingsToggle").setAttribute("aria-expanded", String(open));
  if (open) $("serverUrlInput").focus();
}

function showStatus(message, type = "info", sticky = false) {
  const banner = $("statusBanner");
  if (!banner) return;
  banner.textContent = String(message);
  banner.className = type && type !== "info" ? `banner banner-${type}` : "banner";
  banner.hidden = false;
  clearTimeout(statusTimer);
  if (!sticky) {
    statusTimer = setTimeout(() => {
      banner.hidden = true;
    }, 6000);
  }
}

async function withBusy(button, task) {
  if (!button || button.disabled) return;
  const original = button.textContent;
  const label = button.dataset.busyLabel || "Working…";
  button.disabled = true;
  button.textContent = label;
  try {
    return await task();
  } catch (err) {
    showStatus(humanError(err), "err", true);
    return undefined;
  } finally {
    button.disabled = false;
    button.textContent = original;
  }
}

/* ------------------------------------------------------------------ */
/* Misc utilities                                                     */
/* ------------------------------------------------------------------ */

function safeNormalize(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  try {
    return normalizeServerUrl(raw) || "";
  } catch {
    return raw.replace(/\/+$/, "");
  }
}

function isHttpUrl(value) {
  const raw = String(value || "").trim();
  if (!raw.startsWith("http://") && !raw.startsWith("https://")) return false;
  try {
    const parsed = new URL(raw);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function parseTags(value) {
  return uniqueStrings(
    String(value || "")
      .split(/[,\n]/)
      .map((tag) => tag.trim().replace(/^#+/, "").trim())
      .filter(Boolean),
  );
}

function uniqueStrings(values) {
  const seen = new Set();
  const result = [];
  for (const value of values) {
    const text = String(value || "").trim();
    if (!text) continue;
    const key = text.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(text);
  }
  return result;
}

function jobIdFrom(result) {
  if (!result) return "";
  if (typeof result === "string") return result.trim();
  const first = Array.isArray(result.jobs) ? result.jobs[0] : null;
  return String(
    result.job_id || result.jobId || result.id || first?.job_id || first?.jobId || first?.id || "",
  ).trim();
}

function shortId(id) {
  const text = String(id || "");
  return text.length > 10 ? `${text.slice(0, 8)}…` : text;
}

function stripExtension(name) {
  const text = String(name || "").trim();
  const dot = text.lastIndexOf(".");
  return dot > 0 ? text.slice(0, dot) : text;
}

function formatBytes(bytes) {
  const size = Number(bytes);
  if (!Number.isFinite(size) || size < 0) return "Unknown size";
  if (size < 1024) return `${size} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = size / 1024;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[index]}`;
}

function timeAgo(value) {
  if (!value) return "";
  const stamp = typeof value === "number" ? value : Date.parse(String(value));
  if (!Number.isFinite(stamp)) return "";
  const diff = Date.now() - stamp;
  if (diff < 45_000) return "just now";
  const minutes = Math.round(diff / 60_000);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  if (days < 30) return `${days}d ago`;
  try {
    return new Date(stamp).toLocaleDateString();
  } catch {
    return "";
  }
}

function timestampSlug() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function statusLabel(status) {
  const value = String(status || "").toLowerCase();
  if (!value) return "Queued";
  if (ACTIVE_STATUSES.has(value)) return value === "queued" || value === "pending" ? "Queued" : "Working";
  if (value === "done") return "Done";
  if (value === "failed") return "Failed";
  if (value === "cancelled") return "Cancelled";
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function statusClass(status) {
  const value = String(status || "").toLowerCase();
  if (ACTIVE_STATUSES.has(value)) {
    return value === "queued" || value === "pending" ? "queued" : "active";
  }
  if (value === "done" || value === "failed" || value === "cancelled") return value;
  return "unknown";
}

function kindLabel(kind) {
  const value = String(kind || "").toLowerCase();
  if (value === "url") return "URL";
  if (value === "screenshot" || value === "shot") return "Screenshot";
  if (value === "file" || value === "upload") return "File";
  return value ? value.charAt(0).toUpperCase() + value.slice(1) : "Capture";
}

function humanError(err) {
  if (!err) return "Something went wrong.";
  const message = typeof err === "string" ? err : err.message || String(err);
  const trimmed = String(message).trim();
  return trimmed.length > 220 ? `${trimmed.slice(0, 217)}…` : trimmed;
}

init().catch((err) => {
  showStatus(`Companion failed to start: ${humanError(err)}`, "err", true);
  console.error("Media Vault Companion popup failed:", err);
});
