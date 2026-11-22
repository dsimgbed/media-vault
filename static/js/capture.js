import { state } from "/static/js/state.js";
import { apiFetch, uploadWithProgress } from "/static/js/api.js";
import { $, debounce } from "/static/js/utils.js";

const params = new URLSearchParams(window.location.search);

const qUrl = params.get("url") || "";
const qTitle = params.get("title") || "";
const qFolder = params.get("folder") || "";
const qTags = params.get("tags") || "";
const qFormat = params.get("format") || "";
const qAuto = params.get("auto") === "1";
const qAutoclose = params.get("autoclose") === "1";
const qSource = params.get("source") || "";

const ALLOWED_FORMATS = ["best", "1080p", "720p", "480p", "audio"];

let token = localStorage.getItem("vault_token") || "";
state.token = token;

let health = null;
let folders = [];
let tags = [];
let selectedFile = null;
let duplicateInfo = null;
let closeTimer = null;
let cancelledClose = false;

function validateUrl(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return null;
  try {
    const u = new URL(s);
    if (u.protocol === "http:" || u.protocol === "https:") {
      return u.href;
    }
  } catch (_) {}
  return null;
}

function isLocked() {
  return Boolean(health && health.auth_required && !token);
}

function showLocked() {
  const lock = $("lockCard");
  const app = $("appCard");
  if (lock) lock.hidden = false;
  if (app) app.hidden = true;
}

function showApp() {
  const lock = $("lockCard");
  const app = $("appCard");
  if (lock) lock.hidden = true;
  if (app) app.hidden = false;
}

function showStatus(message, type) {
  const el = $("statusMessage");
  if (!el) return;
  el.hidden = false;
  el.textContent = String(message ?? "");
  el.classList.remove("is-err", "is-ok");
  if (type === "err") el.classList.add("is-err");
  if (type === "ok") el.classList.add("is-ok");
}

function clearStatus() {
  const el = $("statusMessage");
  if (el) {
    el.hidden = true;
    el.textContent = "";
  }
}

function handleMaybeLocked(err) {
  const msg = String(err?.message || "");
  if (/unauthorized/i.test(msg)) {
    token = "";
    state.token = "";
    try { localStorage.removeItem("vault_token"); } catch (_) {}
    showLocked();
    return true;
  }
  return false;
}

function persistFolderIfRemembered() {
  const remember = $("rememberFolderCheck");
  const folderInput = $("folderInput");
  if (remember && remember.checked && folderInput) {
    try { localStorage.setItem("vault_capture_folder", folderInput.value.trim()); } catch (_) {}
    try { localStorage.setItem("vault_capture_remember_folder", "true"); } catch (_) {}
  } else if (remember) {
    try { localStorage.setItem("vault_capture_remember_folder", "false"); } catch (_) {}
  }
}

function persistFormat() {
  const sel = $("formatSelect");
  if (sel) {
    try { localStorage.setItem("vault_capture_format", sel.value); } catch (_) {}
  }
}

function renderFolderChips() {
  const container = $("folderChips");
  if (!container) return;
  container.replaceChildren();
  const top = folders.slice(0, 8);
  for (const f of top) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "chip-btn";
    btn.textContent = f;
    btn.addEventListener("click", () => {
      const fi = $("folderInput");
      if (fi) fi.value = f;
      persistFolderIfRemembered();
    });
    container.appendChild(btn);
  }
}

function toggleTag(tag) {
  const input = $("tagsInput");
  if (!input) return;
  const tokens = input.value.split(/[\s,]+/).map((s) => s.trim()).filter(Boolean);
  const needle = String(tag).toLowerCase();
  const idx = tokens.findIndex((t) => {
    const norm = t.toLowerCase().replace(/^#/, "");
    return norm === needle.replace(/^#/, "");
  });
  if (idx >= 0) {
    tokens.splice(idx, 1);
  } else {
    tokens.push(`#${tag}`);
  }
  input.value = tokens.join(" ");
}

function renderTagChips() {
  const container = $("tagChips");
  if (!container) return;
  container.replaceChildren();
  const top = tags.slice(0, 12);
  for (const t of top) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "chip-btn";
    btn.textContent = `#${t}`;
    btn.addEventListener("click", () => toggleTag(t));
    container.appendChild(btn);
  }
}

function renderDuplicate(data) {
  const badge = $("duplicateBadge");
  if (!badge) return;
  if (!data) {
    badge.hidden = true;
    return;
  }
  badge.hidden = false;
  if (data.duplicate) {
    const folder = (data.item && data.item.folder) ? data.item.folder : (data.folder || "Vault");
    badge.textContent = `Already in Vault (Folder: ${folder})`;
    badge.className = "badge badge-warn";
  } else {
    badge.textContent = "New URL";
    badge.className = "badge badge-ok";
  }
}

async function checkDuplicate(url) {
  const valid = validateUrl(url);
  if (!valid) {
    renderDuplicate(null);
    return;
  }
  if (isLocked()) return;
  try {
    const data = await apiFetch("/api/ingest/check-duplicate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: valid }),
    });
    duplicateInfo = data;
    renderDuplicate(data);
  } catch (err) {
    if (!handleMaybeLocked(err)) {
      // ignore
    }
  }
}

const debouncedDuplicate = debounce(() => {
  const input = $("urlInput");
  const val = input ? input.value.trim() : "";
  const btn = $("submitLinkBtn");
  if (!val) {
    renderDuplicate(null);
    if (btn) btn.disabled = false;
    clearStatus();
    return;
  }
  if (!validateUrl(val)) {
    renderDuplicate(null);
    showStatus("URL must use http:// or https://", "err");
    if (btn) btn.disabled = true;
    return;
  }
  if (btn) btn.disabled = false;
  clearStatus();
  checkDuplicate(val);
}, 300);

async function loadHealth() {
  const res = await fetch("/api/health");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  health = await res.json();
  state.authRequired = Boolean(health.auth_required);
  state.storageMode = health.storage_mode || "telegram";
  state.defaultFolder = health.default_folder || "General";
  state.telegramArchiveEnabled = Boolean(health.telegram_archive_enabled);

  const tgRow = $("tgForwardRow");
  const tgCheck = $("tgForwardCheck");
  const showTg = state.storageMode === "both" && Boolean(health.telegram_archive_enabled);
  if (tgRow) tgRow.hidden = !showTg;
  if (tgCheck) tgCheck.checked = showTg;
}

async function loadFolders() {
  const data = await apiFetch("/api/folders");
  folders = Array.isArray(data.folders) ? data.folders : [];
  const dl = $("folderList");
  if (dl) {
    dl.replaceChildren();
    for (const f of folders) {
      const opt = document.createElement("option");
      opt.value = f;
      dl.appendChild(opt);
    }
  }
  const folderInput = $("folderInput");
  if (folderInput && !folderInput.value) {
    let initial = qFolder;
    if (!initial) {
      const remember = localStorage.getItem("vault_capture_remember_folder");
      const rememberedOn = remember === null ? true : remember !== "false";
      if (rememberedOn) {
        const remembered = localStorage.getItem("vault_capture_folder");
        if (remembered) initial = remembered;
      }
    }
    if (!initial) initial = (data.default_folder || state.defaultFolder || "General");
    folderInput.value = initial;
  }
  renderFolderChips();
}

async function loadTags() {
  const data = await apiFetch("/api/tags");
  const raw = Array.isArray(data.tags) ? data.tags : [];
  tags = raw.map((t) => (typeof t === "string" ? t : (t && t.name) ? t.name : "")).filter(Boolean);
  const dl = $("tagList");
  if (dl) {
    dl.replaceChildren();
    for (const t of tags) {
      const opt = document.createElement("option");
      opt.value = `#${t}`;
      dl.appendChild(opt);
    }
  }
  renderTagChips();
  const tagsInput = $("tagsInput");
  if (tagsInput && qTags && !tagsInput.value) {
    tagsInput.value = qTags;
  }
}

function setTab(tab) {
  const linkBtn = $("tabLink");
  const fileBtn = $("tabFile");
  const panelLink = $("panelLink");
  const panelFile = $("panelFile");
  const submitLink = $("submitLinkBtn");
  const submitFile = $("submitFileBtn");
  const isLink = tab === "link";
  if (linkBtn) {
    linkBtn.classList.toggle("is-active", isLink);
    linkBtn.setAttribute("aria-selected", String(isLink));
  }
  if (fileBtn) {
    fileBtn.classList.toggle("is-active", !isLink);
    fileBtn.setAttribute("aria-selected", String(!isLink));
  }
  if (panelLink) panelLink.hidden = !isLink;
  if (panelFile) panelFile.hidden = isLink;
  if (submitLink) submitLink.hidden = !isLink;
  if (submitFile) submitFile.hidden = isLink;
}

function safeOpenHref(data) {
  let href = "/?mode=uploader";
  if (data && data.media_id) {
    const id = String(data.media_id);
    if (/^[A-Za-z0-9_-]+$/.test(id)) {
      href = `/#media=${id}`;
    }
  }
  return href;
}

function showResult(data) {
  const card = $("resultCard");
  if (card) card.hidden = false;
  const summary = $("resultSummary");
  if (summary) {
    summary.textContent = (data && data.summary) ? data.summary : (data && data.status === "duplicate" ? "Already in Vault" : "Queued");
  }
  const link = $("openInVaultResult");
  if (link) {
    link.href = safeOpenHref(data);
    link.target = "_blank";
    link.rel = "noopener noreferrer";
  }
  if (card) {
    card.classList.remove("is-ok", "is-dup");
    card.classList.add(data && data.status === "duplicate" ? "is-dup" : "is-ok");
  }
  clearStatus();

  const autocloseCheck = $("autocloseCheck");
  const shouldClose = Boolean(autocloseCheck && autocloseCheck.checked && (window.opener || qAutoclose));
  if (shouldClose) {
    startCountdown();
  }
}

function startCountdown() {
  cancelledClose = false;
  const cancelBtn = $("cancelCloseBtn");
  const countdownEl = $("countdown");
  if (cancelBtn) {
    cancelBtn.hidden = false;
    cancelBtn.dataset.cancelled = "";
  }
  const start = Date.now();
  const duration = 1500;
  if (countdownEl) countdownEl.textContent = "Closing in 1.5s...";
  const interval = setInterval(() => {
    if (cancelledClose) {
      clearInterval(interval);
      return;
    }
    const elapsed = Date.now() - start;
    const left = Math.max(0, duration - elapsed);
    if (countdownEl) {
      countdownEl.textContent = left > 0 ? `Closing in ${(left / 1000).toFixed(1)}s...` : "";
    }
    if (left <= 0) {
      clearInterval(interval);
      if (!cancelledClose) {
        try { window.close(); } catch (_) {}
      }
    }
  }, 100);
  if (cancelBtn) {
    cancelBtn.onclick = () => {
      cancelledClose = true;
      clearInterval(interval);
      cancelBtn.hidden = true;
      if (countdownEl) countdownEl.textContent = "Auto-close cancelled.";
    };
  }
}

async function submitLink() {
  const urlInput = $("urlInput");
  const url = validateUrl(urlInput ? urlInput.value : "");
  if (!url) {
    showStatus("Enter a valid http(s) URL", "err");
    return;
  }
  if (isLocked()) {
    showLocked();
    return;
  }
  const btn = $("submitLinkBtn");
  if (btn) btn.disabled = true;
  clearStatus();
  try {
    const titleInput = $("titleInput");
    const folderInput = $("folderInput");
    const tagsInput = $("tagsInput");
    const formatSelect = $("formatSelect");
    const tgCheck = $("tgForwardCheck");
    const forceCheck = $("forceReingest");
    const body = {
      url,
      title: (titleInput && titleInput.value.trim()) ? titleInput.value.trim() : null,
      folder: (folderInput && folderInput.value.trim()) ? folderInput.value.trim() : null,
      tags: (tagsInput && tagsInput.value.trim()) ? tagsInput.value.trim() : null,
      format_spec: formatSelect ? formatSelect.value : "best",
      forward_to_telegram: tgCheck ? Boolean(tgCheck.checked) : true,
      force: forceCheck ? Boolean(forceCheck.checked) : false,
      source: qSource || "bookmarklet",
    };
    const data = await apiFetch("/api/capture/quick", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    persistFolderIfRemembered();
    persistFormat();
    showResult(data);
  } catch (err) {
    if (!handleMaybeLocked(err)) {
      showStatus(err && err.message ? err.message : "Failed to capture", "err");
    }
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function submitFile() {
  const fileInput = $("fileInput");
  if (!fileInput || !fileInput.files || !fileInput.files[0]) {
    if (!selectedFile) {
      showStatus("Choose a file first", "err");
      return;
    }
  }
  const file = selectedFile || (fileInput && fileInput.files ? fileInput.files[0] : null);
  if (!file) {
    showStatus("Choose a file first", "err");
    return;
  }
  if (isLocked()) {
    showLocked();
    return;
  }
  const btn = $("submitFileBtn");
  if (btn) btn.disabled = true;
  clearStatus();
  const progress = $("uploadProgress");
  const bar = $("uploadProgressBar");
  const text = $("uploadProgressText");
  if (progress) progress.hidden = false;
  if (bar) bar.style.width = "0%";
  if (text) text.textContent = "0%";

  try {
    const folderInput = $("folderInput");
    const tagsInput = $("tagsInput");
    const fileTitle = $("fileTitle");
    const tgCheck = $("tgForwardCheck");
    const forceCheck = $("forceReingest");
    const form = new FormData();
    form.append("file", file, file.name);
    form.append("title", (fileTitle && fileTitle.value.trim()) ? fileTitle.value.trim() : file.name);
    form.append("folder", (folderInput && folderInput.value.trim()) ? folderInput.value.trim() : "");
    form.append("tags", (tagsInput && tagsInput.value.trim()) ? tagsInput.value.trim() : "");
    form.append("forward_to_telegram", tgCheck && tgCheck.checked ? "true" : "false");
    form.append("force", forceCheck && forceCheck.checked ? "true" : "false");

    const data = await uploadWithProgress("/api/ingest/upload", form, ({ loaded, total, pct }) => {
      if (bar) bar.style.width = `${pct}%`;
      if (text) text.textContent = `${pct}% (${loaded}/${total})`;
    });
    if (progress) progress.hidden = true;
    persistFolderIfRemembered();
    showResult({
      status: "queued",
      summary: `Queued for ${(folderInput && folderInput.value.trim()) || "Vault"}: ${file.name}`,
      ...data,
    });
  } catch (err) {
    if (progress) progress.hidden = true;
    if (!handleMaybeLocked(err)) {
      showStatus(err && err.message ? err.message : "Upload failed", "err");
    }
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function unlock() {
  const input = $("lockPassword");
  const val = input ? input.value.trim() : "";
  if (!val) {
    const err = $("lockError");
    if (err) { err.hidden = false; err.textContent = "Enter a token"; }
    return;
  }
  token = val;
  state.token = val;
  try { localStorage.setItem("vault_token", val); } catch (_) {}
  const errEl = $("lockError");
  if (errEl) errEl.hidden = true;
  try {
    await loadHealth();
    if (health && health.auth_required && !token) {
      throw new Error("Unauthorized");
    }
    await Promise.all([loadFolders(), loadTags()]);
    showApp();
    await postLoad();
  } catch (e) {
    token = "";
    state.token = "";
    try { localStorage.removeItem("vault_token"); } catch (_) {}
    if (errEl) {
      errEl.hidden = false;
      errEl.textContent = "Invalid token";
    }
    showLocked();
  }
}

async function postLoad() {
  // prefill query url/title
  const urlInput = $("urlInput");
  const titleInput = $("titleInput");
  if (urlInput && qUrl && !urlInput.value) urlInput.value = qUrl;
  if (titleInput && qTitle && !titleInput.value) titleInput.value = qTitle;

  // format
  const formatSelect = $("formatSelect");
  if (formatSelect) {
    let fmt = qFormat || localStorage.getItem("vault_capture_format") || "best";
    if (!ALLOWED_FORMATS.includes(fmt)) fmt = "best";
    formatSelect.value = fmt;
  }

  // remember folder
  const rememberCheck = $("rememberFolderCheck");
  if (rememberCheck) {
    const stored = localStorage.getItem("vault_capture_remember_folder");
    const on = stored === null ? true : stored !== "false";
    rememberCheck.checked = on;
  }

  // autoclose
  const autocloseCheck = $("autocloseCheck");
  if (autocloseCheck) {
    let ac = localStorage.getItem("vault_capture_autoclose") === "true";
    if (qAutoclose) ac = true;
    autocloseCheck.checked = ac;
  }

  // initial URL validation + duplicate check
  if (urlInput && urlInput.value.trim()) {
    const validInit = validateUrl(urlInput.value);
    const submitBtn = $("submitLinkBtn");
    if (!validInit) {
      renderDuplicate(null);
      showStatus("URL must use http:// or https://", "err");
      if (submitBtn) submitBtn.disabled = true;
    } else {
      if (submitBtn) submitBtn.disabled = false;
      checkDuplicate(validInit);
    }
  }

  // auto submit
  if (qAuto) {
    const v = validateUrl(urlInput ? urlInput.value : "");
    if (v && !isLocked()) {
      if (urlInput) urlInput.value = v;
      await submitLink();
    }
  }
}

async function init() {
  // initial input values
  const urlInput = $("urlInput");
  const titleInput = $("titleInput");
  if (urlInput && qUrl) urlInput.value = qUrl;
  if (titleInput && qTitle) titleInput.value = qTitle;

  // format select initial
  const formatSelect = $("formatSelect");
  if (formatSelect) {
    let fmt = qFormat || localStorage.getItem("vault_capture_format") || "best";
    if (!ALLOWED_FORMATS.includes(fmt)) fmt = "best";
    formatSelect.value = fmt;
  }

  // remember checkbox initial
  const rememberCheck = $("rememberFolderCheck");
  if (rememberCheck) {
    const stored = localStorage.getItem("vault_capture_remember_folder");
    const on = stored === null ? true : stored !== "false";
    rememberCheck.checked = on;
  }

  // autoclose checkbox initial
  const autocloseCheck = $("autocloseCheck");
  if (autocloseCheck) {
    let ac = localStorage.getItem("vault_capture_autoclose") === "true";
    if (qAutoclose) ac = true;
    autocloseCheck.checked = ac;
  }

  // tabs
  const tabLink = $("tabLink");
  const tabFile = $("tabFile");
  if (tabLink) tabLink.addEventListener("click", () => setTab("link"));
  if (tabFile) tabFile.addEventListener("click", () => setTab("file"));

  // duplicate debounce
  if (urlInput) urlInput.addEventListener("input", debouncedDuplicate);

  // buttons
  const submitLinkBtn = $("submitLinkBtn");
  const submitFileBtn = $("submitFileBtn");
  if (submitLinkBtn) submitLinkBtn.addEventListener("click", submitLink);
  if (submitFileBtn) submitFileBtn.addEventListener("click", submitFile);

  // unlock
  const unlockBtn = $("unlockBtn");
  if (unlockBtn) unlockBtn.addEventListener("click", unlock);
  const lockPassword = $("lockPassword");
  if (lockPassword) {
    lockPassword.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        unlock();
      }
    });
  }

  // close window
  const closeBtn = $("closeWindowBtn");
  if (closeBtn) closeBtn.addEventListener("click", () => { try { window.close(); } catch (_) {} });

  // file handling
  const fileInput = $("fileInput");
  const dropZone = $("dropZone");
  const fileTitle = $("fileTitle");
  const fileMeta = $("fileMeta");

  function onFileChosen(f) {
    if (!f) return;
    selectedFile = f;
    if (fileMeta) {
      fileMeta.hidden = false;
      fileMeta.textContent = `${f.name} (${f.size} bytes)`;
    }
    if (submitFileBtn) submitFileBtn.disabled = false;
    if (fileTitle && !fileTitle.value) {
      fileTitle.value = f.name.replace(/\.[^.]+$/, "");
    }
  }

  if (fileInput) {
    fileInput.addEventListener("change", () => {
      onFileChosen(fileInput.files && fileInput.files[0]);
    });
  }
  if (dropZone) {
    dropZone.addEventListener("click", (e) => {
      if (e.target === fileInput) return;
      if (fileInput) fileInput.click();
    });
    dropZone.addEventListener("dragover", (e) => { e.preventDefault(); dropZone.classList.add("dragover"); });
    dropZone.addEventListener("dragleave", () => dropZone.classList.remove("dragover"));
    dropZone.addEventListener("drop", (e) => {
      e.preventDefault();
      dropZone.classList.remove("dragover");
      const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
      onFileChosen(f);
    });
  }

  // remember folder changes
  const rememberCheckEl = $("rememberFolderCheck");
  if (rememberCheckEl) {
    rememberCheckEl.addEventListener("change", () => {
      try { localStorage.setItem("vault_capture_remember_folder", rememberCheckEl.checked ? "true" : "false"); } catch (_) {}
      if (rememberCheckEl.checked) {
        const fi = $("folderInput");
        if (fi) try { localStorage.setItem("vault_capture_folder", fi.value.trim()); } catch (_) {}
      }
    });
  }
  const folderInput = $("folderInput");
  if (folderInput) {
    folderInput.addEventListener("change", () => {
      const rc = $("rememberFolderCheck");
      if (rc && rc.checked) {
        try { localStorage.setItem("vault_capture_folder", folderInput.value.trim()); } catch (_) {}
      }
    });
  }

  // format persist
  if (formatSelect) formatSelect.addEventListener("change", persistFormat);

  // autoclose persist
  if (autocloseCheck) {
    autocloseCheck.addEventListener("change", () => {
      try { localStorage.setItem("vault_capture_autoclose", autocloseCheck.checked ? "true" : "false"); } catch (_) {}
    });
  }

  // load
  try {
    await loadHealth();
  } catch (e) {
    showStatus("Failed to load health", "err");
    return;
  }

  if (isLocked()) {
    showLocked();
    return;
  }

  try {
    await Promise.all([loadFolders(), loadTags()]);
  } catch (e) {
    if (handleMaybeLocked(e)) return;
    showStatus("Failed to load folders/tags", "err");
    return;
  }

  showApp();
  await postLoad();
}

init().catch((e) => {
  showStatus((e && e.message) ? e.message : "Initialization failed", "err");
});
