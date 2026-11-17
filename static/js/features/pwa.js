import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { setMode } from "/static/js/mode.js";
import { toast } from "/static/js/ui/dialogs.js";

const IDB_NAME = "media_vault_pwa";
const IDB_VERSION = 1;
const IDB_STORE = "shared_items";

let pwaInitialized = false;
const wiredButtons = new WeakSet();

function getInstallButtons() {
  return [
    document.getElementById("installPwaBtn"),
    document.getElementById("sheetInstallPwaBtn"),
  ].filter(Boolean);
}

function setInstallButtonsVisible(visible) {
  for (const btn of getInstallButtons()) {
    btn.hidden = !visible;
    btn.style.display = visible ? "" : "none";
  }
}

async function triggerInstallPrompt() {
  const promptEvent = state.deferredInstallPrompt;
  if (!promptEvent) return;
  try {
    await promptEvent.prompt();
    const choice = await promptEvent.userChoice;
    if (choice?.outcome === "accepted") {
      state.deferredInstallPrompt = null;
      setInstallButtonsVisible(false);
    }
  } catch (_) {
    state.deferredInstallPrompt = null;
    setInstallButtonsVisible(false);
  }
}

function wireInstallButtons() {
  for (const btn of getInstallButtons()) {
    if (wiredButtons.has(btn)) continue;
    wiredButtons.add(btn);
    btn.addEventListener("click", triggerInstallPrompt);
  }
}

function openPwaDb() {
  if (typeof indexedDB === "undefined") {
    return Promise.resolve(null);
  }
  return new Promise((resolve, reject) => {
    try {
      const req = indexedDB.open(IDB_NAME, IDB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(IDB_STORE)) {
          db.createObjectStore(IDB_STORE, { keyPath: "id" });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    } catch (err) {
      reject(err);
    }
  });
}

async function takeAllSharedItems() {
  let db = null;
  try {
    db = await openPwaDb();
    if (!db || !db.objectStoreNames.contains(IDB_STORE)) return [];
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(IDB_STORE, "readwrite");
      const store = tx.objectStore(IDB_STORE);
      const getReq = store.getAll();
      let items = [];
      getReq.onsuccess = () => {
        items = Array.isArray(getReq.result) ? getReq.result : [];
        if (items.length > 0) {
          store.clear();
        }
      };
      tx.oncomplete = () => resolve(items);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error || new Error("IndexedDB transaction aborted"));
    });
  } catch (_) {
    return [];
  } finally {
    try {
      db?.close();
    } catch (_) {}
  }
}

function extractUrls(...inputs) {
  const found = [];
  const seen = new Set();
  const urlRe = /https?:\/\/[^\s<>"'`]+/gi;
  for (const raw of inputs) {
    if (!raw) continue;
    const matches = String(raw).match(urlRe);
    if (!matches) continue;
    for (let m of matches) {
      m = m.replace(/[.,;!?]+$/, "");
      if (m && !seen.has(m)) {
        seen.add(m);
        found.push(m);
      }
    }
  }
  return found;
}

function cleanShareParamsFromUrl() {
  const keysToClean = ["shared", "share", "url", "text", "title"];
  const currentUrl = new URL(location.href);
  let changed = false;
  for (const key of keysToClean) {
    if (currentUrl.searchParams.has(key)) {
      currentUrl.searchParams.delete(key);
      changed = true;
    }
  }
  if (changed) {
    const nextUrl = currentUrl.pathname + currentUrl.search + location.hash;
    history.replaceState(history.state, "", nextUrl);
  }
}

function flushPendingSharedFiles() {
  const pending = state.pendingSharedFiles;
  if (!pending) return;
  if (state.authRequired && !state.token) return;
  state.pendingSharedFiles = null;
  const files = Array.isArray(pending) ? pending : (pending.files || []);
  const title = Array.isArray(pending) ? "" : (pending.title || "");
  if (files.length) {
    setMode("uploader");
    emit("pwa:shared-files", { files, title });
  }
}

export async function consumeSharedPayload() {
  const params = new URLSearchParams(location.search);
  const isFallback = params.get("share") === "fallback";
  const queryUrl = params.get("url") || "";
  const queryText = params.get("text") || "";
  const queryTitle = params.get("title") || "";

  const sharedItems = await takeAllSharedItems();
  cleanShareParamsFromUrl();

  const urlSources = [queryUrl, queryText];
  const files = [];
  let sharedTitle = queryTitle;

  for (const item of sharedItems) {
    if (!item || typeof item !== "object") continue;
    if (item.url) urlSources.push(item.url);
    if (item.text) urlSources.push(item.text);
    if (!sharedTitle && item.title) sharedTitle = String(item.title);
    if (Array.isArray(item.files)) {
      for (const f of item.files) {
        if (f && typeof f === "object" && f.size > 0) {
          files.push(f);
        }
      }
    }
  }

  const urls = extractUrls(...urlSources);

  if (urls.length > 0) {
    setMode("uploader");
    const urlInput = document.getElementById("urlInput");
    if (urlInput) {
      urlInput.value = urls.join("\n");
      urlInput.rows = Math.max(1, Math.min(4, urls.length));
      urlInput.dispatchEvent(new Event("input", { bubbles: true }));
    }
    toast("Shared link loaded into Uploader.", { type: "info" });
  }

  if (files.length > 0) {
    setMode("uploader");
    if (state.authRequired && !state.token) {
      state.pendingSharedFiles = { files, title: sharedTitle };
      toast("Unlock with your auth token to upload shared file(s).", { type: "info" });
    } else {
      emit("pwa:shared-files", { files, title: sharedTitle });
    }
  }

  if (isFallback && sharedItems.length === 0 && urls.length === 0 && files.length === 0) {
    setMode("uploader");
    toast("Share received — paste link or select file below.", { type: "info" });
  }
}

export async function initPwa() {
  if (!pwaInitialized) {
    pwaInitialized = true;

    wireInstallButtons();
    if (state.deferredInstallPrompt) {
      setInstallButtonsVisible(true);
    }

    window.addEventListener("beforeinstallprompt", (e) => {
      e.preventDefault();
      state.deferredInstallPrompt = e;
      wireInstallButtons();
      setInstallButtonsVisible(true);
    });

    window.addEventListener("appinstalled", () => {
      state.deferredInstallPrompt = null;
      setInstallButtonsVisible(false);
    });

    on("auth:unlocked", () => {
      flushPendingSharedFiles();
    });

    try {
      await navigator.serviceWorker?.register("/sw.js", { scope: "/" });
    } catch (_) {
      // Ignore registration failure on insecure origins or unsupported browsers
    }
  }

  await consumeSharedPayload();
}
