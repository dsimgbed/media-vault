/**
 * Background service worker for the Media Vault Companion extension.
 */

const extApi = globalThis.browser || globalThis.chrome;

import {
  getSettings,
  saveSettings,
  rememberFolderChoice,
  addRecentJob,
  getCookieSyncState,
  saveCookieSyncState,
} from "./lib/storage.js";
import {
  hasServerPermission,
  quickCapture,
  uploadBlob,
  checkHealth,
  uploadCookies,
} from "./lib/api.js";
import {
  hasCookiePermission,
  collectYouTubeCookies,
  formatNetscapeCookies,
  isYouTubeUrl,
  isRelevantAuthCookieChange,
  computeCookieTextDigest,
} from "./lib/cookies.js";

const COOKIE_SYNC_ALARM = "mv-cookie-sync";
const COOKIE_SYNC_PERIOD_MINUTES = 10;
const COOKIE_CHANGE_DEBOUNCE_MS = 4000;
const COOKIE_CHANGE_MIN_INTERVAL_MS = 45_000;
const SERVER_HEALTH_RECHECK_MS = 30 * 60_000;

let syncInFlight = null;
let cookieChangeTimer = null;
let lastCookieChangeSyncAt = 0;

function getExtApi() {
  return extApi;
}

async function ensureCookieSyncAlarm() {
  const alarms = getExtApi()?.alarms;
  if (!alarms || typeof alarms.create !== "function") return;
  try {
    let existing = null;
    if (typeof alarms.get === "function") {
      existing = await new Promise((resolve) => {
        try {
          const maybe = alarms.get(COOKIE_SYNC_ALARM);
          if (maybe && typeof maybe.then === "function") {
            maybe.then((a) => resolve(a || null)).catch(() => resolve(null));
            return;
          }
          alarms.get(COOKIE_SYNC_ALARM, (a) => resolve(a || null));
        } catch (_) {
          resolve(null);
        }
      });
    }
    if (!existing || existing.periodInMinutes !== COOKIE_SYNC_PERIOD_MINUTES) {
      alarms.create(COOKIE_SYNC_ALARM, { periodInMinutes: COOKIE_SYNC_PERIOD_MINUTES });
    }
  } catch (_) {
    // Ignore alarm setup failures on restricted environments.
  }
}

async function syncCookiesToVault({
  force = false,
  reason = "auto",
  verifyServer = false,
} = {}) {
  if (syncInFlight) return syncInFlight;
  const run = (async () => {
    const settings = await getSettings();
    if (!settings?.serverUrl) return { ok: false, skipped: "no_server" };
    if (!force && settings.autoSyncCookies === false) {
      return { ok: false, skipped: "disabled" };
    }
    const [serverOk, cookieOk] = await Promise.all([
      hasServerPermission(settings.serverUrl),
      hasCookiePermission(),
    ]);
    if (!serverOk || !cookieOk) {
      return { ok: false, skipped: "no_permission" };
    }

    const collected = await collectYouTubeCookies();
    const cookies = Array.isArray(collected) ? collected : [];
    const formatted = formatNetscapeCookies(cookies);
    const text = typeof formatted === "string" ? formatted : formatted?.text;
    const count = typeof formatted === "object" && formatted ? Number(formatted.count || 0) : 0;
    const domains = typeof formatted === "object" && Array.isArray(formatted?.domains) ? formatted.domains : [];
    if (!text || count <= 0) {
      return { ok: false, skipped: "empty" };
    }

    const digest = await computeCookieTextDigest(text);
    const prev = await getCookieSyncState();

    if (!force && prev.lastHash && prev.lastHash === digest) {
      const staleCheck = verifyServer || Date.now() - prev.lastSyncedAt > SERVER_HEALTH_RECHECK_MS;
      if (!staleCheck) {
        return { ok: true, skipped: "unchanged", count, domains };
      }
      try {
        const health = await checkHealth(settings);
        if (health?.ytdlp_cookies_configured !== false) {
          return { ok: true, skipped: "unchanged", count, domains };
        }
      } catch (_) {
        return { ok: false, skipped: "server_unreachable" };
      }
    }

    const response = await uploadCookies(settings, text, {
      auto_retry_failed: true,
      retry_unchanged: Boolean(force),
    });
    await saveCookieSyncState({
      lastHash: digest,
      lastSyncedAt: Date.now(),
      lastCount: count,
      lastDomains: domains,
      lastReason: reason,
    });
    return { ok: true, uploaded: true, count, domains, response };
  })();

  syncInFlight = run;
  try {
    return await run;
  } finally {
    if (syncInFlight === run) syncInFlight = null;
  }
}

function scheduleCookieChangeSync(changeInfo) {
  if (!isRelevantAuthCookieChange(changeInfo)) return;
  const criticalName = String(changeInfo?.cookie?.name || "");
  const isCritical =
    criticalName === "LOGIN_INFO" ||
    criticalName === "SAPISID" ||
    criticalName === "__Secure-1PSIDTS" ||
    criticalName === "__Secure-3PSIDTS";

  const elapsed = Date.now() - lastCookieChangeSyncAt;
  const delay = isCritical
    ? COOKIE_CHANGE_DEBOUNCE_MS
    : Math.max(COOKIE_CHANGE_DEBOUNCE_MS, COOKIE_CHANGE_MIN_INTERVAL_MS - elapsed);

  if (cookieChangeTimer) clearTimeout(cookieChangeTimer);
  cookieChangeTimer = setTimeout(() => {
    cookieChangeTimer = null;
    lastCookieChangeSyncAt = Date.now();
    syncCookiesToVault({ reason: "cookie_change" }).catch(() => {});
  }, delay);
}

function notify(title, message) {
  try {
    const api = getExtApi();
    api?.notifications?.create?.({
      type: "basic",
      iconUrl: api.runtime.getURL("icons/icon-128.png"),
      title: title || "Media Vault",
      message: String(message == null ? "" : message),
    });
  } catch (_) {
    // Ignore notification errors.
  }
}

function humanError(err) {
  if (!err) return "Unknown error";
  if (typeof err === "string") return err;
  if (err.message) return err.message;
  return String(err);
}

async function captureVisibleTab(windowId, options) {
  const api = getExtApi();
  const tabs = api?.tabs;
  if (!tabs || typeof tabs.captureVisibleTab !== "function") {
    throw new Error("Screenshot capture is not available in this browser.");
  }

  try {
    const maybe = tabs.captureVisibleTab(windowId, options);
    if (maybe && typeof maybe.then === "function") return await maybe;
    if (typeof maybe === "string") return maybe;
  } catch (_) {
    // Fall through to callback style.
  }

  return await new Promise((resolve, reject) => {
    try {
      tabs.captureVisibleTab(windowId, options, (dataUrl) => {
        const lastError = api?.runtime?.lastError;
        if (lastError) reject(new Error(lastError.message || String(lastError)));
        else resolve(dataUrl);
      });
    } catch (err) {
      reject(err);
    }
  });
}

// Serialized rebuild chain.
let rebuildChain = Promise.resolve();

function rebuildContextMenus() {
  rebuildChain = rebuildChain
    .then(() => doRebuildContextMenus())
    .catch((err) => {
      // Keep chain alive.
    });
  return rebuildChain;
}

async function doRebuildContextMenus() {
  const api = getExtApi();
  const settings = await getSettings();
  const folder = settings?.lastFolder || "General";
  const cached = Array.isArray(settings?.cachedFolders) ? settings.cachedFolders : [];
  const folders = cached.slice(0, 12);

  // Remove all existing menus.
  await new Promise((resolve) => {
    try {
      const maybe = api?.contextMenus?.removeAll?.();
      if (maybe && typeof maybe.then === "function") {
        maybe.then(() => resolve()).catch(() => resolve());
        return;
      }
      if (typeof api?.contextMenus?.removeAll === "function") {
        api.contextMenus.removeAll(() => {
          resolve();
        });
        return;
      }
      resolve();
    } catch (_) {
      resolve();
    }
  });

  const createMenu = (props) =>
    new Promise((resolve) => {
      try {
        const maybe = api?.contextMenus?.create?.(props);
        if (maybe && typeof maybe.then === "function") {
          maybe.then(() => resolve()).catch(() => resolve());
          return;
        }
        if (typeof api?.contextMenus?.create === "function") {
          // Chrome callback style.
          try {
            api.contextMenus.create(props, () => resolve());
          } catch (_) {
            resolve();
          }
          return;
        }
        resolve();
      } catch (_) {
        resolve();
      }
    });

  await createMenu({
    id: "mv-send-page",
    title: `Send Page to Media Vault (${folder})`,
    contexts: ["page"],
  });
  await createMenu({
    id: "mv-send-link",
    title: `Send Link to Media Vault (${folder})`,
    contexts: ["link"],
  });
  await createMenu({
    id: "mv-send-media",
    title: `Send Image/Video to Media Vault (${folder})`,
    contexts: ["image", "video", "audio"],
  });
  await createMenu({
    id: "mv-capture-screenshot",
    title: `Capture Screenshot to Media Vault (${folder})`,
    contexts: ["page"],
  });
  await createMenu({
    id: "mv-folder-parent",
    title: "Send to Folder...",
    contexts: ["page", "link", "image", "video", "audio"],
  });

  for (const f of folders) {
    if (typeof f !== "string" || !f.trim()) continue;
    const name = f.trim();
    await createMenu({
      id: `mv-folder:${name}`,
      title: name === folder ? `★ ${name}` : name,
      contexts: ["page", "link", "image", "video", "audio"],
      parentId: "mv-folder-parent",
    });
  }
}

async function handleMenuClick(info, tab) {
  const menuItemId = info?.menuItemId;

  let targetFolder;
  if (typeof menuItemId === "string" && menuItemId.startsWith("mv-folder:")) {
    targetFolder = menuItemId.slice("mv-folder:".length);
    await rememberFolderChoice(targetFolder);
    await rebuildContextMenus();
  } else {
    const settings = await getSettings();
    targetFolder = settings?.lastFolder || "General";
  }

  const settings = await getSettings();

  if (!settings?.serverUrl) {
    notify("Media Vault", "Open the Media Vault popup to configure your server URL.");
    return;
  }

  const permitted = await hasServerPermission(settings.serverUrl);
  if (!permitted) {
    notify("Media Vault", "Open the Media Vault popup to grant server permission.");
    return;
  }

  // Determine target URL.
  let url = null;
  if (info?.srcUrl) url = info.srcUrl;
  else if (info?.linkUrl) url = info.linkUrl;
  else url = info?.pageUrl || tab?.url;

  if (menuItemId === "mv-capture-screenshot") {
    try {
      const dataUrl = await captureVisibleTab(tab?.windowId, { format: "png" });
      const res = await fetch(dataUrl);
      const blob = await res.blob();
      const filename = `screenshot-${Date.now()}.png`;
      const title = tab?.title ? `${tab.title} — screenshot` : "Screenshot";
      const result = await uploadBlob(settings, {
        blob,
        filename,
        title,
        folder: targetFolder,
        tags: settings?.lastTags || undefined,
        forward_to_telegram: settings.forwardToTelegram,
        force: false,
      });
      const jobId = result?.job_id || result?.jobId || "";
      await addRecentJob({
        jobId,
        title,
        url: tab?.url || url || "",
        folder: targetFolder,
        status: "queued",
      });
      notify(
        "Media Vault",
        jobId
          ? `Screenshot queued in ${targetFolder}.`
          : `Screenshot sent to ${targetFolder}.`,
      );
    } catch (err) {
      notify("Media Vault", humanError(err));
    }
    return;
  }

  // URL captures.
  if (!url || !/^https?:\/\//i.test(String(url))) {
    notify("Media Vault", "This target is not a capturable http(s) URL.");
    return;
  }

  try {
    if (isYouTubeUrl(url) && settings?.autoSyncCookies !== false) {
      try {
        await Promise.race([
          syncCookiesToVault({ force: false, reason: "youtube_capture" }),
          new Promise((resolve) => setTimeout(resolve, 2500)),
        ]);
      } catch (_) {
        // Best-effort sync; do not block URL capture.
      }
    }
    const captureTitle =
      info?.linkText?.trim() ||
      (menuItemId === "mv-send-page" || (!info?.srcUrl && !info?.linkUrl) ? tab?.title?.trim() : "") ||
      undefined;
    const result = await quickCapture(settings, {
      url,
      folder: targetFolder,
      tags: settings?.lastTags || undefined,
      title: captureTitle,
      format_spec: settings?.lastFormat || "best",
      forward_to_telegram: settings?.forwardToTelegram,
      force: false,
      source: "extension",
    });
    const jobId = result?.job_id || result?.jobId || "";
    await addRecentJob({
      jobId,
      title: captureTitle || url,
      url,
      folder: targetFolder,
      status: result?.status || "queued",
    });
    if (result?.summary) {
      notify("Media Vault", result.summary);
    } else {
      notify(
        "Media Vault",
        jobId ? `Queued in ${targetFolder}. Job ${jobId}.` : `Sent to ${targetFolder}.`,
      );
    }
  } catch (err) {
    notify("Media Vault", humanError(err));
  }
}

// Listeners.
try {
  extApi?.runtime?.onInstalled?.addListener(() => {
    rebuildContextMenus();
    ensureCookieSyncAlarm();
    syncCookiesToVault({ reason: "install", verifyServer: true }).catch(() => {});
  });
} catch (_) {}

try {
  extApi?.runtime?.onStartup?.addListener(() => {
    rebuildContextMenus();
    ensureCookieSyncAlarm();
    syncCookiesToVault({ reason: "startup", verifyServer: true }).catch(() => {});
  });
} catch (_) {}

try {
  extApi?.alarms?.onAlarm?.addListener((alarm) => {
    if (alarm?.name === COOKIE_SYNC_ALARM) {
      syncCookiesToVault({ reason: "alarm", verifyServer: true }).catch(() => {});
    }
  });
} catch (_) {}

try {
  extApi?.cookies?.onChanged?.addListener((changeInfo) => {
    scheduleCookieChangeSync(changeInfo);
  });
} catch (_) {}

try {
  extApi?.permissions?.onAdded?.addListener(() => {
    ensureCookieSyncAlarm();
    syncCookiesToVault({ reason: "permission_added", verifyServer: true }).catch(() => {});
  });
} catch (_) {}

try {
  extApi?.contextMenus?.onClicked?.addListener((info, tab) => {
    handleMenuClick(info, tab).catch((err) => {
      notify("Media Vault", humanError(err));
    });
  });
} catch (_) {}

try {
  extApi?.runtime?.onMessage?.addListener((msg, sender, sendResponse) => {
    if (msg?.type === "MV_REFRESH_MENUS") {
      (async () => {
        try {
          if (msg?.folders) {
            await saveSettings({ cachedFolders: msg.folders });
          }
          await rebuildContextMenus();
        } catch (err) {
          // ignore
        }
        try {
          sendResponse?.({ ok: true });
        } catch (_) {
          // ignore
        }
      })();
      return true;
    }

    if (msg?.type === "MV_SYNC_COOKIES") {
      (async () => {
        try {
          await ensureCookieSyncAlarm();
          const res = await syncCookiesToVault({
            force: Boolean(msg?.force),
            reason: String(msg?.reason || "message"),
            verifyServer: Boolean(msg?.verifyServer),
          });
          sendResponse?.(res);
        } catch (err) {
          try {
            sendResponse?.({ ok: false, error: humanError(err) });
          } catch (_) {
            // ignore
          }
        }
      })();
      return true;
    }

    return undefined;
  });
} catch (_) {}

ensureCookieSyncAlarm().catch(() => {});