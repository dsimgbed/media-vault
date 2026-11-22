/**
 * Background service worker for the Media Vault Companion extension.
 */

const extApi = globalThis.browser || globalThis.chrome;

import { getSettings, saveSettings, rememberFolderChoice, addRecentJob } from "./lib/storage.js";
import {
  hasServerPermission,
  quickCapture,
  uploadBlob,
} from "./lib/api.js";

function getExtApi() {
  return extApi;
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
  });
} catch (_) {}

try {
  extApi?.runtime?.onStartup?.addListener(() => {
    rebuildContextMenus();
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
    if (msg?.type !== "MV_REFRESH_MENUS") return undefined;
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
  });
} catch (_) {}