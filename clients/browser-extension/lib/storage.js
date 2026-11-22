/**
 * Persistent settings for the Media Vault Companion extension.
 *
 * Zero-build ES module. Browser APIs are only touched inside functions so the
 * module can be imported in Node for unit tests.
 */

export const DEFAULT_SETTINGS = {
  serverUrl: "",
  authToken: "",
  lastFolder: "General",
  rememberFolder: true,
  lastFormat: "best",
  lastTags: "",
  forwardToTelegram: true,
  cachedFolders: ["General"],
  recentJobs: [],
};

const SETTINGS_KEY = "mv_settings";
const MAX_CACHED_FOLDERS = 25;
const MAX_RECENT_JOBS = 8;

function getExtApi() {
  const api = globalThis.browser || globalThis.chrome;
  return api && typeof api === "object" ? api : null;
}

function getStorageArea() {
  const api = getExtApi();
  const area = api?.storage?.local;
  if (!area || typeof area.get !== "function" || typeof area.set !== "function") {
    return null;
  }
  return area;
}

function cloneDefaultSettings() {
  return {
    ...DEFAULT_SETTINGS,
    cachedFolders: [...DEFAULT_SETTINGS.cachedFolders],
    recentJobs: DEFAULT_SETTINGS.recentJobs.map((job) => ({ ...job })),
  };
}

function toPlainObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

/**
 * Normalize a user-entered server URL.
 *
 * Only absolute http(s) URLs without embedded credentials are accepted.
 * Trailing slashes, query strings and fragments are removed so the value can
 * safely be used as a base for API requests.
 *
 * @param {unknown} raw
 * @returns {string}
 */
export function normalizeServerUrl(raw) {
  if (typeof raw !== "string") return "";
  const trimmed = raw.trim();
  if (!trimmed) return "";

  let parsed;
  try {
    parsed = new URL(trimmed);
  } catch (_) {
    return "";
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return "";
  if (parsed.username || parsed.password) return "";
  if (!parsed.hostname) return "";

  let pathname = (parsed.pathname || "").replace(/\/+$/, "");
  const lower = pathname.toLowerCase();
  for (const suffix of ["/capture", "/index.html", "/share-target"]) {
    if (lower.endsWith(suffix)) {
      pathname = pathname.slice(0, pathname.length - suffix.length);
      break;
    }
  }
  pathname = pathname.replace(/\/+$/, "");
  return `${parsed.origin}${pathname}`;
}

function sanitizeFolderList(value, fallback = DEFAULT_SETTINGS.cachedFolders) {
  if (!Array.isArray(value)) return [...fallback];

  const folders = [];
  const seen = new Set();
  for (const item of value) {
    if (typeof item !== "string") continue;
    const clean = item.trim();
    if (!clean) continue;
    const key = clean.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    folders.push(clean);
    if (folders.length >= MAX_CACHED_FOLDERS) break;
  }
  return folders;
}

function sanitizeRecentJobs(value) {
  if (!Array.isArray(value)) return [];
  return value
    .filter((job) => job && typeof job === "object" && !Array.isArray(job))
    .slice(0, MAX_RECENT_JOBS)
    .map((job) => ({ ...job }));
}

function coerceSettings(raw, fallback = cloneDefaultSettings()) {
  const source = toPlainObject(raw);
  const fb = { ...cloneDefaultSettings(), ...toPlainObject(fallback) };

  const pickString = (key, trimToFallback = false) => {
    const value = source[key];
    if (value === undefined) return fb[key];
    if (typeof value !== "string") return fb[key];
    if (!trimToFallback) return value;
    const trimmed = value.trim();
    return trimmed || fb[key];
  };

  return {
    serverUrl:
      source.serverUrl === undefined
        ? normalizeServerUrl(fb.serverUrl)
        : normalizeServerUrl(source.serverUrl),
    authToken: pickString("authToken"),
    lastFolder: pickString("lastFolder", true) || "General",
    rememberFolder:
      typeof source.rememberFolder === "boolean"
        ? source.rememberFolder
        : typeof fb.rememberFolder === "boolean"
          ? fb.rememberFolder
          : true,
    lastFormat: pickString("lastFormat", true) || "best",
    lastTags: pickString("lastTags"),
    forwardToTelegram:
      typeof source.forwardToTelegram === "boolean"
        ? source.forwardToTelegram
        : typeof fb.forwardToTelegram === "boolean"
          ? fb.forwardToTelegram
          : true,
    cachedFolders:
      source.cachedFolders === undefined
        ? [...fb.cachedFolders]
        : sanitizeFolderList(source.cachedFolders, fb.cachedFolders),
    recentJobs:
      source.recentJobs === undefined
        ? fb.recentJobs.map((job) => ({ ...job }))
        : sanitizeRecentJobs(source.recentJobs),
  };
}

async function readStorage(key) {
  const area = getStorageArea();
  if (!area) return {};

  try {
    const maybe = area.get(key);
    if (maybe && typeof maybe.then === "function") return (await maybe) || {};
    if (maybe && typeof maybe === "object") return maybe;
  } catch (_) {
    // Fall through to the callback-style API used by older browsers.
  }

  return await new Promise((resolve, reject) => {
    try {
      area.get(key, (items) => {
        const lastError = getExtApi()?.runtime?.lastError;
        if (lastError) reject(new Error(lastError.message || String(lastError)));
        else resolve(items || {});
      });
    } catch (err) {
      reject(err);
    }
  });
}

async function writeStorage(items) {
  const area = getStorageArea();
  if (!area) return;

  try {
    const maybe = area.set(items);
    if (maybe && typeof maybe.then === "function") {
      await maybe;
      return;
    }
    if (maybe !== undefined) return;
    if (area.set.length < 2) return; // Synchronous mock / implementation.
  } catch (_) {
    // Fall through to the callback-style API used by older browsers.
  }

  await new Promise((resolve, reject) => {
    try {
      area.set(items, () => {
        const lastError = getExtApi()?.runtime?.lastError;
        if (lastError) reject(new Error(lastError.message || String(lastError)));
        else resolve();
      });
    } catch (err) {
      reject(err);
    }
  });
}

async function persistSettings(settings) {
  const snapshot = {
    ...settings,
    cachedFolders: [...settings.cachedFolders],
    recentJobs: settings.recentJobs.map((job) => ({ ...job })),
  };
  await writeStorage({ [SETTINGS_KEY]: snapshot });
}

/**
 * Read settings from storage.local, merged with DEFAULT_SETTINGS.
 *
 * @returns {Promise<typeof DEFAULT_SETTINGS>}
 */
export async function getSettings() {
  if (!getStorageArea()) return cloneDefaultSettings();
  const items = await readStorage(SETTINGS_KEY);
  return coerceSettings(items?.[SETTINGS_KEY]);
}

/**
 * Merge a partial settings object into the current settings and persist it.
 *
 * @param {Partial<typeof DEFAULT_SETTINGS>} partial
 * @returns {Promise<typeof DEFAULT_SETTINGS>}
 */
export async function saveSettings(partial) {
  const current = await getSettings();
  const patch = toPlainObject(partial);
  const merged = { ...current };

  for (const key of Object.keys(DEFAULT_SETTINGS)) {
    if (Object.prototype.hasOwnProperty.call(patch, key)) {
      merged[key] = patch[key];
    }
  }
  if (Object.prototype.hasOwnProperty.call(patch, "serverUrl")) {
    merged.serverUrl = normalizeServerUrl(patch.serverUrl);
  }

  const next = coerceSettings(merged, current);
  await persistSettings(next);
  return next;
}

/**
 * Remember a folder the user explicitly chose (context-menu submenu / popup).
 *
 * @param {string} folder
 * @returns {Promise<typeof DEFAULT_SETTINGS>}
 */
export async function rememberFolderChoice(folder) {
  const cleanFolder = typeof folder === "string" ? folder.trim() : "";
  const current = await getSettings();
  if (!cleanFolder) return current;

  const cachedFolders = [...current.cachedFolders];
  const key = cleanFolder.toLowerCase();
  const exists = cachedFolders.some((item) => item.toLowerCase() === key);

  if (!exists) {
    cachedFolders.push(cleanFolder);
    while (cachedFolders.length > MAX_CACHED_FOLDERS) cachedFolders.shift();
  }

  const next = {
    ...current,
    cachedFolders,
    lastFolder:
      current.rememberFolder !== false ? cleanFolder : current.lastFolder,
  };

  await persistSettings(next);
  return next;
}

/**
 * Prepend a recent capture entry, deduplicating by jobId when present.
 *
 * @param {{ jobId?: string|number, title?: string, url?: string, folder?: string, status?: string, createdAt?: string }} entry
 * @returns {Promise<Array<Record<string, unknown>>>}
 */
export async function addRecentJob(entry) {
  const current = await getSettings();
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    return current.recentJobs;
  }

  const job = {
    jobId: entry.jobId ?? "",
    title: entry.title || "",
    url: entry.url || "",
    folder: entry.folder || "",
    status: entry.status || "queued",
    createdAt: entry.createdAt || new Date().toISOString(),
  };

  let recentJobs = Array.isArray(current.recentJobs)
    ? [...current.recentJobs]
    : [];

  if (job.jobId !== "" && job.jobId !== null && job.jobId !== undefined) {
    recentJobs = recentJobs.filter(
      (item) => String(item?.jobId ?? "") !== String(job.jobId),
    );
  }

  recentJobs.unshift(job);
  recentJobs = recentJobs.slice(0, MAX_RECENT_JOBS);

  const next = { ...current, recentJobs };
  await persistSettings(next);
  return recentJobs;
}

/**
 * Patch a recent capture entry by jobId.
 *
 * @param {string|number} jobId
 * @param {Record<string, unknown>} patch
 * @returns {Promise<Array<Record<string, unknown>>>}
 */
export async function updateRecentJob(jobId, patch) {
  const current = await getSettings();
  const key = String(jobId ?? "");
  const updates = toPlainObject(patch);
  let matched = false;

  const recentJobs = current.recentJobs.map((job) => {
    if (String(job?.jobId ?? "") !== key) return job;
    matched = true;
    return { ...job, ...updates, jobId: job.jobId };
  });

  if (!matched) return current.recentJobs;

  const next = { ...current, recentJobs };
  await persistSettings(next);
  return recentJobs;
}
