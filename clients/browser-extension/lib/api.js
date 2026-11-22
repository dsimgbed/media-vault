/**
 * API client for the Media Vault Companion extension.
 *
 * Zero-build ES module. Browser APIs are only touched inside functions so the
 * module can be imported in Node for unit tests.
 */

import { normalizeServerUrl } from "./storage.js";

function getExtApi() {
  const api = globalThis.browser || globalThis.chrome;
  return api && typeof api === "object" ? api : null;
}

export function buildOriginPattern(serverUrl) {
  const base = normalizeServerUrl(serverUrl);
  if (!base) return null;
  let parsed;
  try {
    parsed = new URL(base);
  } catch (_) {
    return null;
  }
  if (!parsed || !parsed.origin) return null;
  return `${parsed.origin}/*`;
}

async function permissionContains(serverUrl) {
  const pattern = buildOriginPattern(serverUrl);
  if (!pattern) return false;
  const api = getExtApi();
  const perms = api?.permissions;
  if (!perms || typeof perms.contains !== "function") return false;
  const details = { origins: [pattern] };

  // Promise-style (Firefox / modern Chrome).
  try {
    const maybe = perms.contains(details);
    if (maybe && typeof maybe.then === "function") {
      const result = await maybe;
      return Boolean(result);
    }
    if (typeof maybe === "boolean") return maybe;
    if (maybe !== undefined && maybe !== null) return Boolean(maybe);
  } catch (_) {
    // Fall through to callback style.
  }

  // Callback style (older Chrome).
  return await new Promise((resolve) => {
    try {
      perms.contains(details, (result) => {
        const lastError = api?.runtime?.lastError;
        if (lastError) resolve(false);
        else resolve(Boolean(result));
      });
    } catch (_) {
      resolve(false);
    }
  });
}

export async function hasServerPermission(serverUrl) {
  return permissionContains(serverUrl);
}

export async function requestServerPermission(serverUrl) {
  const pattern = buildOriginPattern(serverUrl);
  if (!pattern) return false;
  const api = getExtApi();
  const perms = api?.permissions;
  if (!perms || typeof perms.request !== "function") return false;
  const details = { origins: [pattern] };

  try {
    const maybe = perms.request(details);
    if (maybe && typeof maybe.then === "function") {
      const result = await maybe;
      return result;
    }
    if (typeof maybe === "boolean") return maybe;
    if (maybe !== undefined && maybe !== null) return maybe;
  } catch (_) {
    // Fall through to callback style.
  }

  return await new Promise((resolve) => {
    try {
      perms.request(details, (result) => {
        const lastError = api?.runtime?.lastError;
        if (lastError) resolve(false);
        else resolve(result);
      });
    } catch (_) {
      resolve(false);
    }
  });
}

function extractErrorMessage(res, rawText, parsed) {
  let detail = null;
  if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
    detail = parsed.detail;
  }

  if (Array.isArray(detail)) {
    const messages = detail
      .map((item) => {
        if (typeof item === "string") return item;
        if (item && typeof item === "object") {
          return item.msg || item.message || item.detail || JSON.stringify(item);
        }
        return String(item);
      })
      .filter((m) => typeof m === "string" && m.trim() !== "");
    if (messages.length) return messages.join("; ");
  }

  if (typeof detail === "string" && detail.trim() !== "") return detail.trim();
  if (detail && typeof detail === "object") {
    if (typeof detail.msg === "string" && detail.msg.trim() !== "") return detail.msg.trim();
    if (typeof detail.message === "string" && detail.message.trim() !== "") return detail.message.trim();
    try {
      return JSON.stringify(detail);
    } catch (_) {
      // ignore
    }
  }

  const text = typeof rawText === "string" ? rawText.trim() : "";
  if (text) return text;
  return `HTTP ${res.status}`;
}

export async function vaultRequest(settings, path, options = {}) {
  const base = normalizeServerUrl(settings?.serverUrl);
  if (!base) throw new Error("Server URL is not configured");

  const url = `${base}${path ?? ""}`;
  const opts = { ...(options || {}) };

  const headers = new Headers(opts.headers || {});
  const token = typeof settings?.authToken === "string" ? settings.authToken.trim() : "";
  if (token) headers.set("Authorization", `Bearer ${token}`);

  let body = opts.body;
  if (
    body &&
    typeof body === "object" &&
    !(body instanceof FormData) &&
    !(body instanceof Blob) &&
    !(body instanceof ArrayBuffer) &&
    !(body instanceof URLSearchParams) &&
    typeof body !== "string"
  ) {
    if (!headers.has("Content-Type")) headers.set("Content-Type", "application/json");
    body = JSON.stringify(body);
  }
  opts.headers = headers;
  opts.body = body;

  const res = await fetch(url, opts);

  const rawText = await res.text();
  let parsed = null;
  if (rawText.trim()) {
    try {
      parsed = JSON.parse(rawText);
    } catch (_) {
      parsed = null;
    }
  }

  if (!res.ok) {
    const message = extractErrorMessage(res, rawText, parsed);
    const err = new Error(message);
    err.status = res.status;
    throw err;
  }

  if (res.status === 204 || !rawText.trim()) return {};
  if (parsed !== null) return parsed;
  return rawText;
}

export async function checkHealth(settings) {
  return vaultRequest(settings, "/api/health", { method: "GET" });
}

export async function fetchFolders(settings) {
  return vaultRequest(settings, "/api/folders", { method: "GET" });
}

export async function fetchTags(settings) {
  return vaultRequest(settings, "/api/tags", { method: "GET" });
}

export async function checkDuplicate(settings, url) {
  return vaultRequest(settings, "/api/ingest/check-duplicate", {
    method: "POST",
    body: { url },
  });
}

export async function ingestUrl(settings, { url, folder, tags, title, format_spec, forward_to_telegram, force } = {}) {
  const body = {
    url,
    folder,
    tags,
    title,
    format_spec,
    forward_to_telegram,
    force,
  };
  return vaultRequest(settings, "/api/ingest/url", { method: "POST", body });
}

export async function quickCapture(settings, { url, folder, tags, title, format_spec, forward_to_telegram, force, source } = {}) {
  const cleanTitle = typeof title === "string" ? title.trim().slice(0, 300) : title;
  const body = {
    url,
    folder,
    tags,
    title: cleanTitle,
    format_spec,
    forward_to_telegram,
    force,
    source: source == null ? "extension" : source,
  };

  try {
    return await vaultRequest(settings, "/api/capture/quick", { method: "POST", body });
  } catch (err) {
    if (!(err?.status === 404 || err?.status === 405)) throw err;

    if (!force) {
      let dup = null;
      try {
        dup = await checkDuplicate(settings, url);
      } catch (_) {
        dup = null;
      }
      if (dup?.duplicate && dup?.item) {
        const item = dup.item;
        return {
          ok: true,
          status: "duplicate",
          duplicate: true,
          job_id: null,
          url,
          folder: item.folder || folder || "General",
          format_spec: format_spec || "best",
          summary: `Already in Vault (${item.folder || folder || "General"}): ${item.title || url}`,
          media_id: item.id ?? null,
          fallback: "ingest_url",
        };
      }
    }

    const res = await ingestUrl(settings, { url, folder, tags, title: cleanTitle, format_spec, forward_to_telegram, force });
    return {
      ok: true,
      status: res?.status || "queued",
      duplicate: false,
      job_id: res?.job_id ?? (Array.isArray(res?.job_ids) ? res.job_ids[0] : null) ?? null,
      job_ids: res?.job_ids,
      url,
      folder: folder || "General",
      format_spec: format_spec || "best",
      summary: `Queued for ${folder || "General"}: ${cleanTitle || url}`,
      media_id: null,
      fallback: "ingest_url",
    };
  }
}

export async function uploadBlob(settings, { blob, filename, title, folder, tags, forward_to_telegram, force } = {}) {
  const form = new FormData();
  form.append("file", blob, filename || "upload.bin");
  if (title !== undefined && title !== null && String(title).trim() !== "") {
    form.append("title", title);
  }
  if (folder !== undefined && folder !== null && String(folder).trim() !== "") {
    form.append("folder", folder);
  }
  if (tags !== undefined && tags !== null) {
    const tagsValue = Array.isArray(tags) ? tags.join(", ") : tags;
    if (String(tagsValue).trim() !== "") form.append("tags", tagsValue);
  }
  if (forward_to_telegram !== undefined) {
    form.append("forward_to_telegram", String(Boolean(forward_to_telegram)));
  }
  if (force !== undefined) {
    form.append("force", String(Boolean(force)));
  }
  return vaultRequest(settings, "/api/ingest/upload", { method: "POST", body: form });
}

export async function getJobStatus(settings, jobId) {
  return vaultRequest(settings, `/api/jobs/${encodeURIComponent(jobId)}`, { method: "GET" });
}

export async function uploadCookies(settings, cookiesText) {
  return vaultRequest(settings, "/api/ytdlp/cookies", {
    method: "POST",
    body: { cookies_text: cookiesText },
  });
}