import { state } from "/static/js/state.js";
import { emit } from "/static/js/bus.js";
import { authHeaders } from "/static/js/api.js";
import { $, escapeHtml, fmtSize } from "/static/js/utils.js";
import { toast } from "/static/js/ui/dialogs.js";
import { hashFileStreaming, sha256Bytes } from "/static/js/crypto/sha256.js";

const STORAGE_KEY = "vault_upload_sessions_v1";
const INLINE_HASH_LIMIT = 128 * 1024 * 1024;
const CHUNK_CONCURRENCY = 2;
const MAX_CHUNK_RETRIES = 4;
const RETRYABLE_HTTP_STATUS = new Set([408, 429, 500, 502, 503, 504]);
const ACTIVE_STATUSES = new Set([
  "queued",
  "checking",
  "hashing",
  "uploading",
  "completing",
  "paused",
]);
const TERMINAL_CLEARABLE_STATUSES = new Set(["done", "failed", "cancelled", "skipped"]);

let initialized = false;
let nextClientId = 1;
let queueItems = [];
let activeRunPromise = null;
let lastActiveCount = 0;
let renderTimer = null;

function defaultChunkSize() {
  const mb = Number(state.uploadChunkSizeMb || 8);
  return Math.max(1, mb) * 1024 * 1024;
}

function makeFingerprint(file) {
  return `${file.name}|${file.size}|${file.lastModified ?? 0}`;
}

function fmtEta(seconds) {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return "";
  const total = Math.round(seconds);
  if (total < 60) return `${total}s`;
  const m = Math.floor(total / 60);
  const s = total % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function fmtSpeed(bytesPerSec) {
  if (!bytesPerSec || !Number.isFinite(bytesPerSec) || bytesPerSec <= 0) return "";
  return `${fmtSize(bytesPerSec)}/s`;
}

function triggerUnauthorized() {
  state.authRequired = true;
  const box = $("authBox");
  if (box) box.style.display = "flex";
  const banner = $("authBanner");
  if (banner) banner.hidden = false;
  emit("auth:required");
}

function parseErrorMessage(rawText, status) {
  if (!rawText) return `HTTP ${status}`;
  try {
    const parsed = JSON.parse(rawText);
    if (typeof parsed?.detail === "string" && parsed.detail) return parsed.detail;
    if (typeof parsed?.message === "string" && parsed.message) return parsed.message;
    if (typeof parsed?.error === "string" && parsed.error) return parsed.error;
  } catch {
    // plain text
  }
  return rawText;
}

async function requestJson(url, opts = {}) {
  const headers = { ...authHeaders(), ...(opts.headers || {}) };
  const res = await fetch(url, { ...opts, headers });
  if (res.status === 401) {
    triggerUnauthorized();
    const err = new Error("Unauthorized — enter your WEB_AUTH_TOKEN above.");
    err.status = 401;
    throw err;
  }
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    const err = new Error(parseErrorMessage(txt, res.status));
    err.status = res.status;
    throw err;
  }
  if (res.status === 204) return {};
  return res.json();
}

function parseExpiryMs(expiresAt) {
  if (typeof expiresAt === "number" && Number.isFinite(expiresAt)) {
    return expiresAt < 1e12 ? expiresAt * 1000 : expiresAt;
  }
  if (typeof expiresAt === "string" && expiresAt) {
    const parsed = Date.parse(expiresAt);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return Date.now() + 24 * 60 * 60 * 1000;
}

function readSavedSessions() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    const now = Date.now();
    let changed = false;
    const valid = {};
    for (const [fp, entry] of Object.entries(parsed)) {
      if (!entry || typeof entry !== "object" || !entry.sessionId) {
        changed = true;
        continue;
      }
      const expMs = parseExpiryMs(entry.expiresAt);
      if (expMs <= now) {
        changed = true;
        continue;
      }
      valid[fp] = { ...entry, expiresAtMs: expMs };
    }
    if (changed) {
      writeSavedSessions(valid);
    }
    return valid;
  } catch {
    return {};
  }
}

function writeSavedSessions(map) {
  try {
    const keys = Object.keys(map);
    if (!keys.length) {
      localStorage.removeItem(STORAGE_KEY);
    } else {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(map));
    }
  } catch {
    // ignore storage quota/availability errors
  }
}

function getSavedSession(fingerprint) {
  const map = readSavedSessions();
  return map[fingerprint] || null;
}

function saveSessionRecord(item, session) {
  if (!session?.id) return;
  const map = readSavedSessions();
  map[item.fingerprint] = {
    sessionId: session.id,
    fingerprint: item.fingerprint,
    filename: item.filename,
    totalSize: item.totalSize,
    chunkSize: session.chunk_size || item.chunkSize,
    totalChunks: session.total_chunks || item.totalChunks,
    receivedBytes: Number(session.received_bytes ?? item.confirmedBytes ?? 0),
    folder: item.folder,
    tags: Array.isArray(item.tags) ? item.tags.slice() : [],
    forwardToTelegram: Boolean(item.forwardToTelegram),
    force: Boolean(item.force),
    expiresAt: session.expires_at || null,
    expiresAtMs: parseExpiryMs(session.expires_at),
    updatedAt: Date.now(),
  };
  writeSavedSessions(map);
}

function updateSavedSessionProgress(fingerprint, sessionId, receivedBytes) {
  const map = readSavedSessions();
  const entry = map[fingerprint];
  if (entry && entry.sessionId === sessionId) {
    entry.receivedBytes = receivedBytes;
    entry.updatedAt = Date.now();
    writeSavedSessions(map);
  }
}

function removeSavedSession(fingerprint, sessionId = null) {
  const map = readSavedSessions();
  let changed = false;
  if (fingerprint && map[fingerprint]) {
    if (!sessionId || map[fingerprint].sessionId === sessionId) {
      delete map[fingerprint];
      changed = true;
    }
  }
  if (sessionId) {
    for (const [fp, entry] of Object.entries(map)) {
      if (entry?.sessionId === sessionId) {
        delete map[fp];
        changed = true;
      }
    }
  }
  if (changed) {
    writeSavedSessions(map);
  }
}

function getUnattachedSavedSessions() {
  const map = readSavedSessions();
  const activeFingerprints = new Set(
    queueItems
      .filter((it) => ACTIVE_STATUSES.has(it.status) || it.status === "duplicate")
      .map((it) => it.fingerprint)
  );
  return Object.values(map).filter((entry) => !activeFingerprints.has(entry.fingerprint));
}

export function getActiveUploadCount() {
  let count = 0;
  for (const item of queueItems) {
    if (ACTIVE_STATUSES.has(item.status)) count++;
  }
  return count;
}

function notifyActiveCountIfChanged() {
  const count = getActiveUploadCount();
  if (count !== lastActiveCount) {
    lastActiveCount = count;
    emit("uploads:changed", { count });
  }
}

function scheduleRender() {
  if (renderTimer != null) return;
  const raf = typeof requestAnimationFrame === "function"
    ? requestAnimationFrame
    : (fn) => setTimeout(fn, 60);
  renderTimer = raf(() => {
    renderTimer = null;
    renderQueue();
  });
}

function abortItemNetwork(item) {
  if (item.abortController) {
    try {
      item.abortController.abort();
    } catch {
      // ignore
    }
    item.abortController = null;
  }
  for (const xhr of item.activeXhrs) {
    try {
      xhr.abort();
    } catch {
      // ignore
    }
  }
  item.activeXhrs.clear();
  item.inFlightLoaded.clear();
  flushItemSleeps(item);
}

function pauseItemForNetwork(item) {
  if (item.status === "cancelled" || item.status === "done" || item.status === "failed") return;
  item.status = "paused";
  item.pausedReason = "offline";
  item.autoResumeOnOnline = true;
  item.speedBps = 0;
  item.etaSec = null;
  item.statusText = "Waiting for network...";
  abortItemNetwork(item);
  notifyActiveCountIfChanged();
  renderQueue();
}

function pauseItemForAuth(item) {
  if (item.status === "cancelled" || item.status === "done" || item.status === "failed") return;
  item.status = "paused";
  item.pausedReason = "auth";
  item.autoResumeOnOnline = false;
  item.speedBps = 0;
  item.etaSec = null;
  item.statusText = "Authentication required — unlock and resume";
  abortItemNetwork(item);
  notifyActiveCountIfChanged();
  renderQueue();
}

function pauseItemByUser(item) {
  if (!["queued", "checking", "hashing", "uploading"].includes(item.status)) return;
  item.status = "paused";
  item.pausedReason = "user";
  item.autoResumeOnOnline = false;
  item.speedBps = 0;
  item.etaSec = null;
  item.statusText = "Paused";
  abortItemNetwork(item);
  notifyActiveCountIfChanged();
  renderQueue();
}

function resumeItem(item) {
  if (item.status !== "paused") return;
  item.pausedReason = null;
  item.autoResumeOnOnline = false;
  item.error = null;
  item.speedBps = 0;
  item.etaSec = null;
  item.status = "queued";
  item.statusText = "Waiting to resume...";
  notifyActiveCountIfChanged();
  renderQueue();
  kickScheduler();
}

async function cancelItem(item) {
  if (item.status === "done" || item.status === "cancelled") return;
  item.status = "cancelled";
  item.pausedReason = null;
  item.autoResumeOnOnline = false;
  item.speedBps = 0;
  item.etaSec = null;
  item.statusText = "Cancelled";
  abortItemNetwork(item);

  const sessionId = item.sessionId;
  item.sessionId = null;
  removeSavedSession(item.fingerprint, sessionId);

  notifyActiveCountIfChanged();
  renderQueue();

  if (sessionId) {
    try {
      await requestJson(`/api/uploads/sessions/${encodeURIComponent(sessionId)}`, {
        method: "DELETE",
      });
    } catch {
      // ignore delete errors on cancel
    }
  }
  kickScheduler();
}

function retryItem(item) {
  if (!["failed", "cancelled"].includes(item.status)) return;
  item.error = null;
  item.pausedReason = null;
  item.autoResumeOnOnline = false;
  item.inFlightLoaded.clear();
  if (!item.sessionId) {
    item.uploadedBytes = 0;
    item.confirmedBytes = 0;
    item.pct = 0;
    item.hashPct = 0;
  }
  item.speedBps = 0;
  item.etaSec = null;
  item.status = "queued";
  item.statusText = "Waiting in queue";
  notifyActiveCountIfChanged();
  renderQueue();
  kickScheduler();
}

function dismissItem(itemId) {
  const idx = queueItems.findIndex((it) => it.id === itemId);
  if (idx === -1) return;
  if (ACTIVE_STATUSES.has(queueItems[idx].status)) return;
  queueItems.splice(idx, 1);
  notifyActiveCountIfChanged();
  renderQueue();
}

function computeMissingChunks(session) {
  const total = Number(session.total_chunks || 0);
  if (Array.isArray(session.missing_chunks)) {
    return session.missing_chunks.slice();
  }
  const received = new Set(Array.isArray(session.received_chunks) ? session.received_chunks : []);
  const missing = [];
  for (let i = 0; i < total; i++) {
    if (!received.has(i)) missing.push(i);
  }
  return missing;
}

function updateUploadMetrics(item) {
  let inFlight = 0;
  for (const loaded of item.inFlightLoaded.values()) {
    inFlight += loaded;
  }
  const rawUploaded = Math.min(item.totalSize, item.confirmedBytes + inFlight);
  item.uploadedBytes = Math.max(item.uploadedBytes, rawUploaded);
  item.pct =
    item.totalSize > 0
      ? Math.min(100, Math.round((item.uploadedBytes / item.totalSize) * 100))
      : 100;

  const now = typeof performance !== "undefined" ? performance.now() : Date.now();
  const dt = (now - item.lastSampleTime) / 1000;
  if (dt >= 0.4) {
    const dBytes = Math.max(0, item.uploadedBytes - item.lastSampleBytes);
    const instBps = dBytes / dt;
    item.speedBps = item.speedBps > 0 ? 0.3 * instBps + 0.7 * item.speedBps : instBps;
    item.lastSampleTime = now;
    item.lastSampleBytes = item.uploadedBytes;
    const remaining = Math.max(0, item.totalSize - item.uploadedBytes);
    item.etaSec = item.speedBps > 1 ? remaining / item.speedBps : null;
  }
  scheduleRender();
}

function sleepCancellable(ms, item) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      item._sleepCleanups.delete(cleanup);
      resolve();
    }, ms);
    const cleanup = () => {
      clearTimeout(timer);
      item._sleepCleanups.delete(cleanup);
      resolve();
    };
    item._sleepCleanups.add(cleanup);
  });
}

function flushItemSleeps(item) {
  for (const fn of item._sleepCleanups) {
    fn();
  }
  item._sleepCleanups.clear();
}

function putChunkXhr(item, chunkIndex, buf, chunkSha) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const url = `/api/uploads/sessions/${encodeURIComponent(item.sessionId)}/chunks/${chunkIndex}`;
    xhr.open("PUT", url);
    if (state.token) {
      xhr.setRequestHeader("Authorization", `Bearer ${state.token}`);
    }
    xhr.setRequestHeader("Content-Type", "application/octet-stream");
    xhr.setRequestHeader("X-Chunk-SHA256", chunkSha);

    item.activeXhrs.add(xhr);

    xhr.upload.onprogress = (e) => {
      if (item.status !== "uploading") return;
      if (e.lengthComputable) {
        item.inFlightLoaded.set(chunkIndex, Math.min(buf.byteLength, e.loaded));
        updateUploadMetrics(item);
      }
    };

    xhr.onload = () => {
      item.activeXhrs.delete(xhr);
      item.inFlightLoaded.delete(chunkIndex);
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          resolve(JSON.parse(xhr.responseText || "{}"));
        } catch {
          resolve({});
        }
        return;
      }
      const err = new Error(parseErrorMessage(xhr.responseText, xhr.status));
      err.status = xhr.status;
      reject(err);
    };

    xhr.onerror = () => {
      item.activeXhrs.delete(xhr);
      item.inFlightLoaded.delete(chunkIndex);
      const err = new Error("Network error during chunk upload");
      err.status = 0;
      err.isNetworkError = true;
      reject(err);
    };

    xhr.onabort = () => {
      item.activeXhrs.delete(xhr);
      item.inFlightLoaded.delete(chunkIndex);
      const err = new DOMException("Aborted", "AbortError");
      err.isAbort = true;
      reject(err);
    };

    xhr.send(buf);
  });
}

async function uploadSingleChunkWithRetry(item, chunkIndex) {
  const start = chunkIndex * item.chunkSize;
  const end = Math.min(item.totalSize, start + item.chunkSize);

  for (let attempt = 0; attempt <= MAX_CHUNK_RETRIES; attempt++) {
    if (item.status !== "uploading") return { interrupted: true };
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
      pauseItemForNetwork(item);
      return { interrupted: true };
    }

    const slice = item.file.slice(start, end);
    const buf = await slice.arrayBuffer();
    if (item.status !== "uploading") return { interrupted: true };

    const chunkSha = await sha256Bytes(buf);
    if (item.status !== "uploading") return { interrupted: true };

    try {
      const res = await putChunkXhr(item, chunkIndex, buf, chunkSha);
      if (item.status !== "uploading") return { interrupted: true };

      const chunkBytes = end - start;
      if (res.received_bytes != null && Number.isFinite(Number(res.received_bytes))) {
        item.confirmedBytes = Math.max(item.confirmedBytes, Number(res.received_bytes));
      } else {
        item.confirmedBytes += chunkBytes;
      }
      updateSavedSessionProgress(item.fingerprint, item.sessionId, item.confirmedBytes);
      updateUploadMetrics(item);
      return { interrupted: false };
    } catch (err) {
      if (err?.name === "AbortError" || err?.isAbort || item.status !== "uploading") {
        return { interrupted: true };
      }
      if (err?.status === 401) {
        triggerUnauthorized();
        pauseItemForAuth(item);
        return { interrupted: true };
      }
      if (err?.status === 410) {
        removeSavedSession(item.fingerprint, item.sessionId);
        item.sessionId = null;
        const expiredErr = new Error("Upload session expired — click Retry to start again.");
        expiredErr.status = 410;
        throw expiredErr;
      }

      const isTransient =
        err?.isNetworkError ||
        err?.status === 0 ||
        RETRYABLE_HTTP_STATUS.has(err?.status);

      if (typeof navigator !== "undefined" && navigator.onLine === false) {
        pauseItemForNetwork(item);
        return { interrupted: true };
      }

      if (!isTransient || attempt >= MAX_CHUNK_RETRIES) {
        throw err;
      }

      const backoffMs = 1000 * (2 ** attempt);
      item.statusText = `Retrying chunk ${chunkIndex + 1}/${item.totalChunks} (${attempt + 1}/${MAX_CHUNK_RETRIES})...`;
      scheduleRender();
      await sleepCancellable(backoffMs, item);
    }
  }

  return { interrupted: false };
}

async function runPreFlightDeduplication(item) {
  if (item.force) return false;

  if (item.totalSize <= INLINE_HASH_LIMIT) {
    if (!item.sha256) {
      item.status = "hashing";
      item.hashPct = 0;
      item.pct = 0;
      item.statusText = "Hashing file...";
      item.abortController = new AbortController();
      renderQueue();

      item.sha256 = await hashFileStreaming(item.file, {
        signal: item.abortController.signal,
        onProgress: ({ pct }) => {
          if (item.status !== "hashing") return;
          item.hashPct = pct;
          item.pct = pct;
          item.statusText = `Hashing ${pct}%`;
          scheduleRender();
        },
      });
    }

    if (item.status !== "hashing" && item.status !== "queued") return true;
    item.status = "checking";
    item.statusText = "Checking for duplicates...";
    item.abortController = new AbortController();
    renderQueue();

    const dupRes = await requestJson("/api/ingest/check-duplicate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sha256: item.sha256 }),
      signal: item.abortController.signal,
    });

    if (dupRes.duplicate && dupRes.item) {
      item.status = "duplicate";
      item.duplicateMedia = dupRes.item;
      item.statusText = "Duplicate already in vault";
      notifyActiveCountIfChanged();
      renderQueue();
      return true;
    }
    return false;
  }

  // Large file (> 128 MB) two-tier dedup
  if (!item.skipDuplicateHash && !item.sha256) {
    item.status = "checking";
    item.pct = 0;
    item.statusText = "Checking size & filename...";
    item.abortController = new AbortController();
    renderQueue();

    const metaRes = await requestJson("/api/ingest/check-duplicate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ size_bytes: item.totalSize, filename: item.filename }),
      signal: item.abortController.signal,
    });

    if (metaRes.duplicate && metaRes.item) {
      item.status = "duplicate";
      item.duplicateMedia = metaRes.item;
      item.statusText = "Duplicate already in vault";
      notifyActiveCountIfChanged();
      renderQueue();
      return true;
    }

    if (metaRes.needs_hash_verification && !item.skipDuplicateHash) {
      item.status = "hashing";
      item.hashPct = 0;
      item.pct = 0;
      item.statusText = "Verifying duplicate...";
      item.abortController = new AbortController();
      renderQueue();

      try {
        item.sha256 = await hashFileStreaming(item.file, {
          signal: item.abortController.signal,
          onProgress: ({ pct }) => {
            if (item.status !== "hashing") return;
            item.hashPct = pct;
            item.pct = pct;
            item.statusText = `Verifying duplicate (${pct}%)...`;
            scheduleRender();
          },
        });
      } catch (err) {
        if (err?.name === "AbortError" && item._skipCheckAborted) {
          item._skipCheckAborted = false;
          item.skipDuplicateHash = true;
          return false;
        }
        throw err;
      }

      if (item.skipDuplicateHash) return false;
      item.status = "checking";
      item.statusText = "Verifying duplicate hash...";
      item.abortController = new AbortController();
      renderQueue();

      const hashRes = await requestJson("/api/ingest/check-duplicate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sha256: item.sha256 }),
        signal: item.abortController.signal,
      });

      if (hashRes.duplicate && hashRes.item) {
        item.status = "duplicate";
        item.duplicateMedia = hashRes.item;
        item.statusText = "Duplicate already in vault";
        notifyActiveCountIfChanged();
        renderQueue();
        return true;
      }
    }
  }

  return false;
}

function sameTagsList(a, b) {
  const aa = Array.isArray(a) ? a : [];
  const bb = Array.isArray(b) ? b : [];
  if (aa.length !== bb.length) return false;
  for (let i = 0; i < aa.length; i += 1) {
    if (aa[i] !== bb[i]) return false;
  }
  return true;
}

async function resolveOrCreateSession(item) {
  const savedCandidate = getSavedSession(item.fingerprint);
  const saved =
    savedCandidate &&
    savedCandidate.folder === item.folder &&
    Boolean(savedCandidate.force) === Boolean(item.force) &&
    (savedCandidate.forwardToTelegram === undefined ||
      Boolean(savedCandidate.forwardToTelegram) === Boolean(item.forwardToTelegram)) &&
    (savedCandidate.tags === undefined || sameTagsList(savedCandidate.tags, item.tags))
      ? savedCandidate
      : null;
  if (saved) {
    item.status = "checking";
    item.statusText = "Checking resumable session...";
    item.abortController = new AbortController();
    renderQueue();

    try {
      const existing = await requestJson(
        `/api/uploads/sessions/${encodeURIComponent(saved.sessionId)}`,
        { signal: item.abortController.signal }
      );
      if (existing.status === "completed" || existing.status === "done") {
        removeSavedSession(item.fingerprint, saved.sessionId);
        return { alreadyCompleted: true, session: existing };
      }
      if (existing.status === "uploading" || existing.status === "created") {
        saveSessionRecord(item, existing);
        return { alreadyCompleted: false, session: existing };
      }
      removeSavedSession(item.fingerprint, saved.sessionId);
    } catch (err) {
      if (err?.name === "AbortError") throw err;
      if (err?.status === 401) throw err;
      // 404 / 410 / expired -> prune and create a fresh session
      removeSavedSession(item.fingerprint, saved.sessionId);
    }
  }

  const duplicated = await runPreFlightDeduplication(item);
  if (duplicated) return { stoppedAsDuplicate: true };
  if (!["queued", "checking", "hashing"].includes(item.status)) {
    return { interrupted: true };
  }

  item.status = "checking";
  item.statusText = "Starting upload session...";
  item.abortController = new AbortController();
  renderQueue();

  const chunkSize = defaultChunkSize();
  const body = {
    filename: item.filename,
    total_size: item.totalSize,
    chunk_size: chunkSize,
    folder: item.folder,
    tags: item.tags,
    forward_to_telegram: item.forwardToTelegram,
    force: item.force,
    fingerprint: item.fingerprint,
  };
  if (item.sha256) {
    body.sha256 = item.sha256;
  }

  const res = await requestJson("/api/uploads/sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: item.abortController.signal,
  });

  if (res.duplicate && res.existing_item) {
    item.status = "duplicate";
    item.duplicateMedia = res.existing_item;
    item.statusText = "Duplicate already in vault";
    notifyActiveCountIfChanged();
    renderQueue();
    return { stoppedAsDuplicate: true };
  }

  const session = res.session;
  saveSessionRecord(item, session);
  return { alreadyCompleted: false, session };
}

async function processItem(item) {
  if (item.status !== "queued") return;
  if (typeof navigator !== "undefined" && navigator.onLine === false) {
    pauseItemForNetwork(item);
    return;
  }

  try {
    const outcome = await resolveOrCreateSession(item);
    if (outcome.stoppedAsDuplicate || outcome.interrupted) return;

    if (outcome.alreadyCompleted) {
      item.status = "done";
      item.jobId = outcome.session?.job_id || null;
      item.uploadedBytes = item.totalSize;
      item.pct = 100;
      item.speedBps = 0;
      item.etaSec = null;
      item.statusText = "Upload complete";
      notifyActiveCountIfChanged();
      renderQueue();
      emit("jobs:refresh");
      emit("media:changed", { source: "upload" });
      return;
    }

    const session = outcome.session;
    item.sessionId = session.id;
    item.chunkSize = Number(session.chunk_size || defaultChunkSize());
    item.totalChunks = Number(session.total_chunks || 0);
    item.confirmedBytes = Number(session.received_bytes || 0);
    item.uploadedBytes = item.confirmedBytes;
    item.pct =
      item.totalSize > 0
        ? Math.min(100, Math.round((item.uploadedBytes / item.totalSize) * 100))
        : 100;

    const missingChunks = computeMissingChunks(session);
    item.status = "uploading";
    item.inFlightLoaded.clear();
    item.speedBps = 0;
    item.etaSec = null;
    item.lastSampleTime = typeof performance !== "undefined" ? performance.now() : Date.now();
    item.lastSampleBytes = item.confirmedBytes;
    item.statusText =
      item.totalChunks > 0
        ? `Uploading ${item.totalChunks - missingChunks.length}/${item.totalChunks} chunks`
        : "Uploading...";
    renderQueue();

    if (item.totalSize > 0 && missingChunks.length > 0) {
      let cursor = 0;
      let fatalError = null;
      const workerCount = Math.min(CHUNK_CONCURRENCY, missingChunks.length);

      const workers = Array.from({ length: workerCount }, async () => {
        while (cursor < missingChunks.length && !fatalError && item.status === "uploading") {
          const idx = missingChunks[cursor++];
          item.statusText = `Uploading chunk ${idx + 1}/${item.totalChunks}`;
          scheduleRender();
          try {
            const chunkRes = await uploadSingleChunkWithRetry(item, idx);
            if (chunkRes.interrupted) return;
          } catch (err) {
            fatalError = err;
            abortItemNetwork(item);
            return;
          }
        }
      });

      await Promise.all(workers);
      if (fatalError) throw fatalError;
      if (item.status !== "uploading") return;
    }

    // Complete session
    item.status = "completing";
    item.pct = 100;
    item.speedBps = 0;
    item.etaSec = null;
    item.statusText = "Finalizing upload...";
    item.abortController = new AbortController();
    renderQueue();

    const completeBody = item.sha256 ? { sha256: item.sha256 } : {};
    const completeRes = await requestJson(
      `/api/uploads/sessions/${encodeURIComponent(item.sessionId)}/complete`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(completeBody),
        signal: item.abortController.signal,
      }
    );

    removeSavedSession(item.fingerprint, item.sessionId);
    item.status = "done";
    item.jobId = completeRes.job_id || null;
    item.uploadedBytes = item.totalSize;
    item.pct = 100;
    item.statusText = "Queued for processing";
    notifyActiveCountIfChanged();
    renderQueue();
    emit("jobs:refresh");
    emit("media:changed", { source: "upload" });
  } catch (err) {
    if (err?.name === "AbortError" || item.status === "paused" || item.status === "cancelled") {
      return;
    }
    if (err?.status === 401) {
      pauseItemForAuth(item);
      return;
    }
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
      pauseItemForNetwork(item);
      return;
    }
    if (err?.status === 410) {
      removeSavedSession(item.fingerprint, item.sessionId);
      item.sessionId = null;
    }
    item.status = "failed";
    item.speedBps = 0;
    item.etaSec = null;
    item.error = err?.message || "Upload failed";
    item.statusText = item.error;
    abortItemNetwork(item);
    notifyActiveCountIfChanged();
    renderQueue();
  } finally {
    item.abortController = null;
    flushItemSleeps(item);
  }
}

function kickScheduler() {
  if (activeRunPromise) return;
  activeRunPromise = (async () => {
    try {
      while (true) {
        const nextItem = queueItems.find((it) => it.status === "queued");
        if (!nextItem) break;
        await processItem(nextItem);
      }
    } finally {
      activeRunPromise = null;
      notifyActiveCountIfChanged();
      renderQueue();
    }
  })();
}

function createQueueItem(file, options) {
  return {
    id: `uq_${nextClientId++}`,
    file,
    filename: file.name,
    totalSize: Number(file.size || 0),
    fingerprint: makeFingerprint(file),
    folder: options.folder || state.defaultFolder || "General",
    tags: options.tags || "",
    forwardToTelegram: Boolean(options.forwardToTelegram),
    force: Boolean(options.force),
    status: "queued",
    statusText: "Waiting in queue",
    error: null,
    sha256: null,
    skipDuplicateHash: false,
    _skipCheckAborted: false,
    duplicateMedia: null,
    sessionId: null,
    chunkSize: defaultChunkSize(),
    totalChunks: 0,
    confirmedBytes: 0,
    inFlightLoaded: new Map(),
    uploadedBytes: 0,
    hashPct: 0,
    pct: 0,
    speedBps: 0,
    etaSec: null,
    lastSampleTime: 0,
    lastSampleBytes: 0,
    autoResumeOnOnline: false,
    pausedReason: null,
    abortController: null,
    activeXhrs: new Set(),
    _sleepCleanups: new Set(),
    jobId: null,
  };
}

export function enqueueFiles(files, options = {}) {
  const list = Array.from(files || []).filter(Boolean);
  if (!list.length) return [];

  const created = [];
  for (const file of list) {
    const fp = makeFingerprint(file);
    const existingActive = queueItems.find(
      (it) => it.fingerprint === fp && ACTIVE_STATUSES.has(it.status)
    );
    if (existingActive) {
      if (existingActive.status === "paused") {
        resumeItem(existingActive);
      } else {
        toast(`${file.name} is already in the upload queue.`, { type: "info" });
      }
      continue;
    }
    const item = createQueueItem(file, options);
    queueItems.push(item);
    created.push(item);
  }

  notifyActiveCountIfChanged();
  renderQueue();
  kickScheduler();
  return created;
}

function pauseAll() {
  const active = queueItems.filter((it) =>
    ["queued", "checking", "hashing", "uploading"].includes(it.status)
  );
  // Pause queued items first so scheduler doesn't advance while aborting active item
  for (const it of active) {
    if (it.status === "queued") pauseItemByUser(it);
  }
  for (const it of active) {
    if (it.status !== "paused") pauseItemByUser(it);
  }
}

function resumeAll() {
  const paused = queueItems.filter((it) => it.status === "paused");
  for (const it of paused) {
    it.pausedReason = null;
    it.autoResumeOnOnline = false;
    it.error = null;
    it.speedBps = 0;
    it.etaSec = null;
    it.status = "queued";
    it.statusText = "Waiting to resume...";
  }
  notifyActiveCountIfChanged();
  renderQueue();
  kickScheduler();
}

function clearFinished() {
  queueItems = queueItems.filter((it) => !TERMINAL_CLEARABLE_STATUSES.has(it.status));
  notifyActiveCountIfChanged();
  renderQueue();
}

function buildSummaryText(unattachedSaved) {
  const total = queueItems.length;
  if (!total && unattachedSaved.length) {
    return `${unattachedSaved.length} interrupted upload${unattachedSaved.length === 1 ? "" : "s"} available to resume`;
  }

  const uploadingItem = queueItems.find((it) =>
    ["checking", "hashing", "uploading", "completing"].includes(it.status)
  );
  const doneCount = queueItems.filter((it) => it.status === "done").length;
  const activeCount = queueItems.filter((it) => ACTIVE_STATUSES.has(it.status)).length;
  const pausedCount = queueItems.filter((it) => it.status === "paused").length;

  if (uploadingItem) {
    const currentIdx = Math.min(total, doneCount + 1);
    const speedTxt = uploadingItem.status === "uploading" ? fmtSpeed(uploadingItem.speedBps) : "";
    const etaTxt = uploadingItem.status === "uploading" ? fmtEta(uploadingItem.etaSec) : "";
    const parts = [`Uploading ${currentIdx} of ${total}`];
    if (speedTxt) parts.push(speedTxt);
    if (etaTxt) parts.push(`ETA ${etaTxt}`);
    return parts.join(" • ");
  }

  if (pausedCount > 0) {
    return `${pausedCount} paused • ${activeCount} active of ${total}`;
  }
  const dupCount = queueItems.filter((it) => it.status === "duplicate").length;
  if (dupCount > 0) {
    return `${dupCount} duplicate${dupCount === 1 ? "" : "s"} awaiting action`;
  }
  const failedCount = queueItems.filter((it) => it.status === "failed").length;
  if (failedCount > 0) {
    return `${doneCount} completed • ${failedCount} failed`;
  }
  return `${doneCount} of ${total} completed`;
}

function renderResumeNotice(unattachedSaved) {
  if (!unattachedSaved.length) return "";
  return `
    <div class="uq-resume-notice">
      ${unattachedSaved
        .map((entry) => {
          const pct =
            entry.totalSize > 0
              ? Math.min(100, Math.round(((entry.receivedBytes || 0) / entry.totalSize) * 100))
              : 0;
          return `
            <div class="uq-resume-row">
              <span class="uq-resume-text">
                Interrupted upload: <strong>${escapeHtml(entry.filename || "file")}</strong> (${pct}%) — re-select file to resume
              </span>
              <button
                type="button"
                class="secondary btn-xs"
                data-action="discard-saved"
                data-session-id="${escapeHtml(entry.sessionId)}"
                data-fingerprint="${escapeHtml(entry.fingerprint)}"
              >Discard</button>
            </div>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderItemActions(item) {
  const idAttr = `data-id="${escapeHtml(item.id)}"`;
  switch (item.status) {
    case "queued":
    case "checking":
      return `
        <button type="button" class="secondary btn-xs" data-action="pause" ${idAttr}>Pause</button>
        <button type="button" class="danger btn-xs" data-action="cancel" ${idAttr}>Cancel</button>
      `;
    case "hashing": {
      const skipCheckBtn =
        item.totalSize > INLINE_HASH_LIMIT
          ? `<button type="button" class="secondary btn-xs" data-action="skip-check" ${idAttr}>Skip Check</button>`
          : "";
      return `
        ${skipCheckBtn}
        <button type="button" class="secondary btn-xs" data-action="pause" ${idAttr}>Pause</button>
        <button type="button" class="danger btn-xs" data-action="cancel" ${idAttr}>Cancel</button>
      `;
    }
    case "uploading":
      return `
        <button type="button" class="secondary btn-xs" data-action="pause" ${idAttr}>Pause</button>
        <button type="button" class="danger btn-xs" data-action="cancel" ${idAttr}>Cancel</button>
      `;
    case "completing":
      return `<span class="small-muted">Finalizing...</span>`;
    case "paused":
      return `
        <button type="button" class="secondary btn-xs" data-action="resume" ${idAttr}>Resume</button>
        <button type="button" class="danger btn-xs" data-action="cancel" ${idAttr}>Cancel</button>
      `;
    case "duplicate":
      return `
        <button type="button" class="secondary btn-xs" data-action="open-duplicate" ${idAttr}>Open Existing</button>
        <button type="button" class="secondary btn-xs" data-action="skip-duplicate" ${idAttr}>Skip</button>
        <button type="button" class="btn-xs" data-action="force-upload" ${idAttr}>Force Upload</button>
      `;
    case "failed":
    case "cancelled":
      return `
        <button type="button" class="secondary btn-xs" data-action="retry" ${idAttr}>Retry</button>
        <button type="button" class="secondary btn-xs" data-action="dismiss" ${idAttr}>Dismiss</button>
      `;
    case "done":
    case "skipped":
      return `<button type="button" class="secondary btn-xs" data-action="dismiss" ${idAttr}>Dismiss</button>`;
    default:
      return "";
  }
}

function renderItemCard(item) {
  const isDone = item.status === "done";
  const isFailed = item.status === "failed";
  const isCancelled = item.status === "cancelled" || item.status === "skipped";
  const pct = Math.max(0, Math.min(100, Math.round(item.pct || 0)));

  const barCls = isDone
    ? "bar bar-done"
    : isFailed
      ? "bar bar-failed"
      : isCancelled
        ? "bar bar-cancelled"
        : "bar";

  const pillCls = isDone
    ? "pill ok"
    : isFailed
      ? "pill err"
      : item.status === "duplicate" || item.status === "paused"
        ? "pill warn"
        : "pill";

  let detailLine = escapeHtml(item.statusText || item.status);
  if (item.status === "uploading") {
    const speedTxt = fmtSpeed(item.speedBps);
    const etaTxt = fmtEta(item.etaSec);
    const progressTxt = `${pct}% (${fmtSize(item.uploadedBytes)} / ${fmtSize(item.totalSize)})`;
    const extras = [progressTxt, speedTxt, etaTxt ? `ETA ${etaTxt}` : ""].filter(Boolean);
    detailLine = escapeHtml(extras.join(" • "));
  } else if (item.status === "hashing") {
    const raw = item.statusText || "Hashing";
    detailLine = escapeHtml(raw.includes("%") ? raw : `${raw} (${pct}%)`);
  }

  let dupPrompt = "";
  if (item.status === "duplicate" && item.duplicateMedia) {
    const dupTitle = item.duplicateMedia.title || item.duplicateMedia.filename || "Existing file";
    const dupFile = item.duplicateMedia.filename || "";
    const dupFolder = item.duplicateMedia.folder || "General";
    dupPrompt = `
      <div class="uq-dup-box small-muted">
        Already exists in <strong>${escapeHtml(dupFolder)}</strong>:
        <strong>${escapeHtml(dupTitle)}</strong>${dupFile && dupFile !== dupTitle ? ` (${escapeHtml(dupFile)})` : ""}
      </div>
    `;
  }

  return `
    <div class="uq-card job-item" data-item-id="${escapeHtml(item.id)}" data-status="${escapeHtml(item.status)}">
      <div class="uq-card-head job-head">
        <span class="uq-title job-title">
          <strong>${escapeHtml(item.filename)}</strong>
          <span class="small-muted"> → ${escapeHtml(item.folder)} • ${escapeHtml(fmtSize(item.totalSize))}</span>
        </span>
        <span class="uq-meta job-meta">
          <span class="${pillCls}">${escapeHtml(item.status)}</span>
          <span class="job-msg">${detailLine}</span>
        </span>
      </div>
      ${dupPrompt}
      <div class="${barCls}"><div style="width:${pct}%"></div></div>
      <div class="uq-card-actions toolbar-group">
        ${renderItemActions(item)}
      </div>
    </div>
  `;
}

function renderQueue() {
  const mount = $("uploadQueue");
  if (!mount) return;

  const unattachedSaved = getUnattachedSavedSessions();
  if (!queueItems.length && !unattachedSaved.length) {
    mount.innerHTML = "";
    mount.hidden = true;
    return;
  }

  mount.hidden = false;

  const hasPausable = queueItems.some((it) =>
    ["queued", "checking", "hashing", "uploading"].includes(it.status)
  );
  const hasPaused = queueItems.some((it) => it.status === "paused");
  const hasClearable = queueItems.some((it) => TERMINAL_CLEARABLE_STATUSES.has(it.status));

  const headerHtml = `
    <div class="uq-header">
      <div class="uq-summary"><strong>${escapeHtml(buildSummaryText(unattachedSaved))}</strong></div>
      <div class="uq-actions toolbar-group">
        ${hasPausable ? `<button type="button" class="secondary btn-xs" data-action="pause-all">Pause All</button>` : ""}
        ${hasPaused ? `<button type="button" class="secondary btn-xs" data-action="resume-all">Resume All</button>` : ""}
        ${hasClearable ? `<button type="button" class="secondary btn-xs" data-action="clear-finished">Clear Finished</button>` : ""}
      </div>
    </div>
  `;

  const noticeHtml = renderResumeNotice(unattachedSaved);
  const itemsHtml = queueItems.length
    ? `<div class="uq-list">${queueItems.map(renderItemCard).join("")}</div>`
    : "";

  mount.innerHTML = `${headerHtml}${noticeHtml}${itemsHtml}`;
}

async function handleQueueClick(e) {
  const btn = e.target.closest("button[data-action]");
  if (!btn) return;
  const action = btn.dataset.action;

  if (action === "pause-all") {
    pauseAll();
    return;
  }
  if (action === "resume-all") {
    resumeAll();
    return;
  }
  if (action === "clear-finished") {
    clearFinished();
    return;
  }
  if (action === "discard-saved") {
    const sessionId = btn.dataset.sessionId;
    const fingerprint = btn.dataset.fingerprint;
    btn.disabled = true;
    removeSavedSession(fingerprint, sessionId);
    renderQueue();
    if (sessionId) {
      try {
        await requestJson(`/api/uploads/sessions/${encodeURIComponent(sessionId)}`, {
          method: "DELETE",
        });
      } catch (err) {
        if (err?.status && err.status !== 404 && err.status !== 410) {
          toast(`Could not discard server session: ${err.message}`, { type: "error" });
        }
      }
    }
    return;
  }

  const itemId = btn.dataset.id;
  const item = queueItems.find((it) => it.id === itemId);
  if (!item) return;

  switch (action) {
    case "pause":
      pauseItemByUser(item);
      break;
    case "resume":
      resumeItem(item);
      break;
    case "cancel":
      await cancelItem(item);
      break;
    case "retry":
      retryItem(item);
      break;
    case "dismiss":
      dismissItem(item.id);
      break;
    case "skip-check":
      if (item.status === "hashing") {
        item.skipDuplicateHash = true;
        item._skipCheckAborted = true;
        item.abortController?.abort();
      }
      break;
    case "open-duplicate":
      if (item.duplicateMedia?.id != null) {
        emit("media:open", { id: item.duplicateMedia.id });
      }
      break;
    case "skip-duplicate":
      item.status = "skipped";
      item.statusText = "Skipped (duplicate)";
      notifyActiveCountIfChanged();
      renderQueue();
      kickScheduler();
      break;
    case "force-upload":
      item.force = true;
      item.duplicateMedia = null;
      item.error = null;
      item.status = "queued";
      item.statusText = "Waiting in queue (forced)";
      notifyActiveCountIfChanged();
      renderQueue();
      kickScheduler();
      break;
  }
}

function handleOffline() {
  let changed = false;
  for (const item of queueItems) {
    if (["queued", "checking", "hashing", "uploading"].includes(item.status)) {
      item.status = "paused";
      item.pausedReason = "offline";
      item.autoResumeOnOnline = true;
      item.speedBps = 0;
      item.etaSec = null;
      item.statusText = "Waiting for network...";
      abortItemNetwork(item);
      changed = true;
    }
  }
  if (changed) {
    notifyActiveCountIfChanged();
    renderQueue();
  }
}

function handleOnline() {
  let resumed = false;
  for (const item of queueItems) {
    if (item.status === "paused" && item.autoResumeOnOnline) {
      item.pausedReason = null;
      item.autoResumeOnOnline = false;
      item.error = null;
      item.status = "queued";
      item.statusText = "Network restored — resuming...";
      resumed = true;
    }
  }
  if (resumed) {
    notifyActiveCountIfChanged();
    renderQueue();
    kickScheduler();
  }
}

export function initUploadQueue() {
  if (initialized) {
    renderQueue();
    return;
  }
  initialized = true;

  // Prune any expired sessions from localStorage on startup
  readSavedSessions();

  const mount = $("uploadQueue");
  mount?.addEventListener("click", handleQueueClick);

  window.addEventListener("offline", handleOffline);
  window.addEventListener("online", handleOnline);

  renderQueue();
}
