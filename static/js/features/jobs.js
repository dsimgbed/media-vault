import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch, withToken } from "/static/js/api.js";
import { $, escapeHtml } from "/static/js/utils.js";
import { toast } from "/static/js/ui/dialogs.js";
import { getActiveUploadCount } from "/static/js/features/upload-queue.js";

const ACTIVE_JOB_STATUSES = new Set(["queued", "downloading", "allocating"]);
const HISTORY_PAGE_SIZE = 25;

let latestOverviewJobs = [];
let lastActiveJobsCount = 0;
let isLoadingHistory = false;

function shouldForceIngest() {
  return Boolean($("forceIngestCheck") && $("forceIngestCheck").checked);
}

function emitCombinedActiveCount(activeJobsCount = lastActiveJobsCount) {
  lastActiveJobsCount = Number(activeJobsCount || 0);
  const uploadCount = getActiveUploadCount();
  emit("jobs:active-count", {
    count: lastActiveJobsCount + uploadCount,
    jobsCount: lastActiveJobsCount,
    uploadsCount: uploadCount,
  });
}

function getJobMediaIds(j) {
  if (Array.isArray(j.media_ids)) return j.media_ids.filter(Boolean);
  if (typeof j.media_ids_json === "string" && j.media_ids_json) {
    try {
      const parsed = JSON.parse(j.media_ids_json);
      if (Array.isArray(parsed)) return parsed.filter(Boolean);
    } catch {
      // ignore malformed json
    }
  }
  return [];
}

function fmtTimeShort(val) {
  if (!val) return "";
  const d =
    typeof val === "number"
      ? new Date(val < 1e12 ? val * 1000 : val)
      : new Date(val);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function computeLocalCounts(jobs) {
  let active = 0;
  let done = 0;
  let failed = 0;
  let cancelled = 0;
  for (const j of jobs) {
    if (ACTIVE_JOB_STATUSES.has(j.status)) active++;
    else if (j.status === "done") done++;
    else if (j.status === "failed") failed++;
    else if (j.status === "cancelled") cancelled++;
  }
  return {
    all: jobs.length,
    active,
    done,
    failed,
    cancelled,
  };
}

function renderJobItem(j) {
  const isDone = j.status === "done";
  const isFailed = j.status === "failed";
  const isCancelled = j.status === "cancelled";
  const isActive = ACTIVE_JOB_STATUSES.has(j.status);
  const pct = Math.max(0, Math.min(100, Math.round(j.progress || (isDone ? 100 : 0))));
  const barCls = isDone
    ? "bar bar-done"
    : isFailed
      ? "bar bar-failed"
      : isCancelled
        ? "bar bar-cancelled"
        : "bar";

  const mediaIds = getJobMediaIds(j);
  let actionHtml = "";
  if (isActive) {
    actionHtml = `<button type="button" class="danger btn-xs job-cancel-btn" data-id="${escapeHtml(j.id)}">Cancel</button>`;
  } else if (isDone && mediaIds.length > 0) {
    const label = mediaIds.length > 1 ? `View Media (${mediaIds.length})` : "View Media";
    actionHtml = `<button type="button" class="secondary btn-xs job-media-link" data-media-id="${escapeHtml(mediaIds[0])}">${label}</button>`;
  } else if (isFailed || isCancelled) {
    if (j.retryable !== false) {
      const expiresAt = fmtTimeShort(j.staged_expires_at);
      const hint = expiresAt
        ? `<span class="job-retry-hint">Staged file kept until ${escapeHtml(expiresAt)}</span>`
        : "";
      actionHtml = `${hint}<button type="button" class="secondary btn-xs job-retry-btn" data-id="${escapeHtml(j.id)}">Retry</button>`;
    } else {
      actionHtml = `<span class="job-retry-hint">Re-upload required</span>`;
    }
  }

  return `
    <div class="job-item" data-job-id="${escapeHtml(j.id)}">
      <div class="job-head">
        <span class="job-title">
          <strong>[${escapeHtml(j.source_origin)}]</strong> ${escapeHtml(j.source_ref)}
        </span>
        <span class="job-meta">
          <span class="status-tag status-${escapeHtml(j.status)}">${escapeHtml(j.status)}${!isDone && !isFailed && !isCancelled ? ` (${pct}%)` : ""}</span>
          <span class="job-msg">${escapeHtml(j.message || "")}</span>
          ${actionHtml}
        </span>
      </div>
      <div class="${barCls}"><div style="width:${pct}%"></div></div>
    </div>
  `;
}

function renderJobCenter() {
  const strip = $("jobsStrip");
  if (!strip) return;

  const counts = state.jobCounts || { all: 0, active: 0, done: 0, failed: 0, cancelled: 0 };
  if (counts.all === 0 && latestOverviewJobs.length === 0) {
    strip.innerHTML = "";
    return;
  }

  const activeJobs = latestOverviewJobs.filter((j) => ACTIVE_JOB_STATUSES.has(j.status));
  const recentFinished = latestOverviewJobs
    .filter((j) => !ACTIVE_JOB_STATUSES.has(j.status))
    .slice(0, 5);
  const overviewItems = state.jobCenterExpanded
    ? activeJobs
    : [...activeJobs, ...recentFinished];

  const completedTotal = Number(counts.done || 0) + Number(counts.cancelled || 0);
  const headerHtml = `
    <div class="jc-header">
      <div class="jc-title-row">
        <strong class="jc-title">Jobs</strong>
        <div class="jc-counts">
          ${counts.active > 0 ? `<span class="pill warn">Active: ${counts.active}</span>` : ""}
          ${counts.done > 0 ? `<span class="pill ok">Done: ${counts.done}</span>` : ""}
          ${counts.failed > 0 ? `<span class="pill err">Failed: ${counts.failed}</span>` : ""}
          ${counts.cancelled > 0 ? `<span class="pill">Cancelled: ${counts.cancelled}</span>` : ""}
        </div>
      </div>
      <div class="jc-actions toolbar-group">
        ${counts.failed > 0 ? `<button type="button" class="secondary btn-xs" data-jc-action="retry-failed">Retry All Failed</button>` : ""}
        ${completedTotal > 0 ? `<button type="button" class="secondary btn-xs" data-jc-action="clear-completed">Clear Completed</button>` : ""}
        <button type="button" class="secondary btn-xs" data-jc-action="toggle-drawer" aria-expanded="${state.jobCenterExpanded ? "true" : "false"}">${state.jobCenterExpanded ? "Hide History" : `History (${counts.all})`}</button>
      </div>
    </div>
  `;

  const activeListHtml = overviewItems.length
    ? `<div class="jc-active-list">${overviewItems.map(renderJobItem).join("")}</div>`
    : "";

  let drawerHtml = "";
  if (state.jobCenterExpanded) {
    const filters = [
      { key: "all", label: "All", count: counts.all },
      { key: "active", label: "Active", count: counts.active },
      { key: "done", label: "Done", count: counts.done },
      { key: "failed", label: "Failed", count: counts.failed },
      { key: "cancelled", label: "Cancelled", count: counts.cancelled },
    ];
    const filterPills = filters
      .map(
        (f) =>
          `<button type="button" class="jc-filter-btn${state.jobFilterStatus === f.key ? " is-active" : ""}" data-jc-filter="${f.key}">${f.label} (${Number(f.count || 0)})</button>`
      )
      .join("");

    let historyBody = "";
    if (isLoadingHistory && state.jobHistoryItems.length === 0) {
      historyBody = `<div class="jc-empty">Loading job history...</div>`;
    } else if (state.jobHistoryItems.length === 0) {
      historyBody = `<div class="jc-empty">No jobs match this filter.</div>`;
    } else {
      historyBody = state.jobHistoryItems.map(renderJobItem).join("");
    }

    const loadMoreHtml = state.jobHistoryHasMore
      ? `<div class="jc-load-more-wrap"><button type="button" class="secondary btn-sm jc-load-more" data-jc-action="load-more"${isLoadingHistory ? " disabled" : ""}>${isLoadingHistory ? "Loading..." : "Load More"}</button></div>`
      : "";

    drawerHtml = `
      <div class="jc-drawer">
        <div class="jc-filters" role="tablist" aria-label="Filter job history">${filterPills}</div>
        <div class="jc-history-list">${historyBody}</div>
        ${loadMoreHtml}
      </div>
    `;
  }

  strip.innerHTML = `${headerHtml}${activeListHtml}${drawerHtml}`;
}

async function loadJobHistory({ reset = true } = {}) {
  const offset = reset ? 0 : state.jobHistoryItems.length;
  const statusParam =
    state.jobFilterStatus && state.jobFilterStatus !== "all"
      ? `&status=${encodeURIComponent(state.jobFilterStatus)}`
      : "";

  isLoadingHistory = true;
  if (reset) {
    renderJobCenter();
  }

  try {
    const data = await apiFetch(
      `/api/jobs?limit=${HISTORY_PAGE_SIZE}&offset=${offset}${statusParam}`
    );
    const items = Array.isArray(data.jobs) ? data.jobs : [];
    state.jobHistoryItems = reset ? items : [...state.jobHistoryItems, ...items];
    state.jobHistoryTotal = Number(data.total ?? state.jobHistoryItems.length);
    state.jobHistoryHasMore = Boolean(data.has_more);
    if (data.counts && typeof data.counts === "object") {
      state.jobCounts = {
        all: Number(data.counts.all || 0),
        active: Number(data.counts.active || 0),
        done: Number(data.counts.done || 0),
        failed: Number(data.counts.failed || 0),
        cancelled: Number(data.counts.cancelled || 0),
      };
    }
  } finally {
    isLoadingHistory = false;
    renderJobCenter();
  }
}

export function stopJobPoll() {
  if (state.jobPollTimer) {
    clearTimeout(state.jobPollTimer);
    state.jobPollTimer = null;
  }
}

export function scheduleJobPoll(hasActive) {
  stopJobPoll();
  if (state.sseConnected) return;
  const delay = hasActive ? 1000 : 4000;
  state.jobPollTimer = setTimeout(async () => {
    if (state.sseConnected) return;
    if (!document.hidden) {
      try {
        await loadJobs();
      } catch (_) {}
    } else {
      scheduleJobPoll(hasActive);
    }
  }, delay);
}

function scheduleMediaRefresh() {
  if (state.mediaRefreshTimer) return;
  state.mediaRefreshTimer = setTimeout(() => {
    state.mediaRefreshTimer = null;
    emit("media:changed", { source: "sse" });
  }, 150);
}

export async function applyJobsList(
  jobs,
  { pollIfDisconnected = true, counts = null } = {}
) {
  const list = Array.isArray(jobs) ? jobs : [];
  latestOverviewJobs = list;

  const active = list.filter((j) => ACTIVE_JOB_STATUSES.has(j.status));
  const sliceDoneCount = list.filter((j) => j.status === "done").length;
  const doneCount =
    counts && typeof counts === "object" && counts.done !== undefined
      ? Number(counts.done)
      : sliceDoneCount;

  if (counts && typeof counts === "object") {
    state.jobCounts = {
      all: Number(counts.all ?? list.length),
      active: Number(counts.active ?? active.length),
      done: doneCount,
      failed: Number(counts.failed ?? 0),
      cancelled: Number(counts.cancelled ?? 0),
    };
  } else if (state.jobCounts.all === 0 || list.length < 8) {
    state.jobCounts = computeLocalCounts(list);
  } else {
    state.jobCounts = { ...state.jobCounts, active: active.length };
  }

  const activeTotal =
    counts && typeof counts === "object" && counts.active !== undefined
      ? Number(counts.active)
      : active.length;
  emitCombinedActiveCount(activeTotal);
  renderJobCenter();

  const newlyFinished =
    (state.hadActiveJobs && activeTotal === 0) ||
    (state.lastDoneCount >= 0 && doneCount > state.lastDoneCount);
  state.hadActiveJobs = activeTotal > 0;
  state.lastDoneCount = doneCount;

  if (newlyFinished) {
    if (state.mediaRefreshTimer) {
      clearTimeout(state.mediaRefreshTimer);
      state.mediaRefreshTimer = null;
    }
    emit("media:changed", { source: "jobs:finished" });
  }
  if (pollIfDisconnected && !state.sseConnected) {
    scheduleJobPoll(activeTotal > 0);
  }
  return newlyFinished;
}

export async function loadJobs() {
  const data = await apiFetch("/api/jobs?limit=25");
  await applyJobsList(data.jobs || [], { counts: data.counts || null });
  if (state.jobCenterExpanded) {
    await loadJobHistory({ reset: true });
  }
}

export function connectEvents() {
  if (state.sse) {
    state.sse.close();
    state.sse = null;
  }
  state.sseConnected = false;
  if (state.authRequired && !state.token) return;
  if (typeof EventSource === "undefined") {
    scheduleJobPoll(state.hadActiveJobs);
    return;
  }

  const es = new EventSource(withToken("/api/events"));
  state.sse = es;

  es.onopen = () => {
    state.sseConnected = true;
    stopJobPoll();
  };

  es.onmessage = async (ev) => {
    state.sseConnected = true;
    stopJobPoll();
    try {
      const payload = JSON.parse(ev.data);
      if (payload.type === "connected" || payload.type === "job_update") {
        let didEmitFinished = false;
        const isStructural =
          payload.type === "connected" ||
          ["done", "failed", "cancelled", "cleared", "queued"].includes(payload.status);
        if (Array.isArray(payload.jobs)) {
          didEmitFinished = await applyJobsList(payload.jobs, {
            pollIfDisconnected: false,
            counts: payload.counts || null,
          });
          if (
            !payload.counts &&
            ["done", "failed", "cancelled"].includes(payload.status)
          ) {
            loadJobs().catch(() => {});
          } else if (state.jobCenterExpanded && isStructural) {
            loadJobHistory({ reset: true }).catch(() => {});
          }
        } else if (isStructural) {
          await loadJobs();
        }
        if (payload.status === "done" && !didEmitFinished) {
          scheduleMediaRefresh();
        }
      } else if (payload.type === "media_update") {
        scheduleMediaRefresh();
      } else if (payload.type === "maintenance_update") {
        emit("maintenance:update", payload);
      } else if (payload.type === "backup_update") {
        emit("backup:update", payload);
      }
    } catch (err) {
      console.warn("SSE event error:", err);
    }
  };

  es.onerror = () => {
    state.sseConnected = false;
    scheduleJobPoll(state.hadActiveJobs);
  };
}

export function initJobs() {
  const strip = $("jobsStrip");
  strip?.addEventListener("click", async (e) => {
    const mediaLink = e.target.closest(".job-media-link");
    if (mediaLink) {
      const id = mediaLink.dataset.mediaId;
      if (id) emit("media:open", { id });
      return;
    }

    const filterBtn = e.target.closest("[data-jc-filter]");
    if (filterBtn) {
      state.jobFilterStatus = filterBtn.dataset.jcFilter || "all";
      state.jobHistoryItems = [];
      state.jobHistoryHasMore = false;
      try {
        await loadJobHistory({ reset: true });
      } catch (err) {
        toast(err.message, { type: "error" });
      }
      return;
    }

    const jcBtn = e.target.closest("[data-jc-action]");
    if (jcBtn) {
      const action = jcBtn.dataset.jcAction;
      if (action === "toggle-drawer") {
        state.jobCenterExpanded = !state.jobCenterExpanded;
        if (state.jobCenterExpanded) {
          try {
            await loadJobHistory({ reset: true });
          } catch (err) {
            toast(err.message, { type: "error" });
          }
        } else {
          renderJobCenter();
        }
        return;
      }

      if (action === "load-more") {
        try {
          await loadJobHistory({ reset: false });
        } catch (err) {
          toast(err.message, { type: "error" });
        }
        return;
      }

      if (action === "retry-failed") {
        jcBtn.disabled = true;
        try {
          const res = await apiFetch("/api/jobs/retry-failed", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ force: shouldForceIngest() }),
          });
          const count = Number(res.retried_count ?? res.retried ?? res.count ?? 0);
          const skipped = Number(res.skipped_count ?? 0);
          const msg =
            skipped > 0
              ? `Retried ${count} failed job(s) (${skipped} skipped — expired or non-retryable).`
              : `Retried ${count} failed job(s).`;
          toast(msg, { type: count > 0 ? "success" : "info" });
          await loadJobs();
        } catch (err) {
          toast(err.message, { type: "error" });
        } finally {
          jcBtn.disabled = false;
        }
        return;
      }

      if (action === "clear-completed") {
        jcBtn.disabled = true;
        try {
          const res = await apiFetch("/api/jobs/clear-completed", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ statuses: ["done", "cancelled"] }),
          });
          const count = Number(res.deleted ?? res.cleared ?? res.count ?? 0);
          toast(`Cleared ${count} completed job(s).`, { type: "info" });
          await loadJobs();
        } catch (err) {
          toast(err.message, { type: "error" });
        } finally {
          jcBtn.disabled = false;
        }
        return;
      }
    }

    const cancelBtn = e.target.closest(".job-cancel-btn");
    if (cancelBtn) {
      cancelBtn.disabled = true;
      try {
        await apiFetch(`/api/jobs/${encodeURIComponent(cancelBtn.dataset.id)}/cancel`, {
          method: "POST",
        });
        await loadJobs();
      } catch (err) {
        toast(err.message, { type: "error" });
      } finally {
        cancelBtn.disabled = false;
      }
      return;
    }

    const retryBtn = e.target.closest(".job-retry-btn");
    if (retryBtn) {
      retryBtn.disabled = true;
      try {
        await apiFetch(`/api/jobs/${encodeURIComponent(retryBtn.dataset.id)}/retry`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ force: shouldForceIngest() }),
        });
        await loadJobs();
      } catch (err) {
        toast(err.message, { type: "error" });
      } finally {
        retryBtn.disabled = false;
      }
    }
  });

  on("jobs:refresh", () => loadJobs().catch(() => {}));
  on("uploads:changed", () => emitCombinedActiveCount(lastActiveJobsCount));
  on("auth:unlocked", () => {
    connectEvents();
  });
}
