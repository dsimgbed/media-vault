import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch, withToken } from "/static/js/api.js";
import { $, escapeHtml } from "/static/js/utils.js";
import { toast } from "/static/js/ui/dialogs.js";

function shouldForceIngest() {
  return Boolean($("forceIngestCheck") && $("forceIngestCheck").checked);
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

export async function applyJobsList(jobs, { pollIfDisconnected = true } = {}) {
  const strip = $("jobsStrip");
  const active = jobs.filter((j) => ["queued", "downloading", "allocating"].includes(j.status));
  const doneCount = jobs.filter((j) => j.status === "done").length;

  emit("jobs:active-count", { count: active.length });

  const shown = jobs.slice(0, 8);
  strip.innerHTML = shown
    .map((j) => {
      const isDone = j.status === "done";
      const isFailed = j.status === "failed";
      const isCancelled = j.status === "cancelled";
      const isActive = ["queued", "downloading", "allocating"].includes(j.status);
      const pct = Math.max(0, Math.min(100, Math.round(j.progress || (isDone ? 100 : 0))));
      const barCls = isDone
        ? "bar bar-done"
        : (isFailed ? "bar bar-failed" : (isCancelled ? "bar bar-cancelled" : "bar"));
      const actionBtn = isActive
        ? `<button type="button" class="danger btn-xs job-cancel-btn" data-id="${escapeHtml(j.id)}">Cancel</button>`
        : ((isFailed || isCancelled)
          ? `<button type="button" class="secondary btn-xs job-retry-btn" data-id="${escapeHtml(j.id)}">Retry</button>`
          : "");
      return `
        <div class="job-item">
          <div class="job-head">
            <span class="job-title">
              <strong>[${escapeHtml(j.source_origin)}]</strong> ${escapeHtml(j.source_ref)}
            </span>
            <span class="job-meta">
              <span class="status-tag status-${escapeHtml(j.status)}">${escapeHtml(j.status)}${(!isDone && !isFailed && !isCancelled) ? ` (${pct}%)` : ""}</span>
              <span class="job-msg">${escapeHtml(j.message || "")}</span>
              ${actionBtn}
            </span>
          </div>
          <div class="${barCls}"><div style="width:${pct}%"></div></div>
        </div>
      `;
    })
    .join("");

  const newlyFinished =
    (state.hadActiveJobs && active.length === 0) ||
    (state.lastDoneCount >= 0 && doneCount > state.lastDoneCount);
  state.hadActiveJobs = active.length > 0;
  state.lastDoneCount = doneCount;

  if (newlyFinished) {
    if (state.mediaRefreshTimer) {
      clearTimeout(state.mediaRefreshTimer);
      state.mediaRefreshTimer = null;
    }
    emit("media:changed", { source: "jobs:finished" });
  }
  if (pollIfDisconnected && !state.sseConnected) {
    scheduleJobPoll(active.length > 0);
  }
}

export async function loadJobs() {
  const data = await apiFetch("/api/jobs?limit=8");
  await applyJobsList(data.jobs || []);
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
        if (Array.isArray(payload.jobs)) {
          await applyJobsList(payload.jobs, { pollIfDisconnected: false });
        } else {
          await loadJobs();
        }
        if (payload.status === "done") {
          scheduleMediaRefresh();
        }
      } else if (payload.type === "media_update") {
        scheduleMediaRefresh();
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
        retryBtn.disabled = false;
      }
    }
  });

  on("auth:unlocked", () => {
    connectEvents();
  });
}
