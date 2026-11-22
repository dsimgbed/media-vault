import { state } from "/static/js/state.js";
import { emit } from "/static/js/bus.js";
import { apiFetch } from "/static/js/api.js";
import { $, escapeHtml } from "/static/js/utils.js";
import { confirmDialog, toast } from "/static/js/ui/dialogs.js";

const ENTRY_LIMIT = 50;
const INTERVAL_PRESETS = [
  { label: "5m", value: 5 },
  { label: "15m", value: 15 },
  { label: "30m", value: 30 },
  { label: "1h", value: 60 },
  { label: "6h", value: 360 },
  { label: "1d", value: 1440 },
];
const ENTRY_FILTERS = [
  { id: "all", label: "All" },
  { id: "ingested", label: "Ingested" },
  { id: "queued", label: "Queued" },
  { id: "seen", label: "Seen" },
  { id: "skipped", label: "Skipped" },
  { id: "failed", label: "Failed" },
];

let subsCache = null;
let subsLoading = false;
let currentView = null;
let formMode = null; // { kind: "create" } or { kind: "edit", id }
let subDraft = defaultDraft();
let subSaving = false;

const previews = new Map();
const drawers = new Map();
const busyActions = new Set();

function defaultDraft() {
  return {
    source_url: "",
    name: "",
    source_kind: "auto",
    folder: String(state.defaultFolder || "General"),
    tags: "",
    format_spec: "",
    interval_minutes: 60,
    max_items_per_check: 10,
    seed_only: true,
    initial_backfill_limit: 0,
    title_include_regex: "",
    title_exclude_regex: "",
    forward_to_telegram: true,
    enabled: true,
  };
}

function isLocked() {
  return Boolean(state.authRequired && !state.token);
}

function resolveView(e) {
  const el = e?.currentTarget;
  if (el && typeof el.querySelector === "function" && el.id !== "subForm") return el;
  return currentView || $("systemMaintenanceView");
}

function findForm(view) {
  if (!view) return null;
  if (view.id === "subForm") return view;
  return view.querySelector("#subForm") || null;
}

function n(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function E(value) {
  return escapeHtml(value ?? "");
}

function fmtDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function clampInt(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, Math.round(num)));
}

function findSub(id, data = subsCache) {
  const list = Array.isArray(data?.subscriptions) ? data.subscriptions : [];
  return list.find((s) => String(s?.id) === String(id)) || null;
}

function isPaused(sub) {
  if (!sub) return false;
  const st = String(sub.last_status ?? sub.status ?? "").toLowerCase();
  return sub.enabled === false || sub.paused === true || st === "paused" || st === "paused_error";
}

function isSeeded(sub) {
  if (!sub) return false;
  const st = String(sub.last_status ?? sub.status ?? "").toLowerCase();
  return Boolean(
    sub.seeded ||
      sub.seeded_at ||
      sub.first_run_complete ||
      sub.initial_seed_complete ||
      st === "seeded"
  );
}

function subKey(id, action) {
  return `${id}:${action}`;
}

function isBusy(id, action) {
  return busyActions.has(subKey(id, action));
}

/** Only http(s) URLs become clickable links; everything else stays inert text. */
function renderSafeUrl(rawUrl, label) {
  const url = String(rawUrl ?? "").trim();
  const text = E(label ?? url ?? "");
  if (/^https?:\/\//i.test(url)) {
    return `<a class="sub-url" href="${E(url)}" rel="noopener noreferrer" target="_blank" title="${E(url)}">${text}</a>`;
  }
  return `<span class="sub-url" title="${E(url)}">${text}</span>`;
}

function renderTitleLink(title, url) {
  const label = String(title || url || "(untitled)");
  return renderSafeUrl(url, label);
}

function renderLocked(view) {
  view.innerHTML = `
    <div class="sys-card sys-locked-card">
      <div class="sys-card-head">
        <strong>Vault Locked</strong>
        <span class="pill warn">Auth required</span>
      </div>
      <p class="small-muted sys-card-copy">
        Unlock vault in Settings to manage source subscriptions.
      </p>
      <div class="sys-actions-row">
        <button type="button" class="secondary btn-sm" data-sys-goto="settings">Open Settings</button>
      </div>
    </div>
  `;
}

function renderLoading(view, label) {
  view.innerHTML = `<div class="sys-card sys-loading">${E(label)}</div>`;
}

function renderError(view, message) {
  view.innerHTML = `
    <div class="sys-card sys-error-card">
      <div class="sys-card-head">
        <strong>Failed to Load Subscriptions</strong>
        <button type="button" class="secondary btn-xs" data-sub-retry>Retry</button>
      </div>
      <div class="sys-error-msg">${E(message)}</div>
    </div>
  `;
}

function renderSummary(summary) {
  const s = summary || {};
  const errored = n(s.errored);
  const pausedError = n(s.paused_error);
  const running = Boolean(s.scheduler_running);
  return `
    <div class="sys-section-bar sub-summary-bar">
      <div class="sys-pills-row">
        <span class="pill ok">Active: ${E(n(s.enabled))}</span>
        <span class="pill warn">Paused: ${E(n(s.paused))}</span>
        <span class="pill ${errored > 0 ? "err" : ""}">Errors: ${E(errored)}</span>
        ${pausedError > 0 ? `<span class="pill err">Auto-paused: ${E(pausedError)}</span>` : ""}
        <span class="pill">Total Archived: ${E(n(s.total_ingested))}</span>
        <span class="pill ${running ? "ok" : "warn"}">Scheduler: ${running ? "Running" : "Off"}</span>
      </div>
      <div class="sys-actions-row">
        <button type="button" class="btn-sm" data-sub-action="add">+ Add Subscription</button>
        <button type="button" class="secondary btn-xs" data-sub-action="refresh">Refresh</button>
      </div>
    </div>
  `;
}

function draftForSub(sub) {
  const d = defaultDraft();
  if (!sub) return d;
  const tags = Array.isArray(sub.tags)
    ? sub.tags.join(", ")
    : String(sub.tags ?? "");
  return {
    ...d,
    source_url: String(sub.source_url ?? ""),
    name: String(sub.name ?? ""),
    source_kind: ["auto", "rss", "ytdlp"].includes(sub.source_kind)
      ? sub.source_kind
      : "auto",
    folder: String(sub.folder ?? d.folder),
    tags,
    format_spec: String(sub.format_spec ?? ""),
    interval_minutes: clampInt(sub.interval_minutes, 5, 10080, 60),
    max_items_per_check: clampInt(sub.max_items_per_check, 1, 100, 10),
    seed_only: sub.seed_only === undefined ? d.seed_only : Boolean(sub.seed_only),
    initial_backfill_limit: clampInt(sub.initial_backfill_limit, 0, 50, 0),
    title_include_regex: String(sub.title_include_regex ?? ""),
    title_exclude_regex: String(sub.title_exclude_regex ?? ""),
    forward_to_telegram:
      sub.forward_to_telegram === undefined
        ? d.forward_to_telegram
        : Boolean(sub.forward_to_telegram),
    enabled: sub.enabled === undefined ? d.enabled : Boolean(sub.enabled),
  };
}

function selectOptions(options, selected) {
  return options
    .map(
      ({ value, label }) =>
        `<option value="${E(value)}"${String(selected) === String(value) ? " selected" : ""}>${E(label)}</option>`
    )
    .join("");
}

function renderForm() {
  const editing = formMode?.kind === "edit" ? findSub(formMode.id) : null;
  const isEdit = Boolean(editing);
  const showFirstRun = !isEdit || !isSeeded(editing);
  const d = subDraft;

  const kindOptions = selectOptions(
    [
      { value: "auto", label: "Auto (YouTube RSS / Feed / yt-dlp)" },
      { value: "rss", label: "RSS / Atom Feed" },
      { value: "ytdlp", label: "yt-dlp Listing / Playlist" },
    ],
    d.source_kind
  );
  const formatOptions = selectOptions(
    [
      { value: "", label: "Best MP4" },
      { value: "1080p", label: "1080p" },
      { value: "720p", label: "720p" },
      { value: "480p", label: "480p" },
      { value: "audio", label: "Audio only" },
    ],
    d.format_spec
  );
  const intervalChips = INTERVAL_PRESETS.map(
    (preset) =>
      `<button type="button" class="sub-chip${n(d.interval_minutes) === preset.value ? " is-active" : ""}" data-sub-interval="${preset.value}">${E(preset.label)}</button>`
  ).join("");

  return `
    <div class="sys-card sub-form-card">
      <div class="sys-card-head">
        <strong>${isEdit ? "Edit Subscription" : "Add Subscription"}</strong>
        <button type="button" class="secondary btn-xs" data-sub-action="cancel-form">Cancel</button>
      </div>
      <p class="small-muted sys-card-copy">
        Watch a YouTube channel or playlist, a podcast/video RSS or Atom feed, or a yt-dlp
        listing. New items are checked on your schedule and archived automatically.
      </p>
      <form id="subForm" class="sub-form" autocomplete="off">
        <div class="sub-form-grid">
          <label class="sub-field sub-field-wide">
            <span>Source URL <em>(required)</em></span>
            <input id="subUrl" type="url" required spellcheck="false"
              placeholder="YouTube channel/playlist or RSS/Atom URL"
              value="${E(d.source_url)}" />
          </label>
          <label class="sub-field">
            <span>Name <em>(optional — auto-derived if blank)</em></span>
            <input id="subName" type="text" value="${E(d.name)}" placeholder="My subscription" />
          </label>
          <label class="sub-field">
            <span>Source Kind</span>
            <select id="subKind">${kindOptions}</select>
          </label>
          <label class="sub-field">
            <span>Folder</span>
            <input id="subFolder" type="text" list="folderList" value="${E(d.folder)}"
              placeholder="${E(String(state.defaultFolder || "General"))}" />
          </label>
          <label class="sub-field">
            <span>Tags <em>(comma or space separated)</em></span>
            <input id="subTags" type="text" value="${E(d.tags)}" placeholder="#podcast, #hd" />
          </label>
          <label class="sub-field">
            <span>Format Preset</span>
            <select id="subFormat">${formatOptions}</select>
          </label>
          <label class="sub-field">
            <span>Max items per check</span>
            <input id="subMaxItems" type="number" min="1" max="100"
              value="${E(n(d.max_items_per_check))}" />
          </label>
          <label class="sub-field">
            <span>Interval (minutes)</span>
            <input id="subInterval" type="number" min="5" max="10080"
              value="${E(n(d.interval_minutes))}" />
          </label>
          <label class="sub-field">
            <span>Title include regex <em>(optional)</em></span>
            <input id="subIncludeRegex" type="text" spellcheck="false"
              value="${E(d.title_include_regex)}" placeholder="^Episode" />
          </label>
          <label class="sub-field">
            <span>Title exclude regex <em>(optional)</em></span>
            <input id="subExcludeRegex" type="text" spellcheck="false"
              value="${E(d.title_exclude_regex)}" placeholder="Trailer|Teaser" />
          </label>
        </div>
        <div class="sub-interval-chips">
          <span class="sub-field-label">Quick interval:</span>
          ${intervalChips}
        </div>
        ${showFirstRun
          ? `
            <div class="sub-first-run">
              <div class="sub-first-run-head">
                <strong>First-run behavior</strong>
                <span class="small-muted">Applied only on the first check. ${isEdit ? "This subscription has not been seeded yet." : ""}</span>
              </div>
              <label class="checkbox-label sub-check">
                <input id="subSeedOnly" type="checkbox"${d.seed_only ? " checked" : ""} />
                Seed existing items as seen on first run (no backlog flood)
              </label>
              <label class="sys-inline-field">
                <span>Initial backfill limit</span>
                <input id="subBackfillLimit" type="number" min="0" max="50"
                  value="${E(n(d.initial_backfill_limit))}" />
              </label>
            </div>
          `
          : ""}
        <div class="sub-checks">
          <label class="checkbox-label sub-check">
            <input id="subForward" type="checkbox"${d.forward_to_telegram ? " checked" : ""} />
            Forward to Telegram
          </label>
          <label class="checkbox-label sub-check">
            <input id="subEnabled" type="checkbox"${d.enabled ? " checked" : ""} />
            Enabled
          </label>
        </div>
        <div class="sub-form-actions">
          <button type="submit" class="btn-sm" data-sub-action="save">${isEdit ? "Save Changes" : "Add Subscription"}</button>
          <button type="button" class="secondary btn-sm" data-sub-action="cancel-form">Cancel</button>
        </div>
      </form>
    </div>
  `;
}

function statusPill(sub) {
  const status = String(sub?.last_status ?? sub?.status ?? "").toLowerCase();
  if (status === "paused_error") {
    return `<span class="pill err" title="${E(sub?.last_error || "")}">Auto-paused</span>`;
  }
  if (isPaused(sub)) {
    return `<span class="pill warn">Paused</span>`;
  }
  if (status === "ok") return `<span class="pill ok">Active</span>`;
  if (status === "seeded") return `<span class="pill ok">Seeded</span>`;
  if (status === "error") return `<span class="pill err">Error</span>`;
  if (status === "pending") return `<span class="pill">Pending</span>`;
  return `<span class="pill">${E(status || "Unknown")}</span>`;
}

function tagsHtml(sub) {
  let tags = sub?.tags;
  if (Array.isArray(tags)) {
    // no-op: handled below
  } else if (typeof tags === "string") {
    tags = tags.split(/[\s,]+/).filter(Boolean);
  } else {
    tags = [];
  }
  if (!tags.length) return "";
  return tags
    .map((tag) => `<span class="pill pill-compact sub-tag">#${E(String(tag).replace(/^#/, ""))}</span>`)
    .join("");
}

function subButton(id, action, label, { disabled = false, danger = false } = {}) {
  const cls = danger ? "danger btn-xs" : "secondary btn-xs";
  return `<button type="button" class="${cls}" data-sub-action="${action}" data-sub-id="${E(id)}"${disabled ? " disabled" : ""}>${E(label)}</button>`;
}

function renderCard(sub) {
  const id = String(sub?.id ?? "");
  const rawTags = sub?.tags;
  const kind = String(sub?.source_kind || "auto");
  const folder = String(sub?.folder || "");
  const format = String(sub?.format_spec || "");
  const interval = n(sub?.interval_minutes);
  const archived = n(sub?.items_ingested_count ?? sub?.total_ingested ?? sub?.archived_count);
  const failures = n(sub?.consecutive_failures ?? sub?.failure_count);
  const lastError = String(sub?.last_error || "");

  const errorBanner = lastError
    ? `<div class="sub-error-banner"><span>${E(lastError)}</span>${failures > 0 ? `<span class="pill err">${E(failures)} consecutive ${failures === 1 ? "failure" : "failures"}</span>` : ""}</div>`
    : "";

  const checking = isBusy(id, "check");
  const previewing = isBusy(id, "dry-run");
  const toggling = isBusy(id, "toggle");

  const preview = previews.has(id)
    ? `<div class="sub-preview-slot">${renderPreview(id, previews.get(id))}</div>`
    : "";
  const drawer = drawers.has(id)
    ? `<div class="sub-drawer-slot">${renderDrawer(id, drawers.get(id))}</div>`
    : "";

  return `
    <div class="sys-card sub-card" data-sub-id="${E(id)}">
      <div class="sys-card-head sub-card-head">
        <div class="sub-title-wrap">
          <strong class="sub-title">${E(sub?.name || sub?.source_url || "Untitled subscription")}</strong>
          <span class="pill pill-compact">${E(kind)}</span>
          ${statusPill(sub)}
        </div>
        <div class="sys-actions-row sub-card-actions">
          ${subButton(id, "toggle", toggling ? "Saving…" : isPaused(sub) ? "Enable" : "Pause", { disabled: toggling })}
          ${subButton(id, "check", checking ? "Checking…" : "Check Now", { disabled: checking })}
          ${subButton(id, "dry-run", previewing ? "Previewing…" : "Preview / Dry Run", { disabled: previewing })}
          ${subButton(id, "entries", drawers.has(id) ? "Hide Entries" : "Entries")}
          ${subButton(id, "edit", "Edit")}
          ${subButton(id, "delete", "Delete", { danger: true })}
        </div>
      </div>
      <div class="sub-url-row">
        <span class="sub-field-label">Source</span>
        ${renderSafeUrl(sub?.source_url, sub?.source_url || "—")}
      </div>
      <div class="sub-meta-row">
        ${folder ? `<span class="pill pill-compact">Folder: ${E(folder)}</span>` : ""}
        ${tagsHtml({ tags: rawTags })}
        <span class="pill pill-compact">Format: ${E(format || "Best MP4")}</span>
        <span class="pill pill-compact">Every ${E(interval)}m</span>
        <span class="pill pill-compact">Next: ${E(fmtDate(sub?.next_check_at))}</span>
        <span class="pill pill-compact">Last: ${E(fmtDate(sub?.last_checked_at || sub?.last_check_at))}</span>
        <span class="pill pill-compact">${E(archived)} archived</span>
      </div>
      ${errorBanner}
      ${preview}
      ${drawer}
    </div>
  `;
}

function previewStat(label, value) {
  return `<div class="sub-preview-stat"><span>${E(label)}</span><strong>${E(value)}</strong></div>`;
}

function actionPill(action) {
  const a = String(action ?? "").toLowerCase();
  if (a === "queue" || a === "queued" || a === "would_queue") return `<span class="pill ok">queue</span>`;
  if (a === "seed" || a === "seeded" || a === "would_seed") return `<span class="pill ok">seed</span>`;
  if (a === "deferred") return `<span class="pill warn">deferred</span>`;
  if (a === "seen") return `<span class="pill pill-compact">seen</span>`;
  if (a === "duplicate" || a === "dup") return `<span class="pill warn">duplicate</span>`;
  if (a === "filter" || a === "filtered") return `<span class="pill warn">filtered</span>`;
  return `<span class="pill pill-compact">${E(a || "skip")}</span>`;
}

function renderPreview(id, preview) {
  const p = preview || {};
  if (p.error) {
    return `
      <div class="sub-preview sub-preview-error">
        <div class="sub-preview-head">
          <strong>Preview failed</strong>
          <button type="button" class="secondary btn-xs" data-sub-action="dismiss-preview" data-sub-id="${E(id)}">Dismiss</button>
        </div>
        <div class="sys-error-msg">${E(p.error)}</div>
      </div>
    `;
  }

  const entries = Array.isArray(p.entries) ? p.entries : [];
  const rows = entries.length
    ? entries
        .map(
          (entry) => `
            <div class="sub-preview-row">
              <span class="sub-entry-title">${renderTitleLink(entry?.title, entry?.url)}</span>
              ${actionPill(entry?.action)}
              <span class="small-muted">${E(fmtDate(entry?.published_at))}</span>
            </div>
          `
        )
        .join("")
    : `<div class="sub-empty small-muted">No previewable entries were found.</div>`;

  return `
    <div class="sub-preview">
      <div class="sub-preview-head">
        <strong>Dry-run preview</strong>
        <div class="sys-actions-row">
          <span class="pill pill-compact">${E(p.backend || "unknown")}</span>
          <button type="button" class="secondary btn-xs" data-sub-action="dismiss-preview" data-sub-id="${E(id)}">Dismiss</button>
        </div>
      </div>
      ${p.resolved_url ? `<div class="sub-preview-source">${renderSafeUrl(p.resolved_url, p.resolved_url)}</div>` : ""}
      ${p.feed_title ? `<div class="small-muted sub-preview-feed">${E(p.feed_title)}</div>` : ""}
      <div class="sub-preview-counts">
        ${previewStat("Discovered", n(p.discovered_count))}
        ${previewStat("Would queue", n(p.queued_count))}
        ${previewStat("Would seed", n(p.seeded_count))}
        ${previewStat("Seen", n(p.skipped_seen_count))}
        ${previewStat("Duplicate", n(p.skipped_duplicate_count))}
        ${previewStat("Filtered", n(p.skipped_filter_count))}
      </div>
      <div class="sub-preview-rows">${rows}</div>
    </div>
  `;
}

function entryStatusPill(status) {
  const s = String(status ?? "").toLowerCase();
  if (s === "ingested" || s === "done") return `<span class="pill ok">Ingested</span>`;
  if (s === "queued" || s === "pending") return `<span class="pill warn">Queued</span>`;
  if (s === "failed" || s === "error") return `<span class="pill err">Failed</span>`;
  if (s === "seen") return `<span class="pill pill-compact">Seen</span>`;
  if (s === "skipped") return `<span class="pill pill-compact">Skipped</span>`;
  return `<span class="pill pill-compact">${E(s || "unknown")}</span>`;
}

function renderEntryRow(entry) {
  const title = String(entry?.title || entry?.entry_key || "(untitled)");
  const when = entry?.published_at || entry?.first_seen_at || entry?.last_attempt_at;
  const error = String(entry?.error || "");
  const mediaId = String(entry?.media_id || "");
  return `
    <div class="sub-entry-row" data-entry-key="${E(entry?.entry_key || "")}">
      <div class="sub-entry-main">
        <div class="sub-entry-title">${renderTitleLink(title, entry?.url)}</div>
        <div class="sub-entry-meta">
          ${entryStatusPill(entry?.status)}
          <span>${E(fmtDate(when))}</span>
        </div>
        ${error ? `<div class="sub-entry-error">${E(error)}</div>` : ""}
      </div>
      ${mediaId ? `<div class="sub-entry-actions"><button type="button" class="secondary btn-xs" data-sub-media="${E(mediaId)}">View</button></div>` : ""}
    </div>
  `;
}

function renderDrawer(id, drawer) {
  const d = drawer || {};
  const status = String(d.status || "all");
  const entries = Array.isArray(d.entries) ? d.entries : [];
  const filters = ENTRY_FILTERS.map(
    (f) =>
      `<button type="button" class="sub-filter-btn${status === f.id ? " is-active" : ""}" data-sub-entry-status="${E(f.id)}">${E(f.label)}</button>`
  ).join("");

  const body = d.error
    ? `<div class="sub-entry-error">${E(d.error)}</div>`
    : entries.length
      ? `<div class="sub-entry-list">${entries.map(renderEntryRow).join("")}</div>`
      : d.loading
        ? `<div class="sys-loading">Loading entries…</div>`
        : `<div class="sub-empty small-muted">No entries match this filter.</div>`;

  return `
    <div class="sub-drawer">
      <div class="sub-drawer-head">
        <strong>Entry history</strong>
        <span class="small-muted">${E(n(d.total))} recorded</span>
      </div>
      <div class="sub-entry-filters">${filters}</div>
      ${body}
      ${d.hasMore ? `<div class="sub-load-more">${subButton(id, "load-more", d.loadingMore ? "Loading…" : "Load More", { disabled: Boolean(d.loadingMore) })}</div>` : ""}
    </div>
  `;
}

function renderSubscriptionsView(view, data) {
  if (!view) return;
  const scroller = view.closest(".system-sheet-body");
  const scrollTop = scroller?.scrollTop || 0;
  const subs = Array.isArray(data?.subscriptions) ? data.subscriptions : [];

  const list = subs.length
    ? `<div class="sub-list">${subs.map(renderCard).join("")}</div>`
    : `
      <div class="sys-card sub-empty">
        <strong>No subscriptions yet</strong>
        <p class="small-muted sys-card-copy">
          Add a YouTube channel or playlist, a podcast/video RSS or Atom feed, or a yt-dlp
          listing URL. Media Vault will check it on your interval and archive new items.
        </p>
      </div>
    `;

  view.innerHTML = `
    <div class="sub-stack">
      ${renderSummary(data?.summary)}
      ${formMode ? renderForm() : ""}
      ${list}
    </div>
  `;

  if (scroller) scroller.scrollTop = scrollTop;
}

/** Read directly from the DOM so Enter-to-submit and click-to-submit behave identically. */
function readFormValues(form) {
  const val = (id) => form.querySelector(`#${id}`)?.value ?? "";
  const checked = (id) => Boolean(form.querySelector(`#${id}`)?.checked);
  return {
    source_url: String(val("subUrl") || "").trim(),
    name: String(val("subName") || "").trim(),
    source_kind: String(val("subKind") || "auto"),
    folder: String(val("subFolder") || "").trim(),
    tags: String(val("subTags") || "").trim(),
    format_spec: String(val("subFormat") || ""),
    interval_minutes: val("subInterval"),
    max_items_per_check: val("subMaxItems"),
    seed_only: checked("subSeedOnly"),
    initial_backfill_limit: val("subBackfillLimit"),
    title_include_regex: String(val("subIncludeRegex") || "").trim(),
    title_exclude_regex: String(val("subExcludeRegex") || "").trim(),
    forward_to_telegram: checked("subForward"),
    enabled: checked("subEnabled"),
  };
}

function buildPayload(values) {
  const body = {
    name: values.name,
    source_url: values.source_url,
    source_kind: ["auto", "rss", "ytdlp"].includes(values.source_kind)
      ? values.source_kind
      : "auto",
    folder: values.folder,
    tags: values.tags,
    format_spec: values.format_spec,
    forward_to_telegram: Boolean(values.forward_to_telegram),
    interval_minutes: clampInt(values.interval_minutes, 5, 10080, 60),
    max_items_per_check: clampInt(values.max_items_per_check, 1, 100, 10),
    enabled: Boolean(values.enabled),
    title_include_regex: values.title_include_regex,
    title_exclude_regex: values.title_exclude_regex,
  };
  if (!formMode || formMode.kind === "create" || !isSeeded(findSub(formMode.id))) {
    body.seed_only = Boolean(values.seed_only);
    body.initial_backfill_limit = clampInt(values.initial_backfill_limit, 0, 50, 0);
  }
  return body;
}

async function saveSubForm(view) {
  if (subSaving) return;
  const form = findForm(view);
  if (!form) return;
  const values = readFormValues(form);
  if (!values.source_url) {
    toast("A source URL is required.", { type: "error" });
    form.querySelector("#subUrl")?.focus();
    return;
  }

  const editing = formMode?.kind === "edit" && formMode.id ? String(formMode.id) : null;
  const saveBtn = form.querySelector('[data-sub-action="save"]');
  const prevLabel = saveBtn?.textContent || "Save";
  subSaving = true;
  if (saveBtn) {
    saveBtn.disabled = true;
    saveBtn.textContent = "Saving…";
  }

  try {
    const body = buildPayload(values);
    const url = editing
      ? `/api/subscriptions/${encodeURIComponent(editing)}`
      : "/api/subscriptions";
    const result = await apiFetch(url, {
      method: editing ? "PATCH" : "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    formMode = null;
    subDraft = defaultDraft();
    toast(
      editing
        ? `Subscription updated: ${result?.name || values.name || "saved"}.`
        : `Subscription added: ${result?.name || values.name || "saved"}.`,
      { type: "success" }
    );
    await loadSubscriptionsTab(view, { silent: true });
  } catch (err) {
    toast(`Failed saving subscription: ${err.message}`, { type: "error" });
  } finally {
    subSaving = false;
    if (saveBtn && saveBtn.isConnected) {
      saveBtn.disabled = false;
      saveBtn.textContent = prevLabel;
    }
  }
}

async function loadEntries(id, view, { append = false } = {}) {
  const drawer = drawers.get(String(id));
  if (!drawer) return;
  const generation = ++drawer.generation;
  if (append) drawer.loadingMore = true;
  else drawer.loading = true;
  if (view && subsCache) renderSubscriptionsView(view, subsCache);

  let data = null;
  let error = null;
  try {
    const qs = new URLSearchParams({
      limit: String(ENTRY_LIMIT),
      offset: String(append ? drawer.offset : 0),
    });
    if (drawer.status && drawer.status !== "all") qs.set("status", drawer.status);
    data = await apiFetch(
      `/api/subscriptions/${encodeURIComponent(id)}/entries?${qs.toString()}`
    );
  } catch (err) {
    error = err;
  }

  // A newer filter change or load superseded this request.
  if (drawer.generation !== generation) return;

  if (error) {
    drawer.error = error.message || "Failed loading entries.";
  } else {
    const incoming = Array.isArray(data?.entries) ? data.entries : [];
    drawer.entries = append ? drawer.entries.concat(incoming) : incoming;
    drawer.total = n(data?.total ?? drawer.entries.length);
    drawer.hasMore = Boolean(data?.has_more);
    drawer.offset = drawer.entries.length;
    drawer.error = "";
  }
  drawer.loading = false;
  drawer.loadingMore = false;
  if (view && subsCache) renderSubscriptionsView(view, subsCache);
}

async function runCheck(id, view, dryRun) {
  const key = subKey(id, dryRun ? "dry-run" : "check");
  if (busyActions.has(key)) return;
  busyActions.add(key);
  if (view && subsCache) renderSubscriptionsView(view, subsCache);
  try {
    const result = await apiFetch(
      `/api/subscriptions/${encodeURIComponent(id)}/check?dry_run=${dryRun ? "true" : "false"}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      }
    );
    if (dryRun) {
      previews.set(id, result || {});
      toast(`Preview ready: ${n(result?.discovered_count)} discovered.`, {
        type: "info",
      });
    } else {
      toast(
        `Check complete: ${n(result?.queued_count)} queued, ${n(result?.seeded_count)} seeded.`,
        { type: "success" }
      );
      await loadSubscriptionsTab(view, { silent: true });
    }
  } catch (err) {
    if (dryRun) {
      previews.set(id, { error: err.message || "Preview failed." });
    } else {
      loadSubscriptionsTab(view, { silent: true }).catch(() => {});
    }
    toast(`Check failed: ${err.message}`, { type: "error" });
  } finally {
    busyActions.delete(key);
    if (view && subsCache) renderSubscriptionsView(view, subsCache);
  }
}

async function toggleSub(id, view) {
  const sub = findSub(id);
  if (!sub || isBusy(id, "toggle")) return;
  const next = isPaused(sub);
  busyActions.add(subKey(id, "toggle"));
  if (view && subsCache) renderSubscriptionsView(view, subsCache);
  try {
    await apiFetch(`/api/subscriptions/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: next }),
    });
    toast(next ? "Subscription enabled." : "Subscription paused.", {
      type: "success",
    });
    await loadSubscriptionsTab(view, { silent: true });
  } catch (err) {
    toast(`Failed updating subscription: ${err.message}`, { type: "error" });
  } finally {
    busyActions.delete(subKey(id, "toggle"));
    if (view && subsCache) renderSubscriptionsView(view, subsCache);
  }
}

async function deleteSub(id, view) {
  const sub = findSub(id);
  if (!sub) return;
  const name = String(sub.name || sub.source_url || "this subscription");
  const ok = await confirmDialog({
    title: "Delete Subscription",
    message: `Delete "${name}"? Archived media stays in the vault, but the subscription and its entry history will be removed.`,
    confirmLabel: "Delete",
    danger: true,
  });
  if (!ok) return;

  busyActions.add(subKey(id, "delete"));
  if (view && subsCache) renderSubscriptionsView(view, subsCache);
  try {
    await apiFetch(`/api/subscriptions/${encodeURIComponent(id)}`, {
      method: "DELETE",
    });
    previews.delete(id);
    drawers.delete(id);
    if (formMode?.kind === "edit" && String(formMode.id) === String(id)) {
      formMode = null;
      subDraft = defaultDraft();
    }
    toast("Subscription deleted.", { type: "info" });
    await loadSubscriptionsTab(view, { silent: true });
  } catch (err) {
    toast(`Failed deleting subscription: ${err.message}`, { type: "error" });
  } finally {
    busyActions.delete(subKey(id, "delete"));
    if (view && subsCache) renderSubscriptionsView(view, subsCache);
  }
}

export async function loadSubscriptionsTab(view, { silent = false } = {}) {
  const target = view || $("systemMaintenanceView");
  if (!target) return;
  currentView = target;

  if (isLocked()) {
    renderLocked(target);
    return;
  }
  if (!silent && !subsCache) {
    renderLoading(target, "Loading subscriptions...");
  }
  if (subsLoading) return;

  subsLoading = true;
  try {
    const data = await apiFetch("/api/subscriptions");
    subsCache = data;
    renderSubscriptionsView(target, data);
  } catch (err) {
    renderError(target, err.message || "Failed loading subscriptions.");
  } finally {
    subsLoading = false;
  }
}

function syncFormDraft(e) {
  const t = e?.target;
  if (!t?.id) return;
  const fields = {
    subUrl: "source_url",
    subName: "name",
    subKind: "source_kind",
    subFolder: "folder",
    subTags: "tags",
    subFormat: "format_spec",
    subInterval: "interval_minutes",
    subMaxItems: "max_items_per_check",
    subSeedOnly: "seed_only",
    subBackfillLimit: "initial_backfill_limit",
    subIncludeRegex: "title_include_regex",
    subExcludeRegex: "title_exclude_regex",
    subForward: "forward_to_telegram",
    subEnabled: "enabled",
  };
  const key = fields[t.id];
  if (!key) return;
  subDraft[key] = t.type === "checkbox" ? Boolean(t.checked) : t.value;
}

export function handleSubscriptionsInput(e) {
  syncFormDraft(e);
}

export function handleSubscriptionsChange(e) {
  syncFormDraft(e);
}

export function handleSubscriptionsClick(e) {
  const target = e?.target && e.target.closest ? e.target : null;
  if (!target) return;
  const view = resolveView(e);

  const retryBtn = target.closest("[data-sub-retry]");
  if (retryBtn) {
    e.preventDefault();
    loadSubscriptionsTab(view);
    return;
  }

  const intervalChip = target.closest("[data-sub-interval]");
  if (intervalChip) {
    e.preventDefault();
    const value = String(intervalChip.dataset.subInterval || "");
    const input = findForm(view)?.querySelector("#subInterval");
    if (input) {
      input.value = value;
      subDraft.interval_minutes = value;
      view?.querySelectorAll("[data-sub-interval]").forEach((chip) => {
        chip.classList.toggle("is-active", chip.dataset.subInterval === value);
      });
    }
    return;
  }

  const filterBtn = target.closest("[data-sub-entry-status]");
  if (filterBtn) {
    e.preventDefault();
    const id = filterBtn.closest("[data-sub-id]")?.dataset.subId;
    const drawer = id ? drawers.get(String(id)) : null;
    if (!drawer) return;
    drawer.status = filterBtn.dataset.subEntryStatus || "all";
    drawer.offset = 0;
    drawer.entries = [];
    drawer.total = 0;
    drawer.hasMore = false;
    drawer.error = "";
    if (view && subsCache) renderSubscriptionsView(view, subsCache);
    loadEntries(id, view, { append: false });
    return;
  }

  const mediaBtn = target.closest("[data-sub-media]");
  if (mediaBtn) {
    e.preventDefault();
    const id = mediaBtn.dataset.subMedia;
    if (!id) return;
    const sheet = $("systemSheet");
    if (sheet?.open) sheet.close();
    emit("media:open", { id });
    return;
  }

  const actionBtn = target.closest("[data-sub-action]");
  if (!actionBtn) return;
  const action = actionBtn.dataset.subAction;
  const id = actionBtn.dataset.subId ? String(actionBtn.dataset.subId) : "";

  if (action === "save") {
    e.preventDefault();
    saveSubForm(view);
    return;
  }

  if (action === "add") {
    e.preventDefault();
    if (formMode?.kind === "create") {
      formMode = null;
    } else {
      formMode = { kind: "create" };
      subDraft = defaultDraft();
    }
    if (subsCache && view) renderSubscriptionsView(view, subsCache);
    if (formMode && view) view.querySelector("#subUrl")?.focus();
    return;
  }

  if (action === "cancel-form") {
    e.preventDefault();
    formMode = null;
    subDraft = defaultDraft();
    if (subsCache && view) renderSubscriptionsView(view, subsCache);
    return;
  }

  if (action === "refresh") {
    e.preventDefault();
    loadSubscriptionsTab(view, { silent: true });
    return;
  }

  if (!id) return;

  if (action === "toggle") {
    e.preventDefault();
    toggleSub(id, view);
  } else if (action === "check") {
    e.preventDefault();
    runCheck(id, view, false);
  } else if (action === "dry-run") {
    e.preventDefault();
    runCheck(id, view, true);
  } else if (action === "dismiss-preview") {
    e.preventDefault();
    previews.delete(id);
    if (subsCache && view) renderSubscriptionsView(view, subsCache);
  } else if (action === "entries") {
    e.preventDefault();
    if (drawers.has(id)) {
      drawers.delete(id);
      if (subsCache && view) renderSubscriptionsView(view, subsCache);
      return;
    }
    drawers.set(id, {
      status: "all",
      offset: 0,
      entries: [],
      total: 0,
      hasMore: false,
      loading: true,
      loadingMore: false,
      generation: 0,
      error: "",
    });
    if (subsCache && view) renderSubscriptionsView(view, subsCache);
    loadEntries(id, view, { append: false });
  } else if (action === "load-more") {
    e.preventDefault();
    loadEntries(id, view, { append: true });
  } else if (action === "edit") {
    e.preventDefault();
    const sub = findSub(id);
    if (!sub) return;
    formMode = { kind: "edit", id };
    subDraft = draftForSub(sub);
    if (subsCache && view) {
      renderSubscriptionsView(view, subsCache);
      view.querySelector("#subUrl")?.focus();
    }
  } else if (action === "delete") {
    e.preventDefault();
    deleteSub(id, view);
  }
}

export function handleSubscriptionsSubmit(e) {
  if (!e) return;
  e.preventDefault();
  saveSubForm(resolveView(e));
}

export function resetSubscriptionsCache() {
  subsCache = null;
  subsLoading = false;
  currentView = null;
  formMode = null;
  subDraft = defaultDraft();
  subSaving = false;
  previews.clear();
  drawers.clear();
  busyActions.clear();
}
