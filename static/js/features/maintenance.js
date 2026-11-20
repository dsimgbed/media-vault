import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch, withToken } from "/static/js/api.js";
import { $, escapeHtml, fmtSize } from "/static/js/utils.js";
import { confirmDialog, toast } from "/static/js/ui/dialogs.js";

const TABS = [
  { id: "settings", label: "Settings" },
  { id: "health", label: "Health" },
  { id: "backups", label: "Backups" },
  { id: "integrity", label: "Integrity" },
];

let activeTab = "settings";
let initialized = false;
let repairPollTimer = null;
let repairPollStartedAt = 0;

let statusCache = null;
let backupsCache = null;
let integrityCache = null;

let auditBusy = false;
let repairStarting = false;
let backupCreating = false;
let purgingCaches = false;
let authRejected = false;

const loadLocks = new Map();
const loadQueued = new Set();

const backupDraft = {
  uploadToTelegram: true,
  note: "",
};

const integrityDraft = {
  verifyTelegram: false,
  backfillThumbs: true,
  backfillPreviews: true,
  backfillMetadata: true,
  purgeOrphanThumbs: false,
  purgeOrphanStaging: false,
  maxTelegramDownloads: 25,
  maxTelegramItemMb: 250,
};

function isLocked() {
  return Boolean(state.authRequired && (!state.token || authRejected));
}

function isSheetOpen() {
  return Boolean($("systemSheet")?.open);
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

function fmtPct(val) {
  const num = Number(val);
  return Number.isFinite(num) ? `${num.toFixed(1)}%` : "0.0%";
}

function stopRepairPoll() {
  if (repairPollTimer) {
    clearTimeout(repairPollTimer);
    repairPollTimer = null;
  }
  repairPollStartedAt = 0;
}

function scheduleRepairPoll() {
  if (repairPollTimer) {
    clearTimeout(repairPollTimer);
    repairPollTimer = null;
  }
  const running = Boolean(
    integrityCache?.busy && integrityCache?.active_run?.status === "running"
  );
  if (!isSheetOpen() || activeTab !== "integrity" || !running || isLocked()) {
    repairPollStartedAt = 0;
    return;
  }
  if (!repairPollStartedAt) {
    repairPollStartedAt = Date.now();
  }
  if (Date.now() - repairPollStartedAt > 10 * 60 * 1000) {
    stopRepairPoll();
    return;
  }
  repairPollTimer = setTimeout(async () => {
    repairPollTimer = null;
    if (!isSheetOpen() || activeTab !== "integrity" || isLocked()) {
      stopRepairPoll();
      return;
    }
    await loadIntegrityTab({ silent: true });
  }, 1500);
}

function renderTabs() {
  const container = $("systemTabs");
  if (!container) return;
  container.innerHTML = TABS.map((t) => {
    const selected = t.id === activeTab;
    return `<button type="button" class="system-tab${selected ? " is-active" : ""}" role="tab" aria-selected="${selected ? "true" : "false"}" data-sys-tab="${escapeHtml(t.id)}">${escapeHtml(t.label)}</button>`;
  }).join("");
}

function renderLockedState(view) {
  view.innerHTML = `
    <div class="sys-card sys-locked-card">
      <div class="sys-card-head">
        <strong>Vault Locked</strong>
        <span class="pill warn">Auth required</span>
      </div>
      <p class="small-muted sys-card-copy">
        Unlock vault in Settings tab to inspect health, backups, and integrity.
      </p>
      <div class="sys-actions-row">
        <button type="button" class="secondary btn-sm" data-sys-goto="settings">Open Settings</button>
      </div>
    </div>
  `;
}

function renderLoading(view, label) {
  view.innerHTML = `<div class="sys-card sys-loading">${escapeHtml(label)}</div>`;
}

function renderError(view, message, retryTab) {
  view.innerHTML = `
    <div class="sys-card sys-error-card">
      <div class="sys-card-head">
        <strong>Failed to Load</strong>
        <button type="button" class="secondary btn-xs" data-sys-retry="${escapeHtml(retryTab)}">Retry</button>
      </div>
      <div class="sys-error-msg">${escapeHtml(message)}</div>
    </div>
  `;
}

function renderHealthView(view, data) {
  const tg = data?.telegram || {};
  const workers = Array.isArray(tg.workers) ? tg.workers : [];
  const bc = tg.block_cache || {};
  const l2 = bc.l2 || {};
  const pf = bc.prefetch || {};
  const zc = data?.zip_cache || {};
  const db = data?.database || {};
  const st = data?.storage || {};
  const ingest = data?.ingest || {};
  const backups = data?.backups || {};

  const bcPct = Math.max(0, Math.min(100, Number(bc.utilization_pct || 0)));
  const onlineCount = Number(tg.online_count ?? 0);
  const workerCount = Number(tg.worker_count ?? 0);
  const cooldownCount = Number(tg.cooldown_count ?? 0);

  const workersHtml = workers.length === 0
    ? `<div class="small-muted">No Telegram workers configured.</div>`
    : `<div class="sys-worker-grid">${workers.map((w) => {
        let statusPill = `<span class="pill ok">Online</span>`;
        if (!w.online) {
          statusPill = `<span class="pill err">Offline</span>`;
        } else if (w.in_cooldown) {
          statusPill = `<span class="pill warn">Cooldown (${escapeHtml(w.cooldown_remaining_sec)}s)</span>`;
        }
        const handle = w.username ? `<span class="small-muted">@${escapeHtml(w.username)}</span>` : "";
        const errHtml = w.last_error
          ? `<div class="sys-worker-err">${escapeHtml(w.last_error)}${w.last_error_at ? ` · ${escapeHtml(fmtDate(w.last_error_at))}` : ""}</div>`
          : "";
        return `
          <div class="sys-worker-card">
            <div class="sys-worker-head">
              <div class="sys-worker-name">
                <strong>${escapeHtml(w.name || "worker")}</strong>
                ${handle}
              </div>
              ${statusPill}
            </div>
            <div class="sys-worker-stats">
              <span>Uploads: <strong>${Number(w.active_uploads || 0)}</strong></span>
              <span>Streams: <strong>${Number(w.active_streams || 0)}</strong></span>
              <span>FloodWait: <strong>${Number(w.flood_wait_count || 0)}</strong></span>
            </div>
            ${errHtml}
          </div>
        `;
      }).join("")}</div>`;

  view.innerHTML = `
    <div class="sys-section-bar">
      <div class="sys-pills-row">
        <span class="pill">Mode: ${escapeHtml(data?.storage_mode || state.storageMode || "telegram")}</span>
        <span class="pill ${workerCount > 0 ? "ok" : ""}">Workers: ${onlineCount}/${workerCount}</span>
        ${cooldownCount > 0 ? `<span class="pill warn">${cooldownCount} in cooldown</span>` : ""}
        <span class="pill">Ingest: ${Number(ingest.concurrency || 0)} slots (${ingest.active_jobs ? "active" : "idle"})</span>
        <span class="pill">Last backup: ${backups.latest ? escapeHtml(fmtDate(backups.latest.created_at)) : "None"}</span>
      </div>
      <button type="button" class="secondary btn-xs" data-sys-action="refresh-health">Refresh</button>
    </div>

    <div class="sys-card">
      <div class="sys-card-head">
        <strong>Telegram Workers</strong>
        <span class="small-muted">Archive: ${tg.archive_enabled ? "enabled" : "disabled"} · Stream cache: ${Number(tg.stream_media_cache_entries || 0)}</span>
      </div>
      ${workersHtml}
    </div>

    <div class="sys-card">
      <div class="sys-card-head">
        <strong>Block Cache</strong>
        <div class="sys-actions-row">
          <span class="pill ${bc.enabled ? "ok" : ""}">${bc.enabled ? "Enabled" : "Disabled"}</span>
          <button type="button" class="secondary btn-xs" data-sys-action="purge-caches" ${purgingCaches ? "disabled" : ""}>${purgingCaches ? "Purging…" : "Purge Caches"}</button>
        </div>
      </div>
      <div class="sys-progress-row">
        <div class="bar"><div style="width:${bcPct.toFixed(1)}%"></div></div>
        <span class="small-muted">${fmtPct(bc.utilization_pct)}</span>
      </div>
      <div class="sys-kv-row">
        <span>Used: <strong>${escapeHtml(fmtSize(bc.used_bytes || 0))}</strong> / ${escapeHtml(fmtSize(bc.max_bytes || 0))}</span>
        <span>Entries: <strong>${Number(bc.entries || 0)}</strong></span>
        <span>Hit rate: <strong>${fmtPct(bc.hit_rate_pct)}</strong> (${Number(bc.hits || 0)} hits / ${Number(bc.misses || 0)} misses / ${Number(bc.evictions || 0)} evictions)</span>
      </div>
      <div class="sys-kv-row">
        <span>L2 Disk Cache: <strong>${l2.enabled ? "Enabled" : "Disabled"}</strong></span>
        <span>Used: <strong>${escapeHtml(fmtSize(l2.used_bytes || 0))}</strong> / ${escapeHtml(fmtSize(l2.max_bytes || 0))}</span>
        <span>Entries: <strong>${Number(l2.entries || 0)}</strong></span>
        <span>Hit rate: <strong>${fmtPct(l2.hit_rate_pct)}</strong> (${Number(l2.evictions || 0)} evictions)</span>
      </div>
      <div class="sys-kv-row">
        <span>Coalescing &amp; Prefetch: coalesced waits <strong>${Number(bc.coalesced_waits || 0)}</strong></span>
        <span>Prefetch hits / scheduled / drops: <strong>${Number(pf.hits || 0)}</strong> / <strong>${Number(pf.scheduled || 0)}</strong> / <strong>${Number(pf.drops || 0)}</strong></span>
      </div>
      <div class="sys-kv-row">
        <span>ZIP Directory Cache: L1 entries <strong>${Number(zc.l1_entries || 0)}</strong></span>
        <span>L2 persisted: <strong>${Number(zc.l2_entries || 0)} · ${escapeHtml(fmtSize(zc.l2_bytes || 0))}</strong></span>
        <span>Hit rate: <strong>${fmtPct(zc.hit_rate_pct)}</strong></span>
      </div>
    </div>

    <div class="sys-grid-2">
      <div class="sys-card">
        <div class="sys-card-head">
          <strong>Database</strong>
          <span class="pill">${escapeHtml(String(db.journal_mode || "unknown").toUpperCase())}</span>
        </div>
        <div class="sys-stat-list">
          <div><span>DB Size</span><strong>${escapeHtml(fmtSize(db.size_bytes || 0))}</strong></div>
          <div><span>WAL Size</span><strong>${escapeHtml(fmtSize(db.wal_size_bytes || 0))}</strong></div>
          <div><span>Media / Parts</span><strong>${Number(db.media_count || 0)} / ${Number(db.parts_count || 0)}</strong></div>
          <div><span>Jobs / Folders / Tags</span><strong>${Number(db.jobs_count || 0)} / ${Number(db.folders_count || 0)} / ${Number(db.tags_count || 0)}</strong></div>
        </div>
      </div>

      <div class="sys-card">
        <div class="sys-card-head">
          <strong>Storage Footprint</strong>
          <span class="small-muted">${Number(ingest.active_staging_dirs || 0)} active staging</span>
        </div>
        <div class="sys-stat-list">
          <div><span>Thumbnails (${Number(st.thumbs_count || 0)})</span><strong>${escapeHtml(fmtSize(st.thumbs_bytes || 0))}</strong></div>
          <div><span>Staging (${Number(st.staging_entries || 0)})</span><strong>${escapeHtml(fmtSize(st.staging_bytes || 0))}</strong></div>
          <div><span>Backups (${Number(st.backups_count ?? backups.count ?? 0)})</span><strong>${escapeHtml(fmtSize(st.backups_bytes || 0))}</strong></div>
          <div><span>Stream Cache (${Number(st.stream_cache_entries || 0)})</span><strong>${escapeHtml(fmtSize(st.stream_cache_bytes || 0))}</strong></div>
        </div>
      </div>
    </div>
  `;
}

function renderBackupsView(view, data) {
  const backups = Array.isArray(data?.backups) ? data.backups : [];
  const cfg = data?.config || {};
  const tgEnabled = Boolean(
    cfg.telegram_enabled || state.telegramArchiveEnabled || statusCache?.telegram?.archive_enabled
  );

  const schedText = Number(cfg.interval_hours) > 0
    ? `Every ${cfg.interval_hours}h`
    : "Manual only";

  const rowsHtml = backups.length === 0
    ? `<div class="small-muted sys-empty">No backups created yet.</div>`
    : `<div class="sys-backup-list">${backups.map((b) => {
        const fname = b.filename || b.id || "backup.db.gz";
        const downloadUrl = withToken(`/api/system/backups/${encodeURIComponent(b.id)}/download`);
        const downloadBtn = b.exists_locally !== false
          ? `<a class="sys-btn-link" href="${escapeHtml(downloadUrl)}" download="${escapeHtml(fname)}">Download</a>`
          : `<span class="pill warn">Remote only</span>`;
        const tgBadge = b.telegram_msg_id
          ? `<span class="pill ok">Telegram #${escapeHtml(b.telegram_msg_id)}</span>`
          : "";
        const noteHtml = b.note
          ? `<div class="sys-backup-note">${escapeHtml(b.note)}</div>`
          : "";
        const shaShort = b.sha256 ? ` · sha256:${escapeHtml(String(b.sha256).slice(0, 12))}` : "";
        const rawSize = b.uncompressed_bytes ? ` (${escapeHtml(fmtSize(b.uncompressed_bytes))} raw)` : "";
        return `
          <div class="sys-backup-row">
            <div class="sys-backup-info">
              <div class="sys-backup-title">
                <strong>${escapeHtml(fname)}</strong>
                <span class="pill pill-compact">${escapeHtml(b.trigger || "manual")}</span>
                ${tgBadge}
              </div>
              <div class="sys-backup-meta">
                ${escapeHtml(fmtDate(b.created_at))} · ${escapeHtml(fmtSize(b.size_bytes || 0))}${rawSize}${shaShort}
              </div>
              ${noteHtml}
            </div>
            <div class="sys-backup-actions">
              ${downloadBtn}
              <button type="button" class="danger btn-xs" data-sys-delete-backup="${escapeHtml(b.id)}" data-sys-backup-name="${escapeHtml(fname)}">Delete</button>
            </div>
          </div>
        `;
      }).join("")}</div>`;

  view.innerHTML = `
    <div class="sys-section-bar">
      <div class="sys-pills-row">
        <span class="pill">Retention: ${escapeHtml(cfg.retention_count ?? "—")}</span>
        <span class="pill">Schedule: ${escapeHtml(schedText)}</span>
        <span class="pill ${cfg.scheduler_running ? "ok" : ""}">Scheduler: ${cfg.scheduler_running ? "Running" : "Off"}</span>
        <span class="pill ${cfg.telegram_enabled ? "ok" : ""}">Telegram Mirror: ${cfg.telegram_enabled ? "Enabled" : "Off"}</span>
      </div>
      <button type="button" class="secondary btn-xs" data-sys-action="refresh-backups">Refresh</button>
    </div>

    <div class="sys-card">
      <div class="sys-card-head">
        <strong>Create Backup</strong>
        <span class="small-muted">${escapeHtml(cfg.backups_dir || "")}</span>
      </div>
      <div class="sys-backup-form">
        <input id="sysBackupNote" type="text" placeholder="Optional backup note..." value="${escapeHtml(backupDraft.note)}" />
        ${tgEnabled ? `
          <label class="checkbox-label">
            <input id="sysBackupTgCheck" type="checkbox" ${backupDraft.uploadToTelegram ? "checked" : ""} />
            Upload to Telegram
          </label>
        ` : ""}
        <button id="sysCreateBackupBtn" type="button" class="btn-sm" ${backupCreating ? "disabled" : ""}>
          ${backupCreating ? "Creating..." : "Create Backup"}
        </button>
      </div>
    </div>

    <div class="sys-card">
      <div class="sys-card-head">
        <strong>Available Backups (${backups.length})</strong>
      </div>
      ${rowsHtml}
    </div>

    <div class="sys-hint-box">
      <strong>Restore / Verify CLI:</strong>
      <code>python scripts/restore_vault.py --backup &lt;file&gt; --verify-only</code>
    </div>
  `;
}

function renderStatusPill(status) {
  if (status === "done") return `<span class="pill ok">Done</span>`;
  if (status === "running") return `<span class="pill warn">Running</span>`;
  if (status === "failed") return `<span class="pill err">Failed</span>`;
  return `<span class="pill">${escapeHtml(status || "Unknown")}</span>`;
}

function renderAuditSummary(audit) {
  if (!audit) {
    return `<div class="small-muted sys-empty">No integrity audit has been run yet.</div>`;
  }
  const s = audit.summary || {};
  const healthyCls = s.healthy ? "ok" : "warn";
  const missingLocalExtra = Number(s.missing_local_unrecoverable || 0) > 0
    ? ` (${Number(s.missing_local_unrecoverable)} unrecoverable)`
    : "";
  const metrics = [
    { label: "Healthy", value: s.healthy ? "Yes" : "Issues", cls: healthyCls },
    { label: "Total Media", value: Number(s.total_media || 0) },
    { label: "Missing Thumbs", value: Number(s.missing_thumbs || 0), cls: s.missing_thumbs > 0 ? "warn" : "" },
    { label: "Missing Previews", value: Number(s.missing_previews || 0), cls: s.missing_previews > 0 ? "warn" : "" },
    { label: "Missing Metadata", value: Number(s.missing_metadata || 0), cls: s.missing_metadata > 0 ? "warn" : "" },
    { label: "Missing Local", value: `${Number(s.missing_local_files || 0)}${missingLocalExtra}`, cls: s.missing_local_files > 0 ? "err" : "" },
    { label: "Orphan Thumbs", value: `${Number(s.orphaned_thumbs || 0)}${s.orphaned_thumbs_bytes ? ` (${fmtSize(s.orphaned_thumbs_bytes)})` : ""}`, cls: s.orphaned_thumbs > 0 ? "warn" : "" },
    { label: "Orphan Staging", value: `${Number(s.orphaned_staging || 0)}${s.orphaned_staging_bytes ? ` (${fmtSize(s.orphaned_staging_bytes)})` : ""}`, cls: s.orphaned_staging > 0 ? "warn" : "" },
    { label: "Broken TG Refs", value: s.telegram_verified ? Number(s.broken_telegram_refs || 0) : "Skipped", cls: s.broken_telegram_refs > 0 ? "err" : "" },
  ];

  const rateLimitedNote = Number(s.telegram_unchecked_rate_limited || 0) > 0
    ? `<div class="small-muted">${Number(s.telegram_unchecked_rate_limited)} Telegram message(s) deferred due to FloodWait rate limit.</div>`
    : "";

  return `
    <div class="sys-run-meta">
      ${renderStatusPill(audit.status)}
      <span class="small-muted">${escapeHtml(fmtDate(audit.updated_at || audit.created_at))}</span>
      <span class="small-muted">${escapeHtml(audit.message || "")}</span>
    </div>
    <div class="sys-metric-grid">
      ${metrics.map((m) => `
        <div class="sys-metric-chip${m.cls ? ` is-${m.cls}` : ""}">
          <span>${escapeHtml(m.label)}</span>
          <strong>${escapeHtml(String(m.value))}</strong>
        </div>
      `).join("")}
    </div>
    ${rateLimitedNote}
  `;
}

function renderRepairSummary(repair) {
  if (!repair || repair.status === "running") return "";
  const s = repair.summary || {};
  const metrics = [
    { label: "Thumbs Repaired", value: Number(s.thumbs_repaired || 0) },
    { label: "Previews Repaired", value: Number(s.previews_repaired || 0) },
    { label: "Metadata Repaired", value: Number(s.metadata_repaired || 0) },
    { label: "Orphan Thumbs Purged", value: Number(s.orphan_thumbs_purged || 0) },
    { label: "Orphan Staging Purged", value: Number(s.orphan_staging_purged || 0) },
    { label: "Freed Space", value: fmtSize(s.freed_bytes || 0) },
    { label: "TG Downloads", value: Number(s.telegram_downloads || 0) },
    { label: "Skipped", value: Number(s.skipped || 0) },
    { label: "Failed", value: Number(s.failed || 0), cls: s.failed > 0 ? "err" : "" },
  ];

  return `
    <div class="sys-sub-block">
      <div class="sys-run-meta">
        <strong>Last Repair</strong>
        ${renderStatusPill(repair.status)}
        <span class="small-muted">${escapeHtml(fmtDate(repair.updated_at || repair.created_at))}</span>
        <span class="small-muted">${escapeHtml(repair.message || "")}</span>
      </div>
      ${repair.summary ? `
        <div class="sys-metric-grid">
          ${metrics.map((m) => `
            <div class="sys-metric-chip${m.cls ? ` is-${m.cls}` : ""}">
              <span>${escapeHtml(m.label)}</span>
              <strong>${escapeHtml(String(m.value))}</strong>
            </div>
          `).join("")}
        </div>
      ` : ""}
    </div>
  `;
}

function renderIntegrityView(view, data) {
  const busy = Boolean(data?.busy);
  const activeRun = data?.active_run;
  const latestAudit = data?.latest_audit;
  const latestRepair = data?.latest_repair;
  const disabledAll = busy || auditBusy || repairStarting;

  const activeHtml = activeRun && activeRun.status === "running"
    ? (() => {
        const pct = Math.max(0, Math.min(100, Math.round(Number(activeRun.progress || 0))));
        return `
          <div class="sys-active-run">
            <div class="sys-run-meta">
              <span class="pill warn">Running ${escapeHtml(activeRun.kind || "task")}</span>
              <strong>${pct}%</strong>
              <span class="small-muted">${escapeHtml(activeRun.message || "Working...")}</span>
            </div>
            <div class="bar"><div style="width:${pct}%"></div></div>
          </div>
        `;
      })()
    : "";

  view.innerHTML = `
    <div class="sys-section-bar">
      <div class="sys-pills-row">
        <span class="pill ${busy ? "warn" : "ok"}">${busy ? "Maintenance Busy" : "Idle"}</span>
      </div>
      <button type="button" class="secondary btn-xs" data-sys-action="refresh-integrity">Refresh</button>
    </div>

    ${activeHtml}

    <div class="sys-card">
      <div class="sys-card-head">
        <strong>Audit</strong>
        <div class="sys-actions-row">
          <label class="checkbox-label">
            <input id="sysVerifyTgCheck" type="checkbox" ${integrityDraft.verifyTelegram ? "checked" : ""} />
            Verify Telegram Archive (rate-limit safe)
          </label>
          <button id="sysRunAuditBtn" type="button" class="btn-sm" ${disabledAll ? "disabled" : ""}>
            ${auditBusy ? "Auditing..." : "Run Audit"}
          </button>
        </div>
      </div>
      ${renderAuditSummary(latestAudit)}
    </div>

    <div class="sys-card">
      <div class="sys-card-head">
        <strong>Repair</strong>
        <button id="sysRunRepairBtn" type="button" class="secondary btn-sm" ${disabledAll ? "disabled" : ""}>
          ${repairStarting ? "Starting..." : "Run Repair"}
        </button>
      </div>
      <div class="sys-checks-grid">
        <label class="checkbox-label">
          <input id="sysBackfillThumbs" type="checkbox" ${integrityDraft.backfillThumbs ? "checked" : ""} ${disabledAll ? "disabled" : ""} />
          Backfill Thumbs
        </label>
        <label class="checkbox-label">
          <input id="sysBackfillPreviews" type="checkbox" ${integrityDraft.backfillPreviews ? "checked" : ""} ${disabledAll ? "disabled" : ""} />
          Backfill Previews
        </label>
        <label class="checkbox-label">
          <input id="sysBackfillMetadata" type="checkbox" ${integrityDraft.backfillMetadata ? "checked" : ""} ${disabledAll ? "disabled" : ""} />
          Backfill Metadata
        </label>
        <label class="checkbox-label sys-check-danger">
          <input id="sysPurgeOrphanThumbs" type="checkbox" ${integrityDraft.purgeOrphanThumbs ? "checked" : ""} ${disabledAll ? "disabled" : ""} />
          Purge Orphan Thumbs
        </label>
        <label class="checkbox-label sys-check-danger">
          <input id="sysPurgeOrphanStaging" type="checkbox" ${integrityDraft.purgeOrphanStaging ? "checked" : ""} ${disabledAll ? "disabled" : ""} />
          Purge Orphan Staging
        </label>
      </div>
      <div class="sys-repair-limits">
        <label class="sys-inline-field">
          <span>Max Telegram downloads</span>
          <input id="sysMaxTgDownloads" type="number" min="0" max="500" value="${Number(integrityDraft.maxTelegramDownloads)}" ${disabledAll ? "disabled" : ""} />
        </label>
        <label class="sys-inline-field">
          <span>Max item size (MB)</span>
          <input id="sysMaxTgMb" type="number" min="1" max="2048" value="${Number(integrityDraft.maxTelegramItemMb)}" ${disabledAll ? "disabled" : ""} />
        </label>
      </div>
      ${renderRepairSummary(latestRepair)}
    </div>
  `;
}

async function runSerializedLoad(key, fn) {
  if (loadLocks.has(key)) {
    loadQueued.add(key);
    return loadLocks.get(key);
  }
  const p = (async () => {
    try {
      await fn();
    } finally {
      loadLocks.delete(key);
      if (loadQueued.delete(key) && isSheetOpen() && activeTab === key && !isLocked()) {
        runSerializedLoad(key, fn);
      }
    }
  })();
  loadLocks.set(key, p);
  return p;
}

async function loadHealthTab({ silent = false } = {}) {
  const view = $("systemMaintenanceView");
  if (!view) return;
  if (isLocked()) {
    renderLockedState(view);
    return;
  }
  if (!silent && !statusCache) {
    renderLoading(view, "Loading system health telemetry...");
  }
  await runSerializedLoad("health", async () => {
    try {
      const data = await apiFetch("/api/system/status");
      statusCache = data;
      if (data?.telegram) {
        state.telegramArchiveEnabled = Boolean(data.telegram.archive_enabled);
      }
      if (activeTab === "health" && isSheetOpen()) {
        renderHealthView(view, data);
      }
    } catch (err) {
      if (activeTab === "health" && isSheetOpen()) {
        if (isLocked()) renderLockedState(view);
        else renderError(view, err.message, "health");
      }
    }
  });
}

async function loadBackupsTab({ silent = false } = {}) {
  const view = $("systemMaintenanceView");
  if (!view) return;
  if (isLocked()) {
    renderLockedState(view);
    return;
  }
  if (!silent && !backupsCache) {
    renderLoading(view, "Loading backups...");
  }
  await runSerializedLoad("backups", async () => {
    try {
      const data = await apiFetch("/api/system/backups");
      backupsCache = data;
      if (activeTab === "backups" && isSheetOpen()) {
        renderBackupsView(view, data);
      }
    } catch (err) {
      if (activeTab === "backups" && isSheetOpen()) {
        if (isLocked()) renderLockedState(view);
        else renderError(view, err.message, "backups");
      }
    }
  });
}

async function loadIntegrityTab({ silent = false } = {}) {
  const view = $("systemMaintenanceView");
  if (!view) return;
  if (isLocked()) {
    stopRepairPoll();
    renderLockedState(view);
    return;
  }
  if (!silent && !integrityCache) {
    renderLoading(view, "Loading integrity status...");
  }
  await runSerializedLoad("integrity", async () => {
    try {
      const data = await apiFetch("/api/system/integrity/status");
      integrityCache = data;
      if (activeTab === "integrity" && isSheetOpen()) {
        renderIntegrityView(view, data);
        if (data?.busy && data?.active_run?.status === "running") {
          scheduleRepairPoll();
        } else {
          stopRepairPoll();
        }
      } else {
        stopRepairPoll();
      }
    } catch (err) {
      stopRepairPoll();
      if (activeTab === "integrity" && isSheetOpen()) {
        if (isLocked()) renderLockedState(view);
        else renderError(view, err.message, "integrity");
      }
    }
  });
}

function loadTab(tab) {
  const view = $("systemMaintenanceView");
  if (!view) return;
  if (isLocked()) {
    stopRepairPoll();
    renderLockedState(view);
    return;
  }
  if (tab === "health") return loadHealthTab();
  if (tab === "backups") return loadBackupsTab();
  if (tab === "integrity") return loadIntegrityTab();
}

function switchTab(tab) {
  const valid = TABS.some((t) => t.id === tab);
  activeTab = valid ? tab : "settings";
  renderTabs();

  const settingsPane = $("systemSettingsPane");
  const maintView = $("systemMaintenanceView");
  if (activeTab === "settings") {
    stopRepairPoll();
    if (settingsPane) settingsPane.hidden = false;
    if (maintView) maintView.hidden = true;
    return;
  }

  if (settingsPane) settingsPane.hidden = true;
  if (maintView) maintView.hidden = false;
  if (activeTab !== "integrity") {
    stopRepairPoll();
  }
  loadTab(activeTab);
}

async function handleCreateBackup() {
  if (backupCreating || isLocked()) return;
  backupCreating = true;
  const view = $("systemMaintenanceView");
  if (backupsCache && view && activeTab === "backups") {
    renderBackupsView(view, backupsCache);
  }
  try {
    const res = await apiFetch("/api/system/backups", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        upload_to_telegram: Boolean(backupDraft.uploadToTelegram),
        note: (backupDraft.note || "").trim() || null,
      }),
    });
    backupDraft.note = "";
    if (res?.telegram_error) {
      toast(`Backup created locally, Telegram upload failed: ${res.telegram_error}`, { type: "error" });
    } else if (res?.telegram_skipped) {
      toast(`Backup created (${res.filename || "local"}) — ${res.telegram_skipped}`, { type: "info" });
    } else {
      toast(`Backup created: ${res?.filename || "success"}`, { type: "success" });
    }
    await loadBackupsTab({ silent: true });
  } catch (err) {
    toast(`Backup failed: ${err.message}`, { type: "error" });
  } finally {
    backupCreating = false;
    if (backupsCache && view && activeTab === "backups") {
      renderBackupsView(view, backupsCache);
    }
  }
}

async function handleDeleteBackup(id, name) {
  const ok = await confirmDialog({
    title: "Delete Backup",
    message: `Delete backup "${name}" from local disk? This cannot be undone.`,
    confirmLabel: "Delete",
    danger: true,
  });
  if (!ok) return;
  try {
    await apiFetch(`/api/system/backups/${encodeURIComponent(id)}`, { method: "DELETE" });
    toast(`Deleted backup ${name}.`, { type: "info" });
    await loadBackupsTab({ silent: true });
  } catch (err) {
    toast(`Failed deleting backup: ${err.message}`, { type: "error" });
  }
}

async function handleRunAudit() {
  if (auditBusy || isLocked()) return;
  auditBusy = true;
  const view = $("systemMaintenanceView");
  if (integrityCache && view && activeTab === "integrity") {
    renderIntegrityView(view, integrityCache);
  }
  try {
    await apiFetch("/api/system/integrity/audit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        verify_telegram: Boolean(integrityDraft.verifyTelegram),
      }),
    });
    toast("Integrity audit complete.", { type: "success" });
    await loadIntegrityTab({ silent: true });
  } catch (err) {
    toast(`Integrity audit failed: ${err.message}`, { type: "error" });
  } finally {
    auditBusy = false;
    if (integrityCache && view && activeTab === "integrity") {
      renderIntegrityView(view, integrityCache);
    }
  }
}

async function handleRunRepair() {
  if (repairStarting || isLocked()) return;
  if (integrityDraft.purgeOrphanThumbs || integrityDraft.purgeOrphanStaging) {
    const ok = await confirmDialog({
      title: "Confirm Orphan Purge",
      message: "Purging orphaned thumbnails or staging entries permanently deletes unreferenced files on disk. Proceed with repair?",
      confirmLabel: "Run Repair",
      danger: true,
    });
    if (!ok) return;
  }

  repairStarting = true;
  const view = $("systemMaintenanceView");
  if (integrityCache && view && activeTab === "integrity") {
    renderIntegrityView(view, integrityCache);
  }
  try {
    const run = await apiFetch("/api/system/integrity/repair", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        backfill_thumbs: Boolean(integrityDraft.backfillThumbs),
        backfill_previews: Boolean(integrityDraft.backfillPreviews),
        backfill_metadata: Boolean(integrityDraft.backfillMetadata),
        purge_orphan_thumbs: Boolean(integrityDraft.purgeOrphanThumbs),
        purge_orphan_staging: Boolean(integrityDraft.purgeOrphanStaging),
        max_telegram_downloads: Math.max(0, Number(integrityDraft.maxTelegramDownloads) || 0),
        max_telegram_item_mb: Math.max(1, Number(integrityDraft.maxTelegramItemMb) || 250),
      }),
    });
    integrityCache = {
      ...(integrityCache || {}),
      busy: true,
      active_run_id: run?.id || null,
      active_run: run || null,
    };
    toast("Integrity repair started.", { type: "info" });
    if (view && activeTab === "integrity") {
      renderIntegrityView(view, integrityCache);
      scheduleRepairPoll();
    }
  } catch (err) {
    toast(`Failed starting repair: ${err.message}`, { type: "error" });
  } finally {
    repairStarting = false;
    if (integrityCache && view && activeTab === "integrity") {
      renderIntegrityView(view, integrityCache);
    }
  }
}

async function handlePurgeCaches() {
  if (isLocked() || purgingCaches) return;
  purgingCaches = true;
  const view = $("sysHealthView");
  const btn = view?.querySelector('[data-sys-action="purge-caches"]');
  if (btn) btn.disabled = true;
  try {
    const ok = await confirmDialog({
      title: "Purge Caches",
      message: "Purge RAM/disk stream caches and persisted ZIP directory cache? Active streams may re-fetch blocks from Telegram.",
      confirmLabel: "Purge Caches",
      danger: true,
    });
    if (!ok) return;
    if (btn) btn.textContent = "Purging…";
    await apiFetch("/api/system/cache/purge", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scope: "all" }),
    });
    toast("Caches purged.", { type: "success" });
    await loadHealthTab({ silent: false });
  } catch (err) {
    toast(err.message, { type: "error" });
  } finally {
    purgingCaches = false;
    if (statusCache && view && activeTab === "health") {
      renderHealthView(view, statusCache);
    } else if (btn) {
      btn.disabled = false;
      btn.textContent = "Purge Caches";
    }
  }
}

function wireEvents() {
  const tabsEl = $("systemTabs");
  tabsEl?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-sys-tab]");
    if (!btn) return;
    switchTab(btn.dataset.sysTab);
  });

  tabsEl?.addEventListener("keydown", (e) => {
    if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
    const idx = TABS.findIndex((t) => t.id === activeTab);
    if (idx < 0) return;
    const nextIdx = e.key === "ArrowRight"
      ? (idx + 1) % TABS.length
      : (idx - 1 + TABS.length) % TABS.length;
    e.preventDefault();
    const nextTab = TABS[nextIdx].id;
    switchTab(nextTab);
    tabsEl.querySelector(`[data-sys-tab="${nextTab}"]`)?.focus();
  });

  const view = $("systemMaintenanceView");
  view?.addEventListener("input", (e) => {
    const t = e.target;
    if (t.id === "sysBackupNote") backupDraft.note = t.value;
    else if (t.id === "sysMaxTgDownloads") integrityDraft.maxTelegramDownloads = Number(t.value) || 0;
    else if (t.id === "sysMaxTgMb") integrityDraft.maxTelegramItemMb = Math.max(1, Number(t.value) || 250);
  });

  view?.addEventListener("change", (e) => {
    const t = e.target;
    if (t.id === "sysBackupTgCheck") backupDraft.uploadToTelegram = t.checked;
    else if (t.id === "sysVerifyTgCheck") integrityDraft.verifyTelegram = t.checked;
    else if (t.id === "sysBackfillThumbs") integrityDraft.backfillThumbs = t.checked;
    else if (t.id === "sysBackfillPreviews") integrityDraft.backfillPreviews = t.checked;
    else if (t.id === "sysBackfillMetadata") integrityDraft.backfillMetadata = t.checked;
    else if (t.id === "sysPurgeOrphanThumbs") integrityDraft.purgeOrphanThumbs = t.checked;
    else if (t.id === "sysPurgeOrphanStaging") integrityDraft.purgeOrphanStaging = t.checked;
  });

  view?.addEventListener("click", (e) => {
    const gotoBtn = e.target.closest("[data-sys-goto]");
    if (gotoBtn) {
      const target = gotoBtn.dataset.sysGoto;
      switchTab(target);
      if (target === "settings") $("sheetTokenInput")?.focus();
      return;
    }

    const retryBtn = e.target.closest("[data-sys-retry]");
    if (retryBtn) {
      loadTab(retryBtn.dataset.sysRetry);
      return;
    }

    const actionBtn = e.target.closest("[data-sys-action]");
    if (actionBtn) {
      const action = actionBtn.dataset.sysAction;
      if (action === "refresh-health") loadHealthTab({ silent: false });
      else if (action === "refresh-backups") loadBackupsTab({ silent: false });
      else if (action === "refresh-integrity") loadIntegrityTab({ silent: false });
      else if (action === "purge-caches") handlePurgeCaches();
      return;
    }

    if (e.target.closest("#sysCreateBackupBtn")) {
      handleCreateBackup();
      return;
    }

    const delBtn = e.target.closest("[data-sys-delete-backup]");
    if (delBtn) {
      handleDeleteBackup(
        delBtn.dataset.sysDeleteBackup,
        delBtn.dataset.sysBackupName || delBtn.dataset.sysDeleteBackup
      );
      return;
    }

    if (e.target.closest("#sysRunAuditBtn")) {
      handleRunAudit();
      return;
    }

    if (e.target.closest("#sysRunRepairBtn")) {
      handleRunRepair();
    }
  });

  $("systemSheet")?.addEventListener("close", () => {
    stopRepairPoll();
  });

  on("auth:unlocked", () => {
    authRejected = false;
    statusCache = null;
    backupsCache = null;
    integrityCache = null;
    if (isSheetOpen() && activeTab !== "settings") {
      loadTab(activeTab);
    }
  });

  on("auth:required", () => {
    authRejected = true;
    statusCache = null;
    backupsCache = null;
    integrityCache = null;
    stopRepairPoll();
    if (isSheetOpen() && activeTab !== "settings") {
      const maintView = $("systemMaintenanceView");
      if (maintView) renderLockedState(maintView);
    }
  });

  on("maintenance:update", async (payload) => {
    if (payload?.kind === "repair" && payload?.status === "done") {
      emit("media:changed", { source: "maintenance:repair" });
    }
    if (!isSheetOpen() || isLocked()) return;
    if (activeTab === "integrity") {
      await loadIntegrityTab({ silent: true });
    } else if (activeTab === "health" && payload?.status !== "running") {
      await loadHealthTab({ silent: true });
    }
  });

  on("backup:update", async () => {
    if (!isSheetOpen() || isLocked()) return;
    if (activeTab === "backups") {
      await loadBackupsTab({ silent: true });
    } else if (activeTab === "health") {
      await loadHealthTab({ silent: true });
    }
  });
}

export function openSystemSheet(tab = "settings") {
  initMaintenance();
  const sheet = $("systemSheet");
  if (!sheet) return;
  if (!sheet.open) {
    sheet.showModal();
  }
  switchTab(tab);
}

export function initMaintenance() {
  if (initialized) return;
  initialized = true;
  renderTabs();
  wireEvents();
}
