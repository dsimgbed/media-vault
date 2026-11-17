import { state } from "/static/js/state.js";
import { apiFetch } from "/static/js/api.js";
import { $, escapeHtml, fmtSize } from "/static/js/utils.js";

export async function loadFolders() {
  const data = await apiFetch("/api/folders");
  const list = $("folderList");
  const filter = $("folderFilter");
  const prev = state.pendingFilters.folder !== null ? state.pendingFilters.folder : filter.value;
  state.pendingFilters.folder = null;

  const folders = data.folders || [];
  list.innerHTML = folders.map((f) => `<option value="${escapeHtml(f)}"></option>`).join("");
  filter.innerHTML =
    `<option value="all">All folders</option>` +
    folders.map((f) => `<option value="${escapeHtml(f)}">${escapeHtml(f)}</option>`).join("");

  filter.value = prev && (prev === "all" || folders.includes(prev)) ? prev : "all";
}

export async function loadDrives() {
  const data = await apiFetch("/api/drives");
  const grid = $("drivesGrid");
  const driveSelect = $("driveFilter");
  const prevDrive = state.pendingFilters.drive !== null ? state.pendingFilters.drive : driveSelect.value;
  state.pendingFilters.drive = null;

  const validValues = new Set(["all"]);
  driveSelect.innerHTML = `<option value="all">All storage</option>`;
  if (data.storage_mode === "telegram" || data.storage_mode === "both") {
    driveSelect.insertAdjacentHTML("beforeend", `<option value="telegram">telegram</option>`);
    validValues.add("telegram");
  }

  if (data.storage_mode === "telegram") {
    const capLabel = state.maxUploadMb >= 1024
      ? `${(state.maxUploadMb / 1024).toFixed(0)} GB`
      : `${state.maxUploadMb} MB`;
    grid.innerHTML = `
      <div class="drive-card">
        <div class="drive-head"><span>Telegram Topic Archive</span><span class="pill ok">Multi-Part • Up to ${capLabel} / file</span></div>
        <div class="drive-meta">Files &gt;1.9 GiB are zero-copy sliced across workers and streamed back as a single unified file.</div>
      </div>`;
    driveSelect.value = prevDrive && validValues.has(prevDrive) ? prevDrive : "all";
    return;
  }

  grid.innerHTML = (data.drives || []).map((d) => {
    driveSelect.insertAdjacentHTML(
      "beforeend",
      `<option value="${escapeHtml(d.name)}">${escapeHtml(d.name)}</option>`
    );
    validValues.add(d.name);
    if (!d.mounted) {
      return `<div class="drive-card">
        <div class="drive-head"><span>${escapeHtml(d.name)}</span><span class="pill">unmounted</span></div>
        <div class="drive-meta">${escapeHtml(d.path)}</div>
      </div>`;
    }
    const pct = d.total_bytes ? Math.round((d.used_bytes / d.total_bytes) * 100) : 0;
    return `<div class="drive-card">
      <div class="drive-head"><span>${escapeHtml(d.name)}</span><span class="pill ok">${fmtSize(d.free_bytes)} free</span></div>
      <div class="drive-meta">${fmtSize(d.used_bytes)} used of ${fmtSize(d.total_bytes)} (${pct}%)</div>
      <div class="bar"><div style="width:${pct}%"></div></div>
    </div>`;
  }).join("");

  driveSelect.value = prevDrive && validValues.has(prevDrive) ? prevDrive : "all";
}
