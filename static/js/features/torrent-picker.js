import { state } from "/static/js/state.js";
import { emit } from "/static/js/bus.js";
import { apiFetch } from "/static/js/api.js";
import { $, escapeHtml, fmtSize } from "/static/js/utils.js";
import { toast } from "/static/js/ui/dialogs.js";

let activeAbortController = null;

function closeTorrentModal(dialog) {
  if (activeAbortController) {
    activeAbortController.abort();
    activeAbortController = null;
  }
  if (dialog?.open) dialog.close();
}

function ensureTorrentModal() {
  let dialog = $("torrentModal");
  if (!dialog) {
    dialog = document.createElement("dialog");
    dialog.id = "torrentModal";
    dialog.className = "playlist-modal torrent-modal";
    dialog.innerHTML = `
      <div class="dialog-surface">
        <div class="modal-head">
          <strong id="trModalTitle" class="modal-title-truncate">Torrent Inspector</strong>
          <button id="trCloseBtn" type="button" class="secondary btn-sm">Close</button>
        </div>
        <div id="trBody" class="modal-body tr-body"></div>
      </div>
    `;
    document.body.appendChild(dialog);

    dialog.querySelector("#trCloseBtn").addEventListener("click", () => closeTorrentModal(dialog));
    dialog.addEventListener("cancel", (e) => {
      e.preventDefault();
      closeTorrentModal(dialog);
    });
    dialog.addEventListener("click", (e) => {
      if (e.target === dialog || e.target.closest?.("[data-tr-close]")) {
        closeTorrentModal(dialog);
      }
    });
  }
  return dialog;
}

function truncateText(value, max = 120) {
  const text = String(value || "");
  return text.length > max ? `${text.slice(0, max - 3)}...` : text;
}

function renderLoading(bodyEl, { file, magnet }) {
  if (file) {
    bodyEl.innerHTML = `
      <div class="tr-loading">
        <div class="tr-loading-msg">Inspecting .torrent file...</div>
        <div class="small-muted">${escapeHtml(file.name || "torrent file")}</div>
      </div>
    `;
    return;
  }

  bodyEl.innerHTML = `
    <div class="tr-loading">
      <div class="tr-loading-msg">Fetching torrent metadata from peers...</div>
      <div class="small-muted">${escapeHtml(truncateText(magnet))}</div>
      <div class="small-muted">This can take up to 45s while DHT/trackers respond.</div>
      <div class="toolbar-group modal-actions-end">
        <button type="button" class="secondary" data-tr-close>Cancel</button>
      </div>
    </div>
  `;
}

function renderError(bodyEl, message) {
  bodyEl.innerHTML = `
    <div class="tr-error">
      <div class="tr-error-title">Torrent inspection failed</div>
      <div class="tr-error-msg">${escapeHtml(message || "Could not inspect torrent.")}</div>
    </div>
    <div class="toolbar-group modal-actions-end">
      <button type="button" class="secondary" data-tr-close>Close</button>
    </div>
  `;
}

function renderTorrentContent(dialog, bodyEl, data, opts, magnet) {
  const files = Array.isArray(data.files) ? data.files : [];
  if (!files.length) {
    bodyEl.innerHTML = `
      <div class="pl-empty">No files found in this torrent.</div>
      <div class="toolbar-group modal-actions-end">
        <button type="button" class="secondary" data-tr-close>Close</button>
      </div>
    `;
    return;
  }

  const torrentName = String(data.name || "").trim() || "Torrent";
  const infohash = String(data.infohash || "");
  const totalBytes = Number(data.total_bytes) || 0;
  const diskFree = Number(data.disk_free_bytes) || 0;
  const maxTotal = Number(data.max_total_bytes) || 0;
  const minBuffer = Number(data.min_free_buffer_bytes) || 0;
  const initialFolder =
    String(opts.defaultFolder || state.defaultFolder || "General").trim() || "General";
  const initialTags = String(opts.defaultTags || "");
  const defaultForce = Boolean(opts.defaultForce);
  const forwardToTelegram =
    opts.forwardToTelegram !== undefined
      ? Boolean(opts.forwardToTelegram)
      : Boolean($("tgForwardCheck")?.checked);
  const filesByIndex = new Map();
  const selectedIndices = new Set();

  files.forEach((f, i) => {
    const idx = Number(f.index ?? i);
    filesByIndex.set(idx, f);
    if (f.selected_default !== false) selectedIndices.add(idx);
  });

  const rowsHtml = files
    .map((f, i) => {
      const idx = Number(f.index ?? i);
      const media = String(f.media_type || "");
      const mediaType = ["video", "image", "audio"].includes(media) ? media : "file";
      const skipped = !f.selected_default;
      const relPath = String(f.path || f.name || `File ${idx}`);
      const name = String(f.name || "");
      const nameHtml =
        name && name !== relPath
          ? `<span class="tr-entry-name">${escapeHtml(name)}</span>`
          : "";
      return `
        <div class="tr-entry${skipped ? " is-skipped" : ""}" data-index="${idx}">
          <label>
            <input type="checkbox" class="tr-entry-check" data-index="${idx}" ${selectedIndices.has(idx) ? "checked" : ""} />
            <span class="tr-entry-index">#${idx}</span>
            <span class="tr-entry-body">
              <span class="tr-entry-path" title="${escapeHtml(relPath)}">${escapeHtml(relPath)}</span>
              ${nameHtml}
              <span class="tr-entry-meta">
                <span class="pill pill-compact tr-entry-type">${escapeHtml(mediaType)}</span>
                ${skipped ? `<span class="pill warn pill-compact">skipped by default</span>` : ""}
              </span>
            </span>
            <span class="tr-entry-size">${escapeHtml(fmtSize(Number(f.size_bytes) || 0))}</span>
          </label>
        </div>
      `;
    })
    .join("");

  bodyEl.innerHTML = `
    <div class="tr-meta-bar" role="group" aria-label="Torrent details">
      <span class="pill tr-hash-pill" title="${escapeHtml(infohash)}">${escapeHtml(infohash.slice(0, 12))}</span>
      <span class="tr-meta-label">${files.length} file${files.length === 1 ? "" : "s"}</span>
      <span class="tr-meta-label">${escapeHtml(fmtSize(totalBytes))} total</span>
      <span class="pill tr-disk-pill">Disk free: ${escapeHtml(fmtSize(diskFree))}</span>
    </div>

    <div class="pl-controls tr-controls">
      <label class="modal-field">
        <span>Folder / Topic</span>
        <input id="trFolderInput" type="text" list="folderList" placeholder="${escapeHtml(initialFolder)}" />
      </label>
      <label class="modal-field">
        <span>Tags</span>
        <input id="trTagsInput" type="text" list="tagList" placeholder="#soap, #hd..." />
      </label>
      <div class="tr-check-row">
        <label class="checkbox-label">
          <input id="trSubfolderCheck" type="checkbox" checked />
          Create subfolder (${escapeHtml(torrentName)})
        </label>
        <label class="checkbox-label">
          <input id="trForceCheck" type="checkbox" />
          Force re-ingest duplicates
        </label>
      </div>
    </div>

    <div id="trDiskWarning" class="tr-warning" role="status" aria-live="polite" hidden></div>

    <div class="pl-toolbar tr-toolbar">
      <input type="search" class="tr-search-input" placeholder="Filter files by name or path..." aria-label="Filter files by name or path" />
      <div class="toolbar-group pl-toolbar-actions">
        <button type="button" class="secondary btn-xs" data-tr-select="videos">Videos Only</button>
        <button type="button" class="secondary btn-xs" data-tr-select="smart">Skip Samples/NFO</button>
        <button type="button" class="secondary btn-xs" data-tr-select="all">Select All</button>
        <button type="button" class="secondary btn-xs" data-tr-select="none">Deselect All</button>
      </div>
    </div>

    <div class="pl-list tr-list">${rowsHtml}</div>

    <div class="pl-footer tr-footer">
      <span class="pl-summary tr-summary"></span>
      <div class="toolbar-group">
        <button type="button" class="secondary" data-tr-cancel>Cancel</button>
        <button type="button" class="tr-submit-btn">Start Leech → Vault (0 files • 0 B)</button>
      </div>
    </div>
  `;

  const folderEl = bodyEl.querySelector("#trFolderInput");
  const tagsEl = bodyEl.querySelector("#trTagsInput");
  const subfolderEl = bodyEl.querySelector("#trSubfolderCheck");
  const forceEl = bodyEl.querySelector("#trForceCheck");
  const searchEl = bodyEl.querySelector(".tr-search-input");
  const warningEl = bodyEl.querySelector("#trDiskWarning");
  const listEl = bodyEl.querySelector(".tr-list");
  const summaryEl = bodyEl.querySelector(".tr-summary");
  const submitBtn = bodyEl.querySelector(".tr-submit-btn");

  folderEl.value = initialFolder;
  tagsEl.value = initialTags;
  subfolderEl.checked = true;
  forceEl.checked = defaultForce;

  const rowEls = Array.from(listEl.querySelectorAll(".tr-entry"));
  const checkEls = Array.from(listEl.querySelectorAll(".tr-entry-check"));

  const selectedBytes = () =>
    files.reduce(
      (sum, f, i) =>
        sum + (selectedIndices.has(Number(f.index ?? i)) ? Number(f.size_bytes) || 0 : 0),
      0
    );

  const syncSummary = () => {
    const bytes = selectedBytes();
    const sizeLabel = fmtSize(bytes);
    const count = selectedIndices.size;
    summaryEl.textContent = `${count} of ${files.length} files selected • ${sizeLabel}`;
    submitBtn.textContent = `Start Leech → Vault (${count} files • ${sizeLabel})`;

    const reasons = [];
    if (maxTotal > 0 && bytes > maxTotal) {
      reasons.push(
        `Selection (${sizeLabel}) exceeds the configured torrent size cap (${fmtSize(maxTotal)}).`
      );
    }
    if (diskFree - bytes < minBuffer) {
      reasons.push(
        `Selection leaves less than ${fmtSize(minBuffer)} free on the staging disk (${fmtSize(diskFree)} free). Deselect files to continue.`
      );
    }
    warningEl.hidden = reasons.length === 0;
    warningEl.textContent = reasons.join(" ");
    submitBtn.disabled = count === 0 || reasons.length > 0;
  };

  const applyFilter = () => {
    const q = searchEl.value.trim().toLowerCase();
    for (const row of rowEls) {
      const f = filesByIndex.get(Number(row.dataset.index));
      const hay = `${f?.path || ""} ${f?.name || ""}`.toLowerCase();
      row.hidden = Boolean(q) && !hay.includes(q);
    }
  };

  listEl.addEventListener("change", (e) => {
    const cb = e.target.closest(".tr-entry-check");
    if (!cb) return;
    const idx = Number(cb.dataset.index);
    if (cb.checked) selectedIndices.add(idx);
    else selectedIndices.delete(idx);
    syncSummary();
  });

  searchEl.addEventListener("input", applyFilter);

  bodyEl.querySelectorAll("[data-tr-select]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const mode = btn.dataset.trSelect;
      const visibleRows = rowEls
        .map((row, i) => ({ row, check: checkEls[i] }))
        .filter(({ row }) => !row.hidden);
      const hasDefaultVideos = files.some(
        (f) => f.media_type === "video" && f.selected_default !== false
      );

      visibleRows.forEach(({ row, check }) => {
        const idx = Number(row.dataset.index);
        const f = filesByIndex.get(idx) || {};
        let next = false;
        if (mode === "all") next = true;
        else if (mode === "none") next = false;
        else if (mode === "smart") next = f.selected_default !== false;
        else if (mode === "videos") {
          next = f.media_type === "video" && (hasDefaultVideos ? f.selected_default !== false : true);
        }
        if (next) selectedIndices.add(idx);
        else selectedIndices.delete(idx);
        check.checked = next;
      });
      syncSummary();
    });
  });

  bodyEl.querySelector("[data-tr-cancel]")?.addEventListener("click", () => closeTorrentModal(dialog));

  submitBtn.addEventListener("click", async () => {
    const selected = Array.from(selectedIndices).sort((a, b) => a - b);
    if (!selected.length || submitBtn.disabled) return;

    const folder = folderEl.value.trim() || state.defaultFolder || "General";
    const tags = tagsEl.value.trim();

    submitBtn.disabled = true;
    submitBtn.textContent = "Starting leech...";
    try {
      await apiFetch("/api/ingest/torrent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          inspect_id: data.inspect_id,
          selected_indices: selected,
          folder,
          use_torrent_subfolder: Boolean(subfolderEl.checked),
          tags,
          forward_to_telegram: forwardToTelegram,
          force: Boolean(forceEl.checked),
        }),
      });
      if (dialog.open) dialog.close();
      const magnetText = String(magnet || "").trim();
      const urlInput = $("urlInput");
      if (urlInput && magnetText && urlInput.value.includes(magnetText)) {
        urlInput.value = "";
      }
      emit("jobs:refresh");
      toast(`Torrent leech queued: ${torrentName} (${selected.length} files)`, { type: "success" });
    } catch (err) {
      toast(`Failed queuing torrent: ${err.message}`, { type: "error" });
      syncSummary();
    }
  });

  syncSummary();
}

export async function openTorrentPicker(
  { magnet = "", file = null } = {},
  { defaultFolder, defaultTags, forwardToTelegram, defaultForce } = {}
) {
  const magnetText = String(magnet || "").trim();
  if (!file && !magnetText) {
    toast("Provide a magnet link or a .torrent file first.", { type: "error" });
    return;
  }

  if (activeAbortController) {
    activeAbortController.abort();
  }
  const controller = new AbortController();
  activeAbortController = controller;

  const dialog = ensureTorrentModal();
  const bodyEl = dialog.querySelector("#trBody");

  renderLoading(bodyEl, { file, magnet: magnetText });
  if (!dialog.open) dialog.showModal();

  try {
    let data;
    if (file) {
      const form = new FormData();
      form.append("file", file);
      data = await apiFetch("/api/ingest/torrent/inspect", {
        method: "POST",
        body: form,
        signal: controller.signal,
      });
    } else {
      data = await apiFetch("/api/ingest/torrent/inspect", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ magnet: magnetText }),
        signal: controller.signal,
      });
    }
    if (controller.signal.aborted || !dialog.open) return;
    if (activeAbortController === controller) {
      activeAbortController = null;
    }
    renderTorrentContent(
      dialog,
      bodyEl,
      data,
      { defaultFolder, defaultTags, forwardToTelegram, defaultForce },
      magnetText
    );
  } catch (err) {
    if (err?.name === "AbortError" || controller.signal.aborted) return;
    if (activeAbortController === controller) {
      activeAbortController = null;
    }
    renderError(bodyEl, err.message);
  }
}
