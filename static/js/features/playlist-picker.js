import { state } from "/static/js/state.js";
import { emit } from "/static/js/bus.js";
import { apiFetch } from "/static/js/api.js";
import { $, escapeHtml, fmtDuration } from "/static/js/utils.js";
import { toast } from "/static/js/ui/dialogs.js";

let activeAbortController = null;

function ensurePlaylistModal() {
  let dialog = $("playlistModal");
  if (!dialog) {
    dialog = document.createElement("dialog");
    dialog.id = "playlistModal";
    dialog.className = "playlist-modal";
    dialog.innerHTML = `
      <div class="dialog-surface">
        <div class="modal-head">
          <strong id="plModalTitle" class="modal-title-truncate">Playlist Preview</strong>
          <button id="plCloseBtn" type="button" class="secondary btn-sm">Close</button>
        </div>
        <div id="plBody" class="modal-body pl-body"></div>
      </div>
    `;
    document.body.appendChild(dialog);

    const closeModal = () => {
      if (activeAbortController) {
        activeAbortController.abort();
        activeAbortController = null;
      }
      if (dialog.open) dialog.close();
    };

    dialog.querySelector("#plCloseBtn").addEventListener("click", closeModal);
    dialog.addEventListener("cancel", (e) => {
      e.preventDefault();
      closeModal();
    });
    dialog.addEventListener("click", (e) => {
      if (e.target === dialog) closeModal();
    });
  }
  return dialog;
}

function resolveInitialFolder(data, defaultFolder) {
  const def = (state.defaultFolder || "General").trim();
  const base = (defaultFolder || "").trim();
  const plTitle = (data.playlist_title || "").trim();
  const suggested = (data.suggested_folder || "").trim();

  if (base && base.toLowerCase() !== def.toLowerCase()) {
    return plTitle ? `${base}/${plTitle}` : base;
  }
  if (plTitle) {
    return suggested || plTitle;
  }
  return suggested || base || def;
}

function renderLoading(bodyEl, titleEl, url) {
  titleEl.textContent = "Inspecting playlist...";
  bodyEl.innerHTML = `
    <div class="pl-loading">
      <div class="pl-loading-msg">Inspecting playlist...</div>
      <div class="small-muted">${escapeHtml(url)}</div>
    </div>
  `;
}

function renderError(bodyEl, titleEl, message) {
  titleEl.textContent = "Playlist Inspection Failed";
  bodyEl.innerHTML = `
    <div class="pl-error">${escapeHtml(message || "Could not inspect playlist.")}</div>
    <div class="toolbar-group modal-actions-end">
      <button type="button" class="secondary" data-pl-close>Close</button>
    </div>
  `;
  bodyEl.querySelector("[data-pl-close]")?.addEventListener("click", () => {
    $("playlistModal")?.close();
  });
}

function renderPlaylistContent(dialog, bodyEl, titleEl, data, opts) {
  const entries = Array.isArray(data.entries) ? data.entries : [];
  const unavailCount = Math.max(0, Number(data.unavailable_count || 0));
  const plTitle = (data.playlist_title || "").trim() || "Playlist Preview";
  titleEl.textContent = plTitle;

  if (!entries.length) {
    const emptyMsg =
      unavailCount > 0
        ? `No available videos found (${unavailCount} unavailable/removed skipped).`
        : "No entries found for this URL.";
    bodyEl.innerHTML = `
      <div class="pl-empty">${escapeHtml(emptyMsg)}</div>
      <div class="toolbar-group modal-actions-end">
        <button type="button" class="secondary" data-pl-close>Close</button>
      </div>
    `;
    bodyEl.querySelector("[data-pl-close]")?.addEventListener("click", () => {
      dialog.close();
    });
    return;
  }

  const dupCount = Number(
    data.duplicate_count ?? entries.filter((ent) => Boolean(ent.already_in_vault)).length
  );
  const defaultForce = Boolean(opts.defaultForce);
  const allDuplicates = entries.length > 0 && entries.every((ent) => Boolean(ent.already_in_vault));

  const selectedIndices = new Set();
  entries.forEach((ent, idx) => {
    if (defaultForce || !ent.already_in_vault) {
      selectedIndices.add(idx);
    }
  });

  const initialFolder = resolveInitialFolder(data, opts.defaultFolder);
  const initialTags = opts.defaultTags || "";
  const initialFormat = opts.defaultFormat || "best";
  const forwardToTelegram =
    opts.forwardToTelegram !== undefined
      ? Boolean(opts.forwardToTelegram)
      : Boolean($("tgForwardCheck")?.checked);

  const standardFormats = [
    { value: "best", label: "Format: Best MP4" },
    { value: "1080p", label: "1080p MP4" },
    { value: "720p", label: "720p MP4" },
    { value: "480p", label: "480p MP4" },
    { value: "audio", label: "Audio only (MP3)" },
  ];
  const hasInitialFmt = standardFormats.some((f) => f.value === initialFormat);
  const formatOptions = hasInitialFmt
    ? standardFormats
    : [...standardFormats, { value: initialFormat, label: `Format: ${initialFormat}` }];

  const allDupHint =
    !defaultForce && allDuplicates
      ? `<div class="pl-hint">All ${entries.length} item(s) are already in the vault. Enable "Force re-ingest duplicates" or select items below to re-queue.</div>`
      : "";

  bodyEl.innerHTML = `
    <div class="pl-controls">
      <label class="modal-field">
        <span>Folder / Topic</span>
        <input id="plFolderInput" type="text" list="folderList" placeholder="${escapeHtml(state.defaultFolder || "General")}" />
      </label>
      <label class="modal-field">
        <span>Tags</span>
        <input id="plTagsInput" type="text" list="tagList" placeholder="#soap, #hd..." />
      </label>
      <label class="modal-field">
        <span>Format</span>
        <select id="plFormatSelect">
          ${formatOptions
            .map(
              (f) =>
                `<option value="${escapeHtml(f.value)}">${escapeHtml(f.label)}</option>`
            )
            .join("")}
        </select>
      </label>
      <label class="checkbox-label pl-force-label">
        <input id="plForceCheck" type="checkbox" />
        Force re-ingest duplicates
      </label>
    </div>

    ${allDupHint}

    <div class="pl-toolbar">
      <input type="search" class="pl-search-input" placeholder="Filter playlist items..." aria-label="Filter playlist items" />
      <div class="toolbar-group pl-toolbar-actions">
        <button type="button" class="secondary btn-xs" data-pl-select="all">Select All</button>
        <button type="button" class="secondary btn-xs" data-pl-select="new">Select New Only</button>
        <button type="button" class="secondary btn-xs" data-pl-select="none">Deselect All</button>
        ${dupCount > 0 ? `<span class="pill warn pl-dup-summary">${dupCount} already in vault</span>` : ""}
        ${unavailCount > 0 ? `<span class="pill pl-unavail-summary">${unavailCount} unavailable skipped</span>` : ""}
      </div>
    </div>

    <div class="pl-list">
      ${entries
        .map((ent, idx) => {
          const isDup = Boolean(ent.already_in_vault);
          const dur = fmtDuration(ent.duration);
          const checkedAttr = selectedIndices.has(idx) ? "checked" : "";
          const dupBadge = isDup
            ? `<span class="pl-dup-badge">In Vault (${escapeHtml(ent.existing_folder || "")})</span>`
            : "";
          const durEl = dur ? `<span class="pl-entry-dur">${escapeHtml(dur)}</span>` : "";
          return `
            <div class="pl-entry${isDup ? " is-duplicate" : ""}" data-idx="${idx}">
              <label>
                <input type="checkbox" class="pl-entry-check" data-idx="${idx}" ${checkedAttr} />
                <span class="pl-entry-index">#${idx + 1}</span>
                <span class="pl-entry-title">${escapeHtml(ent.title || ent.url || `Item ${idx + 1}`)}</span>
                ${durEl}
                ${dupBadge}
              </label>
            </div>
          `;
        })
        .join("")}
    </div>

    <div class="pl-footer">
      <span class="pl-summary"></span>
      <div class="toolbar-group">
        <button type="button" class="secondary" data-pl-cancel>Cancel</button>
        <button type="button" class="pl-submit-btn">Queue Selected (0)</button>
      </div>
    </div>
  `;

  const folderEl = bodyEl.querySelector("#plFolderInput");
  const tagsEl = bodyEl.querySelector("#plTagsInput");
  const formatEl = bodyEl.querySelector("#plFormatSelect");
  const forceEl = bodyEl.querySelector("#plForceCheck");
  const searchEl = bodyEl.querySelector(".pl-search-input");
  const listEl = bodyEl.querySelector(".pl-list");
  const summaryEl = bodyEl.querySelector(".pl-summary");
  const submitBtn = bodyEl.querySelector(".pl-submit-btn");

  folderEl.value = initialFolder;
  tagsEl.value = initialTags;
  formatEl.value = initialFormat;
  forceEl.checked = defaultForce;

  const rowEls = Array.from(listEl.querySelectorAll(".pl-entry"));
  const checkEls = Array.from(listEl.querySelectorAll(".pl-entry-check"));

  const syncSummary = () => {
    const n = selectedIndices.size;
    summaryEl.textContent = `${n} of ${entries.length} selected`;
    submitBtn.textContent = `Queue Selected (${n})`;
    submitBtn.disabled = n === 0;
  };

  const applyFilter = () => {
    const q = searchEl.value.trim().toLowerCase();
    for (const row of rowEls) {
      const idx = Number(row.dataset.idx);
      const ent = entries[idx];
      if (!q) {
        row.hidden = false;
      } else {
        const hay = `${ent.title || ""} ${ent.url || ""}`.toLowerCase();
        row.hidden = !hay.includes(q);
      }
    }
  };

  listEl.addEventListener("change", (e) => {
    const cb = e.target.closest(".pl-entry-check");
    if (!cb) return;
    const idx = Number(cb.dataset.idx);
    if (cb.checked) selectedIndices.add(idx);
    else selectedIndices.delete(idx);
    syncSummary();
  });

  searchEl.addEventListener("input", applyFilter);

  bodyEl.querySelectorAll("[data-pl-select]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const mode = btn.dataset.plSelect;
      for (let i = 0; i < rowEls.length; i++) {
        if (rowEls[i].hidden) continue;
        const idx = Number(rowEls[i].dataset.idx);
        const ent = entries[idx];
        let next = false;
        if (mode === "all") next = true;
        else if (mode === "new") next = !ent.already_in_vault;
        else if (mode === "none") next = false;

        if (next) selectedIndices.add(idx);
        else selectedIndices.delete(idx);
        checkEls[i].checked = next;
      }
      syncSummary();
    });
  });

  bodyEl.querySelector("[data-pl-cancel]")?.addEventListener("click", () => {
    dialog.close();
  });

  submitBtn.addEventListener("click", async () => {
    const selectedUrls = entries
      .filter((_, idx) => selectedIndices.has(idx))
      .map((ent) => ent.url)
      .filter(Boolean);
    if (!selectedUrls.length) return;

    const folder = folderEl.value.trim() || state.defaultFolder || "General";
    const tags = tagsEl.value.trim();
    const format_spec = formatEl.value || "best";
    const force = Boolean(forceEl.checked);

    submitBtn.disabled = true;
    submitBtn.textContent = "Queueing...";
    try {
      await apiFetch("/api/ingest/url", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          urls: selectedUrls,
          folder,
          tags,
          format_spec,
          forward_to_telegram: forwardToTelegram,
          force,
          expand_playlist: false,
        }),
      });
      if (dialog.open) dialog.close();
      const urlInput = $("urlInput");
      if (urlInput) {
        urlInput.value = "";
        urlInput.rows = 1;
      }
      emit("jobs:refresh");
      toast(`Queued ${selectedUrls.length} item(s) from playlist`, { type: "success" });
    } catch (err) {
      toast(`Failed queueing playlist: ${err.message}`, { type: "error" });
      syncSummary();
    }
  });

  syncSummary();
}

export async function openPlaylistPicker(
  url,
  { defaultFolder, defaultTags, defaultFormat, forwardToTelegram, defaultForce } = {}
) {
  const targetUrl = String(url || "").trim();
  if (!targetUrl) {
    toast("Paste a playlist URL first.", { type: "error" });
    return;
  }

  if (activeAbortController) {
    activeAbortController.abort();
  }
  const controller = new AbortController();
  activeAbortController = controller;

  const dialog = ensurePlaylistModal();
  const titleEl = dialog.querySelector("#plModalTitle");
  const bodyEl = dialog.querySelector("#plBody");

  renderLoading(bodyEl, titleEl, targetUrl);
  if (!dialog.open) dialog.showModal();

  try {
    const data = await apiFetch("/api/ytdlp/playlist", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: targetUrl, max_items: 500 }),
      signal: controller.signal,
    });
    if (controller.signal.aborted || !dialog.open) return;
    if (activeAbortController === controller) {
      activeAbortController = null;
    }
    renderPlaylistContent(dialog, bodyEl, titleEl, data, {
      defaultFolder,
      defaultTags,
      defaultFormat,
      forwardToTelegram,
      defaultForce,
    });
  } catch (err) {
    if (err?.name === "AbortError" || controller.signal.aborted) return;
    if (activeAbortController === controller) {
      activeAbortController = null;
    }
    renderError(bodyEl, titleEl, err.message);
  }
}
