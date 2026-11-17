import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch, withToken } from "/static/js/api.js";
import {
  $,
  debounce,
  escapeHtml,
  fmtDuration,
  fmtResolution,
  fmtSize,
  isZipItem,
} from "/static/js/utils.js";
import { getMode, onModeChange } from "/static/js/mode.js";
import { confirmDialog, promptDialog, toast } from "/static/js/ui/dialogs.js";
import { normalizeTagsList, openBulkTagDialog } from "/static/js/ui/tag-picker.js";
import { loadFolders } from "/static/js/features/drives.js";
import {
  getFilterTags,
  loadTags,
  setFilterTags,
  toggleFilterTag,
} from "/static/js/features/tags.js";

let galleryNeedsReload = false;
let galleryLoaded = false;
let lastLoadError = null;

const MAX_MOBILE_PREVIEWS = 2;
const activeMobilePreviews = new Set();
const visiblePreviewCards = new Set();
let mobilePreviewObserver = null;

function stopMobilePreview(card) {
  if (!card) return;
  activeMobilePreviews.delete(card);
  const img = card.querySelector(".thumb-wrap img");
  const staticSrc = img?.dataset.thumb;
  if (img && staticSrc && img.src !== staticSrc) {
    img.src = staticSrc;
  }
}

function stopAllMobilePreviews() {
  for (const card of Array.from(activeMobilePreviews)) {
    stopMobilePreview(card);
  }
}

function startMobilePreview(card) {
  if (!card || activeMobilePreviews.has(card)) return;
  const previewUrl = card.dataset.preview;
  const img = card.querySelector(".thumb-wrap img");
  if (!previewUrl || !img) return;
  while (activeMobilePreviews.size >= MAX_MOBILE_PREVIEWS) {
    const oldest = activeMobilePreviews.values().next().value;
    if (!oldest) break;
    stopMobilePreview(oldest);
  }
  img.src = previewUrl;
  activeMobilePreviews.add(card);
}

function syncMobilePreviews() {
  const mode = getMode();
  if (document.hidden || (mode !== "files" && mode !== "cinema")) {
    stopAllMobilePreviews();
    return;
  }
  for (const card of Array.from(visiblePreviewCards)) {
    if (!card.isConnected) {
      visiblePreviewCards.delete(card);
      stopMobilePreview(card);
      continue;
    }
    if (!activeMobilePreviews.has(card) && activeMobilePreviews.size < MAX_MOBILE_PREVIEWS) {
      startMobilePreview(card);
    }
  }
}

function ensureMobilePreviewObserver() {
  if (typeof IntersectionObserver === "undefined") return null;
  if (!window.matchMedia?.("(hover: none)")?.matches) return null;
  if (!mobilePreviewObserver) {
    mobilePreviewObserver = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          const card = entry.target;
          if (entry.isIntersecting && entry.intersectionRatio >= 0.65) {
            visiblePreviewCards.add(card);
          } else {
            visiblePreviewCards.delete(card);
            stopMobilePreview(card);
          }
        }
        syncMobilePreviews();
      },
      { threshold: 0.65 }
    );
  }
  return mobilePreviewObserver;
}

export function updateBulkToolbar() {
  const bar = $("bulkToolbar");
  const count = state.selectedIds.size;
  const show = state.selectMode || count > 0;
  bar.style.display = show ? "flex" : "none";
  $("bulkCountLabel").textContent = `${count} selected`;
  $("selectModeBtn").textContent = state.selectMode ? "Done Selecting" : "Select";
  $("galleryGrid").classList.toggle("select-mode", state.selectMode);
}

function getEffectiveFilters() {
  const mode = getMode();
  const typeVal = mode === "cinema" ? "video" : ($("typeFilter")?.value || "all");
  const driveVal = state.pendingFilters.drive ?? ($("driveFilter")?.value || "all");
  const folderVal = state.pendingFilters.folder ?? ($("folderFilter")?.value || "all");
  const filterTags = getFilterTags();
  const tagVal = filterTags.length
    ? filterTags.join(",")
    : (state.pendingFilters.tag ?? ($("tagFilter")?.value || "all"));
  const qVal = $("searchInput")?.value.trim() || "";
  const sortVal = $("sortSelect")?.value || "newest";
  return {
    media_type: typeVal,
    drive_name: driveVal,
    folder: folderVal,
    tag: tagVal,
    q: qVal,
    sort: sortVal,
  };
}

function hasActiveFilters() {
  const f = getEffectiveFilters();
  const mode = getMode();
  return Boolean(
    f.q
    || (mode !== "cinema" && f.media_type !== "all")
    || (f.folder && f.folder !== "all")
    || (f.drive_name && f.drive_name !== "all")
    || (f.tag && f.tag !== "all")
  );
}

function resetFilters() {
  if ($("searchInput")) $("searchInput").value = "";
  if (getMode() !== "cinema" && $("typeFilter")) {
    $("typeFilter").value = "all";
    state.savedTypeFilter = "all";
  }
  if ($("folderFilter")) $("folderFilter").value = "all";
  if ($("driveFilter")) $("driveFilter").value = "all";
  if ($("sortSelect")) $("sortSelect").value = "newest";
  state.pendingFilters.folder = null;
  state.pendingFilters.drive = null;
  state.pendingFilters.tag = null;
  setFilterTags([], { notify: false });
  loadMedia().catch((err) => toast(err.message, { type: "error" }));
}

function syncFiltersToUrl() {
  const mode = getMode();
  const filters = getEffectiveFilters();
  const url = new URL(location.href);

  if (filters.q) url.searchParams.set("q", filters.q);
  else url.searchParams.delete("q");

  const persistedType = mode === "cinema" ? state.savedTypeFilter : filters.media_type;
  if (persistedType && persistedType !== "all") url.searchParams.set("type", persistedType);
  else url.searchParams.delete("type");

  if (filters.folder && filters.folder !== "all") url.searchParams.set("folder", filters.folder);
  else url.searchParams.delete("folder");

  if (filters.drive_name && filters.drive_name !== "all") url.searchParams.set("drive", filters.drive_name);
  else url.searchParams.delete("drive");

  if (filters.sort && filters.sort !== "newest") url.searchParams.set("sort", filters.sort);
  else url.searchParams.delete("sort");

  if (filters.tag && filters.tag !== "all") url.searchParams.set("tags", filters.tag);
  else url.searchParams.delete("tags");

  history.replaceState(history.state, "", url.pathname + url.search + location.hash);
}

function renderCardHtml(it, activeTagSet = new Set(getFilterTags())) {
  const isCinema = getMode() === "cinema";
  const thumbUrl = it.thumb_filename ? withToken(`/api/media/${it.id}/thumb`) : "";
  const previewAttr = it.media_type === "video" && it.preview_filename
    ? ` data-preview="${escapeHtml(withToken(`/api/media/${it.id}/preview`))}"`
    : "";
  const thumbHtml = thumbUrl
    ? `<img loading="lazy" src="${escapeHtml(thumbUrl)}" data-thumb="${escapeHtml(thumbUrl)}" alt="${escapeHtml(it.title)}" />`
    : `<div class="thumb-fallback">${escapeHtml(it.media_type)}</div>`;
  const folder = it.folder || state.defaultFolder;
  const durTxt = fmtDuration(it.duration_sec);
  const resTxt = fmtResolution(it);
  const tagsHtml = it.tags && it.tags.length
    ? `<div class="tag-row">${it.tags.map((t) => `<span class="tag-chip${activeTagSet.has(t) ? " active" : ""}" data-tag="${escapeHtml(t)}">#${escapeHtml(t)}</span>`).join("")}</div>`
    : "";

  if (isCinema) {
    const isPlaying = state.cinemaActiveId === it.id;
    const subLeft = resTxt ? `${escapeHtml(folder)} • ${escapeHtml(resTxt)}` : escapeHtml(folder);
    return `
      <article class="card cinema-card${isPlaying ? " is-playing" : ""}" data-id="${escapeHtml(it.id)}"${previewAttr}>
        <div class="thumb-wrap cinema-thumb-wrap">
          ${thumbHtml}
          <span class="cinema-play-overlay" aria-hidden="true">&#9654;</span>
          <span class="badge-tl">${escapeHtml(folder)}</span>
          ${resTxt ? `<span class="badge-tr">${escapeHtml(resTxt)}</span>` : ""}
          ${durTxt ? `<span class="badge-br">${escapeHtml(durTxt)}</span>` : ""}
          ${isPlaying ? `<span class="badge-bl cinema-playing-pill">Now Playing</span>` : ""}
        </div>
        <div class="card-body">
          <div class="card-title" title="${escapeHtml(it.filename)}">${escapeHtml(it.title || it.filename)}</div>
          <div class="card-sub">
            <span>${subLeft}</span>
            <span>${fmtSize(it.size_bytes)}</span>
          </div>
          ${tagsHtml}
        </div>
      </article>
    `;
  }

  const typeBadge = isZipItem(it) ? "📦 ZIP" : (it.media_type === "video" ? "▶ Video" : it.media_type);
  const trParts = [durTxt, resTxt].filter(Boolean);
  const trBadge = trParts.length
    ? `<span class="badge-tr">${escapeHtml(trParts.join(" • "))}</span>`
    : "";
  const partCount = Number(it.part_count || 1);
  const partsBadge = partCount > 1
    ? `<span class="pill ok pill-compact">${partCount} Parts • Unified Stream</span>`
    : "";
  const isSelected = state.selectedIds.has(it.id);

  return `
    <article class="card${isSelected ? " selected" : ""}" data-id="${escapeHtml(it.id)}"${previewAttr}>
      <div class="thumb-wrap">
        <input type="checkbox" class="card-check" ${isSelected ? "checked" : ""} aria-label="Select item" />
        ${thumbHtml}
        <span class="badge-tl">${escapeHtml(typeBadge)} • ${escapeHtml(folder)}</span>
        ${trBadge}
        <span class="badge-br">${escapeHtml(it.drive_name)}</span>
      </div>
      <div class="card-body">
        <div class="card-title" title="${escapeHtml(it.filename)}">${escapeHtml(it.title || it.filename)}</div>
        <div class="card-sub">
          <span>${fmtSize(it.size_bytes)}</span>
          <span>${partsBadge || escapeHtml(it.source_origin.toUpperCase())}</span>
        </div>
        ${tagsHtml}
      </div>
    </article>
  `;
}

function bindCardHoverPreviews(cards) {
  for (const el of cards) {
    const previewUrl = el.dataset.preview;
    if (!previewUrl) continue;
    const img = el.querySelector(".thumb-wrap img");
    if (!img) continue;
    const staticSrc = img.dataset.thumb || img.src;
    el.addEventListener("mouseenter", () => {
      img.src = previewUrl;
    });
    el.addEventListener("mouseleave", () => {
      img.src = staticSrc;
    });
  }
}

export function renderGallery({ append = false, appendedItems = [] } = {}) {
  const grid = $("galleryGrid");
  const items = state.mediaItems;
  const total = state.mediaTotal;
  const activeTagSet = new Set(getFilterTags());

  $("countLabel").textContent = items.length < total
    ? `${items.length} of ${total} item(s)`
    : `${total} item(s)`;

  const loadMoreWrap = $("loadMoreWrap");
  loadMoreWrap.style.display = items.length < total ? "block" : "none";

  if (!append) {
    stopAllMobilePreviews();
    visiblePreviewCards.clear();
    mobilePreviewObserver?.disconnect();

    if (!items.length) {
      if (state.authRequired && !state.token) {
        grid.innerHTML = `<div class="empty"><strong>Vault is locked.</strong> Enter your auth token above to browse files and videos.<div class="empty-actions"><button type="button" class="secondary btn-sm focus-auth-btn">Enter Auth Token</button></div></div>`;
      } else if (lastLoadError) {
        grid.innerHTML = `<div class="empty"><strong>Could not load media:</strong> ${escapeHtml(lastLoadError)}<div class="empty-actions"><button type="button" class="secondary btn-sm retry-media-btn">Retry</button></div></div>`;
      } else if (hasActiveFilters()) {
        grid.innerHTML = `<div class="empty">No items match your current filters.<div class="empty-actions"><button type="button" class="secondary btn-sm reset-filters-btn">Reset Filters</button></div></div>`;
      } else {
        grid.innerHTML = `<div class="empty">No media yet. Paste a link, drop a file, or send one to your Telegram bot.</div>`;
      }
      updateBulkToolbar();
      return;
    }

    grid.innerHTML = items.map((it) => renderCardHtml(it, activeTagSet)).join("");
    const previewCards = grid.querySelectorAll(".card[data-preview]");
    bindCardHoverPreviews(previewCards);
    const obs = ensureMobilePreviewObserver();
    if (obs) {
      previewCards.forEach((card) => obs.observe(card));
    }
    updateBulkToolbar();
    return;
  }

  if (!appendedItems.length) {
    updateBulkToolbar();
    return;
  }

  const emptyEl = grid.querySelector(".empty");
  if (emptyEl) emptyEl.remove();

  const prevCount = grid.children.length;
  grid.insertAdjacentHTML(
    "beforeend",
    appendedItems.map((it) => renderCardHtml(it, activeTagSet)).join("")
  );
  const newCards = Array.from(grid.children)
    .slice(prevCount)
    .filter((el) => el.hasAttribute("data-preview"));
  bindCardHoverPreviews(newCards);
  const obs = ensureMobilePreviewObserver();
  if (obs) {
    newCards.forEach((card) => obs.observe(card));
  }
  updateBulkToolbar();
}

export async function loadMedia({ append = false } = {}) {
  if (append && state.isLoadingMore) return;
  if (state.authRequired && !state.token) {
    galleryLoaded = false;
    lastLoadError = null;
    if (!append) {
      state.mediaItems = [];
      state.mediaTotal = 0;
      renderGallery({ append: false });
      emit("media:loaded", { items: [], total: 0, append: false });
    }
    return;
  }

  state.isLoadingMore = true;
  const btn = $("loadMoreBtn");
  if (btn) btn.disabled = true;

  try {
    if (!append) {
      syncFiltersToUrl();
    }
    const offset = append ? state.mediaItems.length : 0;
    const filters = getEffectiveFilters();
    const params = new URLSearchParams({
      ...filters,
      limit: String(state.pageSize),
      offset: String(offset),
    });
    const data = await apiFetch(`/api/media?${params}`);
    lastLoadError = null;
    galleryLoaded = true;
    state.mediaTotal = data.total || 0;
    if (append) {
      const seen = new Set(state.mediaItems.map((x) => x.id));
      const extra = (data.items || []).filter((x) => !seen.has(x.id));
      state.mediaItems = [...state.mediaItems, ...extra];
      renderGallery({ append: true, appendedItems: extra });
    } else {
      state.mediaItems = data.items || [];
      renderGallery({ append: false });
    }
    emit("media:loaded", { items: state.mediaItems, total: state.mediaTotal, append });
  } catch (err) {
    galleryLoaded = false;
    lastLoadError = err?.message || "Request failed";
    if (!append) {
      renderGallery({ append: false });
      emit("media:loaded", { items: state.mediaItems, total: state.mediaTotal, append: false });
    }
    throw err;
  } finally {
    state.isLoadingMore = false;
    if (btn) btn.disabled = false;
  }
}

export function initGallery() {
  const grid = $("galleryGrid");

  let longPressTimer = null;
  let pressStartX = 0;
  let pressStartY = 0;
  let pressPointerId = null;
  let suppressedCardId = null;
  let suppressResetTimer = null;

  const clearLongPress = () => {
    if (longPressTimer) {
      clearTimeout(longPressTimer);
      longPressTimer = null;
    }
    pressPointerId = null;
  };

  const activateLongPress = (card) => {
    const id = card?.dataset.id;
    if (!id || getMode() === "cinema") return;
    state.selectMode = true;
    state.selectedIds.add(id);
    card.classList.add("selected");
    const cardCb = card.querySelector(".card-check");
    if (cardCb) cardCb.checked = true;
    updateBulkToolbar();
    try {
      navigator.vibrate?.(25);
    } catch (_) {}
    suppressedCardId = id;
    if (suppressResetTimer) clearTimeout(suppressResetTimer);
    suppressResetTimer = setTimeout(() => {
      suppressedCardId = null;
    }, 1000);
  };

  if (grid) {
    if (typeof PointerEvent !== "undefined") {
      grid.addEventListener("pointerdown", (e) => {
        if (e.pointerType !== "touch" || getMode() === "cinema") return;
        if (e.target.closest(".tag-chip") || e.target.closest(".card-check")) return;
        const card = e.target.closest(".card");
        if (!card || !card.dataset.id) return;
        clearLongPress();
        pressPointerId = e.pointerId;
        pressStartX = e.clientX;
        pressStartY = e.clientY;
        longPressTimer = setTimeout(() => {
          longPressTimer = null;
          activateLongPress(card);
        }, 450);
      });

      grid.addEventListener(
        "pointermove",
        (e) => {
          if (!longPressTimer || e.pointerId !== pressPointerId) return;
          if (Math.hypot(e.clientX - pressStartX, e.clientY - pressStartY) > 10) {
            clearLongPress();
          }
        },
        { passive: true }
      );

      grid.addEventListener("pointerup", clearLongPress);
      grid.addEventListener("pointercancel", clearLongPress);
    } else {
      grid.addEventListener(
        "touchstart",
        (e) => {
          if (getMode() === "cinema" || e.touches.length !== 1) return;
          if (e.target.closest(".tag-chip") || e.target.closest(".card-check")) return;
          const card = e.target.closest(".card");
          if (!card || !card.dataset.id) return;
          clearLongPress();
          pressStartX = e.touches[0].clientX;
          pressStartY = e.touches[0].clientY;
          longPressTimer = setTimeout(() => {
            longPressTimer = null;
            activateLongPress(card);
          }, 450);
        },
        { passive: true }
      );

      grid.addEventListener(
        "touchmove",
        (e) => {
          if (!longPressTimer || !e.touches.length) return;
          if (
            Math.hypot(e.touches[0].clientX - pressStartX, e.touches[0].clientY - pressStartY) > 10
          ) {
            clearLongPress();
          }
        },
        { passive: true }
      );

      grid.addEventListener("touchend", clearLongPress);
      grid.addEventListener("touchcancel", clearLongPress);
    }

    grid.addEventListener("contextmenu", (e) => {
      const card = e.target.closest(".card");
      if (card && suppressedCardId && card.dataset.id === suppressedCardId) {
        e.preventDefault();
      }
    });
  }

  window.addEventListener("scroll", clearLongPress, { passive: true, capture: true });

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      stopAllMobilePreviews();
    } else {
      syncMobilePreviews();
    }
  });

  grid?.addEventListener("click", (e) => {
    if (e.target.closest(".focus-auth-btn")) {
      const input = $("bannerTokenInput") || $("tokenInput");
      input?.scrollIntoView({ behavior: "smooth", block: "center" });
      input?.focus();
      return;
    }
    if (e.target.closest(".retry-media-btn")) {
      emit("system:refresh");
      return;
    }
    if (e.target.closest(".reset-filters-btn")) {
      resetFilters();
      return;
    }

    const clickedCard = e.target.closest(".card");
    if (clickedCard && suppressedCardId && clickedCard.dataset.id === suppressedCardId) {
      suppressedCardId = null;
      if (suppressResetTimer) {
        clearTimeout(suppressResetTimer);
        suppressResetTimer = null;
      }
      e.preventDefault();
      e.stopPropagation();
      return;
    }

    const chip = e.target.closest(".tag-chip");
    if (chip) {
      e.stopPropagation();
      const tag = chip.dataset.tag || "";
      if (tag) {
        toggleFilterTag(tag);
      }
      return;
    }

    const cb = e.target.closest(".card-check");
    if (cb) {
      e.stopPropagation();
      const card = cb.closest(".card");
      const id = card?.dataset.id;
      if (!id) return;
      if (cb.checked) state.selectedIds.add(id);
      else state.selectedIds.delete(id);
      card.classList.toggle("selected", cb.checked);
      updateBulkToolbar();
      return;
    }

    const card = clickedCard;
    if (!card) return;
    const id = card.dataset.id;
    if (!id) return;

    if (state.selectMode) {
      if (state.selectedIds.has(id)) state.selectedIds.delete(id);
      else state.selectedIds.add(id);
      const checked = state.selectedIds.has(id);
      const cardCb = card.querySelector(".card-check");
      if (cardCb) cardCb.checked = checked;
      card.classList.toggle("selected", checked);
      updateBulkToolbar();
      return;
    }

    if (getMode() === "cinema") {
      emit("cinema:play", { id });
    } else {
      emit("media:open", { id });
    }
  });

  on("tags:filter-changed", () => {
    loadMedia().catch((err) => toast(err.message, { type: "error" }));
  });

  ["typeFilter", "folderFilter", "tagFilter", "driveFilter", "sortSelect"].forEach((id) => {
    $(id)?.addEventListener("change", () => {
      if (id === "typeFilter" && getMode() !== "cinema") {
        state.savedTypeFilter = $("typeFilter").value;
      }
      if (id === "tagFilter") {
        const val = $("tagFilter").value;
        setFilterTags(val && val !== "all" ? normalizeTagsList(val) : [], { notify: false });
      }
      loadMedia().catch((err) => toast(err.message, { type: "error" }));
    });
  });

  const debouncedSearch = debounce(() => {
    loadMedia().catch((err) => toast(err.message, { type: "error" }));
  }, 250);
  $("searchInput")?.addEventListener("input", debouncedSearch);

  $("loadMoreBtn")?.addEventListener("click", () => {
    loadMedia({ append: true }).catch((err) => toast(err.message, { type: "error" }));
  });

  if (typeof IntersectionObserver !== "undefined") {
    const sentinel = $("loadMoreWrap");
    if (sentinel) {
      const observer = new IntersectionObserver(
        (entries) => {
          if (
            entries.some((e) => e.isIntersecting) &&
            !state.isLoadingMore &&
            state.mediaItems.length < state.mediaTotal
          ) {
            loadMedia({ append: true }).catch(() => {});
          }
        },
        { rootMargin: "200px" }
      );
      observer.observe(sentinel);
    }
  }

  $("selectModeBtn")?.addEventListener("click", () => {
    state.selectMode = !state.selectMode;
    if (!state.selectMode) {
      state.selectedIds.clear();
      renderGallery();
    } else {
      updateBulkToolbar();
    }
  });

  $("bulkSelectAllBtn")?.addEventListener("click", () => {
    state.selectMode = true;
    state.mediaItems.forEach((it) => state.selectedIds.add(it.id));
    renderGallery();
  });

  $("bulkClearBtn")?.addEventListener("click", () => {
    state.selectedIds.clear();
    renderGallery();
  });

  $("bulkMoveBtn")?.addEventListener("click", async () => {
    const ids = Array.from(state.selectedIds);
    if (!ids.length) return;
    const defaultVal = $("folderInput")?.value.trim() || state.defaultFolder;
    const folder = await promptDialog({
      title: "Move to Folder",
      label: "Move selected items to folder / topic:",
      value: defaultVal,
      placeholder: state.defaultFolder,
      datalistId: "folderList",
      confirmLabel: "Move",
    });
    if (folder === null || !folder.trim()) return;
    try {
      await apiFetch("/api/media/bulk/move", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ media_ids: ids, folder: folder.trim() }),
      });
      await Promise.all([loadFolders(), loadMedia()]);
    } catch (err) {
      toast(`Bulk move failed: ${err.message}`, { type: "error" });
    }
  });

  $("bulkTagsBtn")?.addEventListener("click", async () => {
    const ids = Array.from(state.selectedIds);
    if (!ids.length) return;
    const result = await openBulkTagDialog({ count: ids.length });
    if (!result) return;
    try {
      await apiFetch("/api/media/bulk/tags", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ media_ids: ids, tags: result.tags, mode: result.mode }),
      });
      await Promise.all([loadTags(), loadMedia()]);
    } catch (err) {
      toast(`Bulk tag update failed: ${err.message}`, { type: "error" });
    }
  });

  $("bulkDownloadBtn")?.addEventListener("click", () => {
    const ids = Array.from(state.selectedIds);
    if (!ids.length) return;
    const url = withToken(`/api/media/bulk/download?ids=${encodeURIComponent(ids.join(","))}`);
    const a = document.createElement("a");
    a.href = url;
    a.download = "media_bundle.zip";
    document.body.appendChild(a);
    a.click();
    a.remove();
  });

  $("bulkDeleteBtn")?.addEventListener("click", async () => {
    const ids = Array.from(state.selectedIds);
    if (!ids.length) return;
    const ok = await confirmDialog({
      title: "Delete Selected Media",
      message: `Delete ${ids.length} selected item(s) permanently?`,
      confirmLabel: "Delete",
      danger: true,
    });
    if (!ok) return;
    try {
      await apiFetch("/api/media/bulk/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ media_ids: ids }),
      });
      state.selectedIds.clear();
      state.selectMode = false;
      emit("media:changed", { source: "bulk-delete" });
    } catch (err) {
      toast(`Bulk delete failed: ${err.message}`, { type: "error" });
    }
  });

  on("auth:required", () => {
    galleryLoaded = false;
    lastLoadError = null;
    state.mediaItems = [];
    state.mediaTotal = 0;
    renderGallery({ append: false });
  });

  on("cinema:active-changed", ({ id } = {}) => {
    if (getMode() !== "cinema") return;
    grid?.querySelectorAll(".cinema-card").forEach((card) => {
      const active = Boolean(id && card.dataset.id === id);
      card.classList.toggle("is-playing", active);
      const thumb = card.querySelector(".cinema-thumb-wrap");
      if (!thumb) return;
      const existing = thumb.querySelector(".cinema-playing-pill");
      if (active && !existing) {
        thumb.insertAdjacentHTML("beforeend", `<span class="badge-bl cinema-playing-pill">Now Playing</span>`);
      } else if (!active && existing) {
        existing.remove();
      }
    });
  });

  on("cinema:load-more", () => {
    if (!state.isLoadingMore && state.mediaItems.length < state.mediaTotal) {
      loadMedia({ append: true }).catch(() => {});
    }
  });

  onModeChange((mode, prevMode) => {
    updateBulkToolbar();
    if (mode !== "files" && mode !== "cinema") {
      stopAllMobilePreviews();
    } else {
      syncMobilePreviews();
    }
    if (mode === "cinema") {
      galleryNeedsReload = false;
      loadMedia().catch((err) => toast(err.message, { type: "error" }));
    } else if (prevMode === "cinema" && mode === "files") {
      galleryNeedsReload = false;
      loadMedia().catch((err) => toast(err.message, { type: "error" }));
    } else if (prevMode === "cinema" && mode === "uploader") {
      galleryNeedsReload = true;
    } else if (mode === "files" && (galleryNeedsReload || !galleryLoaded)) {
      galleryNeedsReload = false;
      loadMedia().catch((err) => toast(err.message, { type: "error" }));
    }
  });
}
