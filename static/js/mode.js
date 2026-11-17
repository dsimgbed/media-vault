import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { $ } from "/static/js/utils.js";

const VALID_MODES = new Set(["uploader", "files", "cinema"]);
const VALID_TYPES = new Set(["all", "video", "image", "audio", "file"]);
const VALID_SORTS = new Set(["newest", "largest"]);
const modeListeners = new Set();

export function getMode() {
  return state.mode;
}

export function onModeChange(fn) {
  modeListeners.add(fn);
  return () => modeListeners.delete(fn);
}

function updateModeControls(mode) {
  document.querySelectorAll("[data-mode-target]").forEach((el) => {
    const active = el.dataset.modeTarget === mode;
    if (el.getAttribute("role") === "tab") {
      el.setAttribute("aria-selected", active ? "true" : "false");
    } else {
      if (active) el.setAttribute("aria-current", "page");
      else el.removeAttribute("aria-current");
    }
  });
}

function updateUrlMode(mode, { push = false } = {}) {
  const url = new URL(location.href);
  url.searchParams.set("mode", mode);
  const nextUrl = url.pathname + url.search + location.hash;
  if (push) {
    history.pushState({ mode }, "", nextUrl);
  } else {
    history.replaceState(history.state, "", nextUrl);
  }
}

export function setMode(mode, { push = false, _initial = false } = {}) {
  const nextMode = VALID_MODES.has(mode) ? mode : "uploader";
  const prevMode = state.mode;

  if (!_initial) {
    if (nextMode === "cinema" && prevMode !== "cinema") {
      const typeEl = $("typeFilter");
      state.savedTypeFilter = typeEl ? typeEl.value : "all";
      if (typeEl) typeEl.value = "video";
      state.selectMode = false;
      state.selectedIds.clear();
    } else if (prevMode === "cinema" && nextMode !== "cinema") {
      const typeEl = $("typeFilter");
      if (typeEl) typeEl.value = state.savedTypeFilter || "all";
    }
  }

  state.mode = nextMode;
  document.body.dataset.mode = nextMode;
  try {
    localStorage.setItem("vault_mode", nextMode);
  } catch (_) {}

  updateUrlMode(nextMode, { push });
  updateModeControls(nextMode);

  if (!_initial && nextMode !== prevMode) {
    for (const fn of modeListeners) {
      fn(nextMode, prevMode);
    }
    emit("mode:changed", { mode: nextMode, prevMode });
  }
}

function updateActiveJobBadges(count) {
  const n = Number(count || 0);
  document.querySelectorAll('.mode-badge[data-badge-for="uploader"]').forEach((badge) => {
    if (n > 0) {
      badge.textContent = String(n);
      badge.hidden = false;
    } else {
      badge.textContent = "0";
      badge.hidden = true;
    }
  });
}

export function initMode() {
  const params = new URLSearchParams(location.search);

  // Restore filter values from URL params before any initial load
  const qParam = params.get("q");
  if (qParam !== null && $("searchInput")) {
    $("searchInput").value = qParam;
  }

  const sortParam = params.get("sort");
  if (sortParam && VALID_SORTS.has(sortParam) && $("sortSelect")) {
    $("sortSelect").value = sortParam;
  }

  const typeParam = params.get("type");
  const initialUserType = typeParam && VALID_TYPES.has(typeParam) ? typeParam : "all";
  state.savedTypeFilter = initialUserType;

  const folderParam = params.get("folder");
  if (folderParam) {
    state.pendingFilters.folder = folderParam;
  }

  const driveParam = params.get("drive");
  if (driveParam) {
    state.pendingFilters.drive = driveParam;
  }

  const parseTagsFromParams = (searchParams) => {
    const raw = searchParams.get("tags") ?? searchParams.get("tag");
    if (!raw) return [];
    const parsed = [];
    const seen = new Set();
    for (const part of raw.split(",")) {
      const clean = part
        .trim()
        .replace(/^#+/, "")
        .toLowerCase()
        .replace(/[^a-z0-9_-]+/g, "")
        .slice(0, 48);
      if (clean && !seen.has(clean)) {
        seen.add(clean);
        parsed.push(clean);
      }
    }
    return parsed;
  };

  const parsedTags = parseTagsFromParams(params);
  state.selectedFilterTags = parsedTags;
  if (parsedTags.length) {
    state.pendingFilters.tag = parsedTags.join(",");
  }

  // Resolution order: ?mode= URL param > localStorage.vault_mode > default uploader
  const urlMode = params.get("mode");
  let savedMode = null;
  try {
    savedMode = localStorage.getItem("vault_mode");
  } catch (_) {}
  const initialMode = VALID_MODES.has(urlMode)
    ? urlMode
    : (VALID_MODES.has(savedMode) ? savedMode : "uploader");

  if ($("typeFilter")) {
    $("typeFilter").value = initialMode === "cinema" ? "video" : initialUserType;
  }

  setMode(initialMode, { push: false, _initial: true });

  document.querySelectorAll("[data-mode-target]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setMode(btn.dataset.modeTarget);
    });
  });

  on("jobs:active-count", (detail) => {
    updateActiveJobBadges(detail?.count || 0);
  });

  window.addEventListener("popstate", () => {
    const p = new URLSearchParams(location.search);
    const nextTags = parseTagsFromParams(p);
    const prevJoined = (state.selectedFilterTags || []).join(",");
    const nextJoined = nextTags.join(",");
    if (prevJoined !== nextJoined) {
      state.selectedFilterTags = nextTags;
      state.pendingFilters.tag = nextTags.length ? nextJoined : null;
      emit("filters:popstate", { tags: nextTags });
    }
    const m = p.get("mode");
    if (m && VALID_MODES.has(m) && m !== state.mode) {
      setMode(m, { push: false });
    }
  });
}
