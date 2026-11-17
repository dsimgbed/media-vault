import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch } from "/static/js/api.js";
import { $, escapeHtml } from "/static/js/utils.js";
import {
  createTagPicker,
  normalizeTagToken,
  normalizeTagsList,
} from "/static/js/ui/tag-picker.js";

let ingestTagPicker = null;
let filterTagPicker = null;
let tagsInitialized = false;

export function getIngestTagPicker() {
  return ingestTagPicker;
}

export function getFilterTags() {
  return Array.isArray(state.selectedFilterTags) ? [...state.selectedFilterTags] : [];
}

function syncSelectElement(filterEl, tags) {
  if (!filterEl) return;
  const prevSynth = filterEl.querySelector('option[data-tag-picker-synth="true"]');
  if (prevSynth) prevSynth.remove();

  if (!tags.length) {
    filterEl.value = "all";
    return;
  }

  if (tags.length === 1) {
    const t = tags[0];
    let opt = Array.from(filterEl.options).find((o) => o.value === t);
    if (!opt) {
      opt = document.createElement("option");
      opt.value = t;
      opt.textContent = `#${t}`;
      opt.dataset.tagPickerSynth = "true";
      filterEl.appendChild(opt);
    }
    filterEl.value = t;
    return;
  }

  const joined = tags.join(",");
  const opt = document.createElement("option");
  opt.value = joined;
  opt.textContent = tags.map((t) => `#${t}`).join(", ");
  opt.dataset.tagPickerSynth = "true";
  filterEl.appendChild(opt);
  filterEl.value = joined;
}

export function setFilterTags(tags, { notify = true } = {}) {
  const clean = normalizeTagsList(tags);
  state.selectedFilterTags = clean;
  state.pendingFilters.tag = null;

  if (filterTagPicker) {
    filterTagPicker.setTags(clean, { silent: true });
  } else {
    syncSelectElement($("tagFilter"), clean);
  }

  if (notify) {
    emit("tags:filter-changed", { tags: [...clean] });
  }
}

export function toggleFilterTag(tag) {
  const clean = normalizeTagToken(tag);
  if (!clean) return;
  const current = getFilterTags();
  const next = current.includes(clean)
    ? current.filter((t) => t !== clean)
    : [...current, clean];
  setFilterTags(next, { notify: true });
}

export function initTags() {
  if (tagsInitialized) return;
  tagsInitialized = true;

  const ingestMount = $("ingestTagPicker");
  const tagsInput = $("tagsInput");
  if (ingestMount && tagsInput) {
    ingestTagPicker = createTagPicker({
      container: ingestMount,
      inputEl: tagsInput,
      placeholder: "#soap, #hd...",
      allowCreate: true,
      mode: "edit",
      initialTags: normalizeTagsList(tagsInput.value || ""),
      options: state.tags,
    });
  }

  const filterMount = $("tagFilterPicker");
  const tagFilter = $("tagFilter");
  if (filterMount && tagFilter) {
    const initialFilter = state.selectedFilterTags?.length
      ? state.selectedFilterTags
      : (state.pendingFilters.tag && state.pendingFilters.tag !== "all"
        ? normalizeTagsList(state.pendingFilters.tag)
        : []);
    state.selectedFilterTags = initialFilter;

    filterTagPicker = createTagPicker({
      container: filterMount,
      selectEl: tagFilter,
      placeholder: "All tags",
      allowCreate: false,
      mode: "filter",
      initialTags: initialFilter,
      options: state.tags,
      onChange: (nextTags) => {
        state.selectedFilterTags = [...nextTags];
        state.pendingFilters.tag = null;
        emit("tags:filter-changed", { tags: [...nextTags] });
      },
    });
  }

  on("filters:popstate", ({ tags } = {}) => {
    setFilterTags(tags || [], { notify: true });
  });
}

export async function loadTags() {
  const data = await apiFetch("/api/tags");
  const list = $("tagList");
  const filter = $("tagFilter");

  let activeFilterTags = [];
  if (state.pendingFilters.tag !== null) {
    activeFilterTags =
      state.pendingFilters.tag === "all" ? [] : normalizeTagsList(state.pendingFilters.tag);
    state.pendingFilters.tag = null;
  } else if (state.selectedFilterTags?.length) {
    activeFilterTags = [...state.selectedFilterTags];
  } else if (filter && filter.value && filter.value !== "all") {
    activeFilterTags = normalizeTagsList(filter.value);
  }
  state.selectedFilterTags = activeFilterTags;

  const raw = Array.isArray(data.tags) ? data.tags : [];
  state.tags = raw
    .map((t) => {
      if (typeof t === "string") {
        return { name: t, count: null };
      }
      if (t && typeof t === "object") {
        return {
          name: String(t.name ?? ""),
          count: t.count != null ? Number(t.count) : null,
        };
      }
      return null;
    })
    .filter((t) => t && t.name);

  if (list) {
    list.innerHTML = state.tags
      .map((t) => `<option value="#${escapeHtml(t.name)}"></option>`)
      .join("");
  }

  if (filter) {
    filter.innerHTML =
      `<option value="all">All tags</option>` +
      state.tags
        .map((t) => {
          const label = t.count !== null ? `#${t.name} (${t.count})` : `#${t.name}`;
          return `<option value="${escapeHtml(t.name)}">${escapeHtml(label)}</option>`;
        })
        .join("");
  }

  ingestTagPicker?.setOptions(state.tags);
  if (filterTagPicker) {
    filterTagPicker.setOptions(state.tags);
    filterTagPicker.setTags(activeFilterTags, { silent: true });
  } else {
    syncSelectElement(filter, activeFilterTags);
  }
}
