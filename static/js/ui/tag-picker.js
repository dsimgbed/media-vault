import { state } from "/static/js/state.js";
import { escapeHtml } from "/static/js/utils.js";

export function normalizeTagToken(raw) {
  return String(raw ?? "")
    .trim()
    .replace(/^#+/, "")
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "")
    .slice(0, 48);
}

export function normalizeTagsList(raw) {
  const arr = Array.isArray(raw) ? raw : String(raw ?? "").split(/[\s,]+/);
  const out = [];
  const seen = new Set();
  for (const item of arr) {
    for (const part of String(item ?? "").split(/[\s,]+/)) {
      const clean = normalizeTagToken(part);
      if (clean && !seen.has(clean)) {
        seen.add(clean);
        out.push(clean);
      }
    }
  }
  return out;
}

function normalizeTagOptions(rawOptions) {
  const list = Array.isArray(rawOptions) ? rawOptions : [];
  const out = [];
  const seen = new Set();
  for (const entry of list) {
    if (typeof entry === "string") {
      const name = normalizeTagToken(entry);
      if (name && !seen.has(name)) {
        seen.add(name);
        out.push({ name, count: null });
      }
    } else if (entry && typeof entry === "object") {
      const name = normalizeTagToken(entry.name ?? "");
      if (name && !seen.has(name)) {
        seen.add(name);
        out.push({
          name,
          count: entry.count != null ? Number(entry.count) : null,
        });
      }
    }
  }
  return out;
}

export class TagPicker {
  constructor({
    container,
    inputEl = null,
    selectEl = null,
    placeholder = "",
    allowCreate = true,
    mode = "edit",
    initialTags = [],
    options = null,
    onChange = null,
  }) {
    if (!container) {
      throw new Error("TagPicker requires a container element");
    }
    this.container = container;
    this.inputEl = inputEl;
    this.selectEl = selectEl;
    this.placeholder = placeholder || (mode === "filter" ? "All tags" : "#soap, #hd...");
    this.allowCreate = Boolean(allowCreate);
    this.mode = mode === "filter" ? "filter" : "edit";
    this.onChange = typeof onChange === "function" ? onChange : null;

    this.options = normalizeTagOptions(options ?? state.tags);
    this.selected = [];
    this.isOpen = false;
    this.activeIndex = -1;
    this.visibleEntries = [];
    this._lastSyncedInput = "";
    this._syncingFromSelf = false;
    this._destroyed = false;

    this._buildDom();
    this._bindEvents();

    const seed = initialTags && initialTags.length
      ? initialTags
      : (this.inputEl?.value ? normalizeTagsList(this.inputEl.value) : []);
    this.setTags(seed, { silent: true });
  }

  _buildDom() {
    if (this.inputEl) {
      this.inputEl.classList.add("tag-picker-native-hidden");
      this.inputEl.setAttribute("aria-hidden", "true");
      this.inputEl.tabIndex = -1;
    }
    if (this.selectEl) {
      this.selectEl.classList.add("tag-picker-native-hidden");
      this.selectEl.setAttribute("aria-hidden", "true");
      this.selectEl.tabIndex = -1;
    }

    this.root = document.createElement("div");
    this.root.className = `tag-picker tag-picker-${this.mode}`;

    this.control = document.createElement("div");
    this.control.className = "tag-picker-control";

    this.chipsEl = document.createElement("div");
    this.chipsEl.className = "tag-picker-chips";

    this.textInput = document.createElement("input");
    this.textInput.type = "text";
    this.textInput.className = "tag-picker-input";
    this.textInput.autocomplete = "off";
    this.textInput.spellcheck = false;
    this.textInput.setAttribute("aria-label", this.mode === "filter" ? "Filter by tags" : "Tags");
    this.textInput.setAttribute("aria-autocomplete", "list");
    this.textInput.setAttribute("aria-expanded", "false");

    this.clearBtn = document.createElement("button");
    this.clearBtn.type = "button";
    this.clearBtn.className = "tag-picker-clear";
    this.clearBtn.textContent = "Clear";
    this.clearBtn.setAttribute("aria-label", "Clear selected tags");
    this.clearBtn.hidden = true;

    this.dropdown = document.createElement("div");
    this.dropdown.className = "tag-picker-dropdown";
    this.dropdown.setAttribute("role", "listbox");
    this.dropdown.hidden = true;

    this.chipsEl.appendChild(this.textInput);
    this.control.append(this.chipsEl);
    if (this.mode === "filter") {
      this.control.appendChild(this.clearBtn);
    }
    this.root.append(this.control, this.dropdown);

    // Preserve hidden native input/select inside container if present
    this.container.appendChild(this.root);
  }

  _bindEvents() {
    this._onControlClick = (e) => {
      if (e.target.closest(".tag-picker-chip-remove") || e.target.closest(".tag-picker-clear")) {
        return;
      }
      this.textInput.focus();
      this.open();
    };

    this._onFocus = () => {
      this.open();
    };

    this._onInput = () => {
      const val = this.textInput.value;
      if (val.includes(",")) {
        const parts = val.split(",");
        const committed = parts.slice(0, -1);
        const remainder = parts[parts.length - 1] ?? "";
        let added = false;
        for (const p of committed) {
          const clean = normalizeTagToken(p);
          if (clean && !this.selected.includes(clean)) {
            this.selected.push(clean);
            added = true;
          }
        }
        this.textInput.value = remainder.trimStart();
        if (added) {
          this._renderChips();
          this._syncBoundControls();
          this._emitChange();
        }
      }
      if (this.mode === "edit") {
        this._syncBoundControls();
      }
      this.activeIndex = -1;
      this.open();
      this._renderDropdown();
    };

    this._onKeydown = (e) => {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        if (!this.isOpen) {
          this.open();
        }
        if (this.visibleEntries.length > 0) {
          this.activeIndex = (this.activeIndex + 1) % this.visibleEntries.length;
          this._updateHighlight();
        }
        return;
      }

      if (e.key === "ArrowUp") {
        e.preventDefault();
        if (!this.isOpen) {
          this.open();
        }
        if (this.visibleEntries.length > 0) {
          this.activeIndex =
            this.activeIndex <= 0 ? this.visibleEntries.length - 1 : this.activeIndex - 1;
          this._updateHighlight();
        }
        return;
      }

      if (e.key === "Escape") {
        if (this.isOpen) {
          e.preventDefault();
          e.stopPropagation();
          this.close();
        }
        return;
      }

      if (e.key === "Backspace" && !this.textInput.value && this.selected.length > 0) {
        e.preventDefault();
        const last = this.selected[this.selected.length - 1];
        this.removeTag(last);
        return;
      }

      if (e.key === "," || e.key === "Enter") {
        const hasText = Boolean(normalizeTagToken(this.textInput.value));
        const highlighted =
          this.isOpen && this.activeIndex >= 0 ? this.visibleEntries[this.activeIndex] : null;

        if (highlighted) {
          e.preventDefault();
          if (highlighted.kind === "create") {
            this.addTag(highlighted.name);
          } else {
            this.toggleTag(highlighted.name);
          }
          this.textInput.value = "";
          this.activeIndex = -1;
          this._renderDropdown();
          return;
        }

        if (hasText) {
          e.preventDefault();
          const tokens = normalizeTagsList(this.textInput.value);
          let changed = false;
          for (const t of tokens) {
            if (!this.allowCreate && !this.options.some((o) => o.name === t)) {
              continue;
            }
            if (!this.selected.includes(t)) {
              this.selected.push(t);
              changed = true;
            }
          }
          if (!changed && !this.allowCreate && this.visibleEntries.length > 0) {
            const first = this.visibleEntries[0];
            if (first && first.kind === "option") {
              this.toggleTag(first.name);
            }
            this.textInput.value = "";
            this.activeIndex = -1;
            this._renderDropdown();
            return;
          }
          this.textInput.value = "";
          this.activeIndex = -1;
          if (changed) {
            this._renderChips();
            this._syncBoundControls();
            this._emitChange();
          } else {
            this._syncBoundControls();
          }
          this._renderDropdown();
          return;
        }

        if (e.key === ",") {
          e.preventDefault();
        }
      }
    };

    this._onDropdownMouseDown = (e) => {
      // Keep focus in textInput when clicking dropdown options
      e.preventDefault();
    };

    this._onDropdownClick = (e) => {
      const optBtn = e.target.closest(".tag-picker-option");
      if (!optBtn) return;
      const createTag = optBtn.dataset.create;
      const tag = optBtn.dataset.tag;
      if (createTag) {
        this.addTag(createTag);
        this.textInput.value = "";
        this.activeIndex = -1;
        this._renderDropdown();
      } else if (tag) {
        this.toggleTag(tag);
        this.textInput.value = "";
        this.activeIndex = -1;
        this._renderDropdown();
      }
      this.textInput.focus();
    };

    this._onClearClick = (e) => {
      e.stopPropagation();
      this.clear();
      this.textInput.focus();
    };

    this._onDocPointerDown = (e) => {
      if (!this.root.contains(e.target)) {
        if (this.isOpen) {
          this.close();
        }
      }
    };

    this._onNativeInputSync = () => {
      if (this._syncingFromSelf || !this.inputEl) return;
      const next = normalizeTagsList(this.inputEl.value);
      this.textInput.value = "";
      this.setTags(next, { silent: false });
    };

    this.control.addEventListener("click", this._onControlClick);
    this.textInput.addEventListener("focus", this._onFocus);
    this.textInput.addEventListener("input", this._onInput);
    this.textInput.addEventListener("keydown", this._onKeydown);
    this.dropdown.addEventListener("mousedown", this._onDropdownMouseDown);
    this.dropdown.addEventListener("click", this._onDropdownClick);
    this.clearBtn.addEventListener("click", this._onClearClick);
    document.addEventListener("pointerdown", this._onDocPointerDown);
    this.inputEl?.addEventListener("change", this._onNativeInputSync);
  }

  _renderChips() {
    // Remove existing chip nodes while preserving textInput at the end
    for (const child of Array.from(this.chipsEl.querySelectorAll(".tag-picker-chip"))) {
      child.remove();
    }

    for (const tag of this.selected) {
      const chip = document.createElement("span");
      chip.className = "tag-picker-chip";
      chip.dataset.tag = tag;

      const label = document.createElement("span");
      label.className = "tag-picker-chip-label";
      label.textContent = `#${tag}`;

      const removeBtn = document.createElement("button");
      removeBtn.type = "button";
      removeBtn.className = "tag-picker-chip-remove";
      removeBtn.setAttribute("aria-label", `Remove #${tag}`);
      removeBtn.textContent = "×";
      removeBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        this.removeTag(tag);
        this.textInput.focus();
      });

      chip.append(label, removeBtn);
      this.chipsEl.insertBefore(chip, this.textInput);
    }

    if (this.mode === "filter") {
      this.textInput.placeholder = this.selected.length > 0 ? "Add tag..." : this.placeholder;
      this.clearBtn.hidden = this.selected.length === 0;
    } else {
      this.textInput.placeholder = this.selected.length > 0 ? "Add tag..." : this.placeholder;
    }
  }

  _renderDropdown() {
    const rawQuery = this.textInput.value;
    const normQuery = normalizeTagToken(rawQuery);
    const knownOptions = this.options.length ? this.options : normalizeTagOptions(state.tags);

    const matches = knownOptions.filter((opt) => {
      if (!normQuery) return true;
      return opt.name.includes(normQuery);
    });

    const entries = matches.slice(0, 60).map((opt) => ({
      kind: "option",
      name: opt.name,
      count: opt.count,
      selected: this.selected.includes(opt.name),
    }));

    const exactKnown = normQuery && knownOptions.some((o) => o.name === normQuery);
    const alreadySelected = normQuery && this.selected.includes(normQuery);
    if (this.allowCreate && normQuery && !exactKnown && !alreadySelected) {
      entries.push({
        kind: "create",
        name: normQuery,
        count: null,
        selected: false,
      });
    }

    this.visibleEntries = entries;
    if (this.activeIndex >= entries.length) {
      this.activeIndex = entries.length - 1;
    }

    if (!entries.length) {
      this.dropdown.innerHTML = `<div class="tag-picker-empty">${
        normQuery ? "No matching tags" : "No tags yet"
      }</div>`;
      return;
    }

    this.dropdown.innerHTML = entries
      .map((entry, idx) => {
        const activeCls = idx === this.activeIndex ? " is-highlighted" : "";
        if (entry.kind === "create") {
          return `<button type="button" class="tag-picker-option tag-picker-create${activeCls}" role="option" data-create="${escapeHtml(entry.name)}">
            <span class="tag-picker-option-name">Create <strong>#${escapeHtml(entry.name)}</strong></span>
          </button>`;
        }
        const selectedCls = entry.selected ? " is-selected" : "";
        const countBadge =
          entry.count != null
            ? `<span class="tag-picker-count">${escapeHtml(String(entry.count))}</span>`
            : "";
        const checkMark = entry.selected ? `<span aria-hidden="true">✓</span>` : "";
        return `<button type="button" class="tag-picker-option${selectedCls}${activeCls}" role="option" aria-selected="${entry.selected ? "true" : "false"}" data-tag="${escapeHtml(entry.name)}">
          <span class="tag-picker-option-name">#${escapeHtml(entry.name)}</span>
          <span class="tag-picker-option-meta">${checkMark}${countBadge}</span>
        </button>`;
      })
      .join("");
  }

  _updateHighlight() {
    const buttons = this.dropdown.querySelectorAll(".tag-picker-option");
    buttons.forEach((btn, idx) => {
      const active = idx === this.activeIndex;
      btn.classList.toggle("is-highlighted", active);
      if (active) {
        btn.scrollIntoView({ block: "nearest" });
      }
    });
  }

  _syncBoundControls() {
    this._syncingFromSelf = true;
    try {
      if (this.inputEl) {
        const allTags = this.getTags({ includePending: true });
        const formatted = allTags.map((t) => `#${t}`).join(" ");
        this._lastSyncedInput = formatted;
        this.inputEl.value = formatted;
      }

      if (this.selectEl) {
        // Remove previous synthetic multi-tag option if present
        const prevSynth = this.selectEl.querySelector('option[data-tag-picker-synth="true"]');
        if (prevSynth) prevSynth.remove();

        if (this.selected.length === 0) {
          this.selectEl.value = "all";
        } else if (this.selected.length === 1) {
          const target = this.selected[0];
          let opt = Array.from(this.selectEl.options).find((o) => o.value === target);
          if (!opt) {
            opt = document.createElement("option");
            opt.value = target;
            opt.textContent = `#${target}`;
            opt.dataset.tagPickerSynth = "true";
            this.selectEl.appendChild(opt);
          }
          this.selectEl.value = target;
        } else {
          const joined = this.selected.join(",");
          const opt = document.createElement("option");
          opt.value = joined;
          opt.textContent = this.selected.map((t) => `#${t}`).join(", ");
          opt.dataset.tagPickerSynth = "true";
          this.selectEl.appendChild(opt);
          this.selectEl.value = joined;
        }
      }
    } finally {
      this._syncingFromSelf = false;
    }
  }

  _emitChange() {
    if (this.onChange) {
      this.onChange([...this.selected]);
    }
  }

  getSyncedInputValue() {
    return this._lastSyncedInput;
  }

  getTags({ includePending = false } = {}) {
    if (!includePending) {
      return [...this.selected];
    }
    const pending = normalizeTagsList(this.textInput?.value || "");
    if (!pending.length) return [...this.selected];
    const out = [...this.selected];
    const seen = new Set(out);
    for (const t of pending) {
      if (!seen.has(t)) {
        seen.add(t);
        out.push(t);
      }
    }
    return out;
  }

  setTags(tags, { silent = false } = {}) {
    const clean = normalizeTagsList(tags);
    this.selected = clean;
    this._renderChips();
    this._syncBoundControls();
    if (this.isOpen) {
      this._renderDropdown();
    }
    if (!silent) {
      this._emitChange();
    }
  }

  addTag(rawTag) {
    const clean = normalizeTagToken(rawTag);
    if (!clean) return false;
    if (this.selected.includes(clean)) return false;
    this.selected = [...this.selected, clean];
    this._renderChips();
    this._syncBoundControls();
    if (this.isOpen) this._renderDropdown();
    this._emitChange();
    return true;
  }

  removeTag(rawTag) {
    const clean = normalizeTagToken(rawTag);
    if (!clean || !this.selected.includes(clean)) return false;
    this.selected = this.selected.filter((t) => t !== clean);
    this._renderChips();
    this._syncBoundControls();
    if (this.isOpen) this._renderDropdown();
    this._emitChange();
    return true;
  }

  toggleTag(rawTag) {
    const clean = normalizeTagToken(rawTag);
    if (!clean) return false;
    if (this.selected.includes(clean)) {
      return this.removeTag(clean);
    }
    return this.addTag(clean);
  }

  clear() {
    const hadTags = this.selected.length > 0 || Boolean(this.textInput.value);
    this.selected = [];
    this.textInput.value = "";
    this._renderChips();
    this._syncBoundControls();
    if (this.isOpen) this._renderDropdown();
    if (hadTags) {
      this._emitChange();
    }
  }

  setOptions(tagOptions) {
    this.options = normalizeTagOptions(tagOptions);
    this._syncBoundControls();
    if (this.isOpen) {
      this._renderDropdown();
    }
  }

  open() {
    if (this._destroyed || this.isOpen) return;
    this.isOpen = true;
    this.dropdown.hidden = false;
    this.textInput.setAttribute("aria-expanded", "true");
    this._renderDropdown();
  }

  close() {
    if (!this.isOpen) return;
    this.isOpen = false;
    this.activeIndex = -1;
    this.dropdown.hidden = true;
    this.textInput.setAttribute("aria-expanded", "false");
  }

  focus() {
    this.textInput?.focus();
  }

  destroy() {
    if (this._destroyed) return;
    this._destroyed = true;
    this.control.removeEventListener("click", this._onControlClick);
    this.textInput.removeEventListener("focus", this._onFocus);
    this.textInput.removeEventListener("input", this._onInput);
    this.textInput.removeEventListener("keydown", this._onKeydown);
    this.dropdown.removeEventListener("mousedown", this._onDropdownMouseDown);
    this.dropdown.removeEventListener("click", this._onDropdownClick);
    this.clearBtn.removeEventListener("click", this._onClearClick);
    document.removeEventListener("pointerdown", this._onDocPointerDown);
    this.inputEl?.removeEventListener("change", this._onNativeInputSync);
    this.root.remove();
  }
}

export function createTagPicker(opts) {
  return new TagPicker(opts);
}

function buildQuickPickSection(picker) {
  const popular = (state.tags || []).slice(0, 12);
  if (!popular.length) return null;

  const wrap = document.createElement("div");
  wrap.className = "tag-quickpick";

  const label = document.createElement("div");
  label.className = "tag-quickpick-label";
  label.textContent = "Popular tags (tap to toggle)";

  const list = document.createElement("div");
  list.className = "tag-quickpick-list";

  const pillMap = new Map();
  for (const item of popular) {
    const name = typeof item === "string" ? normalizeTagToken(item) : normalizeTagToken(item?.name);
    if (!name) continue;
    const count = typeof item === "object" && item?.count != null ? item.count : null;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "tag-quickpick-pill";
    btn.dataset.tag = name;
    btn.innerHTML =
      count != null
        ? `<span>#${escapeHtml(name)}</span><span class="tag-picker-count">${escapeHtml(String(count))}</span>`
        : `<span>#${escapeHtml(name)}</span>`;
    btn.addEventListener("click", () => {
      picker.toggleTag(name);
    });
    pillMap.set(name, btn);
    list.appendChild(btn);
  }

  const syncPills = (selectedTags) => {
    const activeSet = new Set(selectedTags || picker.getTags());
    for (const [name, btn] of pillMap.entries()) {
      const isActive = activeSet.has(name);
      btn.classList.toggle("active", isActive);
      btn.setAttribute("aria-pressed", isActive ? "true" : "false");
    }
  };

  syncPills(picker.getTags());
  wrap.append(label, list);
  return { element: wrap, sync: syncPills };
}

export function openTagEditorDialog({ title = "Edit Tags", initialTags = [] } = {}) {
  const prevFocus = document.activeElement;
  const dialog = document.createElement("dialog");
  dialog.className = "modal-sm tag-editor-dialog";

  const surface = document.createElement("div");
  surface.className = "dialog-surface";

  const head = document.createElement("div");
  head.className = "modal-head";
  const strong = document.createElement("strong");
  strong.textContent = title;
  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "secondary btn-sm";
  closeBtn.textContent = "Close";
  head.append(strong, closeBtn);

  const body = document.createElement("div");
  body.className = "modal-body";

  const field = document.createElement("div");
  field.className = "modal-field";
  const fieldLabel = document.createElement("span");
  fieldLabel.textContent = "Tags";
  const pickerHost = document.createElement("div");
  pickerHost.className = "tag-picker-mount tag-picker-mount-modal";
  field.append(fieldLabel, pickerHost);

  let quickPick = null;
  const picker = createTagPicker({
    container: pickerHost,
    placeholder: "Search or create #tag...",
    allowCreate: true,
    mode: "edit",
    initialTags,
    options: state.tags,
    onChange: (tags) => {
      quickPick?.sync(tags);
    },
  });

  quickPick = buildQuickPickSection(picker);

  const actions = document.createElement("div");
  actions.className = "toolbar-group modal-actions-end";

  const cancelBtn = document.createElement("button");
  cancelBtn.type = "button";
  cancelBtn.className = "secondary";
  cancelBtn.textContent = "Cancel";

  const saveBtn = document.createElement("button");
  saveBtn.type = "button";
  saveBtn.textContent = "Save Tags";

  actions.append(cancelBtn, saveBtn);
  body.appendChild(field);
  if (quickPick) body.appendChild(quickPick.element);
  body.appendChild(actions);

  surface.append(head, body);
  dialog.appendChild(surface);
  document.body.appendChild(dialog);

  return new Promise((resolve) => {
    let settled = false;
    const finish = (val) => {
      if (settled) return;
      settled = true;
      picker.destroy();
      if (dialog.open) dialog.close();
      dialog.remove();
      if (prevFocus && typeof prevFocus.focus === "function" && prevFocus.isConnected) {
        prevFocus.focus();
      }
      resolve(val);
    };

    closeBtn.addEventListener("click", () => finish(null));
    cancelBtn.addEventListener("click", () => finish(null));
    saveBtn.addEventListener("click", () => {
      finish(picker.getTags({ includePending: true }));
    });
    dialog.addEventListener("cancel", (e) => {
      e.preventDefault();
      if (picker.isOpen) {
        picker.close();
        return;
      }
      finish(null);
    });
    dialog.addEventListener("click", (e) => {
      if (e.target === dialog) finish(null);
    });

    dialog.showModal();
    picker.focus();
  });
}

export function openBulkTagDialog({ count = 0 } = {}) {
  const prevFocus = document.activeElement;
  const dialog = document.createElement("dialog");
  dialog.className = "modal-sm tag-editor-dialog";

  const surface = document.createElement("div");
  surface.className = "dialog-surface";

  const head = document.createElement("div");
  head.className = "modal-head";
  const strong = document.createElement("strong");
  strong.textContent = `Edit Tags (${count} selected)`;
  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "secondary btn-sm";
  closeBtn.textContent = "Close";
  head.append(strong, closeBtn);

  const body = document.createElement("div");
  body.className = "modal-body";

  const modeField = document.createElement("div");
  modeField.className = "modal-field";
  const modeLabel = document.createElement("span");
  modeLabel.textContent = "Action";

  const modeGroup = document.createElement("div");
  modeGroup.className = "bulk-tag-modes";
  modeGroup.setAttribute("role", "radiogroup");
  modeGroup.setAttribute("aria-label", "Tag update mode");

  let selectedMode = "add";
  const modes = [
    { value: "add", label: "Add tags" },
    { value: "remove", label: "Remove tags" },
    { value: "replace", label: "Replace all" },
  ];

  const modeLabels = [];
  for (const m of modes) {
    const lbl = document.createElement("label");
    lbl.className = `bulk-tag-mode${m.value === selectedMode ? " is-active" : ""}`;
    const radio = document.createElement("input");
    radio.type = "radio";
    radio.name = "bulkTagMode";
    radio.value = m.value;
    radio.checked = m.value === selectedMode;
    radio.addEventListener("change", () => {
      if (!radio.checked) return;
      selectedMode = m.value;
      for (const el of modeLabels) {
        el.classList.toggle("is-active", el.querySelector("input")?.checked);
      }
    });
    const txt = document.createElement("span");
    txt.textContent = m.label;
    lbl.append(radio, txt);
    modeLabels.push(lbl);
    modeGroup.appendChild(lbl);
  }
  modeField.append(modeLabel, modeGroup);

  const field = document.createElement("div");
  field.className = "modal-field";
  const fieldLabel = document.createElement("span");
  fieldLabel.textContent = "Tags";
  const pickerHost = document.createElement("div");
  pickerHost.className = "tag-picker-mount tag-picker-mount-modal";
  field.append(fieldLabel, pickerHost);

  let quickPick = null;
  const picker = createTagPicker({
    container: pickerHost,
    placeholder: "Search or create #tag...",
    allowCreate: true,
    mode: "edit",
    initialTags: [],
    options: state.tags,
    onChange: (tags) => {
      quickPick?.sync(tags);
    },
  });

  quickPick = buildQuickPickSection(picker);

  const actions = document.createElement("div");
  actions.className = "toolbar-group modal-actions-end";

  const cancelBtn = document.createElement("button");
  cancelBtn.type = "button";
  cancelBtn.className = "secondary";
  cancelBtn.textContent = "Cancel";

  const applyBtn = document.createElement("button");
  applyBtn.type = "button";
  applyBtn.textContent = "Apply";

  actions.append(cancelBtn, applyBtn);
  body.append(modeField, field);
  if (quickPick) body.appendChild(quickPick.element);
  body.appendChild(actions);

  surface.append(head, body);
  dialog.appendChild(surface);
  document.body.appendChild(dialog);

  return new Promise((resolve) => {
    let settled = false;
    const finish = (val) => {
      if (settled) return;
      settled = true;
      picker.destroy();
      if (dialog.open) dialog.close();
      dialog.remove();
      if (prevFocus && typeof prevFocus.focus === "function" && prevFocus.isConnected) {
        prevFocus.focus();
      }
      resolve(val);
    };

    closeBtn.addEventListener("click", () => finish(null));
    cancelBtn.addEventListener("click", () => finish(null));
    applyBtn.addEventListener("click", () => {
      finish({
        mode: selectedMode,
        tags: picker.getTags({ includePending: true }),
      });
    });
    dialog.addEventListener("cancel", (e) => {
      e.preventDefault();
      if (picker.isOpen) {
        picker.close();
        return;
      }
      finish(null);
    });
    dialog.addEventListener("click", (e) => {
      if (e.target === dialog) finish(null);
    });

    dialog.showModal();
    picker.focus();
  });
}
