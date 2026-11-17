import { escapeHtml } from "/static/js/utils.js";

function getToastRegion() {
  let region = document.getElementById("toastRegion");
  if (!region) {
    region = document.createElement("div");
    region.id = "toastRegion";
    region.className = "toast-region";
    region.setAttribute("role", "status");
    region.setAttribute("aria-live", "polite");
    document.body.appendChild(region);
  }
  return region;
}

export function toast(message, { type = "info", timeout = 4000 } = {}) {
  const region = getToastRegion();
  const el = document.createElement("div");
  const safeType = ["info", "success", "error"].includes(type) ? type : "info";
  el.className = `toast toast-${safeType}`;

  const msgSpan = document.createElement("span");
  msgSpan.className = "toast-msg";
  msgSpan.textContent = String(message ?? "");

  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "toast-close";
  closeBtn.setAttribute("aria-label", "Dismiss notification");
  closeBtn.textContent = "×";

  el.append(msgSpan, closeBtn);
  region.appendChild(el);

  let timer = null;
  const dismiss = () => {
    if (timer) clearTimeout(timer);
    el.remove();
  };

  closeBtn.addEventListener("click", dismiss);
  if (timeout > 0) {
    timer = setTimeout(dismiss, timeout);
  }
  return dismiss;
}

function openModalDialog({ title, bodyEl, primaryControl, cancelValue }) {
  const prevFocus = document.activeElement;
  const dialog = document.createElement("dialog");
  dialog.className = "modal-sm";

  const surface = document.createElement("div");
  surface.className = "dialog-surface";

  const head = document.createElement("div");
  head.className = "modal-head";
  const strong = document.createElement("strong");
  strong.textContent = title || "";
  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "secondary btn-sm";
  closeBtn.textContent = "Close";
  head.append(strong, closeBtn);

  surface.append(head, bodyEl);
  dialog.appendChild(surface);
  document.body.appendChild(dialog);

  return new Promise((resolve) => {
    let settled = false;
    const finish = (val) => {
      if (settled) return;
      settled = true;
      if (dialog.open) dialog.close();
      dialog.remove();
      if (prevFocus && typeof prevFocus.focus === "function" && prevFocus.isConnected) {
        prevFocus.focus();
      }
      resolve(val);
    };

    closeBtn.addEventListener("click", () => finish(cancelValue));
    dialog.addEventListener("cancel", (e) => {
      e.preventDefault();
      finish(cancelValue);
    });
    dialog.addEventListener("click", (e) => {
      if (e.target === dialog) finish(cancelValue);
    });

    dialog.showModal();
    if (primaryControl && typeof primaryControl.focus === "function") {
      primaryControl.focus();
    }

    dialog._finishDialog = finish;
  });
}

export function confirmDialog({
  title = "Confirm",
  message = "",
  confirmLabel = "Confirm",
  cancelLabel = "Cancel",
  danger = false,
} = {}) {
  const body = document.createElement("div");
  body.className = "modal-body";

  const msg = document.createElement("p");
  msg.className = "modal-message";
  msg.textContent = message;

  const actions = document.createElement("div");
  actions.className = "toolbar-group modal-actions-end";

  const cancelBtn = document.createElement("button");
  cancelBtn.type = "button";
  cancelBtn.className = "secondary";
  cancelBtn.textContent = cancelLabel;

  const confirmBtn = document.createElement("button");
  confirmBtn.type = "button";
  confirmBtn.className = danger ? "danger" : "";
  confirmBtn.autofocus = true;
  confirmBtn.textContent = confirmLabel;

  actions.append(cancelBtn, confirmBtn);
  body.append(msg, actions);

  const prevFocus = document.activeElement;
  const dialog = document.createElement("dialog");
  dialog.className = "modal-sm";
  dialog.innerHTML = `<div class="dialog-surface"><div class="modal-head"><strong>${escapeHtml(title)}</strong><button type="button" class="secondary btn-sm dialog-x-btn">Close</button></div></div>`;
  const surface = dialog.querySelector(".dialog-surface");
  surface.appendChild(body);
  document.body.appendChild(dialog);

  return new Promise((resolve) => {
    let settled = false;
    const finish = (val) => {
      if (settled) return;
      settled = true;
      if (dialog.open) dialog.close();
      dialog.remove();
      if (prevFocus && typeof prevFocus.focus === "function" && prevFocus.isConnected) {
        prevFocus.focus();
      }
      resolve(val);
    };

    dialog.querySelector(".dialog-x-btn").addEventListener("click", () => finish(false));
    cancelBtn.addEventListener("click", () => finish(false));
    confirmBtn.addEventListener("click", () => finish(true));
    dialog.addEventListener("cancel", (e) => {
      e.preventDefault();
      finish(false);
    });
    dialog.addEventListener("click", (e) => {
      if (e.target === dialog) finish(false);
    });

    dialog.showModal();
    confirmBtn.focus();
  });
}

export function promptDialog({
  title = "Input",
  label = "",
  value = "",
  placeholder = "",
  datalistId = "",
  confirmLabel = "OK",
} = {}) {
  const form = document.createElement("form");
  form.className = "modal-body";
  form.method = "dialog";

  const field = document.createElement("label");
  field.className = "modal-field";
  if (label) {
    const lbl = document.createElement("span");
    lbl.textContent = label;
    field.appendChild(lbl);
  }

  const input = document.createElement("input");
  input.type = "text";
  input.value = value ?? "";
  input.placeholder = placeholder ?? "";
  input.autofocus = true;
  if (datalistId) {
    input.setAttribute("list", datalistId);
  }
  field.appendChild(input);

  const actions = document.createElement("div");
  actions.className = "toolbar-group modal-actions-end";

  const cancelBtn = document.createElement("button");
  cancelBtn.type = "button";
  cancelBtn.className = "secondary";
  cancelBtn.textContent = "Cancel";

  const okBtn = document.createElement("button");
  okBtn.type = "submit";
  okBtn.textContent = confirmLabel;

  actions.append(cancelBtn, okBtn);
  form.append(field, actions);

  const prevFocus = document.activeElement;
  const dialog = document.createElement("dialog");
  dialog.className = "modal-sm";
  dialog.innerHTML = `<div class="dialog-surface"><div class="modal-head"><strong>${escapeHtml(title)}</strong><button type="button" class="secondary btn-sm dialog-x-btn">Close</button></div></div>`;
  dialog.querySelector(".dialog-surface").appendChild(form);
  document.body.appendChild(dialog);

  return new Promise((resolve) => {
    let settled = false;
    const finish = (val) => {
      if (settled) return;
      settled = true;
      if (dialog.open) dialog.close();
      dialog.remove();
      if (prevFocus && typeof prevFocus.focus === "function" && prevFocus.isConnected) {
        prevFocus.focus();
      }
      resolve(val);
    };

    dialog.querySelector(".dialog-x-btn").addEventListener("click", () => finish(null));
    cancelBtn.addEventListener("click", () => finish(null));
    form.addEventListener("submit", (e) => {
      e.preventDefault();
      finish(input.value);
    });
    dialog.addEventListener("cancel", (e) => {
      e.preventDefault();
      finish(null);
    });
    dialog.addEventListener("click", (e) => {
      if (e.target === dialog) finish(null);
    });

    dialog.showModal();
    input.focus();
    input.select();
  });
}

export function alertDialog({ title = "Notice", message = "" } = {}) {
  const body = document.createElement("div");
  body.className = "modal-body";

  const msg = document.createElement("p");
  msg.className = "modal-message";
  msg.textContent = message;

  const actions = document.createElement("div");
  actions.className = "toolbar-group modal-actions-end";

  const okBtn = document.createElement("button");
  okBtn.type = "button";
  okBtn.autofocus = true;
  okBtn.textContent = "OK";

  actions.appendChild(okBtn);
  body.append(msg, actions);

  const promise = openModalDialog({
    title,
    bodyEl: body,
    primaryControl: okBtn,
    cancelValue: undefined,
  });

  okBtn.addEventListener("click", () => {
    const dialog = okBtn.closest("dialog");
    if (dialog && dialog._finishDialog) dialog._finishDialog(undefined);
  });

  return promise;
}
