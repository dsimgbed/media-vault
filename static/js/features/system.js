import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch } from "/static/js/api.js";
import { $ } from "/static/js/utils.js";
import { toast } from "/static/js/ui/dialogs.js";

function syncAuthBoxes() {
  const show = state.authRequired && !state.token;
  const authBox = $("authBox");
  if (authBox) authBox.style.display = show ? "flex" : "none";
}

export async function loadHealth() {
  const data = await fetch("/api/health").then((r) => r.json());
  state.authRequired = data.auth_required;
  state.storageMode = data.storage_mode || "telegram";
  state.defaultFolder = data.default_folder || "General";
  state.maxUploadMb = data.max_upload_mb || 20480;

  const folderInput = $("folderInput");
  if (folderInput && !folderInput.value) {
    folderInput.value = state.defaultFolder;
  }
  const maxMbLabel = $("maxMbLabel");
  if (maxMbLabel) {
    maxMbLabel.textContent = state.maxUploadMb;
  }

  syncAuthBoxes();

  const tgText = data.telegram_workers > 0
    ? `Telegram: ${data.telegram_workers} worker(s) online (${state.storageMode})`
    : "Telegram: disabled";
  const tgOk = data.telegram_workers > 0;
  for (const el of [$("tgStatus"), $("sheetTgStatus")]) {
    if (!el) continue;
    el.textContent = tgText;
    el.classList.toggle("ok", tgOk);
  }

  const ckText = data.ytdlp_cookies_configured ? "Cookies: loaded" : "Cookies: none";
  const ckOk = Boolean(data.ytdlp_cookies_configured);
  for (const el of [$("cookiesBtn"), $("sheetCookiesBtn")]) {
    if (!el) continue;
    el.textContent = ckText;
    el.classList.toggle("ok", ckOk);
  }

  const ytText = data.ytdlp_version
    ? `yt-dlp: ${data.ytdlp_version} (Update)`
    : "Update yt-dlp";
  for (const el of [$("updateYtdlpBtn"), $("sheetUpdateYtdlpBtn")]) {
    if (!el) continue;
    el.textContent = ytText;
  }

  const tgForwardLabel = $("tgForwardLabel");
  if (tgForwardLabel) {
    tgForwardLabel.style.display =
      state.storageMode === "both" && data.telegram_archive_enabled ? "flex" : "none";
  }
}

async function handleSaveToken(inputEl) {
  const nextToken = (inputEl?.value || "").trim();
  state.token = nextToken;
  localStorage.setItem("vault_token", state.token);
  if ($("tokenInput")) $("tokenInput").value = state.token;
  if ($("sheetTokenInput")) $("sheetTokenInput").value = state.token;
  if ($("authBox")) $("authBox").style.display = "none";
  const sheet = $("systemSheet");
  if (sheet?.open) sheet.close();
  emit("auth:unlocked");
}

async function handleUpdateYtdlp() {
  const btns = [$("updateYtdlpBtn"), $("sheetUpdateYtdlpBtn")].filter(Boolean);
  const prevTexts = btns.map((b) => b.textContent);
  btns.forEach((b) => {
    b.disabled = true;
    b.textContent = "Updating yt-dlp...";
  });
  try {
    const res = await apiFetch("/api/ytdlp/update", { method: "POST" });
    await loadHealth();
    toast(
      res.updated
        ? `Updated yt-dlp: ${res.before} → ${res.after}`
        : `yt-dlp is already up to date (${res.after}).`,
      { type: res.updated ? "success" : "info" }
    );
  } catch (err) {
    btns.forEach((b, i) => {
      b.textContent = prevTexts[i];
    });
    toast(`Failed updating yt-dlp: ${err.message}`, { type: "error" });
  } finally {
    btns.forEach((b) => {
      b.disabled = false;
    });
  }
}

export function initSystem() {
  if ($("sheetTokenInput") && state.token) {
    $("sheetTokenInput").value = state.token;
  }

  $("saveTokenBtn")?.addEventListener("click", () => handleSaveToken($("tokenInput")));
  $("sheetSaveTokenBtn")?.addEventListener("click", () => handleSaveToken($("sheetTokenInput")));

  $("refreshBtn")?.addEventListener("click", () => emit("system:refresh"));
  $("sheetRefreshBtn")?.addEventListener("click", () => {
    const sheet = $("systemSheet");
    if (sheet?.open) sheet.close();
    emit("system:refresh");
  });

  $("mobileSettingsBtn")?.addEventListener("click", () => {
    const sheet = $("systemSheet");
    if (sheet && !sheet.open) sheet.showModal();
  });
  $("closeSystemSheetBtn")?.addEventListener("click", () => $("systemSheet")?.close());
  $("systemSheet")?.addEventListener("click", (e) => {
    if (e.target === $("systemSheet")) $("systemSheet").close();
  });

  $("cookiesBtn")?.addEventListener("click", () => $("cookiesModal")?.showModal());
  $("sheetCookiesBtn")?.addEventListener("click", () => {
    const sheet = $("systemSheet");
    if (sheet?.open) sheet.close();
    $("cookiesModal")?.showModal();
  });

  $("closeCookiesBtn")?.addEventListener("click", () => $("cookiesModal")?.close());
  $("cookiesModal")?.addEventListener("click", (e) => {
    if (e.target === $("cookiesModal")) $("cookiesModal").close();
  });

  $("cookiesFileInput")?.addEventListener("change", async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    $("cookiesText").value = await file.text();
  });

  $("saveCookiesBtn")?.addEventListener("click", async () => {
    const cookies_text = $("cookiesText").value;
    try {
      await apiFetch("/api/ytdlp/cookies", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cookies_text }),
      });
      $("cookiesText").value = "";
      $("cookiesModal").close();
      await loadHealth();
      toast("Cookies saved.", { type: "success" });
    } catch (err) {
      toast(`Failed saving cookies: ${err.message}`, { type: "error" });
    }
  });

  $("clearCookiesBtn")?.addEventListener("click", async () => {
    try {
      await apiFetch("/api/ytdlp/cookies", { method: "DELETE" });
      $("cookiesText").value = "";
      $("cookiesModal").close();
      await loadHealth();
      toast("Cookies cleared.", { type: "info" });
    } catch (err) {
      toast(`Failed clearing cookies: ${err.message}`, { type: "error" });
    }
  });

  $("updateYtdlpBtn")?.addEventListener("click", handleUpdateYtdlp);
  $("sheetUpdateYtdlpBtn")?.addEventListener("click", handleUpdateYtdlp);

  on("auth:required", () => {
    syncAuthBoxes();
  });
}
