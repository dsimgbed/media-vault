import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch } from "/static/js/api.js";
import { $ } from "/static/js/utils.js";
import { toast } from "/static/js/ui/dialogs.js";
import { initMaintenance, openSystemSheet } from "/static/js/features/maintenance.js";

function syncAuthBoxes() {
  const locked = Boolean(state.authRequired && !state.token);
  const authBox = $("authBox");
  if (authBox) authBox.style.display = locked ? "flex" : "none";
  const banner = $("authBanner");
  if (banner) banner.hidden = !locked;
  const mobileBtn = $("mobileSettingsBtn");
  if (mobileBtn) mobileBtn.classList.toggle("is-locked", locked);
}

export async function loadHealth() {
  const data = await fetch("/api/health").then((r) => r.json());
  state.authRequired = data.auth_required;
  state.storageMode = data.storage_mode || "telegram";
  state.defaultFolder = data.default_folder || "General";
  state.maxUploadMb = data.max_upload_mb || 20480;
  state.uploadChunkSizeMb = Number(data.upload_chunk_size_mb || 8);
  state.failedUploadRetentionSec = Number(data.failed_upload_retention_sec ?? 3600);
  state.telegramArchiveEnabled = Boolean(data.telegram_archive_enabled);

  const folderInput = $("folderInput");
  if (folderInput && !folderInput.value) {
    folderInput.value = state.defaultFolder;
  }
  const maxMbLabel = $("maxMbLabel");
  if (maxMbLabel) {
    maxMbLabel.textContent = state.maxUploadMb;
  }

  syncAuthBoxes();

  const totalWorkers = Number(data.telegram_workers || 0);
  const onlineWorkers = Number(data.telegram_workers_online ?? totalWorkers);
  const cooldownWorkers = Number(data.telegram_workers_in_cooldown || 0);
  let tgText = "Telegram: disabled";
  if (totalWorkers > 0) {
    tgText = cooldownWorkers > 0
      ? `Telegram: ${onlineWorkers}/${totalWorkers} online (${cooldownWorkers} cooldown)`
      : `Telegram: ${onlineWorkers}/${totalWorkers} online (${state.storageMode})`;
  }
  const tgOk = totalWorkers > 0;
  const tgWarn = tgOk && (cooldownWorkers > 0 || onlineWorkers < totalWorkers);
  for (const el of [$("tgStatus"), $("sheetTgStatus")]) {
    if (!el) continue;
    el.textContent = tgText;
    el.classList.toggle("ok", tgOk);
    el.classList.toggle("warn", tgWarn);
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
  try {
    localStorage.setItem("vault_token", state.token);
  } catch (_) {}
  for (const id of ["tokenInput", "sheetTokenInput", "bannerTokenInput"]) {
    const el = $(id);
    if (el) el.value = state.token;
  }
  syncAuthBoxes();
  const sheet = $("systemSheet");
  if (sheet?.open && inputEl?.id !== "sheetTokenInput") sheet.close();
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
  for (const id of ["tokenInput", "sheetTokenInput", "bannerTokenInput"]) {
    const el = $(id);
    if (el && state.token) el.value = state.token;
    el?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        handleSaveToken(el);
      }
    });
  }

  $("saveTokenBtn")?.addEventListener("click", () => handleSaveToken($("tokenInput")));
  $("sheetSaveTokenBtn")?.addEventListener("click", () => handleSaveToken($("sheetTokenInput")));
  $("bannerSaveTokenBtn")?.addEventListener("click", () => handleSaveToken($("bannerTokenInput")));

  $("refreshBtn")?.addEventListener("click", () => emit("system:refresh"));
  $("sheetRefreshBtn")?.addEventListener("click", () => {
    const sheet = $("systemSheet");
    if (sheet?.open) sheet.close();
    emit("system:refresh");
  });

  $("mobileSettingsBtn")?.addEventListener("click", () => {
    openSystemSheet("settings");
  });
  $("tgStatus")?.addEventListener("click", () => {
    openSystemSheet("health");
  });
  $("captureToolsBtn")?.addEventListener("click", () => {
    openSystemSheet("capture");
  });
  $("sheetCaptureBtn")?.addEventListener("click", () => {
    openSystemSheet("capture");
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

  initMaintenance();
}
