import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch, withToken } from "/static/js/api.js";
import {
  $,
  escapeHtml,
  fmtDuration,
  fmtResolution,
  fmtSize,
  isZipItem,
} from "/static/js/utils.js";
import { confirmDialog, toast } from "/static/js/ui/dialogs.js";
import { openTagEditorDialog } from "/static/js/ui/tag-picker.js";
import { renderZipExplorer } from "/static/js/features/zip-explorer.js";

let viewerPushedHistory = false;
let isClosingViewer = false;

function clearMediaSession() {
  if (!("mediaSession" in navigator)) return;
  try {
    navigator.mediaSession.metadata = null;
    navigator.mediaSession.playbackState = "none";
    for (const action of ["play", "pause", "seekbackward", "seekforward", "previoustrack", "nexttrack"]) {
      try {
        navigator.mediaSession.setActionHandler(action, null);
      } catch (_) {}
    }
  } catch (_) {}
}

function updateMediaSessionPosition(mediaEl) {
  if (!("mediaSession" in navigator) || !mediaEl) return;
  try {
    if (
      typeof navigator.mediaSession.setPositionState === "function" &&
      Number.isFinite(mediaEl.duration) &&
      mediaEl.duration > 0
    ) {
      navigator.mediaSession.setPositionState({
        duration: mediaEl.duration,
        playbackRate: mediaEl.playbackRate || 1,
        position: Math.min(mediaEl.duration, Math.max(0, mediaEl.currentTime || 0)),
      });
    }
  } catch (_) {}
}

function bindMediaSession(item, mediaEl) {
  if (!("mediaSession" in navigator) || !mediaEl) return;
  try {
    if (typeof MediaMetadata !== "undefined") {
      navigator.mediaSession.metadata = new MediaMetadata({
        title: item.title || item.filename || "Media Vault",
        artist: `${item.drive_name} • ${item.folder || state.defaultFolder}`,
        album: "Media Vault",
        artwork: item.thumb_filename
          ? [{ src: withToken(`/api/media/${item.id}/thumb`), sizes: "512x512", type: "image/webp" }]
          : [],
      });
    }
    navigator.mediaSession.playbackState = mediaEl.paused ? "paused" : "playing";
  } catch (_) {}

  mediaEl.addEventListener("play", () => {
    try {
      navigator.mediaSession.playbackState = "playing";
      updateMediaSessionPosition(mediaEl);
    } catch (_) {}
  });
  mediaEl.addEventListener("pause", () => {
    try {
      navigator.mediaSession.playbackState = "paused";
      updateMediaSessionPosition(mediaEl);
    } catch (_) {}
  });
  mediaEl.addEventListener("timeupdate", () => updateMediaSessionPosition(mediaEl));
  mediaEl.addEventListener("loadedmetadata", () => updateMediaSessionPosition(mediaEl));
  mediaEl.addEventListener("durationchange", () => updateMediaSessionPosition(mediaEl));

  const setHandler = (action, fn) => {
    try {
      navigator.mediaSession.setActionHandler(action, fn);
    } catch (_) {}
  };

  setHandler("play", async () => {
    try {
      await mediaEl.play();
    } catch (_) {}
  });
  setHandler("pause", () => {
    try {
      mediaEl.pause();
    } catch (_) {}
  });
  setHandler("seekbackward", (details) => {
    const offset = details?.seekOffset || 10;
    mediaEl.currentTime = Math.max(0, (mediaEl.currentTime || 0) - offset);
    updateMediaSessionPosition(mediaEl);
  });
  setHandler("seekforward", (details) => {
    const offset = details?.seekOffset || 10;
    const dur = Number.isFinite(mediaEl.duration) ? mediaEl.duration : Infinity;
    mediaEl.currentTime = Math.min(dur, (mediaEl.currentTime || 0) + offset);
    updateMediaSessionPosition(mediaEl);
  });
  setHandler("previoustrack", () => {
    const idx = state.mediaItems.findIndex((x) => x.id === state.activeMediaId);
    if (idx > 0) {
      openViewer(state.mediaItems[idx - 1].id, { pushHistory: false }).catch(() => {});
    }
  });
  setHandler("nexttrack", () => {
    const idx = state.mediaItems.findIndex((x) => x.id === state.activeMediaId);
    if (idx >= 0 && idx < state.mediaItems.length - 1) {
      openViewer(state.mediaItems[idx + 1].id, { pushHistory: false }).catch(() => {});
    }
  });
}

export async function openViewer(mediaId, { pushHistory = true } = {}) {
  const item = await apiFetch(`/api/media/${mediaId}`);
  const modal = $("viewerModal");
  const wasOpen = Boolean(modal?.open);

  state.activeMediaId = item.id;
  state.activeMediaItem = item;
  state.activeMediaTags = item.tags || [];

  const targetHash = `#media=${item.id}`;
  if (!wasOpen) {
    if (pushHistory && location.hash !== targetHash) {
      history.pushState(
        { ...(history.state || {}), mediaId: item.id },
        "",
        location.pathname + location.search + targetHash
      );
      viewerPushedHistory = true;
    } else if (location.hash !== targetHash) {
      history.replaceState(
        history.state,
        "",
        location.pathname + location.search + targetHash
      );
    }
  }

  $("modalTitle").textContent = item.title || item.filename;

  const streamUrl = withToken(`/api/media/${item.id}/stream`);
  const dlUrl = withToken(`/api/media/${item.id}/stream?download=true`);
  $("modalDownload").href = dlUrl;

  const mediaBox = $("modalMedia");
  if (isZipItem(item)) {
    clearMediaSession();
    await renderZipExplorer(item, mediaBox);
  } else if (item.media_type === "video") {
    mediaBox.innerHTML = `<video controls autoplay playsinline src="${streamUrl}"></video>`;
    bindMediaSession(item, mediaBox.querySelector("video"));
  } else if (item.media_type === "image") {
    clearMediaSession();
    mediaBox.innerHTML = `<img src="${streamUrl}" alt="${escapeHtml(item.title)}" />`;
  } else if (item.media_type === "audio") {
    mediaBox.innerHTML = `<audio controls autoplay class="modal-audio" src="${streamUrl}"></audio>`;
    bindMediaSession(item, mediaBox.querySelector("audio"));
  } else {
    clearMediaSession();
    mediaBox.innerHTML = `<div class="modal-unavailable">Preview unavailable for ${escapeHtml(item.mime_type)}. Use Download below.</div>`;
  }

  const srcLink = item.source_url
    ? ` • <a href="${escapeHtml(item.source_url)}" target="_blank" rel="noopener" class="modal-source-link">Source URL</a>`
    : "";
  const topicInfo = item.telegram_topic_id ? ` • Topic #${item.telegram_topic_id}` : "";
  const partCount = Number(item.part_count || (item.parts ? item.parts.length : 1) || 1);
  const partInfo = partCount > 1
    ? ` • <span class="pill ok">${partCount} Parts • Unified Stream</span>`
    : "";
  const resExact = fmtResolution(item, { exact: true });
  const durTxt = fmtDuration(item.duration_sec);
  const codecs = [item.video_codec, item.audio_codec].filter(Boolean).join("/");
  const fpsTxt = item.fps ? `${Math.round(item.fps * 100) / 100}fps` : "";
  const richParts = [resExact, durTxt, codecs, fpsTxt].filter(Boolean);
  const richInfo = richParts.length ? ` • ${escapeHtml(richParts.join(" • "))}` : "";
  const tagsLine = item.tags && item.tags.length
    ? ` • ${item.tags.map((t) => `<span class="tag-chip">#${escapeHtml(t)}</span>`).join(" ")}`
    : "";
  $("modalMeta").innerHTML = `${escapeHtml(item.drive_name)}/${escapeHtml(item.folder || state.defaultFolder)} • ${fmtSize(item.size_bytes)}${richInfo} • ${escapeHtml(item.source_origin)}/${escapeHtml(item.source_kind)}${topicInfo}${partInfo}${tagsLine}${srcLink}`;

  if (!modal.open) modal.showModal();
}

export function closeViewer({ fromPopstate = false } = {}) {
  if (isClosingViewer) return;
  isClosingViewer = true;
  try {
    clearMediaSession();
    const modal = $("viewerModal");
    $("modalMedia").innerHTML = "";
    if (modal?.open) modal.close();
    state.activeMediaId = null;
    state.activeMediaItem = null;
    state.activeMediaTags = [];

    if (!fromPopstate) {
      if (viewerPushedHistory && location.hash.startsWith("#media=")) {
        viewerPushedHistory = false;
        history.back();
      } else if (location.hash.startsWith("#media=")) {
        viewerPushedHistory = false;
        history.replaceState(history.state, "", location.pathname + location.search);
      } else {
        viewerPushedHistory = false;
      }
    } else {
      viewerPushedHistory = false;
    }
  } finally {
    isClosingViewer = false;
  }
}

export function initViewer() {
  const viewerModal = $("viewerModal");
  $("closeModalBtn")?.addEventListener("click", () => closeViewer());
  viewerModal?.addEventListener("click", (e) => {
    if (e.target === viewerModal) closeViewer();
  });
  viewerModal?.addEventListener("cancel", (e) => {
    e.preventDefault();
    closeViewer();
  });

  $("modalEditMetaBtn")?.addEventListener("click", () => {
    if (!state.activeMediaId || !state.activeMediaItem) return;
    $("editMetaTitleInput").value = state.activeMediaItem.title || "";
    $("editMetaFolderInput").value = state.activeMediaItem.folder || state.defaultFolder;
    $("editMetaModal").showModal();
  });

  $("closeEditMetaBtn")?.addEventListener("click", () => $("editMetaModal").close());
  $("cancelEditMetaBtn")?.addEventListener("click", () => $("editMetaModal").close());
  $("editMetaModal")?.addEventListener("click", (e) => {
    if (e.target === $("editMetaModal")) $("editMetaModal").close();
  });

  $("saveEditMetaBtn")?.addEventListener("click", async () => {
    if (!state.activeMediaId) return;
    const title = $("editMetaTitleInput").value.trim();
    const folder = $("editMetaFolderInput").value.trim() || state.defaultFolder;
    const btn = $("saveEditMetaBtn");
    btn.disabled = true;
    try {
      await apiFetch(`/api/media/${state.activeMediaId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: title || undefined, folder }),
      });
      $("editMetaModal").close();
      await openViewer(state.activeMediaId, { pushHistory: false });
      emit("media:changed", { source: "edit-meta" });
    } catch (err) {
      toast(`Failed updating media: ${err.message}`, { type: "error" });
    } finally {
      btn.disabled = false;
    }
  });

  $("modalEditTagsBtn")?.addEventListener("click", async () => {
    if (!state.activeMediaId) return;
    const updatedTags = await openTagEditorDialog({
      title: "Edit Tags",
      initialTags: state.activeMediaTags || [],
    });
    if (updatedTags === null) return;
    try {
      await apiFetch(`/api/media/${state.activeMediaId}/tags`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tags: updatedTags }),
      });
      await openViewer(state.activeMediaId, { pushHistory: false });
      emit("media:changed", { source: "edit-tags" });
    } catch (err) {
      toast(`Failed updating tags: ${err.message}`, { type: "error" });
    }
  });

  $("modalDeleteBtn")?.addEventListener("click", async () => {
    if (!state.activeMediaId) return;
    const ok = await confirmDialog({
      title: "Delete Media",
      message: "Delete this file from the cloud drive?",
      confirmLabel: "Delete",
      danger: true,
    });
    if (!ok) return;
    const deletedId = state.activeMediaId;
    try {
      await apiFetch(`/api/media/${deletedId}`, { method: "DELETE" });
      state.selectedIds.delete(deletedId);
      closeViewer();
      emit("media:changed", { source: "delete" });
    } catch (err) {
      toast(`Failed deleting media: ${err.message}`, { type: "error" });
    }
  });

  const handleLocationHash = () => {
    if (!location.hash.startsWith("#media=")) {
      if ($("viewerModal")?.open) {
        closeViewer({ fromPopstate: true });
      }
      return;
    }
    const id = location.hash.slice(7);
    if (id && id !== state.activeMediaId) {
      openViewer(id, { pushHistory: false }).catch(() => {});
    }
  };

  window.addEventListener("popstate", handleLocationHash);
  window.addEventListener("hashchange", handleLocationHash);

  on("media:open", ({ id }) => {
    if (id) {
      openViewer(id).catch((err) => toast(err.message, { type: "error" }));
    }
  });
}
