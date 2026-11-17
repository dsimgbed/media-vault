import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch, withToken } from "/static/js/api.js";
import {
  $,
  escapeHtml,
  fmtDuration,
  fmtResolution,
  fmtSize,
} from "/static/js/utils.js";
import { getMode, onModeChange } from "/static/js/mode.js";
import { toast } from "/static/js/ui/dialogs.js";

let activeLoadSeq = 0;

function getCinemaVideos() {
  return (state.mediaItems || []).filter((it) => it.media_type === "video");
}

function updateToggleButton(el, active, activeText, inactiveText) {
  if (!el) return;
  el.classList.toggle("is-active", Boolean(active));
  el.setAttribute("aria-pressed", active ? "true" : "false");
  if (activeText && inactiveText) {
    el.textContent = active ? activeText : inactiveText;
  }
}

function updateMediaSessionPosition(video) {
  if (!("mediaSession" in navigator) || !video || getMode() !== "cinema") return;
  try {
    if (
      typeof navigator.mediaSession.setPositionState === "function" &&
      Number.isFinite(video.duration) &&
      video.duration > 0
    ) {
      navigator.mediaSession.setPositionState({
        duration: video.duration,
        playbackRate: video.playbackRate || 1,
        position: Math.min(video.duration, Math.max(0, video.currentTime || 0)),
      });
    }
  } catch (_) {}
}

function bindCinemaMediaSession(item, video) {
  if (!("mediaSession" in navigator) || !video) return;
  try {
    if (typeof MediaMetadata !== "undefined") {
      navigator.mediaSession.metadata = new MediaMetadata({
        title: item.title || item.filename || "Media Vault Cinema",
        artist: `${item.folder || state.defaultFolder} • Media Vault`,
        album: "Cinema",
        artwork: item.thumb_filename
          ? [{ src: withToken(`/api/media/${item.id}/thumb`), sizes: "512x512", type: "image/webp" }]
          : [],
      });
    }
    navigator.mediaSession.playbackState = video.paused ? "paused" : "playing";
  } catch (_) {}

  const setHandler = (action, fn) => {
    try {
      navigator.mediaSession.setActionHandler(action, fn);
    } catch (_) {}
  };

  setHandler("play", async () => {
    try {
      await video.play();
    } catch (_) {}
  });
  setHandler("pause", () => {
    try {
      video.pause();
    } catch (_) {}
  });
  setHandler("seekbackward", (details) => {
    const offset = details?.seekOffset || 10;
    video.currentTime = Math.max(0, (video.currentTime || 0) - offset);
    updateMediaSessionPosition(video);
  });
  setHandler("seekforward", (details) => {
    const offset = details?.seekOffset || 10;
    const dur = Number.isFinite(video.duration) ? video.duration : Infinity;
    video.currentTime = Math.min(dur, (video.currentTime || 0) + offset);
    updateMediaSessionPosition(video);
  });
  setHandler("previoustrack", () => playAdjacent(-1));
  setHandler("nexttrack", () => playAdjacent(1));
}

function updateNavButtons() {
  const videos = getCinemaVideos();
  const idx = videos.findIndex((x) => x.id === state.cinemaActiveId);
  const prevBtn = $("cinemaPrevBtn");
  const nextBtn = $("cinemaNextBtn");
  const detailsBtn = $("cinemaDetailsBtn");
  if (prevBtn) prevBtn.disabled = videos.length <= 1 || (!state.cinemaShuffle && idx <= 0);
  if (nextBtn) {
    nextBtn.disabled = videos.length <= 1 || (!state.cinemaShuffle && idx >= videos.length - 1 && state.mediaItems.length >= state.mediaTotal);
  }
  if (detailsBtn) detailsBtn.disabled = !state.cinemaActiveId;
}

function renderQueue() {
  const list = $("cinemaQueueList");
  const countEl = $("cinemaQueueCount");
  if (!list) return;

  const videos = getCinemaVideos();
  const activeIdx = videos.findIndex((x) => x.id === state.cinemaActiveId);

  if (countEl) {
    if (!videos.length) {
      countEl.textContent = "0 videos";
    } else if (activeIdx >= 0) {
      countEl.textContent = `${activeIdx + 1} / ${videos.length}`;
    } else {
      countEl.textContent = `${videos.length} video(s)`;
    }
  }

  if (!videos.length) {
    list.innerHTML = `<div class="cinema-queue-empty">No videos in current filter.</div>`;
    updateNavButtons();
    return;
  }

  list.innerHTML = videos
    .map((it, i) => {
      const active = it.id === state.cinemaActiveId;
      const thumbUrl = it.thumb_filename ? withToken(`/api/media/${it.id}/thumb`) : "";
      const thumbHtml = thumbUrl
        ? `<img loading="lazy" src="${escapeHtml(thumbUrl)}" alt="${escapeHtml(it.title || it.filename)}" />`
        : `<div class="cinema-queue-thumb-fallback">&#9654;</div>`;
      const dur = fmtDuration(it.duration_sec);
      const res = fmtResolution(it);
      const folder = it.folder || state.defaultFolder;
      const subParts = [folder, res, dur].filter(Boolean);
      return `
        <button type="button" class="cinema-queue-item${active ? " is-active" : ""}" data-id="${escapeHtml(it.id)}">
          <span class="cinema-queue-index">${active ? "▶" : String(i + 1)}</span>
          <span class="cinema-queue-thumb">
            ${thumbHtml}
            ${dur ? `<span class="cinema-queue-dur">${escapeHtml(dur)}</span>` : ""}
          </span>
          <span class="cinema-queue-info">
            <span class="cinema-queue-name">${escapeHtml(it.title || it.filename)}</span>
            <span class="cinema-queue-meta">${escapeHtml(subParts.join(" • "))}</span>
          </span>
        </button>
      `;
    })
    .join("");

  const activeEl = list.querySelector(".cinema-queue-item.is-active");
  if (activeEl) {
    activeEl.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }
  updateNavButtons();
}

function setEmptyStage(message) {
  const video = $("cinemaVideo");
  const overlay = $("cinemaEmptyOverlay");
  const titleEl = $("cinemaTitle");
  const metaEl = $("cinemaMeta");
  if (video) {
    video.pause();
    video.removeAttribute("src");
    video.removeAttribute("poster");
    video.load();
  }
  if (overlay) {
    overlay.hidden = false;
    const msgEl = overlay.querySelector(".cinema-empty-title");
    if (msgEl) msgEl.textContent = message;
  }
  if (titleEl) titleEl.textContent = "Cinema Theater";
  if (metaEl) metaEl.textContent = message;
  state.cinemaActiveId = null;
  state.cinemaActiveItem = null;
  emit("cinema:active-changed", { id: null });
}

function renderNowMeta(item) {
  const titleEl = $("cinemaTitle");
  const metaEl = $("cinemaMeta");
  if (titleEl) titleEl.textContent = item.title || item.filename || "Untitled Video";
  if (metaEl) {
    const folder = item.folder || state.defaultFolder;
    const res = fmtResolution(item, { exact: true });
    const dur = fmtDuration(item.duration_sec);
    const codecs = [item.video_codec, item.audio_codec].filter(Boolean).join("/");
    const fpsTxt = item.fps ? `${Math.round(item.fps * 100) / 100}fps` : "";
    const partCount = Number(item.part_count || (item.parts ? item.parts.length : 1) || 1);
    const partsTxt = partCount > 1 ? `${partCount} Parts` : "";
    const pieces = [folder, res, dur, codecs, fpsTxt, fmtSize(item.size_bytes), partsTxt].filter(Boolean);
    metaEl.textContent = pieces.join(" • ");
  }
}

export async function playCinemaVideo(mediaId, { autoplay = true, scroll = true } = {}) {
  const stage = $("cinemaStage");
  const video = $("cinemaVideo");
  const overlay = $("cinemaEmptyOverlay");
  if (!stage || !video) return;

  stage.hidden = false;
  const seq = ++activeLoadSeq;

  const localHint = state.mediaItems.find((x) => x.id === mediaId);
  state.cinemaActiveId = mediaId;
  if (localHint) {
    state.cinemaActiveItem = localHint;
    renderNowMeta(localHint);
    if (localHint.thumb_filename) {
      video.poster = withToken(`/api/media/${localHint.id}/thumb`);
    } else {
      video.removeAttribute("poster");
    }
  }
  if (overlay) overlay.hidden = true;
  renderQueue();
  emit("cinema:active-changed", { id: mediaId });

  if (scroll) {
    stage.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  let item = localHint;
  try {
    item = await apiFetch(`/api/media/${mediaId}`);
  } catch (err) {
    if (!localHint) {
      toast(`Failed loading video: ${err.message}`, { type: "error" });
      return;
    }
  }
  if (seq !== activeLoadSeq) return;

  state.cinemaActiveId = item.id;
  state.cinemaActiveItem = item;
  renderNowMeta(item);

  const streamUrl = withToken(`/api/media/${item.id}/stream`);
  if (item.thumb_filename) {
    video.poster = withToken(`/api/media/${item.id}/thumb`);
  } else {
    video.removeAttribute("poster");
  }
  if (video.getAttribute("src") !== streamUrl) {
    video.src = streamUrl;
  }
  video.loop = state.cinemaLoop;
  video.playbackRate = state.cinemaSpeed || 1;
  bindCinemaMediaSession(item, video);

  if (autoplay) {
    video.play().catch(() => {});
  }
  updateNavButtons();
}

function playAdjacent(delta) {
  const videos = getCinemaVideos();
  if (!videos.length) return;
  const idx = videos.findIndex((x) => x.id === state.cinemaActiveId);

  if (state.cinemaShuffle && videos.length > 1) {
    let pick = idx;
    while (pick === idx) {
      pick = Math.floor(Math.random() * videos.length);
    }
    playCinemaVideo(videos[pick].id, { autoplay: true, scroll: false }).catch(() => {});
    return;
  }

  const nextIdx = idx < 0 ? 0 : idx + delta;
  if (nextIdx >= 0 && nextIdx < videos.length) {
    playCinemaVideo(videos[nextIdx].id, { autoplay: true, scroll: false }).catch(() => {});
    if (nextIdx >= videos.length - 2 && state.mediaItems.length < state.mediaTotal) {
      emit("cinema:load-more");
    }
  }
}

function toggleTheater(force) {
  const next = typeof force === "boolean" ? force : !state.cinemaTheater;
  state.cinemaTheater = next;
  document.body.classList.toggle("cinema-theater", next);
  const backdrop = $("cinemaLightsBackdrop");
  if (backdrop) backdrop.hidden = !next;
  updateToggleButton($("cinemaTheaterBtn"), next, "Lights On", "Lights Off");
}

function toggleFullscreen() {
  const viewport = $("cinemaViewport");
  const video = $("cinemaVideo");
  if (document.fullscreenElement) {
    document.exitFullscreen?.().catch(() => {});
    return;
  }
  if (viewport?.requestFullscreen) {
    viewport.requestFullscreen().catch(() => {
      video?.webkitEnterFullscreen?.();
    });
  } else if (video?.webkitEnterFullscreen) {
    video.webkitEnterFullscreen();
  }
}

export function initCinema() {
  const stage = $("cinemaStage");
  const video = $("cinemaVideo");
  if (!stage || !video) return;

  if (getMode() === "cinema") {
    stage.hidden = false;
  }

  video.addEventListener("play", () => {
    try {
      if ("mediaSession" in navigator && getMode() === "cinema") {
        navigator.mediaSession.playbackState = "playing";
        updateMediaSessionPosition(video);
      }
    } catch (_) {}
  });
  video.addEventListener("pause", () => {
    try {
      if ("mediaSession" in navigator && getMode() === "cinema") {
        navigator.mediaSession.playbackState = "paused";
        updateMediaSessionPosition(video);
      }
    } catch (_) {}
  });
  video.addEventListener("timeupdate", () => updateMediaSessionPosition(video));
  video.addEventListener("loadedmetadata", () => updateMediaSessionPosition(video));
  video.addEventListener("durationchange", () => updateMediaSessionPosition(video));
  video.addEventListener("ended", () => {
    if (!state.cinemaLoop && state.cinemaAutoNext && getMode() === "cinema") {
      playAdjacent(1);
    }
  });

  $("cinemaPrevBtn")?.addEventListener("click", () => playAdjacent(-1));
  $("cinemaNextBtn")?.addEventListener("click", () => playAdjacent(1));

  $("cinemaAutoNextBtn")?.addEventListener("click", () => {
    state.cinemaAutoNext = !state.cinemaAutoNext;
    updateToggleButton($("cinemaAutoNextBtn"), state.cinemaAutoNext);
  });

  $("cinemaLoopBtn")?.addEventListener("click", () => {
    state.cinemaLoop = !state.cinemaLoop;
    video.loop = state.cinemaLoop;
    updateToggleButton($("cinemaLoopBtn"), state.cinemaLoop);
  });

  $("cinemaShuffleBtn")?.addEventListener("click", () => {
    state.cinemaShuffle = !state.cinemaShuffle;
    updateToggleButton($("cinemaShuffleBtn"), state.cinemaShuffle);
    updateNavButtons();
  });

  $("cinemaSpeedSelect")?.addEventListener("change", (e) => {
    const rate = Number(e.target.value) || 1;
    state.cinemaSpeed = rate;
    video.playbackRate = rate;
  });

  $("cinemaTheaterBtn")?.addEventListener("click", () => toggleTheater());
  $("cinemaLightsBackdrop")?.addEventListener("click", () => toggleTheater(false));
  $("cinemaFullscreenBtn")?.addEventListener("click", () => toggleFullscreen());

  $("cinemaDetailsBtn")?.addEventListener("click", () => {
    if (!state.cinemaActiveId) return;
    if (!video.paused) video.pause();
    emit("media:open", { id: state.cinemaActiveId });
  });

  $("cinemaQueueList")?.addEventListener("click", (e) => {
    const btn = e.target.closest(".cinema-queue-item");
    const id = btn?.dataset.id;
    if (id) {
      playCinemaVideo(id, { autoplay: true, scroll: false }).catch(() => {});
    }
  });

  on("cinema:play", ({ id } = {}) => {
    if (id) {
      playCinemaVideo(id, { autoplay: true, scroll: true }).catch(() => {});
    }
  });

  on("media:changed", ({ source, id } = {}) => {
    if (source === "delete" && id && id === state.cinemaActiveId) {
      state.cinemaActiveId = null;
      state.cinemaActiveItem = null;
    }
  });

  on("media:loaded", () => {
    if (getMode() !== "cinema") return;
    stage.hidden = false;
    const videos = getCinemaVideos();
    if (!videos.length) {
      setEmptyStage(
        state.authRequired && !state.token
          ? "Unlock the vault above to watch your videos"
          : "No videos match the current filter"
      );
      renderQueue();
      return;
    }

    const updatedActive = state.cinemaActiveId
      ? videos.find((v) => v.id === state.cinemaActiveId)
      : null;
    if (!state.cinemaActiveId) {
      playCinemaVideo(videos[0].id, { autoplay: false, scroll: false }).catch(() => {});
    } else if (updatedActive) {
      state.cinemaActiveItem = updatedActive;
      renderNowMeta(updatedActive);
      renderQueue();
    } else {
      renderQueue();
    }
  });

  onModeChange((mode, prevMode) => {
    if (prevMode === "cinema" && mode !== "cinema") {
      if (!video.paused) video.pause();
      toggleTheater(false);
      stage.hidden = true;
    } else if (mode === "cinema") {
      stage.hidden = false;
      renderQueue();
    }
  });

  document.addEventListener("keydown", (e) => {
    if (getMode() !== "cinema") return;
    if (document.querySelector("dialog[open]")) return;
    const target = e.target;
    if (
      target instanceof HTMLElement &&
      (target.matches("input, textarea, select") || target.isContentEditable)
    ) {
      return;
    }

    if (e.key === "Escape" && state.cinemaTheater) {
      e.preventDefault();
      toggleTheater(false);
      return;
    }
    const k = e.key.toLowerCase();
    if (k === "t") {
      e.preventDefault();
      toggleTheater();
    } else if (k === "f") {
      e.preventDefault();
      toggleFullscreen();
    } else if (k === "n") {
      e.preventDefault();
      playAdjacent(1);
    } else if (k === "p") {
      e.preventDefault();
      playAdjacent(-1);
    } else if (k === "m" && video.src) {
      e.preventDefault();
      video.muted = !video.muted;
    } else if ((e.key === " " || k === "k") && video.src) {
      e.preventDefault();
      if (video.paused) video.play().catch(() => {});
      else video.pause();
    } else if ((k === "j" || e.key === "ArrowLeft") && video.src) {
      e.preventDefault();
      video.currentTime = Math.max(0, (video.currentTime || 0) - 10);
    } else if ((k === "l" || e.key === "ArrowRight") && video.src) {
      e.preventDefault();
      const dur = Number.isFinite(video.duration) ? video.duration : Infinity;
      video.currentTime = Math.min(dur, (video.currentTime || 0) + 10);
    }
  });
}
