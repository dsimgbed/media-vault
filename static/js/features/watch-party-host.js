import { state } from "/static/js/state.js";
import { apiFetch } from "/static/js/api.js";
import { $, fmtDuration, fmtResolution } from "/static/js/utils.js";
import { toast, confirmDialog } from "/static/js/ui/dialogs.js";
import { renderQrSvg } from "/static/js/ui/qr.js";

const SUPPRESS_MS = 450;
const SEEK_DEBOUNCE_MS = 150;
const HOST_QUEUE_LIMIT = 50;
const HOST_DRIFT_THRESHOLD_SEC = 1.5;
const HOST_SEEK_ALIGN_SEC = 0.45;
const HOST_BUFFERING_REPORT_DELAY_MS = 650;
const HOST_BUFFERING_RESUME_AHEAD_SEC = 1.5;
const HOST_BUFFERING_CHECK_INTERVAL_MS = 300;
const HOST_BUFFERING_READY_FALLBACK_MS = 1500;
const HOST_POST_RESUME_GRACE_MS = 1200;
const HOST_LOCAL_ACTION_LOCK_MS = 1500;
const HOST_PING_BURST_COUNT = 3;
const HOST_PING_BURST_GAP_MS = 250;
const HOST_RECONNECT_DELAYS = [1000, 2000, 4000, 8000, 15000];
const HOST_NO_RECONNECT_CODES = new Set([4001, 4003, 4004, 4005]);

let hostDialogEl = null;
let hostRoomCode = null;
let hostToken = null;
let hostParticipantId = null;
let hostDisplayName = "";
let hostState = null;
let hostWs = null;
let hostSyncActive = false;
let hostControlMode = "host_only";
let hostWaitForBuffering = false;
let suppressUntil = 0;
let isApplyingSync = false;
let hostSyncTimer = null;
let hostPingTimer = null;
let hostPingBurstTimers = [];
let hostReconnectTimer = null;
let hostReconnectAttempt = 0;
let hostIntentionalClose = false;
let hostClockOffsetMs = 0;
let hostClockSamples = 0;
let hostStateReceivedPerfMs = 0;
let hostLastStateSeq = -1;
let hostLastBufferingSent = false;
let hostBufferingReportTimer = null;
let hostBufferingReadyTimer = null;
let hostBufferingReportedAt = 0;
let hostPostResumeGraceUntil = 0;
let hostUserSeeking = false;
let hostPendingSeekTarget = null;
let hostLocalActionLockUntil = 0;
let hostProgrammaticSeekTarget = null;
let hostProgrammaticSeekUntil = 0;
let hostLastTimeUpdatePos = -1;
let hostDeps = null;
let hostCinemaSelectedHandler = null;

function hostCinemaVideo() {
  try {
    if (hostDeps && typeof hostDeps.getVideoElement === "function") {
      const el = hostDeps.getVideoElement();
      if (el) return el;
    }
  } catch (_) {}
  return $("cinemaVideo");
}

function hostCinemaVideos() {
  if (hostDeps && typeof hostDeps.getCinemaVideos === "function") {
    const videos = hostDeps.getCinemaVideos();
    return Array.isArray(videos) ? videos : [];
  }
  return (state.mediaItems || []).filter((it) => it.media_type === "video");
}

function hostPlayCinema(mediaId, opts = {}) {
  if (hostDeps && typeof hostDeps.playCinemaVideo === "function") {
    return hostDeps.playCinemaVideo(mediaId, opts);
  }
  return Promise.resolve(null);
}

function hostWsSend(payload) {
  if (!hostWs || hostWs.readyState !== WebSocket.OPEN) return false;
  try {
    hostWs.send(JSON.stringify(payload));
    return true;
  } catch (_) {
    return false;
  }
}

function hostWsUrl() {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/api/watch-party/${encodeURIComponent(hostRoomCode || "")}/ws`;
}

function clearHostReconnectTimer() {
  if (hostReconnectTimer) {
    clearTimeout(hostReconnectTimer);
    hostReconnectTimer = null;
  }
}

function cancelHostPendingBuffering() {
  if (hostBufferingReportTimer) {
    clearTimeout(hostBufferingReportTimer);
    hostBufferingReportTimer = null;
  }
}

function stopHostBufferingReadyPoll() {
  if (hostBufferingReadyTimer) {
    clearInterval(hostBufferingReadyTimer);
    hostBufferingReadyTimer = null;
  }
}

function clearHostBufferingTimers() {
  cancelHostPendingBuffering();
  stopHostBufferingReadyPoll();
  hostBufferingReportedAt = 0;
}

function stopHostTimers() {
  if (hostSyncTimer) {
    clearTimeout(hostSyncTimer);
    hostSyncTimer = null;
  }
  if (hostPingTimer) {
    clearInterval(hostPingTimer);
    hostPingTimer = null;
  }
  for (const t of hostPingBurstTimers) clearTimeout(t);
  hostPingBurstTimers = [];
  clearHostBufferingTimers();
}

function sendHostPing() {
  if (hostWs && hostWs.readyState === WebSocket.OPEN) {
    hostWsSend({ type: "ping", client_time_ms: Date.now() });
  }
}

function startHostPing() {
  stopHostTimers();
  sendHostPing();
  for (let i = 1; i < HOST_PING_BURST_COUNT; i += 1) {
    const t = setTimeout(sendHostPing, i * HOST_PING_BURST_GAP_MS);
    hostPingBurstTimers.push(t);
  }
  hostPingTimer = setInterval(sendHostPing, 15000);
}

function handleHostPong(msg) {
  const now = Date.now();
  const clientTime = Number(msg.client_time_ms);
  const serverTime = Number(msg.server_time_ms);
  if (!Number.isFinite(clientTime) || !Number.isFinite(serverTime)) return;
  const rtt = Math.max(0, now - clientTime);
  const sampleOffset = serverTime - (clientTime + rtt / 2);
  if (hostClockSamples === 0) {
    hostClockOffsetMs = sampleOffset;
  } else {
    const alpha = rtt < 150 ? 0.25 : 0.1;
    hostClockOffsetMs = hostClockOffsetMs + alpha * (sampleOffset - hostClockOffsetMs);
  }
  hostClockSamples += 1;
}

function closeHostWs({ intentional = true } = {}) {
  hostIntentionalClose = Boolean(intentional);
  clearHostReconnectTimer();
  stopHostTimers();
  hostLastBufferingSent = false;
  hostUserSeeking = false;
  hostPendingSeekTarget = null;
  hostLocalActionLockUntil = 0;
  hostProgrammaticSeekTarget = null;
  if (hostWs) {
    const sock = hostWs;
    hostWs = null;
    try { sock.close(); } catch (_) {}
  }
}

function hostInviteUrl() {
  if (!hostRoomCode) return "";
  return `${window.location.origin}/watch/${encodeURIComponent(hostRoomCode)}`;
}

function setHostSyncToggle(on) {
  const toggle = $("wpHostSyncToggle");
  if (!toggle) return;
  toggle.textContent = on ? "Sync: On" : "Sync: Off";
  toggle.setAttribute("aria-pressed", on ? "true" : "false");
}

function updateHostPartyButton(count) {
  const btn = $("cinemaWatchPartyBtn");
  if (!btn) return;
  const total = Number(count);
  const connected = Number.isFinite(total) ? Math.max(0, Math.round(total)) : 0;
  btn.classList.toggle("is-active", Boolean(hostSyncActive));
  btn.textContent = hostSyncActive ? `Party • ${connected}` : "Watch Party";
}

function getHostQueuedIds(roomState) {
  if (!roomState) return [];
  if (Array.isArray(roomState.media_ids)) return roomState.media_ids.map(String);
  if (Array.isArray(roomState.queue_ids)) return roomState.queue_ids.map(String);
  if (Array.isArray(roomState.queue)) {
    return roomState.queue
      .map((it) => String(it?.media_id || it?.id || ""))
      .filter(Boolean);
  }
  return [];
}

function syncAddActiveBtnVisibility() {
  const addBtn = $("wpHostAddActiveBtn");
  if (!addBtn) return;
  const queuedIds = getHostQueuedIds(hostState);
  const activeId = state.cinemaActiveId ? String(state.cinemaActiveId) : null;
  addBtn.hidden = !activeId || queuedIds.includes(activeId);
}

function isHostUserSeeking() {
  return Boolean(
    hostUserSeeking ||
    hostSyncTimer !== null ||
    performance.now() < hostLocalActionLockUntil
  );
}

function setHostVideoTimeSynced(video, targetPos) {
  if (!video || !Number.isFinite(targetPos)) return;
  let clamped = Math.max(0, targetPos);
  if (Number.isFinite(video.duration) && video.duration > 0) {
    clamped = Math.min(video.duration, clamped);
  }
  hostProgrammaticSeekTarget = clamped;
  hostProgrammaticSeekUntil = performance.now() + 8000;
  try { video.currentTime = clamped; } catch (_) {}
}

function isHostProgrammaticSeekEvent(video, { consume = false } = {}) {
  if (hostProgrammaticSeekTarget === null) return false;
  if (performance.now() > hostProgrammaticSeekUntil) {
    hostProgrammaticSeekTarget = null;
    return false;
  }
  const pos = Number(video?.currentTime ?? NaN);
  if (Number.isFinite(pos) && Math.abs(pos - hostProgrammaticSeekTarget) <= 1.5) {
    if (consume) hostProgrammaticSeekTarget = null;
    return true;
  }
  hostProgrammaticSeekTarget = null;
  return false;
}

function getHostBufferedAheadSec(video) {
  if (!video || !video.buffered || video.buffered.length === 0) return 0;
  const t = Number(video.currentTime || 0);
  if (!Number.isFinite(t)) return 0;
  for (let i = 0; i < video.buffered.length; i += 1) {
    const start = video.buffered.start(i);
    const end = video.buffered.end(i);
    if (t >= start - 0.15 && t <= end + 0.15) {
      return Math.max(0, end - t);
    }
  }
  return 0;
}

function hasHostEnoughBuffer(video) {
  if (!video || !video.src || video.ended) return true;
  if (video.readyState >= 4) return true;
  if (video.readyState >= 3) {
    const ahead = getHostBufferedAheadSec(video);
    if (ahead >= HOST_BUFFERING_RESUME_AHEAD_SEC) return true;
    const dur = Number(video.duration);
    if (Number.isFinite(dur) && dur > 0 && dur - (Number(video.currentTime) || 0) <= 1.0) {
      return true;
    }
    if (
      hostBufferingReportedAt > 0 &&
      performance.now() - hostBufferingReportedAt >= HOST_BUFFERING_READY_FALLBACK_MS
    ) {
      return true;
    }
  }
  return false;
}

function checkHostBufferingReady() {
  if (!hostLastBufferingSent) {
    stopHostBufferingReadyPoll();
    return;
  }
  const video = hostCinemaVideo();
  if (!hostSyncActive || !video || !video.src || video.ended) {
    reportHostBuffering(false);
    return;
  }
  if (hostState && !hostState.is_playing && !hostState.paused_for_buffering) {
    reportHostBuffering(false);
    return;
  }
  if (hasHostEnoughBuffer(video)) {
    reportHostBuffering(false);
  }
}

function reportHostBuffering(isBuffering) {
  const desired = Boolean(isBuffering);
  if (!desired) {
    cancelHostPendingBuffering();
    stopHostBufferingReadyPoll();
    hostBufferingReportedAt = 0;
  }
  if (!hostSyncActive) {
    hostLastBufferingSent = false;
    return;
  }
  if (hostLastBufferingSent === desired) return;
  hostLastBufferingSent = desired;
  if (desired) {
    hostBufferingReportedAt = performance.now();
    stopHostBufferingReadyPoll();
    hostBufferingReadyTimer = setInterval(checkHostBufferingReady, HOST_BUFFERING_CHECK_INTERVAL_MS);
  }
  hostWsSend({ type: "buffering", is_buffering: desired });
}

function scheduleHostBufferingReport() {
  const video = hostCinemaVideo();
  if (!hostSyncActive || !video || !video.src) return;
  if (hostLastBufferingSent || hostBufferingReportTimer) return;
  if (video.paused || video.ended || video.seeking || isApplyingSync || isHostUserSeeking()) return;
  if (video.readyState >= 3) return;
  if (hostState && !hostState.is_playing) return;

  const graceRemaining = Math.max(0, hostPostResumeGraceUntil - performance.now());
  const delay = Math.max(HOST_BUFFERING_REPORT_DELAY_MS, graceRemaining);
  hostBufferingReportTimer = setTimeout(() => {
    hostBufferingReportTimer = null;
    const v = hostCinemaVideo();
    if (!hostSyncActive || !v || !v.src) return;
    if (v.paused || v.ended || v.seeking || isApplyingSync || isHostUserSeeking()) return;
    if (v.readyState >= 3) return;
    if (hostState && !hostState.is_playing) return;
    reportHostBuffering(true);
  }, delay);
}

function sendHostSeekDebounced(positionSec) {
  if (hostSyncTimer) clearTimeout(hostSyncTimer);
  const target = Number(positionSec);
  if (!Number.isFinite(target) || target < 0) return;
  hostSyncTimer = setTimeout(() => {
    hostSyncTimer = null;
    if (!hostSyncActive) return;
    hostWsSend({ type: "seek", position_sec: target });
  }, SEEK_DEBOUNCE_MS);
}

function sendHostSeekImmediate(positionSec) {
  if (hostSyncTimer) {
    clearTimeout(hostSyncTimer);
    hostSyncTimer = null;
  }
  const target = Number(positionSec);
  if (!hostSyncActive || !Number.isFinite(target) || target < 0) return;
  hostWsSend({ type: "seek", position_sec: target });
}

function bindHostCinemaSync(video) {
  if (!video || video.__wpHostCinemaSyncBound) return;
  video.__wpHostCinemaSyncBound = true;
  video.addEventListener("play", () => {
    if (!hostSyncActive || isApplyingSync || performance.now() < suppressUntil) return;
    cancelHostPendingBuffering();
    hostPostResumeGraceUntil = performance.now() + HOST_POST_RESUME_GRACE_MS;
    hostLocalActionLockUntil = performance.now() + 900;
    hostWsSend({ type: "play", position_sec: Number(video.currentTime || 0) });
    reportHostBuffering(false);
  });
  video.addEventListener("pause", () => {
    cancelHostPendingBuffering();
    if (!hostSyncActive || isApplyingSync || performance.now() < suppressUntil) return;
    hostLocalActionLockUntil = performance.now() + 900;
    reportHostBuffering(false);
    hostWsSend({ type: "pause", position_sec: Number(video.currentTime || 0) });
  });
  video.addEventListener("seeking", () => {
    if (!hostSyncActive || isApplyingSync || performance.now() < suppressUntil) return;
    if (isHostProgrammaticSeekEvent(video, { consume: false })) return;
    cancelHostPendingBuffering();
    hostUserSeeking = true;
    const targetPos = Number(video.currentTime || 0);
    hostPendingSeekTarget = targetPos;
    hostLocalActionLockUntil = performance.now() + HOST_LOCAL_ACTION_LOCK_MS;
    sendHostSeekDebounced(targetPos);
  });
  video.addEventListener("seeked", () => {
    if (hostLastBufferingSent) checkHostBufferingReady();
    const wasProgrammatic = isHostProgrammaticSeekEvent(video, { consume: true });
    if (!hostSyncActive || isApplyingSync || performance.now() < suppressUntil || wasProgrammatic) return;
    hostUserSeeking = false;
    const targetPos = Number(video.currentTime || 0);
    const shouldSend =
      hostSyncTimer !== null ||
      hostPendingSeekTarget === null ||
      Math.abs(targetPos - hostPendingSeekTarget) > 0.15;
    hostPendingSeekTarget = targetPos;
    hostLocalActionLockUntil = performance.now() + 900;
    if (shouldSend) {
      sendHostSeekImmediate(targetPos);
    }
  });
  video.addEventListener("ratechange", () => {
    if (!hostSyncActive || isApplyingSync || performance.now() < suppressUntil) return;
    const rate = Number(video.playbackRate || 1);
    if (!Number.isFinite(rate) || rate <= 0) return;
    hostLocalActionLockUntil = performance.now() + 700;
    hostWsSend({ type: "rate", rate });
  });
  video.addEventListener("waiting", () => {
    if (!hostSyncActive) return;
    scheduleHostBufferingReport();
  });
  video.addEventListener("stalled", () => {
    if (!hostSyncActive) return;
    // Only treat network stall as buffering if playback is actually starved.
    if (!video.paused && !video.ended && video.readyState < 3) {
      scheduleHostBufferingReport();
    }
  });
  video.addEventListener("playing", () => {
    if (!hostSyncActive) return;
    cancelHostPendingBuffering();
    reportHostBuffering(false);
  });
  video.addEventListener("canplay", () => {
    if (!hostSyncActive) return;
    if (video.readyState >= 3) cancelHostPendingBuffering();
    if (hostLastBufferingSent) checkHostBufferingReady();
  });
  video.addEventListener("canplaythrough", () => {
    if (!hostSyncActive) return;
    cancelHostPendingBuffering();
    if (hostLastBufferingSent) reportHostBuffering(false);
  });
  video.addEventListener("progress", () => {
    if (!hostSyncActive) return;
    if (hostLastBufferingSent) checkHostBufferingReady();
  });
  video.addEventListener("timeupdate", () => {
    if (!hostSyncActive) return;
    const pos = Number(video.currentTime || 0);
    if (!video.paused && Number.isFinite(pos) && Math.abs(pos - hostLastTimeUpdatePos) > 0.05) {
      hostLastTimeUpdatePos = pos;
      cancelHostPendingBuffering();
      if (hostLastBufferingSent) reportHostBuffering(false);
    }
  });
  video.addEventListener("ended", () => {
    clearHostBufferingTimers();
    reportHostBuffering(false);
  });
}

function getHostExpectedPosition(roomState, fallbackSec = 0) {
  const hasEffective =
    roomState?.effective_position_sec !== undefined &&
    roomState?.effective_position_sec !== null &&
    Number.isFinite(Number(roomState.effective_position_sec));
  const base = hasEffective
    ? Number(roomState.effective_position_sec)
    : Number(roomState?.position_sec ?? fallbackSec);
  let expected = Number.isFinite(base) ? Math.max(0, base) : Math.max(0, Number(fallbackSec) || 0);
  if (roomState?.is_playing) {
    const rate = Number(roomState.playback_rate || 1) || 1;
    const refTimeMs = hasEffective
      ? Number(roomState.server_time_ms || 0)
      : Number(roomState.state_updated_at_ms || roomState.server_time_ms || 0);
    let elapsed = 0;
    if (hostClockSamples > 0 && Number.isFinite(refTimeMs) && refTimeMs > 0) {
      elapsed = Math.max(0, (Date.now() + hostClockOffsetMs - refTimeMs) / 1000);
    } else if (hasEffective && hostStateReceivedPerfMs > 0) {
      elapsed = Math.max(0, (performance.now() - hostStateReceivedPerfMs) / 1000);
    }
    expected = expected + elapsed * rate;
  }
  return Math.max(0, expected);
}

function applyCinemaPlaybackState(roomState, { reason = "", allowSeek = true } = {}) {
  const video = hostCinemaVideo();
  if (!video || !roomState) return;
  const rate = Number(roomState.playback_rate || 1) || 1;
  const shouldPlay = Boolean(roomState.is_playing);
  const currentlyPlaying = !video.paused;
  const currentTime = Number(video.currentTime) || 0;
  const expected = getHostExpectedPosition(roomState, currentTime);
  const duration = Number(video.duration);
  const target = Number.isFinite(duration) && duration > 0 ? Math.min(expected, duration) : expected;

  const seekThreshold = reason === "seek" ? HOST_SEEK_ALIGN_SEC : HOST_DRIFT_THRESHOLD_SEC;
  const needRate = Math.abs((Number(video.playbackRate) || 1) - rate) > 0.01;
  const needSeek = allowSeek && Math.abs(currentTime - target) > seekThreshold;
  const needPlay = shouldPlay && !currentlyPlaying;
  const needPause = !shouldPlay && currentlyPlaying;

  if (!needRate && !needSeek && !needPlay && !needPause) {
    return;
  }

  cancelHostPendingBuffering();
  isApplyingSync = true;
  suppressUntil = performance.now() + SUPPRESS_MS;
  if (needSeek || needPlay) {
    hostPostResumeGraceUntil = performance.now() + HOST_POST_RESUME_GRACE_MS;
  }
  if (needRate) {
    try { video.playbackRate = rate; } catch (_) {}
  }
  if (needSeek) {
    setHostVideoTimeSynced(video, target);
  }
  if (needPlay) {
    const p = video.play?.();
    if (p && typeof p.catch === "function") p.catch(() => {});
  } else if (needPause) {
    if (!roomState.paused_for_buffering) {
      reportHostBuffering(false);
    }
    try { video.pause(); } catch (_) {}
  }
  window.setTimeout(() => { isApplyingSync = false; }, 0);
}

function applyCinemaSyncState(roomState, opts = {}) {
  const video = hostCinemaVideo();
  if (!video || !roomState) return;
  const nextActiveId = roomState.active_media_id ? String(roomState.active_media_id) : null;
  if (nextActiveId && nextActiveId !== String(state.cinemaActiveId || "")) {
    clearHostBufferingTimers();
    hostLastBufferingSent = false;
    hostProgrammaticSeekTarget = null;
    hostPlayCinema(nextActiveId, {
      autoplay: Boolean(roomState.is_playing),
      scroll: false,
      fromWatchParty: true,
    })
      .then(() => applyCinemaPlaybackState(roomState, opts))
      .catch(() => {});
    return;
  }
  applyCinemaPlaybackState(roomState, opts);
}

function handleHostPermanentDisconnect(message) {
  clearHostReconnectTimer();
  stopHostTimers();
  hostLastBufferingSent = false;
  hostUserSeeking = false;
  hostPendingSeekTarget = null;
  hostLocalActionLockUntil = 0;
  hostProgrammaticSeekTarget = null;
  if (hostSyncActive) {
    hostSyncActive = false;
    setHostSyncToggle(false);
    updateHostPartyButton(hostState?.participant_count);
    toast(message || "Watch Party sync disconnected", { type: "info" });
  }
}

function scheduleHostReconnect() {
  if (hostIntentionalClose || !hostSyncActive || !hostRoomCode || !hostToken) return;
  if (hostWs && (hostWs.readyState === WebSocket.OPEN || hostWs.readyState === WebSocket.CONNECTING)) return;
  if (hostReconnectAttempt >= HOST_RECONNECT_DELAYS.length) {
    handleHostPermanentDisconnect("Watch Party sync disconnected after repeated retries.");
    return;
  }
  const delay = HOST_RECONNECT_DELAYS[hostReconnectAttempt];
  hostReconnectAttempt += 1;
  clearHostReconnectTimer();
  hostReconnectTimer = setTimeout(() => {
    hostReconnectTimer = null;
    connectHostWs();
  }, delay);
}

function connectHostWs() {
  if (!hostRoomCode || !hostToken) return;
  if (hostWs && (hostWs.readyState === WebSocket.OPEN || hostWs.readyState === WebSocket.CONNECTING)) return;
  clearHostReconnectTimer();
  stopHostTimers();
  hostIntentionalClose = false;

  let socket;
  try {
    socket = new WebSocket(hostWsUrl());
  } catch (_) {
    scheduleHostReconnect();
    return;
  }
  hostWs = socket;
  socket.onopen = () => {
    if (hostWs !== socket) return;
    hostWsSend({ type: "auth", token: hostToken });
  };
  socket.onmessage = (ev) => {
    if (hostWs !== socket) return;
    let raw = ev?.data;
    if (typeof raw !== "string") {
      try { raw = JSON.stringify(raw); } catch (_) { return; }
    }
    handleHostWsMessage(raw);
  };
  socket.onclose = (ev) => {
    if (hostWs === socket) hostWs = null;
    stopHostTimers();
    if (hostIntentionalClose || !hostSyncActive) return;

    const code = ev?.code;
    if (code && HOST_NO_RECONNECT_CODES.has(code)) {
      let msg = "Watch Party sync disconnected.";
      if (code === 4004) msg = "Watch Party room ended.";
      else if (code === 4005) msg = "Watch Party sync moved to another tab.";
      else if (code === 4001 || code === 4003) msg = "Watch Party host session expired.";
      handleHostPermanentDisconnect(msg);
      return;
    }

    scheduleHostReconnect();
  };
  socket.onerror = () => {
    // onclose handles reconnect/cleanup.
  };
}

function handleHostWsMessage(raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch (_) { return; }
  if (!msg || typeof msg.type !== "string") return;

  if (msg.type === "pong") {
    handleHostPong(msg);
    return;
  }
  if (msg.type === "auth_ok") {
    hostReconnectAttempt = 0;
    clearHostReconnectTimer();
    clearHostBufferingTimers();
    hostLastBufferingSent = false;
    hostUserSeeking = false;
    hostPendingSeekTarget = null;
    hostLocalActionLockUntil = 0;
    hostProgrammaticSeekTarget = null;
    hostParticipantId = String(msg.participant_id || hostParticipantId || "");
    if (msg.room_state) {
      hostStateReceivedPerfMs = performance.now();
      hostState = msg.room_state;
      hostLastStateSeq = Number(hostState.state_seq ?? hostState.seq ?? -1);
      hostControlMode = String(hostState.control_mode || "host_only");
      hostWaitForBuffering = Boolean(hostState.wait_for_buffering);
      renderHostParticipants(hostState.participants);
      renderHostQueue(hostState.queue);
      syncAddActiveBtnVisibility();
      updateHostPartyButton(hostState.participant_count);
    }
    startHostPing();
    return;
  }
  if (msg.type === "sync_state") {
    const prevState = hostState;
    const roomState = msg.room_state || null;
    if (roomState) {
      const seq = Number(roomState.state_seq ?? roomState.seq ?? 0);
      if (seq > 0 && seq < hostLastStateSeq) return;
      if (seq > 0) hostLastStateSeq = seq;
      hostStateReceivedPerfMs = performance.now();
      hostState = roomState;
      hostControlMode = String(roomState.control_mode || "host_only");
      hostWaitForBuffering = Boolean(roomState.wait_for_buffering);
      renderHostParticipants(roomState.participants);
      renderHostQueue(roomState.queue);
      syncAddActiveBtnVisibility();
      const metaStatus = $("wpHostRoomMetaStatus");
      if (metaStatus) {
        const label = roomState.paused_for_buffering
          ? "Waiting for buffering"
          : (roomState.is_playing ? "Playing" : "Paused");
        metaStatus.textContent = `${hostRoomCode ? `Room ${hostRoomCode}` : ""} • ${label}`.trim();
      }
      if (hostSyncActive) {
        const reason = String(msg.reason || "");
        const actorId = String(msg.actor_participant_id || "");
        const isSelfActor = Boolean(actorId && hostParticipantId && actorId === String(hostParticipantId));
        const bufferingPauseChanged =
          Boolean(roomState.paused_for_buffering) !== Boolean(prevState?.paused_for_buffering);
        const playStateChanged =
          Boolean(roomState.is_playing) !== Boolean(prevState?.is_playing);
        const nextActiveId = roomState.active_media_id ? String(roomState.active_media_id) : null;
        const trackChanged = Boolean(nextActiveId && nextActiveId !== String(state.cinemaActiveId || ""));

        if (reason === "buffering" && !bufferingPauseChanged && !playStateChanged && !trackChanged) {
          // Participant buffering indicator update only — never disturb host playback.
        } else if (isSelfActor && reason !== "buffering" && reason !== "select") {
          // Echo of this host tab's own play/pause/seek/rate command — never fight local video.
          if (reason === "seek" && hostPendingSeekTarget !== null) {
            const echoedPos = Number(roomState.position_sec ?? roomState.effective_position_sec ?? NaN);
            if (Number.isFinite(echoedPos) && Math.abs(echoedPos - hostPendingSeekTarget) <= 0.5) {
              hostPendingSeekTarget = null;
              hostLocalActionLockUntil = Math.min(hostLocalActionLockUntil, performance.now() + 250);
            }
          }
        } else if (bufferingPauseChanged) {
          // Wait-for-buffering paused or resumed the room: pause without seeking; resume without gratuitous seek.
          applyCinemaSyncState(roomState, {
            reason,
            allowSeek: !roomState.paused_for_buffering,
          });
        } else if (hostControlMode === "everyone") {
          if (!isHostUserSeeking() || reason === "select" || reason === "seek") {
            applyCinemaSyncState(roomState, { reason });
          }
        } else {
          // In host_only mode, only apply genuine remote host changes (e.g. another host tab or queue Play Now).
          const video = hostCinemaVideo();
          const videoPlayMismatch = Boolean(video && Boolean(roomState.is_playing) !== !video.paused);
          const expectedPos = getHostExpectedPosition(roomState, video?.currentTime ?? 0);
          const seekThresh = reason === "seek" ? HOST_SEEK_ALIGN_SEC : HOST_DRIFT_THRESHOLD_SEC;
          const posDrifted = Boolean(
            video &&
            !isHostUserSeeking() &&
            Math.abs((Number(video.currentTime) || 0) - expectedPos) > seekThresh
          );
          const rateChanged = Boolean(
            video &&
            !isHostUserSeeking() &&
            Math.abs((Number(video.playbackRate) || 1) - (Number(roomState.playback_rate || 1) || 1)) > 0.01
          );
          if (trackChanged || (!isHostUserSeeking() && (videoPlayMismatch || posDrifted || rateChanged))) {
            applyCinemaSyncState(roomState, { reason });
          }
        }
      }
    }
    updateHostPartyButton(roomState?.participant_count);
    return;
  }
  if (msg.type === "presence") {
    if (Array.isArray(msg.participants) && hostState) {
      hostState.participants = msg.participants;
      renderHostParticipants(msg.participants);
      updateHostPartyButton(msg.participant_count ?? hostState.participants.length);
    }
    return;
  }
  if (msg.type === "room_closed") {
    hostIntentionalClose = true;
    handleHostPermanentDisconnect(String(msg.reason || "Watch Party room ended."));
    return;
  }
  if (msg.type === "error") {
    toast(String(msg.message || msg.detail || "Watch party sync error"), { type: "error" });
  }
}

export function notifyCinemaVideoSelected(mediaId) {
  if (typeof hostCinemaSelectedHandler === "function") {
    hostCinemaSelectedHandler(mediaId);
  }
}

function handleCinemaVideoSelected(mediaId) {
  if (!hostSyncActive || !hostRoomCode || !hostToken) return;
  const id = String(mediaId || "");
  const mediaIds = Array.isArray(hostState?.media_ids)
    ? hostState.media_ids
    : (Array.isArray(hostState?.queue_ids) ? hostState.queue_ids : []);
  if (mediaIds.includes(id)) {
    hostWsSend({ type: "select_item", media_id: id });
    return;
  }
  apiFetch(`/api/watch-party/rooms/${encodeURIComponent(hostRoomCode)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ add_media_ids: [id], active_media_id: id }),
  }).then(() => {}).catch(() => {});
}

function hostRoomUrl(path = "") {
  return `/api/watch-party/rooms/${encodeURIComponent(hostRoomCode || "")}${path}`;
}

async function hostUpdateRoom(patch) {
  const res = await apiFetch(hostRoomUrl(), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  hostState = res?.room_state || hostState;
  return res;
}

async function hostKickParticipant(participantId) {
  try {
    await apiFetch(hostRoomUrl("/kick"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ participant_id: String(participantId) }),
    });
    toast("Participant removed", { type: "info" });
  } catch (err) {
    toast(`Kick failed: ${err?.message || err}`, { type: "error" });
  }
}

async function hostEndRoom() {
  if (!hostRoomCode) return;
  const confirmed = await confirmDialog({
    title: "End Watch Party",
    message: "End this Watch Party? Connected guests will be disconnected.",
    confirmLabel: "End Party",
    cancelLabel: "Keep Party",
    danger: true,
  });
  if (!confirmed) return;
  try {
    await apiFetch(hostRoomUrl(), { method: "DELETE" });
  } catch (err) {
    toast(`Could not end Watch Party: ${err?.message || err}`, { type: "error" });
    return;
  }
  closeHostWs();
  hostSyncActive = false;
  setHostSyncToggle(false);
  hostRoomCode = null;
  hostToken = null;
  hostState = null;
  hostParticipantId = null;
  const btn = $("cinemaWatchPartyBtn");
  if (btn) {
    btn.classList.remove("is-active");
    btn.textContent = "Watch Party";
  }
  closeHostDialog();
}

async function hostCopyInvite() {
  const url = hostInviteUrl();
  if (!url) return;
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(url);
    } else {
      const ta = document.createElement("textarea");
      ta.value = url;
      ta.setAttribute("readonly", "");
      ta.className = "wp-sr-copy";
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
    }
    toast("Invite link copied", { type: "success" });
  } catch (_) {
    toast("Could not copy invite link", { type: "error" });
  }
}

function hostOpenPartyRoom() {
  if (!hostRoomCode || !hostToken) return;
  const code = hostRoomCode;
  const payload = {
    token: hostToken,
    role: "host",
    participant_id: hostParticipantId,
    display_name: hostDisplayName,
  };
  const handoffKey = `wp_handoff_${code}`;
  try {
    sessionStorage.setItem(`wp_session_${code}`, JSON.stringify(payload));
  } catch (_) {}
  try {
    localStorage.setItem(
      handoffKey,
      JSON.stringify({ ...payload, exp: Date.now() + 15000 })
    );
    setTimeout(() => {
      try {
        localStorage.removeItem(handoffKey);
      } catch (_) {}
    }, 15000);
  } catch (_) {}
  window.open(`/watch/${encodeURIComponent(code)}`, "_blank");
}

function hostDefaultRoomTitle() {
  const activeId = state.cinemaActiveId;
  if (!activeId) return "Movie Night";
  const active = (state.mediaItems || []).find((it) => String(it.id) === String(activeId));
  const title = String((active && (active.title || active.filename)) || "").trim();
  return title || "Movie Night";
}

function buildHostField(labelText, id, opts = {}) {
  const field = document.createElement("label");
  field.className = "modal-field";
  const lbl = document.createElement("span");
  lbl.textContent = labelText;
  const input = document.createElement("input");
  input.id = id;
  input.type = opts.type || "text";
  if (opts.placeholder) input.placeholder = opts.placeholder;
  input.value = opts.value != null ? String(opts.value) : "";
  if (opts.maxLength) input.maxLength = opts.maxLength;
  field.append(lbl, input);
  return { field, input };
}

function buildHostSelect(labelText, id, value, options) {
  const field = document.createElement("label");
  field.className = "modal-field";
  const lbl = document.createElement("span");
  lbl.textContent = labelText;
  const select = document.createElement("select");
  select.id = id;
  for (const opt of options) {
    const o = document.createElement("option");
    o.value = opt.value;
    o.textContent = opt.label;
    select.append(o);
  }
  select.value = value;
  field.append(lbl, select);
  return { field, select };
}

function buildHostCreateView() {
  const view = document.createElement("section");
  view.id = "wpHostCreateView";
  view.className = "wp-host-view";

  const grid = document.createElement("div");
  grid.className = "wp-host-grid";

  const titleField = buildHostField("Room Title", "wpHostCreateTitleInput", { value: hostDefaultRoomTitle() });
  const nameField = buildHostField("Host Display Name", "wpHostCreateNameInput", { value: "Host" });
  const pinField = buildHostField("Room PIN (optional)", "wpHostCreatePinInput", {
    type: "password",
    placeholder: "Optional PIN (leave blank for open)",
  });
  const modeField = buildHostSelect("Control Mode", "wpHostCreateControlMode", "host_only", [
    { value: "host_only", label: "Host only" },
    { value: "everyone", label: "Everyone can control" },
  ]);
  const ttlField = buildHostSelect("Room Duration", "wpHostCreateTtl", "21600", [
    { value: "7200", label: "2 hours (7200)" },
    { value: "21600", label: "6 hours (21600, default)" },
    { value: "43200", label: "12 hours (43200)" },
    { value: "86400", label: "24 hours (86400)" },
  ]);

  grid.append(titleField.field, nameField.field, pinField.field, modeField.field, ttlField.field);

  const waitWrap = document.createElement("label");
  waitWrap.className = "checkbox-label";
  const waitCheck = document.createElement("input");
  waitCheck.type = "checkbox";
  waitCheck.id = "wpHostCreateWaitBufferingCheck";
  const waitLbl = document.createElement("span");
  waitLbl.textContent = "Wait for buffering peers";
  waitWrap.append(waitCheck, waitLbl);

  const queueField = buildHostSelect("Queue", "wpHostCreateQueue", "all", [
    { value: "all", label: "All videos in current Cinema queue" },
    { value: "current", label: "Current video only" },
  ]);

  const actions = document.createElement("div");
  actions.className = "toolbar-group modal-actions-end";

  const cancelBtn = document.createElement("button");
  cancelBtn.type = "button";
  cancelBtn.className = "secondary";
  cancelBtn.id = "wpHostCreateCancelBtn";
  cancelBtn.textContent = "Cancel";

  const submitBtn = document.createElement("button");
  submitBtn.type = "submit";
  submitBtn.id = "wpHostCreateSubmitBtn";
  submitBtn.textContent = "Start Watch Party";

  actions.append(cancelBtn, submitBtn);

  const form = document.createElement("form");
  form.id = "wpHostCreateForm";
  form.className = "wp-host-form";
  form.append(grid, waitWrap, queueField.field, actions);

  view.append(form);
  return view;
}

function buildHostManageView() {
  const view = document.createElement("section");
  view.id = "wpHostManageView";
  view.className = "wp-host-view";
  view.hidden = true;

  const metaRow = document.createElement("div");
  metaRow.className = "wp-host-row";
  const metaMain = document.createElement("div");
  const metaTitle = document.createElement("div");
  metaTitle.id = "wpHostRoomMetaTitle";
  metaTitle.className = "wp-host-name";
  const metaStatus = document.createElement("div");
  metaStatus.id = "wpHostRoomMetaStatus";
  metaStatus.className = "wp-host-meta";
  metaStatus.textContent = "Paused";
  metaMain.append(metaTitle, metaStatus);
  metaRow.append(metaMain);

  const inviteRow = document.createElement("div");
  inviteRow.className = "wp-host-invite-row";
  const inviteInput = document.createElement("input");
  inviteInput.id = "wpHostInviteLink";
  inviteInput.type = "text";
  inviteInput.readOnly = true;
  const copyBtn = document.createElement("button");
  copyBtn.type = "button";
  copyBtn.className = "secondary btn-sm";
  copyBtn.id = "wpHostCopyBtn";
  copyBtn.textContent = "Copy Link";
  const openBtn = document.createElement("button");
  openBtn.type = "button";
  openBtn.className = "secondary btn-sm";
  openBtn.id = "wpHostOpenBtn";
  openBtn.textContent = "Open Party Room";
  inviteRow.append(inviteInput, copyBtn, openBtn);

  const qrRow = document.createElement("div");
  qrRow.id = "wpHostQrRow";
  qrRow.className = "wp-host-qr-row";
  qrRow.hidden = true;
  const qrBox = document.createElement("div");
  qrBox.id = "wpHostQrBox";
  qrBox.className = "wp-host-qr-box";
  const qrText = document.createElement("div");
  const qrTitle = document.createElement("div");
  qrTitle.className = "wp-host-name";
  qrTitle.textContent = "Scan to join on phone";
  const qrSub = document.createElement("div");
  qrSub.className = "wp-host-meta";
  qrSub.textContent = "Point a phone camera at this QR code to open the Watch Party invite URL.";
  qrText.append(qrTitle, qrSub);
  qrRow.append(qrBox, qrText);

  const syncRow = document.createElement("div");
  syncRow.className = "wp-host-row";
  const syncMain = document.createElement("div");
  const syncTitle = document.createElement("div");
  syncTitle.className = "wp-host-name";
  syncTitle.textContent = "Sync Cinema Player";
  const syncSub = document.createElement("div");
  syncSub.className = "wp-host-meta";
  syncSub.textContent = "Push host playback to room guests";
  syncMain.append(syncTitle, syncSub);
  const syncToggle = document.createElement("button");
  syncToggle.type = "button";
  syncToggle.id = "wpHostSyncToggle";
  syncToggle.className = "secondary btn-sm";
  syncToggle.textContent = "Sync: Off";
  syncToggle.setAttribute("aria-pressed", "false");
  syncRow.append(syncMain, syncToggle);

  const settingsRow = document.createElement("div");
  settingsRow.className = "wp-host-row";
  const modeWrap = document.createElement("label");
  modeWrap.className = "modal-field";
  const modeLbl = document.createElement("span");
  modeLbl.textContent = "Control Mode";
  const modeSelect = document.createElement("select");
  modeSelect.id = "wpHostControlMode";
  const modeOptHost = document.createElement("option");
  modeOptHost.value = "host_only";
  modeOptHost.textContent = "Host only";
  const modeOptEveryone = document.createElement("option");
  modeOptEveryone.value = "everyone";
  modeOptEveryone.textContent = "Everyone can control";
  modeSelect.append(modeOptHost, modeOptEveryone);
  modeSelect.value = hostControlMode;
  modeWrap.append(modeLbl, modeSelect);
  const waitWrap = document.createElement("label");
  waitWrap.className = "checkbox-label";
  const waitCheck = document.createElement("input");
  waitCheck.type = "checkbox";
  waitCheck.id = "wpHostWaitBufferingCheck";
  const waitLbl = document.createElement("span");
  waitLbl.textContent = "Wait for buffering";
  waitWrap.append(waitCheck, waitLbl);
  settingsRow.append(modeWrap, waitWrap);

  const addRow = document.createElement("div");
  addRow.className = "wp-host-row";
  const addMain = document.createElement("div");
  const addTitle = document.createElement("div");
  addTitle.className = "wp-host-name";
  addTitle.textContent = "Add the active Cinema video to this room";
  const addSub = document.createElement("div");
  addSub.className = "wp-host-meta";
  addSub.textContent = "Queue the currently playing Cinema video";
  addMain.append(addTitle, addSub);
  const addBtn = document.createElement("button");
  addBtn.type = "button";
  addBtn.className = "secondary btn-sm";
  addBtn.id = "wpHostAddActiveBtn";
  addBtn.textContent = "Add Active Cinema Video";
  addRow.append(addMain, addBtn);

  const queueCard = document.createElement("div");
  queueCard.className = "wp-host-queue-card";
  const queueTitle = document.createElement("div");
  queueTitle.className = "wp-host-queue-title";
  queueTitle.textContent = "Room Queue";
  const queueList = document.createElement("div");
  queueList.id = "wpHostQueueList";
  queueList.className = "wp-host-list";
  queueCard.append(queueTitle, queueList);

  const participantsCard = document.createElement("div");
  participantsCard.className = "wp-host-queue-card";
  const participantsTitle = document.createElement("div");
  participantsTitle.className = "wp-host-queue-title";
  participantsTitle.textContent = "Participants";
  const participantsList = document.createElement("div");
  participantsList.id = "wpHostParticipantsList";
  participantsList.className = "wp-host-list";
  participantsCard.append(participantsTitle, participantsList);

  const endRow = document.createElement("div");
  endRow.className = "wp-host-row";
  const endMain = document.createElement("div");
  const endTitle = document.createElement("div");
  endTitle.className = "wp-host-name";
  endTitle.textContent = "End this Watch Party";
  const endSub = document.createElement("div");
  endSub.className = "wp-host-meta";
  endSub.textContent = "Guests will be disconnected immediately";
  endMain.append(endTitle, endSub);
  const endBtn = document.createElement("button");
  endBtn.type = "button";
  endBtn.className = "danger";
  endBtn.id = "wpHostEndBtn";
  endBtn.textContent = "End Watch Party";
  endRow.append(endMain, endBtn);

  view.append(
    metaRow,
    inviteRow,
    qrRow,
    syncRow,
    settingsRow,
    addRow,
    queueCard,
    participantsCard,
    endRow,
  );
  return view;
}

function buildHostDialog() {
  const dialog = document.createElement("dialog");
  dialog.className = "wp-host-dialog";
  dialog.id = "wpHostDialog";

  const surface = document.createElement("div");
  surface.className = "dialog-surface";

  const head = document.createElement("div");
  head.className = "modal-head";
  const strong = document.createElement("strong");
  strong.id = "wpHostDialogTitle";
  strong.textContent = "Watch Party";
  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "secondary btn-sm dialog-x-btn";
  closeBtn.textContent = "Close";
  head.append(strong, closeBtn);

  const body = document.createElement("div");
  body.className = "modal-body wp-host-body";
  body.append(buildHostCreateView(), buildHostManageView());

  const roomsCard = document.createElement("div");
  roomsCard.className = "wp-host-queue-card";
  const roomsTitle = document.createElement("div");
  roomsTitle.className = "wp-host-queue-title";
  roomsTitle.textContent = "Active Watch Parties";
  const roomsList = document.createElement("div");
  roomsList.id = "wpHostRoomsList";
  roomsList.className = "wp-host-list";
  roomsCard.append(roomsTitle, roomsList);
  body.append(roomsCard);

  surface.append(head, body);
  dialog.appendChild(surface);

  return dialog;
}

function closeHostDialog() {
  if (hostDialogEl && hostDialogEl.open) {
    try { hostDialogEl.close(); } catch (_) {}
  }
  if (hostDialogEl && hostDialogEl.isConnected) hostDialogEl.remove();
  hostDialogEl = null;
}

function wireHostDialog(dialog) {
  dialog.addEventListener("cancel", (ev) => {
    ev.preventDefault();
    closeHostDialog();
  });
  dialog.addEventListener("click", (ev) => {
    if (ev.target === dialog) closeHostDialog();
  });
  const closeX = dialog.querySelector(".dialog-x-btn");
  if (closeX) closeX.addEventListener("click", () => closeHostDialog());
  const cancelCreate = dialog.querySelector("#wpHostCreateCancelBtn");
  if (cancelCreate) cancelCreate.addEventListener("click", () => closeHostDialog());

  const syncToggle = dialog.querySelector("#wpHostSyncToggle");
  if (syncToggle) syncToggle.addEventListener("click", () => { void onHostSyncToggle(); });

  const controlModeSelect = dialog.querySelector("#wpHostControlMode");
  if (controlModeSelect) {
    controlModeSelect.addEventListener("change", (ev) => { void onHostControlModeChange(ev); });
  }

  const waitCheck = dialog.querySelector("#wpHostWaitBufferingCheck");
  if (waitCheck) {
    waitCheck.addEventListener("change", (ev) => { void onHostWaitBufferingChange(ev); });
  }

  const addBtn = dialog.querySelector("#wpHostAddActiveBtn");
  if (addBtn) addBtn.addEventListener("click", () => { void hostAddActiveCinemaVideo(); });

  const endBtn = dialog.querySelector("#wpHostEndBtn");
  if (endBtn) endBtn.addEventListener("click", () => { void hostEndRoom(); });

  const copyBtn = dialog.querySelector("#wpHostCopyBtn");
  if (copyBtn) copyBtn.addEventListener("click", () => { void hostCopyInvite(); });

  const openBtn = dialog.querySelector("#wpHostOpenBtn");
  if (openBtn) openBtn.addEventListener("click", () => hostOpenPartyRoom());

  const createForm = dialog.querySelector("#wpHostCreateForm");
  if (createForm) {
    createForm.addEventListener("submit", (ev) => {
      ev.preventDefault();
      void startWatchParty(ev).catch((err) => {
        toast(`Could not start Watch Party: ${err?.message || err}`, { type: "error" });
      });
    });
  }
}

async function openHostModal() {
  if (hostDialogEl && hostDialogEl.isConnected) hostDialogEl.remove();
  const dialog = buildHostDialog();
  document.body.appendChild(dialog);
  hostDialogEl = dialog;
  wireHostDialog(dialog);
  if (hostRoomCode && hostToken) {
    enterHostManageView();
  }
  try {
    await dialog.showModal();
  } catch (_) {
    dialog.remove();
    if (hostDialogEl === dialog) hostDialogEl = null;
    return;
  }
  await loadHostRooms();
}

function renderHostQueue(list) {
  const container = $("wpHostQueueList");
  if (!container) return;
  container.replaceChildren();
  const items = Array.isArray(list) ? list : [];
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "wp-host-empty";
    empty.textContent = "Queue is empty.";
    container.appendChild(empty);
    return;
  }
  for (const item of items) {
    const mediaId = String(item?.media_id || item?.id || "");
    if (!mediaId) continue;
    const row = document.createElement("div");
    row.className = "wp-host-row";

    const info = document.createElement("div");
    const title = document.createElement("div");
    title.className = "wp-host-name";
    title.textContent = String(item?.title || item?.name || "Untitled");
    const meta = document.createElement("div");
    meta.className = "wp-host-meta";
    const bits = [];
    if (item?.duration_sec) bits.push(fmtDuration(item.duration_sec));
    const res = fmtResolution(item, { exact: false });
    if (res) bits.push(res);
    meta.textContent = bits.join(" • ");
    info.append(title, meta);

    const playBtn = document.createElement("button");
    playBtn.type = "button";
    playBtn.className = "secondary btn-xs";
    playBtn.textContent = "Play Now";
    playBtn.addEventListener("click", () => { void hostQueuePlay(mediaId); });

    const removeBtn = document.createElement("button");
    removeBtn.type = "button";
    removeBtn.className = "danger btn-xs";
    removeBtn.textContent = "Remove";
    removeBtn.addEventListener("click", () => { void hostQueueRemove(mediaId); });

    row.append(info, playBtn, removeBtn);
    container.appendChild(row);
  }
}

function renderHostParticipants(list) {
  const container = $("wpHostParticipantsList");
  if (!container) return;
  container.replaceChildren();
  const items = Array.isArray(list) ? list : [];
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "wp-host-empty";
    empty.textContent = "No viewers yet.";
    container.appendChild(empty);
    return;
  }
  for (const p of items) {
    const row = document.createElement("div");
    row.className = "wp-host-row";

    const dot = document.createElement("span");
    dot.className = `wp-host-dot${p?.is_buffering ? " is-buffering" : ""}`;
    dot.setAttribute("aria-hidden", "true");

    const main = document.createElement("div");
    const name = document.createElement("div");
    name.className = "wp-host-name";
    name.textContent = String(p?.display_name || "Guest");
    const meta = document.createElement("div");
    meta.className = "wp-host-meta";
    const bits = [];
    if (p?.role) bits.push(String(p.role) === "host" ? "Host" : "Guest");
    if (p?.is_buffering) bits.push("Buffering");
    if (hostParticipantId && String(p?.participant_id) === String(hostParticipantId)) bits.push("You");
    meta.textContent = bits.join(" • ");
    main.append(name, meta);

    row.append(dot, main);

    const isSelf = Boolean(hostParticipantId) && String(p?.participant_id) === String(hostParticipantId);
    const isHostRow = String(p?.role) === "host";
    if (hostSyncActive && !isSelf && !isHostRow) {
      const kickBtn = document.createElement("button");
      kickBtn.type = "button";
      kickBtn.className = "secondary btn-xs";
      kickBtn.textContent = "Kick";
      kickBtn.addEventListener("click", () => { void hostKickParticipant(p?.participant_id); });
      row.appendChild(kickBtn);
    }
    container.appendChild(row);
  }
}

async function loadHostRooms() {
  const listEl = $("wpHostRoomsList");
  if (!listEl) return;
  try {
    const data = await apiFetch("/api/watch-party/rooms");
    const rooms = Array.isArray(data?.rooms) ? data.rooms : [];
    listEl.replaceChildren();
    if (!rooms.length) {
      const empty = document.createElement("div");
      empty.className = "wp-host-empty";
      empty.textContent = "No active Watch Parties.";
      listEl.appendChild(empty);
      return;
    }
    for (const room of rooms) {
      const row = document.createElement("div");
      row.className = "wp-host-row";
      const info = document.createElement("div");
      const title = document.createElement("div");
      title.className = "wp-host-name";
      title.textContent = String(room?.title || "Watch Party");
      const meta = document.createElement("div");
      meta.className = "wp-host-meta";
      const bits = [];
      if (room?.room_code) bits.push(String(room.room_code));
      if (room?.is_playing) bits.push("Playing");
      meta.textContent = bits.join(" • ");
      info.append(title, meta);
      const manageBtn = document.createElement("button");
      manageBtn.type = "button";
      manageBtn.className = "secondary btn-sm";
      manageBtn.textContent = "Manage";
      manageBtn.addEventListener("click", () => { void resumeHostRoom(String(room?.room_code || "")); });
      row.append(info, manageBtn);
      listEl.appendChild(row);
    }
  } catch (err) {
    toast(`Could not load Watch Parties: ${err?.message || err}`, { type: "error" });
  }
}

async function resumeHostRoom(code) {
  if (!code) return;
  const displayName = String($("wpHostCreateNameInput")?.value || hostDisplayName || "Host");
  const data = await apiFetch(`/api/watch-party/rooms/${encodeURIComponent(code)}/host-session`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ display_name: displayName }),
  });
  hostRoomCode = String(data?.room_code || code);
  hostToken = String(data?.host_token || "");
  hostParticipantId = String(data?.participant_id || "");
  hostDisplayName = String(data?.display_name || displayName);
  hostState = data?.room_state || null;
  hostControlMode = String(hostState?.control_mode || "host_only");
  hostWaitForBuffering = Boolean(hostState?.wait_for_buffering);
  hostSyncActive = true;
  hostReconnectAttempt = 0;
  setHostSyncToggle(true);
  bindHostCinemaSync(hostCinemaVideo());
  connectHostWs();
  updateHostPartyButton(hostState?.participant_count);
  enterHostManageView();
}

function enterHostManageView() {
  const createView = $("wpHostCreateView");
  const manageView = $("wpHostManageView");
  if (createView) createView.hidden = true;
  if (manageView) manageView.hidden = false;

  const metaTitle = $("wpHostRoomMetaTitle");
  const metaStatus = $("wpHostRoomMetaStatus");
  const inviteEl = $("wpHostInviteLink");
  if (metaTitle) metaTitle.textContent = String((hostState && hostState.title) || "Watch Party");
  if (metaStatus) {
    const label = hostState?.paused_for_buffering
      ? "Waiting for buffering"
      : (hostState?.is_playing ? "Playing" : "Paused");
    metaStatus.textContent = `${hostRoomCode ? `Room ${hostRoomCode}` : ""} • ${label}`.trim();
  }
  const inviteUrl = hostInviteUrl();
  if (inviteEl) inviteEl.value = inviteUrl;

  const qrRow = $("wpHostQrRow");
  const qrBox = $("wpHostQrBox");
  if (qrRow && qrBox) {
    qrBox.replaceChildren();
    const svg = inviteUrl ? renderQrSvg(inviteUrl, { label: `QR code for ${inviteUrl}` }) : null;
    if (svg) {
      qrBox.appendChild(svg);
      qrRow.hidden = false;
    } else {
      qrRow.hidden = true;
    }
  }

  const modeEl = $("wpHostControlMode");
  if (modeEl) modeEl.value = hostControlMode === "everyone" ? "everyone" : "host_only";

  const waitEl = $("wpHostWaitBufferingCheck");
  if (waitEl) waitEl.checked = hostWaitForBuffering;

  const syncToggle = $("wpHostSyncToggle");
  if (syncToggle) {
    syncToggle.textContent = hostSyncActive ? "Sync: On" : "Sync: Off";
    syncToggle.setAttribute("aria-pressed", String(hostSyncActive));
  }

  if (hostState) {
    renderHostQueue(hostState.queue);
    renderHostParticipants(hostState.participants);
    updateHostPartyButton(hostState.participant_count);
  }
  syncAddActiveBtnVisibility();
}

function onHostSyncToggle() {
  if (!hostRoomCode) return;
  if (hostSyncActive) {
    closeHostWs({ intentional: true });
    hostSyncActive = false;
    setHostSyncToggle(false);
    updateHostPartyButton(hostState?.participant_count);
    return;
  }
  hostSyncActive = true;
  hostReconnectAttempt = 0;
  bindHostCinemaSync(hostCinemaVideo());
  connectHostWs();
  setHostSyncToggle(true);
  updateHostPartyButton(hostState?.participant_count);
}

async function onHostControlModeChange(ev) {
  const value = ev?.target?.value === "everyone" ? "everyone" : "host_only";
  hostControlMode = value;
  if (!hostRoomCode) return;
  try {
    await hostUpdateRoom({ control_mode: value });
  } catch (err) {
    toast(`Could not update Control Mode: ${err?.message || err}`, { type: "error" });
  }
}

async function onHostWaitBufferingChange(ev) {
  const value = Boolean(ev?.target?.checked);
  hostWaitForBuffering = value;
  if (!hostRoomCode) return;
  try {
    await hostUpdateRoom({ wait_for_buffering: value });
  } catch (err) {
    toast(`Could not update Wait for Buffering: ${err?.message || err}`, { type: "error" });
  }
}

function startWatchParty(ev) {
  if (ev && typeof ev.preventDefault === "function") ev.preventDefault();
  const title = String($("wpHostCreateTitleInput")?.value || "").trim() || "Movie Night";
  const displayName = String($("wpHostCreateNameInput")?.value || "").trim() || "Host";
  const pinRaw = String($("wpHostCreatePinInput")?.value || "").trim();
  const pin = pinRaw ? pinRaw : null;
  const controlMode = $("wpHostCreateControlMode")?.value === "everyone" ? "everyone" : "host_only";
  const waitForBuffering = Boolean($("wpHostCreateWaitBufferingCheck")?.checked);
  const ttlSec = Number($("wpHostCreateTtl")?.value) || 21600;
  const queueMode = String($("wpHostCreateQueue")?.value) || "all";
  const includeAll = queueMode !== "current";

  const video = hostCinemaVideo();
  const activeId = state.cinemaActiveId ? String(state.cinemaActiveId) : null;
  const mediaIds = [];
  const pushId = (id) => {
    const s = String(id || "");
    if (s && !mediaIds.includes(s)) mediaIds.push(s);
  };
  if (activeId) pushId(activeId);
  if (includeAll) {
    for (const it of hostCinemaVideos()) {
      if (mediaIds.length >= HOST_QUEUE_LIMIT) break;
      pushId(it?.id);
    }
  }

  const body = {
    title,
    media_ids: mediaIds,
    active_media_id: activeId,
    pin,
    control_mode: controlMode,
    wait_for_buffering: waitForBuffering,
    ttl_sec: ttlSec,
    host_name: displayName,
    initial_position_sec: Number(video?.currentTime || 0) || 0,
    initial_is_playing: Boolean(video && !video.paused),
    initial_playback_rate: Number(video?.playbackRate || 1) || 1,
  };

  return apiFetch("/api/watch-party/rooms", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then(async (data) => {
    hostRoomCode = String(data?.room_code || "");
    hostToken = String(data?.host_token || "");
    hostParticipantId = String(data?.host_participant_id || "");
    hostDisplayName = String(data?.host_name || displayName);
    hostState = data?.room_state || null;
    hostControlMode = String(body.control_mode || "host_only");
    hostWaitForBuffering = Boolean(body.wait_for_buffering);
    hostSyncActive = true;
    hostReconnectAttempt = 0;
    setHostSyncToggle(true);
    bindHostCinemaSync(hostCinemaVideo());
    connectHostWs();
    updateHostPartyButton(hostState?.participant_count);
    enterHostManageView();
  });
}

function hostQueuePlay(mediaId) {
  return hostUpdateRoom({ active_media_id: String(mediaId) }).then(() => {
    if (hostState) applyCinemaSyncState(hostState);
  });
}

function hostQueueRemove(mediaId) {
  return hostUpdateRoom({ remove_media_ids: [String(mediaId)] })
    .then(() => {
      if (hostState) renderHostQueue(hostState.queue);
      syncAddActiveBtnVisibility();
    })
    .catch(() => {});
}

function hostAddActiveCinemaVideo() {
  const activeId = state.cinemaActiveId ? String(state.cinemaActiveId) : null;
  if (!hostRoomCode || !activeId) return Promise.resolve(false);
  return hostUpdateRoom({ add_media_ids: [activeId], active_media_id: activeId }).then(() => {
    if (hostState) renderHostQueue(hostState.queue);
    syncAddActiveBtnVisibility();
    return true;
  });
}

export function initWatchPartyHost({ getCinemaVideos, playCinemaVideo, getVideoElement } = {}) {
  hostDeps = { getCinemaVideos, playCinemaVideo, getVideoElement };
  hostCinemaSelectedHandler = (mediaId) => handleCinemaVideoSelected(mediaId);

  const btn = $("cinemaWatchPartyBtn");
  if (btn) {
    btn.addEventListener("click", () => { void openHostModal(); });
  }
  return () => {
    hostCinemaSelectedHandler = null;
    closeHostWs();
    if (hostDialogEl && hostDialogEl.isConnected) hostDialogEl.remove();
    hostDialogEl = null;
  };
}
