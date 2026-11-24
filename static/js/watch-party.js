import { $, escapeHtml, fmtDuration, fmtResolution } from "/static/js/utils.js";

const ROOM_CODE_RE = /^\/watch\/([^/?#]+)/;
const SYNC_INTERVAL_MS = 1000;
const PING_INTERVAL_MS = 15000;
const PING_BURST_COUNT = 3;
const PING_BURST_GAP_MS = 250;
const SEEK_DEBOUNCE_MS = 150;
const SUPPRESS_MS = 450;
const DRIFT_SYNCED_SEC = 0.45;
const DRIFT_NUDGE_SEC = 1.5;
const BUFFERING_REPORT_DELAY_MS = 650;
const BUFFERING_RESUME_AHEAD_SEC = 1.5;
const BUFFERING_CHECK_INTERVAL_MS = 300;
const BUFFERING_READY_FALLBACK_MS = 1500;
const POST_RESUME_GRACE_MS = 1200;
const LOCAL_SEEK_LOCK_MS = 1500;
const RECONNECT_DELAYS = [1000, 2000, 4000, 8000, 15000];
const NO_RECONNECT_CODES = new Set([4001, 4003, 4004, 4005]);
const MAX_CHAT_LEN = 500;

let roomCode = "";
let session = null; // {token, role, participant_id, display_name}
let roomState = null;
let chatHistory = [];
let ws = null;
let intentionalClose = false;
let reconnectAttempt = 0;
let reconnectTimer = null;
let pingInterval = null;
let pingBurstTimers = [];
let syncTimer = null;
let seekSendTimer = null;
let lastStateSeq = -1;
let clockOffsetMs = 0;
let clockSamples = 0;
let stateReceivedPerfMs = 0;
let suppressLocalUntil = 0;
let isApplyingSync = false;
let lastBufferingSent = false;
let bufferingReportTimer = null;
let bufferingReadyTimer = null;
let bufferingReportedAt = 0;
let postResumeGraceUntil = 0;
let userSeeking = false;
let pendingSeekTargetSec = null;
let localSeekLockUntil = 0;
let programmaticSeekTarget = null;
let programmaticSeekUntil = 0;
let lastTimeUpdatePos = -1;
let lastControlToastAt = 0;
let activeMediaId = null;
let pendingMetadataSeekHandler = null;
let lastAutoAdvancedFrom = null;

function ssKey() {
  return `wp_session_${roomCode}`;
}

function loadSession() {
  const handoffKey = `wp_handoff_${roomCode}`;
  try {
    const handoffRaw = localStorage.getItem(handoffKey);
    if (handoffRaw) {
      localStorage.removeItem(handoffKey);
      const handoff = JSON.parse(handoffRaw);
      if (
        handoff &&
        typeof handoff.token === "string" &&
        handoff.token &&
        (!handoff.exp || Number(handoff.exp) >= Date.now())
      ) {
        const adopted = {
          token: String(handoff.token),
          role: String(handoff.role || "host"),
          participant_id: String(handoff.participant_id || ""),
          display_name: String(handoff.display_name || "Host"),
        };
        try {
          sessionStorage.setItem(ssKey(), JSON.stringify(adopted));
        } catch (_) {}
        return adopted;
      }
    }
  } catch (_) {}

  try {
    const raw = sessionStorage.getItem(ssKey());
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed.token !== "string" || !parsed.token) return null;
    return {
      token: String(parsed.token),
      role: String(parsed.role || "guest"),
      participant_id: String(parsed.participant_id || ""),
      display_name: String(parsed.display_name || ""),
    };
  } catch (_) {
    return null;
  }
}

function saveSession(next) {
  session = next;
  try {
    sessionStorage.setItem(ssKey(), JSON.stringify(next));
  } catch (_) {}
}

function clearSession() {
  session = null;
  try {
    sessionStorage.removeItem(ssKey());
  } catch (_) {}
}

function getRoomCode() {
  const m = window.location.pathname.match(ROOM_CODE_RE);
  if (!m) return "";
  try {
    return decodeURIComponent(m[1]);
  } catch (_) {
    return m[1] || "";
  }
}

function showView(view) {
  const views = ["wpErrorView", "wpJoinView", "wpRoomView"];
  for (const id of views) {
    const el = $(id);
    if (el) el.hidden = id !== view;
  }
}

function setErrorView(title, message) {
  const t = $("wpErrorTitle");
  const m = $("wpErrorMessage");
  if (t) t.textContent = String(title || "Watch Party Error");
  if (m) m.textContent = String(message || "Something went wrong.");
  showView("wpErrorView");
}

function setBadge(el, text, cls) {
  if (!el) return;
  el.textContent = String(text || "");
  el.classList.remove("wp-badge-ok", "wp-badge-warn", "wp-badge-err");
  if (cls) el.classList.add(cls);
}

function showToast(message, kind = "info") {
  const region = $("wpToastRegion");
  if (!region) return;
  const toast = document.createElement("div");
  toast.className = `wp-toast wp-toast-${kind}`;
  toast.setAttribute("role", "status");
  toast.textContent = String(message || "");
  region.appendChild(toast);
  setTimeout(() => {
    if (toast.parentNode) toast.parentNode.removeChild(toast);
  }, 3600);
}

function showControlToast(message) {
  const now = Date.now();
  if (now - lastControlToastAt < 1500) return;
  lastControlToastAt = now;
  showToast(message, "warn");
}

async function fetchJson(url, opts = {}) {
  const res = await fetch(url, opts);
  let data = null;
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (ct.includes("application/json")) {
    try { data = await res.json(); } catch (_) { data = null; }
  } else {
    try {
      const txt = await res.text();
      data = txt ? { detail: txt } : null;
    } catch (_) { data = null; }
  }
  if (!res.ok) {
    const err = new Error((data && (data.detail || data.message || data.error)) || `HTTP ${res.status}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

function roomApi(path) {
  return `/api/watch-party/${encodeURIComponent(roomCode)}${path}`;
}

function mediaStreamUrl(mediaId) {
  if (!session || !session.token || !mediaId) return "";
  return `${roomApi(`/media/${encodeURIComponent(mediaId)}/stream`)}?wp_token=${encodeURIComponent(session.token)}`;
}

function mediaThumbUrl(mediaId) {
  if (!session || !session.token || !mediaId) return "";
  return `${roomApi(`/media/${encodeURIComponent(mediaId)}/thumb`)}?wp_token=${encodeURIComponent(session.token)}`;
}

function wsUrl() {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}${roomApi("/ws")}`;
}

function canControl() {
  if (!session) return false;
  if (session.role === "host") return true;
  return Boolean(roomState && roomState.control_mode === "everyone");
}

function isHost() {
  return Boolean(session && session.role === "host");
}

function updateControlUi() {
  const hostBar = $("wpHostControlsBar");
  if (hostBar) hostBar.hidden = !isHost();
  const endBtn = $("wpEndRoomBtn");
  if (endBtn) endBtn.hidden = !isHost();

  const controlBadge = $("wpControlBadge");
  const mode = roomState && roomState.control_mode === "everyone" ? "Everyone controls" : "Host controls";
  if (controlBadge) controlBadge.textContent = mode;

  const toggleControlsBtn = $("wpToggleControlsBtn");
  if (toggleControlsBtn) {
    toggleControlsBtn.textContent = roomState && roomState.control_mode === "everyone"
      ? "Host Only"
      : "Allow Everyone";
    toggleControlsBtn.disabled = !isHost();
  }
  const toggleWaitBtn = $("wpToggleWaitBufferBtn");
  if (toggleWaitBtn) {
    const on = Boolean(roomState && roomState.wait_for_buffering);
    toggleWaitBtn.textContent = on ? "Wait for Buffering: On" : "Wait for Buffering: Off";
    toggleWaitBtn.disabled = !isHost();
  }

  const chatInput = $("wpChatInput");
  const chatSendBtn = $("wpChatSendBtn");
  if (chatInput) chatInput.disabled = !session;
  if (chatSendBtn) chatSendBtn.disabled = !session;

  const speed = $("wpSpeedSelect");
  if (speed) speed.disabled = !session;
}

function updateTopMeta() {
  const title = $("wpRoomTitle");
  if (title) {
    const name = (roomState && (roomState.title || roomState.room_title)) || "Watch Party";
    title.textContent = String(name);
  }
  const viewerCount = $("wpViewerCountBadge");
  const count = Array.isArray(roomState?.participants) ? roomState.participants.length : (roomState?.viewer_count ?? 0);
  if (viewerCount) viewerCount.textContent = `${count} watching`;
}

function updateNowPlaying() {
  const nowTitle = $("wpNowTitle");
  const nowMeta = $("wpNowMeta");
  const item = getActiveItem();
  if (nowTitle) nowTitle.textContent = item ? String(item.title || item.name || "Untitled") : "Nothing playing";
  if (nowMeta) {
    if (!item) {
      nowMeta.textContent = "Waiting for host to pick something.";
    } else {
      const bits = [];
      if (item.duration_sec) bits.push(fmtDuration(item.duration_sec));
      const res = fmtResolution(item, { exact: false });
      if (res) bits.push(res);
      if (item.folder) bits.push(String(item.folder));
      nowMeta.textContent = bits.join(" • ") || "Ready";
    }
  }
  const empty = $("wpEmptyStage");
  if (empty) empty.hidden = Boolean(item);
}

function getActiveItem() {
  if (!roomState) return null;
  const id = roomState.active_media_id || roomState.active_item_id || roomState.current_media_id;
  const q = Array.isArray(roomState.queue) ? roomState.queue : [];
  if (id) {
    const found = q.find((it) => String(it.media_id || it.id) === String(id));
    if (found) return found;
  }
  return roomState.active_item || null;
}

function renderParticipants() {
  const list = $("wpViewersList");
  const countEl = $("wpViewersCount");
  const participants = Array.isArray(roomState?.participants) ? roomState.participants : [];
  if (countEl) countEl.textContent = String(participants.length);
  if (!list) return;
  list.replaceChildren();
  for (const p of participants) {
    const row = document.createElement("div");
    row.className = "wp-viewer-row";
    if (p.is_buffering) row.classList.add("is-buffering");
    if (p.role === "host") row.classList.add("is-host");

    const main = document.createElement("div");
    main.className = "wp-viewer-main";

    const name = document.createElement("span");
    name.className = "wp-viewer-name";
    const dn = String(p.display_name || "Guest");
    name.textContent = dn;
    main.appendChild(name);

    const meta = document.createElement("span");
    meta.className = "wp-viewer-meta";
    const bits = [];
    if (p.role === "host") bits.push("Host");
    if (p.is_buffering) bits.push("Buffering");
    if (session && String(p.participant_id) === String(session.participant_id)) bits.push("You");
    meta.textContent = bits.join(" • ");
    main.appendChild(meta);

    row.appendChild(main);

    if (isHost() && String(p.participant_id) !== String(session.participant_id)) {
      const kickBtn = document.createElement("button");
      kickBtn.type = "button";
      kickBtn.className = "wp-kick-btn";
      kickBtn.textContent = "Kick";
      kickBtn.addEventListener("click", () => {
        sendWs({ type: "kick", participant_id: String(p.participant_id) });
      });
      row.appendChild(kickBtn);
    }

    list.appendChild(row);
  }
  updateBufferingBanner(participants);
}

function updateBufferingBanner(participants) {
  const banner = $("wpBufferingBanner");
  const text = $("wpBufferingText");
  const buffering = (participants || []).filter((p) => p.is_buffering);
  if (!banner || !text) return;
  const activePlayback = Boolean(roomState?.is_playing || roomState?.paused_for_buffering);
  if (buffering.length === 0 || !activePlayback) {
    banner.hidden = true;
    text.textContent = "";
    return;
  }
  banner.hidden = false;
  const names = buffering.map((p) => String(p.display_name || "Someone")).slice(0, 3);
  const extra = buffering.length - names.length;
  const who = extra > 0 ? `${names.join(", ")} +${extra}` : names.join(", ");
  text.textContent = roomState?.paused_for_buffering
    ? `Waiting for ${who} to buffer...`
    : `${who} buffering...`;
}

function renderQueue() {
  const list = $("wpQueueList");
  const countEl = $("wpQueueCount");
  const queue = Array.isArray(roomState?.queue) ? roomState.queue : [];
  if (countEl) countEl.textContent = String(queue.length);
  if (!list) return;
  list.replaceChildren();
  const activeId = String(roomState?.active_media_id || roomState?.active_item_id || "");
  for (const item of queue) {
    const mediaId = String(item.media_id || item.id || "");
    const row = document.createElement("button");
    row.type = "button";
    row.className = "wp-queue-item";
    if (mediaId && mediaId === activeId) row.classList.add("is-active");
    row.disabled = !session;

    const thumb = document.createElement("img");
    thumb.className = "wp-queue-thumb";
    thumb.alt = "";
    thumb.loading = "lazy";
    if (mediaId) thumb.src = mediaThumbUrl(mediaId);
    row.appendChild(thumb);

    const body = document.createElement("div");
    body.className = "wp-queue-body";
    const t = document.createElement("div");
    t.className = "wp-queue-title";
    t.textContent = String(item.title || item.name || "Untitled");
    body.appendChild(t);

    const meta = document.createElement("div");
    meta.className = "wp-queue-meta";
    const bits = [];
    if (item.duration_sec) bits.push(fmtDuration(item.duration_sec));
    const res = fmtResolution(item, { exact: false });
    if (res) bits.push(res);
    if (item.folder) bits.push(String(item.folder));
    meta.textContent = bits.join(" • ");
    body.appendChild(meta);
    row.appendChild(body);

    row.addEventListener("click", () => {
      if (!canControl()) {
        snapToAuthoritative(true);
        showControlToast("Only the host can control playback");
        return;
      }
      if (!mediaId) return;
      sendWs({ type: "select_item", media_id: mediaId });
    });

    list.appendChild(row);
  }
}

function appendChatMessage(msg, { scroll = true } = {}) {
  const wrap = $("wpChatMessages");
  if (!wrap) return;
  const nearBottom = (wrap.scrollHeight - wrap.scrollTop - wrap.clientHeight) < 80;

  const row = document.createElement("div");
  row.className = "wp-chat-msg";
  if (msg && msg.kind === "system") row.classList.add("is-system");
  if (session && msg && String(msg.participant_id || "") === String(session.participant_id)) {
    row.classList.add("is-self");
  }

  const head = document.createElement("div");
  head.className = "wp-chat-head";
  const author = document.createElement("span");
  author.className = "wp-chat-author";
  author.textContent = String(msg?.display_name || (msg?.kind === "system" ? "System" : "Guest"));
  head.appendChild(author);

  const time = document.createElement("span");
  time.className = "wp-chat-time";
  const ts = Number(msg?.ts || msg?.timestamp || msg?.created_at_ms || 0);
  if (ts > 0) {
    const d = new Date(ts);
    if (!Number.isNaN(d.getTime())) {
      time.textContent = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    }
  }
  head.appendChild(time);
  row.appendChild(head);

  const body = document.createElement("div");
  body.className = "wp-chat-body";
  body.textContent = String(msg?.text || "");
  row.appendChild(body);

  wrap.appendChild(row);
  if (scroll && nearBottom) {
    wrap.scrollTop = wrap.scrollHeight;
  }
}

function renderChatHistory() {
  const wrap = $("wpChatMessages");
  if (!wrap) return;
  wrap.replaceChildren();
  for (const msg of chatHistory) appendChatMessage(msg, { scroll: false });
  wrap.scrollTop = wrap.scrollHeight;
}

function spawnReaction(emoji) {
  const layer = $("wpReactionsLayer");
  if (!layer) return;
  const el = document.createElement("div");
  const lane = Math.floor(Math.random() * 6);
  el.className = `wp-reaction-float wp-lane-${lane}`;
  el.textContent = String(emoji || "👍").slice(0, 16);
  layer.appendChild(el);
  const remove = () => { if (el.parentNode) el.parentNode.removeChild(el); };
  el.addEventListener("animationend", remove, { once: true });
  setTimeout(remove, 2600);
}

function renderReactionBar() {
  const bar = $("wpReactionBar");
  if (!bar) return;
  bar.replaceChildren();
  const emojis = ["👍", "❤️", "😂", "😮", "🔥", "👏", "🍿", "🎉"];
  for (const emoji of emojis) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "wp-reaction-btn";
    btn.textContent = emoji;
    btn.setAttribute("aria-label", `React ${emoji}`);
    btn.addEventListener("click", () => {
      sendWs({ type: "reaction", emoji });
    });
    bar.appendChild(btn);
  }
}

function setTab(tab) {
  const mapping = [
    ["chat", "wpTabChat", "wpPanelChat"],
    ["queue", "wpTabQueue", "wpPanelQueue"],
    ["viewers", "wpTabViewers", "wpPanelViewers"],
  ];
  for (const [name, tabId, panelId] of mapping) {
    const t = $(tabId);
    const p = $(panelId);
    const active = name === tab;
    if (t) {
      t.classList.toggle("is-active", active);
      t.setAttribute("aria-selected", String(active));
    }
    if (p) p.hidden = !active;
  }
}

function scheduleReconnect() {
  if (intentionalClose || !session) return;
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
  const delay = RECONNECT_DELAYS[Math.min(reconnectAttempt, RECONNECT_DELAYS.length - 1)];
  reconnectAttempt += 1;
  setBadge($("wpSyncBadge"), "Reconnecting...", "wp-badge-warn");
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectWs();
  }, delay);
}

function clearReconnectTimer() {
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
}

function stopPing() {
  if (pingInterval) {
    clearInterval(pingInterval);
    pingInterval = null;
  }
  for (const t of pingBurstTimers) clearTimeout(t);
  pingBurstTimers = [];
}

function startPing() {
  stopPing();
  sendPing();
  for (let i = 1; i < PING_BURST_COUNT; i += 1) {
    const t = setTimeout(() => sendPing(), i * PING_BURST_GAP_MS);
    pingBurstTimers.push(t);
  }
  pingInterval = setInterval(sendPing, PING_INTERVAL_MS);
}

function sendPing() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  sendWs({ type: "ping", client_time_ms: Date.now() });
}

function sendWs(payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try {
    ws.send(JSON.stringify(payload));
    return true;
  } catch (_) {
    return false;
  }
}

function handlePong(msg) {
  const now = Date.now();
  const clientTime = Number(msg.client_time_ms);
  const serverTime = Number(msg.server_time_ms);
  if (!Number.isFinite(clientTime) || !Number.isFinite(serverTime)) return;
  const rtt = Math.max(0, now - clientTime);
  const sampleOffset = serverTime - (clientTime + rtt / 2);
  if (clockSamples === 0) {
    clockOffsetMs = sampleOffset;
  } else {
    // Smooth, but prefer recent low-latency samples.
    const alpha = rtt < 150 ? 0.25 : 0.1;
    clockOffsetMs = clockOffsetMs + alpha * (sampleOffset - clockOffsetMs);
  }
  clockSamples += 1;
  const rttEl = $("wpRttBadge");
  if (rttEl) rttEl.textContent = `${Math.round(rtt)}ms RTT`;
}

function getEstimatedServerNowMs() {
  return Date.now() + clockOffsetMs;
}

function isUserSeeking() {
  return Boolean(userSeeking || seekSendTimer !== null || performance.now() < localSeekLockUntil);
}

function setVideoTimeSynced(video, targetPos) {
  if (!video || !Number.isFinite(targetPos)) return;
  let clamped = Math.max(0, targetPos);
  if (Number.isFinite(video.duration) && video.duration > 0) {
    clamped = Math.min(video.duration, clamped);
  }
  programmaticSeekTarget = clamped;
  programmaticSeekUntil = performance.now() + 8000;
  try { video.currentTime = clamped; } catch (_) {}
}

function isProgrammaticSeekEvent(video, { consume = false } = {}) {
  if (programmaticSeekTarget === null) return false;
  if (performance.now() > programmaticSeekUntil) {
    programmaticSeekTarget = null;
    return false;
  }
  const pos = Number(video?.currentTime ?? NaN);
  if (Number.isFinite(pos) && Math.abs(pos - programmaticSeekTarget) <= 1.5) {
    if (consume) programmaticSeekTarget = null;
    return true;
  }
  programmaticSeekTarget = null;
  return false;
}

function getExpectedPositionSec() {
  if (!roomState) return 0;
  const hasEffective =
    roomState.effective_position_sec !== undefined &&
    roomState.effective_position_sec !== null &&
    Number.isFinite(Number(roomState.effective_position_sec));
  const base = hasEffective
    ? Number(roomState.effective_position_sec)
    : Number(roomState.position_sec || 0);
  const refTimeMs = hasEffective
    ? Number(roomState.server_time_ms || 0)
    : Number(roomState.state_updated_at_ms || roomState.server_time_ms || 0);
  const rate = Number(roomState.playback_rate || 1) || 1;
  const playing = Boolean(roomState.is_playing);
  let expected = Number.isFinite(base) ? base : 0;
  if (playing) {
    let elapsed = 0;
    if (clockSamples > 0 && Number.isFinite(refTimeMs) && refTimeMs > 0) {
      elapsed = Math.max(0, (getEstimatedServerNowMs() - refTimeMs) / 1000);
    } else if (hasEffective && stateReceivedPerfMs > 0) {
      elapsed = Math.max(0, (performance.now() - stateReceivedPerfMs) / 1000);
    } else if (Number.isFinite(refTimeMs) && refTimeMs > 0) {
      elapsed = Math.max(0, (getEstimatedServerNowMs() - refTimeMs) / 1000);
    }
    expected = expected + elapsed * rate;
  }
  const video = $("wpVideo");
  if (video && Number.isFinite(video.duration) && video.duration > 0) {
    expected = Math.max(0, Math.min(video.duration, expected));
  } else {
    expected = Math.max(0, expected);
  }
  return expected;
}

function clearPendingMetadataSeek(video) {
  if (pendingMetadataSeekHandler && video) {
    video.removeEventListener("loadedmetadata", pendingMetadataSeekHandler);
  }
  pendingMetadataSeekHandler = null;
}

function ensureActiveMedia() {
  const video = $("wpVideo");
  if (!video || !session) return;
  const item = getActiveItem();
  const nextId = item ? String(item.media_id || item.id || "") : "";
  if (!nextId) {
    clearPendingMetadataSeek(video);
    clearBufferingTimers();
    lastBufferingSent = false;
    programmaticSeekTarget = null;
    activeMediaId = null;
    lastAutoAdvancedFrom = null;
    video.removeAttribute("src");
    video.load();
    updateNowPlaying();
    return;
  }
  if (nextId !== activeMediaId) {
    clearPendingMetadataSeek(video);
    clearBufferingTimers();
    lastBufferingSent = false;
    programmaticSeekTarget = null;
    activeMediaId = nextId;
    lastAutoAdvancedFrom = null;
    isApplyingSync = true;
    suppressLocalUntil = performance.now() + 1000;
    postResumeGraceUntil = performance.now() + POST_RESUME_GRACE_MS;
    video.pause();
    video.src = mediaStreamUrl(nextId);
    video.load();
    const rate = Number(roomState?.playback_rate || 1) || 1;
    video.playbackRate = rate;
    video.defaultPlaybackRate = rate;
    const seekToExpected = () => {
      const targetRate = Number(roomState?.playback_rate || 1) || 1;
      const targetPos = getExpectedPositionSec();
      isApplyingSync = true;
      suppressLocalUntil = performance.now() + SUPPRESS_MS;
      postResumeGraceUntil = performance.now() + POST_RESUME_GRACE_MS;
      try { video.playbackRate = targetRate; } catch (_) {}
      if (Number.isFinite(targetPos) && Math.abs((video.currentTime || 0) - targetPos) > DRIFT_SYNCED_SEC) {
        setVideoTimeSynced(video, targetPos);
      }
      window.setTimeout(() => { isApplyingSync = false; }, 0);
    };
    if (video.readyState >= 1) {
      seekToExpected();
    } else {
      const handler = () => {
        if (pendingMetadataSeekHandler === handler) {
          pendingMetadataSeekHandler = null;
        }
        if (activeMediaId !== nextId) return;
        seekToExpected();
      };
      pendingMetadataSeekHandler = handler;
      video.addEventListener("loadedmetadata", handler, { once: true });
      window.setTimeout(() => { isApplyingSync = false; }, 0);
    }
  }
  updateNowPlaying();
}

function showAutoplayOverlay(show) {
  const overlay = $("wpAutoplayOverlay");
  if (overlay) overlay.hidden = !show;
}

async function tryPlaySynced({ forceSeek = false } = {}) {
  const video = $("wpVideo");
  if (!video) return;
  cancelPendingBufferingReport();
  const expected = getExpectedPositionSec();
  const needSeek =
    Number.isFinite(expected) &&
    (forceSeek || Math.abs((video.currentTime || 0) - expected) > DRIFT_SYNCED_SEC);
  isApplyingSync = true;
  suppressLocalUntil = performance.now() + SUPPRESS_MS;
  postResumeGraceUntil = performance.now() + POST_RESUME_GRACE_MS;
  if (needSeek) {
    setVideoTimeSynced(video, expected);
  }
  const rate = Number(roomState?.playback_rate || 1) || 1;
  try { video.playbackRate = rate; } catch (_) {}
  try {
    await video.play();
    showAutoplayOverlay(false);
  } catch (_) {
    showAutoplayOverlay(true);
    setBadge($("wpSyncBadge"), "Tap to sync", "wp-badge-warn");
  } finally {
    suppressLocalUntil = Math.max(suppressLocalUntil, performance.now() + SUPPRESS_MS);
    window.setTimeout(() => { isApplyingSync = false; }, 0);
  }
}

function applyPlaybackState({ reason = "" } = {}) {
  const video = $("wpVideo");
  if (!video || !roomState) return;
  ensureActiveMedia();
  const nominalRate = Number(roomState.playback_rate || 1) || 1;
  if ($("wpSpeedSelect")) $("wpSpeedSelect").value = String(nominalRate);

  if (roomState.is_playing) {
    if (video.paused) {
      void tryPlaySynced();
    } else {
      if (Math.abs((video.playbackRate || 1) - nominalRate) > 0.01 && reason === "rate") {
        isApplyingSync = true;
        suppressLocalUntil = performance.now() + SUPPRESS_MS;
        try { video.playbackRate = nominalRate; } catch (_) {}
        window.setTimeout(() => { isApplyingSync = false; }, 0);
      }
      if (reason === "seek" && !isUserSeeking()) {
        const expected = getExpectedPositionSec();
        if (Number.isFinite(expected) && Math.abs((video.currentTime || 0) - expected) > DRIFT_SYNCED_SEC) {
          cancelPendingBufferingReport();
          isApplyingSync = true;
          suppressLocalUntil = performance.now() + SUPPRESS_MS;
          postResumeGraceUntil = performance.now() + POST_RESUME_GRACE_MS;
          try { video.playbackRate = nominalRate; } catch (_) {}
          setVideoTimeSynced(video, expected);
          window.setTimeout(() => { isApplyingSync = false; }, 0);
        }
      }
    }
  } else {
    cancelPendingBufferingReport();
    if (!roomState.paused_for_buffering) {
      reportBuffering(false);
    }
    if (!video.paused) {
      isApplyingSync = true;
      suppressLocalUntil = performance.now() + SUPPRESS_MS;
      try { video.pause(); } catch (_) {}
      window.setTimeout(() => { isApplyingSync = false; }, 0);
    }
    // Keep paused position close to authoritative, but avoid flushing a peer's
    // in-flight buffer over a small delta during a buffering pause.
    if (!isUserSeeking()) {
      const expected = getExpectedPositionSec();
      const alignThreshold = roomState.paused_for_buffering ? DRIFT_NUDGE_SEC : DRIFT_SYNCED_SEC;
      if (Number.isFinite(expected)) {
        const delta = Math.abs((video.currentTime || 0) - expected);
        if (delta > alignThreshold) {
          isApplyingSync = true;
          suppressLocalUntil = performance.now() + SUPPRESS_MS;
          setVideoTimeSynced(video, expected);
          window.setTimeout(() => { isApplyingSync = false; }, 0);
        }
      }
    }
    setBadge(
      $("wpSyncBadge"),
      roomState.paused_for_buffering ? "Buffering" : "Paused",
      "wp-badge-warn"
    );
  }
}

function snapToAuthoritative(showToastMsg) {
  const video = $("wpVideo");
  if (!video || !roomState) return;
  cancelPendingBufferingReport();
  const expected = getExpectedPositionSec();
  const nominalRate = Number(roomState.playback_rate || 1) || 1;
  isApplyingSync = true;
  suppressLocalUntil = performance.now() + SUPPRESS_MS;
  postResumeGraceUntil = performance.now() + POST_RESUME_GRACE_MS;
  try { video.playbackRate = nominalRate; } catch (_) {}
  if (Number.isFinite(expected) && Math.abs((video.currentTime || 0) - expected) > DRIFT_SYNCED_SEC) {
    setVideoTimeSynced(video, expected);
  }
  if (roomState.is_playing) {
    void video.play().catch(() => { showAutoplayOverlay(true); });
  } else {
    try { video.pause(); } catch (_) {}
  }
  window.setTimeout(() => { isApplyingSync = false; }, 0);
  if (showToastMsg) showControlToast("Only the host can control playback");
}

function startSyncMonitor() {
  if (syncTimer) clearInterval(syncTimer);
  syncTimer = setInterval(syncTick, SYNC_INTERVAL_MS);
}

function stopSyncMonitor() {
  if (syncTimer) {
    clearInterval(syncTimer);
    syncTimer = null;
  }
}

function syncTick() {
  const video = $("wpVideo");
  const driftLabel = $("wpDriftLabel");
  if (!video || !roomState) return;
  if (lastBufferingSent) {
    checkBufferingReady();
  }
  if (!activeMediaId || !video.src) {
    if (driftLabel) driftLabel.textContent = "Sync --";
    return;
  }
  if (isUserSeeking() || video.seeking) {
    return;
  }

  const expected = getExpectedPositionSec();
  const delta = (video.currentTime || 0) - expected;
  const absDelta = Math.abs(delta);
  const nominalRate = Number(roomState.playback_rate || 1) || 1;
  if (driftLabel) driftLabel.textContent = `Sync ±${Math.round(absDelta * 1000)}ms`;

  if (!roomState.is_playing) {
    const alignThreshold = roomState.paused_for_buffering ? DRIFT_NUDGE_SEC : DRIFT_SYNCED_SEC;
    if (absDelta > alignThreshold) {
      // Keep paused clients close without flushing in-flight buffers during buffering pauses.
      isApplyingSync = true;
      suppressLocalUntil = performance.now() + SUPPRESS_MS;
      setVideoTimeSynced(video, expected);
      try { video.playbackRate = nominalRate; } catch (_) {}
      window.setTimeout(() => { isApplyingSync = false; }, 0);
    }
    setBadge(
      $("wpSyncBadge"),
      roomState.paused_for_buffering ? "Buffering" : "Paused",
      "wp-badge-warn"
    );
    return;
  }

  if (video.paused || video.readyState < 1) {
    setBadge($("wpSyncBadge"), "Syncing", "wp-badge-warn");
    return;
  }

  if (absDelta < DRIFT_SYNCED_SEC) {
    if (Math.abs(video.playbackRate - nominalRate) > 0.01) {
      isApplyingSync = true;
      suppressLocalUntil = performance.now() + SUPPRESS_MS;
      try { video.playbackRate = nominalRate; } catch (_) {}
      window.setTimeout(() => { isApplyingSync = false; }, 0);
    }
    setBadge($("wpSyncBadge"), "Synced", "wp-badge-ok");
    return;
  }

  if (absDelta <= DRIFT_NUDGE_SEC) {
    const nudged = nominalRate * (delta > 0 ? 0.95 : 1.05);
    if (Math.abs(video.playbackRate - nudged) > 0.005) {
      isApplyingSync = true;
      suppressLocalUntil = performance.now() + SUPPRESS_MS;
      try { video.playbackRate = nudged; } catch (_) {}
      window.setTimeout(() => { isApplyingSync = false; }, 0);
    }
    setBadge($("wpSyncBadge"), "Syncing", "wp-badge-warn");
    return;
  }

  cancelPendingBufferingReport();
  isApplyingSync = true;
  suppressLocalUntil = performance.now() + SUPPRESS_MS;
  postResumeGraceUntil = performance.now() + POST_RESUME_GRACE_MS;
  try { video.playbackRate = nominalRate; } catch (_) {}
  setVideoTimeSynced(video, expected);
  window.setTimeout(() => { isApplyingSync = false; }, 0);
  setBadge($("wpSyncBadge"), "Syncing", "wp-badge-warn");
}

function applyRoomState(next, { force = false, reason = "", actorParticipantId = "" } = {}) {
  if (!next || typeof next !== "object") return;
  const seq = Number(next.state_seq ?? next.seq ?? 0);
  if (!force && seq > 0 && seq < lastStateSeq) return;
  if (seq > 0) lastStateSeq = seq;

  const prevPlaying = Boolean(roomState?.is_playing);
  const prevPausedForBuffering = Boolean(roomState?.paused_for_buffering);
  const prevMediaId = String(roomState?.active_media_id || roomState?.active_item_id || "");

  stateReceivedPerfMs = performance.now();
  roomState = { ...(roomState || {}), ...next };
  updateTopMeta();
  updateControlUi();
  renderParticipants();
  renderQueue();

  const nextMediaId = String(roomState?.active_media_id || roomState?.active_item_id || "");
  const playbackChanged =
    Boolean(roomState.is_playing) !== prevPlaying ||
    Boolean(roomState.paused_for_buffering) !== prevPausedForBuffering ||
    nextMediaId !== prevMediaId;

  if (!force && reason === "buffering" && !playbackChanged) {
    return;
  }

  const isSelfActor = Boolean(
    actorParticipantId &&
    session &&
    String(actorParticipantId) === String(session.participant_id)
  );
  if (isSelfActor && !force && reason !== "buffering" && reason !== "select") {
    if (reason === "seek" && pendingSeekTargetSec !== null) {
      const echoedPos = Number(roomState.position_sec ?? roomState.effective_position_sec ?? NaN);
      if (Number.isFinite(echoedPos) && Math.abs(echoedPos - pendingSeekTargetSec) <= 0.5) {
        pendingSeekTargetSec = null;
        localSeekLockUntil = Math.min(localSeekLockUntil, performance.now() + 250);
      }
    }
    if (!roomState.is_playing) {
      setBadge(
        $("wpSyncBadge"),
        roomState.paused_for_buffering ? "Buffering" : "Paused",
        "wp-badge-warn"
      );
    } else if (!$("wpVideo")?.paused) {
      setBadge($("wpSyncBadge"), "Synced", "wp-badge-ok");
    }
    return;
  }

  ensureActiveMedia();
  applyPlaybackState({ reason });
}

function handleWsMessage(raw) {
  let msg;
  try {
    msg = JSON.parse(raw);
  } catch (_) {
    return;
  }
  if (!msg || typeof msg.type !== "string") return;

  switch (msg.type) {
    case "auth_ok": {
      reconnectAttempt = 0;
      clearReconnectTimer();
      clearBufferingTimers();
      lastBufferingSent = false;
      userSeeking = false;
      pendingSeekTargetSec = null;
      localSeekLockUntil = 0;
      programmaticSeekTarget = null;
      if (msg.participant_id || msg.role || msg.display_name) {
        saveSession({
          token: session.token,
          role: String(msg.role || session.role || "guest"),
          participant_id: String(msg.participant_id || session.participant_id || ""),
          display_name: String(msg.display_name || session.display_name || ""),
        });
      }
      if (Array.isArray(msg.chat_history)) {
        chatHistory = msg.chat_history.slice(-200);
        renderChatHistory();
      }
      if (msg.room_state) {
        applyRoomState(msg.room_state, { force: true });
      }
      setBadge($("wpSyncBadge"), "Syncing", "wp-badge-warn");
      startPing();
      startSyncMonitor();
      break;
    }
    case "sync_state": {
      const meta = {
        reason: String(msg.reason || ""),
        actorParticipantId: String(msg.actor_participant_id || ""),
      };
      if (msg.room_state) applyRoomState(msg.room_state, meta);
      else applyRoomState(msg, { force: false, ...meta });
      break;
    }
    case "pong": {
      handlePong(msg);
      break;
    }
    case "presence": {
      if (msg.room_state) {
        const incomingSeq = Number(msg.room_state.state_seq ?? msg.room_state.seq ?? 0);
        if (!roomState || incomingSeq > lastStateSeq) {
          applyRoomState(msg.room_state, { force: false });
        } else {
          if (Array.isArray(msg.room_state.participants)) {
            roomState.participants = msg.room_state.participants;
          } else if (Array.isArray(msg.participants)) {
            roomState.participants = msg.participants;
          }
          if (msg.room_state.viewer_count != null) {
            roomState.viewer_count = msg.room_state.viewer_count;
          } else if (msg.participant_count != null) {
            roomState.viewer_count = msg.participant_count;
          }
          if (msg.room_state.participant_count != null) {
            roomState.participant_count = msg.room_state.participant_count;
          }
          if (msg.room_state.paused_for_buffering !== undefined) {
            roomState.paused_for_buffering = Boolean(msg.room_state.paused_for_buffering);
          }
          renderParticipants();
          updateTopMeta();
        }
      } else if (Array.isArray(msg.participants) && roomState) {
        roomState.participants = msg.participants;
        if (msg.participant_count != null) {
          roomState.viewer_count = msg.participant_count;
          roomState.participant_count = msg.participant_count;
        }
        renderParticipants();
        updateTopMeta();
      }
      if (msg.event === "join" && msg.display_name) {
        showToast(`${String(msg.display_name)} joined`, "info");
      } else if (msg.event === "leave" && msg.display_name) {
        showToast(`${String(msg.display_name)} left`, "info");
      } else if (msg.event === "kick" && msg.display_name) {
        showToast(`${String(msg.display_name)} was removed`, "warn");
      }
      break;
    }
    case "chat_message": {
      chatHistory.push(msg);
      if (chatHistory.length > 300) chatHistory = chatHistory.slice(-300);
      appendChatMessage(msg);
      break;
    }
    case "reaction": {
      spawnReaction(String(msg.emoji || "👍"));
      break;
    }
    case "kicked": {
      intentionalClose = true;
      clearSession();
      stopPing();
      stopSyncMonitor();
      setErrorView("Removed from Watch Party", String(msg.reason || "You were removed from this room."));
      if (ws) try { ws.close(); } catch (_) {}
      break;
    }
    case "room_closed": {
      intentionalClose = true;
      clearSession();
      stopPing();
      stopSyncMonitor();
      setErrorView("Watch Party Ended", String(msg.reason || "This room has expired or been closed."));
      if (ws) try { ws.close(); } catch (_) {}
      break;
    }
    case "error": {
      showToast(String(msg.message || msg.detail || "Something went wrong"), "err");
      break;
    }
    default:
      break;
  }
}

function connectWs() {
  if (!session || !roomCode) return;
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
  intentionalClose = false;
  setBadge($("wpSyncBadge"), reconnectAttempt > 0 ? "Reconnecting..." : "Connecting...", "wp-badge-warn");

  let socket;
  try {
    socket = new WebSocket(wsUrl());
  } catch (_) {
    scheduleReconnect();
    return;
  }
  ws = socket;

  socket.onopen = () => {
    if (ws !== socket) return;
    sendWs({ type: "auth", token: session.token });
  };

  socket.onmessage = (ev) => {
    if (ws !== socket) return;
    handleWsMessage(ev.data);
  };

  socket.onclose = (ev) => {
    if (ws === socket) ws = null;
    stopPing();
    stopSyncMonitor();
    if (intentionalClose) return;

    if (ev && NO_RECONNECT_CODES.has(ev.code)) {
      if (ev.code === 4001 || ev.code === 4003) {
        clearSession();
        setErrorView(
          ev.code === 4003 ? "Removed from Watch Party" : "Session expired",
          ev.code === 4003 ? "You were removed from this room." : "Your watch party session is no longer valid. Please rejoin."
        );
        return;
      }
      clearSession();
      setErrorView("Watch Party Ended", "This room is no longer available.");
      return;
    }

    scheduleReconnect();
  };

  socket.onerror = () => {
    // onclose handles reconnect.
  };
}

function closeWs({ intentional = true } = {}) {
  intentionalClose = intentional;
  clearReconnectTimer();
  clearBufferingTimers();
  clearTimeout(seekSendTimer);
  seekSendTimer = null;
  lastBufferingSent = false;
  userSeeking = false;
  pendingSeekTargetSec = null;
  localSeekLockUntil = 0;
  programmaticSeekTarget = null;
  stopPing();
  stopSyncMonitor();
  if (ws) {
    try { ws.close(); } catch (_) {}
    ws = null;
  }
}

function getBufferedAheadSec(video) {
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

function hasEnoughBufferToResume(video) {
  if (!video || !video.src || video.ended) return true;
  if (video.readyState >= 4) return true;
  if (video.readyState >= 3) {
    const ahead = getBufferedAheadSec(video);
    if (ahead >= BUFFERING_RESUME_AHEAD_SEC) return true;
    const dur = Number(video.duration);
    if (Number.isFinite(dur) && dur > 0 && dur - (Number(video.currentTime) || 0) <= 1.0) {
      return true;
    }
    if (bufferingReportedAt > 0 && performance.now() - bufferingReportedAt >= BUFFERING_READY_FALLBACK_MS) {
      return true;
    }
  }
  return false;
}

function cancelPendingBufferingReport() {
  if (bufferingReportTimer) {
    clearTimeout(bufferingReportTimer);
    bufferingReportTimer = null;
  }
}

function stopBufferingReadyPoll() {
  if (bufferingReadyTimer) {
    clearInterval(bufferingReadyTimer);
    bufferingReadyTimer = null;
  }
}

function clearBufferingTimers() {
  cancelPendingBufferingReport();
  stopBufferingReadyPoll();
  bufferingReportedAt = 0;
}

function checkBufferingReady() {
  if (!lastBufferingSent) {
    stopBufferingReadyPoll();
    return;
  }
  const video = $("wpVideo");
  if (!video || !video.src || video.ended) {
    reportBuffering(false);
    return;
  }
  if (roomState && !roomState.is_playing && !roomState.paused_for_buffering) {
    reportBuffering(false);
    return;
  }
  if (hasEnoughBufferToResume(video)) {
    reportBuffering(false);
  }
}

function reportBuffering(isBuffering) {
  const desired = Boolean(isBuffering);
  if (!desired) {
    cancelPendingBufferingReport();
    stopBufferingReadyPoll();
    bufferingReportedAt = 0;
  }
  if (!session) return;
  if (lastBufferingSent === desired) return;
  lastBufferingSent = desired;
  if (desired) {
    bufferingReportedAt = performance.now();
    stopBufferingReadyPoll();
    bufferingReadyTimer = setInterval(checkBufferingReady, BUFFERING_CHECK_INTERVAL_MS);
  }
  sendWs({ type: "buffering", is_buffering: desired });
}

function scheduleBufferingReport() {
  const video = $("wpVideo");
  if (!session || !video || !video.src) return;
  if (lastBufferingSent || bufferingReportTimer) return;
  if (video.paused || video.ended || video.seeking || isApplyingSync || isUserSeeking()) return;
  if (video.readyState >= 3) return;
  if (!roomState || !roomState.is_playing) return;

  const graceRemaining = Math.max(0, postResumeGraceUntil - performance.now());
  const delay = Math.max(BUFFERING_REPORT_DELAY_MS, graceRemaining);
  bufferingReportTimer = setTimeout(() => {
    bufferingReportTimer = null;
    const v = $("wpVideo");
    if (!session || !v || !v.src) return;
    if (v.paused || v.ended || v.seeking || isApplyingSync || isUserSeeking()) return;
    if (v.readyState >= 3) return;
    if (!roomState || !roomState.is_playing) return;
    reportBuffering(true);
  }, delay);
}

function sendSeekDebounced(positionSec) {
  clearTimeout(seekSendTimer);
  const target = Number(positionSec);
  if (!Number.isFinite(target) || target < 0) return;
  seekSendTimer = setTimeout(() => {
    seekSendTimer = null;
    sendWs({ type: "seek", position_sec: target });
  }, SEEK_DEBOUNCE_MS);
}

function sendSeekImmediate(positionSec) {
  clearTimeout(seekSendTimer);
  seekSendTimer = null;
  const target = Number(positionSec);
  if (!Number.isFinite(target) || target < 0) return;
  sendWs({ type: "seek", position_sec: target });
}

function bindVideoEvents() {
  const video = $("wpVideo");
  if (!video) return;

  video.addEventListener("play", () => {
    if (performance.now() < suppressLocalUntil || isApplyingSync) return;
    if (!canControl()) {
      snapToAuthoritative(true);
      return;
    }
    cancelPendingBufferingReport();
    postResumeGraceUntil = performance.now() + POST_RESUME_GRACE_MS;
    sendWs({ type: "play", position_sec: Number(video.currentTime || 0) });
    reportBuffering(false);
  });

  video.addEventListener("pause", () => {
    cancelPendingBufferingReport();
    if (performance.now() < suppressLocalUntil || isApplyingSync) return;
    if (!canControl()) {
      snapToAuthoritative(true);
      return;
    }
    reportBuffering(false);
    sendWs({ type: "pause", position_sec: Number(video.currentTime || 0) });
  });

  video.addEventListener("seeking", () => {
    if (performance.now() < suppressLocalUntil || isApplyingSync) return;
    if (isProgrammaticSeekEvent(video, { consume: false })) return;
    if (!canControl()) return;
    cancelPendingBufferingReport();
    userSeeking = true;
    const targetPos = Number(video.currentTime || 0);
    pendingSeekTargetSec = targetPos;
    localSeekLockUntil = performance.now() + LOCAL_SEEK_LOCK_MS;
    sendSeekDebounced(targetPos);
  });

  video.addEventListener("seeked", () => {
    if (lastBufferingSent) checkBufferingReady();
    const wasProgrammatic = isProgrammaticSeekEvent(video, { consume: true });
    if (performance.now() < suppressLocalUntil || isApplyingSync || wasProgrammatic) return;
    if (!canControl()) {
      snapToAuthoritative(true);
      return;
    }
    userSeeking = false;
    const targetPos = Number(video.currentTime || 0);
    const shouldSend =
      seekSendTimer !== null ||
      pendingSeekTargetSec === null ||
      Math.abs(targetPos - pendingSeekTargetSec) > 0.15;
    pendingSeekTargetSec = targetPos;
    localSeekLockUntil = performance.now() + 900;
    if (shouldSend) {
      sendSeekImmediate(targetPos);
    }
  });

  video.addEventListener("ratechange", () => {
    if (performance.now() < suppressLocalUntil || isApplyingSync) return;
    const rate = Number(video.playbackRate || 1);
    const nominal = Number(roomState?.playback_rate || 1);
    // Ignore small nudge changes caused by drift correction.
    if (Math.abs(rate - nominal) < 0.02) return;
    if (Math.abs(rate - nominal * 0.95) < 0.02 || Math.abs(rate - nominal * 1.05) < 0.02) return;
    if (!canControl()) {
      snapToAuthoritative(true);
      return;
    }
    sendWs({ type: "rate", playback_rate: rate });
  });

  video.addEventListener("waiting", () => {
    scheduleBufferingReport();
  });
  video.addEventListener("stalled", () => {
    // Only treat network stall as buffering if playback is actually starved.
    if (!video.paused && !video.ended && video.readyState < 3) {
      scheduleBufferingReport();
    }
  });
  video.addEventListener("playing", () => {
    cancelPendingBufferingReport();
    reportBuffering(false);
  });
  video.addEventListener("canplay", () => {
    if (video.readyState >= 3) cancelPendingBufferingReport();
    if (lastBufferingSent) checkBufferingReady();
  });
  video.addEventListener("canplaythrough", () => {
    cancelPendingBufferingReport();
    if (lastBufferingSent) reportBuffering(false);
  });
  video.addEventListener("progress", () => {
    if (lastBufferingSent) checkBufferingReady();
  });
  video.addEventListener("timeupdate", () => {
    const pos = Number(video.currentTime || 0);
    if (!video.paused && Number.isFinite(pos) && Math.abs(pos - lastTimeUpdatePos) > 0.05) {
      lastTimeUpdatePos = pos;
      cancelPendingBufferingReport();
      if (lastBufferingSent) reportBuffering(false);
    }
  });

  video.addEventListener("ended", () => {
    clearBufferingTimers();
    reportBuffering(false);
    if (!canControl() || !roomState || !activeMediaId) return;
    if (lastAutoAdvancedFrom === activeMediaId) return;
    const queue = Array.isArray(roomState.queue) ? roomState.queue : [];
    const currentIdx = queue.findIndex(
      (it) => String(it?.media_id || it?.id || "") === String(activeMediaId)
    );
    if (currentIdx < 0 || currentIdx + 1 >= queue.length) return;
    const nextItem = queue[currentIdx + 1];
    const nextId = String(nextItem?.media_id || nextItem?.id || "");
    if (!nextId) return;
    lastAutoAdvancedFrom = activeMediaId;
    sendWs({ type: "select_item", media_id: nextId });
    sendWs({ type: "play", position_sec: 0 });
  });
}

function bindUiEvents() {
  const joinForm = $("wpJoinForm");
  if (joinForm) {
    joinForm.addEventListener("submit", onJoinSubmit);
  }

  const copyBtn = $("wpCopyInviteBtn");
  if (copyBtn) copyBtn.addEventListener("click", onCopyInvite);

  const leaveBtn = $("wpLeaveBtn");
  if (leaveBtn) leaveBtn.addEventListener("click", onLeave);

  const endBtn = $("wpEndRoomBtn");
  if (endBtn) endBtn.addEventListener("click", onEndRoom);

  const resyncBtn = $("wpResyncBtn");
  if (resyncBtn) resyncBtn.addEventListener("click", () => {
    snapToAuthoritative(false);
    showToast("Resyncing playback", "info");
  });

  const autoplayBtn = $("wpAutoplayBtn");
  if (autoplayBtn) {
    autoplayBtn.addEventListener("click", async () => {
      const video = $("wpVideo");
      if (!video) return;
      showAutoplayOverlay(false);
      try { video.muted = false; } catch (_) {}
      await tryPlaySynced();
    });
  }

  const speedSelect = $("wpSpeedSelect");
  if (speedSelect) {
    speedSelect.addEventListener("change", () => {
      const rate = Number(speedSelect.value);
      if (!Number.isFinite(rate) || rate <= 0) return;
      if (performance.now() < suppressLocalUntil || isApplyingSync) return;
      if (!canControl()) {
        speedSelect.value = String(Number(roomState?.playback_rate || 1));
        snapToAuthoritative(true);
        return;
      }
      sendWs({ type: "rate", playback_rate: rate });
    });
  }

  const toggleControlsBtn = $("wpToggleControlsBtn");
  if (toggleControlsBtn) {
    toggleControlsBtn.addEventListener("click", () => {
      if (!isHost()) return;
      const nextMode = roomState?.control_mode === "everyone" ? "host_only" : "everyone";
      sendWs({ type: "update_settings", control_mode: nextMode });
    });
  }

  const toggleWaitBtn = $("wpToggleWaitBufferBtn");
  if (toggleWaitBtn) {
    toggleWaitBtn.addEventListener("click", () => {
      if (!isHost()) return;
      sendWs({ type: "update_settings", wait_for_buffering: !Boolean(roomState?.wait_for_buffering) });
    });
  }

  const chatForm = $("wpChatForm");
  if (chatForm) chatForm.addEventListener("submit", onChatSubmit);

  const chatInput = $("wpChatInput");
  if (chatInput) {
    chatInput.addEventListener("input", () => {
      const cc = $("wpChatCharCount");
      if (cc) cc.textContent = `${String(chatInput.value || "").length}/${MAX_CHAT_LEN}`;
    });
  }

  const tabChat = $("wpTabChat");
  const tabQueue = $("wpTabQueue");
  const tabViewers = $("wpTabViewers");
  if (tabChat) tabChat.addEventListener("click", () => setTab("chat"));
  if (tabQueue) tabQueue.addEventListener("click", () => setTab("queue"));
  if (tabViewers) tabViewers.addEventListener("click", () => setTab("viewers"));

  window.addEventListener("beforeunload", () => {
    closeWs({ intentional: true });
  });

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && session && (!ws || ws.readyState === WebSocket.CLOSED)) {
      scheduleReconnect();
    }
  });
}

async function onCopyInvite() {
  const url = `${window.location.origin}/watch/${encodeURIComponent(roomCode)}`;
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
    showToast("Invite link copied", "ok");
  } catch (_) {
    showToast("Could not copy invite link", "err");
  }
}

function onLeave() {
  intentionalClose = true;
  sendWs({ type: "leave" });
  closeWs({ intentional: true });
  clearSession();
  setErrorView("You left the Watch Party", "You can rejoin with the invite link if the room is still active.");
}

function onEndRoom() {
  if (!isHost()) return;
  // WS close_room is handled by integration glue; as fallback try HTTP DELETE with no vault token.
  sendWs({ type: "close_room" });
}

async function onJoinSubmit(ev) {
  ev.preventDefault();
  const nameInput = $("wpNameInput");
  const pinInput = $("wpPinInput");
  const joinBtn = $("wpJoinBtn");
  const joinError = $("wpJoinError");
  const displayName = String(nameInput?.value || "").trim();
  const pin = String(pinInput?.value || "").trim();

  if (joinError) { joinError.hidden = true; joinError.textContent = ""; }
  if (!displayName) {
    if (joinError) { joinError.hidden = false; joinError.textContent = "Enter a display name to join."; }
    return;
  }

  if (joinBtn) joinBtn.disabled = true;
  try {
    const body = { display_name: displayName };
    if (pin) body.pin = pin;
    const data = await fetchJson(roomApi("/join"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const token = String(data?.guest_token || data?.host_token || data?.token || "");
    if (!token) throw new Error("Missing room token in join response.");
    const nextSession = {
      token,
      role: String(data?.role || "guest"),
      participant_id: String(data?.participant_id || ""),
      display_name: String(data?.display_name || displayName),
    };
    saveSession(nextSession);

    if (Array.isArray(data?.chat_history)) {
      chatHistory = data.chat_history.slice(-200);
    } else {
      chatHistory = [];
    }
    if (data?.room_state) {
      roomState = null;
      lastStateSeq = -1;
      applyRoomState(data.room_state, { force: true });
    }

    enterRoomView();
    connectWs();
  } catch (err) {
    const status = err?.status;
    let message = err?.message || "Could not join the watch party.";
    if (status === 403) message = "Incorrect PIN. Please try again.";
    else if (status === 404) message = "This watch party could not be found or is closed.";
    else if (status === 409) message = "This watch party is full.";
    else if (status === 429) {
      const retry = err?.data?.retry_after_sec || err?.data?.retry_after || null;
      message = retry ? `Too many attempts. Try again in ${retry} seconds.` : "Too many attempts. Please wait and try again.";
    }
    if (joinError) { joinError.hidden = false; joinError.textContent = String(message); }
  } finally {
    if (joinBtn) joinBtn.disabled = false;
  }
}

function onChatSubmit(ev) {
  ev.preventDefault();
  const input = $("wpChatInput");
  if (!input) return;
  const text = String(input.value || "").trim();
  if (!text) return;
  if (text.length > MAX_CHAT_LEN) {
    showToast(`Message too long (max ${MAX_CHAT_LEN})`, "warn");
    return;
  }
  const ok = sendWs({ type: "chat_message", text });
  if (ok) {
    input.value = "";
    const cc = $("wpChatCharCount");
    if (cc) cc.textContent = `0/${MAX_CHAT_LEN}`;
  } else {
    showToast("Not connected. Trying to reconnect...", "warn");
  }
}

function enterRoomView() {
  showView("wpRoomView");
  renderReactionBar();
  renderChatHistory();
  renderParticipants();
  renderQueue();
  updateTopMeta();
  updateControlUi();
  updateNowPlaying();
  const cc = $("wpChatCharCount");
  if (cc) cc.textContent = `0/${MAX_CHAT_LEN}`;
}

function showJoinView(info) {
  const title = $("wpJoinTitle");
  const meta = $("wpJoinMeta");
  const pinField = $("wpPinField");
  if (title) title.textContent = String(info?.title || info?.room_title || "Join Watch Party");
  if (meta) {
    const bits = [];
    if (info?.host_name) bits.push(`Hosted by ${String(info.host_name)}`);
    if (info?.viewer_count != null) bits.push(`${info.viewer_count} watching`);
    meta.textContent = bits.join(" • ") || "Enter your name to join.";
  }
  if (pinField) pinField.hidden = !Boolean(info?.requires_pin);
  const err = $("wpJoinError");
  if (err) { err.hidden = true; err.textContent = ""; }
  showView("wpJoinView");
}

async function boot() {
  roomCode = getRoomCode();
  if (!roomCode) {
    setErrorView("Invalid Watch Party Link", "This watch party link is missing a room code.");
    return;
  }

  bindUiEvents();
  bindVideoEvents();
  renderReactionBar();
  setTab("chat");

  let info = null;
  try {
    info = await fetchJson(roomApi("/info"), { method: "GET" });
  } catch (err) {
    if (err?.status === 404) {
      setErrorView("Watch Party Not Found", "This room does not exist or has been closed.");
      return;
    }
    setErrorView("Could Not Load Watch Party", err?.message || "Please try again.");
    return;
  }

  if (!info?.active) {
    setErrorView("Watch Party Ended", "This room has expired or been closed.");
    return;
  }

  const saved = loadSession();
  if (saved && saved.token) {
    session = saved;
    // Optimistically enter room and connect; WS auth failure will fall back.
    enterRoomView();
    connectWs();
    return;
  }

  showJoinView(info);
}

boot().catch((err) => {
  console.error("[watch-party] boot failed", err);
  setErrorView("Watch Party Error", "An unexpected error occurred while loading this room.");
});
