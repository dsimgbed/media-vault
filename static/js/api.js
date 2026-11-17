import { state } from "/static/js/state.js";
import { emit } from "/static/js/bus.js";
import { $ } from "/static/js/utils.js";

export function authHeaders() {
  return state.token ? { Authorization: `Bearer ${state.token}` } : {};
}

export function withToken(url) {
  if (!state.token) return url;
  const sep = url.includes("?") ? "&" : "?";
  return `${url}${sep}token=${encodeURIComponent(state.token)}`;
}

function handleUnauthorized() {
  state.authRequired = true;
  const box = $("authBox");
  if (box) box.style.display = "flex";
  const banner = $("authBanner");
  if (banner) banner.hidden = false;
  emit("auth:required");
}

export async function apiFetch(url, opts = {}) {
  const headers = { ...authHeaders(), ...(opts.headers || {}) };
  const res = await fetch(url, { ...opts, headers });
  if (res.status === 401) {
    handleUnauthorized();
    throw new Error("Unauthorized — enter your WEB_AUTH_TOKEN above.");
  }
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(txt || `HTTP ${res.status}`);
  }
  return res.json();
}

export function uploadWithProgress(url, formData, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", url);
    if (state.token) {
      xhr.setRequestHeader("Authorization", `Bearer ${state.token}`);
    }
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && typeof onProgress === "function") {
        const pct = Math.round((e.loaded / e.total) * 100);
        onProgress({ loaded: e.loaded, total: e.total, pct });
      }
    };
    xhr.onload = () => {
      if (xhr.status === 401) {
        handleUnauthorized();
        reject(new Error("Unauthorized — enter your WEB_AUTH_TOKEN above."));
        return;
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          resolve(JSON.parse(xhr.responseText));
        } catch {
          resolve({});
        }
      } else {
        reject(new Error(xhr.responseText || `Upload failed (${xhr.status})`));
      }
    };
    xhr.onerror = () => reject(new Error("Network error during upload"));
    xhr.send(formData);
  });
}
