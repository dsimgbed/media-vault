export const $ = (id) => document.getElementById(id);

export function escapeHtml(val) {
  return String(val ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export function fmtSize(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = bytes;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(1)} ${units[i]}`;
}

export function fmtDuration(sec) {
  if (!sec || sec <= 0) return "";
  const total = Math.round(sec);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const ss = String(s).padStart(2, "0");
  if (h > 0) {
    return `${h}:${String(m).padStart(2, "0")}:${ss}`;
  }
  return `${m}:${ss}`;
}

export function fmtResolution(it, { exact = false } = {}) {
  const w = Number(it.width || 0);
  const h = Number(it.height || 0);
  if (!w || !h) return "";
  if (exact) return `${w}×${h}`;
  if (h >= 2160 || w >= 3840) return "4K";
  if (h >= 1440 && h <= 1460) return "1440p";
  if (h >= 1070 && h <= 1090) return "1080p";
  if (h >= 710 && h <= 730) return "720p";
  if (h >= 470 && h <= 490) return "480p";
  return `${w}×${h}`;
}

export function debounce(fn, delay = 250) {
  let timer = null;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

export function isZipItem(item) {
  const name = (item.filename || "").toLowerCase();
  return name.endsWith(".zip") || item.mime_type === "application/zip";
}
