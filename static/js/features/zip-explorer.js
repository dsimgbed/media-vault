import { apiFetch, authHeaders, withToken } from "/static/js/api.js";
import { $, escapeHtml, fmtSize } from "/static/js/utils.js";

export function isPreviewableZipEntry(entry) {
  if (["image", "video", "audio"].includes(entry.media_type)) return true;
  const mime = (entry.mime_type || "").toLowerCase();
  if (mime.startsWith("text/") || mime === "application/json" || mime === "application/xml") return true;
  const name = (entry.name || "").toLowerCase();
  return /\.(txt|md|json|csv|log|xml|ya?ml|py|js|ts|html|css|ini|conf|sh)$/.test(name);
}

export async function renderZipExplorer(item, mediaBox) {
  mediaBox.innerHTML = `<div class="zip-status-msg">Loading ZIP archive directory...</div>`;
  let data;
  try {
    data = await apiFetch(`/api/media/${item.id}/zip/entries`);
  } catch (err) {
    mediaBox.innerHTML = `<div class="zip-status-err">Could not inspect ZIP archive: ${escapeHtml(err.message)}</div>`;
    return;
  }

  const entries = data.entries || [];
  let currentDir = "";
  let activePreviewEntry = null;

  function renderExplorer() {
    const prefix = currentDir ? `${currentDir}/` : "";
    const subdirs = new Set();
    const files = [];

    for (const e of entries) {
      if (!e.path.startsWith(prefix)) continue;
      const rest = e.path.slice(prefix.length);
      if (!rest) continue;
      const slashIdx = rest.indexOf("/");
      if (slashIdx >= 0) {
        subdirs.add(rest.slice(0, slashIdx));
      } else {
        files.push(e);
      }
    }

    const dirList = Array.from(subdirs).sort((a, b) => a.localeCompare(b));
    files.sort((a, b) => a.name.localeCompare(b.name));

    const parts = currentDir ? currentDir.split("/") : [];
    const crumbsHtml = [
      `<button type="button" class="zip-crumb" data-dir="">Root (${entries.length})</button>`,
      ...parts.map((part, idx) => {
        const target = parts.slice(0, idx + 1).join("/");
        return `<span>/</span><button type="button" class="zip-crumb" data-dir="${escapeHtml(target)}">${escapeHtml(part)}</button>`;
      }),
    ].join(" ");

    const rowsHtml = [
      ...dirList.map((d) => {
        const nextDir = currentDir ? `${currentDir}/${d}` : d;
        return `
          <div class="zip-row zip-folder-row" data-dir="${escapeHtml(nextDir)}">
            <span>📁 <strong>${escapeHtml(d)}</strong></span>
            <span class="pill">Folder</span>
          </div>
        `;
      }),
      ...files.map((f, idx) => {
        const dlHref = withToken(`/api/media/${item.id}/zip/stream?path=${encodeURIComponent(f.path)}&download=true`);
        const canPreview = isPreviewableZipEntry(f);
        return `
          <div class="zip-row">
            <div class="zip-file-info">
              <div class="zip-file-name"><strong>${escapeHtml(f.name)}</strong></div>
              <div class="zip-file-meta">${escapeHtml(f.media_type)} • ${fmtSize(f.size_bytes)}</div>
            </div>
            <div class="toolbar-group zip-file-actions">
              ${canPreview ? `<button type="button" class="secondary btn-zip zip-preview-btn" data-idx="${idx}">Preview</button>` : ""}
              <a href="${escapeHtml(dlHref)}" download="${escapeHtml(f.name)}"><button type="button" class="secondary btn-zip">Download</button></a>
            </div>
          </div>
        `;
      }),
    ].join("") || `<div class="zip-empty-dir">Empty folder</div>`;

    mediaBox.innerHTML = `
      <div class="zip-explorer">
        <div class="zip-breadcrumbs">${crumbsHtml}</div>
        <div class="zip-body${activePreviewEntry ? " with-preview" : ""}">
          <div class="zip-list">${rowsHtml}</div>
          ${activePreviewEntry ? `<div id="zipPreviewPane" class="zip-preview-pane">Loading preview...</div>` : ""}
        </div>
      </div>
    `;

    mediaBox.querySelectorAll(".zip-crumb").forEach((btn) => {
      btn.addEventListener("click", () => {
        currentDir = btn.dataset.dir || "";
        renderExplorer();
      });
    });

    mediaBox.querySelectorAll(".zip-folder-row").forEach((row) => {
      row.addEventListener("click", () => {
        currentDir = row.dataset.dir || "";
        renderExplorer();
      });
    });

    mediaBox.querySelectorAll(".zip-preview-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        activePreviewEntry = files[Number(btn.dataset.idx)];
        renderExplorer();
      });
    });

    if (activePreviewEntry) {
      const pane = $("zipPreviewPane");
      const entryUrl = withToken(`/api/media/${item.id}/zip/stream?path=${encodeURIComponent(activePreviewEntry.path)}`);
      if (activePreviewEntry.media_type === "video") {
        pane.innerHTML = `<video controls autoplay playsinline src="${escapeHtml(entryUrl)}"></video>`;
      } else if (activePreviewEntry.media_type === "image") {
        pane.innerHTML = `<img src="${escapeHtml(entryUrl)}" alt="${escapeHtml(activePreviewEntry.name)}" />`;
      } else if (activePreviewEntry.media_type === "audio") {
        pane.innerHTML = `<audio controls autoplay src="${escapeHtml(entryUrl)}"></audio>`;
      } else {
        fetch(entryUrl, { headers: authHeaders() })
          .then((r) => r.text())
          .then((txt) => {
            if (pane) pane.innerHTML = `<pre>${escapeHtml(txt.slice(0, 200000))}</pre>`;
          })
          .catch((err) => {
            if (pane) pane.innerHTML = `<div class="zip-status-err">Preview failed: ${escapeHtml(err.message)}</div>`;
          });
      }
    }
  }

  renderExplorer();
}
