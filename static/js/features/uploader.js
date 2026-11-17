import { state } from "/static/js/state.js";
import { emit, on } from "/static/js/bus.js";
import { apiFetch, uploadWithProgress } from "/static/js/api.js";
import { $, fmtSize } from "/static/js/utils.js";
import { toast } from "/static/js/ui/dialogs.js";
import { loadJobs } from "/static/js/features/jobs.js";
import { getIngestTagPicker } from "/static/js/features/tags.js";
import { normalizeTagsList } from "/static/js/ui/tag-picker.js";

export function selectedFolder() {
  return $("folderInput")?.value.trim() || state.defaultFolder;
}

export function selectedTags() {
  const picker = getIngestTagPicker();
  const inputEl = $("tagsInput");
  if (picker) {
    if (inputEl && inputEl.value !== picker.getSyncedInputValue()) {
      return normalizeTagsList(inputEl.value).map((t) => `#${t}`).join(" ");
    }
    const tags = picker.getTags({ includePending: true });
    return tags.map((t) => `#${t}`).join(" ");
  }
  return inputEl?.value.trim() || "";
}

export function shouldForwardToTelegram() {
  return Boolean($("tgForwardCheck")?.checked);
}

export function shouldForceIngest() {
  return Boolean($("forceIngestCheck") && $("forceIngestCheck").checked);
}

export function shouldExpandPlaylist() {
  return Boolean($("expandPlaylistCheck") && $("expandPlaylistCheck").checked);
}

async function submitUrlIngest(url, formatSpec) {
  await apiFetch("/api/ingest/url", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url,
      folder: selectedFolder(),
      tags: selectedTags(),
      forward_to_telegram: shouldForwardToTelegram(),
      format_spec: formatSpec || $("formatSelect").value || "best",
      expand_playlist: shouldExpandPlaylist(),
      force: shouldForceIngest(),
    }),
  });
  $("urlInput").value = "";
  $("urlInput").rows = 1;
  await loadJobs();
}

function uploadFile(file) {
  const form = new FormData();
  form.append("file", file);
  form.append("folder", selectedFolder());
  if (selectedTags()) form.append("tags", selectedTags());
  form.append("forward_to_telegram", shouldForwardToTelegram() ? "true" : "false");
  form.append("force", shouldForceIngest() ? "true" : "false");

  $("uploadProgress").style.display = "block";
  return uploadWithProgress("/api/ingest/upload", form, ({ loaded, total, pct }) => {
    $("uploadProgressBar").style.width = `${pct}%`;
    $("uploadProgressText").textContent =
      `Uploading ${file.name} → ${selectedFolder()}: ${pct}% (${fmtSize(loaded)} / ${fmtSize(total)})`;
  });
}

function uploadFolderFiles(fileEntries, archiveName) {
  const form = new FormData();
  form.append("archive_name", archiveName || "folder_archive");
  form.append("folder", selectedFolder());
  if (selectedTags()) form.append("tags", selectedTags());
  form.append("forward_to_telegram", shouldForwardToTelegram() ? "true" : "false");
  form.append("force", shouldForceIngest() ? "true" : "false");
  for (const entry of fileEntries) {
    form.append("files", entry.file);
    form.append("relative_paths", entry.relativePath || entry.file.name);
  }

  $("uploadProgress").style.display = "block";
  return uploadWithProgress("/api/ingest/upload-folder", form, ({ loaded, total, pct }) => {
    $("uploadProgressBar").style.width = `${pct}%`;
    $("uploadProgressText").textContent =
      `Uploading folder ${archiveName} (${fileEntries.length} files) → ${selectedFolder()}: ${pct}% (${fmtSize(loaded)} / ${fmtSize(total)})`;
  });
}

function finishUploadProgress() {
  $("uploadProgress").style.display = "none";
  $("uploadProgressBar").style.width = "0%";
}

async function handleFiles(files) {
  const list = Array.from(files || []);
  if (!list.length) return;
  for (const f of list) {
    try {
      await uploadFile(f);
      loadJobs().catch(() => {});
    } catch (err) {
      toast(`Failed uploading ${f.name}: ${err.message}`, { type: "error" });
    }
  }
  finishUploadProgress();
  emit("media:changed", { source: "upload" });
  loadJobs().catch(() => {});
}

async function handleFolderSelection(fileList) {
  const arr = Array.from(fileList || []);
  if (!arr.length) return;
  const firstRel = (arr[0].webkitRelativePath || arr[0].name || "folder_archive").replace(/\\/g, "/");
  const archiveName = firstRel.includes("/") ? firstRel.split("/")[0] : "folder_archive";
  const entries = arr.map((f) => ({
    file: f,
    relativePath: f.webkitRelativePath || f.name,
  }));
  try {
    await uploadFolderFiles(entries, archiveName);
  } catch (err) {
    toast(`Failed uploading folder ${archiveName}: ${err.message}`, { type: "error" });
  }
  finishUploadProgress();
  emit("media:changed", { source: "upload-folder" });
  loadJobs().catch(() => {});
}

async function collectEntryFiles(entry, prefix = "") {
  if (entry.isFile) {
    const file = await new Promise((resolve, reject) => entry.file(resolve, reject));
    return [{ file, relativePath: prefix ? `${prefix}/${entry.name}` : entry.name }];
  }
  if (entry.isDirectory) {
    const reader = entry.createReader();
    const allEntries = [];
    while (true) {
      const batch = await new Promise((resolve, reject) => reader.readEntries(resolve, reject));
      if (!batch.length) break;
      allEntries.push(...batch);
    }
    const subPrefix = prefix ? `${prefix}/${entry.name}` : entry.name;
    const collected = [];
    for (const child of allEntries) {
      collected.push(...(await collectEntryFiles(child, subPrefix)));
    }
    return collected;
  }
  return [];
}

export function initUploader() {
  $("urlInput")?.addEventListener("input", () => {
    const el = $("urlInput");
    const lines = el.value.split(/\r?\n/).length;
    el.rows = Math.max(1, Math.min(4, lines));
  });

  $("urlInput")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey && !$("urlInput").value.includes("\n")) {
      e.preventDefault();
      $("urlForm").requestSubmit();
    }
  });

  $("urlForm")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const url = $("urlInput").value.trim();
    if (!url) return;
    try {
      await submitUrlIngest(url, $("formatSelect").value);
    } catch (err) {
      toast(`Ingest failed: ${err.message}`, { type: "error" });
    }
  });

  $("closeFormatsBtn")?.addEventListener("click", () => $("formatsModal").close());
  $("formatsModal")?.addEventListener("click", (e) => {
    if (e.target === $("formatsModal")) $("formatsModal").close();
  });

  $("inspectFormatsBtn")?.addEventListener("click", async () => {
    const url = $("urlInput").value.split(/\r?\n/).map((s) => s.trim()).find(Boolean) || "";
    if (!url) {
      toast("Paste a video URL first to inspect formats.", { type: "error" });
      return;
    }
    const btn = $("inspectFormatsBtn");
    const prev = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Loading...";
    $("formatsModalTitle").textContent = "Inspecting Formats...";
    $("formatsStatus").textContent = `Fetching available streams for ${url}...`;
    $("formatsList").innerHTML = "";
    $("formatsModal").showModal();

    try {
      const data = await apiFetch("/api/ytdlp/formats", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url }),
      });
      $("formatsModalTitle").textContent = data.title || "Available Formats";
      const fmts = data.formats || [];
      $("formatsStatus").textContent = fmts.length
        ? `${fmts.length} format(s) found. Pick one to select or download immediately:`
        : "No selectable yt-dlp formats found (this may be a direct download link).";

      $("formatsList").innerHTML = fmts
        .map((f, idx) => {
          const sizeTxt = f.size_bytes ? fmtSize(f.size_bytes) : "size unknown";
          const fpsTxt = f.fps ? ` • ${Math.round(f.fps)}fps` : "";
          const codecTxt = [f.vcodec, f.acodec].filter(Boolean).join(" + ") || f.ext;
          const noteTxt = f.note ? ` • ${f.note}` : "";
          return `
            <div class="format-row">
              <div class="format-info">
                <div><strong>${f.resolution}</strong> <span class="pill">${f.ext}</span> <span class="pill">${f.kind}</span></div>
                <div class="format-sub">id: ${f.id} • ${codecTxt}${fpsTxt} • ${sizeTxt}${noteTxt}</div>
              </div>
              <div class="toolbar-group format-actions">
                <button type="button" class="secondary fmt-select-btn" data-idx="${idx}">Use</button>
                <button type="button" class="fmt-dl-btn" data-idx="${idx}">Download</button>
              </div>
            </div>
          `;
        })
        .join("");

      $("formatsList").querySelectorAll(".fmt-select-btn").forEach((b) => {
        b.addEventListener("click", () => {
          const f = fmts[Number(b.dataset.idx)];
          const sel = $("formatSelect");
          let opt = Array.from(sel.options).find((o) => o.value === f.select_spec);
          if (!opt) {
            opt = document.createElement("option");
            opt.value = f.select_spec;
            opt.textContent = `${f.resolution} (${f.ext}, id:${f.id})`;
            sel.appendChild(opt);
          }
          sel.value = f.select_spec;
          $("formatsModal").close();
        });
      });

      $("formatsList").querySelectorAll(".fmt-dl-btn").forEach((b) => {
        b.addEventListener("click", async () => {
          const f = fmts[Number(b.dataset.idx)];
          $("formatsModal").close();
          try {
            await submitUrlIngest(url, f.select_spec);
          } catch (err) {
            toast(`Ingest failed: ${err.message}`, { type: "error" });
          }
        });
      });
    } catch (err) {
      $("formatsModalTitle").textContent = "Format Lookup Failed";
      $("formatsStatus").textContent = err.message;
    } finally {
      btn.disabled = false;
      btn.textContent = prev;
    }
  });

  const dz = $("dropzone");
  dz?.addEventListener("click", () => $("fileInput").click());
  $("selectFolderBtn")?.addEventListener("click", () => $("folderUploadInput").click());
  $("fileInput")?.addEventListener("change", (e) => handleFiles(e.target.files));
  $("folderUploadInput")?.addEventListener("change", (e) => handleFolderSelection(e.target.files));
  dz?.addEventListener("dragover", (e) => {
    e.preventDefault();
    dz.classList.add("dragover");
  });
  dz?.addEventListener("dragleave", () => dz.classList.remove("dragover"));
  dz?.addEventListener("drop", async (e) => {
    e.preventDefault();
    dz.classList.remove("dragover");
    const items = e.dataTransfer?.items;
    if (items && items.length && typeof items[0].webkitGetAsEntry === "function") {
      const entries = Array.from(items).map((it) => it.webkitGetAsEntry()).filter(Boolean);
      const dirs = entries.filter((ent) => ent.isDirectory);
      const looseFiles = entries.filter((ent) => ent.isFile);
      if (dirs.length) {
        for (const dir of dirs) {
          try {
            const dirFiles = await collectEntryFiles(dir, "");
            if (dirFiles.length) {
              await uploadFolderFiles(dirFiles, dir.name);
            }
          } catch (err) {
            toast(`Failed uploading folder ${dir.name}: ${err.message}`, { type: "error" });
          }
        }
        if (looseFiles.length) {
          const resolvedFiles = await Promise.all(
            looseFiles.map((ent) => new Promise((resolve, reject) => ent.file(resolve, reject)))
          );
          await handleFiles(resolvedFiles);
        } else {
          finishUploadProgress();
          emit("media:changed", { source: "upload-drop" });
          loadJobs().catch(() => {});
        }
        return;
      }
    }
    handleFiles(e.dataTransfer.files);
  });

  on("pwa:shared-files", ({ files } = {}) => {
    if (files?.length) {
      handleFiles(files);
    }
  });
}
