import { on } from "/static/js/bus.js";
import { initMode } from "/static/js/mode.js";
import { initSystem, loadHealth } from "/static/js/features/system.js";
import { loadDrives, loadFolders } from "/static/js/features/drives.js";
import { initTags, loadTags } from "/static/js/features/tags.js";
import { connectEvents, initJobs, loadJobs } from "/static/js/features/jobs.js";
import { initUploader } from "/static/js/features/uploader.js";
import { initGallery, loadMedia } from "/static/js/features/gallery.js";
import { initViewer, openViewer } from "/static/js/features/viewer.js";

async function refreshMediaData() {
  try {
    await Promise.all([loadMedia(), loadDrives(), loadFolders(), loadTags()]);
  } catch (err) {
    console.warn(err.message);
  }
}

async function refreshAll() {
  try {
    await Promise.all([loadDrives(), loadFolders(), loadTags(), loadJobs(), loadMedia()]);
  } catch (err) {
    console.warn(err.message);
  }
}

initMode();
initSystem();
initTags();
initUploader();
initJobs();
initGallery();
initViewer();

on("media:changed", () => {
  refreshMediaData();
});

on("auth:unlocked", () => {
  refreshAll();
});

on("system:refresh", () => {
  refreshAll();
});

(async function init() {
  try {
    await loadHealth();
  } catch (err) {
    console.warn(err.message);
  }
  await import("/static/js/features/pwa.js")
    .then((m) => m.initPwa?.())
    .catch(() => {});
  await refreshAll();
  connectEvents();
  if (location.hash.startsWith("#media=")) {
    const id = location.hash.slice(7);
    if (id) openViewer(id, { pushHistory: false }).catch(() => {});
  }
})();
