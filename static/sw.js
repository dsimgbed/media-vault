const CACHE_NAME = "media-vault-shell-v1";
const SHELL_ASSETS = [
  "/",
  "/manifest.webmanifest",
  "/manifest.json",
  "/static/css/base.css",
  "/static/css/layout.css",
  "/static/css/components.css",
  "/static/css/mobile.css",
  "/static/js/main.js",
];

const IDB_NAME = "media_vault_pwa";
const IDB_VERSION = 1;
const IDB_STORE = "shared_items";

function openPwaDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, IDB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(IDB_STORE)) {
        db.createObjectStore(IDB_STORE, { keyPath: "id" });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function saveSharedItem(payload) {
  const db = await openPwaDb();
  try {
    await new Promise((resolve, reject) => {
      const tx = db.transaction(IDB_STORE, "readwrite");
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error || new Error("IndexedDB transaction aborted"));
      tx.objectStore(IDB_STORE).put(payload);
    });
  } finally {
    db.close();
  }
}

function strField(val) {
  return typeof val === "string" ? val : "";
}

async function handleShareTarget(request) {
  try {
    const formData = await request.formData();
    const title = strField(formData.get("title"));
    const text = strField(formData.get("text"));
    const sharedUrl = strField(formData.get("url"));
    const files = formData
      .getAll("files")
      .filter((f) => f && typeof f === "object" && f.size > 0);
    const now = Date.now();
    await saveSharedItem({
      id: now,
      title,
      text,
      url: sharedUrl,
      files,
      createdAt: now,
    });
    return Response.redirect("/?mode=uploader&shared=1", 303);
  } catch (_) {
    return Response.redirect("/?mode=uploader&share=fallback", 303);
  }
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches
      .open(CACHE_NAME)
      .then((cache) => cache.addAll(SHELL_ASSETS).catch(() => {}))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
      )
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;

  if (url.pathname === "/share-target" && event.request.method === "POST") {
    event.respondWith(handleShareTarget(event.request));
    return;
  }

  // Strict API, Range streaming, and non-GET bypass
  if (event.request.method !== "GET") return;
  if (url.pathname === "/api" || url.pathname.startsWith("/api/")) return;
  if (event.request.headers.has("range")) return;

  const isShellOrStatic =
    url.pathname === "/" ||
    url.pathname.startsWith("/static/") ||
    url.pathname.startsWith("/manifest.");
  if (!isShellOrStatic) return;

  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response && response.status === 200) {
          const copy = response.clone();
          caches
            .open(CACHE_NAME)
            .then((cache) => cache.put(event.request, copy))
            .catch(() => {});
        }
        return response;
      })
      .catch(async () => {
        const cached = await caches.match(event.request);
        if (cached) return cached;
        if (event.request.mode === "navigate" || url.pathname === "/") {
          const shell = await caches.match("/");
          if (shell) return shell;
        }
        return Response.error();
      })
  );
});
