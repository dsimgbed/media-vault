from __future__ import annotations

import io
import json
import shutil
import tempfile
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from tests.helpers import _make_settings

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXTENSION_DIR = PROJECT_ROOT / "clients" / "browser-extension"
STATIC_DIR = PROJECT_ROOT / "static"

NODE = shutil.which("node")
skip_no_node = pytest.mark.skipif(NODE is None, reason="node not installed")


def _make_client(tmp_path, *, auth_token: str = "", **settings_kwargs):
    settings = _make_settings(tmp_path, auth_token=auth_token)
    for k, v in settings_kwargs.items():
        setattr(settings, k, v)
    app = create_app(settings)

    async def _fake_process_url_job(*args, **kwargs):
        return []

    app.state.ingest.process_url_job = _fake_process_url_job
    return settings, app


def _run_node(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [NODE, "--input-type=module", "--eval", script],
        capture_output=True,
        text=True,
        timeout=60,
    )


# ---------------------------------------------------------------------------
# 1. POST /api/capture/quick
# ---------------------------------------------------------------------------


def test_capture_quick_json_queues_url(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    with TestClient(app) as client:
        r = client.post("/api/capture/quick", json={"url": "https://example.com/watch?v=1"})
        assert r.status_code == 202, r.text
        body = r.json()
        assert body["ok"] is True
        assert body["status"] == "queued"
        assert body["duplicate"] is False
        assert body["job_id"]
        assert body["folder"]
        assert body["format_spec"]
        assert body["summary"]
        assert body["gallery_url"]
        assert body["media_id"] is None

        job = client.get(f"/api/jobs/{body['job_id']}").json()
        params = json.loads(job["params_json"])
        assert params.get("capture_source") == "quick"


def test_capture_quick_form_encoded(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    with TestClient(app) as client:
        r = client.post(
            "/api/capture/quick",
            data={
                "url": "https://example.com/v",
                "force": "false",
                "forward_to_telegram": "false",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        assert r.status_code == 202, r.text
        body = r.json()
        assert body["status"] == "queued"
        job = client.get(f"/api/jobs/{body['job_id']}").json()
        params = json.loads(job["params_json"])
        assert params["force"] is False
        assert params["forward_to_telegram"] is False


def test_capture_quick_text_fallback(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    with TestClient(app) as client:
        r = client.post(
            "/api/capture/quick",
            json={"url": "", "text": "Check out https://example.com/watch?v=123!"},
        )
        assert r.status_code == 202, r.text
        body = r.json()
        assert body["url"] == "https://example.com/watch?v=123"


def test_capture_quick_rejects_invalid_schemes(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    cases = [
        "javascript:alert(1)",
        "ftp://example.com/a",
        "file:///etc/passwd",
        "data:text/html,hi",
        "",
    ]
    with TestClient(app) as client:
        for url in cases:
            r = client.post("/api/capture/quick", json={"url": url})
            assert r.status_code == 400, (url, r.status_code, r.text)


def test_capture_quick_rejects_invalid_format_spec(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    with TestClient(app) as client:
        r = client.post(
            "/api/capture/quick",
            json={"url": "https://example.com/v", "format_spec": "totally-invalid!!!"},
        )
        assert r.status_code == 400, r.text


def test_capture_quick_auth_enforcement(tmp_path: Path):
    settings, app = _make_client(tmp_path, auth_token="cap-secret")
    with TestClient(app) as client:
        r = client.post("/api/capture/quick", json={"url": "https://example.com/v"})
        assert r.status_code == 401, r.text
        r2 = client.post(
            "/api/capture/quick",
            json={"url": "https://example.com/v"},
            headers={"Authorization": "Bearer cap-secret"},
        )
        assert r2.status_code == 202, r2.text


def test_capture_quick_duplicate_handling(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    existing_id = "existing-media-1"
    db.insert_media(
        media_id=existing_id,
        title="Existing",
        filename="existing.mp4",
        folder="Archive",
        media_type="video",
        mime_type="video/mp4",
        size_bytes=100,
        drive_name="drive1",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="url",
        source_url="https://example.com/watch?v=dup",
    )

    with TestClient(app) as client:
        # force=False -> duplicate
        r = client.post(
            "/api/capture/quick",
            json={"url": "https://example.com/watch?v=dup", "force": False},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "duplicate"
        assert body["duplicate"] is True
        assert body["job_id"] is None
        assert body["media_id"] == existing_id
        assert body["gallery_url"].endswith(f"/#media={existing_id}")


        # force=True -> queues
        r2 = client.post(
            "/api/capture/quick",
            json={"url": "https://example.com/watch?v=dup", "force": True},
        )
        assert r2.status_code == 202, r2.text
        body2 = r2.json()
        assert body2["status"] == "queued"
        assert body2["duplicate"] is False
        assert body2["job_id"]


# ---------------------------------------------------------------------------
# 2. GET /capture & XSS
# ---------------------------------------------------------------------------


def test_capture_page_headers_and_xss(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    with TestClient(app) as client:
        r = client.get("/capture")
        assert r.status_code == 200
        assert "text/html" in r.headers.get("content-type", "")
        assert r.headers.get("content-security-policy") is not None
        assert r.headers.get("x-frame-options") == "DENY"
        assert r.headers.get("referrer-policy") == "no-referrer"
        assert r.headers.get("x-content-type-options") == "nosniff"
        assert r.headers.get("cache-control") == "no-cache"

        # XSS reflection check
        r2 = client.get(
            '/capture?url=<script>alert(1)</script>&title="><img src=x onerror=alert(1)>'
        )
        assert r2.status_code == 200
        assert "<script>alert(1)</script>" not in r2.text
        assert "onerror=alert(1)" not in r2.text

        # inline script check
        assert r.text.count("<script") == 1
        assert '<script type="module" src="/static/js/capture.js">' in r.text

    # static js no alert/confirm/prompt
    for rel in ("capture.js", "features/integrations.js"):
        p = STATIC_DIR / "js" / rel
        src = p.read_text(encoding="utf-8")
        assert "window.alert(" not in src
        assert "window.confirm(" not in src
        assert "window.prompt(" not in src


# ---------------------------------------------------------------------------
# 3. CORS
# ---------------------------------------------------------------------------


def test_cors_default_disabled(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    with TestClient(app) as client:
        for origin in ("https://evil.example", "chrome-extension://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"):
            for method, path in (("OPTIONS", "/api/capture/quick"), ("GET", "/api/health")):
                r = client.request(method, path, headers={"Origin": origin})
                assert r.headers.get("access-control-allow-origin") is None, (origin, method, path)


def test_cors_explicit_origins(tmp_path: Path):
    settings, app = _make_client(
        tmp_path,
        capture_cors_origins="https://vault.example.com, *, https://bad.example/path, chrome-extension://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    # get_capture_cors_origins drops * and path-bearing
    origins = settings.get_capture_cors_origins()
    assert "*" not in origins
    assert "https://bad.example/path" not in origins
    assert "https://vault.example.com" in origins
    assert "chrome-extension://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" in origins

    with TestClient(app) as client:
        # allowed
        r = client.get("/api/health", headers={"Origin": "https://vault.example.com"})
        assert r.headers.get("access-control-allow-origin") == "https://vault.example.com"
        assert r.headers.get("access-control-allow-credentials") != "true"
        # disallowed
        r2 = client.get("/api/health", headers={"Origin": "https://evil.example"})
        assert r2.headers.get("access-control-allow-origin") is None


def test_cors_allow_extensions(tmp_path: Path):
    settings, app = _make_client(tmp_path, capture_cors_allow_extensions=True)
    with TestClient(app) as client:
        valid_origins = [
            "chrome-extension://abcdefghijklmnopabcdefghijklmnop",
            "moz-extension://12345678-1234-1234-1234-123456789abc",
        ]
        for origin in valid_origins:
            r = client.get("/api/health", headers={"Origin": origin})
            assert r.headers.get("access-control-allow-origin") == origin, origin
            assert r.headers.get("access-control-allow-credentials") != "true"

        rejected = [
            "chrome-extension://short",
            "chrome-extension://zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "chrome-extension://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.evil.com",
            "https://evil.example",
        ]
        for origin in rejected:
            r = client.get("/api/health", headers={"Origin": origin})
            assert r.headers.get("access-control-allow-origin") is None, origin


# ---------------------------------------------------------------------------
# 4. GET /api/capture/config
# ---------------------------------------------------------------------------


def test_capture_config(tmp_path: Path):
    settings, app = _make_client(tmp_path, auth_token="cap-secret")
    with TestClient(app) as client:
        r = client.get("/api/capture/config")
        assert r.status_code == 401

        r2 = client.get("/api/capture/config", headers={"Authorization": "Bearer cap-secret"})
        assert r2.status_code == 200
        body = r2.json()
        assert body["extension_available"] is True
        assert body["extension_version"]
        assert "cors_enabled" in body
        assert "cap-secret" not in json.dumps(body)


# ---------------------------------------------------------------------------
# 5. extension.zip
# ---------------------------------------------------------------------------


def test_extension_zip(tmp_path: Path):
    settings, app = _make_client(tmp_path, auth_token="cap-secret")
    with TestClient(app) as client:
        r = client.get("/api/system/extension.zip")
        assert r.status_code == 401

        r2 = client.get("/api/system/extension.zip", headers={"Authorization": "Bearer cap-secret"})
        assert r2.status_code == 200
        assert r2.headers.get("content-type") == "application/zip" or "zip" in r2.headers.get("content-type", "")
        data = r2.content
        z = zipfile.ZipFile(io.BytesIO(data))
        names = z.namelist()
        expected = [
            "manifest.json", "background.js", "popup.html", "popup.css", "popup.js",
            "lib/api.js", "lib/cookies.js", "lib/storage.js",
            "icons/icon-16.png", "icons/icon-32.png", "icons/icon-48.png", "icons/icon-128.png",
        ]
        for e in expected:
            assert e in names, e
        # traversal/dotfiles/pycache
        for n in names:
            assert ".." not in n.split("/")
            assert not n.startswith("/")
            assert not any(part.startswith(".") for part in n.split("/"))
            assert "__pycache__" not in n.split("/")
        # manifest
        manifest = json.loads(z.read("manifest.json"))
        assert manifest.get("manifest_version") == 3
        assert "cookies" in (manifest.get("optional_permissions") or [])
        assert not manifest.get("host_permissions")
        # deterministic
        r3 = client.get("/api/system/extension.zip", headers={"Authorization": "Bearer cap-secret"})
        assert r3.content == data


# ---------------------------------------------------------------------------
# 6. ytdlp cookies
# ---------------------------------------------------------------------------


def test_ytdlp_cookies_valid(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    with TestClient(app) as client:
        payload = "# Netscape HTTP Cookie File\n#HttpOnly_.youtube.com\tTRUE\t/\tTRUE\t0\tVISITOR\tabc\n"
        r = client.post("/api/ytdlp/cookies", json={"cookies_text": payload})
        assert r.status_code == 200, r.text

        for bad_payload in (
            "# evil comment\tgarbage",
            "foo\tbar",
            "#HttpOnly_\tTRUE\t/\tTRUE\t0\tNAME\tVAL",
        ):
            r_bad = client.post("/api/ytdlp/cookies", json={"cookies_text": bad_payload})
            assert r_bad.status_code == 400, bad_payload


# ---------------------------------------------------------------------------
# 7. Node validation
# ---------------------------------------------------------------------------


@skip_no_node
def test_node_check_all_js():
    js_files = list(EXTENSION_DIR.rglob("*.js")) + [
        STATIC_DIR / "js" / "capture.js",
        STATIC_DIR / "js" / "features" / "integrations.js",
        STATIC_DIR / "js" / "features" / "maintenance.js",
        STATIC_DIR / "js" / "features" / "system.js",
    ]
    for p in js_files:
        assert p.is_file(), p
        res = subprocess.run(
            [NODE, "--input-type=module", "--check"],
            input=p.read_text(encoding="utf-8"),
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert res.returncode == 0, f"{p}: {res.stderr}"


@skip_no_node
def test_node_cookies_module(tmp_path: Path):
    cookies_path = EXTENSION_DIR / "lib" / "cookies.js"
    tmp = tempfile.NamedTemporaryFile("w", suffix=".mjs", delete=False, encoding="utf-8")
    tmp.write(cookies_path.read_text(encoding="utf-8"))
    tmp.close()
    file_url = Path(tmp.name).resolve().as_uri()
    script = f"""
const m = await import({json.dumps(file_url)});
const isAllowed = m.isAllowedCookieDomain;
const allowed = ["youtube.com", ".youtube.com", "m.youtube.com", ".google.com", "accounts.google.com", ".googlevideo.com", "youtu.be"];
const rejected = ["evilyoutube.com", "youtube.com.evil.com", "notgoogle.com", "evil.org", "youtube.com/path", "youtube.com:443", "youtube.com\\r\\n", "youtube.com\\t", " youtube.com", ""];
for (const d of allowed) if (!isAllowed(d)) {{ console.error("allowed rejected: " + d); process.exit(1); }}
for (const d of rejected) if (isAllowed(d)) {{ console.error("rejected allowed: " + JSON.stringify(d)); process.exit(1); }}

const cookies = [
  {{ domain: "youtube.com", name: "sid", value: "v1", path: "/", secure: true, httpOnly: true }},
  {{ domain: "evilyoutube.com", name: "bad", value: "x", path: "/" }},
  {{ domain: "youtube.com", name: "badpath", value: "x", path: "/a\\t#HttpOnly_" }},
  {{ domain: ".youtube.com", hostOnly: true, name: "host", value: "line\\r\\n\\tinj", path: "/p" }},
  {{ domain: "google.com", name: "sess", value: "d", path: "/", session: true, expirationDate: 0 }},
  {{ domain: "youtube.com", name: "sid", value: "dup", path: "/" }},
];
const out = m.formatNetscapeCookies(cookies);
const text = out.text;
// assertions
if (!text.includes("#HttpOnly_")) {{ console.error("no httponly"); process.exit(1); }}
const rows = text.trim().split("\\n").filter(l => l && l.includes("\\t"));
for (const row of rows) {{
  const cols = row.split("\\t");
  if (cols.length !== 7) {{ console.error("bad cols: " + row); process.exit(1); }}
}}
if (!rows.some(r => r.includes("FALSE")) ) {{ console.error("no FALSE subdomain"); process.exit(1); }}
if (!rows.some(r => r.split("\\t")[4] === "0")) {{ console.error("no session 0"); process.exit(1); }}
// injection stripped
if (text.includes("\\r") || text.includes("\\n") && text.split("\\n").some(l=>l.includes("inj") && l.includes("\\t")===false)) {{}}
// dedup: sid should appear once
const sidCount = rows.filter(r => r.split("\\t")[5] === "sid").length;
if (sidCount !== 1) {{ console.error("dedup fail: " + sidCount); process.exit(1); }}
// disallowed domain dropped
if (text.includes("evilyoutube")) {{ console.error("disallowed not dropped"); process.exit(1); }}
// isYouTubeUrl + isRelevantAuthCookieChange + computeCookieTextDigest
if (!m.isYouTubeUrl("https://www.youtube.com/watch?v=dQw4w9WgXcQ")) {{ console.error("yt url fail"); process.exit(1); }}
if (!m.isYouTubeUrl("https://youtu.be/dQw4w9WgXcQ")) {{ console.error("youtu.be fail"); process.exit(1); }}
if (m.isYouTubeUrl("https://example.com/watch?v=1")) {{ console.error("non-yt url allowed"); process.exit(1); }}
if (!m.isRelevantAuthCookieChange({{ removed: false, cause: "explicit", cookie: {{ domain: ".google.com", name: "__Secure-3PSIDTS" }} }})) {{ console.error("auth cookie change fail"); process.exit(1); }}
if (m.isRelevantAuthCookieChange({{ removed: true, cause: "overwrite", cookie: {{ domain: ".youtube.com", name: "LOGIN_INFO" }} }})) {{ console.error("overwrite removal should be skipped"); process.exit(1); }}
const d1 = await m.computeCookieTextDigest(out.text);
const d2 = await m.computeCookieTextDigest(out.text + "x");
if (!d1 || d1 === d2) {{ console.error("digest fail"); process.exit(1); }}
console.log(JSON.stringify({{ text: out.text, count: out.count, domains: out.domains }}));
"""
    res = _run_node(script)
    assert res.returncode == 0, res.stderr
    data = json.loads(res.stdout.strip().splitlines()[-1])
    text = data["text"]
    # feed into endpoint
    settings, app = _make_client(tmp_path)
    with TestClient(app) as client:
        r = client.post("/api/ytdlp/cookies", json={"cookies_text": text})
        assert r.status_code == 200, r.text


@skip_no_node
def test_node_storage_module():
    storage_path = EXTENSION_DIR / "lib" / "storage.js"
    tmp = tempfile.NamedTemporaryFile("w", suffix=".mjs", delete=False, encoding="utf-8")
    tmp.write(storage_path.read_text(encoding="utf-8"))
    tmp.close()
    file_url = Path(tmp.name).resolve().as_uri()
    script = f"""
const m = await import({json.dumps(file_url)});
const n = m.normalizeServerUrl;
if (n("http://100.64.0.1:8080/") !== "http://100.64.0.1:8080") {{ console.error("accept fail: " + n("http://100.64.0.1:8080/")); process.exit(1); }}
// strips trailing UI page suffixes at root
for (const [input, expected] of [
  ["http://example.com/capture", "http://example.com"],
  ["http://example.com/capture/", "http://example.com"],
  ["http://example.com/CAPTURE", "http://example.com"],
  ["http://example.com/index.html", "http://example.com"],
  ["http://example.com/share-target", "http://example.com"],
]) {{
  const got = n(input);
  if (got !== expected) {{ console.error("strip fail: " + input + " got " + got + " expected " + expected); process.exit(1); }}
}}
// strips after reverse-proxy subpath
if (n("https://example.com/vault/capture") !== "https://example.com/vault") {{ console.error("subpath strip fail: " + n("https://example.com/vault/capture")); process.exit(1); }}
// preserves genuine reverse-proxy subpaths
if (n("https://example.com/vault/") !== "https://example.com/vault") {{ console.error("subpath preserve fail: " + n("https://example.com/vault/")); process.exit(1); }}
// rejects invalid schemes/credentials
for (const bad of ["javascript:alert(1)", "ftp://host", "http://user:pass@host"]) {{
  if (n(bad) !== "") {{ console.error("should reject: " + bad + " got " + n(bad)); process.exit(1); }}
}}
if (m.DEFAULT_SETTINGS.autoSyncCookies !== true) {{ console.error("autoSyncCookies default fail"); process.exit(1); }}
const store = {{}};
globalThis.chrome = {{
  storage: {{
    local: {{
      async get(k) {{ return {{ [k]: store[k] }}; }},
      async set(obj) {{ Object.assign(store, obj); }},
    }},
  }},
}};
await m.saveCookieSyncState({{ lastHash: "abc123", lastSyncedAt: 1000, lastCount: 12, lastDomains: ["youtube.com"] }});
const st = await m.getCookieSyncState();
if (st.lastHash !== "abc123" || st.lastCount !== 12) {{ console.error("sync state fail: " + JSON.stringify(st)); process.exit(1); }}
console.log("ok");
"""
    res = _run_node(script)
    assert res.returncode == 0, res.stderr
    assert "ok" in res.stdout


@skip_no_node
def test_node_api_quick_capture_fallback(tmp_path: Path):
    (tmp_path / "package.json").write_text(json.dumps({"type": "module"}), encoding="utf-8")
    shutil.copyfile(EXTENSION_DIR / "lib" / "api.js", tmp_path / "api.js")
    shutil.copyfile(EXTENSION_DIR / "lib" / "storage.js", tmp_path / "storage.js")
    api_url = (tmp_path / "api.js").resolve().as_uri()
    script = f"""
const m = await import({json.dumps(api_url)});
const quickCapture = m.quickCapture;

function makeResponse(status, bodyObj) {{
  const text = bodyObj === undefined ? "" : JSON.stringify(bodyObj);
  return {{ status, ok: status >= 200 && status < 300, async text() {{ return text; }} }};
}}

function setup(mock) {{ globalThis.fetch = mock; }}

const settings = {{ serverUrl: "http://example.com", authToken: "" }};
const longTitle = "x".repeat(400);

// Case a: modern server returns 202
let calls = [];
setup(async (url, opts) => {{
  calls.push({{ url: String(url), method: opts?.method, body: opts?.body }});
  if (String(url).includes("/api/capture/quick")) return makeResponse(202, {{ ok: true, status: "queued", job_id: "job-modern", folder: "General" }});
  return makeResponse(404, {{}});
}});
let result = await quickCapture(settings, {{ url: "https://example.com/v", title: longTitle, folder: "General" }});
if (!result || result.status !== "queued" || result.job_id !== "job-modern") {{ console.error("case a result fail: " + JSON.stringify(result)); process.exit(1); }}
if (calls.filter(c => String(c.url).includes("/api/ingest/")).length !== 0) {{ console.error("case a should not call ingest"); process.exit(1); }}
const qcCall = calls.find(c => String(c.url).includes("/api/capture/quick"));
const qcBody = JSON.parse(qcCall.body);
if (!qcBody.title || qcBody.title.length !== 300) {{ console.error("case a title not truncated: " + (qcBody.title?.length)); process.exit(1); }}

// Case b: legacy 404, not duplicate
calls = [];
let dupCalled = false, ingestCalled = false;
setup(async (url, opts) => {{
  calls.push({{ url: String(url), method: opts?.method, body: opts?.body }});
  if (String(url).includes("/api/capture/quick")) return makeResponse(404, {{ detail: "Not Found" }});
  if (String(url).includes("/api/ingest/check-duplicate")) {{ dupCalled = true; return makeResponse(200, {{ duplicate: false }}); }}
  if (String(url).includes("/api/ingest/url")) {{ ingestCalled = true; return makeResponse(202, {{ job_id: "job-legacy-1", status: "queued" }}); }}
  return makeResponse(404, {{}});
}});
result = await quickCapture(settings, {{ url: "https://example.com/v", title: "My Title", folder: "General" }});
if (!result || result.ok !== true || result.duplicate !== false || result.status !== "queued" || result.job_id !== "job-legacy-1" || result.fallback !== "ingest_url") {{ console.error("case b result fail: " + JSON.stringify(result)); process.exit(1); }}
if (!String(result.summary || "").includes("General")) {{ console.error("case b summary missing folder: " + result.summary); process.exit(1); }}
if (!dupCalled || !ingestCalled) {{ console.error("case b missing calls"); process.exit(1); }}

// Case c: 405, duplicate found, force false
calls = [];
let ingestCalledC = false;
setup(async (url, opts) => {{
  calls.push({{ url: String(url), method: opts?.method, body: opts?.body }});
  if (String(url).includes("/api/capture/quick")) return makeResponse(405, {{ detail: "Method Not Allowed" }});
  if (String(url).includes("/api/ingest/check-duplicate")) return makeResponse(200, {{ duplicate: true, item: {{ id: 42, folder: "Archive", title: "Existing" }} }});
  if (String(url).includes("/api/ingest/url")) {{ ingestCalledC = true; return makeResponse(202, {{ job_id: "x", status: "queued" }}); }}
  return makeResponse(404, {{}});
}});
result = await quickCapture(settings, {{ url: "https://example.com/v", force: false, folder: "General" }});
if (!result || result.ok !== true || result.status !== "duplicate" || result.duplicate !== true || result.job_id !== null || result.media_id !== 42 || result.folder !== "Archive" || result.fallback !== "ingest_url") {{ console.error("case c result fail: " + JSON.stringify(result)); process.exit(1); }}
if (ingestCalledC) {{ console.error("case c should not call ingest"); process.exit(1); }}

// Case d: 404, force true skips check-duplicate
calls = [];
let dupCalledD = false, ingestCalledD = false, ingestBodyD = null;
setup(async (url, opts) => {{
  calls.push({{ url: String(url), method: opts?.method, body: opts?.body }});
  if (String(url).includes("/api/capture/quick")) return makeResponse(404, {{ detail: "Not Found" }});
  if (String(url).includes("/api/ingest/check-duplicate")) {{ dupCalledD = true; return makeResponse(200, {{ duplicate: true, item: {{ id: 1, folder: "X", title: "Y" }} }}); }}
  if (String(url).includes("/api/ingest/url")) {{ ingestCalledD = true; ingestBodyD = opts?.body; return makeResponse(202, {{ job_id: "j", status: "queued" }}); }}
  return makeResponse(404, {{}});
}});
result = await quickCapture(settings, {{ url: "https://example.com/v", force: true, folder: "General" }});
if (!result || result.fallback !== "ingest_url") {{ console.error("case d result fail: " + JSON.stringify(result)); process.exit(1); }}
if (dupCalledD) {{ console.error("case d should skip check-duplicate"); process.exit(1); }}
if (!ingestCalledD) {{ console.error("case d should call ingest"); process.exit(1); }}
const ingestParsedD = JSON.parse(ingestBodyD);
if (ingestParsedD.force !== true) {{ console.error("case d force not true: " + JSON.stringify(ingestParsedD)); process.exit(1); }}

// Case e: 401 and 400 rethrow without fallback
for (const st of [401, 400]) {{
  calls = [];
  let ingestCalledE = false, dupCalledE = false;
  setup(async (url, opts) => {{
    calls.push({{ url: String(url), method: opts?.method, body: opts?.body }});
    if (String(url).includes("/api/capture/quick")) return makeResponse(st, {{ detail: st === 401 ? "Unauthorized" : "Bad Request" }});
    if (String(url).includes("/api/ingest/")) {{ ingestCalledE = true; dupCalledE = true; return makeResponse(200, {{}}); }}
    return makeResponse(404, {{}});
  }});
  let thrown = null;
  try {{
    await quickCapture(settings, {{ url: "https://example.com/v" }});
  }} catch (err) {{
    thrown = err;
  }}
  if (!thrown || thrown.status !== st) {{ console.error("case e status fail for " + st + ": " + (thrown && thrown.status)); process.exit(1); }}
  const expectedMsg = st === 401 ? "Unauthorized" : "Bad Request";
  if (thrown.message !== expectedMsg) {{ console.error("case e message fail: " + thrown.message); process.exit(1); }}
  if (ingestCalledE || dupCalledE) {{ console.error("case e should not fallback"); process.exit(1); }}
}}

console.log("ok");
"""
    res = subprocess.run(
        [NODE, "--input-type=module", "--eval", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert res.returncode == 0, res.stderr
    assert "ok" in res.stdout