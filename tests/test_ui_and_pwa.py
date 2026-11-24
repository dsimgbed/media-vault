from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app
from tests.helpers import (
    STATIC_DIR,
    _insert_tagged_media,
    _make_settings,
    _ui_source,
)


def test_ui_served_and_url_validation(tmp_path: Path):
    settings = _make_settings(tmp_path)
    app = create_app(settings)

    with TestClient(app) as client:
        home = client.get("/")
        assert home.status_code == 200
        assert "Media Vault" in home.text

        bad = client.post("/api/ingest/url", json={"url": "ftp://example.com/file.mp4"})
        assert bad.status_code == 400


def test_module1_frontend_modularization_mode_switcher_and_dialogs(tmp_path: Path):
    settings = _make_settings(tmp_path)
    app = create_app(settings)

    index_file = STATIC_DIR / "index.html"
    assert index_file.is_file()
    assert len(index_file.read_text(encoding="utf-8").splitlines()) < 350

    with TestClient(app) as client:
        index_res = client.get("/")
        assert index_res.status_code == 200
        assert index_res.headers.get("cache-control") == "no-cache"
        assert '<script type="module" src="/static/js/main.js"></script>' in index_res.text

        for css_rel in ("css/base.css", "css/layout.css", "css/components.css", "css/mobile.css"):
            res = client.get(f"/static/{css_rel}")
            assert res.status_code == 200, css_rel
            assert res.headers.get("cache-control") == "no-cache"

        for js_rel in (
            "js/main.js",
            "js/state.js",
            "js/api.js",
            "js/bus.js",
            "js/mode.js",
            "js/utils.js",
            "js/ui/dialogs.js",
            "js/ui/tag-picker.js",
            "js/features/system.js",
            "js/features/drives.js",
            "js/features/tags.js",
            "js/features/uploader.js",
            "js/features/jobs.js",
            "js/features/gallery.js",
            "js/features/cinema.js",
            "js/features/viewer.js",
            "js/features/zip-explorer.js",
            "js/features/pwa.js",
        ):
            res = client.get(f"/static/{js_rel}")
            assert res.status_code == 200, js_rel
            assert "javascript" in res.headers.get("content-type", "")
            assert res.headers.get("cache-control") == "no-cache"
            for forbidden in ("window.alert(", "window.confirm(", "window.prompt("):
                assert forbidden not in res.text, f"{forbidden} found in {js_rel}"

        full_ui = _ui_source(client)
        for token in (
            'data-mode-target="uploader"',
            'data-mode-target="files"',
            'data-mode-target="cinema"',
            "vault_mode",
            "bottom-nav",
            "@media (max-width: 768px)",
            "confirmDialog",
            "promptDialog",
            "alertDialog",
            "authBanner",
            "cinemaStage",
            "cinemaVideo",
            "cinemaQueueList",
            "cinemaTheaterBtn",
        ):
            assert token in full_ui


def test_module1_pwa_manifest_service_worker_share_target_and_mobile_ux(tmp_path: Path):
    settings = _make_settings(tmp_path, auth_token="pwa-secret")
    app = create_app(settings)

    with TestClient(app) as client:
        for manifest_path in ("/manifest.webmanifest", "/manifest.json"):
            m_res = client.get(manifest_path)
            assert m_res.status_code == 200
            assert "application/manifest+json" in m_res.headers.get("content-type", "")
            manifest = m_res.json()
            assert manifest["display"] == "standalone"
            assert manifest["share_target"]["action"] == "/share-target"
            assert manifest["share_target"]["method"] == "POST"
            assert manifest["share_target"]["enctype"] == "multipart/form-data"
            assert any(i["sizes"] == "192x192" for i in manifest["icons"])
            assert any(i["sizes"] == "512x512" for i in manifest["icons"])

        sw_res = client.get("/sw.js")
        assert sw_res.status_code == 200
        assert sw_res.headers.get("service-worker-allowed") == "/"
        assert sw_res.headers.get("cache-control") == "no-cache"
        assert "/share-target" in sw_res.text
        assert 'url.pathname.startsWith("/api/")' in sw_res.text
        assert 'event.request.headers.has("range")' in sw_res.text

        for asset in (
            "/favicon.ico",
            "/static/icons/icon-192.png",
            "/static/icons/icon-512.png",
            "/static/icons/icon.svg",
        ):
            icon_res = client.get(asset)
            assert icon_res.status_code == 200, asset
            assert len(icon_res.content) > 100

        # Share target fallback routes must not bypass auth or ingest unauthenticated data
        get_share = client.get(
            "/share-target?url=https://example.com/watch?v=1&title=Clip",
            follow_redirects=False,
        )
        assert get_share.status_code == 303
        assert "mode=uploader" in get_share.headers["location"]
        assert "shared=1" in get_share.headers["location"]
        assert "example.com" in get_share.headers["location"]

        post_share = client.post(
            "/share-target",
            data={"url": "https://example.com/v.mp4", "title": "Shared"},
            follow_redirects=False,
        )
        assert post_share.status_code == 303
        assert post_share.headers["location"] == "/?mode=uploader&share=fallback"
        assert app.state.db.list_jobs() == []

        full_ui = _ui_source(client)
        for hook in (
            "mediaSession",
            "MediaMetadata",
            "previoustrack",
            "nexttrack",
            "(hover: none)",
            "ingestTagPicker",
            "tagFilterPicker",
            "openTagEditorDialog",
            "openBulkTagDialog",
        ):
            assert hook in full_ui


def test_module1_multi_tag_filtering_and_tag_picker_ui(tmp_path: Path):
    settings = _make_settings(tmp_path)
    app = create_app(settings)
    db = app.state.db

    _insert_tagged_media(db, "m1", "#soap #hd #clip")
    _insert_tagged_media(db, "m2", "#soap #4k")
    _insert_tagged_media(db, "m3", "#hd #archive")

    with TestClient(app) as client:
        res_single = client.get("/api/media?tag=soap").json()
        assert {it["id"] for it in res_single["items"]} == {"m1", "m2"}

        res_multi_tag = client.get("/api/media?tag=soap,hd").json()
        assert [it["id"] for it in res_multi_tag["items"]] == ["m1"]

        res_multi_tags_alias = client.get("/api/media?tags=soap,hd").json()
        assert [it["id"] for it in res_multi_tags_alias["items"]] == ["m1"]

        res_combined = client.get("/api/media?tag=soap&tags=4k").json()
        assert [it["id"] for it in res_combined["items"]] == ["m2"]


def test_v0131_uploader_gallery_controls_sr_only_inputs_and_sw_v7(tmp_path: Path):
    settings = _make_settings(tmp_path)
    app = create_app(settings)

    index_text = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    assert len(index_text.splitlines()) < 350

    with TestClient(app) as client:
        html = client.get("/").text
        # #galleryMaxItems compact numeric input is present alongside #galleryArchiveMode
        assert 'id="galleryMaxItems"' in html
        assert 'min="1"' in html and 'max="2000"' in html and 'value="200"' in html

        # File inputs use visually-hidden .sr-only-input class instead of hidden attribute (Android WebView)
        for input_id in ("fileInput", "folderUploadInput", "torrentFileInput"):
            assert f'id="{input_id}" class="sr-only-input"' in html

        base_css = client.get("/static/css/base.css").text
        assert ".sr-only-input" in base_css
        assert "pointer-events: none;" in base_css

        uploader_js = client.get("/static/js/features/uploader.js").text
        # galleryInstallHint must be hidden when BOTH galleryDlInstalled && galleryDlEnabled are truthy
        assert "installHint.hidden = Boolean(state.galleryDlInstalled && state.galleryDlEnabled)" in uploader_js

        sw_js = client.get("/sw.js").text
        assert 'const CACHE_NAME = "media-vault-shell-v7";' in sw_js


def test_v0131_read_cached_html_caches_by_mtime_and_invalidates(tmp_path: Path):
    import asyncio
    import os
    from app.routers.deps import clear_html_cache, read_cached_html

    clear_html_cache()
    page = tmp_path / "sample.html"
    page.write_text("<h1>v1</h1>", encoding="utf-8")

    first = asyncio.run(read_cached_html(page))
    assert first == "<h1>v1</h1>"
    second = asyncio.run(read_cached_html(page))
    assert second == "<h1>v1</h1>"

    # Update content and bump mtime_ns -> cache must invalidate and return new content
    page.write_text("<h1>v2</h1>", encoding="utf-8")
    st = page.stat()
    os.utime(page, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))
    third = asyncio.run(read_cached_html(page))
    assert third == "<h1>v2</h1>"

    # Deleted file returns None
    page.unlink()
    assert asyncio.run(read_cached_html(page)) is None
    clear_html_cache()

