from __future__ import annotations

import asyncio
import base64
import io
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from tests.helpers import _make_settings

DAV = "{DAV:}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_webdav_client(tmp_path: Path, *, auth_token: str = "", **overrides: Any):
    settings = _make_settings(tmp_path, auth_token=auth_token)
    for key, value in overrides.items():
        setattr(settings, key, value)
    app = create_app(settings)

    async def _noop_process_url_job(*args, **kwargs):
        return []

    app.state.ingest.process_url_job = _noop_process_url_job
    return settings, app


def _insert_local_media(
    app,
    *,
    media_id: str,
    filename: str,
    folder: str,
    content: bytes,
    mime_type: str = "application/octet-stream",
    media_type: str = "file",
    title: str | None = None,
) -> dict[str, Any]:
    drive_path = Path(app.state.settings.get_drives()[0].path)
    target_dir = drive_path / "webdav-test"
    target_dir.mkdir(parents=True, exist_ok=True)
    file_path = target_dir / f"{media_id}-{filename.replace('/', '_')}"
    file_path.write_bytes(content)
    return app.state.db.insert_media(
        media_id=media_id,
        title=title or filename,
        filename=filename,
        folder=folder,
        media_type=media_type,
        mime_type=mime_type,
        size_bytes=len(content),
        drive_name="drive1",
        file_path=str(file_path),
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
    )


def _insert_telegram_media(
    app,
    *,
    media_id: str,
    filename: str,
    folder: str,
    content: bytes,
    mime_type: str = "video/mp4",
    media_type: str = "video",
    chat_id: str = "-100123",
    msg_id: int = 101,
    worker_name: str = "bot1",
) -> dict[str, Any]:
    return app.state.db.insert_media(
        media_id=media_id,
        title=filename,
        filename=filename,
        folder=folder,
        media_type=media_type,
        mime_type=mime_type,
        size_bytes=len(content),
        drive_name="telegram",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
        telegram_chat_id=chat_id,
        telegram_topic_id=77,
        telegram_msg_id=msg_id,
        telegram_worker_name=worker_name,
    )


def _insert_multipart_telegram_media(
    app,
    *,
    media_id: str,
    filename: str,
    folder: str,
    part_contents: list[bytes],
    mime_type: str = "video/mp4",
    media_type: str = "video",
    chat_id: str = "-100123",
    base_msg_id: int = 500,
) -> tuple[dict[str, Any], bytes]:
    full = b"".join(part_contents)
    parts = []
    offset = 0
    for idx, part in enumerate(part_contents):
        end = offset + len(part) - 1
        parts.append(
            {
                "part_index": idx,
                "start_byte": offset,
                "end_byte": end,
                "size_bytes": len(part),
                "telegram_chat_id": chat_id,
                "telegram_topic_id": 77,
                "telegram_msg_id": base_msg_id + idx,
                "telegram_worker_name": "bot1",
            }
        )
        offset += len(part)
    item = app.state.db.insert_media(
        media_id=media_id,
        title=filename,
        filename=filename,
        folder=folder,
        media_type=media_type,
        mime_type=mime_type,
        size_bytes=len(full),
        drive_name="telegram",
        file_path="",
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
        telegram_chat_id=chat_id,
        telegram_topic_id=77,
        telegram_msg_id=base_msg_id,
        telegram_worker_name="bot1",
        parts=parts,
    )
    return item, full


def _propfind(
    client: TestClient,
    path: str,
    *,
    depth: str = "1",
    body: str | bytes = "",
    headers: dict[str, str] | None = None,
    auth: tuple[str, str] | None = None,
):
    req_headers = {"Depth": depth}
    if headers:
        req_headers.update(headers)
    data = body.encode("utf-8") if isinstance(body, str) else body
    return client.request("PROPFIND", path, content=data, headers=req_headers, auth=auth)


def _parse_multistatus(xml_bytes: bytes) -> ET.Element:
    root = ET.fromstring(xml_bytes)
    assert root.tag == f"{DAV}multistatus"
    return root


def _responses_by_href(root: ET.Element) -> dict[str, ET.Element]:
    out: dict[str, ET.Element] = {}
    for resp in root.findall(f"{DAV}response"):
        href_el = resp.find(f"{DAV}href")
        assert href_el is not None and href_el.text
        out[href_el.text] = resp
    return out


def _propstat_statuses(resp: ET.Element) -> dict[str, list[str]]:
    statuses: dict[str, list[str]] = {}
    for ps in resp.findall(f"{DAV}propstat"):
        status_el = ps.find(f"{DAV}status")
        status = (status_el.text or "").strip() if status_el is not None else ""
        prop = ps.find(f"{DAV}prop")
        if prop is None:
            continue
        for child in list(prop):
            local = child.tag.rsplit("}", 1)[-1]
            statuses.setdefault(local, []).append(status)
    return statuses


def _prop_text(resp: ET.Element, local_name: str) -> str | None:
    for ps in resp.findall(f"{DAV}propstat"):
        prop = ps.find(f"{DAV}prop")
        if prop is None:
            continue
        for child in list(prop):
            if child.tag.rsplit("}", 1)[-1] == local_name:
                return child.text
    return None


def _has_prop(resp: ET.Element, local_name: str) -> bool:
    for ps in resp.findall(f"{DAV}propstat"):
        prop = ps.find(f"{DAV}prop")
        if prop is None:
            continue
        for child in list(prop):
            if child.tag.rsplit("}", 1)[-1] == local_name:
                return True
    return False


async def _raw_asgi_request(
    app,
    method: str,
    raw_path: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes = b"",
) -> httpx.Response:
    raw_headers: list[tuple[bytes, bytes]] = []
    for k, v in (headers or {}).items():
        raw_headers.append((k.lower().encode("latin-1"), str(v).encode("latin-1")))
    if body and not any(k == b"content-length" for k, _ in raw_headers):
        raw_headers.append((b"content-length", str(len(body)).encode("ascii")))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.1"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": "http",
        "path": raw_path.split("?", 1)[0],
        "raw_path": raw_path.encode("utf-8", errors="surrogateescape"),
        "query_string": raw_path.split("?", 1)[1].encode("latin-1") if "?" in raw_path else b"",
        "headers": raw_headers,
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
    }

    req_sent = False
    status_code = 500
    resp_headers: list[tuple[bytes, bytes]] = []
    body_parts: list[bytes] = []

    async def receive():
        nonlocal req_sent
        if req_sent:
            return {"type": "http.disconnect"}
        req_sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        nonlocal status_code, resp_headers
        if message["type"] == "http.response.start":
            status_code = int(message["status"])
            resp_headers = list(message.get("headers") or [])
        elif message["type"] == "http.response.body":
            body_parts.append(bytes(message.get("body") or b""))

    await app(scope, receive, send)
    return httpx.Response(
        status_code=status_code,
        headers=[(k.decode("latin-1"), v.decode("latin-1")) for k, v in resp_headers],
        content=b"".join(body_parts),
        request=httpx.Request(method, f"http://testserver{raw_path}"),
    )


def _raw_asgi_request_sync(app, method: str, raw_path: str, **kwargs) -> httpx.Response:
    return asyncio.run(_raw_asgi_request(app, method, raw_path, **kwargs))


# ---------------------------------------------------------------------------
# 1. Authentication & Safe Defaults
# ---------------------------------------------------------------------------


def test_webdav_disabled_returns_404(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_enabled=False)
    with TestClient(app) as client:
        for method, path in [
            ("OPTIONS", "/webdav"),
            ("PROPFIND", "/webdav/"),
            ("GET", "/webdav/"),
            ("HEAD", "/webdav/"),
            ("GET", "/webdav.m3u"),
        ]:
            res = client.request(method, path)
            assert res.status_code == 404, (method, path, res.text)


def test_webdav_fail_closed_default_requires_auth(tmp_path: Path):
    _, app = _make_webdav_client(
        tmp_path,
        auth_token="",
        webdav_password="",
        webdav_allow_anonymous=False,
    )
    with TestClient(app) as client:
        for method, path in [("OPTIONS", "/webdav/"), ("PROPFIND", "/webdav/"), ("GET", "/webdav/"), ("HEAD", "/webdav/")]:
            res = client.request(method, path)
            assert res.status_code == 401, (method, path, res.text)
            assert res.headers.get("www-authenticate") == 'Basic realm="Media Vault", charset="UTF-8"'
        m3u = client.get("/webdav.m3u")
        assert m3u.status_code == 401
        assert m3u.headers.get("www-authenticate") == 'Basic realm="Media Vault", charset="UTF-8"'


def test_webdav_explicit_anonymous_opt_in(tmp_path: Path):
    _, app = _make_webdav_client(
        tmp_path,
        auth_token="",
        webdav_password="",
        webdav_allow_anonymous=True,
    )
    with TestClient(app) as client:
        assert client.request("OPTIONS", "/webdav/").status_code == 200
        prop = _propfind(client, "/webdav/", depth="0")
        assert prop.status_code == 207
        assert client.get("/webdav/").status_code == 200
        assert client.head("/webdav/").status_code == 200
        assert client.get("/webdav.m3u").status_code == 200


def test_webdav_token_fallback_enforced_even_if_anonymous_flag_true(tmp_path: Path):
    _, app = _make_webdav_client(
        tmp_path,
        auth_token="tok123",
        webdav_password="",
        webdav_allow_anonymous=True,
    )
    good = ("vault", "tok123")
    with TestClient(app) as client:
        # anonymous flag NEVER bypasses configured token/password
        assert client.get("/webdav/").status_code == 401
        assert client.get("/webdav/", headers={"Authorization": "Bearer tok123"}).status_code == 401
        assert client.get("/webdav/", auth=("vault", "wrong")).status_code == 401
        assert client.get("/webdav/", auth=good).status_code == 200
        prop = _propfind(client, "/webdav/", depth="0", auth=good)
        assert prop.status_code == 207
        assert client.get("/webdav.m3u", auth=good).status_code == 200


def test_webdav_dedicated_password_overrides_token_and_supports_colon(tmp_path: Path):
    _, app = _make_webdav_client(
        tmp_path,
        auth_token="tok123",
        webdav_password="p:ass:word",
        webdav_username="alice",
        webdav_allow_anonymous=True,
    )
    with TestClient(app) as client:
        # token should not work when dedicated password is set
        assert client.get("/webdav/", auth=("alice", "tok123")).status_code == 401
        # wrong user
        assert client.get("/webdav/", auth=("vault", "p:ass:word")).status_code == 401
        # correct dedicated password with colons (RFC 7617)
        ok = client.get("/webdav/", auth=("alice", "p:ass:word"))
        assert ok.status_code == 200

        # invalid base64 / missing colon
        bad_headers = [
            {"Authorization": "Basic not-base64!!!"},
            {"Authorization": "Basic " + base64.b64encode(b"nocolon").decode("ascii")},
        ]
        for hdrs in bad_headers:
            res = client.get("/webdav/", headers=hdrs)
            assert res.status_code == 401, hdrs


def test_webdav_empty_username_allows_any_username_with_valid_password(tmp_path: Path):
    _, app = _make_webdav_client(
        tmp_path,
        webdav_password="secret",
        webdav_username="",
        webdav_allow_anonymous=False,
    )
    with TestClient(app) as client:
        assert client.get("/webdav/", auth=("anyuser", "secret")).status_code == 200
        assert client.get("/webdav/", auth=("another", "secret")).status_code == 200
        assert client.get("/webdav/", auth=("anyuser", "wrong")).status_code == 401


def test_api_auth_behavior_untouched_by_webdav_config(tmp_path: Path):
    _, app = _make_webdav_client(
        tmp_path,
        auth_token="apitok",
        webdav_password="davpass",
        webdav_username="davuser",
        webdav_allow_anonymous=False,
    )
    with TestClient(app) as client:
        # API still uses bearer web_auth_token, not webdav basic creds
        assert client.get("/api/health").status_code == 200  # health is public
        assert client.get("/api/folders").status_code == 401
        assert client.get("/api/folders", auth=("davuser", "davpass")).status_code == 401
        ok = client.get("/api/folders", headers={"Authorization": "Bearer apitok"})
        assert ok.status_code == 200, ok.text
        assert "folders" in ok.json()


# ---------------------------------------------------------------------------
# 2. RFC 4918 OPTIONS, PROPFIND, and Write-Method Rejections
# ---------------------------------------------------------------------------


def test_webdav_options_no_redirect_and_headers(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    with TestClient(app, follow_redirects=False) as client:
        for path in ["/webdav", "/webdav/"]:
            res = client.options(path)
            assert res.status_code == 200, (path, res.status_code, res.text)
            assert res.headers.get("dav") == "1"
            assert res.headers.get("allow") == "OPTIONS, GET, HEAD, PROPFIND"
            assert res.headers.get("ms-author-via") == "DAV"


def test_propfind_depth_and_modes(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    _insert_local_media(app, media_id="m1", filename="a.txt", folder="Docs", content=b"AAA", mime_type="text/plain")
    _insert_local_media(app, media_id="m2", filename="b.txt", folder="Docs/Sub", content=b"BBB", mime_type="text/plain")

    with TestClient(app) as client:
        # root Depth 0
        res0 = _propfind(client, "/webdav/", depth="0")
        assert res0.status_code == 207
        assert res0.headers["content-type"].startswith("application/xml")
        root = _parse_multistatus(res0.content)
        hrefs0 = list(_responses_by_href(root).keys())
        assert hrefs0 == ["/webdav/"]

        # root Depth 1 - target + direct children, not grandchildren
        res1 = _propfind(client, "/webdav/", depth="1")
        root1 = _parse_multistatus(res1.content)
        hrefs1 = sorted(_responses_by_href(root1).keys())
        assert "/webdav/" in hrefs1
        assert "/webdav/Docs/" in hrefs1
        assert "/webdav/Docs/Sub/" not in hrefs1
        assert "/webdav/Docs/a.txt" not in hrefs1

        # folder Depth 1
        res_docs = _propfind(client, "/webdav/Docs/", depth="1")
        hrefs_docs = sorted(_responses_by_href(_parse_multistatus(res_docs.content)).keys())
        assert "/webdav/Docs/" in hrefs_docs
        assert "/webdav/Docs/Sub/" in hrefs_docs
        assert "/webdav/Docs/a.txt" in hrefs_docs
        assert "/webdav/Docs/Sub/b.txt" not in hrefs_docs

        # Depth infinity -> 403 with finite-depth error
        res_inf = _propfind(client, "/webdav/", depth="infinity")
        assert res_inf.status_code == 403
        err = ET.fromstring(res_inf.content)
        assert err.tag == f"{DAV}error"
        assert err.find(f"{DAV}propfind-finite-depth") is not None

        # Invalid depth
        res_bad = _propfind(client, "/webdav/", depth="2")
        assert res_bad.status_code == 400

        # allprop by empty body
        res_empty = _propfind(client, "/webdav/Docs/", depth="0", body="")
        resp_empty = _responses_by_href(_parse_multistatus(res_empty.content))["/webdav/Docs/"]
        for name in ["displayname", "resourcetype", "getlastmodified", "creationdate", "getetag"]:
            assert _has_prop(resp_empty, name)

        # explicit allprop
        res_all = _propfind(
            client, "/webdav/Docs/a.txt", depth="0",
            body='<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>',
        )
        resp_all = _responses_by_href(_parse_multistatus(res_all.content))["/webdav/Docs/a.txt"]
        assert _prop_text(resp_all, "getcontentlength") == "3"
        assert _prop_text(resp_all, "getcontenttype") == "text/plain"

        # propname
        res_name = _propfind(
            client, "/webdav/Docs/a.txt", depth="0",
            body='<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:propname/></D:propfind>',
        )
        resp_name = _responses_by_href(_parse_multistatus(res_name.content))["/webdav/Docs/a.txt"]
        # propname returns names with no values
        assert _has_prop(resp_name, "displayname")
        assert _prop_text(resp_name, "displayname") in (None, "")

        # selective prop with found and unknown
        res_prop = _propfind(
            client, "/webdav/Docs/a.txt", depth="0",
            body=(
                '<?xml version="1.0"?>'
                '<D:propfind xmlns:D="DAV:"><D:prop>'
                '<D:displayname/><D:getcontentlength/><Z:custom xmlns:Z="urn:x"/>'
                '</D:prop></D:propfind>'
            ),
        )
        resp_prop = _responses_by_href(_parse_multistatus(res_prop.content))["/webdav/Docs/a.txt"]
        statuses = _propstat_statuses(resp_prop)
        assert any("200" in s for s in statuses.get("displayname", []))
        assert any("200" in s for s in statuses.get("getcontentlength", []))
        assert any("404" in s for s in statuses.get("custom", []))


def test_propfind_xxe_dtd_and_size_limit(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    with TestClient(app) as client:
        bad_bodies = [
            b'<?xml version="1.0"?><!DOCTYPE foo [<!ENTITY xxe "boom">]><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>',
            b'<?xml version="1.0"?><!ENTITY x "x"><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>',
            b"<D:propfind xmlns:D=\"DAV:\"><D:allprop/></D:propfind>" + (b" " * 70000),
        ]
        for body in bad_bodies:
            res = _propfind(client, "/webdav/", depth="0", body=body)
            assert res.status_code == 400, (res.status_code, res.text)


def test_propfind_special_unicode_emoji_filenames_and_exact_get(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    names_and_content = [
        ("a & b <c>.txt", b"amp"),
        ('quote"single\' .bin', b"quote"),
        ("space name #1?.mp4", b"space"),
        ("CJK_日本語_中文.mp4", b"cjk"),
        ("accenté café.txt", b"accent"),
        ("🎬 movie.mp4", b"emoji"),
    ]
    for i, (name, content) in enumerate(names_and_content):
        _insert_local_media(
            app,
            media_id=f"sp{i}",
            filename=name,
            folder="Special",
            content=content,
            mime_type="application/octet-stream",
            media_type="file",
        )

    with TestClient(app) as client:
        res = _propfind(client, "/webdav/Special/", depth="1")
        assert res.status_code == 207
        # must be parseable XML
        root = _parse_multistatus(res.content)
        hrefs = _responses_by_href(root)
        file_hrefs = [h for h in hrefs.keys() if not h.endswith("/Special/")]
        assert len(file_hrefs) == len(names_and_content)
        for href in file_hrefs:
            # href should be percent-encoded (no raw spaces/control/unicode in raw form except encoded)
            assert " " not in href
            assert "<" not in href
            assert ">" not in href
            # GET using the exact <D:href> from PROPFIND returns 200 with exact bytes
            got = client.get(href)
            assert got.status_code == 200, (href, got.status_code)
        # Verify exact round-trip by matching content set
        got_contents = {client.get(h).content for h in file_hrefs}
        assert got_contents == {c for _, c in names_and_content}


def test_write_and_lock_methods_rejected_and_require_auth(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, auth_token="tok", webdav_password="", webdav_allow_anonymous=False)
    write_methods = ["PUT", "POST", "DELETE", "PATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK", "PROPPATCH"]
    with TestClient(app) as client:
        for method in write_methods:
            # unauthenticated -> 401
            unauth = client.request(method, "/webdav/")
            assert unauth.status_code == 401, (method, unauth.status_code)
            assert unauth.headers.get("www-authenticate") == 'Basic realm="Media Vault", charset="UTF-8"'
            # authenticated -> 405 with Allow
            auth_res = client.request(method, "/webdav/", auth=("vault", "tok"))
            assert auth_res.status_code == 405, (method, auth_res.status_code, auth_res.text)
            assert auth_res.headers.get("allow") == "OPTIONS, GET, HEAD, PROPFIND"


# ---------------------------------------------------------------------------
# 3. Nested Folders & Deterministic Collision Resolution
# ---------------------------------------------------------------------------


def test_nested_folders_and_case_insensitive_resolution(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    _insert_local_media(app, media_id="n1", filename="movie.mkv", folder="Movies/Sci-Fi", content=b"scifi")

    with TestClient(app) as client:
        root = _parse_multistatus(_propfind(client, "/webdav/", depth="1").content)
        root_hrefs = sorted(_responses_by_href(root).keys())
        assert "/webdav/Movies/" in root_hrefs

        movies = _parse_multistatus(_propfind(client, "/webdav/Movies/", depth="1").content)
        movies_hrefs = sorted(_responses_by_href(movies).keys())
        assert "/webdav/Movies/Sci-Fi/" in movies_hrefs

        leaf = _parse_multistatus(_propfind(client, "/webdav/Movies/Sci-Fi/", depth="1").content)
        leaf_hrefs = sorted(_responses_by_href(leaf).keys())
        assert "/webdav/Movies/Sci-Fi/movie.mkv" in leaf_hrefs

        # case-insensitive
        ci = _propfind(client, "/webdav/movies/sci-fi/", depth="1")
        assert ci.status_code == 207
        ci_hrefs = sorted(_responses_by_href(_parse_multistatus(ci.content)).keys())
        assert any(h.lower().endswith("/movies/sci-fi/movie.mkv") for h in ci_hrefs)


def test_collision_resolution_deterministic_and_round_trip(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    # Two items same name, plus case-colliding, and a file colliding with child folder name
    # Insert in non-sorted order to test determinism
    _insert_local_media(app, media_id="id-b", filename="video.mp4", folder="Collide", content=b"BBB")
    _insert_local_media(app, media_id="id-a", filename="video.mp4", folder="Collide", content=b"AAA")
    _insert_local_media(app, media_id="id-c", filename="Video.MP4", folder="Collide", content=b"CCC")
    # file colliding with child folder name
    _insert_local_media(app, media_id="id-folder", filename="Reports", folder="Collide", content=b"FOLDERFILE")
    _insert_local_media(app, media_id="id-child", filename="inside.txt", folder="Collide/Reports", content=b"CHILD")

    with TestClient(app) as client:
        res = _propfind(client, "/webdav/Collide/", depth="1")
        assert res.status_code == 207
        root = _parse_multistatus(res.content)
        hrefs = sorted(_responses_by_href(root).keys())

        # child folder must exist
        assert "/webdav/Collide/Reports/" in hrefs
        # colliding file with folder name must be decorated
        reports_files = [h for h in hrefs if "Reports" in h and not h.endswith("/Reports/")]
        assert len(reports_files) == 1
        assert "id-folder" in reports_files[0]

        # video collisions: all three should be present, deterministic, decorated
        video_hrefs = [h for h in hrefs if "video" in h.lower() or "Video" in h]
        assert len(video_hrefs) == 3
        # all should have media_id decoration because of collision
        assert all("id-a" in h or "id-b" in h or "id-c" in h for h in video_hrefs)
        # deterministic ordering/naming
        assert video_hrefs == sorted(video_hrefs)

        # 1:1 round-trip: fetch EVERY href via GET returns exact corresponding payload
        payloads_by_id = {
            "id-a": b"AAA",
            "id-b": b"BBB",
            "id-c": b"CCC",
            "id-folder": b"FOLDERFILE",
        }
        seen_ids = set()
        for href in hrefs:
            if href.endswith("/"):
                continue
            got = client.get(href)
            assert got.status_code == 200, (href, got.status_code)
            # identify which id this href belongs to
            matched = [mid for mid in payloads_by_id if mid in href]
            assert len(matched) == 1, (href, matched)
            mid = matched[0]
            assert got.content == payloads_by_id[mid]
            seen_ids.add(mid)
        assert seen_ids == set(payloads_by_id.keys())

        # child folder file also reachable
        child_res = _propfind(client, "/webdav/Collide/Reports/", depth="1")
        child_hrefs = _responses_by_href(_parse_multistatus(child_res.content))
        child_file_hrefs = [h for h in child_hrefs if h.endswith("inside.txt")]
        assert len(child_file_hrefs) == 1
        got_child = client.get(child_file_hrefs[0])
        assert got_child.status_code == 200
        assert got_child.content == b"CHILD"


# ---------------------------------------------------------------------------
# 4. Streaming (HEAD & GET) & Zero-MTProto Invariant
# ---------------------------------------------------------------------------


def test_local_file_head_get_range_and_conditional(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    content = b"0123456789abcdef"  # 16 bytes
    _insert_local_media(
        app, media_id="loc1", filename="file.bin", folder="Files",
        content=content, mime_type="application/octet-stream",
    )

    with TestClient(app) as client:
        head = client.head("/webdav/Files/file.bin")
        assert head.status_code == 200
        assert head.headers.get("content-length") == str(len(content))
        assert head.headers.get("accept-ranges") == "bytes"
        assert head.headers.get("etag")
        assert head.headers.get("last-modified")
        assert head.headers.get("content-type") == "application/octet-stream"
        assert head.content == b""

        full = client.get("/webdav/Files/file.bin")
        assert full.status_code == 200
        assert full.content == content

        # single byte range
        r1 = client.get("/webdav/Files/file.bin", headers={"Range": "bytes=2-2"})
        assert r1.status_code == 206
        assert r1.content == b"2"
        assert r1.headers.get("content-range") == f"bytes 2-2/{len(content)}"

        # suffix range
        rs = client.get("/webdav/Files/file.bin", headers={"Range": "bytes=-5"})
        assert rs.status_code == 206
        assert rs.content == content[-5:]
        assert rs.headers.get("content-range") == f"bytes {len(content)-5}-{len(content)-1}/{len(content)}"

        # open-ended
        ro = client.get("/webdav/Files/file.bin", headers={"Range": "bytes=10-"})
        assert ro.status_code == 206
        assert ro.content == content[10:]

        # case-insensitive BYTES
        rc = client.get("/webdav/Files/file.bin", headers={"Range": "BYTES=0-3"})
        assert rc.status_code == 206
        assert rc.content == content[0:4]

        # out of bounds
        r416 = client.get("/webdav/Files/file.bin", headers={"Range": "bytes=999-1000"})
        assert r416.status_code == 416
        assert r416.headers.get("content-range") == f"bytes */{len(content)}"

        # If-None-Match
        etag = head.headers["etag"]
        inm = client.get("/webdav/Files/file.bin", headers={"If-None-Match": etag})
        assert inm.status_code == 304


def test_single_part_telegram_zero_mtproto_and_streaming(tmp_path: Path, monkeypatch):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    content = bytes(range(256)) * 4  # 1024 bytes
    _insert_telegram_media(
        app, media_id="tg1", filename="clip.mp4", folder="Telegram",
        content=content, mime_type="video/mp4", media_type="video",
    )

    calls = {"prepare": 0, "iter": 0}

    async def fake_prepare(chat_id: str, msg_id: int, worker_name: str | None = None):
        calls["prepare"] += 1
        return object(), {"chat_id": chat_id, "msg_id": msg_id}

    async def fake_iter(client, media, start: int, end: int):
        calls["iter"] += 1
        yield content[start : end + 1]

    monkeypatch.setattr(app.state.tg_pool, "prepare_stream", fake_prepare)
    monkeypatch.setattr(app.state.tg_pool, "iter_telegram_range", fake_iter)

    with TestClient(app) as client:
        # PROPFIND (root and folder) + HEAD (full and ranged) -> 0 prepare_stream
        r_root = _propfind(client, "/webdav/", depth="1")
        assert r_root.status_code == 207
        r_folder = _propfind(client, "/webdav/Telegram/", depth="1")
        assert r_folder.status_code == 207
        h_full = client.head("/webdav/Telegram/clip.mp4")
        assert h_full.status_code == 200
        assert h_full.headers.get("content-length") == str(len(content))
        h_range = client.head("/webdav/Telegram/clip.mp4", headers={"Range": "bytes=0-9"})
        assert h_range.status_code == 206
        assert h_range.headers.get("content-range") == f"bytes 0-9/{len(content)}"
        assert calls["prepare"] == 0, f"prepare_stream called during PROPFIND/HEAD: {calls}"

        # GET full 200
        g_full = client.get("/webdav/Telegram/clip.mp4")
        assert g_full.status_code == 200
        assert g_full.content == content
        assert calls["iter"] >= 1

        # GET ranged 206
        g_range = client.get("/webdav/Telegram/clip.mp4", headers={"Range": "bytes=100-199"})
        assert g_range.status_code == 206
        assert g_range.content == content[100:200]
        assert g_range.headers.get("content-range") == f"bytes 100-199/{len(content)}"

        # 416
        g_416 = client.get("/webdav/Telegram/clip.mp4", headers={"Range": "bytes=99999-100000"})
        assert g_416.status_code == 416
        assert g_416.headers.get("content-range") == f"bytes */{len(content)}"

        # If-None-Match -> 304 with 0 prepare_stream calls added
        etag = h_full.headers["etag"]
        prep_before = calls["prepare"]
        g_304 = client.get("/webdav/Telegram/clip.mp4", headers={"If-None-Match": etag})
        assert g_304.status_code == 304
        assert calls["prepare"] == prep_before, f"prepare_stream called on 304: {calls}"


def test_multipart_telegram_zero_mtproto_and_cross_part_range(tmp_path: Path, monkeypatch):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    part0 = b"A" * 100
    part1 = b"B" * 80
    item, full = _insert_multipart_telegram_media(
        app, media_id="tg-mp", filename="big.mp4", folder="Multi",
        part_contents=[part0, part1], mime_type="video/mp4",
    )

    calls = {"prepare": 0, "iter_mp": 0}

    async def fake_prepare(chat_id: str, msg_id: int, worker_name: str | None = None):
        calls["prepare"] += 1
        return object(), {"chat_id": chat_id, "msg_id": msg_id}

    async def fake_iter_mp(parts, start: int, end: int, **kwargs):
        calls["iter_mp"] += 1
        # stitch from known full content respecting requested range
        yield full[start : end + 1]

    monkeypatch.setattr(app.state.tg_pool, "prepare_stream", fake_prepare)
    monkeypatch.setattr(app.state.tg_pool, "iter_telegram_multipart_range", fake_iter_mp)

    with TestClient(app) as client:
        # PROPFIND and HEAD -> 0 prepare_stream
        assert _propfind(client, "/webdav/", depth="1").status_code == 207
        assert _propfind(client, "/webdav/Multi/", depth="1").status_code == 207
        h = client.head("/webdav/Multi/big.mp4")
        assert h.status_code == 200
        assert h.headers.get("content-length") == str(len(full))
        hr = client.head("/webdav/Multi/big.mp4", headers={"Range": "bytes=90-110"})
        assert hr.status_code == 206
        assert calls["prepare"] == 0, f"prepare_stream called during PROPFIND/HEAD: {calls}"

        # GET full
        g_full = client.get("/webdav/Multi/big.mp4")
        assert g_full.status_code == 200
        assert g_full.content == full

        # cross-part range spanning part 0 into part 1
        g_cross = client.get("/webdav/Multi/big.mp4", headers={"Range": "bytes=90-110"})
        assert g_cross.status_code == 206
        assert g_cross.content == full[90:111]
        assert g_cross.headers.get("content-range") == f"bytes 90-110/{len(full)}"
        assert calls["iter_mp"] >= 1


def test_zero_byte_file_and_missing_local_file(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    _insert_local_media(
        app, media_id="z1", filename="empty.bin", folder="Edge",
        content=b"", mime_type="application/octet-stream",
    )
    # missing local file, no telegram backing
    drive_path = Path(app.state.settings.get_drives()[0].path)
    missing_path = drive_path / "webdav-test" / "gone.bin"
    missing_path.parent.mkdir(parents=True, exist_ok=True)
    # do not create the file
    app.state.db.insert_media(
        media_id="miss1",
        title="gone.bin",
        filename="gone.bin",
        folder="Edge",
        media_type="file",
        mime_type="application/octet-stream",
        size_bytes=10,
        drive_name="drive1",
        file_path=str(missing_path),
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
    )

    with TestClient(app) as client:
        # zero-byte
        gz = client.get("/webdav/Edge/empty.bin")
        assert gz.status_code == 200
        assert gz.content == b""
        rz = client.get("/webdav/Edge/empty.bin", headers={"Range": "bytes=0-0"})
        assert rz.status_code == 416

        # missing
        hm = client.head("/webdav/Edge/gone.bin")
        assert hm.status_code == 404
        gm = client.get("/webdav/Edge/gone.bin")
        assert gm.status_code == 404


# ---------------------------------------------------------------------------
# 5. Path Traversal & Security
# ---------------------------------------------------------------------------


def test_path_traversal_and_malformed_paths_rejected(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    with TestClient(app) as client:
        # Raw ASGI requests to preserve exact encoding
        bad_paths = [
            "/webdav/%2e%2e/%2e%2e/etc/passwd",
            "/webdav/..%2f..%2fetc/passwd",
            "/webdav/%2e%2e%2f%2e%2e%2fetc%2fpasswd",
            "/webdav/foo%5c..%5cbar",
            "/webdav/foo%00bar",
            "/webdav/%01bad",
        ]
        for path in bad_paths:
            res = _raw_asgi_request_sync(app, "GET", path)
            assert res.status_code == 400, (path, res.status_code, res.text)
            res_head = _raw_asgi_request_sync(app, "HEAD", path)
            assert res_head.status_code == 400, (path, res_head.status_code)

        # also PROPFIND should reject traversal
        res_pf = _raw_asgi_request_sync(app, "PROPFIND", "/webdav/%2e%2e/", headers={"Depth": "0"})
        assert res_pf.status_code == 400


def test_collection_html_escapes_filenames(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    _insert_local_media(
        app, media_id="xss1", filename="<script>alert(1)</script>.txt", folder="XSS",
        content=b"xss", mime_type="text/plain",
    )

    with TestClient(app) as client:
        root_html = client.get("/webdav/")
        assert root_html.status_code == 200
        assert "text/html" in root_html.headers.get("content-type", "")
        assert "<script>alert(1)</script>" not in root_html.text
        assert "XSS/" in root_html.text or "XSS" in root_html.text

        dir_html = client.get("/webdav/XSS/")
        assert dir_html.status_code == 200
        assert "<script>" not in dir_html.text
        assert "&lt;script&gt;" in dir_html.text
        assert "&lt;/script&gt;" in dir_html.text or "&lt;_script&gt;" in dir_html.text


# ---------------------------------------------------------------------------
# 6. M3U Playlist Bridge & Status/UI Endpoints
# ---------------------------------------------------------------------------


def test_m3u_playlist_auth_filters_and_canonical_urls(tmp_path: Path):
    _, app = _make_webdav_client(
        tmp_path, auth_token="tok", webdav_password="", webdav_allow_anonymous=False
    )
    # colliding names in same folder, tag filter selects only one
    _insert_local_media(app, media_id="v1", filename="video.mp4", folder="Movies", content=b"V1", mime_type="video/mp4", media_type="video")
    app.state.db.insert_media(
        media_id="v2",
        title="video.mp4",
        filename="video.mp4",
        folder="Movies",
        media_type="video",
        mime_type="video/mp4",
        size_bytes=2,
        drive_name="drive1",
        file_path=str((Path(app.state.settings.get_drives()[0].path) / "webdav-test" / "v2-video.mp4")),
        thumb_filename=None,
        source_origin="web",
        source_kind="upload",
        source_url=None,
        tags="fav",
    )
    (Path(app.state.settings.get_drives()[0].path) / "webdav-test" / "v2-video.mp4").write_bytes(b"V2")
    _insert_local_media(app, media_id="a1", filename="song.mp3", folder="Music", content=b"A1", mime_type="audio/mpeg", media_type="audio")

    good = ("vault", "tok")
    with TestClient(app) as client:
        assert client.get("/webdav.m3u").status_code == 401
        assert client.head("/webdav.m3u").status_code == 401

        res = client.get("/webdav.m3u", auth=good)
        assert res.status_code == 200
        body = res.text
        assert "#EXTM3U" in body
        assert "#EXTINF:" in body

        # filter by folder
        res_folder = client.get("/webdav.m3u?folder=Movies", auth=good)
        assert res_folder.status_code == 200
        assert "/webdav/Movies/" in res_folder.text
        assert "/webdav/Music/" not in res_folder.text

        # filter by media_type
        res_audio = client.get("/webdav.m3u?media_type=audio", auth=good)
        assert res_audio.status_code == 200
        assert "song.mp3" in res_audio.text
        assert "video.mp4" not in res_audio.text

        # filter by tag - only v2 is in playlist; URL must be canonical and GET 200
        res_tag = client.get("/webdav.m3u?tag=fav", auth=good)
        assert res_tag.status_code == 200
        lines = [ln.strip() for ln in res_tag.text.splitlines() if ln.strip() and not ln.startswith("#")]
        # only the tagged colliding file
        assert len(lines) == 1
        url = lines[0]
        assert url.startswith("http://testserver/webdav/Movies/")
        assert "v2" in url
        # exact canonical URL returns 200
        href = url.replace("http://testserver", "")
        got = client.get(href, auth=good)
        assert got.status_code == 200
        assert got.content == b"V2"

        # also verify unfiltered playlist includes canonical colliding URLs that resolve
        all_lines = [ln.strip() for ln in res.text.splitlines() if ln.strip() and not ln.startswith("#")]
        movie_urls = [ln for ln in all_lines if "/Movies/" in ln]
        assert len(movie_urls) == 2
        payloads = set()
        for murl in movie_urls:
            mhref = murl.replace("http://testserver", "")
            mgot = client.get(mhref, auth=good)
            assert mgot.status_code == 200
            payloads.add(mgot.content)
        assert payloads == {b"V1", b"V2"}


def test_m3u_head(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    _insert_local_media(app, media_id="h1", filename="a.mp4", folder="M", content=b"A", mime_type="video/mp4", media_type="video")
    with TestClient(app) as client:
        head = client.head("/webdav.m3u")
        assert head.status_code == 200
        assert "audio/x-mpegurl" in head.headers.get("content-type", "")
        assert int(head.headers.get("content-length", "0")) > 0
        assert head.content == b""


def test_status_endpoints_and_integrations_js_webdav_hooks(tmp_path: Path):
    _, app = _make_webdav_client(
        tmp_path,
        auth_token="apitok",
        webdav_password="davsecret",
        webdav_username="alice",
        webdav_allow_anonymous=False,
    )

    with TestClient(app) as client:
        health = client.get("/api/health")
        assert health.status_code == 200
        h = health.json()
        assert h["webdav_enabled"] is True
        assert h["webdav_auth_required"] is True
        assert h["webdav_configured"] is True
        # must not leak secrets/custom username
        health_text = health.text.lower()
        assert "davsecret" not in health_text
        assert "apitok" not in health_text
        assert "alice" not in health_text

        auth_headers = {"Authorization": "Bearer apitok"}
        sys_status = client.get("/api/system/status", headers=auth_headers)
        assert sys_status.status_code == 200
        ss = sys_status.json()
        assert "webdav" in ss
        assert ss["webdav"]["enabled"] is True
        assert ss["webdav"]["read_only"] is True
        ss_text = sys_status.text.lower()
        assert "davsecret" not in ss_text
        assert "apitok" not in ss_text

        cap = client.get("/api/capture/config", headers=auth_headers)
        assert cap.status_code == 200
        cc = cap.json()
        assert "webdav" in cc
        assert cc["webdav"]["enabled"] is True
        cap_text = cap.text.lower()
        assert "davsecret" not in cap_text
        assert "apitok" not in cap_text

        js = client.get("/static/js/features/integrations.js")
        assert js.status_code == 200
        assert "webdav-url" in js.text
        assert "webdav-m3u" in js.text
        assert "webdav-rclone" in js.text
        assert "webdav-vlc" in js.text


def test_propfind_bounded_stream_rejects_oversized_chunked_body_without_content_length(
    tmp_path: Path,
):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)

    chunks_sent = 0

    async def _run_raw_streamed_propfind() -> int:
        nonlocal chunks_sent
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "PROPFIND",
            "scheme": "http",
            "path": "/webdav/",
            "raw_path": b"/webdav/",
            "root_path": "",
            "query_string": b"",
            "headers": [(b"depth", b"0")],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }

        async def receive():
            nonlocal chunks_sent
            chunks_sent += 1
            if chunks_sent <= 20:
                return {"type": "http.request", "body": b"A" * 16384, "more_body": True}
            return {"type": "http.request", "body": b"", "more_body": False}

        status_holder: list[int] = []

        async def send(message):
            if message["type"] == "http.response.start":
                status_holder.append(int(message["status"]))

        await app(scope, receive, send)
        return status_holder[0]

    import asyncio

    with TestClient(app):
        status = asyncio.run(_run_raw_streamed_propfind())
    assert status == 400
    # Must stop reading as soon as 64 KiB is exceeded (5 chunks of 16 KiB = 80 KiB),
    # well before consuming all 20 chunks (320 KiB).
    assert chunks_sent <= 6


def test_m3u_playlist_under_reverse_proxy_root_path_has_no_duplicate_prefix(
    tmp_path: Path,
):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    _insert_local_media(
        app,
        media_id="rp01",
        folder="Movies",
        filename="proxy_test.mp4",
        content=b"0123456789",
        title="Proxy\x07\x00\nClip",
    )

    with TestClient(app, root_path="/vault") as client:
        res = client.get("/webdav.m3u")
        assert res.status_code == 200
        assert "/vault/vault/" not in res.text
        assert "http://testserver/vault/webdav/Movies/proxy_test.mp4" in res.text
        # Control chars stripped from #EXTINF title
        assert "\x07" not in res.text
        assert "\x00" not in res.text
        assert "#EXTINF:-1,Proxy Clip" in res.text


def test_webdav_if_range_matching_and_mismatching(tmp_path: Path):
    _, app = _make_webdav_client(tmp_path, webdav_allow_anonymous=True)
    payload = b"0123456789abcdef"
    _insert_local_media(
        app,
        media_id="ifr01",
        folder="Movies",
        filename="range_test.bin",
        content=payload,
        mime_type="application/octet-stream",
    )

    with TestClient(app) as client:
        head = client.head("/webdav/Movies/range_test.bin")
        assert head.status_code == 200
        etag = head.headers["etag"]
        last_mod = head.headers["last-modified"]

        # Strong ETag match -> 206 Partial Content
        r_match = client.get(
            "/webdav/Movies/range_test.bin",
            headers={"Range": "bytes=0-3", "If-Range": etag},
        )
        assert r_match.status_code == 206
        assert r_match.content == b"0123"

        # Last-Modified match -> 206 Partial Content
        r_lm = client.get(
            "/webdav/Movies/range_test.bin",
            headers={"Range": "bytes=4-7", "If-Range": last_mod},
        )
        assert r_lm.status_code == 206
        assert r_lm.content == b"4567"

        # Mismatched ETag -> ignore Range, return full 200
        r_mismatch = client.get(
            "/webdav/Movies/range_test.bin",
            headers={"Range": "bytes=0-3", "If-Range": '"stale-etag"'},
        )
        assert r_mismatch.status_code == 200
        assert r_mismatch.content == payload

        # Weak ETag -> never matches If-Range per RFC 9110 -> full 200
        r_weak = client.get(
            "/webdav/Movies/range_test.bin",
            headers={"Range": "bytes=0-3", "If-Range": f"W/{etag}"},
        )
        assert r_weak.status_code == 200
        assert r_weak.content == payload

