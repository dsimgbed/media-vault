from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from app.main import create_app
from tests.helpers import STATIC_DIR, _make_settings, _sample_jpeg_bytes


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_client(tmp_path: Path, *, auth_token: str = "", **settings_kwargs):
    settings = _make_settings(tmp_path, auth_token=auth_token)
    for key, value in settings_kwargs.items():
        setattr(settings, key, value)
    app = create_app(settings)
    return settings, app


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _insert_media(
    db,
    tmp_path: Path,
    *,
    media_id: str,
    filename: str,
    payload: bytes,
    title: str | None = None,
    folder: str = "General",
    mime_type: str = "application/octet-stream",
    thumb_filename: str | None = None,
    source_url: str | None = None,
    tags: str | None = None,
    duration_sec: float | None = None,
) -> Path:
    path = tmp_path / "drives" / "drive1" / folder / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    db.insert_media(
        media_id=media_id,
        title=title if title is not None else Path(filename).stem,
        filename=filename,
        folder=folder,
        media_type="video" if mime_type.startswith("video/") else "file",
        mime_type=mime_type,
        size_bytes=len(payload),
        drive_name="drive1",
        file_path=str(path),
        thumb_filename=thumb_filename,
        source_origin="web",
        source_kind="upload",
        source_url=source_url,
        tags=tags,
        duration_sec=duration_sec,
    )
    return path


def _insert_telegram_media(
    db,
    *,
    media_id: str,
    filename: str,
    size_bytes: int,
    chat_id: str = "-100123456",
    msg_id: int = 1001,
    worker_name: str = "bot1",
    mime_type: str = "video/mp4",
) -> None:
    db.insert_media(
        media_id=media_id,
        title=Path(filename).stem,
        filename=filename,
        folder="General",
        media_type="video",
        mime_type=mime_type,
        size_bytes=size_bytes,
        drive_name="telegram",
        file_path="",
        thumb_filename=None,
        source_origin="telegram",
        source_kind="upload",
        source_url=None,
        telegram_chat_id=chat_id,
        telegram_topic_id=1,
        telegram_msg_id=msg_id,
        telegram_worker_name=worker_name,
    )


def _create_room(client: TestClient, headers: dict[str, str] | None = None, **payload):
    resp = client.post(
        "/api/watch-party/rooms",
        json=payload,
        headers=headers or {},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _join(client: TestClient, room_code: str, name: str = "Guest", pin: str | None = None):
    body: dict[str, Any] = {"display_name": name}
    if pin is not None:
        body["pin"] = pin
    return client.post(f"/api/watch-party/{room_code}/join", json=body)


def _recv(ws, expected_type: str | None = None) -> dict[str, Any]:
    msg = ws.receive_json()
    if expected_type is not None:
        assert msg.get("type") == expected_type, msg
    return msg


def _expect_close(ws, code: int) -> None:
    with pytest.raises(WebSocketDisconnect) as exc_info:
        ws.receive_json()
    assert exc_info.value.code == code


def _room_view(client: TestClient, room_code: str, headers: dict[str, str] | None = None):
    resp = client.get(f"/api/watch-party/rooms/{room_code}", headers=headers or {})
    assert resp.status_code == 200, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# 1. Room CRUD & host auth enforcement
# ---------------------------------------------------------------------------


def test_room_crud_requires_web_auth_and_validates_media(tmp_path: Path):
    settings, app = _make_client(tmp_path, auth_token="wp-secret")
    headers = _auth("wp-secret")
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE", mime_type="video/mp4")
    _insert_media(db, tmp_path, media_id="m2", filename="two.mp4", payload=b"TWO", mime_type="video/mp4")

    with TestClient(app) as client:
        payload = {"title": "Auth Room", "media_ids": ["m1", "m2"], "pin": "4321"}

        # Creation requires the application web token when configured.
        assert client.post("/api/watch-party/rooms", json=payload).status_code == 401
        created_resp = client.post("/api/watch-party/rooms", json=payload, headers=headers)
        assert created_resp.status_code == 201, created_resp.text
        room = created_resp.json()
        code = room["room_code"]
        assert room["join_path"] == f"/watch/{code}"
        assert room["join_url"].endswith(f"/watch/{code}")
        assert room["host_token"]
        assert room["requires_pin"] is True
        # PIN must never be returned in plaintext, nor as a digest/salt.
        assert "4321" not in created_resp.text
        for secret_key in ("pin", "pin_hash", "pin_salt", "pin_digest"):
            assert secret_key not in room

        # Every host-management endpoint requires require_auth.
        unauthorized = [
            ("GET", "/api/watch-party/rooms", None),
            ("GET", f"/api/watch-party/rooms/{code}", None),
            ("POST", f"/api/watch-party/rooms/{code}/host-session", {}),
            ("PATCH", f"/api/watch-party/rooms/{code}", {"title": "nope"}),
            ("POST", f"/api/watch-party/rooms/{code}/kick", {"participant_id": "nobody"}),
            ("DELETE", f"/api/watch-party/rooms/{code}", None),
        ]
        for method, path, body in unauthorized:
            resp = client.request(method, path, json=body)
            assert resp.status_code == 401, (method, path, resp.status_code, resp.text)

        # Unknown media ids are rejected at creation time.
        bad = client.post(
            "/api/watch-party/rooms",
            json={"media_ids": ["does-not-exist"]},
            headers=headers,
        )
        assert bad.status_code in (400, 404), bad.text

        assert client.get("/api/watch-party/rooms", headers=headers).json()["rooms"]
        view = _room_view(client, code, headers)
        assert "4321" not in json.dumps(view)
        assert "pin_hash" not in view and "pin_salt" not in view

        session = client.post(
            f"/api/watch-party/rooms/{code}/host-session",
            json={"display_name": "Second Host"},
            headers=headers,
        )
        assert session.status_code == 200, session.text
        assert session.json()["host_token"] and session.json()["role"] == "host"

        guest = _join(client, code, name="Kick Me", pin="4321")
        assert guest.status_code == 200, guest.text
        kicked = client.post(
            f"/api/watch-party/rooms/{code}/kick",
            json={"participant_id": guest.json()["participant_id"]},
            headers=headers,
        )
        assert kicked.status_code == 200, kicked.text
        assert kicked.json()["kicked"] is True

        deleted = client.delete(f"/api/watch-party/rooms/{code}", headers=headers)
        assert deleted.status_code == 200 and deleted.json()["closed"] is True
        assert client.get(f"/api/watch-party/{code}/info").json()["active"] is False


def test_patch_room_updates_fields_without_clobbering_omitted_values(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    for media_id in ("m1", "m2", "m3"):
        _insert_media(db, tmp_path, media_id=media_id, filename=f"{media_id}.mp4", payload=b"x" * 32)

    with TestClient(app) as client:
        room = _create_room(
            client,
            title="Original Title",
            media_ids=["m1"],
            pin="1111",
            control_mode="host_only",
            wait_for_buffering=False,
            ttl_sec=300,
        )
        code = room["room_code"]
        before = _room_view(client, code)
        expires_before = before["expires_at"]

        # Title only: everything else must remain untouched.
        r = client.patch(f"/api/watch-party/rooms/{code}", json={"title": "New Title"})
        assert r.status_code == 200, r.text
        state = r.json()
        assert state["title"] == "New Title"
        assert state["control_mode"] == "host_only"
        assert state["wait_for_buffering"] is False
        assert state["media_ids"] == ["m1"]
        assert state["expires_at"] == expires_before
        assert _join(client, code, pin="1111").status_code == 200

        # Settings only: title stays put.
        r = client.patch(
            f"/api/watch-party/rooms/{code}",
            json={"control_mode": "everyone", "wait_for_buffering": True},
        )
        assert r.status_code == 200, r.text
        state = r.json()
        assert state["title"] == "New Title"
        assert state["control_mode"] == "everyone"
        assert state["wait_for_buffering"] is True
        assert state["media_ids"] == ["m1"]

        # Queue mutations: add, remove, then explicit replacement.
        r = client.patch(f"/api/watch-party/rooms/{code}", json={"add_media_ids": ["m2"]})
        assert r.status_code == 200 and r.json()["media_ids"] == ["m1", "m2"]
        r = client.patch(f"/api/watch-party/rooms/{code}", json={"remove_media_ids": ["m1"]})
        assert r.status_code == 200, r.text
        assert r.json()["media_ids"] == ["m2"]
        assert r.json()["active_media_id"] == "m2"
        r = client.patch(f"/api/watch-party/rooms/{code}", json={"queue": ["m1", "m2", "m3"]})
        assert r.status_code == 200 and r.json()["media_ids"] == ["m1", "m2", "m3"]
        assert r.json()["title"] == "New Title"
        assert r.json()["control_mode"] == "everyone"

        # PIN replacement and clearing.
        r = client.patch(f"/api/watch-party/rooms/{code}", json={"pin": "2222"})
        assert r.status_code == 200 and r.json()["requires_pin"] is True
        assert "2222" not in r.text
        assert _join(client, code, pin="1111").status_code == 403
        assert _join(client, code, pin="2222").status_code == 200
        r = client.patch(f"/api/watch-party/rooms/{code}", json={"clear_pin": True})
        assert r.status_code == 200 and r.json()["requires_pin"] is False
        assert _join(client, code).status_code == 200

        # TTL extension without touching the rest of the room.
        view_before = _room_view(client, code)
        r = client.patch(f"/api/watch-party/rooms/{code}", json={"extend_ttl_sec": 300})
        assert r.status_code == 200, r.text
        assert r.json()["expires_at"] >= view_before["expires_at"] + 299
        assert r.json()["title"] == "New Title"
        assert r.json()["media_ids"] == ["m1", "m2", "m3"]
        assert r.json()["requires_pin"] is False

        # Contradictory PIN operations are rejected atomically.
        bad = client.patch(
            f"/api/watch-party/rooms/{code}",
            json={"pin": "3333", "clear_pin": True},
        )
        assert bad.status_code == 400


def test_room_capacity_and_disabled_feature(tmp_path: Path):
    settings, app = _make_client(tmp_path, watch_party_max_rooms=1)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE")

    with TestClient(app) as client:
        first = _create_room(client, title="First", media_ids=["m1"])
        assert first["room_code"]
        second = client.post("/api/watch-party/rooms", json={"title": "Second", "media_ids": ["m1"]})
        assert second.status_code == 409, second.text

    # Per-room participant capacity (the host occupies a slot).
    cap_settings, cap_app = _make_client(tmp_path / "capacity")
    cap_db = cap_app.state.db
    _insert_media(cap_db, tmp_path / "capacity", media_id="cap1", filename="cap.mp4", payload=b"CAP")
    with TestClient(cap_app) as client:
        room = _create_room(client, media_ids=["cap1"], max_participants=2)
        code = room["room_code"]
        assert _join(client, code, name="Guest 1").status_code == 200
        full = _join(client, code, name="Guest 2")
        assert full.status_code == 409, full.text

    # Feature flag must short-circuit creation and joining.
    off_settings, off_app = _make_client(tmp_path / "off", watch_party_enabled=False)
    with TestClient(off_app) as client:
        disabled = client.post("/api/watch-party/rooms", json={"title": "Nope"})
        assert disabled.status_code == 503, disabled.text
        join_disabled = client.post(
            "/api/watch-party/whatever/join", json={"display_name": "Guest"}
        )
        assert join_disabled.status_code == 503


# ---------------------------------------------------------------------------
# 2. PIN protection, non-leakage & brute-force throttling
# ---------------------------------------------------------------------------


def test_public_info_redaction_and_pin_join_throttling(tmp_path: Path):
    web_token = "super-secret-web-token"
    settings, app = _make_client(
        tmp_path,
        auth_token=web_token,
        watch_party_pin_max_attempts=3,
    )
    headers = _auth(web_token)
    db = app.state.db
    secret_path = _insert_media(
        db,
        tmp_path,
        media_id="secret1",
        filename="secret-file.mp4",
        payload=b"SECRET-BYTES",
        title="Top Secret Episode",
        folder="PrivateFolder",
        mime_type="video/mp4",
        thumb_filename=None,
        source_url="https://secret.example/private/video",
        tags="secret-tag",
    )

    with TestClient(app) as client:
        pin_room = _create_room(
            client,
            headers,
            title="Secrets & Spies",
            media_ids=["secret1"],
            pin="9876",
        )
        open_room = _create_room(
            client,
            headers,
            title="Open Cinema",
            media_ids=["secret1"],
        )
        pin_code = pin_room["room_code"]
        open_code = open_room["room_code"]

        # Protected rooms only expose a generic label.
        info = client.get(f"/api/watch-party/{pin_code}/info")
        assert info.status_code == 200
        body = info.json()
        assert body["active"] is True
        assert body["requires_pin"] is True
        assert body["title"] == "Protected Watch Party"
        for secret in (
            "Secrets & Spies",
            "Top Secret Episode",
            "secret-file.mp4",
            "PrivateFolder",
            str(secret_path),
            "secret.example",
            "secret-tag",
            "SECRET-BYTES",
        ):
            assert secret not in info.text, secret
        for key in ("file_path", "drive_name", "telegram_chat_id", "source_url", "folder", "tags"):
            assert key not in body

        # Open rooms expose the real title but still no metadata.
        info_open = client.get(f"/api/watch-party/{open_code}/info")
        assert info_open.status_code == 200
        open_body = info_open.json()
        assert open_body["active"] is True
        assert open_body["requires_pin"] is False
        assert open_body["title"] == "Open Cinema"
        for secret in (
            "Top Secret Episode",
            "secret-file.mp4",
            "PrivateFolder",
            str(secret_path),
            "secret.example",
            "secret-tag",
        ):
            assert secret not in info_open.text, secret

        unknown = client.get("/api/watch-party/not-a-room/info")
        assert unknown.status_code == 200
        assert unknown.json()["active"] is False

        # Missing / wrong PINs both count toward PIN throttling with Retry-After (H4).
        assert _join(client, pin_code, name="No PIN").status_code == 401
        for _ in range(2):
            assert _join(client, pin_code, name="Wrong", pin="0000").status_code == 403
        limited = _join(client, pin_code, name="Wrong", pin="0000")
        assert limited.status_code == 429, limited.text
        assert int(limited.headers["retry-after"]) >= 1

        # A different room has its own PIN bucket and accepts the right PIN.
        other = _create_room(
            client,
            headers,
            title="Other Secrets",
            media_ids=["secret1"],
            pin="5555",
        )
        joined = _join(client, other["room_code"], name="Good Guest", pin="5555")
        assert joined.status_code == 200, joined.text
        data = joined.json()
        assert data["role"] == "guest"
        assert data["guest_token"] and data["participant_id"]
        assert data["room_state"]["title"] == "Other Secrets"

        # Guest room_state is sanitized and never carries the web auth token.
        blob = json.dumps(data)
        assert web_token not in blob
        state_blob = json.dumps(data["room_state"])
        for key in ("file_path", "drive_name", "telegram_chat_id", "source_url", "folder", "tags"):
            assert key not in state_blob, key
        # The sanitized media title is intentionally visible to guests; the
        # underlying path, filename, folder, URL, and tags are not.
        assert "Top Secret Episode" in state_blob
        for secret in (
            "secret-file.mp4",
            "PrivateFolder",
            str(secret_path),
            "secret.example",
            "secret-tag",
        ):
            assert secret not in state_blob, secret


# ---------------------------------------------------------------------------
# 3. TTL expiration, idle timeout & clean shutdown
# ---------------------------------------------------------------------------


def test_expired_room_blocks_access_and_reaper_notifies_clients(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE" * 32)
    wp = app.state.watch_party

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1"])
        code = room["room_code"]
        guest = _join(client, code, name="Guest").json()
        token = guest["guest_token"]

        # Expire the room out of band (no long sleeps needed).
        expired = wp.rooms[code]
        expired.expires_at = time.time() - 1
        expired.expires_mono = time.monotonic() - 1

        assert client.get(f"/api/watch-party/{code}/info").json()["active"] is False
        assert _join(client, code, name="Late Guest").status_code == 404
        stream = client.get(
            f"/api/watch-party/{code}/media/m1/stream", params={"wp_token": token}
        )
        assert stream.status_code == 404

        # Even a valid token cannot open a WebSocket to an expired room.
        with client.websocket_connect(f"/api/watch-party/{code}/ws?wp_token={token}") as ws:
            assert _recv(ws)["type"] == "error"
            _expect_close(ws, 4004)

        # A connected client is notified when the reaper collects the room.
        room2 = _create_room(client, media_ids=["m1"])
        code2 = room2["room_code"]
        guest2 = _join(client, code2, name="Connected Guest").json()
        with client.websocket_connect(
            f"/api/watch-party/{code2}/ws?wp_token={guest2['guest_token']}"
        ) as ws:
            assert _recv(ws, "auth_ok")["room_state"]["room_code"] == code2
            wp.rooms[code2].expires_mono = time.monotonic() - 1
            closed = client.portal.call(wp._reap_once)
            assert code2 in closed
            notice = _recv(ws, "room_closed")
            assert notice["reason"]
            _expect_close(ws, 4004)

        assert code2 not in wp.rooms
        assert client.get(f"/api/watch-party/{code2}/info").json()["active"] is False

        # A live reaper task is running under the app lifespan.
        keep = _create_room(client, media_ids=["m1"])
        assert wp._reaper_task is not None and not wp._reaper_task.done()
        assert keep["room_code"] in wp.rooms

    # Exiting the lifespan must stop the reaper and close all remaining rooms.
    assert wp._reaper_task is None
    assert wp.rooms == {}


def test_stop_reaper_cancels_task_and_closes_rooms_without_leaks(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    wp = app.state.watch_party

    async def _scenario() -> None:
        try:
            wp.start_reaper()
            room = wp.create_room(title="Shutdown")
            code = room["room_code"]
            assert wp._reaper_task is not None and not wp._reaper_task.done()
            await wp.stop_reaper()
            assert wp._reaper_task is None
            assert code not in wp.rooms
            leaked = [
                task
                for task in asyncio.all_tasks()
                if task.get_name() == "watch-party-reaper" and not task.done()
            ]
            assert leaked == []
        finally:
            app.state.db.close()

    asyncio.run(_scenario())


# ---------------------------------------------------------------------------
# 4. Multi-client WebSocket sync & real-time features
# ---------------------------------------------------------------------------


def test_ws_auth_methods_ping_and_host_control_broadcasts(tmp_path: Path):
    web_token = "web-token-never-valid-here"
    settings, app = _make_client(tmp_path, auth_token=web_token)
    headers = _auth(web_token)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE" * 16, mime_type="video/mp4")
    _insert_media(db, tmp_path, media_id="m2", filename="two.mp4", payload=b"TWO" * 16, mime_type="video/mp4")

    with TestClient(app) as client:
        room = _create_room(client, headers, media_ids=["m1", "m2"])
        code = room["room_code"]
        other = _create_room(client, headers, media_ids=["m1"])
        other_token = _join(client, other["room_code"], name="Other").json()["guest_token"]

        # Query-string auth for the host.
        with client.websocket_connect(f"/api/watch-party/{code}/ws?wp_token={room['host_token']}") as host:
            host_hello = _recv(host, "auth_ok")
            assert host_hello["role"] == "host"

            guest = _join(client, code, name="Guest").json()
            guest_token = guest["guest_token"]

            # First-message auth for the guest.
            with client.websocket_connect(f"/api/watch-party/{code}/ws") as guest_ws:
                guest_ws.send_json({"type": "auth", "token": guest_token})
                hello = _recv(guest_ws, "auth_ok")
                assert hello["participant_id"] == guest["participant_id"]
                assert _recv(host, "presence")["event"] == "join"

                # Invalid first-frame token -> error + 4001.
                with client.websocket_connect(f"/api/watch-party/{code}/ws") as bad_ws:
                    bad_ws.send_json({"type": "auth", "token": "not-a-token"})
                    assert _recv(bad_ws)["code"] == "unauthorized"
                    _expect_close(bad_ws, 4001)

                # Query-string invalid token -> error + 4001.
                with client.websocket_connect(f"/api/watch-party/{code}/ws?wp_token=nope") as bad_ws:
                    assert _recv(bad_ws)["code"] == "unauthorized"
                    _expect_close(bad_ws, 4001)

                # The global web auth token is never a watch-party token.
                with client.websocket_connect(f"/api/watch-party/{code}/ws") as bad_ws:
                    bad_ws.send_json({"type": "auth", "token": web_token})
                    assert _recv(bad_ws)["code"] == "unauthorized"
                    _expect_close(bad_ws, 4001)

                # A valid token from another room is rejected by this room.
                with client.websocket_connect(f"/api/watch-party/{code}/ws") as bad_ws:
                    bad_ws.send_json({"type": "auth", "token": other_token})
                    assert _recv(bad_ws)["code"] == "unauthorized"
                    _expect_close(bad_ws, 4001)

                # ping -> pong echo with a server clock sample.
                guest_ws.send_json({"type": "ping", "client_time_ms": 12345})
                pong = _recv(guest_ws, "pong")
                assert float(pong["client_time_ms"]) == 12345.0
                assert int(pong["server_time_ms"]) > 0

                # Host control frames fan out to every client with a rising seq.
                messages = [
                    {"type": "play", "position_sec": 3.0},
                    {"type": "pause"},
                    {"type": "seek", "position_sec": 7.5},
                    {"type": "rate", "rate": 1.5},
                    {"type": "select_item", "media_id": "m2"},
                    {"type": "queue_update", "queue": ["m2", "m1"]},
                    {"type": "update_settings", "title": "Renamed by Host"},
                ]
                expected_seq = 0
                for message in messages:
                    host.send_json(message)
                    h = _recv(host, "sync_state")
                    g = _recv(guest_ws, "sync_state")
                    expected_seq += 1
                    assert h["state_seq"] == expected_seq, message
                    assert g["state_seq"] == expected_seq, message
                    assert h["room_state"]["state_seq"] == expected_seq
                    assert g["room_state"]["title"] == (
                        "Renamed by Host" if message["type"] == "update_settings" else "Watch Party"
                    )

                final = _room_view(client, code, headers)
                assert final["title"] == "Renamed by Host"
                assert final["control_mode"] == "host_only"
                assert final["room_state"]["media_ids"] == ["m2", "m1"]
                assert final["active_media_id"] == "m2"
                assert final["playback_rate"] == 1.5


def test_late_joiner_receives_authoritative_playing_clock(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(
        db,
        tmp_path,
        media_id="m1",
        filename="movie.mp4",
        payload=b"VIDEO" * 32,
        mime_type="video/mp4",
        duration_sec=3600.0,
    )

    with TestClient(app) as client:
        t_before = time.monotonic()
        room = _create_room(
            client,
            media_ids=["m1"],
            initial_position_sec=10.0,
            initial_is_playing=True,
            initial_playback_rate=2.0,
        )
        code = room["room_code"]
        t_after = time.monotonic()

        with client.websocket_connect(f"/api/watch-party/{code}/ws?wp_token={room['host_token']}") as host:
            assert _recv(host, "auth_ok")["room_state"]["position_sec"] == 10.0

            time.sleep(0.25)

            # The host's rate frame syncs the authoritative base position at the
            # current effective clock before the late joiner arrives.
            t_sync = time.monotonic()
            host.send_json({"type": "rate", "rate": 2.0})
            synced = _recv(host, "sync_state")
            assert synced["room_state"]["position_sec"] > 10.0

            late = _join(client, code, name="Late Guest").json()
            with client.websocket_connect(f"/api/watch-party/{code}/ws") as late_ws:
                late_ws.send_json({"type": "auth", "token": late["guest_token"]})
                state = _recv(late_ws, "auth_ok")["room_state"]
                _recv(host, "presence")

            assert state["is_playing"] is True
            assert state["playback_rate"] == 2.0
            elapsed_min = max(0.0, t_sync - t_after)
            elapsed_max = time.monotonic() - t_before
            assert state["position_sec"] >= 10.0 + elapsed_min * 2.0 - 0.1
            assert state["effective_position_sec"] <= 10.0 + elapsed_max * 2.0 + 0.1
            assert state["effective_position_sec"] >= state["position_sec"]
            assert state["effective_position_sec"] > 10.0
            assert state["effective_position_sec"] < 10.0 + (time.monotonic() - t_before + 1.0) * 2.0
            assert t_after >= t_before


def test_control_mode_enforcement_for_guest_frames(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE" * 32, mime_type="video/mp4")
    _insert_media(db, tmp_path, media_id="m2", filename="two.mp4", payload=b"TWO" * 32, mime_type="video/mp4")

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1", "m2"])
        code = room["room_code"]
        with client.websocket_connect(f"/api/watch-party/{code}/ws?wp_token={room['host_token']}") as host:
            assert _recv(host, "auth_ok")["type"] == "auth_ok"

            guest = _join(client, code, name="Guest").json()
            with client.websocket_connect(f"/api/watch-party/{code}/ws") as guest_ws:
                guest_ws.send_json({"type": "auth", "token": guest["guest_token"]})
                assert _recv(guest_ws, "auth_ok")["type"] == "auth_ok"
                assert _recv(host, "presence")["event"] == "join"

                denied_in_host_only = [
                    {"type": "play", "position_sec": 5.0},
                    {"type": "pause"},
                    {"type": "seek", "position_sec": 9.0},
                    {"type": "rate", "rate": 3.0},
                    {"type": "select_item", "media_id": "m2"},
                    {"type": "queue_update", "queue": ["m2"]},
                    {"type": "kick", "participant_id": guest["participant_id"]},
                    {"type": "close_room"},
                ]
                for message in denied_in_host_only:
                    guest_ws.send_json(message)
                    assert _recv(guest_ws, "error")["code"] == "forbidden", message

                unchanged = _room_view(client, code)
                assert unchanged["state_seq"] == 0
                assert unchanged["is_playing"] is False
                assert unchanged["active_media_id"] == "m1"
                assert unchanged["playback_rate"] == 1.0
                assert unchanged["control_mode"] == "host_only"

                # Host opens controls to everyone.
                host.send_json({"type": "update_settings", "control_mode": "everyone"})
                assert _recv(host, "sync_state")["room_state"]["control_mode"] == "everyone"
                assert _recv(guest_ws, "sync_state")["room_state"]["control_mode"] == "everyone"

                # Playback controls now succeed for the guest.
                allowed = [
                    {"type": "play", "position_sec": 2.0},
                    {"type": "pause"},
                    {"type": "seek", "position_sec": 6.0},
                    {"type": "rate", "rate": 1.75},
                    {"type": "select_item", "media_id": "m2"},
                ]
                seq = 1
                for message in allowed:
                    guest_ws.send_json(message)
                    h = _recv(host, "sync_state")
                    g = _recv(guest_ws, "sync_state")
                    seq += 1
                    assert h["state_seq"] == seq
                    assert g["state_seq"] == seq

                # Moderation and queue edits stay strictly host-only.
                still_host_only = [
                    {"type": "queue_update", "queue": ["m1"]},
                    {"type": "kick", "participant_id": guest["participant_id"]},
                    {"type": "close_room"},
                ]
                for message in still_host_only:
                    guest_ws.send_json(message)
                    assert _recv(guest_ws, "error")["code"] == "forbidden", message

                final = _room_view(client, code)
                assert final["state_seq"] == seq
                assert final["control_mode"] == "everyone"
                assert final["active_media_id"] == "m2"
                assert final["playback_rate"] == 1.75


def test_wait_for_buffering_pauses_and_auto_resumes(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE" * 32, mime_type="video/mp4")

    with TestClient(app) as client:
        room = _create_room(
            client,
            media_ids=["m1"],
            wait_for_buffering=True,
            initial_is_playing=True,
        )
        code = room["room_code"]
        with client.websocket_connect(f"/api/watch-party/{code}/ws?wp_token={room['host_token']}") as host:
            assert _recv(host, "auth_ok")["type"] == "auth_ok"
            guest = _join(client, code, name="Buffer Guest").json()
            with client.websocket_connect(f"/api/watch-party/{code}/ws") as guest_ws:
                guest_ws.send_json({"type": "auth", "token": guest["guest_token"]})
                assert _recv(guest_ws, "auth_ok")["type"] == "auth_ok"
                assert _recv(host, "presence")["event"] == "join"

                guest_ws.send_json({"type": "buffering", "is_buffering": True})
                h = _recv(host, "sync_state")
                g = _recv(guest_ws, "sync_state")
                assert h["room_state"]["is_playing"] is False
                assert g["room_state"]["paused_for_buffering"] is True

                guest_ws.send_json({"type": "buffering", "is_buffering": False})
                h = _recv(host, "sync_state")
                g = _recv(guest_ws, "sync_state")
                assert h["room_state"]["is_playing"] is True
                assert g["room_state"]["paused_for_buffering"] is False

                # The legacy ``ready`` frame also resumes playback.
                guest_ws.send_json({"type": "buffering", "is_buffering": True})
                assert _recv(host, "sync_state")["room_state"]["paused_for_buffering"] is True
                assert _recv(guest_ws, "sync_state")["room_state"]["paused_for_buffering"] is True

                guest_ws.send_json({"type": "ready", "ready": True})
                assert _recv(host, "sync_state")["room_state"]["is_playing"] is True
                assert _recv(guest_ws, "sync_state")["room_state"]["is_playing"] is True


def test_chat_ring_buffer_rate_limit_reactions_and_js_xss_guardrails(tmp_path: Path):
    settings, app = _make_client(
        tmp_path,
        watch_party_chat_history_limit=2,
        watch_party_chat_rate_limit_count=4,
    )
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE" * 32)

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1"])
        code = room["room_code"]
        with client.websocket_connect(f"/api/watch-party/{code}/ws?wp_token={room['host_token']}") as host:
            assert _recv(host, "auth_ok")["type"] == "auth_ok"
            guest = _join(client, code, name="Chatter").json()
            with client.websocket_connect(f"/api/watch-party/{code}/ws") as guest_ws:
                guest_ws.send_json({"type": "auth", "token": guest["guest_token"]})
                assert _recv(guest_ws, "auth_ok")["type"] == "auth_ok"
                assert _recv(host, "presence")["event"] == "join"

                xss = "<script>alert(1)</script>"
                messages = [xss, "second message", "third message", "fourth message"]
                for text in messages:
                    guest_ws.send_json({"type": "chat_message", "text": text})
                    h = _recv(host, "chat_message")
                    g = _recv(guest_ws, "chat_message")
                    assert h["text"] == text and g["text"] == text

                # Fifth message exceeds the configured rate limit.
                guest_ws.send_json({"type": "chat_message", "text": "too fast"})
                limited = _recv(guest_ws, "error")
                assert limited["code"] == "rate_limited"
                assert limited.get("retry_after_sec", 1) >= 1

                history = _room_view(client, code)["chat_history"]
                assert [m["text"] for m in history] == ["third message", "fourth message"]
                assert history[0]["id"] == 3

                # Allowed reactions are broadcast but never stored in history.
                guest_ws.send_json({"type": "reaction", "emoji": "🔥"})
                assert _recv(host, "reaction")["emoji"] == "🔥"
                assert _recv(guest_ws, "reaction")["emoji"] == "🔥"
                for bad in ("<script>alert(1)</script>", "💀", "not-an-emoji", 123):
                    guest_ws.send_json({"type": "reaction", "emoji": bad})
                    assert _recv(guest_ws, "error")["code"] == "bad_request"
                assert len(_room_view(client, code)["chat_history"]) == 2

    # The shipped client renders all untrusted text via textContent only.
    js = (STATIC_DIR / "js" / "watch-party.js").read_text(encoding="utf-8")
    assert "innerHTML" not in js
    assert "outerHTML" not in js
    assert "textContent" in js
    assert "body.textContent = String(msg?.text || \"\")" in js


def test_host_kick_and_close_room_notifications(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE" * 32)

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1"])
        code = room["room_code"]
        with client.websocket_connect(f"/api/watch-party/{code}/ws?wp_token={room['host_token']}") as host:
            assert _recv(host, "auth_ok")["type"] == "auth_ok"
            guest1 = _join(client, code, name="Kicked Guest").json()
            with client.websocket_connect(f"/api/watch-party/{code}/ws") as kicked_ws:
                kicked_ws.send_json({"type": "auth", "token": guest1["guest_token"]})
                assert _recv(kicked_ws, "auth_ok")["type"] == "auth_ok"
                assert _recv(host, "presence")["event"] == "join"

                host.send_json({"type": "kick", "participant_id": guest1["participant_id"]})
                notice = _recv(kicked_ws, "kicked")
                assert notice["reason"]
                _expect_close(kicked_ws, 4003)
                assert _recv(host, "presence")["event"] == "kick"

                # The kicked token is invalid immediately on reconnect and on streams.
                with client.websocket_connect(f"/api/watch-party/{code}/ws") as reconnect:
                    reconnect.send_json({"type": "auth", "token": guest1["guest_token"]})
                    assert _recv(reconnect)["code"] == "unauthorized"
                    _expect_close(reconnect, 4001)
                assert client.get(
                    f"/api/watch-party/{code}/media/m1/stream",
                    params={"wp_token": guest1["guest_token"]},
                ).status_code == 401

                # A fresh guest receives the room_closed broadcast from host close.
                guest2 = _join(client, code, name="Remaining Guest").json()
                with client.websocket_connect(f"/api/watch-party/{code}/ws") as guest2_ws:
                    guest2_ws.send_json({"type": "auth", "token": guest2["guest_token"]})
                    assert _recv(guest2_ws, "auth_ok")["type"] == "auth_ok"
                    assert _recv(host, "presence")["event"] == "join"

                    host.send_json({"type": "close_room", "reason": "Party over"})
                    h = _recv(host, "room_closed")
                    g = _recv(guest2_ws, "room_closed")
                    assert h["reason"] == "Party over" and g["reason"] == "Party over"
                    _expect_close(host, 4004)
                    _expect_close(guest2_ws, 4004)

                assert client.get(f"/api/watch-party/{code}/info").json()["active"] is False
                assert _join(client, code, name="Too Late").status_code == 404


def test_media_deletion_cleans_up_active_room_queues(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE" * 32, mime_type="video/mp4")
    _insert_media(db, tmp_path, media_id="m2", filename="two.mp4", payload=b"TWO" * 32, mime_type="video/mp4")

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1", "m2"])
        code = room["room_code"]
        guest = _join(client, code, name="Guest").json()
        with client.websocket_connect(f"/api/watch-party/{code}/ws") as guest_ws:
            guest_ws.send_json({"type": "auth", "token": guest["guest_token"]})
            assert _recv(guest_ws, "auth_ok")["type"] == "auth_ok"

            deleted = client.delete("/api/media/m1")
            assert deleted.status_code == 200, deleted.text

            sync = _recv(guest_ws, "sync_state")
            assert sync["reason"] == "queue_update"
            assert sync["room_state"]["media_ids"] == ["m2"]
            assert sync["room_state"]["active_media_id"] == "m2"

            view = _room_view(client, code)
            state = view["room_state"]
            assert state["media_ids"] == ["m2"]
            assert state["active_media_id"] == "m2"

            # The deleted item is no longer streamable in this room.
            assert client.get(
                f"/api/watch-party/{code}/media/m1/stream",
                params={"wp_token": guest["guest_token"]},
            ).status_code == 403


# ---------------------------------------------------------------------------
# 5. Scoped guest streaming & security isolation
# ---------------------------------------------------------------------------


def test_local_media_stream_ranges_head_and_bearer_auth(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    payload = bytes((i * 7 + 11) % 256 for i in range(256))
    _insert_media(
        db,
        tmp_path,
        media_id="local1",
        filename="clip.bin",
        payload=payload,
        mime_type="application/octet-stream",
    )

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["local1"])
        code = room["room_code"]
        guest = _join(client, code, name="Streamer").json()
        token = guest["guest_token"]
        url = f"/api/watch-party/{code}/media/local1/stream"

        # Full GET via query token.
        r = client.get(url, params={"wp_token": token})
        assert r.status_code == 200, r.text
        assert r.content == payload
        assert r.headers["accept-ranges"] == "bytes"
        assert r.headers["cache-control"] == "private, no-store"

        # Partial GET.
        r = client.get(url, params={"wp_token": token}, headers={"Range": "bytes=0-9"})
        assert r.status_code == 206
        assert r.content == payload[:10]
        assert r.headers["content-range"] == f"bytes 0-9/{len(payload)}"

        # Unsatisfiable range.
        r = client.get(url, params={"wp_token": token}, headers={"Range": "bytes=999999-"})
        assert r.status_code == 416
        assert r.headers["content-range"] == f"bytes */{len(payload)}"

        # HEAD must return metadata only, with and without a Range header.
        r = client.head(url, params={"wp_token": token})
        assert r.status_code == 200
        assert r.content == b""
        assert int(r.headers["content-length"]) == len(payload)
        r = client.head(url, params={"wp_token": token}, headers={"Range": "bytes=0-9"})
        assert r.status_code == 206
        assert r.content == b""
        assert r.headers["content-range"] == f"bytes 0-9/{len(payload)}"
        assert r.headers["content-length"] == "10"

        # Bearer header auth is accepted as an alternative to wp_token.
        r = client.get(url, headers=_auth(token))
        assert r.status_code == 200 and r.content == payload


def test_telegram_media_stream_uses_pool_and_head_skips_prepare(tmp_path: Path, monkeypatch):
    settings, app = _make_client(tmp_path, storage_mode="telegram")
    payload = bytes((i * 13 + 5) % 256 for i in range(4096))
    _insert_telegram_media(
        app.state.db,
        media_id="tg1",
        filename="telegram.mp4",
        size_bytes=len(payload),
        chat_id="-100555",
        msg_id=777,
    )

    prepare_calls: list[tuple[Any, ...]] = []
    iter_calls: list[tuple[int, int]] = []

    async def _fake_prepare_stream(chat_id, msg_id, worker_name=None):
        prepare_calls.append((chat_id, msg_id, worker_name))
        return object(), SimpleNamespace(msg_id=int(msg_id), size=len(payload))

    async def _fake_iter_telegram_range(client, media, start, end, total_size=None):
        iter_calls.append((int(start), int(end)))
        yield payload[int(start) : int(end) + 1]

    monkeypatch.setattr(app.state.tg_pool, "prepare_stream", _fake_prepare_stream)
    monkeypatch.setattr(app.state.tg_pool, "iter_telegram_range", _fake_iter_telegram_range)

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["tg1"])
        code = room["room_code"]
        token = _join(client, code, name="TG Guest").json()["guest_token"]
        url = f"/api/watch-party/{code}/media/tg1/stream"

        r = client.get(url, params={"wp_token": token})
        assert r.status_code == 200, r.text
        assert r.content == payload
        assert prepare_calls and prepare_calls[0][:2] == ("-100555", 777)
        assert (0, len(payload) - 1) in iter_calls

        r = client.get(url, params={"wp_token": token}, headers={"Range": "bytes=0-15"})
        assert r.status_code == 206
        assert r.content == payload[:16]
        assert r.headers["content-range"] == f"bytes 0-15/{len(payload)}"
        assert (0, 15) in iter_calls
        assert len(prepare_calls) >= 2

        # HEAD is metadata-only: no Telegram RPCs at all.
        prepare_before = len(prepare_calls)
        iter_before = len(iter_calls)
        head = client.head(url, params={"wp_token": token})
        assert head.status_code == 200
        assert head.content == b""
        assert head.headers["content-length"] == str(len(payload))
        assert len(prepare_calls) == prepare_before
        assert len(iter_calls) == iter_before


def test_thumbnail_proxy_requires_queue_membership(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    settings.thumbs_dir.mkdir(parents=True, exist_ok=True)
    thumb_bytes = _sample_jpeg_bytes()
    (settings.thumbs_dir / "thumb-one.jpg").write_bytes(thumb_bytes)

    _insert_media(
        db,
        tmp_path,
        media_id="withthumb",
        filename="thumb.mp4",
        payload=b"DATA" * 16,
        thumb_filename="thumb-one.jpg",
        mime_type="video/mp4",
    )
    _insert_media(
        db,
        tmp_path,
        media_id="unqueued",
        filename="unqueued.mp4",
        payload=b"NOPE" * 16,
        mime_type="video/mp4",
    )

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["withthumb"])
        code = room["room_code"]
        token = _join(client, code, name="Thumb Guest").json()["guest_token"]
        thumb_url = f"/api/watch-party/{code}/media/withthumb/thumb"

        r = client.get(thumb_url, params={"wp_token": token})
        assert r.status_code == 200, r.text
        assert r.content == thumb_bytes
        assert "image/jpeg" in r.headers["content-type"]
        assert r.headers["cache-control"] == "private, max-age=300"

        head = client.head(thumb_url, params={"wp_token": token})
        assert head.status_code == 200
        assert head.content == b""
        assert int(head.headers["content-length"]) == len(thumb_bytes)

        # A real DB item outside the room queue is forbidden on both proxies.
        for endpoint in ("stream", "thumb"):
            url = f"/api/watch-party/{code}/media/unqueued/{endpoint}"
            assert client.get(url, params={"wp_token": token}).status_code == 403
            assert client.head(url, params={"wp_token": token}).status_code == 403

        # Removing the item from the queue revokes access immediately.
        patched = client.patch(f"/api/watch-party/rooms/{code}", json={"remove_media_ids": ["withthumb"]})
        assert patched.status_code == 200
        for endpoint in ("stream", "thumb"):
            url = f"/api/watch-party/{code}/media/withthumb/{endpoint}"
            assert client.get(url, params={"wp_token": token}).status_code == 403
            assert client.head(url, params={"wp_token": token}).status_code == 403


def test_stream_rejects_wrong_room_web_token_closed_room_and_kicked_guest(tmp_path: Path):
    web_token = "global-web-token"
    settings, app = _make_client(tmp_path, auth_token=web_token)
    headers = _auth(web_token)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE" * 32)

    with TestClient(app) as client:
        room_a = _create_room(client, headers, media_ids=["m1"])
        room_b = _create_room(client, headers, media_ids=["m1"])
        code_a = room_a["room_code"]
        code_b = room_b["room_code"]
        token_a = _join(client, code_a, name="Guest A").json()["guest_token"]
        token_b = _join(client, code_b, name="Guest B").json()["guest_token"]
        stream_a = f"/api/watch-party/{code_a}/media/m1/stream"

        # A token from another room is never accepted.
        assert client.get(stream_a, params={"wp_token": token_b}).status_code == 401
        # The global web auth token is never a room token, in either transport.
        assert client.get(stream_a, params={"wp_token": web_token}).status_code == 401
        assert client.get(stream_a, headers=_auth(web_token)).status_code == 401
        assert client.get(stream_a).status_code == 401

        # Kicked guests lose stream access immediately.
        guest_id = _join(client, code_a, name="To Kick").json()["participant_id"]
        kicked = client.post(
            f"/api/watch-party/rooms/{code_a}/kick",
            json={"participant_id": guest_id},
            headers=headers,
        )
        assert kicked.status_code == 200
        # Re-join to obtain a token, then kick that participant via the HTTP API.
        victim = _join(client, code_a, name="Victim").json()
        client.post(
            f"/api/watch-party/rooms/{code_a}/kick",
            json={"participant_id": victim["participant_id"]},
            headers=headers,
        )
        assert client.get(stream_a, params={"wp_token": victim["guest_token"]}).status_code == 401

        # Closing the room invalidates every remaining token.
        assert client.delete(f"/api/watch-party/rooms/{code_a}", headers=headers).status_code == 200
        assert client.get(stream_a, params={"wp_token": token_a}).status_code == 404


# ---------------------------------------------------------------------------
# 6. Standalone /watch/{code} page & strict CSP headers
# ---------------------------------------------------------------------------


def test_watch_page_headers_csp_malformed_codes_and_static_guardrails(tmp_path: Path):
    settings, app = _make_client(tmp_path)

    with TestClient(app) as client:
        r = client.get("/watch/valid_room-code123")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "watch-party.js" in r.text
        assert r.headers["x-content-type-options"] == "nosniff"
        assert r.headers["x-frame-options"] == "DENY"
        assert r.headers["referrer-policy"] == "no-referrer"
        assert r.headers["cache-control"] == "no-cache"

        csp = r.headers["content-security-policy"]
        for directive in (
            "default-src 'self'",
            "script-src 'self'",
            "style-src 'self'",
            "connect-src 'self' ws: wss:",
            "media-src 'self' blob:",
            "frame-ancestors 'none'",
        ):
            assert directive in csp, directive

        for bad_code in ("../etc/passwd", "bad%20code", "x" * 65, "a.b"):
            bad = client.get(f"/watch/{bad_code}")
            assert bad.status_code in (400, 404), (bad_code, bad.status_code)
            assert "root:" not in bad.text

    html = (STATIC_DIR / "watch-party.html").read_text(encoding="utf-8")
    assert html.count("<script") == 1
    assert '<script type="module" src="/static/js/watch-party.js"></script>' in html
    assert "<style" not in html.lower()
    for forbidden in ("style=", "onclick=", "onerror=", "onload=", "onmouseover="):
        assert forbidden not in html.lower(), forbidden
    assert re.search(r"javascript\s*:", html, re.IGNORECASE) is None


# ---------------------------------------------------------------------------
# 7. Adversarial hardening regressions (H1, H2, M1, L2, NaN/Inf, binary WS, Host UI)
# ---------------------------------------------------------------------------


def test_ghost_join_reclamation_after_grace_window_h1(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="clip.mp4", payload=b"CLIP")

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1"], max_participants=2)
        code = room["room_code"]

        # Ghost guest joins over HTTP but never opens a WS.
        ghost = _join(client, code, name="Ghost")
        assert ghost.status_code == 200
        ghost_id = ghost.json()["participant_id"]
        ghost_token = ghost.json()["guest_token"]

        # Within the 30s grace window, room enforces capacity cleanly.
        assert _join(client, code, name="Too Soon").status_code == 409

        # Once the ghost ages past the 30s grace window without connecting WS,
        # a real guest can reclaim its slot and the evicted ghost token is revoked.
        wp = app.state.watch_party
        wp._rooms[code].participants[ghost_id].last_seen_mono -= 60.0
        legit = _join(client, code, name="Real Guest")
        assert legit.status_code == 200, legit.text
        assert (
            client.get(
                f"/api/watch-party/{code}/media/m1/stream",
                params={"wp_token": ghost_token},
            ).status_code
            == 401
        )


def test_rate_limit_map_saturation_fails_closed_without_evicting_active_buckets_h2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    import app.watch_party as wp_mod

    monkeypatch.setattr(wp_mod, "_MAX_PIN_ATTEMPTS_KEYS", 2)
    settings, app = _make_client(tmp_path, watch_party_pin_max_attempts=2)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="clip.mp4", payload=b"CLIP")

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1"], pin="1234")
        code = room["room_code"]
        wp = app.state.watch_party

        # Exhaust PIN budget for victim IP.
        for _ in range(2):
            with pytest.raises(wp_mod.InvalidPinError):
                wp.join_room(code, display_name="A", pin="0000", client_ip="10.0.0.1")
        with pytest.raises(wp_mod.RateLimitError):
            wp.join_room(code, display_name="A", pin="0000", client_ip="10.0.0.1")

        # Fill second key slot, then probe with a third IP -> must fail-closed (429),
        # and MUST NOT evict 10.0.0.1's active rate-limit bucket!
        with pytest.raises(wp_mod.InvalidPinError):
            wp.join_room(code, display_name="B", pin="0000", client_ip="10.0.0.2")
        with pytest.raises(wp_mod.RateLimitError):
            wp.join_room(code, display_name="C", pin="0000", client_ip="10.0.0.3")
        with pytest.raises(wp_mod.RateLimitError):
            wp.join_room(code, display_name="A", pin="1234", client_ip="10.0.0.1")


def test_queue_replacement_revokes_active_item_even_if_outside_old_queue_m1(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="one.mp4", payload=b"ONE")
    _insert_media(db, tmp_path, media_id="m2", filename="two.mp4", payload=b"TWO")

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1", "m2"])
        code = room["room_code"]
        guest_token = _join(client, code, name="Guest").json()["guest_token"]

        # Simulate active_media_id drifting outside old queue list, then replace queue with ["m2"].
        wp = app.state.watch_party
        wp._rooms[code].queue = ["m2"]
        wp._rooms[code].active_media_id = "m1"

        res = client.patch(f"/api/watch-party/rooms/{code}", json={"queue": ["m2"]})
        assert res.status_code == 200
        # Even if queue already matched ["m2"], let's test replacing with a new list ["m2"] from ["m1"] or ["m2", ...]
        wp._rooms[code].queue = []
        wp._rooms[code].active_media_id = "m1"
        res2 = client.patch(f"/api/watch-party/rooms/{code}", json={"queue": ["m2"]})
        assert res2.status_code == 200
        assert res2.json()["active_media_id"] == "m2"
        assert (
            client.get(
                f"/api/watch-party/{code}/media/m1/stream",
                params={"wp_token": guest_token},
            ).status_code
            == 403
        )


def test_thumb_traversal_and_ws_nan_infinity_and_binary_frames(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    outside_secret = tmp_path / "secret.png"
    outside_secret.write_bytes(b"SECRET-OUTSIDE-THUMBS")
    _insert_media(
        db,
        tmp_path,
        media_id="m1",
        filename="clip.mp4",
        payload=b"VIDEO",
        thumb_filename=str(outside_secret),
    )

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1"])
        code = room["room_code"]
        host_token = room["host_token"]

        # L2: absolute or traversal thumb_filename is rejected (never reads outside thumbs_dir).
        thumb = client.get(
            f"/api/watch-party/{code}/media/m1/thumb",
            params={"wp_token": host_token},
        )
        assert thumb.status_code == 404
        assert b"SECRET-OUTSIDE-THUMBS" not in thumb.content

        # NaN / Infinity floats in WS commands return structured errors without crashing.
        with client.websocket_connect(f"/api/watch-party/{code}/ws") as ws:
            ws.send_json({"type": "auth", "token": host_token})
            assert ws.receive_json()["type"] == "auth_ok"

            for bad_payload in (
                '{"type": "seek", "position_sec": NaN}',
                '{"type": "seek", "position_sec": Infinity}',
                '{"type": "seek", "position_sec": "NaN"}',
                '{"type": "rate", "playback_rate": "Infinity"}',
                '{"type": "rate", "playback_rate": -1}',
            ):
                ws.send_text(bad_payload)
                err = ws.receive_json()
                assert err["type"] == "error", (bad_payload, err)

            # Binary frame is rejected cleanly without unhandled server crash.
            ws.send_bytes(b"\x00\x01\x02")
            err_bin = ws.receive_json()
            assert err_bin["type"] == "error"


def test_cinema_host_watch_party_ui_wired_and_index_under_350_lines(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    index_lines = (STATIC_DIR / "index.html").read_text(encoding="utf-8").splitlines()
    assert len(index_lines) < 350

    with TestClient(app) as client:
        index_html = client.get("/").text
        assert "cinemaWatchPartyBtn" in index_html
        host_js = client.get("/static/js/features/watch-party-host.js")
        assert host_js.status_code == 200
        assert "wp_handoff_" in host_js.text
        for forbidden in ("window.alert(", "window.confirm(", "window.prompt("):
            assert forbidden not in host_js.text


def test_pause_command_accepts_optional_clamped_position_sec(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(db, tmp_path, media_id="m1", filename="clip.mp4", payload=b"VIDEO", duration_sec=120.0)

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1"])
        code = room["room_code"]
        host_token = room["host_token"]

        with client.websocket_connect(f"/api/watch-party/{code}/ws") as ws:
            ws.send_json({"type": "auth", "token": host_token})
            assert ws.receive_json()["type"] == "auth_ok"

            ws.send_json({"type": "play", "position_sec": 10.0})
            play_msg = ws.receive_json()
            assert play_msg["type"] == "sync_state"
            assert play_msg["room_state"]["is_playing"] is True

            # Pause with explicit position_sec sets exact position
            ws.send_json({"type": "pause", "position_sec": 42.5})
            pause_msg = ws.receive_json()
            assert pause_msg["type"] == "sync_state"
            assert pause_msg["room_state"]["is_playing"] is False
            assert pause_msg["room_state"]["position_sec"] == pytest.approx(42.5, abs=0.01)

            # Pause with out-of-range position_sec clamps to [0, duration]
            ws.send_json({"type": "pause", "position_sec": 9999.0})
            clamped_hi = ws.receive_json()
            assert clamped_hi["room_state"]["position_sec"] == pytest.approx(120.0, abs=0.01)

            # Pause with negative position_sec is rejected like play/seek
            ws.send_json({"type": "pause", "position_sec": -25.0})
            rejected = ws.receive_json()
            assert rejected["type"] == "error"

            # Pause without position_sec falls back to effective_position
            ws.send_json({"type": "seek", "position_sec": 33.0})
            ws.receive_json()
            ws.send_json({"type": "pause"})
            fallback_msg = ws.receive_json()
            assert fallback_msg["room_state"]["is_playing"] is False
            assert fallback_msg["room_state"]["position_sec"] == pytest.approx(33.0, abs=0.5)


def test_watch_party_v0131_client_clock_host_sync_qr_and_strict_stream_headers(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    payload = b"0123456789ABCDEF" * 4
    _insert_media(db, tmp_path, media_id="m1", filename="clip.mp4", payload=payload)

    with TestClient(app) as client:
        room = _create_room(client, media_ids=["m1"])
        code = room["room_code"]
        guest_token = _join(client, code, name="Viewer").json()["guest_token"]

        # Standalone page & media proxy include strict X-Robots-Tag + Cache-Control + Referrer-Policy
        page = client.get(f"/watch/{code}")
        assert page.status_code == 200
        assert page.headers.get("x-robots-tag") == "noindex, nofollow"

        for method in ("GET", "HEAD"):
            res = client.request(
                method,
                f"/api/watch-party/{code}/media/m1/stream",
                params={"wp_token": guest_token},
                headers={"Range": "bytes=0-15"},
            )
            assert res.status_code == 206
            assert res.headers.get("cache-control") == "private, no-store"
            assert res.headers.get("referrer-policy") == "no-referrer"
            assert res.headers.get("x-robots-tag") == "noindex, nofollow"
            assert res.headers.get("content-range") == f"bytes 0-15/{len(payload)}"

        res_416 = client.get(
            f"/api/watch-party/{code}/media/m1/stream",
            params={"wp_token": guest_token},
            headers={"Range": "bytes=9999-10000"},
        )
        assert res_416.status_code == 416
        assert res_416.headers.get("x-robots-tag") == "noindex, nofollow"
        assert res_416.headers.get("content-range") == f"bytes */{len(payload)}"

        # Unified /api/media/{id}/stream supports HEAD, Range, 416, and download=true
        media_head = client.head("/api/media/m1/stream")
        assert media_head.status_code == 200
        assert media_head.headers.get("content-length") == str(len(payload))
        assert media_head.headers.get("accept-ranges") == "bytes"
        assert media_head.content == b""

        media_head_range = client.head("/api/media/m1/stream", headers={"Range": "bytes=4-7"})
        assert media_head_range.status_code == 206
        assert media_head_range.headers.get("content-range") == f"bytes 4-7/{len(payload)}"
        assert media_head_range.headers.get("content-length") == "4"

        media_416 = client.get("/api/media/m1/stream", headers={"Range": "bytes=500-600"})
        assert media_416.status_code == 416
        assert media_416.headers.get("content-range") == f"bytes */{len(payload)}"

        media_dl = client.get("/api/media/m1/stream", params={"download": "true"})
        assert media_dl.status_code == 200
        assert 'attachment; filename="clip.mp4"' in media_dl.headers.get("content-disposition", "")
        assert media_dl.content == payload

        # Static JS regression guards for Sections 2 & 3
        wp_js = client.get("/static/js/watch-party.js").text
        assert "effective_position_sec" in wp_js
        assert "state_updated_at_ms" in wp_js
        assert "loadedmetadata" in wp_js
        assert "addEventListener(\"ended\"" in wp_js
        assert "incomingSeq > lastStateSeq" in wp_js

        host_js = client.get("/static/js/features/watch-party-host.js").text
        assert "HOST_DRIFT_THRESHOLD_SEC" in host_js
        assert "HOST_NO_RECONNECT_CODES" in host_js
        assert "renderQrSvg" in host_js
        assert "!activeId || queuedIds.includes(activeId)" in host_js
        assert "if (hostRoomCode && hostToken)" in host_js

        qr_js = client.get("/static/js/ui/qr.js")
        assert qr_js.status_code == 200
        assert "export function encodeQrMatrix" in qr_js.text
        assert "export function renderQrSvg" in qr_js.text
        qr_without_svg_ns = qr_js.text.replace("http://www.w3.org/2000/svg", "")
        assert "http://" not in qr_without_svg_ns and "https://" not in qr_without_svg_ns


def test_buffering_recovery_manual_overrides_actor_ids_and_stale_timeout(tmp_path: Path):
    settings, app = _make_client(tmp_path)
    db = app.state.db
    _insert_media(
        db,
        tmp_path,
        media_id="m1",
        filename="movie.mp4",
        payload=b"VIDEO" * 32,
        mime_type="video/mp4",
        duration_sec=300.0,
    )
    _insert_media(
        db,
        tmp_path,
        media_id="m2",
        filename="sequel.mp4",
        payload=b"VIDEO2" * 32,
        mime_type="video/mp4",
        duration_sec=200.0,
    )

    with TestClient(app) as client:
        room = _create_room(
            client,
            media_ids=["m1", "m2"],
            wait_for_buffering=True,
            initial_is_playing=False,
        )
        code = room["room_code"]
        with client.websocket_connect(f"/api/watch-party/{code}/ws?wp_token={room['host_token']}") as host:
            host_hello = _recv(host, "auth_ok")
            host_pid = host_hello["participant_id"]
            guest = _join(client, code, name="Slow Peer").json()
            guest_pid = guest["participant_id"]
            with client.websocket_connect(f"/api/watch-party/{code}/ws") as guest_ws:
                guest_ws.send_json({"type": "auth", "token": guest["guest_token"]})
                assert _recv(guest_ws, "auth_ok")["type"] == "auth_ok"
                assert _recv(host, "presence")["event"] == "join"

                # 1. Buffering reports while manually paused are ignored and never stick.
                guest_ws.send_json({"type": "buffering", "is_buffering": True})
                guest_ws.send_json({"type": "ping", "client_time_ms": 1})
                assert _recv(guest_ws, "pong")["type"] == "pong"
                paused_view = _room_view(client, code)["room_state"]
                assert paused_view["is_playing"] is False
                assert paused_view["paused_for_buffering"] is False
                assert all(not p["is_buffering"] for p in paused_view["participants"])

                # 2. Control sync_state broadcasts carry actor_participant_id.
                host.send_json({"type": "play", "position_sec": 12.0})
                h_play = _recv(host, "sync_state")
                g_play = _recv(guest_ws, "sync_state")
                assert h_play["actor_participant_id"] == host_pid
                assert g_play["actor_participant_id"] == host_pid
                assert h_play["room_state"]["is_playing"] is True

                host.send_json({"type": "seek", "position_sec": 25.0})
                h_seek = _recv(host, "sync_state")
                g_seek = _recv(guest_ws, "sync_state")
                assert h_seek["actor_participant_id"] == host_pid
                assert g_seek["actor_participant_id"] == host_pid
                assert h_seek["room_state"]["position_sec"] == pytest.approx(25.0, abs=0.05)

                host.send_json({"type": "rate", "rate": 1.25})
                assert _recv(host, "sync_state")["actor_participant_id"] == host_pid
                assert _recv(guest_ws, "sync_state")["actor_participant_id"] == host_pid

                # 3. Guest buffers -> room pauses for buffering; explicit host play clears stuck state.
                guest_ws.send_json({"type": "buffering", "is_buffering": True})
                h_buf = _recv(host, "sync_state")
                g_buf = _recv(guest_ws, "sync_state")
                assert h_buf["actor_participant_id"] == guest_pid
                assert g_buf["room_state"]["paused_for_buffering"] is True
                assert g_buf["room_state"]["is_playing"] is False

                host.send_json({"type": "play", "position_sec": 26.0})
                h_override = _recv(host, "sync_state")
                g_override = _recv(guest_ws, "sync_state")
                assert h_override["room_state"]["is_playing"] is True
                assert h_override["room_state"]["paused_for_buffering"] is False
                assert all(not p["is_buffering"] for p in g_override["room_state"]["participants"])

                # 4. Explicit seek during paused_for_buffering clears stuck buffering and resumes.
                guest_ws.send_json({"type": "buffering", "is_buffering": True})
                assert _recv(host, "sync_state")["room_state"]["paused_for_buffering"] is True
                assert _recv(guest_ws, "sync_state")["room_state"]["paused_for_buffering"] is True

                host.send_json({"type": "seek", "position_sec": 60.0})
                h_seek2 = _recv(host, "sync_state")
                g_seek2 = _recv(guest_ws, "sync_state")
                assert h_seek2["room_state"]["is_playing"] is True
                assert h_seek2["room_state"]["paused_for_buffering"] is False
                assert h_seek2["room_state"]["position_sec"] == pytest.approx(60.0, abs=0.05)
                assert all(not p["is_buffering"] for p in g_seek2["room_state"]["participants"])

                # 5. Manual pause clears all buffering flags cleanly.
                guest_ws.send_json({"type": "buffering", "is_buffering": True})
                assert _recv(host, "sync_state")["room_state"]["paused_for_buffering"] is True
                assert _recv(guest_ws, "sync_state")["room_state"]["paused_for_buffering"] is True

                host.send_json({"type": "pause", "position_sec": 61.0})
                h_pause = _recv(host, "sync_state")
                g_pause = _recv(guest_ws, "sync_state")
                assert h_pause["actor_participant_id"] == host_pid
                assert h_pause["room_state"]["is_playing"] is False
                assert h_pause["room_state"]["paused_for_buffering"] is False
                assert all(not p["is_buffering"] for p in g_pause["room_state"]["participants"])

                # 6. Disabling wait_for_buffering via WS resumes a room paused for buffering.
                host.send_json({"type": "play", "position_sec": 61.0})
                _recv(host, "sync_state")
                _recv(guest_ws, "sync_state")
                guest_ws.send_json({"type": "buffering", "is_buffering": True})
                assert _recv(host, "sync_state")["room_state"]["paused_for_buffering"] is True
                assert _recv(guest_ws, "sync_state")["room_state"]["paused_for_buffering"] is True

                host.send_json({"type": "update_settings", "wait_for_buffering": False})
                h_off = _recv(host, "sync_state")
                g_off = _recv(guest_ws, "sync_state")
                assert h_off["room_state"]["wait_for_buffering"] is False
                assert h_off["room_state"]["paused_for_buffering"] is False
                assert g_off["room_state"]["is_playing"] is True

                # 7. Disabling wait_for_buffering via HTTP PATCH also resumes a buffering-paused room.
                host.send_json({"type": "update_settings", "wait_for_buffering": True})
                _recv(host, "sync_state")
                _recv(guest_ws, "sync_state")
                guest_ws.send_json({"type": "buffering", "is_buffering": False})
                _recv(host, "sync_state")
                _recv(guest_ws, "sync_state")
                guest_ws.send_json({"type": "buffering", "is_buffering": True})
                assert _recv(host, "sync_state")["room_state"]["paused_for_buffering"] is True
                assert _recv(guest_ws, "sync_state")["room_state"]["paused_for_buffering"] is True

                patched = client.patch(
                    f"/api/watch-party/rooms/{code}",
                    json={"wait_for_buffering": False},
                )
                assert patched.status_code == 200
                h_patch = _recv(host, "sync_state")
                g_patch = _recv(guest_ws, "sync_state")
                assert h_patch["room_state"]["is_playing"] is True
                assert g_patch["room_state"]["paused_for_buffering"] is False

                # 8. Server-side stale buffering timeout auto-recovers even if client never clears.
                wp = app.state.watch_party
                wp._buffering_timeout_sec = 0.15
                host.send_json({"type": "update_settings", "wait_for_buffering": True})
                _recv(host, "sync_state")
                _recv(guest_ws, "sync_state")
                guest_ws.send_json({"type": "buffering", "is_buffering": False})
                _recv(host, "sync_state")
                _recv(guest_ws, "sync_state")

                guest_ws.send_json({"type": "buffering", "is_buffering": True})
                assert _recv(host, "sync_state")["room_state"]["paused_for_buffering"] is True
                assert _recv(guest_ws, "sync_state")["room_state"]["paused_for_buffering"] is True

                # Without any further message from guest, timeout clears buffering and resumes.
                h_auto = _recv(host, "sync_state")
                g_auto = _recv(guest_ws, "sync_state")
                assert h_auto["reason"] == "buffering"
                assert h_auto["room_state"]["is_playing"] is True
                assert h_auto["room_state"]["paused_for_buffering"] is False
                assert all(not p["is_buffering"] for p in g_auto["room_state"]["participants"])

                # 9. Selecting another media item clears buffering and includes actor_participant_id.
                host.send_json({"type": "select_item", "media_id": "m2"})
                h_sel = _recv(host, "sync_state")
                g_sel = _recv(guest_ws, "sync_state")
                assert h_sel["actor_participant_id"] == host_pid
                assert g_sel["room_state"]["active_media_id"] == "m2"


def test_watch_party_client_buffering_and_seek_regressions(tmp_path: Path):
    wp_js = (STATIC_DIR / "js" / "watch-party.js").read_text(encoding="utf-8")
    host_js = (STATIC_DIR / "js" / "features" / "watch-party-host.js").read_text(encoding="utf-8")

    # Standalone client: debounced buffering, paused-state recovery, seek protection, echo skip.
    for token in (
        "BUFFERING_REPORT_DELAY_MS",
        "BUFFERING_RESUME_AHEAD_SEC",
        "POST_RESUME_GRACE_MS",
        "checkBufferingReady",
        'addEventListener("canplaythrough"',
        'addEventListener("seeking"',
        'addEventListener("seeked"',
        "isUserSeeking()",
        "setVideoTimeSynced",
        "isProgrammaticSeekEvent",
        "actor_participant_id",
        'reason === "buffering" && !playbackChanged',
    ):
        assert token in wp_js, token
    # Ensure canplay is NOT gated on `!video.paused` (which caused the wait-for-buffering deadlock).
    assert 'addEventListener("canplay", () => {\n    if (!video.paused)' not in wp_js

    # Host Cinema bridge: clock offset, debounced buffering, paused-state recovery, seek protection, echo skip.
    for token in (
        "HOST_BUFFERING_REPORT_DELAY_MS",
        "HOST_BUFFERING_RESUME_AHEAD_SEC",
        "HOST_POST_RESUME_GRACE_MS",
        "handleHostPong",
        "hostClockOffsetMs",
        "checkHostBufferingReady",
        'addEventListener("canplaythrough"',
        'addEventListener("seeking"',
        'addEventListener("seeked"',
        "isHostUserSeeking()",
        "setHostVideoTimeSynced",
        "isHostProgrammaticSeekEvent",
        "actor_participant_id",
        'reason === "buffering" && !bufferingPauseChanged',
    ):
        assert token in host_js, token



