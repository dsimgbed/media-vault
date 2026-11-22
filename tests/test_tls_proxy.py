from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app
from app.tls_proxy import (
    TlsFragmentProxy,
    _is_tls_client_hello,
    _parse_connect_target,
    _split_client_hello,
)
from tests.helpers import _make_settings


def test_tls_proxy_helpers():
    assert _parse_connect_target("www.eporner.com:443") == ("www.eporner.com", 443)
    assert _parse_connect_target("www.pornhub.com") == ("www.pornhub.com", 443)
    assert _parse_connect_target("[::1]:8443") == ("::1", 8443)
    assert _parse_connect_target("[2001:db8::1]") == ("2001:db8::1", 443)

    hello = b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc" + b"X" * 200
    assert _is_tls_client_hello(hello) is True
    assert _is_tls_client_hello(b"GET / HTTP/1.1\r\n") is False
    assert _is_tls_client_hello(b"\x16\x03\x01") is False

    parts = _split_client_hello(hello)
    assert len(parts) >= 3
    assert parts[0] == hello[:2]
    assert parts[1] == hello[2:5]
    assert b"".join(parts) == hello


def test_tls_proxy_connect_fragments_client_hello_and_forwards_http():
    async def run() -> None:
        received_chunks: list[bytes] = []

        async def echo_tls_upstream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    chunk = await reader.read(4096)
                    if not chunk:
                        break
                    received_chunks.append(chunk)
                    writer.write(chunk)
                    await writer.drain()
            finally:
                writer.close()

        async def http_upstream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                req = await reader.readuntil(b"\r\n\r\n")
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Length: "
                    + str(len(req)).encode()
                    + b"\r\nConnection: close\r\n\r\n"
                    + req
                )
                await writer.drain()
            finally:
                writer.close()

        tls_srv = await asyncio.start_server(echo_tls_upstream, "127.0.0.1", 0)
        http_srv = await asyncio.start_server(http_upstream, "127.0.0.1", 0)
        tls_port = tls_srv.sockets[0].getsockname()[1]
        http_port = http_srv.sockets[0].getsockname()[1]

        proxy = TlsFragmentProxy(fragment_delay=0.01)
        proxy_url = await proxy.start()
        assert proxy.is_running
        assert proxy_url.startswith("http://127.0.0.1:")
        proxy_port = proxy.port
        assert proxy_port is not None

        try:
            # 1. CONNECT tunnel with TLS ClientHello fragmentation
            r, w = await asyncio.open_connection("127.0.0.1", proxy_port)
            w.write(f"CONNECT 127.0.0.1:{tls_port} HTTP/1.1\r\nHost: 127.0.0.1:{tls_port}\r\n\r\n".encode())
            await w.drain()
            resp_hdr = await r.readuntil(b"\r\n\r\n")
            assert resp_hdr.startswith(b"HTTP/1.1 200")

            fake_hello = b"\x16\x03\x01\x01\x00\x01\x00\x00\xfc" + b"Z" * 256
            w.write(fake_hello)
            await w.drain()
            echoed = await r.readexactly(len(fake_hello))
            assert echoed == fake_hello
            assert len(received_chunks) >= 2
            assert b"".join(received_chunks) == fake_hello
            w.close()
            await w.wait_closed()

            # 2. Plain HTTP proxy GET request forwarding
            r2, w2 = await asyncio.open_connection("127.0.0.1", proxy_port)
            w2.write(
                (
                    f"GET http://127.0.0.1:{http_port}/xhr/video/abc?x=1 HTTP/1.1\r\n"
                    f"Host: 127.0.0.1:{http_port}\r\n"
                    "Proxy-Connection: keep-alive\r\n"
                    "\r\n"
                ).encode()
            )
            await w2.drain()
            raw_http_resp = await r2.read()
            assert raw_http_resp.startswith(b"HTTP/1.1 200 OK")
            assert b"GET /xhr/video/abc?x=1 HTTP/1.1" in raw_http_resp
            assert b"Proxy-Connection:" not in raw_http_resp
            assert b"Connection: close" in raw_http_resp
            w2.close()
            await w2.wait_closed()
        finally:
            await proxy.stop()
            assert not proxy.is_running
            tls_srv.close()
            http_srv.close()
            await tls_srv.wait_closed()
            await http_srv.wait_closed()

    asyncio.run(run())


def test_ingest_effective_proxy_and_router_wiring(tmp_path: Path, monkeypatch):
    # 1. Explicit YTDLP_PROXY overrides built-in TLS fragment proxy
    settings_explicit = _make_settings(tmp_path / "explicit")
    settings_explicit.ytdlp_proxy = "socks5://127.0.0.1:1080"
    app_explicit = create_app(settings_explicit)
    assert asyncio.run(app_explicit.state.ingest.get_effective_proxy()) == "socks5://127.0.0.1:1080"
    assert not app_explicit.state.ingest._tls_proxy.is_running

    # 2. Disabled TLS fragment proxy returns empty proxy
    settings_disabled = _make_settings(tmp_path / "disabled")
    settings_disabled.ytdlp_tls_fragment_proxy = False
    app_disabled = create_app(settings_disabled)
    assert asyncio.run(app_disabled.state.ingest.get_effective_proxy()) == ""
    assert not app_disabled.state.ingest._tls_proxy.is_running

    # 3. Default settings start TLS fragment proxy on demand and pass it to router calls
    settings = _make_settings(tmp_path / "auto")
    app = create_app(settings)
    seen_proxies: list[str] = []

    async def fake_subproc(*args: str):
        if "--proxy" in args:
            seen_proxies.append(args[args.index("--proxy") + 1])
        return (
            0,
            json.dumps(
                {
                    "title": "Test Video",
                    "webpage_url": "https://www.eporner.com/video-abc/test/",
                    "formats": [
                        {"format_id": "720p", "ext": "mp4", "vcodec": "h264", "acodec": "aac", "height": 720}
                    ],
                }
            ),
            "",
        )

    monkeypatch.setattr("app.ingest._run_subprocess", fake_subproc)

    with TestClient(app) as client:
        res = client.post(
            "/api/ytdlp/formats",
            json={"url": "https://www.eporner.com/video-abc/test/"},
        )
        assert res.status_code == 200
        assert len(seen_proxies) == 1
        assert seen_proxies[0].startswith("http://127.0.0.1:")
        assert app.state.ingest._tls_proxy.is_running

    assert not app.state.ingest._tls_proxy.is_running


def test_update_ytdlp_includes_curl_cffi(monkeypatch):
    from app.ingest import update_ytdlp

    recorded: list[tuple[str, ...]] = []

    async def fake_subproc(*args: str):
        recorded.append(args)
        if "--version" in args:
            return 0, "2026.10.26\n", ""
        return 0, "Successfully installed yt-dlp\n", ""

    monkeypatch.setattr("app.ingest._run_subprocess", fake_subproc)
    res = asyncio.run(update_ytdlp())
    assert res["ok"] is True
    pip_call = next(c for c in recorded if "pip" in c)
    assert "yt-dlp[default,curl-cffi]" in pip_call
