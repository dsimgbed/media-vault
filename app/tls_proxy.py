from __future__ import annotations

import asyncio
import logging
import socket
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)


def _parse_connect_target(target: str) -> tuple[str, int]:
    raw = target.strip()
    if raw.startswith("["):
        end_bracket = raw.find("]")
        if end_bracket == -1:
            raise ValueError(f"Invalid CONNECT target: {target!r}")
        host = raw[1:end_bracket]
        rest = raw[end_bracket + 1 :]
        if rest.startswith(":"):
            port = int(rest[1:])
        elif not rest:
            port = 443
        else:
            raise ValueError(f"Invalid CONNECT target: {target!r}")
        return host, port
    if ":" in raw:
        host, port_s = raw.rsplit(":", 1)
        return host, int(port_s)
    return raw, 443


def _is_tls_client_hello(data: bytes) -> bool:
    return (
        len(data) >= 10
        and data[0] == 0x16
        and data[1] == 0x03
        and data[5] == 0x01
    )


def _split_client_hello(data: bytes) -> list[bytes]:
    n = len(data)
    if n <= 10:
        return [data]
    mid = min(n, 5 + max(24, (n - 5) // 2))
    cuts = [2, 5]
    if 5 < mid < n:
        cuts.append(mid)
    chunks: list[bytes] = []
    prev = 0
    for cut in cuts:
        if prev < cut <= n:
            chunks.append(data[prev:cut])
            prev = cut
    if prev < n:
        chunks.append(data[prev:])
    return [c for c in chunks if c]


class TlsFragmentProxy:
    """Lightweight loopback HTTP/CONNECT proxy that fragments TLS ClientHello records.

    Splitting the initial TLS ClientHello across multiple small TCP segments bypasses
    stateless SNI-based packet filters while preserving full TLS verification and SNI.
    """

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        connect_timeout: float = 15.0,
        fragment_delay: float = 0.005,
    ) -> None:
        self.host = host
        self._requested_port = int(port)
        self.connect_timeout = float(connect_timeout)
        self.fragment_delay = float(fragment_delay)
        self._server: asyncio.Server | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock: asyncio.Lock | None = None
        self._port: int | None = None
        self._client_tasks: set[asyncio.Task[None]] = set()

    @property
    def port(self) -> int | None:
        return self._port

    @property
    def url(self) -> str:
        if self._port is None:
            return ""
        return f"http://{self.host}:{self._port}"

    @property
    def is_running(self) -> bool:
        return (
            self._server is not None
            and self._server.is_serving()
            and self._loop is not None
            and not self._loop.is_closed()
        )

    async def start(self) -> str:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            if self._server is not None:
                try:
                    self._server.close()
                except Exception:
                    pass
                self._server = None
            self._port = None
            self._client_tasks.clear()
            self._loop = loop
            self._lock = asyncio.Lock()

        assert self._lock is not None
        async with self._lock:
            if self.is_running and self._port is not None:
                return self.url
            server = await asyncio.start_server(
                self._dispatch_client,
                host=self.host,
                port=self._requested_port,
            )
            self._server = server
            sock = server.sockets[0] if server.sockets else None
            if sock is None:
                raise RuntimeError("TLS fragment proxy failed to bind a listening socket")
            self._port = int(sock.getsockname()[1])
            logger.info("TLS fragment proxy listening on %s", self.url)
            return self.url

    async def stop(self) -> None:
        server = self._server
        self._server = None
        self._port = None
        tasks = list(self._client_tasks)
        self._client_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if server is not None:
            server.close()
            if hasattr(server, "close_clients"):
                server.close_clients()
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=1.0)
            except Exception:
                if hasattr(server, "abort_clients"):
                    server.abort_clients()

    def _dispatch_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.create_task(self._handle_client(reader, writer))
        self._client_tasks.add(task)
        task.add_done_callback(self._client_tasks.discard)

    async def _pipe(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        fragment_tls: bool = False,
        half_close: bool = True,
    ) -> None:
        first = True
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                if first and fragment_tls and _is_tls_client_hello(data):
                    first = False
                    sock = writer.get_extra_info("socket")
                    if sock is not None:
                        try:
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        except OSError:
                            pass
                    chunks = _split_client_hello(data)
                    for idx, chunk in enumerate(chunks):
                        writer.write(chunk)
                        await writer.drain()
                        if idx + 1 < len(chunks) and self.fragment_delay > 0:
                            await asyncio.sleep(self.fragment_delay)
                    continue
                first = False
                writer.write(data)
                await writer.drain()
            if half_close and writer.can_write_eof():
                try:
                    writer.write_eof()
                except Exception:
                    pass
        except (asyncio.CancelledError, ConnectionError, TimeoutError, OSError):
            pass
        except Exception:
            pass

    @staticmethod
    async def _close_writers(*writers: asyncio.StreamWriter | None) -> None:
        active = [w for w in writers if w is not None]
        for w in active:
            try:
                w.close()
            except Exception:
                pass
        for w in active:
            try:
                await asyncio.wait_for(w.wait_closed(), timeout=0.5)
            except Exception:
                pass

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        r_writer: asyncio.StreamWriter | None = None
        try:
            try:
                raw_header = await asyncio.wait_for(
                    reader.readuntil(b"\r\n\r\n"),
                    timeout=self.connect_timeout,
                )
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError, ConnectionError):
                return

            lines = raw_header[:-4].split(b"\r\n")
            if not lines:
                return
            try:
                request_line = lines[0].decode("latin-1")
                method, target, _version = request_line.split(" ", 2)
            except ValueError:
                writer.write(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n")
                await writer.drain()
                return

            if method.upper() == "CONNECT":
                try:
                    host, port = _parse_connect_target(target)
                except ValueError:
                    writer.write(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n")
                    await writer.drain()
                    return
                try:
                    r_reader, r_writer = await asyncio.wait_for(
                        asyncio.open_connection(host, port),
                        timeout=self.connect_timeout,
                    )
                except Exception:
                    writer.write(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n")
                    await writer.drain()
                    return

                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()

                c2s = asyncio.create_task(
                    self._pipe(reader, r_writer, fragment_tls=True, half_close=True)
                )
                s2c = asyncio.create_task(
                    self._pipe(r_reader, writer, fragment_tls=False, half_close=True)
                )
                try:
                    await asyncio.gather(c2s, s2c, return_exceptions=True)
                finally:
                    for t in (c2s, s2c):
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(c2s, s2c, return_exceptions=True)
                return

            parsed = urlsplit(target)
            if parsed.scheme.lower() != "http" or not parsed.hostname:
                writer.write(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n")
                await writer.drain()
                return

            host = parsed.hostname
            port = parsed.port or 80
            origin_path = parsed.path or "/"
            if parsed.query:
                origin_path = f"{origin_path}?{parsed.query}"

            try:
                r_reader, r_writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=self.connect_timeout,
                )
            except Exception:
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n")
                await writer.drain()
                return

            out_lines = [f"{method} {origin_path} HTTP/1.1".encode("latin-1")]
            has_host = False
            for hline in lines[1:]:
                if not hline:
                    continue
                low = hline.lower()
                if low.startswith((b"proxy-connection:", b"proxy-authorization:", b"connection:")):
                    continue
                if low.startswith(b"host:"):
                    has_host = True
                out_lines.append(hline)
            if not has_host:
                out_lines.append(f"Host: {parsed.netloc}".encode("latin-1"))
            out_lines.append(b"Connection: close")
            r_writer.write(b"\r\n".join(out_lines) + b"\r\n\r\n")
            await r_writer.drain()

            c2s = asyncio.create_task(
                self._pipe(reader, r_writer, fragment_tls=False, half_close=True)
            )
            s2c = asyncio.create_task(
                self._pipe(r_reader, writer, fragment_tls=False, half_close=True)
            )
            try:
                await s2c
            finally:
                if not c2s.done():
                    c2s.cancel()
                await asyncio.gather(c2s, s2c, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            await self._close_writers(r_writer, writer)
