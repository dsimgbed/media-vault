from __future__ import annotations

import asyncio
import io
import shutil
import struct
import subprocess
import tempfile
import zipfile
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.streaming_zip import (
    ZIP64_ENTRY_COUNT_LIMIT,
    ZIP64_LIMIT,
    StreamingZipEntry,
    deduplicate_arcname,
    iter_streaming_zip64,
    sanitize_zip_arcname,
)


async def _collect_archive(
    entries: list[StreamingZipEntry],
    *,
    force_zip64: bool = False,
    zip64_limit: int = ZIP64_LIMIT,
    zip64_count_limit: int = ZIP64_ENTRY_COUNT_LIMIT,
) -> bytes:
    buf = bytearray()
    async for chunk in iter_streaming_zip64(
        entries,
        force_zip64=force_zip64,
        zip64_limit=zip64_limit,
        zip64_count_limit=zip64_count_limit,
    ):
        buf.extend(chunk)
    return bytes(buf)


def _chunked_stream(chunks: list[bytes]) -> AsyncIterator[bytes]:
    async def _gen() -> AsyncIterator[bytes]:
        for chunk in chunks:
            await asyncio.sleep(0)
            yield chunk

    return _gen()


def test_constants():
    assert ZIP64_LIMIT == 0xFFFFFFFF
    assert ZIP64_ENTRY_COUNT_LIMIT == 0xFFFF


def test_sanitize_zip_arcname():
    assert sanitize_zip_arcname(r"folder\sub\photo.jpg") == "folder/sub/photo.jpg"
    assert sanitize_zip_arcname("../../etc/passwd") == "etc/passwd"
    assert sanitize_zip_arcname("/./a//../b/\x00c\x1f\x7f.txt") == "a/b/c.txt"
    assert sanitize_zip_arcname("日本語/写真🎉.png") == "日本語/写真🎉.png"
    assert sanitize_zip_arcname("", fallback="custom_fallback.bin") == "custom_fallback.bin"
    assert sanitize_zip_arcname(".././\x00", fallback="../safe.dat") == "safe.dat"
    assert sanitize_zip_arcname("../..", fallback="..") == "file.bin"
    assert sanitize_zip_arcname(r"C:\Windows\cmd.exe") == "Windows/cmd.exe"
    assert sanitize_zip_arcname("C:secret.txt") == "secret.txt"
    assert sanitize_zip_arcname("D:/dir/sub/file.bin") == "dir/sub/file.bin"
    assert sanitize_zip_arcname("C:", fallback="safe.bin") == "safe.bin"


def test_deduplicate_arcname():
    used: set[str] = set()
    first = deduplicate_arcname("dir/photo.jpg", used, index=0, media_id="abcdef1234")
    second = deduplicate_arcname(r"other\photo.jpg", used, index=1, media_id="1234567890")
    # Force exact collision against the generated deduplicated name
    third = deduplicate_arcname("photo_2_123456.jpg", used, index=2, media_id="zzzzzz")
    fourth = deduplicate_arcname("photo.jpg", used, index=1, media_id="1234567890")
    fifth = deduplicate_arcname("photo.jpg", used, index=1, media_id="1234567890")

    assert first == "photo.jpg"
    assert second == "photo_2_123456.jpg"
    assert third == "photo_2_123456_3_zzzzzz.jpg"
    assert fourth == "photo_2_123456_2.jpg"
    assert fifth == "photo_2_123456_3.jpg"
    assert used == {first, second, third, fourth, fifth}

    # Unsafe / empty / Windows drive-letter names fall back cleanly and still deduplicate
    used_safe: set[str] = set()
    assert deduplicate_arcname("..\x00", used_safe, index=0, media_id="a1b2c3d4") == "file.bin"
    assert deduplicate_arcname("", used_safe, index=1, media_id="a1b2c3d4") == "file_2_a1b2c3.bin"
    assert deduplicate_arcname("C:secret.txt", used_safe, index=2, media_id="a1b2c3d4") == "secret.txt"
    assert deduplicate_arcname(r"C:\Windows\cmd.exe", used_safe, index=3, media_id="a1b2c3d4") == "cmd.exe"


def test_multi_file_archive_roundtrip_and_async_factory():
    payload_a = b"alpha-" * 1000
    payload_b = b"bravo-" * 333
    payload_c = b"\x00\xff\x10\x20" * 512

    async def async_factory() -> AsyncIterator[bytes]:
        await asyncio.sleep(0)
        return _chunked_stream([b"", payload_c[:200], b"", payload_c[200:]])

    entries = [
        StreamingZipEntry(
            arcname="dir/alpha.bin",
            stream_factory=lambda: _chunked_stream([payload_a[:500], payload_a[500:]]),
            expected_size=len(payload_a),
            mtime=datetime(2025, 6, 15, 14, 30, 40, tzinfo=timezone.utc),
        ),
        StreamingZipEntry(
            arcname="bravo.txt",
            stream_factory=lambda: _chunked_stream([payload_b]),
            expected_size=len(payload_b),
            mtime=datetime(1970, 1, 1, 0, 0, 0),  # clamps to 1980
        ),
        StreamingZipEntry(
            arcname="charlie.dat",
            stream_factory=async_factory,
            expected_size=None,  # unknown size -> per-entry ZIP64
            mtime=datetime(2150, 12, 31, 23, 59, 58),  # clamps to 2107
        ),
    ]

    archive = asyncio.run(_collect_archive(entries))

    with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
        assert zf.testzip() is None
        assert zf.namelist() == ["dir/alpha.bin", "bravo.txt", "charlie.dat"]
        assert zf.read("dir/alpha.bin") == payload_a
        assert zf.read("bravo.txt") == payload_b
        assert zf.read("charlie.dat") == payload_c
        assert zf.getinfo("dir/alpha.bin").date_time == (2025, 6, 15, 14, 30, 40)
        assert zf.getinfo("bravo.txt").date_time[0] == 1980
        assert zf.getinfo("charlie.dat").date_time[0] == 2107

    if shutil.which("unzip"):
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
            tmp.write(archive)
            tmp.flush()
            proc = subprocess.run(["unzip", "-t", tmp.name], capture_output=True, text=True)
            assert proc.returncode == 0, proc.stdout + proc.stderr


def test_duplicate_filenames_in_streamed_archive():
    used: set[str] = set()
    items = [
        ("id111111", "album/shot.jpg", b"one"),
        ("id222222", "other/shot.jpg", b"two"),
        ("id333333", "shot.jpg", b"three"),
    ]
    entries = [
        StreamingZipEntry(
            arcname=deduplicate_arcname(fname, used, index=idx, media_id=mid),
            stream_factory=(lambda data=body: _chunked_stream([data])),
            expected_size=len(body),
        )
        for idx, (mid, fname, body) in enumerate(items)
    ]

    archive = asyncio.run(_collect_archive(entries))
    with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
        assert zf.testzip() is None
        assert zf.namelist() == [
            "shot.jpg",
            "shot_2_id2222.jpg",
            "shot_3_id3333.jpg",
        ]
        assert zf.read("shot.jpg") == b"one"
        assert zf.read("shot_2_id2222.jpg") == b"two"
        assert zf.read("shot_3_id3333.jpg") == b"three"


def test_utf8_emoji_cjk_filenames():
    names_and_data = [
        ("日本語/東京タワー🗼.txt", "東京".encode("utf-8")),
        ("中文/测试文件📁/报告_🎉.bin", b"cjk-emoji-payload"),
        ("café/naïve_résumé_🚀.dat", b"accented-utf8"),
    ]
    entries = [
        StreamingZipEntry(
            arcname=name,
            stream_factory=(lambda d=data: _chunked_stream([d])),
            expected_size=len(data),
        )
        for name, data in names_and_data
    ]

    archive = asyncio.run(_collect_archive(entries))
    with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
        assert zf.testzip() is None
        assert zf.namelist() == [name for name, _ in names_and_data]
        for name, data in names_and_data:
            info = zf.getinfo(name)
            assert info.flag_bits & 0x0800
            assert info.flag_bits & 0x0008
            assert zf.read(name) == data


def test_empty_files_and_empty_archive():
    empty_archive = asyncio.run(_collect_archive([]))
    with zipfile.ZipFile(io.BytesIO(empty_archive), "r") as zf:
        assert zf.testzip() is None
        assert zf.namelist() == []

    entries = [
        StreamingZipEntry(
            arcname="zero_known.bin",
            stream_factory=lambda: _chunked_stream([]),
            expected_size=0,
        ),
        StreamingZipEntry(
            arcname="zero_unknown.bin",
            stream_factory=lambda: _chunked_stream([b""]),
            expected_size=None,
        ),
        StreamingZipEntry(
            arcname="nonempty.txt",
            stream_factory=lambda: _chunked_stream([b"hello"]),
            expected_size=5,
        ),
    ]
    archive = asyncio.run(_collect_archive(entries))
    with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
        assert zf.testzip() is None
        assert zf.namelist() == ["zero_known.bin", "zero_unknown.bin", "nonempty.txt"]
        assert zf.read("zero_known.bin") == b""
        assert zf.read("zero_unknown.bin") == b""
        assert zf.read("nonempty.txt") == b"hello"


def _parse_lfh_extra_ids_and_dd_sizes(archive: bytes) -> list[tuple[str, list[int], int]]:
    """Walk LFHs + payloads + Data Descriptors from the start of a STORED archive."""
    # Find Central Directory start via Zip64 EOCD or standard EOCD
    eocd_pos = archive.rfind(b"PK\x05\x06")
    assert eocd_pos != -1
    _, _, _, _, _, cd_size, cd_offset, _ = struct.unpack_from("<4sHHHHIIH", archive, eocd_pos)
    loc_pos = archive.rfind(b"PK\x06\x07", 0, eocd_pos)
    if loc_pos != -1 and loc_pos == eocd_pos - 20:
        _, _, z64_eocd_off, _ = struct.unpack_from("<4sIQI", archive, loc_pos)
        _, _, _, _, _, _, _, _, cd_size, cd_offset = struct.unpack_from(
            "<4sQHHIIQQQQ", archive, z64_eocd_off
        )

    pos = 0
    results: list[tuple[str, list[int], int]] = []
    # Read entry sizes from Central Directory via zipfile so we know payload lengths
    with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
        infos = zf.infolist()

    for info in infos:
        assert archive[pos : pos + 4] == b"PK\x03\x04"
        _, ver, flag, comp, _, _, crc, csz, usz, nlen, xlen = struct.unpack_from(
            "<4sHHHHHIIIHH", archive, pos
        )
        assert comp == 0
        assert flag == 0x0808
        assert (crc, csz, usz) == (0, 0, 0)
        pos += 30
        fname = archive[pos : pos + nlen].decode("utf-8")
        pos += nlen
        extra = archive[pos : pos + xlen]
        pos += xlen

        extra_ids: list[int] = []
        epos = 0
        while epos + 4 <= len(extra):
            tag, sz = struct.unpack_from("<HH", extra, epos)
            extra_ids.append(tag)
            epos += 4 + sz

        pos += info.file_size
        assert archive[pos : pos + 4] == b"PK\x07\x08"
        if 0x0001 in extra_ids:
            assert ver == 45
            dd_sig, dd_crc, dd_csz, dd_usz = struct.unpack_from("<4sIQQ", archive, pos)
            dd_len = 24
        else:
            assert ver == 20
            dd_sig, dd_crc, dd_csz, dd_usz = struct.unpack_from("<4sIII", archive, pos)
            dd_len = 16
        assert dd_sig == b"PK\x07\x08"
        assert dd_crc == info.CRC
        assert dd_csz == info.file_size
        assert dd_usz == info.file_size
        pos += dd_len
        results.append((fname, extra_ids, dd_len))

    assert pos == cd_offset
    assert pos + cd_size <= eocd_pos
    return results


def test_zip64_force_flag_and_binary_markers():
    entries = [
        StreamingZipEntry("one.bin", lambda: _chunked_stream([b"hello "]), expected_size=6),
        StreamingZipEntry("two.bin", lambda: _chunked_stream([b"world!"]), expected_size=6),
    ]
    archive = asyncio.run(_collect_archive(entries, force_zip64=True))

    with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
        assert zf.testzip() is None
        assert zf.read("one.bin") == b"hello "
        assert zf.read("two.bin") == b"world!"
        for info in zf.infolist():
            assert info.extract_version == 45

    assert b"PK\x06\x06" in archive
    assert b"PK\x06\x07" in archive
    eocd_pos = archive.rfind(b"PK\x05\x06")
    assert eocd_pos == len(archive) - 22
    loc_pos = eocd_pos - 20
    assert archive[loc_pos : loc_pos + 4] == b"PK\x06\x07"
    z64_eocd_pos = loc_pos - 56
    assert archive[z64_eocd_pos : z64_eocd_pos + 4] == b"PK\x06\x06"

    parsed = _parse_lfh_extra_ids_and_dd_sizes(archive)
    assert parsed == [
        ("one.bin", [0x0001], 24),
        ("two.bin", [0x0001], 24),
    ]

    if shutil.which("unzip"):
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
            tmp.write(archive)
            tmp.flush()
            proc = subprocess.run(["unzip", "-t", tmp.name], capture_output=True, text=True)
            assert proc.returncode == 0, proc.stdout + proc.stderr


def test_zip64_lowered_size_and_offset_threshold():
    # Entry 1: 80 bytes (>= zip64_limit=64) -> size triggers ZIP64
    # Entry 2: 10 bytes (< 64), but local_header_offset >= 64 -> offset triggers ZIP64
    big_data = b"X" * 80
    small_data = b"tiny-entry"
    entries = [
        StreamingZipEntry("big.bin", lambda: _chunked_stream([big_data]), expected_size=len(big_data)),
        StreamingZipEntry(
            "small_after_offset.bin",
            lambda: _chunked_stream([small_data]),
            expected_size=len(small_data),
        ),
    ]

    archive = asyncio.run(_collect_archive(entries, zip64_limit=64))

    with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
        assert zf.testzip() is None
        assert zf.read("big.bin") == big_data
        assert zf.read("small_after_offset.bin") == small_data
        assert zf.infolist()[0].header_offset == 0
        assert zf.infolist()[1].header_offset >= 64

    assert b"PK\x06\x06" in archive
    assert b"PK\x06\x07" in archive
    parsed = _parse_lfh_extra_ids_and_dd_sizes(archive)
    assert parsed == [
        ("big.bin", [0x0001], 24),
        ("small_after_offset.bin", [0x0001], 24),
    ]


def test_zip64_lowered_count_threshold():
    entries = [
        StreamingZipEntry("a.txt", lambda: _chunked_stream([b"aa"]), expected_size=2),
        StreamingZipEntry("b.txt", lambda: _chunked_stream([b"bb"]), expected_size=2),
        StreamingZipEntry("c.txt", lambda: _chunked_stream([b"cc"]), expected_size=2),
    ]

    archive = asyncio.run(_collect_archive(entries, zip64_count_limit=2))

    with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
        assert zf.testzip() is None
        assert zf.namelist() == ["a.txt", "b.txt", "c.txt"]
        assert zf.read("a.txt") == b"aa"
        assert zf.read("b.txt") == b"bb"
        assert zf.read("c.txt") == b"cc"

    eocd_pos = archive.rfind(b"PK\x05\x06")
    _, _, _, cnt_disk, cnt_total, _, _, _ = struct.unpack_from("<4sHHHHIIH", archive, eocd_pos)
    assert cnt_disk == 0xFFFF
    assert cnt_total == 0xFFFF

    loc_pos = eocd_pos - 20
    assert archive[loc_pos : loc_pos + 4] == b"PK\x06\x07"
    z64_eocd_pos = loc_pos - 56
    assert archive[z64_eocd_pos : z64_eocd_pos + 4] == b"PK\x06\x06"
    _, _, _, _, _, _, z64_cnt_disk, z64_cnt_total, _, _ = struct.unpack_from(
        "<4sQHHIIQQQQ", archive, z64_eocd_pos
    )
    assert z64_cnt_disk == 3
    assert z64_cnt_total == 3


def test_exceeding_zip32_limit_without_zip64_header_raises():
    entries = [
        StreamingZipEntry(
            "liar.bin",
            lambda: _chunked_stream([b"A" * 40, b"B" * 30]),
            expected_size=50,  # < zip64_limit=64, so LFH is ZIP32
        )
    ]
    with pytest.raises(ValueError, match="Entry exceeded ZIP32 limit without ZIP64 header"):
        asyncio.run(_collect_archive(entries, zip64_limit=64))


def test_zero_buffer_lazy_streaming_proof():
    events: list[str] = []
    allow_chunk2 = asyncio.Event()

    async def entry1_stream() -> AsyncIterator[bytes]:
        events.append("entry1:produce_chunk1")
        yield b"chunk-1-"
        await allow_chunk2.wait()
        events.append("entry1:produce_chunk2")
        yield b"chunk-2"

    def entry1_factory() -> AsyncIterator[bytes]:
        events.append("entry1:factory_called")
        return entry1_stream()

    def entry2_factory() -> AsyncIterator[bytes]:
        events.append("entry2:factory_called")
        return _chunked_stream([b"second"])

    async def run() -> bytes:
        gen = iter_streaming_zip64(
            [
                StreamingZipEntry("first.bin", entry1_factory, expected_size=15),
                StreamingZipEntry("second.bin", entry2_factory, expected_size=6),
            ]
        )
        received = bytearray()

        # First yield is LFH for Entry 1 (factory not yet called or called right after LFH)
        lfh1 = await anext(gen)
        assert lfh1.startswith(b"PK\x03\x04")
        received.extend(lfh1)

        # Next yield MUST be Entry 1's chunk-1 BEFORE chunk-2 is produced AND BEFORE entry2_factory
        chunk1 = await anext(gen)
        assert chunk1 == b"chunk-1-"
        assert "entry1:produce_chunk1" in events
        assert "entry1:produce_chunk2" not in events
        assert "entry2:factory_called" not in events
        received.extend(chunk1)

        # Now unblock chunk 2 and consume the rest of the archive
        allow_chunk2.set()
        async for rest in gen:
            received.extend(rest)
        return bytes(received)

    archive = asyncio.run(run())
    assert events == [
        "entry1:factory_called",
        "entry1:produce_chunk1",
        "entry1:produce_chunk2",
        "entry2:factory_called",
    ]
    with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
        assert zf.testzip() is None
        assert zf.read("first.bin") == b"chunk-1-chunk-2"
        assert zf.read("second.bin") == b"second"


def test_midstream_aclose_cleans_up_active_stream_and_skips_later_entries():
    state = {"entry1_closed": False, "entry2_called": False}

    async def entry1_stream() -> AsyncIterator[bytes]:
        try:
            yield b"first-part"
            await asyncio.sleep(10)
            yield b"never-reached"
        finally:
            state["entry1_closed"] = True

    def entry2_factory() -> AsyncIterator[bytes]:
        state["entry2_called"] = True
        return _chunked_stream([b"two"])

    async def run() -> None:
        gen = iter_streaming_zip64(
            [
                StreamingZipEntry("one.bin", entry1_stream, expected_size=23),
                StreamingZipEntry("two.bin", entry2_factory, expected_size=3),
            ]
        )
        lfh = await anext(gen)
        assert lfh.startswith(b"PK\x03\x04")
        first_chunk = await anext(gen)
        assert first_chunk == b"first-part"

        await gen.aclose()

    asyncio.run(run())
    assert state["entry1_closed"] is True
    assert state["entry2_called"] is False


def test_midstream_aclose_calls_sync_close_fallback_on_custom_iterator():
    state = {"closed": False, "entry2_called": False}

    class SyncCloseAsyncIter:
        def __init__(self) -> None:
            self._emitted = False

        def __aiter__(self) -> SyncCloseAsyncIter:
            return self

        async def __anext__(self) -> bytes:
            if not self._emitted:
                self._emitted = True
                return b"payload-part"
            await asyncio.sleep(10)
            return b"unreachable"

        def close(self) -> None:
            state["closed"] = True

    async def run() -> None:
        gen = iter_streaming_zip64(
            [
                StreamingZipEntry("custom.bin", SyncCloseAsyncIter, expected_size=20),
                StreamingZipEntry(
                    "next.bin",
                    lambda: (state.__setitem__("entry2_called", True) or _chunked_stream([b"x"])),
                    expected_size=1,
                ),
            ]
        )
        await anext(gen)  # LFH
        assert await anext(gen) == b"payload-part"
        await gen.aclose()

    asyncio.run(run())
    assert state["closed"] is True
    assert state["entry2_called"] is False


def test_task_cancellation_midstream_cleans_up_upstream_iterator():
    state = {"entry1_aclosed": False, "entry2_called": False}
    chunk1_emitted = asyncio.Event()

    class CustomClosableStream:
        def __init__(self) -> None:
            self._step = 0

        def __aiter__(self) -> CustomClosableStream:
            return self

        async def __anext__(self) -> bytes:
            if self._step == 0:
                self._step = 1
                return b"part-1"
            await asyncio.sleep(10)
            return b"never"

        async def aclose(self) -> None:
            state["entry1_aclosed"] = True

    def entry2_factory() -> AsyncIterator[bytes]:
        state["entry2_called"] = True
        return _chunked_stream([b"two"])

    async def run() -> None:
        gen = iter_streaming_zip64(
            [
                StreamingZipEntry("one.bin", CustomClosableStream, expected_size=12),
                StreamingZipEntry("two.bin", entry2_factory, expected_size=3),
            ]
        )

        async def consumer() -> None:
            async for chunk in gen:
                if chunk == b"part-1":
                    chunk1_emitted.set()

        task = asyncio.create_task(consumer())
        await chunk1_emitted.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert state["entry1_aclosed"] is True
    assert state["entry2_called"] is False
