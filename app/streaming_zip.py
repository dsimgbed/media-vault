from __future__ import annotations

import re
import struct
import zlib
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ZIP64_LIMIT = 0xFFFFFFFF
ZIP64_ENTRY_COUNT_LIMIT = 0xFFFF

_ZIP_STORED = 0
_GP_FLAG_DD_UTF8 = 0x0808  # bit 3 (data descriptor) | bit 11 (UTF-8 filename)
_VERSION_ZIP32 = 20
_VERSION_ZIP64 = 45
_UNIX_REGULAR_FILE_ATTR = 0o100644 << 16
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:+")

_LFH_STRUCT = struct.Struct("<4sHHHHHIIIHH")
_LFH_ZIP64_EXTRA_STRUCT = struct.Struct("<HHQQ")
_DD_ZIP32_STRUCT = struct.Struct("<4sIII")
_DD_ZIP64_STRUCT = struct.Struct("<4sIQQ")
_CDFH_STRUCT = struct.Struct("<4sHHHHHHIIIHHHHHII")
_ZIP64_EXTRA_HDR_STRUCT = struct.Struct("<HH")
_ZIP64_EOCD_STRUCT = struct.Struct("<4sQHHIIQQQQ")
_ZIP64_LOCATOR_STRUCT = struct.Struct("<4sIQI")
_EOCD_STRUCT = struct.Struct("<4sHHHHIIH")


def _strip_control_chars(value: str) -> str:
    return "".join(ch for ch in value if ord(ch) >= 0x20 and ord(ch) != 0x7F)


def _strip_drive_prefix(value: str) -> str:
    return _WINDOWS_DRIVE_RE.sub("", value)


def _clean_arcname_segments(raw_name: str) -> str:
    cleaned = _strip_control_chars(raw_name).replace("\\", "/")
    cleaned = _strip_drive_prefix(cleaned.lstrip())
    parts: list[str] = []
    for raw_seg in cleaned.split("/"):
        seg = raw_seg.strip()
        if not parts:
            seg = _strip_drive_prefix(seg).strip()
        if seg and seg not in (".", ".."):
            parts.append(seg)
    return "/".join(parts)


def sanitize_zip_arcname(raw_name: str, fallback: str = "file.bin") -> str:
    """Normalize and sanitize a ZIP archive entry path while preserving UTF-8."""
    sanitized = _clean_arcname_segments(raw_name or "")
    if sanitized:
        return sanitized
    sanitized_fallback = _clean_arcname_segments(fallback or "")
    if sanitized_fallback:
        return sanitized_fallback
    return "file.bin"


def deduplicate_arcname(
    raw_name: str,
    used_names: set[str],
    *,
    index: int,
    media_id: str,
) -> str:
    """Sanitize a filename (basename) and deduplicate it against `used_names`."""
    cleaned_raw = _strip_drive_prefix(_strip_control_chars(raw_name or "").replace("\\", "/").lstrip())
    base_candidate = Path(cleaned_raw).name if cleaned_raw else ""
    base_name = sanitize_zip_arcname(base_candidate, fallback="file.bin")

    if base_name not in used_names:
        used_names.add(base_name)
        return base_name

    p = Path(base_name)
    safe_media_id = _strip_control_chars(media_id or "").replace("/", "").replace("\\", "")[:6]
    stem_base = f"{p.stem}_{index + 1}_{safe_media_id}"
    candidate = f"{stem_base}{p.suffix}"
    counter = 2
    while candidate in used_names:
        candidate = f"{stem_base}_{counter}{p.suffix}"
        counter += 1

    used_names.add(candidate)
    return candidate


@dataclass
class StreamingZipEntry:
    arcname: str
    stream_factory: Callable[[], AsyncIterator[bytes] | Awaitable[AsyncIterator[bytes]]]
    expected_size: int | None = None
    mtime: datetime | None = None


def _pack_dos_datetime(mtime: datetime | None) -> tuple[int, int]:
    if mtime is None:
        dt = datetime.now(timezone.utc)
    elif mtime.tzinfo is not None:
        dt = mtime.astimezone(timezone.utc)
    else:
        dt = mtime

    year = max(1980, min(2107, dt.year))
    dos_time = (dt.hour << 11) | (dt.minute << 5) | (dt.second // 2)
    dos_date = ((year - 1980) << 9) | (dt.month << 5) | dt.day
    return dos_time, dos_date


async def _close_upstream_iterator(stream_iter: object) -> None:
    aclose = getattr(stream_iter, "aclose", None)
    if callable(aclose):
        result = aclose()
        if isinstance(result, Awaitable):
            await result
        return
    close = getattr(stream_iter, "close", None)
    if callable(close):
        close()


async def iter_streaming_zip64(
    entries: Sequence[StreamingZipEntry],
    *,
    force_zip64: bool = False,
    zip64_limit: int = ZIP64_LIMIT,
    zip64_count_limit: int = ZIP64_ENTRY_COUNT_LIMIT,
) -> AsyncIterator[bytes]:
    """Stream a ZIP64/ZIP32 (STORED + data descriptor) archive with zero full-file buffering."""
    offset = 0
    records: list[tuple[int, bool, int, int, int, int, bytes]] = []

    for entry in entries:
        safe_arcname = sanitize_zip_arcname(entry.arcname)
        name_bytes = safe_arcname.encode("utf-8")
        dos_time, dos_date = _pack_dos_datetime(entry.mtime)

        local_header_offset = offset
        entry_zip64 = bool(
            force_zip64
            or local_header_offset >= zip64_limit
            or (entry.expected_size is not None and entry.expected_size >= zip64_limit)
            or entry.expected_size is None
        )

        lfh_version = _VERSION_ZIP64 if entry_zip64 else _VERSION_ZIP32
        lfh_extra = (
            _LFH_ZIP64_EXTRA_STRUCT.pack(0x0001, 16, 0, 0) if entry_zip64 else b""
        )
        lfh = (
            _LFH_STRUCT.pack(
                b"PK\x03\x04",
                lfh_version,
                _GP_FLAG_DD_UTF8,
                _ZIP_STORED,
                dos_time,
                dos_date,
                0,
                0,
                0,
                len(name_bytes),
                len(lfh_extra),
            )
            + name_bytes
            + lfh_extra
        )
        offset += len(lfh)
        yield lfh

        crc = 0
        size = 0

        raw_stream = entry.stream_factory()
        if not hasattr(raw_stream, "__aiter__") and isinstance(raw_stream, Awaitable):
            stream_iter = await raw_stream
        else:
            stream_iter = raw_stream

        try:
            async for chunk in stream_iter:
                if not chunk:
                    continue
                if isinstance(chunk, memoryview):
                    chunk_bytes = chunk.tobytes()
                elif isinstance(chunk, bytearray):
                    chunk_bytes = bytes(chunk)
                else:
                    chunk_bytes = chunk
                chunk_len = len(chunk_bytes)
                if not entry_zip64 and (size + chunk_len) >= zip64_limit:
                    raise ValueError("Entry exceeded ZIP32 limit without ZIP64 header")
                crc = zlib.crc32(chunk_bytes, crc)
                size += chunk_len
                offset += chunk_len
                yield chunk_bytes
        finally:
            await _close_upstream_iterator(stream_iter)

        crc32_val = crc & 0xFFFFFFFF
        if entry_zip64:
            dd = _DD_ZIP64_STRUCT.pack(b"PK\x07\x08", crc32_val, size, size)
        else:
            dd = _DD_ZIP32_STRUCT.pack(b"PK\x07\x08", crc32_val, size, size)
        offset += len(dd)
        yield dd

        records.append(
            (
                local_header_offset,
                entry_zip64,
                dos_time,
                dos_date,
                crc32_val,
                size,
                name_bytes,
            )
        )

    cd_offset = offset
    for (
        local_header_offset,
        entry_zip64,
        dos_time,
        dos_date,
        crc32_val,
        size,
        name_bytes,
    ) in records:
        extra_parts: list[bytes] = []
        if size >= zip64_limit or force_zip64:
            cd_uncomp = 0xFFFFFFFF
            cd_comp = 0xFFFFFFFF
            extra_parts.append(struct.pack("<QQ", size, size))
        else:
            cd_uncomp = size
            cd_comp = size

        if local_header_offset >= zip64_limit or force_zip64:
            cd_lho = 0xFFFFFFFF
            extra_parts.append(struct.pack("<Q", local_header_offset))
        else:
            cd_lho = local_header_offset

        if extra_parts:
            payload = b"".join(extra_parts)
            cd_extra = _ZIP64_EXTRA_HDR_STRUCT.pack(0x0001, len(payload)) + payload
        else:
            cd_extra = b""

        cd_version_needed = _VERSION_ZIP64 if (entry_zip64 or bool(cd_extra)) else _VERSION_ZIP32
        version_made_by = (3 << 8) | cd_version_needed

        cdfh = (
            _CDFH_STRUCT.pack(
                b"PK\x01\x02",
                version_made_by,
                cd_version_needed,
                _GP_FLAG_DD_UTF8,
                _ZIP_STORED,
                dos_time,
                dos_date,
                crc32_val,
                cd_comp,
                cd_uncomp,
                len(name_bytes),
                len(cd_extra),
                0,
                0,
                0,
                _UNIX_REGULAR_FILE_ATTR,
                cd_lho,
            )
            + name_bytes
            + cd_extra
        )
        offset += len(cdfh)
        yield cdfh

    cd_size = offset - cd_offset
    count = len(records)

    need_zip64_eocd = bool(
        force_zip64
        or count >= zip64_count_limit
        or cd_size >= zip64_limit
        or cd_offset >= zip64_limit
    )

    if need_zip64_eocd:
        zip64_eocd_offset = offset
        zip64_eocd = _ZIP64_EOCD_STRUCT.pack(
            b"PK\x06\x06",
            44,
            _VERSION_ZIP64,
            _VERSION_ZIP64,
            0,
            0,
            count,
            count,
            cd_size,
            cd_offset,
        )
        offset += len(zip64_eocd)
        yield zip64_eocd

        zip64_locator = _ZIP64_LOCATOR_STRUCT.pack(
            b"PK\x06\x07",
            0,
            zip64_eocd_offset,
            1,
        )
        offset += len(zip64_locator)
        yield zip64_locator

    eocd_count = (
        0xFFFF
        if (count >= zip64_count_limit or (need_zip64_eocd and count >= 0xFFFF))
        else count
    )
    eocd_cd_size = 0xFFFFFFFF if (cd_size >= zip64_limit or force_zip64) else cd_size
    eocd_cd_offset = 0xFFFFFFFF if (cd_offset >= zip64_limit or force_zip64) else cd_offset

    eocd = _EOCD_STRUCT.pack(
        b"PK\x05\x06",
        0,
        0,
        eocd_count,
        eocd_count,
        eocd_cd_size,
        eocd_cd_offset,
        0,
    )
    offset += len(eocd)
    yield eocd
