from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
import shutil
import tempfile
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse

from app.allocator import fmt_size, sanitize_folder
from app.config import Settings
from app.db import Database
from app.ingest import IngestService, ProgressHook, _run_subprocess_streaming, classify_media
import app.ingest as ingest_mod

logger = logging.getLogger(__name__)

MAX_TORRENT_FILE_BYTES = 10 * 1024 * 1024
MAX_BENCODE_DEPTH = 32
MAX_BENCODE_ITEMS = 100_000


class TorrentError(RuntimeError):
    pass


class BencodeError(ValueError):
    pass


class TorrentQuotaError(TorrentError):
    pass


class TorrentUnavailableError(TorrentError):
    pass


# ---------- Bencode ----------

def _is_ascii_digit(b: int) -> bool:
    return 48 <= b <= 57


def _bdecode_value(
    data: bytes,
    i: int,
    depth: int,
    max_depth: int,
    max_items: int,
    counter: list[int],
) -> tuple[Any, int, tuple[int, int] | None]:
    # returns (obj, new_i, info_span_or_None)
    if i >= len(data):
        raise BencodeError("Unexpected end of data")
    if depth > max_depth:
        raise BencodeError("Maximum bencode depth exceeded")
    b = data[i]
    info_span: tuple[int, int] | None = None

    if b == 105:  # i
        # integer (bounded digit length to prevent CPU DoS on huge literals)
        j = data.find(b"e", i, min(len(data), i + 25))
        if j == -1:
            raise BencodeError("Unterminated or oversized integer")
        num_bytes = data[i + 1 : j]
        if not num_bytes or len(num_bytes) > 21:
            raise BencodeError("Empty or oversized integer")
        s = num_bytes
        if s.startswith(b"-"):
            digits = s[1:]
            if not digits or not all(_is_ascii_digit(x) for x in digits):
                raise BencodeError("Invalid integer")
            if len(digits) > 1 and digits[0:1] == b"0":
                raise BencodeError("Leading zeros in integer")
            if digits == b"0":
                raise BencodeError("Negative zero")
        else:
            if not all(_is_ascii_digit(x) for x in s):
                raise BencodeError("Invalid integer")
            if len(s) > 1 and s[0:1] == b"0":
                raise BencodeError("Leading zeros in integer")
        try:
            val = int(num_bytes)
        except ValueError:
            raise BencodeError("Invalid integer")
        counter[0] += 1
        if counter[0] > max_items:
            raise BencodeError("Too many bencode items")
        return val, j + 1, None

    if b == 108:  # l
        items_list: list[Any] = []
        i += 1
        while i < len(data) and data[i] != 101:  # e
            if counter[0] >= max_items:
                raise BencodeError("Too many bencode items")
            val, i, span = _bdecode_value(data, i, depth + 1, max_depth, max_items, counter)
            items_list.append(val)
            if span is not None and info_span is None:
                info_span = span
        if i >= len(data):
            raise BencodeError("Unterminated list")
        i += 1
        counter[0] += 1
        if counter[0] > max_items:
            raise BencodeError("Too many bencode items")
        return items_list, i, info_span

    if b == 100:  # d
        d: dict[Any, Any] = {}
        i += 1
        info_value_span: tuple[int, int] | None = None
        while i < len(data) and data[i] != 101:
            if counter[0] >= max_items:
                raise BencodeError("Too many bencode items")
            key, i, _ = _bdecode_value(data, i, depth + 1, max_depth, max_items, counter)
            if not isinstance(key, (bytes, bytearray)):
                raise BencodeError("Dict keys must be bytes")
            key = bytes(key)
            if key in d:
                raise BencodeError("Duplicate dict key in bencode")
            value_start = i
            val, i, _ = _bdecode_value(data, i, depth + 1, max_depth, max_items, counter)
            value_end = i
            if depth == 0 and key == b"info":
                info_value_span = (value_start, value_end)
            d[key] = val
        if i >= len(data):
            raise BencodeError("Unterminated dict")
        i += 1  # e
        counter[0] += 1
        if counter[0] > max_items:
            raise BencodeError("Too many bencode items")
        return d, i, info_value_span

    if _is_ascii_digit(b):
        colon = data.find(b":", i, min(len(data), i + 12))
        if colon == -1:
            raise BencodeError("Missing or oversized byte string length")
        length_bytes = data[i:colon]
        if not length_bytes or len(length_bytes) > 10:
            raise BencodeError("Empty or oversized string length")
        if length_bytes.startswith(b"0") and len(length_bytes) > 1:
            raise BencodeError("Leading zeros in string length")
        if any(not _is_ascii_digit(x) for x in length_bytes):
            raise BencodeError("Non-ASCII digits in string length")
        try:
            n = int(length_bytes)
        except ValueError:
            raise BencodeError("Invalid string length")
        if n < 0:
            raise BencodeError("Negative string length")
        start = colon + 1
        end = start + n
        if end > len(data):
            raise BencodeError("Truncated byte string")
        raw = data[start:end]
        counter[0] += 1
        if counter[0] > max_items:
            raise BencodeError("Too many bencode items")
        return raw, end, None

    raise BencodeError(f"Invalid bencode token: {b!r}")


def _bdecode_full(data: bytes, max_depth, max_items):
    counter = [0]
    obj, end, span = _bdecode_value(data, 0, 0, max_depth, max_items, counter)
    if end != len(data):
        raise BencodeError("Trailing bytes after top-level object")
    if not isinstance(obj, dict):
        # top-level must be dict for torrents but bdecode itself can be anything? Contract says strict; trailing check. We'll allow.
        pass
    return obj, span


def bdecode(data, *, max_depth=MAX_BENCODE_DEPTH, max_items=MAX_BENCODE_ITEMS):
    if not isinstance(data, (bytes, bytearray)):
        raise BencodeError("data must be bytes")
    data = bytes(data)
    if not data:
        raise BencodeError("empty data")
    obj, span = _bdecode_full(data, max_depth, max_items)
    return obj


def bencode(value: Any) -> bytes:
    """Encode a Python value (int, bytes/str, list/tuple, dict) into canonical bencode bytes."""
    if isinstance(value, bool):
        raise BencodeError("booleans are not valid bencode values")
    if isinstance(value, int):
        return f"i{value}e".encode("ascii")
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
        return f"{len(raw)}:".encode("ascii") + raw
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return f"{len(raw)}:".encode("ascii") + raw
    if isinstance(value, (list, tuple)):
        return b"l" + b"".join(bencode(v) for v in value) + b"e"
    if isinstance(value, dict):
        items: list[tuple[bytes, Any]] = []
        for k, v in value.items():
            kb = k.encode("utf-8") if isinstance(k, str) else bytes(k) if isinstance(k, (bytes, bytearray)) else None
            if kb is None:
                raise BencodeError("dict keys must be bytes or str")
            items.append((kb, v))
        items.sort(key=lambda pair: pair[0])
        return b"d" + b"".join(bencode(k) + bencode(v) for k, v in items) + b"e"
    raise BencodeError(f"Unsupported type for bencode: {type(value).__name__}")



def _validate_path_segment(seg: str) -> None:
    if seg == "" or seg == "." or seg == "..":
        raise BencodeError(f"Unsafe path in torrent metadata: {seg}")
    if "\x00" in seg:
        raise BencodeError(f"Unsafe path in torrent metadata: {seg}")
    for c in seg:
        if ord(c) < 32 or ord(c) == 127:
            raise BencodeError(f"Unsafe path in torrent metadata: {seg}")
    if "/" in seg or "\\" in seg:
        raise BencodeError(f"Unsafe path in torrent metadata: {seg}")
    if len(seg) >= 2 and seg[1] == ":":
        raise BencodeError(f"Unsafe path in torrent metadata: {seg}")


def is_default_selected(rel_path: str, size_bytes: int, media_type: str) -> bool:
    lower_suffix = Path(rel_path).suffix.lower()
    deselect_exts = {".nfo", ".txt", ".url", ".lnk", ".exe", ".bat", ".cmd", ".scr", ".com", ".msi", ".sfv", ".md5", ".sha1", ".diz", ".torrent"}
    if lower_suffix in deselect_exts:
        return False
    basename = Path(rel_path).name.lower()
    if basename in {"thumbs.db", "desktop.ini", ".ds_store"}:
        return False
    if basename.startswith("readme") or basename.startswith("tracked_by_") or basename.startswith("downloaded from"):
        return False
    # sample detection
    parts = rel_path.replace("\\", "/").split("/")
    parent_segments = [p.lower() for p in parts[:-1]]
    if "sample" in parent_segments or "samples" in parent_segments:
        return False
    stem = Path(rel_path).stem
    if media_type == "video" and re.search(r"(^|[\s._\-\[\(])sample([\s._\-\]\)]|$)", stem.lower()) and size_bytes < 150 * 1024 * 1024:
        return False
    return True


def parse_torrent_bytes(data: bytes) -> dict[str, Any]:
    if not isinstance(data, (bytes, bytearray)):
        raise BencodeError("data must be bytes")
    data = bytes(data)
    if not data:
        raise BencodeError("empty torrent data")
    if len(data) > MAX_TORRENT_FILE_BYTES:
        raise BencodeError("torrent file too large")
    obj, info_span = _bdecode_full(data, MAX_BENCODE_DEPTH, MAX_BENCODE_ITEMS)
    if not isinstance(obj, dict):
        raise BencodeError("top-level must be dict")
    info = obj.get(b"info")
    if not isinstance(info, dict):
        raise BencodeError("missing info dict")
    if info_span is None:
        raise BencodeError("missing info dict")

    # infohash
    if info.get(b"meta version") == 2 and b"pieces" not in info:
        infohash = hashlib.sha256(data[info_span[0]:info_span[1]]).hexdigest()[:40]
    else:
        infohash = hashlib.sha1(data[info_span[0]:info_span[1]]).hexdigest()

    # name
    name_raw = info.get(b"name.utf-8", info.get(b"name"))
    if isinstance(name_raw, (bytes, bytearray)):
        name = bytes(name_raw).decode("utf-8", errors="replace")
    else:
        name = str(name_raw) if name_raw is not None else ""
    _validate_path_segment(name)

    # files
    files_list: list[dict[str, Any]] = []
    is_multi = False
    total_bytes = 0
    selected_default_bytes = 0

    if b"files" in info:
        is_multi = True
        raw_files = info[b"files"]
        if not isinstance(raw_files, list):
            raise BencodeError("files must be list")
        idx = 1
        for f in raw_files:
            if not isinstance(f, dict):
                raise BencodeError("file entry must be dict")
            # length
            length = f.get(b"length")
            if length is None:
                raise BencodeError("missing length")
            size_bytes = int(length)
            if size_bytes < 0:
                raise BencodeError("negative length")
            # path
            path_key = b"path.utf-8" if b"path.utf-8" in f else b"path"
            segments_raw = f.get(path_key, [])
            if not isinstance(segments_raw, list):
                raise BencodeError("path must be list")
            segments: list[str] = []
            for seg in segments_raw:
                if isinstance(seg, (bytes, bytearray)):
                    s = bytes(seg).decode("utf-8", errors="replace")
                else:
                    s = str(seg)
                _validate_path_segment(s)
                segments.append(s)
            if not segments:
                # maybe use name?
                segments = [name]
            leaf = segments[-1]
            rel_path = f"{name}/" + "/".join(segments)
            # BEP-47 padding
            attr = f.get(b"attr")
            attr_bytes = attr if isinstance(attr, (bytes, bytearray)) else b""
            is_pad = False
            if b"p" in (attr_bytes or b"") if isinstance(attr_bytes, (bytes, bytearray)) else False:
                is_pad = True
            if ".pad" in [s.lower() for s in segments]:
                is_pad = True
            # media type
            media_type, mime_type = classify_media(Path(leaf))
            selected_default = (not is_pad) and is_default_selected(rel_path, size_bytes, media_type)
            # pad files default false
            if is_pad:
                selected_default = False
            files_list.append({
                "index": idx,
                "path": rel_path,
                "name": leaf,
                "size_bytes": size_bytes,
                "media_type": media_type,
                "mime_type": mime_type,
                "selected_default": bool(selected_default),
            })
            total_bytes += size_bytes
            if selected_default:
                selected_default_bytes += size_bytes
            idx += 1
    elif b"length" in info:
        length = info[b"length"]
        size_bytes = int(length)
        if size_bytes < 0:
            raise BencodeError("negative length")
        rel_path = name
        media_type, mime_type = classify_media(Path(name))
        selected_default = is_default_selected(rel_path, size_bytes, media_type)
        files_list.append({
            "index": 1,
            "path": rel_path,
            "name": name,
            "size_bytes": size_bytes,
            "media_type": media_type,
            "mime_type": mime_type,
            "selected_default": bool(selected_default),
        })
        total_bytes += size_bytes
        if selected_default:
            selected_default_bytes += size_bytes
    else:
        raise BencodeError("torrent has neither length nor files")

    return {
        "infohash": infohash,
        "name": name,
        "total_bytes": total_bytes,
        "selected_default_bytes": selected_default_bytes,
        "file_count": len(files_list),
        "is_multi_file": is_multi,
        "files": files_list,
    }


# ---------- Magnet helpers ----------

def validate_magnet_uri(magnet: str) -> str:
    if not isinstance(magnet, str):
        raise ValueError("magnet must be str")
    s = magnet.strip()
    if not s:
        raise ValueError("empty magnet")
    if len(s) > 8192:
        raise ValueError("magnet too long")
    if any(c.isspace() for c in s):
        raise ValueError("whitespace in magnet")
    if any(ord(c) < 32 or ord(c) == 127 for c in s):
        raise ValueError("control char in magnet")
    low = s.lower()
    if not low.startswith("magnet:?"):
        raise ValueError("not a magnet uri")
    # normalize prefix
    # parse query
    # ensure magnet:? prefix exactly
    # reconstruct normalized: keep original casing? requirement: normalized to magnet:?
    # We'll normalize prefix to "magnet:?"
    rest = s[len("magnet:?"):] if low.startswith("magnet:?") else s
    # Actually s starts magnet:? (case-insensitive). Normalize:
    idx = low.find("magnet:?")
    prefix = s[:idx]
    # should be empty
    s = "magnet:?" + s[idx + len("magnet:?"):]
    low = s.lower()
    # parse params
    parsed = urlparse(s)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    # xt
    xt_vals = qs.get("xt", [])
    ok = False
    for v in xt_vals:
        vl = v.lower()
        if vl.startswith("urn:btih:"):
            payload = v[len("urn:btih:"):]
            if len(payload) == 40 and all(c in "0123456789abcdefABCDEF" for c in payload):
                ok = True
                break
            if len(payload) == 32:
                # base32
                try:
                    base64.b32decode(payload.upper() + "=" * ((8 - len(payload) % 8) % 8))
                    ok = True
                    break
                except Exception:
                    pass
        if vl.startswith("urn:btmh:"):
            payload = v[len("urn:btmh:"):]
            if payload and all(c in "0123456789abcdefABCDEF" for c in payload):
                ok = True
                break
    if not ok:
        raise ValueError("invalid xt in magnet")
    return s


def extract_magnet_infohash(magnet: str) -> str | None:
    try:
        clean = validate_magnet_uri(magnet)
    except ValueError:
        return None
    parsed = urlparse(clean)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    for v in qs.get("xt", []):
        vl = v.lower()
        if vl.startswith("urn:btih:"):
            payload = v[len("urn:btih:"):]
            if len(payload) == 40 and all(c in "0123456789abcdefABCDEF" for c in payload):
                return payload.lower()
            if len(payload) == 32:
                try:
                    decoded = base64.b32decode(payload.upper() + "=" * ((8 - len(payload) % 8) % 8))
                    return decoded.hex()
                except Exception:
                    return None
    return None


def is_magnet_uri(value: str) -> bool:
    if not isinstance(value, str):
        return False
    return value.strip().lower().startswith("magnet:?")


def validate_download_limit(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if s == "":
        return s
    if re.match(r"^[0-9]+[KkMmGg]?$", s):
        return s
    raise ValueError("invalid download limit")


# ---------- Binary / disk ----------

_aria2c_version_cache: str | None = None
_aria2c_version_checked: bool = False


def find_aria2c() -> str | None:
    return shutil.which("aria2c")


def get_aria2c_version() -> str | None:
    global _aria2c_version_cache, _aria2c_version_checked
    if _aria2c_version_checked:
        return _aria2c_version_cache
    _aria2c_version_checked = True
    aria = find_aria2c()
    if not aria:
        _aria2c_version_cache = None
        return None
    try:
        import subprocess
        res = subprocess.run([aria, "--version"], capture_output=True, text=True, timeout=3)
        line = (res.stdout or "").splitlines()[0] if res.stdout else ""
        # aria2 version 1.37.0
        m = re.search(r"aria2 version (\S+)", line)
        if m:
            _aria2c_version_cache = m.group(1)
        else:
            # maybe first line
            _aria2c_version_cache = line.strip() or None
    except Exception:
        _aria2c_version_cache = None
    return _aria2c_version_cache


def check_staging_disk_space(staging_dir: Path, required_bytes: int, max_total_bytes: int, min_free_buffer_bytes: int) -> dict[str, Any]:
    p = Path(staging_dir)
    if not p.exists():
        try:
            p.mkdir(parents=True, exist_ok=True)
        except Exception:
            # check parent
            p = p.parent
    try:
        usage = shutil.disk_usage(p)
    except Exception:
        # try parent
        usage = shutil.disk_usage(p.parent)
    free = usage.free
    exceeds = max_total_bytes > 0 and required_bytes > max_total_bytes
    insufficient = (free - max(0, required_bytes)) < max(0, min_free_buffer_bytes)
    return {
        "disk_free_bytes": free,
        "max_total_bytes": max_total_bytes,
        "min_free_buffer_bytes": min_free_buffer_bytes,
        "exceeds_quota": exceeds,
        "insufficient_disk": insufficient,
    }


def assert_staging_disk_space(staging_dir: Path, required_bytes: int, max_total_bytes: int, min_free_buffer_bytes: int) -> dict[str, Any]:
    info = check_staging_disk_space(staging_dir, required_bytes, max_total_bytes, min_free_buffer_bytes)
    if info["exceeds_quota"]:
        raise TorrentQuotaError(f"Torrent size {required_bytes} exceeds max total {max_total_bytes} bytes")
    if info["insufficient_disk"]:
        raise TorrentQuotaError("Insufficient disk space for torrent download")
    return info


# ---------- Progress parsing ----------

_ARIA_PROGRESS_RE = re.compile(
    r"#\S+\s+(\S+?)/(\S+?)\((\d+(?:\.\d+)?)%\)(?:.*?CN:(\d+))?(?:.*?SD:(\d+))?(?:.*?DL:(\S+))?(?:.*?ETA:(\S+))?"
)


def parse_aria2c_progress_line(line: str) -> dict[str, Any] | None:
    if not isinstance(line, str):
        return None
    m = re.search(r"#\S+\s+(\S+?)/(\S+?)\((\d+(?:\.\d+)?)%\)", line)
    if not m:
        return None
    downloaded = m.group(1)
    total = m.group(2)
    pct = float(m.group(3))
    # connections
    cn_m = re.search(r"CN:(\d+)", line)
    connections = int(cn_m.group(1)) if cn_m else None
    sd_m = re.search(r"SD:(\d+)", line)
    seeders = int(sd_m.group(1)) if sd_m else None
    dl_m = re.search(r"DL:(\S+)", line)
    speed = dl_m.group(1) if dl_m else None
    eta_m = re.search(r"ETA:(\S+)", line)
    eta = eta_m.group(1) if eta_m else None
    return {
        "pct": pct,
        "downloaded": downloaded,
        "total": total,
        "connections": connections,
        "seeders": seeders,
        "speed": speed,
        "eta": eta,
    }


def format_progress_message(parsed: dict[str, Any]) -> str:
    parts = [f"[leech] {parsed['pct']:.0f}%"]
    if parsed.get("speed"):
        parts.append(f"• {parsed['speed']}")
    peers = parsed.get("connections")
    seeds = parsed.get("seeders")
    if peers is not None or seeds is not None:
        peer_str = f"{peers if peers is not None else 0} peers"
        if seeds is not None:
            peer_str += f" ({seeds} seeds)"
        parts.append(f"• {peer_str}")
    if parsed.get("eta"):
        parts.append(f"• ETA {parsed['eta']}")
    return " ".join(parts)


# ---------- Service ----------

class TorrentIngestService:
    def __init__(self, settings: Settings, db: Database, ingest: IngestService) -> None:
        self.settings = settings
        self.db = db
        self.ingest = ingest
        conc = max(1, int(getattr(settings, "torrent_concurrency", 2)))
        self._semaphore = asyncio.Semaphore(conc)
        self._configured_concurrency = conc
        self._cache: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
        self._cache_lock = threading.Lock()
        self._cache_max = 32
        self._cache_ttl = 1800.0
        self._cache_max_bytes = 64 * 1024 * 1024

    def _settings_get(self, name, default=None):
        return getattr(self.settings, name, default)

    def get_status(self) -> dict[str, Any]:
        s = self.settings
        staging = getattr(s, "staging_dir", Path("./data/staging"))
        try:
            free = shutil.disk_usage(staging if Path(staging).exists() else Path(staging).parent).free
        except Exception:
            free = 0
        return {
            "enabled": bool(getattr(s, "torrent_enabled", True)),
            "aria2c_installed": find_aria2c() is not None,
            "aria2c_version": get_aria2c_version(),
            "concurrency": max(1, int(getattr(s, "torrent_concurrency", 2))),
            "max_total_mb": int(getattr(s, "torrent_max_total_mb", 0)),
            "min_free_buffer_mb": int(getattr(s, "torrent_min_free_buffer_mb", 0)),
            "disk_free_bytes": free,
            "enable_dht": bool(getattr(s, "torrent_enable_dht", True)),
            "bt_stop_timeout_sec": int(getattr(s, "torrent_bt_stop_timeout_sec", 300)),
        }

    def cache_inspect(self, torrent_bytes: bytes, parsed: dict[str, Any], *, magnet: str | None = None) -> str:
        inspect_id = uuid.uuid4().hex[:16]
        entry = {
            "torrent_bytes": torrent_bytes,
            "parsed": parsed,
            "magnet": magnet,
            "created_at": time.time(),
        }
        with self._cache_lock:
            self._purge_cache_locked()
            # evict if needed
            self._cache[inspect_id] = entry
            self._enforce_cache_limits_locked()
        return inspect_id

    def _purge_cache_locked(self) -> None:
        now = time.time()
        expired = [k for k, v in self._cache.items() if now - v["created_at"] > self._cache_ttl]
        for k in expired:
            del self._cache[k]

    def _cache_total_bytes_locked(self) -> int:
        return sum(len(v.get("torrent_bytes", b"")) for v in self._cache.values())

    def _enforce_cache_limits_locked(self) -> None:
        while len(self._cache) > self._cache_max or self._cache_total_bytes_locked() > self._cache_max_bytes:
            if not self._cache:
                break
            self._cache.popitem(last=False)

    def get_cached_inspect(self, inspect_id: str) -> dict[str, Any] | None:
        with self._cache_lock:
            self._purge_cache_locked()
            entry = self._cache.get(inspect_id)
            if entry is None:
                return None
            # move to end (LRU)
            self._cache.move_to_end(inspect_id)
            return entry

    @staticmethod
    def _clean_http_proxy(raw_proxy: str | None) -> str:
        p = (raw_proxy or "").strip()
        if not p or any(ch in p for ch in ("\r", "\n", "\x00", " ", "\t")):
            return ""
        if p.startswith(("http://", "https://")):
            return p
        return ""

    @staticmethod
    def _natural_sort_key(text: str) -> list[Any]:
        return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", text)]

    @staticmethod
    def _is_safe_staged_file(path: Path, root_resolved: Path) -> bool:
        try:
            if path.is_symlink() or not path.is_file():
                return False
            # Reject any symlinked ancestor between root_resolved and path
            cur = path.parent
            while cur != cur.parent:
                if cur.is_symlink():
                    return False
                try:
                    if cur.resolve() == root_resolved:
                        break
                except Exception:
                    return False
                cur = cur.parent
            rp = path.resolve()
            return rp.is_relative_to(root_resolved)
        except Exception:
            return False

    @staticmethod
    def _staging_dir_size_bytes(root: Path, root_resolved: Path) -> int:
        total = 0
        source_meta = root_resolved / "_source.torrent"
        try:
            for p in root.rglob("*"):
                if p.is_symlink() or not p.is_file():
                    continue
                if p.suffix.lower() == ".aria2":
                    continue
                try:
                    rp = p.resolve()
                    if rp == source_meta:
                        continue
                    if rp.is_relative_to(root_resolved):
                        total += p.stat().st_size
                except OSError:
                    continue
        except OSError:
            pass
        return total

    @staticmethod
    def _terminate_proc(proc: asyncio.subprocess.Process | None) -> None:
        if proc is None or getattr(proc, "returncode", None) is not None:
            return
        try:
            proc.terminate()
        except (ProcessLookupError, AttributeError, OSError):
            pass

    def inspect_torrent_bytes(self, torrent_bytes: bytes, *, magnet: str | None = None) -> dict[str, Any]:
        parsed = parse_torrent_bytes(torrent_bytes)
        s = self.settings
        staging_dir = Path(getattr(s, "staging_dir", "./data/staging"))
        max_total = getattr(s, "torrent_max_total_bytes", None)
        if max_total is None:
            max_total = int(getattr(s, "torrent_max_total_mb", 0)) * 1024 * 1024
        min_buf = getattr(s, "torrent_min_free_buffer_bytes", None)
        if min_buf is None:
            min_buf = int(getattr(s, "torrent_min_free_buffer_mb", 0)) * 1024 * 1024
        check_bytes = parsed["selected_default_bytes"] if parsed["selected_default_bytes"] > 0 else parsed["total_bytes"]
        disk_info = check_staging_disk_space(staging_dir, check_bytes, max_total, min_buf)
        inspect_id = self.cache_inspect(torrent_bytes, parsed, magnet=magnet)
        suggested_folder = sanitize_folder(parsed["name"], getattr(s, "default_folder", "General"))
        return {
            "inspect_id": inspect_id,
            "infohash": parsed["infohash"],
            "name": parsed["name"],
            "total_bytes": parsed["total_bytes"],
            "selected_default_bytes": parsed["selected_default_bytes"],
            "file_count": parsed["file_count"],
            "is_multi_file": parsed["is_multi_file"],
            "files": parsed["files"],
            "suggested_folder": suggested_folder,
            **disk_info,
        }

    async def inspect_magnet(self, magnet: str) -> dict[str, Any]:
        s = self.settings
        if not getattr(s, "torrent_enabled", True):
            raise TorrentUnavailableError("Torrent ingestion is disabled")
        aria = find_aria2c()
        if aria is None:
            raise TorrentUnavailableError("aria2c is not installed")
        clean_magnet = validate_magnet_uri(magnet)
        staging_dir = Path(getattr(s, "staging_dir", "./data/staging"))
        staging_dir.mkdir(parents=True, exist_ok=True)
        meta_dir = Path(tempfile.mkdtemp(prefix="torrent_meta_", dir=staging_dir))
        self.ingest.register_staging_dir(meta_dir)
        inspect_key = f"inspect_{uuid.uuid4().hex[:8]}"
        proc_ref: dict[str, asyncio.subprocess.Process] = {}

        def _on_inspect_proc(proc: asyncio.subprocess.Process) -> None:
            proc_ref["proc"] = proc
            self.ingest.register_proc(inspect_key, proc)

        try:
            dht = bool(getattr(s, "torrent_enable_dht", True))
            bt_stop = int(getattr(s, "torrent_bt_stop_timeout_sec", 300))
            meta_timeout = max(5, int(getattr(s, "torrent_metadata_timeout_sec", 45)))
            proxy = self._clean_http_proxy(getattr(s, "torrent_proxy", ""))
            cmd = [
                aria,
                "--bt-metadata-only=true",
                "--bt-save-metadata=true",
                "--seed-time=0",
                "--seed-ratio=0.0",
                "--bt-seed-unverified=false",
                "--follow-metalink=false",
                f"--enable-dht={'true' if dht else 'false'}",
                f"--bt-stop-timeout={min(bt_stop, meta_timeout)}",
                "--dir",
                str(meta_dir),
                "--console-log-level=notice",
            ]
            if proxy:
                cmd += ["--all-proxy", proxy]
            cmd += ["--", clean_magnet]

            streaming_kwargs: dict[str, Any] = {"on_line": None}
            if ingest_mod._accepts_kwarg(ingest_mod._run_subprocess_streaming, "on_proc"):
                streaming_kwargs["on_proc"] = _on_inspect_proc

            try:
                rc, tail = await asyncio.wait_for(
                    ingest_mod._run_subprocess_streaming(*cmd, **streaming_kwargs),
                    timeout=meta_timeout,
                )
            except asyncio.TimeoutError as exc:
                self._terminate_proc(proc_ref.get("proc"))
                raise TorrentError(
                    f"Timed out after {meta_timeout}s waiting for magnet metadata (no reachable peers/DHT)"
                ) from exc

            def _read_metadata() -> bytes | None:
                meta_resolved = meta_dir.resolve()
                for p in meta_dir.rglob("*.torrent"):
                    if not self._is_safe_staged_file(p, meta_resolved):
                        continue
                    try:
                        if p.stat().st_size <= MAX_TORRENT_FILE_BYTES:
                            return p.read_bytes()
                    except OSError:
                        continue
                return None

            raw_bytes = await asyncio.to_thread(_read_metadata)
            if raw_bytes is None:
                detail = " | ".join(tail[-5:])[-300:] if tail else f"exit code {rc}"
                raise TorrentError(f"aria2c did not produce metadata: {detail}")
            return await asyncio.to_thread(self.inspect_torrent_bytes, raw_bytes, magnet=clean_magnet)
        finally:
            proc = proc_ref.get("proc")
            if proc is not None:
                try:
                    self.ingest.unregister_proc(inspect_key, proc)
                except Exception:
                    pass
            shutil.rmtree(meta_dir, ignore_errors=True)
            self.ingest.unregister_staging_dir(meta_dir)

    def build_aria2c_leech_cmd(
        self,
        *,
        job_dir: Path,
        source_arg: str,
        selected_indices: list[int] | None = None,
        enable_dht: bool = True,
        bt_stop_timeout: int = 300,
        download_limit: str = "",
        proxy: str = "",
        max_total_bytes: int = 0,
    ) -> list[str]:
        cmd = [find_aria2c() or "aria2c"]
        cmd += [
            "--seed-time=0",
            "--seed-ratio=0.0",
            "--bt-seed-unverified=false",
            "--file-allocation=none",
            "--follow-metalink=false",
            "--bt-remove-unselected-file=true",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--summary-interval=1",
            "--console-log-level=notice",
            f"--enable-dht={'true' if enable_dht else 'false'}",
            f"--bt-stop-timeout={max(10, int(bt_stop_timeout))}",
            "--dir",
            str(job_dir),
        ]
        clean_limit = validate_download_limit(download_limit) if download_limit else ""
        if clean_limit:
            cmd += [f"--max-overall-download-limit={clean_limit}"]
        clean_proxy = self._clean_http_proxy(proxy)
        if clean_proxy:
            cmd += ["--all-proxy", clean_proxy]
        if selected_indices:
            csv = ",".join(str(i) for i in sorted(set(selected_indices)))
            cmd += [f"--select-file={csv}"]
        cmd += ["--", source_arg]
        return cmd

    async def process_torrent_job(
        self,
        job_id: str,
        *,
        torrent_bytes: bytes | None = None,
        magnet: str | None = None,
        selected_indices: list[int] | None = None,
        source_origin: str = "web",
        folder: str | None = None,
        use_torrent_subfolder: bool = True,
        tags: str | list[str] | None = None,
        forward_to_telegram: bool = True,
        force: bool = False,
        on_progress: ProgressHook | None = None,
    ) -> list[dict[str, Any]]:
        if job_id in self.ingest._cancelled_jobs:
            self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
            raise asyncio.CancelledError()

        s = self.settings
        if not getattr(s, "torrent_enabled", True):
            self.db.update_job(job_id, status="failed", message="Torrent ingestion is disabled")
            raise TorrentUnavailableError("Torrent ingestion is disabled")
        if find_aria2c() is None:
            self.db.update_job(job_id, status="failed", message="aria2c is not installed")
            raise TorrentUnavailableError("aria2c is not installed")

        default_folder = getattr(s, "default_folder", "General")
        staging_dir = Path(getattr(s, "staging_dir", "./data/staging"))
        staging_dir.mkdir(parents=True, exist_ok=True)
        max_total_bytes = getattr(s, "torrent_max_total_bytes", None)
        if max_total_bytes is None:
            max_total_bytes = int(getattr(s, "torrent_max_total_mb", 0)) * 1024 * 1024
        min_free_buffer_bytes = getattr(s, "torrent_min_free_buffer_bytes", None)
        if min_free_buffer_bytes is None:
            min_free_buffer_bytes = int(getattr(s, "torrent_min_free_buffer_mb", 0)) * 1024 * 1024

        parsed = None
        infohash = None
        torrent_name = None
        selected_rel_paths: list[str] = []
        if torrent_bytes is not None:
            parsed = await asyncio.to_thread(parse_torrent_bytes, torrent_bytes)
            infohash = parsed["infohash"]
            torrent_name = parsed["name"]
            valid_indices = {f["index"] for f in parsed["files"]}
            if selected_indices is None:
                defaults = [f["index"] for f in parsed["files"] if f.get("selected_default")]
                if not defaults:
                    defaults = [f["index"] for f in parsed["files"]]
                selected_indices = defaults
            else:
                if not selected_indices:
                    raise TorrentError("selected_indices must be non-empty")
                if any(i not in valid_indices for i in selected_indices):
                    raise TorrentError("selected_indices contains invalid index")
                selected_indices = list(dict.fromkeys(selected_indices))
            selected_files = [f for f in parsed["files"] if f["index"] in set(selected_indices)]
            expected_bytes = sum(f["size_bytes"] for f in selected_files)
            await asyncio.to_thread(
                assert_staging_disk_space,
                staging_dir,
                expected_bytes,
                max_total_bytes,
                min_free_buffer_bytes,
            )
            selected_rel_paths = [f["path"] for f in selected_files]
        elif magnet is not None:
            magnet = validate_magnet_uri(magnet)
            if selected_indices is not None:
                clean_indices = [int(i) for i in selected_indices if isinstance(i, int) and i >= 1]
                selected_indices = list(dict.fromkeys(clean_indices)) if clean_indices else None
            await asyncio.to_thread(
                assert_staging_disk_space,
                staging_dir,
                0,
                max_total_bytes,
                min_free_buffer_bytes,
            )
            infohash = extract_magnet_infohash(magnet)
        else:
            raise TorrentError("Either torrent_bytes or magnet must be provided")

        def _compute_target_folder(resolved_name: str | None) -> str:
            if use_torrent_subfolder and resolved_name:
                clean_base = (folder or "").strip()
                if not clean_base or sanitize_folder(clean_base, default_folder) == default_folder:
                    return sanitize_folder(resolved_name, default_folder)
                return sanitize_folder(f"{clean_base}/{resolved_name}", default_folder)
            return sanitize_folder(folder, default_folder)

        target_folder = _compute_target_folder(torrent_name)

        from app.db import normalize_tags as _nt

        normalized_tags = _nt(tags)

        uses_tg = self.settings.storage_mode == "telegram" or (
            self.settings.storage_mode == "both" and forward_to_telegram
        )
        dl_weight = 70.0 if uses_tg else 95.0

        sem = self._semaphore
        self.ingest._active_slots = getattr(self.ingest, "_active_slots", 0) + 1
        try:
            async with sem:
                if job_id in self.ingest._cancelled_jobs:
                    raise asyncio.CancelledError()
                job_dir = Path(tempfile.mkdtemp(prefix="torrent_", dir=staging_dir))
                job_dir_resolved = job_dir.resolve()
                self.ingest.register_staging_dir(job_dir)
                try:
                    if torrent_bytes is not None:
                        meta_file = job_dir / "_source.torrent"
                        await asyncio.to_thread(meta_file.write_bytes, torrent_bytes)
                        source_arg = str(meta_file)
                    else:
                        source_arg = magnet or ""

                    dht = bool(getattr(s, "torrent_enable_dht", True))
                    bt_stop = int(getattr(s, "torrent_bt_stop_timeout_sec", 300))
                    download_limit_raw = getattr(s, "torrent_max_download_limit", "")
                    try:
                        download_limit = validate_download_limit(download_limit_raw) if download_limit_raw else ""
                    except ValueError:
                        download_limit = ""
                    proxy = self._clean_http_proxy(getattr(s, "torrent_proxy", ""))
                    cmd = self.build_aria2c_leech_cmd(
                        job_dir=job_dir,
                        source_arg=source_arg,
                        selected_indices=selected_indices if selected_indices else None,
                        enable_dht=dht,
                        bt_stop_timeout=bt_stop,
                        download_limit=download_limit,
                        proxy=proxy,
                    )

                    self.db.update_job(
                        job_id,
                        status="downloading",
                        progress=1.0,
                        message="[leech] Starting aria2c...",
                    )
                    if on_progress:
                        await on_progress(1.0, "[leech] Starting aria2c...")

                    proc_ref: dict[str, asyncio.subprocess.Process] = {}
                    quota_exc: list[TorrentQuotaError | None] = [None]
                    last_disk_check = [0.0]

                    async def _check_staging_limits() -> None:
                        try:
                            free = (await asyncio.to_thread(shutil.disk_usage, staging_dir)).free
                        except Exception:
                            free = 0
                        if min_free_buffer_bytes > 0 and free < min_free_buffer_bytes:
                            raise TorrentQuotaError(
                                "Aborted torrent download: staging disk free space dropped below buffer"
                            )
                        if max_total_bytes > 0:
                            current_bytes = await asyncio.to_thread(
                                self._staging_dir_size_bytes, job_dir, job_dir_resolved
                            )
                            if current_bytes > max_total_bytes:
                                raise TorrentQuotaError(
                                    f"Aborted torrent download: staging size ({current_bytes} B) exceeded max quota ({max_total_bytes} B)"
                                )

                    async def _quota_watchdog() -> None:
                        try:
                            while True:
                                await asyncio.sleep(3.0)
                                if job_id in self.ingest._cancelled_jobs:
                                    self._terminate_proc(proc_ref.get("proc"))
                                    return
                                try:
                                    await _check_staging_limits()
                                except TorrentQuotaError as exc:
                                    quota_exc[0] = exc
                                    self._terminate_proc(proc_ref.get("proc"))
                                    return
                        except asyncio.CancelledError:
                            return

                    async def on_line(line: str) -> None:
                        if job_id in self.ingest._cancelled_jobs:
                            self._terminate_proc(proc_ref.get("proc"))
                            raise asyncio.CancelledError()
                        if quota_exc[0] is not None:
                            raise quota_exc[0]
                        parsed_p = parse_aria2c_progress_line(line)
                        if parsed_p is not None:
                            msg = format_progress_message(parsed_p)
                            overall = max(1.0, min(dl_weight, (parsed_p["pct"] / 100.0) * dl_weight))
                            self.db.update_job(job_id, status="downloading", progress=overall, message=msg)
                            if on_progress:
                                await on_progress(overall, msg)
                        now = time.monotonic()
                        if now - last_disk_check[0] >= 5.0:
                            last_disk_check[0] = now
                            try:
                                await _check_staging_limits()
                            except TorrentQuotaError as exc:
                                quota_exc[0] = exc
                                self._terminate_proc(proc_ref.get("proc"))
                                raise

                    def _on_proc(proc: asyncio.subprocess.Process) -> None:
                        proc_ref["proc"] = proc
                        self.ingest.register_proc(job_id, proc)

                    streaming_kwargs: dict[str, Any] = {"on_line": on_line}
                    if ingest_mod._accepts_kwarg(ingest_mod._run_subprocess_streaming, "on_proc"):
                        streaming_kwargs["on_proc"] = _on_proc

                    watchdog_task = asyncio.create_task(_quota_watchdog())
                    try:
                        rc, tail = await ingest_mod._run_subprocess_streaming(
                            *cmd,
                            **streaming_kwargs,
                        )
                    finally:
                        watchdog_task.cancel()
                        try:
                            await watchdog_task
                        except asyncio.CancelledError:
                            pass
                        proc = proc_ref.get("proc")
                        if proc is not None:
                            try:
                                self.ingest.unregister_proc(job_id, proc)
                            except Exception:
                                pass

                    if quota_exc[0] is not None:
                        raise quota_exc[0]

                    if job_id in self.ingest._cancelled_jobs:
                        raise asyncio.CancelledError()

                    if rc != 0:
                        raise TorrentError(f"aria2c failed: {' | '.join(tail[-5:])[-300:]}")

                    def _collect_staged() -> tuple[list[Path], int, str | None]:
                        source_meta = job_dir_resolved / "_source.torrent"
                        selected_set = set(selected_rel_paths)
                        items_found: list[Path] = []
                        total_sz = 0
                        for p in job_dir.rglob("*"):
                            if not self._is_safe_staged_file(p, job_dir_resolved):
                                continue
                            if p.suffix.lower() == ".aria2":
                                continue
                            rp = p.resolve()
                            if rp == source_meta:
                                continue
                            if torrent_bytes is not None and selected_set:
                                rel = rp.relative_to(job_dir_resolved).as_posix()
                                if rel not in selected_set:
                                    continue
                            items_found.append(p)
                            try:
                                total_sz += p.stat().st_size
                            except OSError:
                                pass
                        items_found.sort(
                            key=lambda p: self._natural_sort_key(
                                p.resolve().relative_to(job_dir_resolved).as_posix()
                            )
                        )
                        inferred_name: str | None = None
                        if torrent_bytes is None and use_torrent_subfolder and not torrent_name and items_found:
                            top_parts = {
                                p.resolve().relative_to(job_dir_resolved).parts[0]
                                for p in items_found
                                if p.resolve().relative_to(job_dir_resolved).parts
                            }
                            if len(top_parts) == 1:
                                cand = next(iter(top_parts))
                                if len(items_found) == 1 and cand == items_found[0].name:
                                    cand = items_found[0].stem
                                inferred_name = cand
                        return items_found, total_sz, inferred_name

                    collected, total_downloaded_bytes, inferred_name = await asyncio.to_thread(_collect_staged)

                    if max_total_bytes > 0 and total_downloaded_bytes > max_total_bytes:
                        raise TorrentQuotaError("Downloaded size exceeds quota")
                    if inferred_name:
                        target_folder = _compute_target_folder(inferred_name)

                    if not collected:
                        raise TorrentError("Torrent download produced no files.")

                    items: list[dict[str, Any]] = []
                    total_files = len(collected)
                    archive_span = max(1.0, 99.0 - dl_weight)
                    for idx, file_path in enumerate(collected, start=1):
                        if job_id in self.ingest._cancelled_jobs:
                            raise asyncio.CancelledError()
                        base_pct = dl_weight + ((idx - 1) / total_files) * archive_span
                        step_span = archive_span / total_files
                        archive_msg = f"[leech] Archiving {idx}/{total_files}: {file_path.name}"
                        self.db.update_job(
                            job_id,
                            status="allocating",
                            progress=base_pct,
                            message=archive_msg,
                        )
                        if on_progress:
                            await on_progress(base_pct, archive_msg)

                        async def _file_progress(pct: float, msg: str, _base=base_pct, _span=step_span, _name=file_path.name, _i=idx) -> None:
                            overall = min(99.0, _base + (max(0.0, min(100.0, pct)) / 100.0) * _span)
                            display = f"[leech] Uploading {_i}/{total_files}: {_name} ({pct:.0f}%)"
                            self.db.update_job(job_id, status="allocating", progress=overall, message=display)
                            if on_progress:
                                await on_progress(overall, display)

                        file_source_url = f"magnet:?xt=urn:btih:{infohash}#{idx}" if infohash else None
                        item = await self.ingest.finalize_staged_file(
                            file_path,
                            folder=target_folder,
                            tags=normalized_tags,
                            source_origin=source_origin,
                            source_kind="torrent",
                            source_url=file_source_url,
                            title=file_path.stem,
                            forward_to_telegram=forward_to_telegram,
                            progress_cb=_file_progress if uses_tg else None,
                            force=force,
                        )
                        items.append(item)
                        try:
                            file_path.unlink(missing_ok=True)
                        except OSError:
                            pass

                    self.db.update_job(
                        job_id,
                        status="done",
                        progress=100.0,
                        message=f"Saved {len(items)} file(s) to {target_folder}",
                        media_ids_json=json.dumps([i["id"] for i in items]),
                    )
                    if on_progress:
                        await on_progress(100.0, f"Saved {len(items)} file(s) to {target_folder}")
                    return items
                finally:
                    shutil.rmtree(job_dir, ignore_errors=True)
                    self.ingest.unregister_staging_dir(job_dir)
        except asyncio.CancelledError:
            if self.ingest._job_tasks.get(job_id) in (None, asyncio.current_task()):
                job = self.db.get_job(job_id)
                if not job or job.get("status") != "cancelled":
                    self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
            raise
        except Exception as exc:
            if self.ingest._job_tasks.get(job_id) not in (None, asyncio.current_task()):
                raise
            if job_id in self.ingest._cancelled_jobs:
                self.db.update_job(job_id, status="cancelled", message="Cancelled by user")
                raise asyncio.CancelledError()
            self.db.update_job(job_id, status="failed", message=str(exc))
            raise
        finally:
            self.ingest._active_slots = max(0, self.ingest._active_slots - 1)
            if self.ingest._job_tasks.get(job_id) in (None, asyncio.current_task()):
                self.ingest._active_procs.pop(job_id, None)

